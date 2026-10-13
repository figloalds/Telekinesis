use super::{
    catalog_db::Peer,
    peering::{dial_role, route},
    status::retry_delay,
    supervisor::Shared,
};
use crate::{
    core::id,
    paired_sync::{PublishedState, SyncPage, SyncRoute},
    runtime::{self, Discovery},
    ws,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

pub(crate) fn lane_role(lane: usize) -> &'static str {
    if lane == 0 { "control" } else { "bulk" }
}
pub(crate) fn link_key(peer: &Peer, lane: usize) -> String {
    format!("{}:{}:{lane}", peer.endpoint, peer.revision)
}
fn lane_active(shared: &Shared, peer: &Peer) -> bool {
    let s = shared.lock().unwrap();
    !s.stopping.load(Ordering::Relaxed)
        && s.peers().is_ok_and(|peers| {
            peers
                .iter()
                .any(|p| p.endpoint == peer.endpoint && p.revision == peer.revision)
        })
}

fn lane_wait(shared: &Shared, peer: &Peer, delay: Duration) {
    let until = Instant::now() + delay;
    while Instant::now() < until && lane_active(shared, peer) {
        std::thread::sleep(
            Duration::from_millis(50).min(until.saturating_duration_since(Instant::now())),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn exchange_bulk(
    worker: &mut runtime::RpcLane,
    info: &Discovery,
    binding: &SyncRoute,
    lane: crate::bulk_sync::Lane,
    socket: &mut ws::Socket,
    inventories: &mut BTreeMap<String, crate::bulk_sync::Page>,
    connection_failed: &mut bool,
) -> Result<Value> {
    let started = Instant::now();
    let page: crate::bulk_sync::Page = serde_json::from_value(worker.call(info, &id(),
        json!({"op":"published-bulk-offer","route":binding,"lane":lane,"inventory":inventories.get(&binding.repo)}))
        .context("LOCAL_BULK_OFFER_FAILED")?)?;
    let offer_ms = started.elapsed().as_millis();
    let sent_bytes = page.object_bytes();
    let exchange_started = Instant::now();
    let wire_before = (socket.sent_bytes, socket.received_bytes);
    let response: crate::bulk_sync::Page = serde_json::from_value(
        ws::call(socket, &json!({"op":"objects","page":page})).map_err(|error| {
            *connection_failed = error.downcast_ref::<ws::RemoteError>().is_none();
            error.context("REMOTE_BULK_EXCHANGE_FAILED")
        })?,
    )?;
    let remote_ms = exchange_started.elapsed().as_millis();
    let received_bytes = response.object_bytes();
    let apply_started = Instant::now();
    let mut result = worker.call(info, &id(),
        json!({"op":"published-bulk-apply","route":binding,"lane":lane,"page":response,"request":page.request}))
        .context("LOCAL_BULK_APPLY_FAILED")?;
    result["transfer"] = json!({"offer_ms":offer_ms,"remote_exchange_ms":remote_ms,
        "apply_ms":apply_started.elapsed().as_millis(),"total_ms":started.elapsed().as_millis(),
        "sent_object_bytes":sent_bytes,"received_object_bytes":received_bytes,
        "sent_wire_bytes":socket.sent_bytes-wire_before.0,"received_wire_bytes":socket.received_bytes-wire_before.1,
        "transport":socket.codec(),"object_lane":lane});
    // Both inventories describe verified objects when the corresponding page
    // was produced. Receipt of this page can advance them on the next exchange.
    result["object_inventory"] = json!({"local_available":page.known_objects.len(),
        "remote_available":response.known_objects.len(),
        "local_only":page.known_objects.difference(&response.known_objects).count(),
        "remote_only":response.known_objects.difference(&page.known_objects).count(),
        "local_partial":page.partial_objects.len(),"remote_partial":response.partial_objects.len()});
    inventories.insert(binding.repo.clone(), response.inventory());
    Ok(result)
}

pub(crate) fn run_lane(shared: Shared, peer: Peer, lane: usize) {
    let key = link_key(&peer, lane);
    let catalog_key = link_key(&peer, 0);
    let metadata_only = lane == 0;
    let mut connection_failures = 0u32;
    let mut workers = BTreeMap::<String, runtime::RpcLane>::new();
    let mut inventories = BTreeMap::<String, crate::bulk_sync::Page>::new();
    while lane_active(&shared, &peer) {
        let result = (|| -> Result<()> {
            let installation = shared.lock().unwrap().installation.clone();
            let bulk_lane = (!metadata_only).then_some(crate::bulk_sync::Lane {
                index: lane.saturating_sub(1),
                count: peer.pool_size() - 1,
            });
            let (mut socket, remote, parallel) = dial_role(
                &peer.endpoint,
                &peer.code,
                &installation,
                Some(lane_role(lane)),
                bulk_lane,
            )?;
            ensure!(remote == peer.installation, "PEER_INSTALLATION_CHANGED");
            inventories.clear();
            {
                let mut s = shared.lock().unwrap();
                let status = s.links.entry(key.clone()).or_default();
                status.transport = Some(socket.codec().to_owned());
                status.parallel_bulk = parallel;
            }
            shared
                .lock()
                .unwrap()
                .links
                .entry(key.clone())
                .or_default()
                .connected = true;
            while lane_active(&shared, &peer) {
                let remote: Vec<PublishedState> = if metadata_only {
                    let catalog: Vec<PublishedState> =
                        serde_json::from_value(ws::call(&mut socket, &json!({"op":"list"}))?)?;
                    shared
                        .lock()
                        .unwrap()
                        .remote_catalogs
                        .insert(catalog_key.clone(), catalog.clone());
                    catalog
                } else {
                    // Keep idle bulk sockets alive without repeating discovery
                    // or putting control work behind content transfers.
                    ws::call(&mut socket, &json!({"op":"ping"}))?;
                    shared
                        .lock()
                        .unwrap()
                        .remote_catalogs
                        .get(&catalog_key)
                        .cloned()
                        .unwrap_or_default()
                };
                let (local, interval) = {
                    let s = shared.lock().unwrap();
                    (s.projects()?, s.config.network.sync_interval_ms)
                };
                let mut active_repositories = BTreeSet::new();
                let mut transferred_content = false;
                for remote in remote {
                    // Events visit every repository on the reserved lane;
                    // object cursors have exactly one serial bulk owner.
                    if !metadata_only && !parallel && peer.bulk_lane(&remote.repo) != lane {
                        continue;
                    }
                    let Some(local) = local
                        .iter()
                        .find(|p| p.repo == remote.repo && p.desired_running)
                    else {
                        continue;
                    };
                    active_repositories.insert(remote.repo.clone());
                    if !lane_active(&shared, &peer) {
                        break;
                    }
                    {
                        let mut s = shared.lock().unwrap();
                        let status = s
                            .links
                            .entry(key.clone())
                            .or_default()
                            .repositories
                            .entry(remote.repo.clone())
                            .or_default();
                        if status.retry_at.is_some_and(|until| Instant::now() < until) {
                            continue;
                        }
                    }
                    let mut connection_failed = false;
                    let exchange = (|| -> Result<Value> {
                        let info = shared
                            .lock()
                            .unwrap()
                            .discovery(&local.state)
                            .context("LOCAL_WORKER_UNAVAILABLE")?;
                        let binding = route(&info, &peer.installation, &remote.replica);
                        let worker = workers.entry(local.state.clone()).or_default();
                        if parallel && !metadata_only {
                            let result = exchange_bulk(
                                worker,
                                &info,
                                &binding,
                                bulk_lane.unwrap(),
                                &mut socket,
                                &mut inventories,
                                &mut connection_failed,
                            )?;
                            transferred_content |= result["transfer"]["sent_object_bytes"]
                                .as_u64()
                                .unwrap_or(0)
                                > 0
                                || result["transfer"]["received_object_bytes"]
                                    .as_u64()
                                    .unwrap_or(0)
                                    > 0;
                            return Ok(result);
                        }
                        let started = Instant::now();
                        let page: SyncPage = serde_json::from_value(worker.call(
                            &info,
                            &id(),
                            json!({"op":"published-offer","route":binding,"max_frame":4*1024*1024,"metadata_only":metadata_only}),
                        ).context("LOCAL_SYNC_OFFER_FAILED")?)?;
                        let offer_ms = started.elapsed().as_millis();
                        let sent_bytes = page
                            .bundle
                            .objects
                            .values()
                            .map(|hex| hex.len() / 2)
                            .sum::<usize>()
                            + page
                                .object_parts
                                .values()
                                .map(|part| part.hex.len() / 2)
                                .sum::<usize>();
                        let sent: BTreeSet<_> =
                            page.bundle.events.iter().map(|e| e.id.clone()).collect();
                        let exchange_started = Instant::now();
                        let wire_before = (socket.sent_bytes, socket.received_bytes);
                        let response = ws::call(&mut socket, &json!({"op":"exchange","page":page}))
                            .map_err(|error| {
                                connection_failed =
                                    error.downcast_ref::<ws::RemoteError>().is_none();
                                error.context("REMOTE_SYNC_EXCHANGE_FAILED")
                            })?;
                        let remote_ms = exchange_started.elapsed().as_millis();
                        let received_bytes = response["bundle"]["objects"]
                            .as_object()
                            .into_iter()
                            .flat_map(|objects| objects.values())
                            .filter_map(Value::as_str)
                            .map(|hex| hex.len() / 2)
                            .sum::<usize>()
                            + response["object_parts"]
                                .as_object()
                                .into_iter()
                                .flat_map(|parts| parts.values())
                                .filter_map(|part| part["hex"].as_str())
                                .map(|hex| hex.len() / 2)
                                .sum::<usize>();
                        let apply_started = Instant::now();
                        let mut result = worker.call(
                            &info,
                            &id(),
                            json!({"op":"published-apply","route":binding,"page":response,"request":page.request,"sent":sent,"max_frame":4*1024*1024,"metadata_only":metadata_only}),
                        ).context("LOCAL_SYNC_APPLY_FAILED")?;
                        transferred_content |= sent_bytes > 0
                            || received_bytes > 0
                            || result["metadata_advanced"] == true;
                        result["transfer"] = json!({"offer_ms":offer_ms,"remote_exchange_ms":remote_ms,
                            "apply_ms":apply_started.elapsed().as_millis(),"total_ms":started.elapsed().as_millis(),
                            "sent_object_bytes":sent_bytes,"received_object_bytes":received_bytes,
                            "sent_wire_bytes":socket.sent_bytes-wire_before.0,"received_wire_bytes":socket.received_bytes-wire_before.1,"transport":socket.codec()});
                        Ok(result)
                    })();
                    if connection_failed {
                        return exchange.map(|_| ());
                    }
                    let mut s = shared.lock().unwrap();
                    let link = s.links.entry(key.clone()).or_default();
                    let status = link.repositories.entry(remote.repo).or_default();
                    match exchange {
                        Ok(result) => {
                            status.succeeded(&result);
                            link.exchanges += 1;
                        }
                        Err(error) => status.failed(&error),
                    }
                }
                {
                    let mut s = shared.lock().unwrap();
                    let status = s.links.entry(key.clone()).or_default();
                    status.last_success_ms = Some(crate::core::time());
                    status.error = None;
                    status
                        .repositories
                        .retain(|repo, _| active_repositories.contains(repo));
                }
                connection_failures = 0;
                // The configured interval is idle polling, not a bandwidth cap.
                // Keep a small yield between active pages for fairness/recovery.
                lane_wait(
                    &shared,
                    &peer,
                    Duration::from_millis(if transferred_content {
                        interval.min(10)
                    } else {
                        interval
                    }),
                );
            }
            let _ = socket.close(None);
            Ok(())
        })();
        {
            let mut s = shared.lock().unwrap();
            let status = s.links.entry(key.clone()).or_default();
            status.connected = false;
            status.error = result.err().map(|e| format!("{e:#}"));
            for repository in status.repositories.values_mut() {
                repository.caught_up = false;
                repository.metadata_caught_up = false;
            }
        }
        if lane_active(&shared, &peer) {
            connection_failures = connection_failures.saturating_add(1);
            lane_wait(&shared, &peer, retry_delay(connection_failures));
        }
    }
    shared.lock().unwrap().links.remove(&key);
    if metadata_only {
        shared.lock().unwrap().remote_catalogs.remove(&catalog_key);
    }
}
