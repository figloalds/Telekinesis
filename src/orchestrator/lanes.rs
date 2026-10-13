use super::{
    catalog_db::Peer,
    peering::{dial_role, route},
    supervisor::Shared,
};
use crate::{
    core::id,
    paired_sync::{PublishedState, SyncPage, SyncRoute},
    runtime::{self, Discovery},
    util::{backoff::Backoff, lock::lock},
    ws,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

/// While a lane waits between passes it pings at least this often, so any
/// `sync_interval_ms` (up to 60 s) stays below the acceptor's 15 s idle drop.
pub(crate) const KEEPALIVE: Duration = Duration::from_secs(5);

pub(crate) fn lane_role(lane: usize) -> &'static str {
    if lane == 0 { "control" } else { "bulk" }
}
pub(crate) fn link_key(peer: &Peer, lane: usize) -> String {
    format!("{}:{}:{lane}", peer.endpoint, peer.revision)
}
fn lane_active(shared: &Shared, peer: &Peer) -> bool {
    let s = lock(shared);
    !s.stopping.load(Ordering::Relaxed)
        && s.peers().is_ok_and(|peers| {
            peers
                .iter()
                .any(|p| p.endpoint == peer.endpoint && p.revision == peer.revision)
        })
}

/// Waits `delay` (between passes, or before a reconnect when `socket` is
/// `None`) and returns early once the lane is retired. With a socket it pings
/// every `keepalive`; a failed ping ends the wait with that error.
fn lane_wait(
    shared: &Shared,
    peer: &Peer,
    delay: Duration,
    mut socket: Option<&mut ws::Socket>,
    keepalive: Duration,
) -> Result<()> {
    let until = Instant::now() + delay;
    let mut ping_at = Instant::now() + keepalive;
    while Instant::now() < until && lane_active(shared, peer) {
        if let Some(socket) = socket.as_deref_mut()
            && Instant::now() >= ping_at
        {
            ws::call(socket, &json!({"op":"ping"}))?;
            ping_at = Instant::now() + keepalive;
        }
        let wake = if socket.is_some() {
            until.min(ping_at)
        } else {
            until
        };
        std::thread::sleep(
            Duration::from_millis(50).min(wake.saturating_duration_since(Instant::now())),
        );
    }
    Ok(())
}

/// Rotates the catalog so each pass starts at a different repository: a
/// slow or failing repository cannot always run first on its lane.
fn rotate_catalog(catalog: &mut [PublishedState], pass: usize) {
    if !catalog.is_empty() {
        let start = pass % catalog.len();
        catalog.rotate_left(start);
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
    // Lane reconnects: 1 s → 60 s with jitter; the attempt counter resets
    // only after 30 s of healthy passes (`Backoff::success`).
    let mut connection = Backoff::lane();
    let mut workers = BTreeMap::<String, runtime::RpcLane>::new();
    let mut inventories = BTreeMap::<String, crate::bulk_sync::Page>::new();
    // The local worker address each repository was last exchanged through:
    // a new address (restarted worker) resets that repository's backoff.
    let mut addresses = BTreeMap::<String, String>::new();
    let mut pass = 0usize;
    while lane_active(&shared, &peer) {
        // Set when the transport failed during one repository's exchange: that
        // repository is charged (and backs off), the lane only reconnects.
        let mut charged = false;
        let result = (|| -> Result<()> {
            let installation = lock(&shared).installation.clone();
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
                let mut s = lock(&shared);
                let status = s.links.entry(key.clone()).or_default();
                status.transport = Some(socket.codec().to_owned());
                status.parallel_bulk = parallel;
            }
            while lane_active(&shared, &peer) {
                let mut remote: Vec<PublishedState> = if metadata_only {
                    let catalog: Vec<PublishedState> =
                        serde_json::from_value(ws::call(&mut socket, &json!({"op":"list"}))?)?;
                    lock(&shared)
                        .remote_catalogs
                        .insert(catalog_key.clone(), catalog.clone());
                    catalog
                } else {
                    // Keep idle bulk sockets alive without repeating discovery
                    // or putting control work behind content transfers.
                    ws::call(&mut socket, &json!({"op":"ping"}))?;
                    lock(&shared)
                        .remote_catalogs
                        .get(&catalog_key)
                        .cloned()
                        .unwrap_or_default()
                };
                // A dial alone proves nothing: connected only after a request succeeded.
                lock(&shared)
                    .links
                    .entry(key.clone())
                    .or_default()
                    .connected = true;
                let (local, interval) = {
                    let s = lock(&shared);
                    (s.projects()?, s.config.network.sync_interval_ms)
                };
                rotate_catalog(&mut remote, pass);
                pass = pass.wrapping_add(1);
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
                    let info = {
                        let mut s = lock(&shared);
                        let info = s
                            .discovery(&local.state)
                            .context("LOCAL_WORKER_UNAVAILABLE");
                        let status = s
                            .links
                            .entry(key.clone())
                            .or_default()
                            .repositories
                            .entry(remote.repo.clone())
                            .or_default();
                        if let Ok(info) = &info
                            && addresses
                                .insert(remote.repo.clone(), info.address.clone())
                                .is_some_and(|previous| previous != info.address)
                        {
                            status.reset_backoff();
                        }
                        if status.retry_at.is_some_and(|until| Instant::now() < until) {
                            continue;
                        }
                        info
                    };
                    let mut connection_failed = false;
                    let exchange = (|| -> Result<Value> {
                        let info = info?;
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
                    let mut s = lock(&shared);
                    let link = s.links.entry(key.clone()).or_default();
                    let status = link.repositories.entry(remote.repo.clone()).or_default();
                    match exchange {
                        Ok(result) => {
                            status.succeeded(&result);
                            link.exchanges += 1;
                        }
                        Err(error) => {
                            // A failed bulk exchange may leave this route's
                            // cached remote inventory stale (for example a
                            // replica change): the next offer starts without it.
                            if parallel && !metadata_only {
                                inventories.remove(&remote.repo);
                            }
                            // Charge the repository even when its transport
                            // failed, before the lane reconnects. Contention
                            // only defers it 50–200 ms; the next pass retries it.
                            status.failed(&error);
                            if connection_failed {
                                charged = true;
                                return Err(error);
                            }
                        }
                    }
                }
                {
                    let mut s = lock(&shared);
                    let status = s.links.entry(key.clone()).or_default();
                    status.last_success_ms = Some(crate::core::time());
                    status.error = None;
                    status
                        .repositories
                        .retain(|repo, _| active_repositories.contains(repo));
                }
                connection.success(Instant::now());
                // The configured interval is idle polling, not a bandwidth cap.
                // Keep a small yield between active pages for fairness/recovery.
                // Waiting pings the acceptor (`KEEPALIVE`) so it never idles out.
                let wait = Duration::from_millis(if transferred_content {
                    interval.min(10)
                } else {
                    interval
                });
                lane_wait(&shared, &peer, wait, Some(&mut socket), KEEPALIVE)?;
            }
            let _ = socket.close(None);
            Ok(())
        })();
        {
            let mut s = lock(&shared);
            let status = s.links.entry(key.clone()).or_default();
            status.connected = false;
            status.error = result.err().map(|e| format!("{e:#}"));
            for repository in status.repositories.values_mut() {
                repository.caught_up = false;
                repository.metadata_caught_up = false;
            }
        }
        if lane_active(&shared, &peer) {
            // A charged failure belongs to its repository: reconnect after the
            // base delay without escalating the lane backoff.
            let delay = if charged {
                connection.base
            } else {
                connection.failure(Instant::now())
            };
            let _ = lane_wait(&shared, &peer, delay, None, KEEPALIVE);
        }
    }
    lock(&shared).links.remove(&key);
    if metadata_only {
        lock(&shared).remote_catalogs.remove(&catalog_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::{Config, Network, Supervisor};
    use std::{
        net::TcpListener,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize},
        },
    };

    fn state(repo: &str) -> PublishedState {
        PublishedState {
            repo: repo.into(),
            replica: id(),
            label: repo.into(),
            branches: vec![],
        }
    }

    #[test]
    fn every_repository_leads_a_pass_in_turn() {
        let catalog: Vec<_> = ["a", "b", "c"].into_iter().map(state).collect();
        let firsts: Vec<_> = (0..6)
            .map(|pass| {
                let mut rotated = catalog.clone();
                rotate_catalog(&mut rotated, pass);
                let order: Vec<_> = rotated.iter().map(|p| p.repo.clone()).collect();
                let mut sorted = order.clone();
                sorted.sort();
                assert_eq!(sorted, ["a", "b", "c"], "rotation keeps every repository");
                order[0].clone()
            })
            .collect();
        assert_eq!(firsts, ["a", "b", "c", "a", "b", "c"]);
        let mut empty: Vec<PublishedState> = vec![];
        rotate_catalog(&mut empty, 7);
    }

    /// A peer row (so `lane_active` holds) in a fresh supervisor catalog.
    fn supervisor_with_peer(root: &std::path::Path) -> (Shared, Peer) {
        let supervisor = Supervisor::open(Config {
            format_version: 2,
            data_directory: root.join("data"),
            default_mount_directory: root.join("mounts"),
            maximum_running: 1,
            network: Network::default(),
        })
        .unwrap();
        supervisor
            .upsert_peer("ws://127.0.0.1:9", "code", &id(), 2)
            .unwrap();
        let peer = supervisor.peers().unwrap().remove(0);
        (Arc::new(Mutex::new(supervisor)), peer)
    }

    #[test]
    fn waiting_between_passes_keeps_the_connection_alive() {
        let root = tempfile::tempdir().unwrap();
        let (shared, peer) = supervisor_with_peer(root.path());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let pings = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let server = {
            let (pings, done) = (pings.clone(), done.clone());
            std::thread::spawn(move || {
                let mut socket = ws::accept(listener.accept().unwrap().0).unwrap();
                while !done.load(Ordering::Relaxed) {
                    let Ok(request) = ws::receive::<Value>(&mut socket) else {
                        return;
                    };
                    assert_eq!(request["op"], "ping");
                    pings.fetch_add(1, Ordering::Relaxed);
                    ws::reply(&mut socket, Ok(json!({"alive":true}))).unwrap();
                }
            })
        };
        let mut socket = ws::connect(&endpoint).unwrap();
        let started = Instant::now();
        lane_wait(
            &shared,
            &peer,
            Duration::from_millis(1100),
            Some(&mut socket),
            Duration::from_millis(200),
        )
        .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(1000));
        let sent = pings.load(Ordering::Relaxed);
        assert!((4..=6).contains(&sent), "{sent} pings in 1.1 s at 200 ms");
        // A retired lane stops waiting at once.
        lock(&shared).stopping.store(true, Ordering::Relaxed);
        let started = Instant::now();
        lane_wait(
            &shared,
            &peer,
            Duration::from_secs(30),
            Some(&mut socket),
            Duration::from_millis(200),
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        done.store(true, Ordering::Relaxed);
        let _ = socket.close(None);
        drop(socket);
        server.join().unwrap();
    }

    #[test]
    fn a_failed_keepalive_ends_the_wait_with_its_error() {
        let root = tempfile::tempdir().unwrap();
        let (shared, peer) = supervisor_with_peer(root.path());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut socket = ws::accept(listener.accept().unwrap().0).unwrap();
            let _: Value = ws::receive(&mut socket).unwrap();
            // Close instead of answering, as an idle-dropping acceptor would.
            let _ = socket.close(None);
            let _ = socket.flush();
        });
        let mut socket = ws::connect(&endpoint).unwrap();
        let error = lane_wait(
            &shared,
            &peer,
            Duration::from_secs(10),
            Some(&mut socket),
            Duration::from_millis(100),
        )
        .unwrap_err();
        assert!(
            error.downcast_ref::<ws::RemoteError>().is_none(),
            "{error:#}"
        );
        server.join().unwrap();
    }
}
