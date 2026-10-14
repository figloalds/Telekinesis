//! Owned by WP2.1 in Wave 2 (WP1.12 in Wave 1; deleted by WP3.1).
//!
//! The legacy v2 lanes (one dialer thread per `(peer, revision, lane)`),
//! adapted from the global `Supervisor` mutex to the actor orchestrator
//! (REVAMP_PLAN §14 WP2.1 task 4). The exchange logic is the WP1.12 code,
//! moved mechanically; what changed is where each fact comes from:
//!
//! * the local folders: the catalog snapshot (`desired && sync_enabled`);
//! * the local worker: its controller's `FolderView` (`LegacyBridge::discovery`),
//!   never `runtime.json`;
//! * link status and remote catalogs: the bridge's own maps, held only for map
//!   operations (never across I/O);
//! * retirement: the lane's `Cancel` (the bridge cancels it when the catalog
//!   drops or re-revisions the peer), instead of re-reading the catalog every
//!   50 ms under the supervisor lock;
//! * status: `StatusEvent::{LaneState, ExchangeFinished, PeerCatalog,
//!   LegacyLink}` into the hub, and `GateEvidence` to the folder controller
//!   (the legacy gate rule, §3.3 Wave 2).
//!
//! `lane_role` and `link_key` keep their legacy signatures: the legacy
//! `api::manage` and `supervisor.rs`, which stay compiled until WP2.INT, use
//! them.
use super::{
    catalog_db::Peer,
    legacy_bridge::LegacyBridge,
    peering::{dial_role, route},
};
use crate::{
    core::id,
    model::{
        errors::ErrorWire,
        orchestrator::{Ctx, FolderCmd, FolderRecord, GateEvidence, PeerRecord},
        protocol::{LaneRole, feed::ExchangeSummary},
        status::{Direction, LaneState, StatusEvent},
    },
    paired_sync::{PublishedState, SyncPage, SyncRoute},
    runtime::{self, Discovery},
    util::{backoff::Backoff, cancel::Cancel, time::now_ms},
    ws,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Shutdown, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// While a lane waits between passes it pings at least this often, so any
/// `sync_interval_ms` (up to 60 s) stays below the acceptor's 15 s idle drop.
pub(crate) const KEEPALIVE: Duration = Duration::from_secs(5);

pub(crate) fn lane_role(lane: usize) -> &'static str {
    if lane == 0 { "control" } else { "bulk" }
}

/// The key of one lane's link status: `ENDPOINT:REVISION:LANE`.
pub(crate) fn lane_key(endpoint: &str, revision: &str, lane: usize) -> String {
    format!("{endpoint}:{revision}:{lane}")
}

/// [`lane_key`] of a legacy `catalog_db::Peer` (the legacy `api::manage` and
/// `supervisor.rs`; deleted with them).
pub(crate) fn link_key(peer: &Peer, lane: usize) -> String {
    lane_key(&peer.endpoint, &peer.revision, lane)
}

/// A paired peer as the lanes use it: the catalog's `PeerRecord`, with the
/// legacy pool helpers.
#[derive(Clone)]
pub(crate) struct LanePeer {
    pub(crate) endpoint: String,
    pub(crate) code: String,
    pub(crate) installation: String,
    pub(crate) connections: usize,
    pub(crate) revision: String,
}

impl std::fmt::Debug for LanePeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanePeer")
            .field("endpoint", &self.endpoint)
            .field("code", &"<redacted>")
            .field("installation", &self.installation)
            .field("connections", &self.connections)
            .field("revision", &self.revision)
            .finish()
    }
}

impl LanePeer {
    pub(crate) fn from_record(record: &PeerRecord) -> Self {
        Self {
            endpoint: record.endpoint.clone(),
            code: record.code.clone(),
            installation: record.installation.clone(),
            connections: usize::try_from(record.connections).unwrap_or(usize::MAX),
            revision: record.revision.clone(),
        }
    }

    /// Lane 0 is control, the rest bulk: `max(connections, 2)` lanes.
    pub(crate) fn pool_size(&self) -> usize {
        self.connections.max(2)
    }

    /// The serial bulk lane that owns `repo` (a peer without parallel bulk).
    pub(crate) fn bulk_lane(&self, repo: &str) -> usize {
        1 + repo
            .bytes()
            .fold(0usize, |n, b| n.wrapping_mul(31).wrapping_add(b as usize))
            % (self.pool_size() - 1)
    }

    pub(crate) fn key(&self, lane: usize) -> String {
        lane_key(&self.endpoint, &self.revision, lane)
    }
}

/// A duplicate of the TCP stream under `socket`, to shut it down from another
/// thread (cancellation) while a blocking call is parked on it.
pub(crate) fn socket_killer(socket: &ws::Socket) -> Option<TcpStream> {
    use tungstenite::stream::MaybeTlsStream;
    match socket.get_ref() {
        MaybeTlsStream::Plain(tcp) => tcp.try_clone().ok(),
        MaybeTlsStream::NativeTls(tls) => tls.get_ref().try_clone().ok(),
        _ => None,
    }
}

/// Registers `stream`'s shutdown on `cancel` (a per-connection child token,
/// dropped with the connection, so the hook never accumulates).
///
/// On Linux the shutdown wakes a call parked on the socket at once. Windows
/// does not wake a blocked receive on shutdown: the shutdown sends FIN, so a
/// responsive peer closes and the call ends; against a silent peer the call
/// ends at the socket's 15 s timeout (the legacy lanes' bound). A lane that
/// is waiting between passes (the common case) wakes at once on both.
pub(crate) fn kill_on_cancel(cancel: &Cancel, stream: Option<TcpStream>) {
    if let Some(stream) = stream {
        cancel.on_cancel(Box::new(move || {
            let _ = stream.shutdown(Shutdown::Both);
        }));
    }
}

/// Waits `delay` (between passes, or before a reconnect when `socket` is
/// `None`) and returns early once the lane is cancelled or the daemon is
/// stopping. With a socket it pings every `keepalive`; a failed ping ends the
/// wait with that error.
pub(crate) fn lane_wait(
    cancel: &Cancel,
    stopping: &AtomicBool,
    delay: Duration,
    mut socket: Option<&mut ws::Socket>,
    keepalive: Duration,
) -> Result<()> {
    /// `stopping` has no wake-up of its own (the bridge's shutdown cancels
    /// the lane right after setting it); this bounds a missed one.
    const STOPPING_SLICE: Duration = Duration::from_millis(250);
    let until = Instant::now() + delay;
    let mut ping_at = Instant::now() + keepalive;
    loop {
        if cancel.is_cancelled() || stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        let now = Instant::now();
        if now >= until {
            return Ok(());
        }
        if let Some(socket) = socket.as_deref_mut()
            && now >= ping_at
        {
            ws::call(socket, &json!({"op":"ping"}))?;
            ping_at = Instant::now() + keepalive;
            continue;
        }
        let wake = if socket.is_some() {
            until.min(ping_at)
        } else {
            until
        };
        let slice = wake
            .saturating_duration_since(Instant::now())
            .min(STOPPING_SLICE);
        if cancel.sleep(slice) {
            return Ok(());
        }
    }
}

/// Rotates the catalog so each pass starts at a different repository: a
/// slow or failing repository cannot always run first on its lane.
fn rotate_catalog(catalog: &mut [PublishedState], pass: usize) {
    if !catalog.is_empty() {
        let start = pass % catalog.len();
        catalog.rotate_left(start);
    }
}

/// Parts of a page that complete their object (`offset + bytes == total`).
fn completed_parts<'a>(parts: impl Iterator<Item = (u64, u64, usize)> + 'a) -> u64 {
    parts
        .filter(|(offset, total, hex)| offset.saturating_add((*hex / 2) as u64) == *total)
        .count() as u64
}

/// `(bytes, completed objects)` of a page's `object_parts` JSON.
pub(crate) fn part_stats(parts: &Value) -> (u64, u64) {
    let parts: Vec<(u64, u64, usize)> = parts
        .as_object()
        .into_iter()
        .flat_map(|parts| parts.values())
        .map(|part| {
            (
                part["offset"].as_u64().unwrap_or(0),
                part["total"].as_u64().unwrap_or(u64::MAX),
                part["hex"].as_str().map_or(0, str::len),
            )
        })
        .collect();
    let bytes = parts.iter().map(|(_, _, hex)| (*hex / 2) as u64).sum();
    (bytes, completed_parts(parts.into_iter()))
}

/// One finished exchange as the hub's `ExchangeFinished` event.
#[allow(clippy::too_many_arguments)]
pub(crate) fn exchange_event(
    installation: &str,
    repo: &str,
    lane: u32,
    op: &str,
    direction: Direction,
    error: Option<&anyhow::Error>,
    summary: Option<ExchangeSummary>,
    (bytes_up, bytes_down): (u64, u64),
    (objects_up, objects_down): (u64, u64),
    duration: Duration,
) -> StatusEvent {
    let ok = error.is_none();
    StatusEvent::ExchangeFinished {
        installation: installation.to_owned(),
        repo: repo.to_owned(),
        lane,
        op: op.to_owned(),
        direction,
        ok,
        error: error.map(ErrorWire::from_error),
        summary,
        bytes_up,
        bytes_down,
        // Only completed objects of a successful exchange count (the hub
        // treats them as progress).
        objects_up: if ok { objects_up } else { 0 },
        objects_down: if ok { objects_down } else { 0 },
        duration_ms: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
    }
}

/// What one repository exchange moved, for the status hub.
#[derive(Default)]
struct Moved {
    objects_up: u64,
    objects_down: u64,
    events_up: u32,
    events_down: u32,
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
    moved: &mut Moved,
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
    moved.objects_up = completed_parts(
        page.object_parts
            .values()
            .map(|p| (p.offset, p.total, p.hex.len())),
    );
    moved.objects_down = completed_parts(
        response
            .object_parts
            .values()
            .map(|p| (p.offset, p.total, p.hex.len())),
    );
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

/// The v6 `published-apply` result as an [`ExchangeSummary`] (§14 WP2.1
/// task 4: "v6 `metadata_progress` mapped to `ExchangeSummary`"). The v6
/// result has no feed tips or cursors; they stay 0.
pub(crate) fn summary_of(result: &Value, moved_up: u32, moved_down: u32) -> ExchangeSummary {
    let progress = &result["metadata_progress"];
    let count = |key: &str| progress[key].as_u64().unwrap_or(0);
    let transfer = &result["transfer"];
    ExchangeSummary {
        sent_events: moved_up,
        received_events: moved_down,
        to_send: count("local_only_events"),
        to_receive: count("remote_only_events"),
        local_pending: count("local_pending"),
        remote_pending: count("remote_pending"),
        blocked: count("local_blocked"),
        quarantined: count("local_quarantined"),
        caught_up: result["metadata_caught_up"] == true,
        metadata_advanced: result["metadata_advanced"] == true,
        bytes_up: transfer["sent_wire_bytes"].as_u64().unwrap_or(0),
        bytes_down: transfer["received_wire_bytes"].as_u64().unwrap_or(0),
        ..ExchangeSummary::default()
    }
}

/// One dialer lane: dial, then exchange every shared repository each pass
/// until the lane is cancelled (peer removed or re-paired, daemon shutdown).
pub(crate) fn run_lane(
    bridge: Arc<LegacyBridge>,
    ctx: Arc<Ctx>,
    peer: LanePeer,
    lane: usize,
    cancel: Cancel,
) {
    let key = peer.key(lane);
    let catalog_key = peer.key(0);
    let metadata_only = lane == 0;
    let lane_index = u32::try_from(lane).unwrap_or(u32::MAX);
    let role = if metadata_only {
        LaneRole::Control
    } else {
        LaneRole::Bulk
    };
    let active = || !cancel.is_cancelled() && !ctx.stopping.load(Ordering::Acquire);
    // Lane reconnects: 1 s → 60 s with jitter; the attempt counter resets
    // only after 30 s of healthy passes (`Backoff::success`).
    let mut connection = Backoff::lane();
    let mut workers = BTreeMap::<String, runtime::RpcLane>::new();
    let mut inventories = BTreeMap::<String, crate::bulk_sync::Page>::new();
    // The local worker address each repository was last exchanged through:
    // a new address (restarted worker) resets that repository's backoff.
    let mut addresses = BTreeMap::<String, String>::new();
    let mut last_catalog: Option<Value> = None;
    let mut pass = 0usize;
    while active() {
        // Set when the transport failed during one repository's exchange: that
        // repository is charged (and backs off), the lane only reconnects.
        let mut charged = false;
        bridge.lane_state(
            &ctx,
            &peer,
            lane_index,
            role,
            LaneState::Dialing,
            None,
            None,
        );
        let result = (|| -> Result<()> {
            let bulk_lane = (!metadata_only).then_some(crate::bulk_sync::Lane {
                index: lane.saturating_sub(1),
                count: peer.pool_size() - 1,
            });
            let (mut socket, remote, parallel) = dial_role(
                &peer.endpoint,
                &peer.code,
                &ctx.installation,
                Some(lane_role(lane)),
                bulk_lane,
            )?;
            // Cancellation (retirement, shutdown) unblocks a call parked on
            // this socket; the hook goes with the connection's token.
            let connection_cancel = cancel.child();
            kill_on_cancel(&connection_cancel, socket_killer(&socket));
            ensure!(remote == peer.installation, "PEER_INSTALLATION_CHANGED");
            inventories.clear();
            bridge.update_link(&key, |status| {
                status.transport = Some(socket.codec().to_owned());
                status.parallel_bulk = parallel;
            });
            let mut ready = false;
            while active() {
                let mut remote: Vec<PublishedState> = if metadata_only {
                    let catalog: Vec<PublishedState> =
                        serde_json::from_value(ws::call(&mut socket, &json!({"op":"list"}))?)?;
                    let value = serde_json::to_value(&catalog).unwrap_or(Value::Null);
                    if last_catalog.as_ref() != Some(&value) {
                        bridge.peer_catalog(&ctx, &peer, &catalog);
                        last_catalog = Some(value);
                    }
                    bridge.set_catalog(&catalog_key, catalog.clone());
                    catalog
                } else {
                    // Keep idle bulk sockets alive without repeating discovery
                    // or putting control work behind content transfers.
                    ws::call(&mut socket, &json!({"op":"ping"}))?;
                    bridge.catalog(&catalog_key)
                };
                // A dial alone proves nothing: connected only after a request succeeded.
                bridge.update_link(&key, |status| status.connected = true);
                if !ready {
                    ready = true;
                    bridge.lane_state(&ctx, &peer, lane_index, role, LaneState::Ready, None, None);
                }
                let snapshot = ctx.catalog.snapshot();
                let interval = ctx.config.network.sync_interval_ms;
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
                    let Some(local) = snapshot
                        .folders
                        .iter()
                        .find(|p| p.repo == remote.repo && p.desired_running && p.sync_enabled)
                    else {
                        continue;
                    };
                    active_repositories.insert(remote.repo.clone());
                    if !active() {
                        break;
                    }
                    let info = bridge
                        .discovery(&ctx, local)
                        .context("LOCAL_WORKER_UNAVAILABLE");
                    let mut due = true;
                    bridge.update_link(&key, |link| {
                        let status = link.repositories.entry(remote.repo.clone()).or_default();
                        if let Ok(info) = &info
                            && addresses
                                .insert(remote.repo.clone(), info.address.clone())
                                .is_some_and(|previous| previous != info.address)
                        {
                            status.reset_backoff();
                        }
                        due = status.retry_at.is_none_or(|until| Instant::now() >= until);
                    });
                    if !due {
                        continue;
                    }
                    let started = Instant::now();
                    let mut connection_failed = false;
                    let mut moved = Moved::default();
                    let op = if metadata_only {
                        "exchange"
                    } else if parallel {
                        "objects"
                    } else {
                        "bulk.exchange"
                    };
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
                                &mut moved,
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
                        moved.events_up = u32::try_from(sent.len()).unwrap_or(u32::MAX);
                        moved.objects_up = completed_parts(
                            page.object_parts
                                .values()
                                .map(|p| (p.offset, p.total, p.hex.len())),
                        );
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
                        moved.events_down = response["bundle"]["events"]
                            .as_array()
                            .map_or(0, |events| u32::try_from(events.len()).unwrap_or(u32::MAX));
                        moved.objects_down = part_stats(&response["object_parts"]).1;
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
                    let mut caught_up = false;
                    bridge.update_link(&key, |link| {
                        let status = link.repositories.entry(remote.repo.clone()).or_default();
                        match &exchange {
                            Ok(result) => {
                                status.succeeded(result);
                                caught_up = status.metadata_caught_up && status.error.is_none();
                                link.exchanges += 1;
                            }
                            Err(error) => {
                                // Charge the repository even when its transport
                                // failed, before the lane reconnects. Contention
                                // only defers it 50–200 ms; the next pass retries it.
                                status.failed(error);
                            }
                        }
                    });
                    if exchange.is_err() && parallel && !metadata_only {
                        // A failed bulk exchange may leave this route's
                        // cached remote inventory stale (for example a
                        // replica change): the next offer starts without it.
                        inventories.remove(&remote.repo);
                    }
                    let summary = match &exchange {
                        Ok(result) if metadata_only => {
                            Some(summary_of(result, moved.events_up, moved.events_down))
                        }
                        _ => None,
                    };
                    let transfer = exchange.as_ref().map(|result| &result["transfer"]).ok();
                    let wire_bytes =
                        |key: &str| transfer.and_then(|t| t[key].as_u64()).unwrap_or(0);
                    ctx.status.record(exchange_event(
                        &peer.installation,
                        &remote.repo,
                        lane_index,
                        op,
                        Direction::Outbound,
                        exchange.as_ref().err(),
                        summary,
                        (
                            wire_bytes("sent_wire_bytes"),
                            wire_bytes("received_wire_bytes"),
                        ),
                        (moved.objects_up, moved.objects_down),
                        started.elapsed(),
                    ));
                    if metadata_only && caught_up {
                        gate_evidence(&ctx, &snapshot.gates, local, &peer);
                    }
                    if let Err(error) = exchange
                        && connection_failed
                    {
                        charged = true;
                        return Err(error);
                    }
                }
                bridge.update_link(&key, |status| {
                    status.last_success_ms = Some(crate::core::time());
                    status.error = None;
                    status
                        .repositories
                        .retain(|repo, _| active_repositories.contains(repo));
                });
                bridge.publish_link(&ctx, &key);
                connection.success(Instant::now());
                // The configured interval is idle polling, not a bandwidth cap.
                // Keep a small yield between active pages for fairness/recovery.
                // Waiting pings the acceptor (`KEEPALIVE`) so it never idles out.
                let wait = Duration::from_millis(if transferred_content {
                    interval.min(10)
                } else {
                    interval
                });
                lane_wait(&cancel, &ctx.stopping, wait, Some(&mut socket), KEEPALIVE)?;
            }
            let _ = socket.close(None);
            Ok(())
        })();
        let error = result.err().map(|e| format!("{e:#}"));
        bridge.update_link(&key, |status| {
            status.connected = false;
            status.error = error.clone();
            for repository in status.repositories.values_mut() {
                repository.caught_up = false;
                repository.metadata_caught_up = false;
            }
        });
        bridge.publish_link(&ctx, &key);
        if active() {
            // A charged failure belongs to its repository: reconnect after the
            // base delay without escalating the lane backoff.
            let delay = if charged {
                connection.base
            } else {
                connection.failure(Instant::now())
            };
            bridge.lane_state(
                &ctx,
                &peer,
                lane_index,
                role,
                LaneState::Backoff,
                error.as_deref().map(|text| {
                    ErrorWire::new(
                        crate::model::errors::code_in(text).unwrap_or("CONNECTION_LOST"),
                        text,
                    )
                }),
                Some(now_ms().saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX))),
            );
            let _ = lane_wait(&cancel, &ctx.stopping, delay, None, KEEPALIVE);
        }
    }
    bridge.retire_lane(&key, metadata_only.then_some(catalog_key.as_str()));
    bridge.lane_state(&ctx, &peer, lane_index, role, LaneState::Closed, None, None);
}

/// The legacy gate rule (§3.3, Wave 2): a control-lane exchange that left the
/// folder `metadata_caught_up` with no error is evidence for its gate. Sent
/// only to folders that still have a gate row, and only while it is not
/// overridden; the controller ignores evidence from anyone but the source.
fn gate_evidence(
    ctx: &Ctx,
    gates: &BTreeMap<String, crate::model::orchestrator::GateRecord>,
    folder: &FolderRecord,
    peer: &LanePeer,
) {
    if gates
        .get(&folder.state)
        .is_none_or(|gate| gate.override_ms.is_some())
    {
        return;
    }
    let _ = ctx.folders.send(
        &folder.state,
        FolderCmd::GateEvidence(GateEvidence {
            installation: peer.installation.clone(),
            at_ms: now_ms(),
            caught_up: true,
            blocked: false,
            waiting_on: Vec::new(),
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::{
        net::TcpListener,
        sync::{
            Arc,
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

    #[test]
    fn lane_peers_keep_the_legacy_pool_and_keys() {
        let peer = LanePeer::from_record(&PeerRecord {
            endpoint: "ws://127.0.0.1:9".into(),
            code: "code".into(),
            installation: id(),
            connections: 1,
            revision: "r1".into(),
        });
        assert_eq!(peer.pool_size(), 2);
        assert_eq!(peer.key(0), "ws://127.0.0.1:9:r1:0");
        // One bulk lane: every repository belongs to it.
        assert_eq!(peer.bulk_lane("any"), 1);
        assert!(!format!("{peer:?}").contains("code\""));
    }

    #[test]
    fn waiting_between_passes_keeps_the_connection_alive() {
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
        let cancel = Cancel::new();
        let stopping = AtomicBool::new(false);
        let started = Instant::now();
        lane_wait(
            &cancel,
            &stopping,
            Duration::from_millis(1100),
            Some(&mut socket),
            Duration::from_millis(200),
        )
        .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(1000));
        let sent = pings.load(Ordering::Relaxed);
        assert!((4..=6).contains(&sent), "{sent} pings in 1.1 s at 200 ms");
        // A retired lane stops waiting at once.
        let waiter = {
            let cancel = cancel.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(100));
                cancel.cancel();
            })
        };
        let started = Instant::now();
        lane_wait(
            &cancel,
            &stopping,
            Duration::from_secs(30),
            Some(&mut socket),
            Duration::from_millis(200),
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        waiter.join().unwrap();
        // So does a stopping daemon.
        let stopping = AtomicBool::new(true);
        let started = Instant::now();
        lane_wait(
            &Cancel::new(),
            &stopping,
            Duration::from_secs(30),
            None,
            Duration::from_millis(200),
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_millis(100));
        done.store(true, Ordering::Relaxed);
        let _ = socket.close(None);
        drop(socket);
        server.join().unwrap();
    }

    #[test]
    fn a_failed_keepalive_ends_the_wait_with_its_error() {
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
            &Cancel::new(),
            &AtomicBool::new(false),
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

    /// Linux only: Windows does not wake a blocked receive on shutdown (see
    /// `kill_on_cancel`).
    #[cfg(unix)]
    #[test]
    fn cancelling_a_lane_unblocks_a_call_parked_on_its_socket() {
        // A peer that accepts and never answers: the parked `ws::call` returns
        // as soon as the lane's token is cancelled, not after 15 s.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut socket = ws::accept(listener.accept().unwrap().0).unwrap();
            let _ = ws::receive::<Value>(&mut socket);
            std::thread::sleep(Duration::from_secs(3));
        });
        let mut socket = ws::connect(&endpoint).unwrap();
        let cancel = Cancel::new();
        let connection = cancel.child();
        kill_on_cancel(&connection, socket_killer(&socket));
        let canceller = {
            let cancel = cancel.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(200));
                cancel.cancel();
            })
        };
        let started = Instant::now();
        assert!(ws::call(&mut socket, &json!({"op":"list"})).is_err());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        canceller.join().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn apply_results_map_onto_exchange_summaries() {
        let result = json!({"metadata_caught_up":true,"metadata_advanced":false,
            "metadata_progress":{"local_events":5,"remote_events":4,"local_only_events":1,
                "remote_only_events":0,"local_pending":2,"remote_pending":3,
                "local_blocked":1,"local_quarantined":4},
            "transfer":{"sent_wire_bytes":100,"received_wire_bytes":50}});
        let summary = summary_of(&result, 3, 2);
        assert_eq!(summary.sent_events, 3);
        assert_eq!(summary.received_events, 2);
        assert_eq!(summary.to_send, 1);
        assert_eq!(summary.to_receive, 0);
        assert_eq!(summary.local_pending, 2);
        assert_eq!(summary.remote_pending, 3);
        assert_eq!(summary.blocked, 1);
        assert_eq!(summary.quarantined, 4);
        assert!(summary.caught_up && !summary.metadata_advanced);
        assert_eq!((summary.bytes_up, summary.bytes_down), (100, 50));
        assert_eq!(
            completed_parts([(0, 4, 8), (0, 9, 8), (4, 8, 8)].into_iter()),
            2
        );
    }
}
