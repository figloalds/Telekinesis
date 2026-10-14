//! Owned by WP2.1 (deleted by WP3.1).
//!
//! Bridges the legacy v2 lanes into `NetworkApi` and `LegacyNetworkView`
//! until the cutover (REVAMP_PLAN §3.1, §7.3, §14 WP2.1 task 4). The wire is
//! unchanged (6298d0b peers interoperate); what changed is ownership:
//!
//! * **Lanes** (`lanes::run_lane`) are spawned and cancelled per
//!   `(peer, revision, lane)` by the `legacy-lanes` thread, which follows the
//!   catalog generation (the `LegacyLaneManager` of the plan). A peer that is
//!   removed or re-paired has its lanes cancelled; cancellation shuts their
//!   sockets down, so no lane outlives its peer by more than a syscall.
//! * **Link status and remote catalogs** live in two maps here (`Tracked`,
//!   held for map operations only). `LegacyNetworkView::{folder_sync,
//!   peers}` render them as the exact legacy JSON (the code of
//!   `Supervisor::inspect` and `Action::Peers`, moved).
//! * **Local workers** are found through their controllers
//!   (`FolderDirectory::view`: `Running` and its endpoint); the catalog
//!   snapshot gives the folders and the verifier. Nothing here takes a lock
//!   that is held across I/O, and nothing reads `runtime.json`.
//! * **Inbound** connections (`serve_inbound`) are served on the caller's
//!   thread (one per connection, `run.rs` caps them); each registers its
//!   socket's shutdown on a child of the bridge token.
//! * **Status**: lanes record `LaneState`, `ExchangeFinished` (with the v6
//!   `metadata_progress` as an `ExchangeSummary`), `PeerCatalog` and the
//!   `LegacyLink` passthrough; the inbound side records `InboundSession` and
//!   inbound `ExchangeFinished`; the manager records `PeerConfigured` and
//!   `PeerRemoved`. The legacy gate rule sends `FolderCmd::GateEvidence`.
use super::{
    lanes::{self, LanePeer, kill_on_cancel, lane_role},
    peering::{self, dial},
    status::LinkStatus,
};
use crate::{
    core::id,
    model::{
        errors::ErrorWire,
        orchestrator::{
            CatalogApi, CatalogCmd, CatalogSnapshot, Ctx, EntryState, FolderPhase, FolderRecord,
            LegacyNetworkView, NetworkApi, PairResult, PeerRecord, PublishedEntry, limits,
        },
        protocol::LaneRole,
        status::{Direction, LaneState, StatusEvent},
    },
    paired_sync::PublishedState,
    runtime::Discovery,
    util::{cancel::Cancel, lock::Tracked, log::event, notify::Notify, time::now_ms},
};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    net::TcpStream,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// The lane manager re-reads the catalog at least this often (it wakes at
/// once on a new generation).
const WATCH_SLICE: Duration = Duration::from_millis(250);

/// Threads of one kind (lanes, inbound connections) and a wake-up when one
/// ends, so shutdown can wait for them with a deadline.
#[derive(Default)]
struct Activity {
    count: AtomicUsize,
    ended: Notify,
}

impl Activity {
    fn enter(self: &Arc<Self>) -> ActivityGuard {
        self.count.fetch_add(1, Ordering::AcqRel);
        ActivityGuard(self.clone())
    }

    /// [`enter`](Self::enter) unless `limit` threads are already active (one
    /// atomic step: concurrent callers cannot overshoot the limit).
    fn try_enter(self: &Arc<Self>, limit: usize) -> Option<ActivityGuard> {
        self.count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < limit).then_some(active + 1)
            })
            .ok()
            .map(|_| ActivityGuard(self.clone()))
    }

    fn active(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Waits until no thread is active or `deadline` passes; returns how many
    /// are still running.
    fn wait_idle(&self, deadline: Instant) -> usize {
        loop {
            let seen = self.ended.count();
            let active = self.active();
            let remaining = deadline.saturating_duration_since(Instant::now());
            if active == 0 || remaining.is_zero() {
                return active;
            }
            self.ended.wait(seen, remaining);
        }
    }
}

struct ActivityGuard(Arc<Activity>);

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::AcqRel);
        self.0.ended.poke();
    }
}

/// A reserved inbound connection slot ([`LegacyBridge::reserve_inbound`]):
/// it counts in [`LegacyBridge::inbound_sessions`] from the reservation
/// until it is dropped.
pub struct InboundSlot {
    _held: ActivityGuard,
}

impl std::fmt::Debug for InboundSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InboundSlot")
    }
}

/// One running lane: its token and its thread (a thread that ended without
/// being retired, for example by a panic, is reaped and the lane respawned,
/// as the legacy run loop did).
struct Lane {
    cancel: Cancel,
    thread: Option<JoinHandle<()>>,
}

/// The legacy network: `NetworkApi` (v2 pair, remote list, unpair) and the
/// legacy JSON views. Create it with [`LegacyBridge::start`].
pub struct LegacyBridge {
    /// Weak: `Ctx` owns the bridge (`ctx.network`, `ctx.legacy`).
    ctx: Weak<Ctx>,
    installation: String,
    links: Tracked<BTreeMap<String, LinkStatus>>,
    catalogs: Tracked<BTreeMap<String, Vec<PublishedState>>>,
    /// Running lanes by link key, each with its own token and thread.
    lanes: Tracked<BTreeMap<String, Lane>>,
    /// Configured peers (installation → endpoint), for `PeerConfigured` and
    /// `PeerRemoved`.
    configured: Tracked<BTreeMap<String, String>>,
    lane_threads: Arc<Activity>,
    inbound_threads: Arc<Activity>,
    /// Cancels every lane, every inbound connection and the manager.
    cancel: Cancel,
    /// `reconfigure()` asks the manager to reconcile without a new generation.
    dirty: AtomicBool,
    manager: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for LegacyBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LegacyBridge")
            .field("installation", &self.installation)
            .field("lanes", &self.lane_threads.active())
            .field("inbound", &self.inbound_threads.active())
            .field("cancelled", &self.cancel.is_cancelled())
            .finish_non_exhaustive()
    }
}

fn wire(error: anyhow::Error) -> ErrorWire {
    ErrorWire::from_error(&error)
}

fn not_ready() -> ErrorWire {
    ErrorWire::coded("NETWORK_NOT_READY", "the daemon context is gone")
}

impl LegacyBridge {
    /// Starts the lane manager (`legacy-lanes`): it spawns the lanes of every
    /// catalog peer now and follows the catalog from then on. The bridge's
    /// token is a child of `ctx.shutdown`.
    pub fn start(ctx: Arc<Ctx>) -> Arc<LegacyBridge> {
        let bridge = Arc::new(LegacyBridge {
            ctx: Arc::downgrade(&ctx),
            installation: ctx.installation.clone(),
            links: Tracked::new("legacy-bridge.links", BTreeMap::new()),
            catalogs: Tracked::new("legacy-bridge.catalogs", BTreeMap::new()),
            lanes: Tracked::new("legacy-bridge.lanes", BTreeMap::new()),
            configured: Tracked::new("legacy-bridge.peers", BTreeMap::new()),
            lane_threads: Arc::default(),
            inbound_threads: Arc::default(),
            cancel: ctx.shutdown.child(),
            dirty: AtomicBool::new(false),
            manager: Mutex::new(None),
        });
        let manager = {
            let bridge = bridge.clone();
            thread::Builder::new()
                .name("legacy-lanes".into())
                .spawn(move || bridge.manage_lanes())
        };
        match manager {
            Ok(handle) => *crate::util::lock::lock(&bridge.manager) = Some(handle),
            Err(error) => {
                event!(error, NETWORK_NOT_READY, {
                    "detail": "cannot start the legacy lane manager; no lanes are dialed",
                    "error": error.to_string(),
                });
            }
        }
        bridge
    }

    fn ctx(&self) -> Result<Arc<Ctx>, ErrorWire> {
        self.ctx.upgrade().ok_or_else(not_ready)
    }

    /// Concurrent inbound connections being served.
    pub fn inbound_sessions(&self) -> usize {
        self.inbound_threads.active()
    }

    /// Running dialer lanes.
    pub fn lanes(&self) -> usize {
        self.lane_threads.active()
    }

    /// Reserves one inbound connection slot unless `limit` connections are
    /// already being served (or reserved). An accept loop reserves before it
    /// starts the connection's thread, so a burst of connections cannot
    /// exceed the cap; the slot is released when dropped.
    pub fn reserve_inbound(&self, limit: usize) -> Option<InboundSlot> {
        self.inbound_threads
            .try_enter(limit)
            .map(|held| InboundSlot { _held: held })
    }

    /// Serves one inbound legacy connection on the calling thread until the
    /// peer goes away or the bridge shuts down.
    pub fn serve_inbound(&self, stream: TcpStream) {
        let slot = InboundSlot {
            _held: self.inbound_threads.enter(),
        };
        self.serve_inbound_reserved(slot, stream);
    }

    /// [`serve_inbound`](Self::serve_inbound) in a slot taken by
    /// [`reserve_inbound`](Self::reserve_inbound); the slot is released when
    /// the connection ends.
    pub fn serve_inbound_reserved(&self, slot: InboundSlot, stream: TcpStream) {
        let _active = slot;
        let Ok(ctx) = self.ctx() else {
            return;
        };
        // The connection's shutdown hook lives on a child token dropped with
        // the connection (hooks never accumulate on the bridge token).
        let connection = self.cancel.child();
        kill_on_cancel(&connection, stream.try_clone().ok());
        if connection.is_cancelled() {
            return;
        }
        if let Err(error) = peering::serve_inbound(self, &ctx, stream) {
            // A peer that went away, an idle drop (15 s), or a shutdown.
            let _ = error;
        }
    }

    // -- the lane manager -----------------------------------------------------

    fn manage_lanes(self: Arc<Self>) {
        let mut generation = None;
        while !self.cancel.is_cancelled() {
            let Some(ctx) = self.ctx.upgrade() else {
                break;
            };
            let catalog: Arc<dyn CatalogApi> = ctx.catalog.clone();
            let snapshot = catalog.snapshot();
            let reaped = self.reap_finished();
            if generation != Some(snapshot.generation)
                || self.dirty.swap(false, Ordering::AcqRel)
                || reaped
            {
                self.reconcile(&ctx, &snapshot);
                generation = Some(snapshot.generation);
            }
            drop(ctx);
            catalog.wait_newer(snapshot.generation, WATCH_SLICE);
        }
        self.retire_all();
    }

    /// Drops lanes whose thread ended without being retired; `true` if any.
    fn reap_finished(&self) -> bool {
        let mut lanes = self.lanes.lock();
        let before = lanes.len();
        lanes.retain(|_, lane| lane.thread.as_ref().is_none_or(|t| !t.is_finished()));
        lanes.len() != before
    }

    /// Spawns the lanes the catalog wants and cancels the others.
    fn reconcile(self: &Arc<Self>, ctx: &Arc<Ctx>, snapshot: &CatalogSnapshot) {
        let live = !self.cancel.is_cancelled() && !ctx.stopping.load(Ordering::Acquire);
        let wanted: BTreeMap<String, (LanePeer, usize)> = if live {
            snapshot
                .peers
                .iter()
                .map(LanePeer::from_record)
                .flat_map(|peer| {
                    (0..peer.pool_size()).map(move |lane| (peer.key(lane), (peer.clone(), lane)))
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        let (retired, spawn) = {
            let mut lanes = self.lanes.lock();
            let gone: Vec<String> = lanes
                .keys()
                .filter(|key| !wanted.contains_key(*key))
                .cloned()
                .collect();
            let retired: Vec<Cancel> = gone
                .iter()
                .filter_map(|key| lanes.remove(key))
                .map(|lane| lane.cancel)
                .collect();
            let mut spawn: Vec<(String, LanePeer, usize, Cancel)> = Vec::new();
            for (key, (peer, lane)) in &wanted {
                if !lanes.contains_key(key) {
                    let cancel = self.cancel.child();
                    lanes.insert(
                        key.clone(),
                        Lane {
                            cancel: cancel.clone(),
                            thread: None,
                        },
                    );
                    spawn.push((key.clone(), peer.clone(), *lane, cancel));
                }
            }
            (retired, spawn)
        };
        for cancel in retired {
            cancel.cancel();
        }
        for (key, peer, lane, cancel) in spawn {
            let bridge = self.clone();
            let lane_ctx = ctx.clone();
            let guard = self.lane_threads.enter();
            let spawned = thread::Builder::new()
                .name(format!("legacy-lane-{lane}"))
                .spawn(move || {
                    let _guard = guard;
                    lanes::run_lane(bridge, lane_ctx, peer, lane, cancel);
                });
            match spawned {
                Ok(handle) => {
                    if let Some(slot) = self.lanes.lock().get_mut(&key) {
                        slot.thread = Some(handle);
                    }
                }
                Err(error) => {
                    self.lanes.lock().remove(&key);
                    event!(error, NETWORK_NOT_READY, {
                        "detail": "cannot start a legacy lane thread",
                        "lane": key,
                        "error": error.to_string(),
                    });
                }
            }
        }
        self.sync_configured(ctx, snapshot, live);
    }

    /// `PeerConfigured` / `PeerRemoved` for the peers that came and went.
    fn sync_configured(&self, ctx: &Ctx, snapshot: &CatalogSnapshot, live: bool) {
        let now: BTreeMap<String, String> = if live {
            snapshot
                .peers
                .iter()
                .map(|peer| (peer.installation.clone(), peer.endpoint.clone()))
                .collect()
        } else {
            BTreeMap::new()
        };
        let (added, removed) = {
            let mut configured = self.configured.lock();
            let added: Vec<(String, String)> = now
                .iter()
                .filter(|(installation, endpoint)| configured.get(*installation) != Some(*endpoint))
                .map(|(i, e)| (i.clone(), e.clone()))
                .collect();
            let removed: Vec<String> = configured
                .keys()
                .filter(|installation| !now.contains_key(*installation))
                .cloned()
                .collect();
            *configured = now;
            (added, removed)
        };
        for (installation, endpoint) in added {
            ctx.status.record(StatusEvent::PeerConfigured {
                installation,
                endpoint,
            });
        }
        for installation in removed {
            ctx.status.record(StatusEvent::PeerRemoved { installation });
        }
    }

    fn retire_all(&self) {
        let retired: Vec<Cancel> = std::mem::take(&mut *self.lanes.lock())
            .into_values()
            .map(|lane| lane.cancel)
            .collect();
        for cancel in retired {
            cancel.cancel();
        }
    }

    // -- state shared with the lanes ------------------------------------------

    /// Applies `change` to the link status of `key` (map operation only).
    pub(crate) fn update_link(&self, key: &str, change: impl FnOnce(&mut LinkStatus)) {
        change(self.links.lock().entry(key.to_owned()).or_default());
    }

    /// Records the link's current status as a `LegacyLink` passthrough event.
    pub(crate) fn publish_link(&self, ctx: &Ctx, key: &str) {
        let value = self.links.lock().get(key).map(|link| json!(link));
        if let Some(value) = value {
            ctx.status.record(StatusEvent::LegacyLink {
                key: key.to_owned(),
                value,
            });
        }
    }

    pub(crate) fn set_catalog(&self, key: &str, catalog: Vec<PublishedState>) {
        self.catalogs.lock().insert(key.to_owned(), catalog);
    }

    pub(crate) fn catalog(&self, key: &str) -> Vec<PublishedState> {
        self.catalogs.lock().get(key).cloned().unwrap_or_default()
    }

    /// A lane ended: its link (and, for lane 0, the peer's catalog) go.
    pub(crate) fn retire_lane(&self, key: &str, catalog_key: Option<&str>) {
        self.links.lock().remove(key);
        if let Some(catalog_key) = catalog_key {
            self.catalogs.lock().remove(catalog_key);
        }
    }

    /// The local worker of `folder`: its controller's endpoint while the
    /// folder is `Running` (never `runtime.json`).
    pub(crate) fn discovery(&self, ctx: &Ctx, folder: &FolderRecord) -> Result<Discovery> {
        let view = ctx
            .folders
            .view(&folder.state)
            .with_context(|| format!("WORKER_NOT_RUNNING: {} has no controller", folder.label))?;
        match (&view.phase, &view.worker) {
            (FolderPhase::Running { .. }, Some(worker)) => Ok(Discovery {
                format: 2,
                address: worker.address.clone(),
                state: folder.directory.to_string_lossy().into_owned(),
                mount: None,
                repo: folder.repo.clone(),
                device: folder.device.clone(),
            }),
            _ => bail!("WORKER_NOT_RUNNING: {} has no running worker", folder.label),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lane_state(
        &self,
        ctx: &Ctx,
        peer: &LanePeer,
        lane: u32,
        role: LaneRole,
        state: LaneState,
        error: Option<ErrorWire>,
        backoff_until_ms: Option<u64>,
    ) {
        ctx.status.record(StatusEvent::LaneState {
            installation: peer.installation.clone(),
            lane,
            role,
            direction: Direction::Outbound,
            state,
            error,
            backoff_until_ms,
            rtt_ms: None,
        });
    }

    pub(crate) fn peer_catalog(&self, ctx: &Ctx, peer: &LanePeer, catalog: &[PublishedState]) {
        let at_ms = now_ms();
        ctx.status.record(StatusEvent::PeerCatalog {
            installation: peer.installation.clone(),
            entries: catalog.iter().map(|state| entry_of(state, at_ms)).collect(),
        });
    }

    pub(crate) fn inbound_session(
        &self,
        ctx: &Ctx,
        installation: &str,
        lane: u32,
        role: LaneRole,
        opened: bool,
    ) {
        ctx.status.record(StatusEvent::InboundSession {
            installation: installation.to_owned(),
            lane,
            role,
            opened,
        });
    }

    // -- the legacy JSON views ------------------------------------------------

    fn peers_of(&self) -> Vec<PeerRecord> {
        self.ctx
            .upgrade()
            .map(|ctx| ctx.catalog.snapshot().peers.clone())
            .unwrap_or_default()
    }

    /// The legacy `sync` member of `folder` against `peers` (the code of
    /// `Supervisor::inspect`).
    pub(crate) fn folder_sync_with(&self, folder: &FolderRecord, peers: &[PeerRecord]) -> Value {
        let links = self.links.lock();
        let catalogs = self.catalogs.lock();
        json!(
            peers
                .iter()
                .map(LanePeer::from_record)
                .map(|peer| {
                    let link = links.get(&peer.key(0));
                    let remote_available = link
                        .filter(|l| l.connected)
                        .and_then(|_| catalogs.get(&peer.key(0)))
                        .map(|catalog| catalog.iter().any(|p| p.repo == folder.repo));
                    json!({"endpoint":peer.endpoint,"connected":link.is_some_and(|l| l.connected),
                        "remote_available":remote_available,
                        "error":link.and_then(|l| l.error.as_ref()),
                        "metadata":link.and_then(|l| l.repositories.get(&folder.repo)),
                        "lanes":(0..peer.pool_size()).map(|lane| {
                            let status = links.get(&peer.key(lane));
                            json!({"lane":lane,"role":lane_role(lane),
                                "connected":status.is_some_and(|s| s.connected),
                                "transport":status.and_then(|s| s.transport.as_ref()),
                                "parallel_bulk":status.is_some_and(|s| s.parallel_bulk),
                                "error":status.and_then(|s| s.error.as_ref()),
                                "repository":status.and_then(|s| s.repositories.get(&folder.repo))})
                        }).collect::<Vec<_>>()})
                })
                .collect::<Vec<_>>()
        )
    }

    /// The legacy Peers reply against `peers` (the code of `Action::Peers`).
    pub(crate) fn peers_with(&self, peers: &[PeerRecord]) -> Value {
        let links = self.links.lock();
        json!(
            peers
                .iter()
                .map(LanePeer::from_record)
                .map(|p| json!({"endpoint":p.endpoint,"installation":p.installation,
                    "connections":p.pool_size(),"configured_connections":p.connections,
                    "reserved_connections":1,"lanes":(0..p.pool_size()).map(|lane| {
                        let mut status = json!(links.get(&p.key(lane)).cloned().unwrap_or_default());
                        status["role"] = json!(lane_role(lane));
                        status
                    }).collect::<Vec<_>>()}))
                .collect::<Vec<_>>()
        )
    }
}

/// A legacy published folder as a `PublishedEntry` (`running`: the legacy
/// `list` held only running workers).
fn entry_of(state: &PublishedState, at_ms: u64) -> PublishedEntry {
    PublishedEntry {
        repo: state.repo.clone(),
        replica: state.replica.clone(),
        label: state.label.clone(),
        branches: state.branches.clone(),
        state: EntryState::Running,
        tip: None,
        sync_generation: 0,
        content: None,
        error: None,
        updated_at_ms: at_ms,
    }
}

impl NetworkApi for LegacyBridge {
    /// The legacy pairing: a control dial with the code, then the peer row
    /// (a new revision restarts its lanes).
    fn pair(
        &self,
        endpoint: &str,
        code: &str,
        connections: Option<u32>,
    ) -> Result<PairResult, ErrorWire> {
        let ctx = self.ctx()?;
        let configured = connections
            .unwrap_or_else(|| u32::try_from(ctx.config.network.connections).unwrap_or(u32::MAX));
        if !(1..=16).contains(&configured) {
            return Err(ErrorWire::new(
                "INVALID_CONNECTION_COUNT",
                "INVALID_CONNECTION_COUNT",
            ));
        }
        let (socket, remote) = dial(endpoint, code, &self.installation).map_err(wire)?;
        drop(socket);
        if remote == self.installation {
            return Err(ErrorWire::new(
                "CANNOT_PAIR_WITH_SELF",
                "CANNOT_PAIR_WITH_SELF",
            ));
        }
        ctx.catalog.apply(CatalogCmd::UpsertPeer {
            record: PeerRecord {
                endpoint: endpoint.to_owned(),
                code: code.to_owned(),
                installation: remote.clone(),
                connections: configured,
                revision: id(),
            },
        })?;
        Ok(PairResult {
            endpoint: endpoint.to_owned(),
            installation: remote,
            connections: configured.max(limits::MIN_POOL),
            configured_connections: configured,
            reserved_connections: 1,
        })
    }

    fn remote_list(&self, endpoint: &str) -> Result<Vec<PublishedEntry>, ErrorWire> {
        let ctx = self.ctx()?;
        let peer = ctx
            .catalog
            .snapshot()
            .peer_by_endpoint(endpoint)
            .cloned()
            .ok_or_else(|| ErrorWire::new("PEER_NOT_PAIRED", "PEER_NOT_PAIRED"))?;
        drop(ctx);
        let catalog = peering::remote_catalog(
            &peer.endpoint,
            &peer.code,
            &self.installation,
            &peer.installation,
        )
        .map_err(wire)?;
        let at_ms = now_ms();
        Ok(catalog.iter().map(|state| entry_of(state, at_ms)).collect())
    }

    /// Removes the peer row; the lane manager cancels its lanes at the new
    /// catalog generation.
    fn unpair(&self, endpoint: &str) -> Result<(), ErrorWire> {
        let ctx = self.ctx()?;
        ctx.catalog.apply(CatalogCmd::RemovePeer {
            endpoint: endpoint.to_owned(),
        })?;
        Ok(())
    }

    fn reconfigure(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Cancels every lane and inbound connection (their sockets are shut
    /// down), then waits for their threads until `deadline`.
    fn shutdown(&self, deadline: Duration) {
        let until = Instant::now() + deadline;
        self.cancel.cancel();
        self.retire_all();
        let lanes = self.lane_threads.wait_idle(until);
        let inbound = self.inbound_threads.wait_idle(until);
        let manager = crate::util::lock::lock(&self.manager).take();
        if let Some(manager) = manager {
            // The manager wakes within one catalog wait slice.
            while !manager.is_finished() && Instant::now() < until {
                thread::sleep(Duration::from_millis(10));
            }
            if manager.is_finished() {
                let _ = manager.join();
            }
        }
        if lanes + inbound > 0 {
            event!(warn, REQUEST_TIMEOUT, {
                "detail": "legacy network threads still running at the shutdown deadline",
                "lanes": lanes,
                "inbound": inbound,
            });
        }
    }
}

impl LegacyNetworkView for LegacyBridge {
    fn folder_sync(&self, folder: &FolderRecord) -> Value {
        self.folder_sync_with(folder, &self.peers_of())
    }

    fn peers(&self) -> Value {
        self.peers_with(&self.peers_of())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_inbound_slots_never_exceed_the_limit() {
        let activity = Arc::new(Activity::default());
        let held: Vec<_> = (0..5).filter_map(|_| activity.try_enter(3)).collect();
        assert_eq!(held.len(), 3);
        assert_eq!(activity.active(), 3);
        drop(held);
        assert_eq!(activity.active(), 0);
        // Concurrent reservations (an accept burst): the count never passes
        // the limit, and every slot is released.
        let peak = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (activity, peak) = (activity.clone(), peak.clone());
                thread::spawn(move || {
                    for _ in 0..2_000 {
                        if let Some(slot) = activity.try_enter(3) {
                            peak.fetch_max(activity.active(), Ordering::AcqRel);
                            drop(slot);
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(peak.load(Ordering::Acquire) <= 3, "{peak:?}");
        assert_eq!(activity.active(), 0);
    }
}
