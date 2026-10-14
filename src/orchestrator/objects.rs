//! Owned by WP2.7 (interim, Wave 2) and WP3.3 (objects v3, Wave 3).
//!
//! One bulk lane of a peer (pool lanes 1..N-1), on the interim path of
//! REVAMP_PLAN §6.5: for every repository shared with the peer (its catalog
//! lists it, and a local folder holds it with sync enabled), worker
//! `published-bulk-offer`, then peer `objects.exchange`, then worker
//! `published-bulk-apply`. The pages are the legacy `bulk_sync::Page` (bulk
//! protocol 1, hex parts) with `Lane{index, count}` taken from the lane's
//! [`LaneSpec`], so objects keep exactly one owning lane (invariant 19).
//! Bulk lanes carry no metadata: they never stage, activate or acknowledge
//! events (invariant 18).
//!
//! The lane is scheduled by the same engine as the control lane
//! ([`super::scheduler`]): per-(peer, repo, `objects.exchange`) keys, at
//! most `REPOS_PER_LANE` (2) exchanges at once and one per repository, a
//! per-key backoff by class, and no counting of Contention or of folders
//! that are not serving. After an exchange that moved object bytes the key
//! runs again after 10 ms; otherwise it waits `sync_interval_ms`, or a
//! trigger: the local feed tip or content generation moved (new objects to
//! want or offer), or the peer's published tip or content counters moved.
//!
//! **Wire shape.** Request: header `{repo, lane}`, body the JSON page.
//! Reply: the JSON page (`reply_to` = our request), as the body or streamed
//! as body CHUNK frames (`hash: None`, in offset order) before the RESPONSE.
//! A request body must fit the session's per-op body limit
//! (`protocol::max_body`), which is 1 MiB for `objects.*` until CCR-WP2.7-1
//! raises it for `objects.exchange`: a larger page fails its exchange with
//! `WS_MESSAGE_TOO_LARGE`, charged to its key. A plain reply body has the
//! same limit on the acceptor's side; a streamed reply does not.
//!
//! **Status.** `ExchangeStarted`/`ExchangeFinished{op: objects.exchange}` on
//! status lane `index + 1`, with the object bytes of each direction and the
//! objects completed: installed here (`objects_down`), or sent in full and
//! listed as available by the peer's reply (`objects_up`).
//!
//! WP3.3 replaces this file with objects v3 (§6.4).
use super::{
    Config,
    meta_exchange::{body_of, invalid, parse},
    scheduler::{
        DriveOptions, ExchangeError, Finished, Job, Next, Report, Signals, Target, check_session,
        drive, refuse, short, sync_interval,
    },
};
use crate::{
    bulk_sync::{self, Page},
    model::{
        orchestrator::{CallError, Ctx, FolderView, PeerContext, PublishedEntry},
        protocol::{LaneRole, LaneSpec, deadlines, objects::REPOS_PER_LANE, ops},
    },
    paired_sync::SyncRoute,
    session::{self, Priority},
    util::{cancel::Cancel, lock::lock},
};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Worker ops of the interim path (served by `bulk_sync::worker_control`).
pub mod worker_ops {
    pub const OFFER: &str = "published-bulk-offer";
    pub const APPLY: &str = "published-bulk-apply";
    /// The acceptor's op (`inbound_objects`, WP2.8).
    pub const EXCHANGE: &str = "published-bulk-exchange";
}

/// Local `published-bulk-offer` and `published-bulk-apply` deadline.
pub const WORKER_DEADLINE: Duration = Duration::from_secs(30);
/// A key whose exchange moved object bytes runs again after this.
pub const CONTENT_YIELD: Duration = Duration::from_millis(10);

/// Options of an interim bulk lane.
#[derive(Clone, Debug)]
pub struct BulkOptions {
    pub drive: DriveOptions,
    /// Worker `published-bulk-offer`.
    pub offer: Duration,
    /// Peer `objects.exchange` (60 s).
    pub exchange: Duration,
    /// Worker `published-bulk-apply`.
    pub apply: Duration,
    /// See [`CONTENT_YIELD`].
    pub content_yield: Duration,
}

impl BulkOptions {
    /// 2 repositories at once, the configured interval, the frozen deadlines.
    pub fn for_config(config: &Config) -> Self {
        Self {
            drive: DriveOptions::new(REPOS_PER_LANE, sync_interval(config)),
            offer: WORKER_DEADLINE,
            exchange: deadlines::OBJECTS_EXCHANGE,
            apply: WORKER_DEADLINE,
            content_yield: CONTENT_YIELD,
        }
    }
}

/// The status lane number of a bulk lane (`index + 1`; lane 0 is control).
pub fn status_lane(lane: LaneSpec) -> u32 {
    lane.index.saturating_add(1)
}

/// The legacy page lane of a [`LaneSpec`].
pub fn page_lane(lane: LaneSpec) -> bulk_sync::Lane {
    bulk_sync::Lane {
        index: lane.index as usize,
        count: lane.count as usize,
    }
}

/// A completed interim exchange.
#[derive(Clone, Debug)]
pub struct BulkOutcome {
    /// Our page's request id.
    pub request: String,
    /// Object bytes sent and received (decoded parts).
    pub sent_bytes: u64,
    pub received_bytes: u64,
    /// Objects we sent in full that the peer now lists as available.
    pub objects_up: u64,
    /// Objects the reply completed here.
    pub objects_down: u64,
    /// The peer's inventory (its reply without parts), for the next offer.
    pub inventory: Page,
    pub duration_ms: u64,
}

impl BulkOutcome {
    /// Object bytes moved in either direction.
    pub fn moved(&self) -> bool {
        self.sent_bytes > 0 || self.received_bytes > 0
    }
}

/// One bulk lane: runs until `cancel` fires or the session closes or goes
/// away.
pub fn run_bulk_lane(
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: session::Conn,
    cancel: Cancel,
) {
    let options = BulkOptions::for_config(&ctx.config);
    run_bulk_lane_with(ctx, peer, lane, conn, cancel, options);
}

/// [`run_bulk_lane`] with explicit options.
pub fn run_bulk_lane_with(
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: session::Conn,
    cancel: Cancel,
    options: BulkOptions,
) {
    let status = status_lane(lane);
    let name = format!("bulk-{}-{}", short(&peer.installation), status);
    let checked = if lane.is_valid() {
        check_session(&peer, &conn, LaneRole::Bulk, Some(lane))
    } else {
        Err(crate::model::errors::ErrorWire::coded(
            "INVALID_BULK_LANE",
            format!(
                "lane {}/{} is not a valid bulk lane",
                lane.index, lane.count
            ),
        ))
    };
    if let Err(error) = checked {
        refuse(&ctx, &peer, &conn, &cancel, status, error, &options.drive);
        return;
    }
    let job = Arc::new(BulkJob {
        ctx: ctx.clone(),
        peer: peer.clone(),
        conn: conn.clone(),
        lane,
        options: options.clone(),
        inventories: Mutex::new(Inventories::default()),
    });
    drive(
        &ctx,
        &peer,
        &conn,
        &cancel,
        job,
        &options.drive,
        status,
        &name,
    );
}

struct BulkJob {
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    conn: session::Conn,
    lane: LaneSpec,
    options: BulkOptions,
    inventories: Mutex<Inventories>,
}

/// The peer's last inventory per repository (the legacy lane's cache): the
/// next offer sends the parts it lacks. Dropped after a failure of the key,
/// and when the key is reset (new worker, new remote replica, readiness
/// regained), removed or created.
#[derive(Default)]
struct Inventories {
    pages: HashMap<String, Page>,
    /// Bumped by every [`Job::forget`] of a repository. An exchange that was
    /// running when its key was reset or removed keeps the epoch it started
    /// with, and does not put its (now stale) inventory back.
    epochs: HashMap<String, u64>,
}

impl Inventories {
    fn epoch(&self, repo: &str) -> u64 {
        self.epochs.get(repo).copied().unwrap_or(0)
    }
}

impl Job for BulkJob {
    fn op(&self) -> &str {
        ops::OBJECTS_EXCHANGE
    }

    fn scope(&self) -> String {
        format!("{}#{}", ops::OBJECTS_EXCHANGE, self.lane.index)
    }

    fn class(&self) -> &'static str {
        "objects"
    }

    fn triggered(&self, before: &Signals, after: &Signals, absorbing: bool) -> bool {
        before.tip != after.tip
            || before.remote_tip != after.remote_tip
            || before.remote_content != after.remote_content
            || (before.content != after.content && !absorbing)
    }

    fn run(
        &self,
        target: &Target,
        retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> Result<Next, ExchangeError> {
        let (epoch, inventory) = {
            let cache = lock(&self.inventories);
            (
                cache.epoch(&target.repo),
                cache.pages.get(&target.repo).cloned(),
            )
        };
        let result = bulk_exchange(
            &self.ctx,
            &self.conn,
            &self.peer,
            self.lane,
            &target.folder,
            &target.remote,
            inventory.as_ref(),
            &self.options,
            retry_after,
        );
        let mut cache = lock(&self.inventories);
        match result {
            Ok(outcome) => {
                let moved = outcome.moved();
                // Not if the key was reset or removed meanwhile: this is the
                // inventory of the old route (or of a key that is gone).
                if cache.epoch(&target.repo) == epoch {
                    cache.pages.insert(target.repo.clone(), outcome.inventory);
                }
                Ok(Next {
                    after: moved.then_some(self.options.content_yield),
                    progress: moved,
                })
            }
            Err(error) => {
                // A failed exchange may leave the cached inventory stale (a
                // replica change, say): the next offer starts without it.
                cache.pages.remove(&target.repo);
                Err(error)
            }
        }
    }

    fn forget(&self, repo: &str) {
        let mut cache = lock(&self.inventories);
        cache.pages.remove(repo);
        let epoch = cache.epochs.entry(repo.to_owned()).or_default();
        *epoch = epoch.wrapping_add(1);
    }
}

/// One interim object exchange of `folder` with `peer`'s `remote` entry on
/// bulk lane `lane`. `inventory` is the peer's inventory from the previous
/// exchange of this repository on this lane, if any. It is used only if it
/// fits this exchange's route and lane ([`fits`]); any other is left out.
#[allow(clippy::too_many_arguments)]
pub fn bulk_exchange(
    ctx: &Ctx,
    conn: &session::Conn,
    peer: &PeerContext,
    lane: LaneSpec,
    folder: &FolderView,
    remote: &PublishedEntry,
    inventory: Option<&Page>,
    options: &BulkOptions,
    retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
) -> Result<BulkOutcome, ExchangeError> {
    let repo = folder.record.repo.clone();
    let report = Report::start(
        ctx,
        &peer.installation,
        &repo,
        status_lane(lane),
        ops::OBJECTS_EXCHANGE,
    );
    let mut finished = Finished::default();
    let result = run(
        ctx,
        conn,
        peer,
        lane,
        folder,
        remote,
        inventory,
        options,
        &mut finished,
    );
    let duration_ms = report.finish(result.as_ref().map(|_| &finished), retry_after);
    let mut outcome = result?;
    outcome.duration_ms = duration_ms;
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn run(
    ctx: &Ctx,
    conn: &session::Conn,
    peer: &PeerContext,
    lane: LaneSpec,
    folder: &FolderView,
    remote: &PublishedEntry,
    inventory: Option<&Page>,
    options: &BulkOptions,
    finished: &mut Finished,
) -> Result<BulkOutcome, ExchangeError> {
    let local = ExchangeError::local;
    let route = SyncRoute {
        peer: peer.installation.clone(),
        repo: folder.record.repo.clone(),
        local_replica: folder.record.device.clone(),
        remote_replica: remote.replica.clone(),
    };
    if remote.repo != route.repo {
        return Err(local(invalid(
            "REPOSITORY_MISMATCH",
            format!("the peer entry is for {}, not {}", remote.repo, route.repo),
        )));
    }
    let page_lane = page_lane(lane);
    // The worker refuses an inventory of another route (`BULK_ROUTE_MISMATCH`,
    // Fatal): a stale one is dropped here rather than park the key.
    let inventory = inventory.filter(|page| fits(page, &route, page_lane));
    let client = ctx.folders.client(&folder.record.state).ok_or_else(|| {
        local(CallError::Unavailable(format!(
            "LOCAL_WORKER_UNAVAILABLE: folder {} has no running worker",
            folder.record.state
        )))
    })?;

    // 1. The local offer (with the peer's last inventory: the parts it lacks).
    let page = client
        .call(
            worker_ops::OFFER,
            json!({"route": route, "lane": page_lane, "inventory": inventory}),
            options.offer,
        )
        .map_err(local)?;
    let page: Page = parse(page, worker_ops::OFFER).map_err(local)?;
    if page.lane != page_lane || page.repo != route.repo {
        return Err(local(invalid(
            "INCOMPATIBLE_BULK_LANE",
            format!(
                "published-bulk-offer returned a page of lane {}/{} for {}",
                page.lane.index, page.lane.count, page.repo
            ),
        )));
    }

    // 2. The peer.
    let body =
        serde_json::to_vec(&page).map_err(|error| local(invalid("INVALID_SYNC_REQUEST", error)))?;
    let header = json!({"repo": route.repo, "lane": lane});
    // The reply is a plain body (≤ the op's body limit), or streamed as
    // CHUNK frames before the RESPONSE (any size up to `MAX_REPLY`).
    let mut streamed = Vec::new();
    let reply = conn
        .call_streaming(
            ops::OBJECTS_EXCHANGE,
            header,
            body,
            Priority::Data,
            Some(options.exchange),
            options.exchange,
            &mut |chunk, bytes| collect(&mut streamed, &chunk, bytes),
        )
        .map_err(ExchangeError::peer)?;
    let reply = if streamed.is_empty() {
        reply
    } else {
        session::Reply {
            header: reply.header,
            body: streamed,
        }
    };
    let reply: Page = body_of(&reply, ops::OBJECTS_EXCHANGE).map_err(ExchangeError::peer)?;
    if reply.reply_to.as_deref() != Some(page.request.as_str()) {
        return Err(ExchangeError::peer(invalid(
            "UNBOUND_PEER_RESPONSE",
            format!(
                "the objects.exchange reply answers {:?}, not {}",
                reply.reply_to, page.request
            ),
        )));
    }
    let sent_bytes = page.object_bytes() as u64;
    let received_bytes = reply.object_bytes() as u64;
    let objects_up = completed(&page)
        .filter(|hash| reply.known_objects.contains(*hash))
        .count() as u64;
    let objects_down = completed(&reply).count() as u64;

    // 3. The local apply (the worker checks `reply_to` against the request).
    let applied = client
        .call(
            worker_ops::APPLY,
            json!({"route": route, "lane": page_lane, "page": reply, "request": page.request}),
            options.apply,
        )
        .map_err(local)?;
    let _: serde_json::Value = parse(applied, worker_ops::APPLY).map_err(local)?;
    *finished = Finished {
        summary: None,
        bytes_up: sent_bytes,
        bytes_down: received_bytes,
        objects_up,
        objects_down,
    };
    Ok(BulkOutcome {
        request: page.request,
        sent_bytes,
        received_bytes,
        objects_up,
        objects_down,
        inventory: reply.inventory(),
        duration_ms: 0,
    })
}

/// Largest streamed `objects.exchange` reply (the legacy wire's message bound).
pub const MAX_REPLY: usize = crate::ws::MAX_MESSAGE;

/// Appends one streamed reply chunk: in order, and within [`MAX_REPLY`].
fn collect(
    body: &mut Vec<u8>,
    chunk: &crate::model::protocol::frame::ChunkHeader,
    bytes: &[u8],
) -> Result<(), crate::model::errors::ErrorWire> {
    use crate::model::errors::ErrorWire;
    if chunk.hash.is_some() || chunk.offset != body.len() as u64 {
        return Err(ErrorWire::coded(
            "PROTOCOL_VIOLATION",
            format!(
                "objects.exchange reply chunk at {} (hash {:?}) after {} bytes",
                chunk.offset,
                chunk.hash,
                body.len()
            ),
        ));
    }
    if body.len() + bytes.len() > MAX_REPLY {
        return Err(ErrorWire::coded(
            "WS_MESSAGE_TOO_LARGE",
            format!("the objects.exchange reply exceeds {MAX_REPLY} bytes"),
        ));
    }
    body.extend_from_slice(bytes);
    Ok(())
}

/// Whether `page`, a page of the peer, is one the worker accepts for `route`
/// and `lane` (the route half of `bulk_sync`'s page validation): our bulk
/// protocol and lane, our repository, from the remote replica to us.
pub fn fits(page: &Page, route: &SyncRoute, lane: bulk_sync::Lane) -> bool {
    page.format == bulk_sync::PROTOCOL
        && page.lane == lane
        && page.repo == route.repo
        && page.sender == route.remote_replica
        && page.receiver == route.local_replica
}

/// Objects whose part in `page` ends at their total size (the part that
/// completes them).
fn completed(page: &Page) -> impl Iterator<Item = &String> {
    page.object_parts.iter().filter_map(|(hash, part)| {
        let len = (part.hex.len() / 2) as u64;
        (part.offset.checked_add(len) == Some(part.total)).then_some(hash)
    })
}
