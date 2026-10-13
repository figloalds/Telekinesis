//! Durable inbox of received events and their causal activation (REVAMP_PLAN
//! §4.4): staging, the row state machine, bounded activation slices, link
//! validation and the startup drain.
//!
//! Every `incoming_events` row is in one state:
//!
//! | `state` | meaning | legacy `error` |
//! |---|---|---|
//! | `ready` | every parent is committed; can be tried now | NULL |
//! | `waiting` | parents missing or staged; recorded in `incoming_waits` | NULL |
//! | `retry` | transient failure (`SQLITE_BUSY`/`LOCKED`/IO); due at `retry_at_ms` | NULL |
//! | `quarantined` | validation failure; `code` | the failure text |
//! | `blocked` | a quarantined ancestor (`blocked_on`) | `QUARANTINED_CAUSAL_ANCESTOR: <id>` |
//! | NULL | written by an older binary (or startup evacuation); classified lazily | as written |
//!
//! `blocked_on` always names the root: a `quarantined` row, or a legacy row
//! that is still to be classified as one. An older binary named the direct
//! parent in its `QUARANTINED_CAUSAL_ANCESTOR` text; classification follows
//! that chain to the root. Whenever a row leaves the rejected states (it
//! becomes `ready`, `waiting` or `retry`, for example a legacy transient
//! quarantine that is retried, or it commits), every row blocked on it
//! becomes `waiting` on it, so it activates with it. Activation re-checks
//! every link, so a row with another quarantined ancestor is blocked again.
//!
//! The legacy `error` column is non-NULL exactly for `quarantined` and
//! `blocked`, so the legacy `incoming_quarantined` count and an older
//! binary's drain see what they always saw. Staging never activates;
//! activation runs in slices that pop `ready` and due `retry` rows in
//! `(received_ms, rowid)` order with point lookups only, commit each event in
//! its own savepoint inside the slice transaction, and release the children
//! of every committed event from `waiting` to `ready` within the same slice.
use super::commit::Check;
use super::*;
use crate::model::errors::{ErrorClass, ErrorWire, code_in, code_of, explicit_class};
use crate::model::health::InboxCounts;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::{Duration, Instant},
};

/// Inbox cache (owned by WP1.6 / WP2.4; nothing cached in Wave 1).
#[derive(Default)]
pub(crate) struct Cache {}

/// Rows loaded per slice, both for classification and for the ready window.
const WINDOW: usize = 512;
/// Retry backoff: `min(RETRY_CAP_MS, 2^attempts s)`, i.e. 1 s → 5 min.
const RETRY_CAP_MS: u64 = 300_000;
/// `schema:inbox` stamp.
const SCHEMA_VERSION: i64 = 1;
const READY: &str = "ready";
const WAITING: &str = "waiting";
const RETRY: &str = "retry";
const QUARANTINED: &str = "quarantined";
const BLOCKED: &str = "blocked";
const ANCESTOR: &str = "QUARANTINED_CAUSAL_ANCESTOR";

/// One event to stage, with the peer feed position it came from (if any).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedEvent {
    pub event: Event,
    /// `(peer replica, seq)`; recorded in `incoming_sources`.
    pub source: Option<(String, i64)>,
}

/// What `stage_incoming` did with each event.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageOutcome {
    /// Newly staged rows (in any state: ready, waiting, blocked or quarantined).
    pub staged: Vec<String>,
    /// The identical event is already in the journal.
    pub already_committed: Vec<String>,
    /// The identical event is already staged.
    pub already_staged: Vec<String>,
    /// Refused; nothing was written for these.
    pub rejected: Vec<(String, ErrorWire)>,
}

/// Bounds of one activation slice. The time budget starts after the window
/// load, and a slice always makes at least one attempt when a row is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceBudget {
    pub max_events: usize,
    pub max_ms: u64,
    /// Commit at most one slow-path event inline per slice; otherwise stop at
    /// the first slow event and return its `SlowTicket`.
    pub inline_slow: bool,
}

impl SliceBudget {
    /// The background activator's slice (§4.4: 64 events or 25 ms; slow
    /// events are prepared off-lock).
    pub const ACTIVATOR: Self = Self {
        max_events: 64,
        max_ms: 25,
        inline_slow: false,
    };
    /// No event or time bound (one inline slow event per slice still).
    pub const UNBOUNDED: Self = Self {
        max_events: usize::MAX,
        max_ms: u64::MAX,
        inline_slow: true,
    };
}

/// What one slice did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceOutcome {
    /// Committed by this slice (or found already committed), in order.
    pub activated: Vec<String>,
    /// Commits through an incremental successor.
    pub fast: u32,
    /// Commits through full replay.
    pub slow: u32,
    pub quarantined: Vec<(String, ErrorWire)>,
    /// Set when `inline_slow` is false and the next event needs the slow
    /// path; its row stays `ready`.
    pub slow_ticket: Option<SlowTicket>,
    /// More rows may be ready: the slice stopped on its budget, on a slow
    /// event, or on a full window.
    pub more: bool,
}

/// A ready event that needs the slow path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlowTicket {
    pub id: String,
    pub branch: String,
    /// The branch's journal sequence when the ticket was issued.
    pub base_seq: i64,
}

/// A slow commit, prepared by `Store::prepare_slow`.
#[derive(Debug)]
pub enum Prepared {
    /// Nothing precomputed: `commit_prepared` runs the slow path under the
    /// caller's lock (Wave 1).
    Inline(SlowTicket),
    /// Precomputed off-lock (WP2.4).
    Computed(Box<PreparedCommit>),
}

/// The opaque result of an off-lock slow-path preparation (WP2.4 fills it).
#[derive(Debug)]
pub struct PreparedCommit {
    ticket: SlowTicket,
}

impl PreparedCommit {
    pub fn ticket(&self) -> &SlowTicket {
        &self.ticket
    }
}

/// What `commit_prepared` did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOutcome {
    Committed {
        id: String,
        slow: bool,
    },
    /// The ticket no longer applies (row gone, state changed or parents not
    /// committed); re-queue with backoff, never commit inline.
    Stale,
    /// The event was quarantined (validation) or moved to `retry` (transient).
    Failed(ErrorWire),
}

/// Light view of a payload: what direct-link validation needs.
#[derive(Deserialize)]
struct Links {
    branch: Branch,
    changes: Vec<LinkChange>,
}
#[derive(Deserialize)]
struct LinkChange {
    entity: String,
    field: String,
}

/// `payload, state, error, blocked_on` of a staged row.
type StagedRow = (String, Option<String>, Option<String>, Option<String>);

/// Where a referenced revision is.
enum Parent {
    Committed(Links),
    /// Staged and usable (any state but quarantined/blocked).
    Staged(Links),
    /// Staged, quarantined or blocked; the root quarantined event.
    Quarantined(String),
    Missing,
}

/// Classification of an event's causal links.
enum Class {
    Ready,
    Waiting(BTreeSet<String>),
    Blocked(String),
    Quarantined(anyhow::Error),
}

/// One activation attempt of one row.
enum Attempt {
    /// Committed now (`fast` path or not), or found already committed.
    Activated {
        fast: Option<bool>,
        released: Vec<String>,
    },
    Quarantined(ErrorWire),
    Retried(ErrorWire),
    /// Moved to `waiting` or `blocked`; not an attempt.
    Deferred,
    /// Not a ready row any more.
    Gone,
    Ticket(SlowTicket),
    /// A second slow event in a slice that already committed one inline.
    SlowSpent,
}

#[derive(Default)]
struct SliceStats {
    attempts: usize,
    elapsed: Duration,
}

/// `inbox::ensure_schema` (`schema:inbox=1`): the state columns (named-column
/// writers only, REVAMP_PLAN §9), `incoming_waits`, `incoming_sources`, their
/// indexes (plus a partial index on `blocked_on`, so releasing the rows
/// blocked on a committed event is a point lookup) and the cleanup trigger.
/// Idempotent; runs in its own savepoint.
pub(super) fn ensure_schema(store: &mut Store) -> Result<()> {
    let db = &store.db;
    db.execute_batch("SAVEPOINT inbox_schema")?;
    let result = (|| -> Result<()> {
        let columns: BTreeSet<String> = db
            .prepare("SELECT name FROM pragma_table_info('incoming_events')")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for column in [
            "state TEXT",
            "received_ms INTEGER",
            "attempts INTEGER",
            "retry_at_ms INTEGER",
            "code TEXT",
            "error_class TEXT",
            "blocked_on TEXT",
        ] {
            let name = column.split(' ').next().unwrap_or(column);
            if !columns.contains(name) {
                db.execute_batch(&format!("ALTER TABLE incoming_events ADD COLUMN {column}"))?;
            }
        }
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS incoming_waits(child TEXT NOT NULL,parent TEXT NOT NULL,PRIMARY KEY(child,parent));
            CREATE INDEX IF NOT EXISTS incoming_waits_parent ON incoming_waits(parent);
            CREATE TABLE IF NOT EXISTS incoming_sources(event TEXT NOT NULL,peer TEXT NOT NULL,seq INTEGER NOT NULL,PRIMARY KEY(event,peer));
            CREATE INDEX IF NOT EXISTS incoming_events_state ON incoming_events(state,received_ms);
            CREATE INDEX IF NOT EXISTS incoming_events_blocked_on ON incoming_events(blocked_on) WHERE blocked_on IS NOT NULL;
            CREATE TRIGGER IF NOT EXISTS incoming_events_cleanup AFTER DELETE ON incoming_events BEGIN
                DELETE FROM incoming_waits WHERE child=OLD.id;
                DELETE FROM incoming_sources WHERE event=OLD.id;
            END;",
        )?;
        db.execute(
            "INSERT INTO config(key,value) VALUES('schema:inbox',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE CAST(config.value AS INTEGER)<?1",
            [SCHEMA_VERSION],
        )?;
        Ok(())
    })();
    match result {
        Ok(()) => db.execute_batch("RELEASE inbox_schema")?,
        Err(error) => {
            let _ = db.execute_batch("ROLLBACK TO inbox_schema; RELEASE inbox_schema");
            return Err(error);
        }
    }
    Ok(())
}

/// Phase 8 of `Store::open`: recover staged incoming events. A restart
/// retries every `retry` row and re-derives waits; NULL rows are classified;
/// with `defer=false` every ready row is activated (progress per slice).
pub(super) fn startup_drain(store: &mut Store, defer_activation: bool) -> Result<()> {
    crate::startup::progress("recovering incoming events");
    store.with_write("inbox_startup", |store| store.reconcile_rows())?;
    while store.with_write("inbox_classify", |store| {
        store.classify_unclassified(WINDOW)
    })? >= WINDOW
    {
        crate::startup::progress("classifying incoming events");
    }
    if !defer_activation {
        let mut incremental = vec![];
        loop {
            crate::startup::progress("recovering incoming events");
            let slice = store.activate_slice(SliceBudget::UNBOUNDED)?;
            if slice.fast > 0 {
                incremental.extend(slice.activated);
            }
            if !slice.more {
                break;
            }
        }
        // Fast-path activations seal the startup snapshot only every 128
        // events; seal once after the drain so the next start resumes at the
        // tip, as it did when every remote namespace event took the slow path.
        let mut branches = BTreeSet::new();
        for id in &incremental {
            let branch: Option<String> = store
                .db
                .query_row("SELECT branch FROM events WHERE id=?", [id], |r| r.get(0))
                .optional()?;
            branches.extend(branch);
        }
        for branch in branches {
            crate::startup::progress("sealing startup snapshot");
            store.with_write("inbox_seal", |store| snapshot::persist(&store.db, &branch))?;
        }
    }
    Ok(())
}

impl Store {
    pub fn receive(
        &mut self,
        bundle: Bundle,
        allowed_devices: &BTreeSet<String>,
    ) -> Result<Vec<String>> {
        self.receive_with_budget(bundle, allowed_devices, None)
    }
    /// Legacy receive: the page checks of the v6 protocol, staging, then
    /// activation slices until `budget` attempts (`Some(n)`: at most `n`
    /// attempts and 50 ms of activation work) or until no row is ready.
    /// Returns the bundle's already committed ids and every id activated.
    pub(crate) fn receive_with_budget(
        &mut self,
        bundle: Bundle,
        allowed_devices: &BTreeSet<String>,
        budget: Option<usize>,
    ) -> Result<Vec<String>> {
        ensure!(bundle.repo == self.repo, "REPOSITORY_MISMATCH");
        for b in &bundle.branches {
            ensure!(b.shared, "PRIVATE_BRANCH_REJECTED");
        }
        for e in &bundle.events {
            ensure!(
                e.branch.shared && allowed_devices.contains(&e.device),
                "UNAUTHORIZED_EVENT"
            );
        }
        // Object bookkeeping and staging write no projection state: a view
        // that is fresh now stays fresh (no O(projection) reload per page).
        let view = self.keep_view();
        let authorized: BTreeSet<_> = bundle.events.iter().flat_map(Event::objects).collect();
        for (h, data) in bundle.objects {
            ensure!(authorized.contains(&h), "UNAUTHORIZED_OBJECT");
            let bytes = hex::decode(data)?;
            ensure!(hash(&bytes) == h, "CORRUPT_OBJECT");
            self.put_object(&bytes)?;
            // Only actual verified inbound bytes grant reuse authorization. A
            // manifest/hash alone cannot relabel private-only local CAS content.
            self.db.execute(
                "INSERT OR IGNORE INTO received_shared_objects VALUES(?)",
                [h],
            )?;
        }
        // A refused event refuses the whole page, as before: nothing is staged.
        let staged = self.stage_events(
            bundle
                .events
                .into_iter()
                .map(|event| StagedEvent {
                    event,
                    source: None,
                })
                .collect(),
            allowed_devices,
            true,
        )?;
        self.rekey_view(view);
        let mut accepted = staged.already_committed;
        match budget {
            None => accepted.extend(self.drain_incoming()?),
            Some(maximum) => {
                let mut attempts = 0;
                let mut spent = Duration::ZERO;
                loop {
                    let limit = Duration::from_millis(50).saturating_sub(spent);
                    let (slice, stats) = self.slice_in_transaction(SliceBudget {
                        max_events: maximum.saturating_sub(attempts),
                        max_ms: limit.as_millis() as u64,
                        inline_slow: true,
                    })?;
                    accepted.extend(slice.activated);
                    attempts += stats.attempts;
                    spent += stats.elapsed;
                    if !slice.more
                        || attempts >= maximum
                        || stats.attempts == 0
                        || spent >= Duration::from_millis(50)
                    {
                        break;
                    }
                }
            }
        }
        for b in bundle.branches {
            if let Ok(existing) = self.branch(&b.id) {
                ensure!(existing == b, "BRANCH_DESCRIPTOR_CHANGED");
            } else {
                // A branch with staged events (in any state) is registered by
                // its first activated event, never as an empty branch.
                let staged: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM incoming_events WHERE json_extract(payload,'$.branch.id')=?)",
                    [&b.id],
                    |r| r.get(0),
                )?;
                if staged {
                    continue;
                }
                self.db
                    .execute("INSERT INTO branches VALUES(?,?,1)", params![b.id, b.name])?;
                self.rebuild(&b.id)?;
            }
        }
        Ok(accepted)
    }
    /// Activate until no row is ready (one inline slow event per slice).
    pub(crate) fn drain_incoming(&mut self) -> Result<Vec<String>> {
        let mut accepted = vec![];
        loop {
            let slice = self.activate_slice(SliceBudget::UNBOUNDED)?;
            accepted.extend(slice.activated);
            if !slice.more {
                return Ok(accepted);
            }
        }
    }

    /// Stage events on any connection; staging never activates. Per event:
    /// a foreign repository, a private branch, an unauthorized device, a
    /// malformed id or an `EVENT_ID_REUSED` (against committed or staged
    /// payloads) is `rejected`; the rest become `ready`, `waiting` (with
    /// their missing or staged parents in `incoming_waits`), `blocked` (a
    /// quarantined ancestor) or `quarantined` (an invalid direct link or a
    /// causal cycle). Sources go to `incoming_sources`. One transaction.
    pub fn stage_incoming(
        &self,
        events: Vec<StagedEvent>,
        allowed: &BTreeSet<String>,
    ) -> Result<StageOutcome> {
        self.stage_events(events, allowed, false)
    }
    /// `strict`: the first rejection fails the call and stages nothing.
    fn stage_events(
        &self,
        events: Vec<StagedEvent>,
        allowed: &BTreeSet<String>,
        strict: bool,
    ) -> Result<StageOutcome> {
        let view = self.keep_view();
        let outer = begin_write(&self.db, "inbox_stage")?;
        let result = self.stage_inner(events, allowed, strict);
        let result = finish_write(&self.db, "inbox_stage", outer, result);
        if result.is_ok() {
            self.rekey_view(view);
        }
        result
    }
    fn stage_inner(
        &self,
        events: Vec<StagedEvent>,
        allowed: &BTreeSet<String>,
        strict: bool,
    ) -> Result<StageOutcome> {
        let mut out = StageOutcome::default();
        let now = time() as i64;
        for StagedEvent { event, source } in events {
            let payload = serde_json::to_string(&event)?;
            // Strict (legacy receive) callers checked the page already; there a
            // foreign repository or a bad id is staged and quarantined at
            // activation, exactly as before.
            let refusal = if strict {
                None
            } else if event.repo != self.repo {
                Some(ErrorWire::coded("REPOSITORY_MISMATCH", &event.id))
            } else if !event.branch.shared {
                Some(ErrorWire::coded("PRIVATE_EVENT_REJECTED", &event.id))
            } else if !allowed.contains(&event.device) {
                Some(ErrorWire::coded("UNAUTHORIZED_EVENT", &event.id))
            } else if event.id.is_empty() || event.id.len() > 128 {
                Some(ErrorWire::coded("INVALID_EVENT", "event id length"))
            } else {
                None
            };
            let committed: Option<String> = if refusal.is_none() {
                self.db
                    .prepare_cached("SELECT payload FROM events WHERE id=?")?
                    .query_row([&event.id], |r| r.get(0))
                    .optional()?
            } else {
                None
            };
            let staged: Option<String> = if refusal.is_none() && committed.is_none() {
                self.db
                    .prepare_cached("SELECT payload FROM incoming_events WHERE id=?")?
                    .query_row([&event.id], |r| r.get(0))
                    .optional()?
            } else {
                None
            };
            let reused = committed
                .as_ref()
                .map(|old| serde_json::from_str::<Event>(old).map(|old| old != event))
                .transpose()?
                .unwrap_or(false)
                || staged.as_ref().is_some_and(|old| old != &payload);
            let refusal = refusal
                .or_else(|| reused.then(|| ErrorWire::new("EVENT_ID_REUSED", "EVENT_ID_REUSED")));
            if let Some(error) = refusal {
                if strict {
                    return Err(error.into_anyhow());
                }
                out.rejected.push((event.id.clone(), error));
                continue;
            }
            if committed.is_some() {
                out.already_committed.push(event.id);
                continue;
            }
            if staged.is_none() {
                self.db.execute(
                    "INSERT INTO incoming_events(id,payload,state,received_ms,attempts) VALUES(?,?,?,?,0)",
                    params![event.id, payload, WAITING, now],
                )?;
                let class = self.classify_links(&event)?;
                self.apply_class(&event.id, class)?;
            }
            if let Some((peer, seq)) = &source {
                self.db.execute(
                    "INSERT INTO incoming_sources(event,peer,seq) VALUES(?,?,?) ON CONFLICT(event,peer) DO UPDATE SET seq=excluded.seq",
                    params![event.id, peer, seq],
                )?;
            }
            if staged.is_some() {
                out.already_staged.push(event.id);
            } else {
                out.staged.push(event.id);
            }
        }
        Ok(out)
    }

    /// Activate up to `b.max_events` ready (and due retry) events in one
    /// write transaction (`BEGIN IMMEDIATE` from autocommit, else a savepoint
    /// inside the caller's transaction). Validation failures quarantine the
    /// event and block its staged descendants; `SQLITE_BUSY`/`LOCKED`/IO
    /// failures move it to `retry`; nothing is ever quarantined for a
    /// transient error.
    pub fn activate_slice(&mut self, b: SliceBudget) -> Result<SliceOutcome> {
        Ok(self.slice_in_transaction(b)?.0)
    }
    fn slice_in_transaction(&mut self, b: SliceBudget) -> Result<(SliceOutcome, SliceStats)> {
        self.with_write("inbox_slice", |store| store.slice(b))
    }
    fn slice(&mut self, b: SliceBudget) -> Result<(SliceOutcome, SliceStats)> {
        let view = self.keep_view();
        let classified = self.classify_unclassified(WINDOW)?;
        self.rekey_view(view);
        let now = time() as i64;
        // The window: keys only (payloads are point lookups when popped).
        let key = |r: &rusqlite::Row<'_>| -> rusqlite::Result<(i64, i64, String)> {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        };
        let mut window: Vec<(i64, i64, String)> = self
            .db
            .prepare_cached(
                "SELECT coalesce(received_ms,0),rowid,id FROM incoming_events WHERE state='ready' ORDER BY received_ms,rowid LIMIT ?",
            )?
            .query_map([WINDOW as i64], key)?
            .collect::<rusqlite::Result<_>>()?;
        window.extend(
            self.db
                .prepare_cached(
                    "SELECT coalesce(received_ms,0),rowid,id FROM incoming_events WHERE state='retry' AND coalesce(retry_at_ms,0)<=? ORDER BY received_ms,rowid LIMIT ?",
                )?
                .query_map(params![now, WINDOW as i64], key)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        );
        // Keep the oldest WINDOW rows of both queries.
        window.sort();
        window.truncate(WINDOW);
        let mut queue: BinaryHeap<Reverse<(i64, i64, String)>> =
            window.into_iter().map(Reverse).collect();
        let full = queue.len() >= WINDOW || classified >= WINDOW;
        // Fault point `core.inbox.window` (hang): a slow window load, which
        // the budget below must not count.
        crate::util::fault::hang("core.inbox.window");
        // The budget is measured after the window load (fixes core.rs:2352).
        let started = Instant::now();
        let mut out = SliceOutcome::default();
        let mut stats = SliceStats::default();
        let mut slow_used = false;
        let mut stopped = false;
        while let Some(Reverse((_, _, id))) = queue.pop() {
            if stats.attempts >= b.max_events
                || (stats.attempts > 0 && started.elapsed() >= Duration::from_millis(b.max_ms))
            {
                stopped = true;
                break;
            }
            crate::startup::progress("recovering incoming events");
            match self.attempt(&id, b.inline_slow, slow_used)? {
                Attempt::Activated { fast, released } => {
                    stats.attempts += 1;
                    match fast {
                        Some(true) => out.fast += 1,
                        Some(false) => {
                            out.slow += 1;
                            slow_used = true;
                        }
                        None => {}
                    }
                    out.activated.push(id);
                    for child in released {
                        let key: Option<(i64, i64)> = self
                            .db
                            .prepare_cached(
                                "SELECT coalesce(received_ms,0),rowid FROM incoming_events WHERE id=? AND state='ready'",
                            )?
                            .query_row([&child], |r| Ok((r.get(0)?, r.get(1)?)))
                            .optional()?;
                        if let Some((received, rowid)) = key {
                            queue.push(Reverse((received, rowid, child)));
                        }
                    }
                }
                Attempt::Quarantined(error) => {
                    stats.attempts += 1;
                    out.quarantined.push((id, error));
                }
                Attempt::Retried(_) => stats.attempts += 1,
                Attempt::Deferred | Attempt::Gone => {}
                Attempt::Ticket(ticket) => {
                    out.slow_ticket = Some(ticket);
                    stopped = true;
                    break;
                }
                Attempt::SlowSpent => {
                    stopped = true;
                    break;
                }
            }
        }
        stats.elapsed = started.elapsed();
        out.more = stopped || full;
        Ok((out, stats))
    }
    /// One activation attempt of row `id`.
    fn attempt(&mut self, id: &str, inline_slow: bool, slow_used: bool) -> Result<Attempt> {
        let row: Option<(String, Option<String>)> = self
            .db
            .prepare_cached("SELECT payload,state FROM incoming_events WHERE id=?")?
            .query_row([id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        let Some((payload, state)) = row else {
            return Ok(Attempt::Gone);
        };
        if !matches!(state.as_deref(), Some(READY) | Some(RETRY)) {
            return Ok(Attempt::Gone);
        }
        let event: Event = match serde_json::from_str(&payload) {
            Ok(event) => event,
            Err(error) => {
                let error = anyhow::Error::new(error).context("INVALID_EVENT");
                return self.fail(id, error);
            }
        };
        if event.id != id {
            return self.fail(
                id,
                anyhow::anyhow!("INVALID_EVENT: staged payload id mismatch"),
            );
        }
        // Point-query the parents; the window only holds rows that were ready.
        match self.classify_links(&event)? {
            Class::Ready => {}
            class => {
                if let Class::Quarantined(error) = class {
                    return self.fail(id, error);
                }
                self.apply_class(id, class)?;
                return Ok(Attempt::Deferred);
            }
        }
        let checked = match self.check_commit(&event, false) {
            Ok(Check::Committed) => {
                let released = self.finish_row(id)?;
                return Ok(Attempt::Activated {
                    fast: None,
                    released,
                });
            }
            Ok(Check::Ready(checked)) => checked,
            Err(error) => return self.fail(id, error),
        };
        let fast = match self.fast_successor(&event, false) {
            Ok(fast) => fast,
            Err(error) => return self.fail(id, error),
        };
        if fast.is_none() {
            if !inline_slow {
                return Ok(Attempt::Ticket(SlowTicket {
                    id: id.to_owned(),
                    branch: event.branch.id.clone(),
                    base_seq: self.branch_seq(&event.branch.id)?,
                }));
            }
            if slow_used {
                return Ok(Attempt::SlowSpent);
            }
        }
        let incremental = fast.is_some();
        // Row bookkeeping first, the commit last: the commit's view patch then
        // stays valid for the next event (no write follows it).
        self.db.execute_batch("SAVEPOINT inbox_event")?;
        let released = match self.finish_row(id) {
            Ok(released) => released,
            Err(error) => {
                self.abort_event()?;
                return Err(error);
            }
        };
        match self.commit_checked(event, false, checked, fast) {
            Ok(()) => {
                self.db.execute_batch("RELEASE inbox_event")?;
                Ok(Attempt::Activated {
                    fast: Some(incremental),
                    released,
                })
            }
            Err(error) => {
                // If SQLite already rolled the whole transaction back (an I/O
                // or full-disk error), the savepoint is gone: fail the slice.
                if let Err(rollback) = self.abort_event() {
                    return Err(error.context(format!("{rollback:#}")));
                }
                if error.downcast_ref::<CausalPrerequisitesPending>().is_some() {
                    return self.defer_pending(id);
                }
                self.fail(id, error)
            }
        }
    }
    /// Roll back the per-event savepoint and drop the (possibly patched) view.
    fn abort_event(&mut self) -> Result<()> {
        self.view_cache.borrow_mut().take();
        self.db
            .execute_batch("ROLLBACK TO inbox_event; RELEASE inbox_event")?;
        Ok(())
    }
    /// Delete a committed row and release its children: every waiting row
    /// with no other wait becomes `ready`. Returns the released ids.
    fn finish_row(&mut self, id: &str) -> Result<Vec<String>> {
        self.db
            .prepare_cached("DELETE FROM incoming_events WHERE id=?")?
            .execute([id])?;
        // Rows still blocked on a committing event (written by an older
        // build) wait on it instead and are released below with the rest.
        self.unblock(id)?;
        let children: Vec<String> = self
            .db
            .prepare_cached("SELECT child FROM incoming_waits WHERE parent=?")?
            .query_map([id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        if children.is_empty() {
            return Ok(children);
        }
        self.db
            .prepare_cached("DELETE FROM incoming_waits WHERE parent=?")?
            .execute([id])?;
        let mut released = vec![];
        for child in children {
            let promoted = self
                .db
                .prepare_cached(
                    "UPDATE incoming_events SET state='ready' WHERE id=?1 AND state='waiting' AND NOT EXISTS(SELECT 1 FROM incoming_waits WHERE child=?1)",
                )?
                .execute([&child])?;
            if promoted > 0 {
                released.push(child);
            }
        }
        Ok(released)
    }
    /// The commit found parents outside this branch's journal: wait for them.
    fn defer_pending(&mut self, id: &str) -> Result<Attempt> {
        let payload: String = self.db.query_row(
            "SELECT payload FROM incoming_events WHERE id=?",
            [id],
            |r| r.get(0),
        )?;
        let event: Event = serde_json::from_str(&payload)?;
        let mut waits = BTreeSet::new();
        for parent in references(&event) {
            let branch: Option<String> = self
                .db
                .query_row("SELECT branch FROM events WHERE id=?", [parent], |r| {
                    r.get(0)
                })
                .optional()?;
            if branch.as_deref() != Some(&event.branch.id) {
                waits.insert(parent.clone());
            }
        }
        if waits.is_empty() {
            // Nothing identifiable to wait for: retry with backoff rather
            // than spin or quarantine.
            let error = anyhow::Error::new(CausalPrerequisitesPending);
            return self.retry(id, &error);
        }
        self.apply_class(id, Class::Waiting(waits))?;
        Ok(Attempt::Deferred)
    }
    /// Classify a failure of row `id`: transient → `retry`, else quarantine.
    fn fail(&mut self, id: &str, error: anyhow::Error) -> Result<Attempt> {
        if is_transient(&error) {
            return self.retry(id, &error);
        }
        let wire = quarantine_wire(&error);
        self.quarantine(id, &wire)?;
        Ok(Attempt::Quarantined(wire))
    }
    fn retry(&mut self, id: &str, error: &anyhow::Error) -> Result<Attempt> {
        let attempts: i64 = self
            .db
            .query_row(
                "SELECT coalesce(attempts,0) FROM incoming_events WHERE id=?",
                [id],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        let delay = retry_delay_ms(attempts.max(0) as u64);
        let mut wire = ErrorWire::from_error(error);
        wire.class = ErrorClass::Transient;
        wire.retry_after_ms = Some(delay);
        self.db.execute(
            "UPDATE incoming_events SET state='retry',attempts=coalesce(attempts,0)+1,retry_at_ms=?,code=?,error_class='transient',error=NULL,blocked_on=NULL WHERE id=?",
            params![time() as i64 + delay as i64, wire.code, id],
        )?;
        self.unblock(id)?;
        Ok(Attempt::Retried(wire))
    }
    /// Quarantine row `id` and block every staged descendant on it.
    fn quarantine(&self, id: &str, wire: &ErrorWire) -> Result<()> {
        self.db.execute(
            "UPDATE incoming_events SET state='quarantined',error=?,code=?,error_class=?,blocked_on=NULL,retry_at_ms=NULL WHERE id=?",
            params![wire.message, wire.code, wire.class.as_str(), id],
        )?;
        self.db
            .execute("DELETE FROM incoming_waits WHERE child=?", [id])?;
        self.block_descendants(id, id)
    }
    /// Mark every staged row that (transitively) waits on `from` as blocked
    /// on `root`. Rows already quarantined or blocked keep their reason.
    fn block_descendants(&self, from: &str, root: &str) -> Result<()> {
        let mut todo = vec![from.to_owned()];
        let mut seen = BTreeSet::new();
        while let Some(parent) = todo.pop() {
            if !seen.insert(parent.clone()) {
                continue;
            }
            let children: Vec<String> = self
                .db
                .prepare_cached(
                    "SELECT w.child FROM incoming_waits w JOIN incoming_events e ON e.id=w.child WHERE w.parent=? AND (e.state IN ('ready','waiting','retry') OR (e.state IS NULL AND e.error IS NULL))",
                )?
                .query_map([&parent], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            for child in children {
                self.set_blocked(&child, root)?;
                todo.push(child);
            }
        }
        Ok(())
    }
    fn set_blocked(&self, id: &str, root: &str) -> Result<()> {
        self.db.execute(
            "UPDATE incoming_events SET state='blocked',error=?,code=?,error_class='fatal',blocked_on=?,retry_at_ms=NULL WHERE id=?",
            params![format!("{ANCESTOR}: {root}"), ANCESTOR, root, id],
        )?;
        self.repoint_blocked(id, root)
    }
    /// `id` is now blocked on `root`: rows blocked on `id` name `root` too
    /// (`blocked_on` always names the root, so `revalidate(root)` finds them).
    fn repoint_blocked(&self, id: &str, root: &str) -> Result<()> {
        if id != root {
            self.db
                .prepare_cached(
                    "UPDATE incoming_events SET blocked_on=?2,error=?3 WHERE blocked_on=?1 AND state='blocked' AND id<>?2",
                )?
                .execute(params![id, root, format!("{ANCESTOR}: {root}")])?;
        }
        Ok(())
    }
    /// Row `id` can commit (it is `ready`, `waiting` or `retry`, or it is
    /// committing now): every row blocked on it waits on it instead, so it
    /// activates with it. Activation re-checks every link, so a row with
    /// another quarantined ancestor is blocked again then. Normally nothing
    /// is blocked on such a row; this repairs rows blocked on a legacy
    /// transient quarantine (RC11) and rows written by an older build.
    fn unblock(&self, id: &str) -> Result<()> {
        let rows: Vec<String> = self
            .db
            .prepare_cached(
                "SELECT id FROM incoming_events WHERE blocked_on=?1 AND state='blocked' AND id<>?1",
            )?
            .query_map([id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for row in rows {
            self.db
                .prepare_cached("INSERT OR IGNORE INTO incoming_waits(child,parent) VALUES(?,?)")?
                .execute(params![row, id])?;
            self.db
                .prepare_cached(
                    "UPDATE incoming_events SET state='waiting',error=NULL,code=NULL,error_class=NULL,blocked_on=NULL,retry_at_ms=NULL WHERE id=?",
                )?
                .execute([&row])?;
        }
        Ok(())
    }
    /// Write a classification to an existing row (and its waits).
    fn apply_class(&self, id: &str, class: Class) -> Result<()> {
        match class {
            Class::Ready => {
                self.db
                    .execute("DELETE FROM incoming_waits WHERE child=?", [id])?;
                self.db.execute(
                    "UPDATE incoming_events SET state='ready',error=NULL,code=NULL,error_class=NULL,blocked_on=NULL WHERE id=?",
                    [id],
                )?;
                self.unblock(id)?;
            }
            Class::Waiting(parents) => {
                self.db
                    .execute("DELETE FROM incoming_waits WHERE child=?", [id])?;
                for parent in &parents {
                    self.db
                        .prepare_cached(
                            "INSERT OR IGNORE INTO incoming_waits(child,parent) VALUES(?,?)",
                        )?
                        .execute(params![id, parent])?;
                }
                self.db.execute(
                    "UPDATE incoming_events SET state='waiting',error=NULL,code=NULL,error_class=NULL,blocked_on=NULL WHERE id=?",
                    [id],
                )?;
                self.unblock(id)?;
                // A cycle closes only when this event already has waiting
                // children; then search its staged ancestry for itself.
                if self.closes_cycle(id, &parents)? {
                    let wire = ErrorWire {
                        code: "CAUSAL_CYCLE".into(),
                        class: ErrorClass::Fatal,
                        message: format!("CAUSAL_CYCLE: {id} is its own causal ancestor"),
                        retry_after_ms: None,
                    };
                    self.quarantine(id, &wire)?;
                }
            }
            Class::Blocked(root) => {
                self.set_blocked(id, &root)?;
                self.block_descendants(id, &root)?;
            }
            Class::Quarantined(error) => {
                self.quarantine(id, &quarantine_wire(&error))?;
            }
        }
        Ok(())
    }
    /// Whether `id`, now waiting on `parents`, is its own staged ancestor.
    fn closes_cycle(&self, id: &str, parents: &BTreeSet<String>) -> Result<bool> {
        let has_children: bool = self
            .db
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM incoming_waits WHERE parent=?)")?
            .query_row([id], |r| r.get(0))?;
        if !has_children {
            return Ok(false);
        }
        let mut todo: Vec<String> = parents.iter().cloned().collect();
        let mut seen = BTreeSet::new();
        while let Some(next) = todo.pop() {
            if next == id {
                return Ok(true);
            }
            if !seen.insert(next.clone()) {
                continue;
            }
            let grand: Vec<String> = self
                .db
                .prepare_cached("SELECT parent FROM incoming_waits WHERE child=?")?
                .query_map([&next], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            todo.extend(grand);
        }
        Ok(false)
    }
    /// Classify the causal links of `event` by point lookups: dependencies
    /// first, then change parents (the order of `validate_direct_links`).
    fn classify_links(&self, event: &Event) -> Result<Class> {
        let mut waits = BTreeSet::new();
        let mut cache: BTreeMap<String, Parent> = BTreeMap::new();
        let dependencies = event.dependencies.iter().map(|d| (d, None));
        let parents = event
            .changes
            .iter()
            .flat_map(|c| c.parents.iter().map(move |p| (p, Some(c))));
        for (revision, change) in dependencies.chain(parents) {
            if revision == &event.id {
                return Ok(Class::Quarantined(anyhow::anyhow!(
                    "CAUSAL_CYCLE: {revision}"
                )));
            }
            if !cache.contains_key(revision) {
                let parent = self.parent(revision)?;
                cache.insert(revision.clone(), parent);
            }
            let links = match &cache[revision] {
                Parent::Quarantined(root) => return Ok(Class::Blocked(root.clone())),
                Parent::Missing => {
                    waits.insert(revision.clone());
                    continue;
                }
                Parent::Committed(links) => links,
                Parent::Staged(links) => {
                    waits.insert(revision.clone());
                    links
                }
            };
            if let Err(error) = check_link(event, revision, change, links) {
                return Ok(Class::Quarantined(error));
            }
        }
        Ok(if waits.is_empty() {
            Class::Ready
        } else {
            Class::Waiting(waits)
        })
    }
    /// Where revision `id` is: committed, staged (usable or quarantined) or missing.
    fn parent(&self, id: &str) -> Result<Parent> {
        let committed: Option<String> = self
            .db
            .prepare_cached("SELECT payload FROM events WHERE id=?")?
            .query_row([id], |r| r.get(0))
            .optional()?;
        if let Some(payload) = committed {
            return Ok(Parent::Committed(serde_json::from_str(&payload)?));
        }
        let staged: Option<StagedRow> = self
            .db
            .prepare_cached(
                "SELECT payload,state,error,blocked_on FROM incoming_events WHERE id=?",
            )?
            .query_row([id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .optional()?;
        let Some((payload, state, error, blocked_on)) = staged else {
            return Ok(Parent::Missing);
        };
        if let Some(root) = self.rejected_root_from(id, state, error, blocked_on)? {
            return Ok(Parent::Quarantined(root));
        }
        match serde_json::from_str(&payload) {
            Ok(links) => Ok(Parent::Staged(links)),
            // An unparseable staged parent can never commit; its children
            // wait for it to be quarantined at its own activation.
            Err(_) => Ok(Parent::Missing),
        }
    }
    /// Classify up to `limit` rows with `state IS NULL` (written by an older
    /// binary or by startup evacuation). Returns how many were classified.
    fn classify_unclassified(&mut self, limit: usize) -> Result<usize> {
        let rows: Vec<(String, String, Option<String>)> = self
            .db
            .prepare_cached(
                "SELECT id,payload,error FROM incoming_events WHERE state IS NULL ORDER BY rowid LIMIT ?",
            )?
            .query_map([limit as i64], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        for (id, payload, error) in &rows {
            crate::startup::progress("classifying incoming events");
            self.db.execute(
                "UPDATE incoming_events SET received_ms=coalesce(received_ms,0),attempts=coalesce(attempts,0) WHERE id=?",
                [id],
            )?;
            // A legacy quarantine of a transient failure (RC11) is not a
            // verdict: the row is re-classified on its links below.
            if let Some(text) = error.as_deref().filter(|text| !legacy_transient(text)) {
                // Quarantined by an older binary: keep its verdict and text.
                // Its ancestor text names the direct parent, which may itself
                // be blocked: follow the chain to the root.
                let root = match blocked_root(text) {
                    Some(ancestor) => self.rejected_root(&ancestor)?,
                    None => Some(id.clone()),
                };
                match root {
                    Some(root) if root != *id => {
                        self.db.execute(
                            "UPDATE incoming_events SET state='blocked',code=?,error_class='fatal',blocked_on=? WHERE id=?",
                            params![ANCESTOR, root, id],
                        )?;
                        self.repoint_blocked(id, &root)?;
                        self.block_descendants(id, &root)?;
                        continue;
                    }
                    Some(_) => {
                        let code = code_in(text).unwrap_or(crate::model::errors::UNKNOWN_CODE);
                        self.db.execute(
                            "UPDATE incoming_events SET state='quarantined',code=?,error_class=? WHERE id=?",
                            params![code, quarantine_class(code).as_str(), id],
                        )?;
                        self.block_descendants(id, id)?;
                        continue;
                    }
                    // The ancestor it was blocked on can commit now (it is
                    // committed, usable, or a legacy transient failure).
                    None => {}
                }
            }
            let class = match serde_json::from_str::<Event>(payload) {
                Ok(event) if event.id == *id => self.classify_links(&event)?,
                Ok(_) => {
                    Class::Quarantined(anyhow::anyhow!("INVALID_EVENT: staged payload id mismatch"))
                }
                Err(error) => {
                    Class::Quarantined(anyhow::Error::new(error).context("INVALID_EVENT"))
                }
            };
            self.apply_class(id, class)?;
        }
        Ok(rows.len())
    }
    /// The quarantined root that makes staged row `id` unusable as a parent,
    /// or `None` when it can still commit (it is usable, committed or
    /// missing). See [`rejected_root_from`](Self::rejected_root_from).
    fn rejected_root(&self, id: &str) -> Result<Option<String>> {
        let row: Option<(Option<String>, Option<String>, Option<String>)> = self
            .db
            .prepare_cached("SELECT state,error,blocked_on FROM incoming_events WHERE id=?")?
            .query_row([id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .optional()?;
        match row {
            Some((state, error, blocked_on)) => {
                self.rejected_root_from(id, state, error, blocked_on)
            }
            None => Ok(None),
        }
    }
    /// `rejected_root` of row `id` whose `state, error, blocked_on` are given.
    /// A `quarantined` row is its own root and a `blocked` row names its
    /// root. A legacy row (`state` NULL) is a root when its `error` is a
    /// verdict, usable when it has no `error` or a transient one (RC11), and
    /// blocked when its `error` is `QUARANTINED_CAUSAL_ANCESTOR: <ancestor>`:
    /// an older binary named the direct parent there, so the chain is
    /// followed to the root, and a chain that ends at a row that can commit
    /// means the legacy row can too. A classified `blocked` row is never
    /// usable: if its chain ends at a row that can commit (data written by
    /// an older build), that row is named, so its children are released
    /// with it (`unblock`). A cycle (or a blocked row without a root) makes
    /// the row where it closes the root.
    fn rejected_root_from(
        &self,
        id: &str,
        mut state: Option<String>,
        mut error: Option<String>,
        mut blocked_on: Option<String>,
    ) -> Result<Option<String>> {
        let mut next = id.to_owned();
        let mut seen = BTreeSet::new();
        // Whether the chain went through a classified `blocked` row.
        let mut blocked = false;
        loop {
            let usable = blocked.then(|| next.clone());
            let ancestor = match state.as_deref() {
                Some(QUARANTINED) => return Ok(Some(next)),
                Some(BLOCKED) => {
                    blocked = true;
                    blocked_on
                }
                Some(_) => return Ok(usable),
                None => match error.as_deref() {
                    None => return Ok(usable),
                    Some(text) if legacy_transient(text) => return Ok(usable),
                    Some(text) => match blocked_root(text) {
                        None => return Ok(Some(next)),
                        ancestor => ancestor,
                    },
                },
            };
            seen.insert(next.clone());
            let Some(ancestor) = ancestor.filter(|a| !seen.contains(a)) else {
                return Ok(Some(next));
            };
            let row: Option<(Option<String>, Option<String>, Option<String>)> = self
                .db
                .prepare_cached("SELECT state,error,blocked_on FROM incoming_events WHERE id=?")?
                .query_row([&ancestor], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .optional()?;
            let Some(row) = row else {
                // Committed or missing: it can commit (or has). A blocked
                // row naming it is stale until the startup repair.
                return Ok(blocked.then_some(next));
            };
            (state, error, blocked_on) = row;
            next = ancestor;
        }
    }
    /// Startup repairs: a restart retries every `retry` row now; waits on
    /// committed parents are dropped (an older binary may have committed
    /// them); waiting rows with no wait left become `ready`; rows whose
    /// legacy `error` disagrees with their state are re-classified, and so
    /// are blocked rows whose `blocked_on` is no longer a quarantined root
    /// (it committed under an older binary, or an older build left it
    /// pointing at an intermediate row); classification resolves their
    /// `QUARANTINED_CAUSAL_ANCESTOR` text to the root, or releases them.
    fn reconcile_rows(&mut self) -> Result<()> {
        self.db.execute_batch(
            "UPDATE incoming_events SET state='ready',retry_at_ms=NULL WHERE state='retry';
            DELETE FROM incoming_waits WHERE parent IN (SELECT id FROM events);
            DELETE FROM incoming_waits WHERE child NOT IN (SELECT id FROM incoming_events);
            UPDATE incoming_events SET state=NULL WHERE (state IN ('ready','waiting','retry') AND error IS NOT NULL)
                OR (state IN ('quarantined','blocked') AND error IS NULL)
                OR (state IS NOT NULL AND state NOT IN ('ready','waiting','retry','quarantined','blocked'));
            UPDATE incoming_events SET state=NULL WHERE state='blocked' AND NOT EXISTS(
                SELECT 1 FROM incoming_events r WHERE r.id=incoming_events.blocked_on
                AND (r.state='quarantined' OR (r.state IS NULL AND r.error IS NOT NULL)));
            UPDATE incoming_events SET state='ready' WHERE state='waiting'
                AND NOT EXISTS(SELECT 1 FROM incoming_waits w WHERE w.child=incoming_events.id);",
        )?;
        Ok(())
    }
    /// The branch's latest journal sequence (0 when it has no events).
    fn branch_seq(&self, branch: &str) -> Result<i64> {
        Ok(self
            .db
            .query_row(
                "SELECT s.seq FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? ORDER BY s.seq DESC LIMIT 1",
                [branch],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }
    /// Run `f` in a write transaction: `BEGIN IMMEDIATE` from autocommit
    /// (CCR-WP0.INT-5: no deferred read-then-write upgrade that fails with
    /// `SQLITE_BUSY` when another connection writes), else a savepoint inside
    /// the caller's transaction. Any failure rolls back and drops the view.
    fn with_write<T>(&mut self, name: &str, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        // Opening the transaction changes no data: keep a fresh view fresh.
        let view = self.keep_view();
        let outer = begin_write(&self.db, name)?;
        self.rekey_view(view);
        let result = f(self);
        // A view that is fresh at the end of the transaction is exactly the
        // committed state once `COMMIT` succeeds.
        let view = if result.is_ok() {
            self.keep_view()
        } else {
            None
        };
        let result = finish_write(&self.db, name, outer, result);
        if result.is_err() {
            self.view_cache.borrow_mut().take();
        } else {
            self.rekey_view(view);
        }
        // Commits of this transaction count once it committed (and not at
        // all if it rolled back); inside a caller's transaction this waits.
        self.settle_commit_counters();
        result
    }
    /// The `data_version` under which the cached view is fresh right now, if
    /// it is. Pair with `rekey_view` around writes that touch no projection
    /// state (inbox rows, object bookkeeping, transaction boundaries).
    fn keep_view(&self) -> Option<i64> {
        let data_version = self.data_version().ok()?;
        self.view_cache
            .borrow()
            .as_ref()
            .is_some_and(|v| {
                v.changes == self.db.total_changes()
                    && v.data_version == data_version
                    && v.transactional != self.db.is_autocommit()
            })
            .then_some(data_version)
    }
    /// Re-key a view kept by `keep_view` to the connection's current change
    /// count and transaction state, unless another connection committed in
    /// between (then the view is dropped, exactly as `view` would).
    fn rekey_view(&self, kept: Option<i64>) {
        let Some(data_version) = kept else {
            return;
        };
        let mut cache = self.view_cache.borrow_mut();
        if self.data_version().ok() != Some(data_version) {
            *cache = None;
        } else if let Some(view) = cache.as_mut() {
            view.changes = self.db.total_changes();
            view.transactional = !self.db.is_autocommit();
        }
    }

    /// Off-lock preparation of a slow commit. Wave 1: nothing is precomputed;
    /// `commit_prepared` runs the slow path under the caller's lock.
    pub fn prepare_slow(read: &Store, t: &SlowTicket) -> Result<Prepared> {
        let _ = read;
        Ok(Prepared::Inline(t.clone()))
    }
    /// Commit a prepared slow event under the caller's lock, in its own write
    /// transaction. The row must still be staged as `ready` (or `retry`) with
    /// every parent committed; otherwise the ticket is `Stale`. An event that
    /// was committed meanwhile is reported as committed.
    pub fn commit_prepared(&mut self, p: Prepared) -> Result<CommitOutcome> {
        let ticket = match p {
            Prepared::Inline(ticket) => ticket,
            Prepared::Computed(prepared) => prepared.ticket,
        };
        self.with_write("inbox_prepared", |store| {
            let committed = |store: &Store| -> Result<bool> {
                Ok(store.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM events WHERE id=?)",
                    [&ticket.id],
                    |r| r.get(0),
                )?)
            };
            Ok(match store.attempt(&ticket.id, true, false)? {
                Attempt::Activated { fast, .. } => CommitOutcome::Committed {
                    id: ticket.id.clone(),
                    slow: fast == Some(false),
                },
                Attempt::Quarantined(error) | Attempt::Retried(error) => {
                    CommitOutcome::Failed(error)
                }
                Attempt::Gone if committed(store)? => CommitOutcome::Committed {
                    id: ticket.id.clone(),
                    slow: false,
                },
                Attempt::Gone | Attempt::Deferred | Attempt::Ticket(_) | Attempt::SlowSpent => {
                    CommitOutcome::Stale
                }
            })
        })
    }
    /// Inbox rows by state (§4.4).
    pub fn inbox_counts(&self) -> Result<InboxCounts> {
        let mut counts = InboxCounts::default();
        let mut statement = self
            .db
            .prepare_cached("SELECT state,count(*) FROM incoming_events GROUP BY state")?;
        let rows = statement.query_map([], |r| Ok((r.get::<_, Option<String>>(0)?, r.get(1)?)))?;
        for row in rows {
            let (state, count): (Option<String>, i64) = row?;
            let count = count.max(0) as u64;
            match state.as_deref() {
                Some(READY) => counts.ready += count,
                Some(WAITING) => counts.waiting += count,
                Some(RETRY) => counts.retry += count,
                Some(BLOCKED) => counts.blocked += count,
                Some(QUARANTINED) => counts.quarantined += count,
                _ => counts.unclassified += count,
            }
        }
        Ok(counts)
    }
    /// Revisions that waiting rows need and that are neither committed nor
    /// staged (the need list of a feed exchange), in id order.
    pub fn missing_parents(&self, limit: usize) -> Result<Vec<String>> {
        Ok(self
            .db
            .prepare_cached(
                "SELECT DISTINCT w.parent FROM incoming_waits w JOIN incoming_events c ON c.id=w.child AND c.state='waiting'
                WHERE NOT EXISTS(SELECT 1 FROM incoming_events i WHERE i.id=w.parent)
                AND NOT EXISTS(SELECT 1 FROM events e WHERE e.id=w.parent)
                ORDER BY w.parent LIMIT ?",
            )?
            .query_map([limit as i64], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    /// Quarantined rows: `(id, code, message)`, oldest first. Blocked
    /// descendants are not listed (they name their root in `blocked_on`).
    pub fn quarantined(&self, limit: usize) -> Result<Vec<(String, String, String)>> {
        let rows: Vec<(String, Option<String>, Option<String>)> = self
            .db
            .prepare_cached(
                "SELECT id,code,error FROM incoming_events WHERE state='quarantined'
                OR (state IS NULL AND error IS NOT NULL AND error NOT LIKE 'QUARANTINED_CAUSAL_ANCESTOR%')
                ORDER BY coalesce(received_ms,0),rowid LIMIT ?",
            )?
            .query_map([limit as i64], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows
            .into_iter()
            .map(|(id, code, error)| {
                let message = error.unwrap_or_default();
                let code = code
                    .or_else(|| code_in(&message).map(str::to_owned))
                    .unwrap_or_else(|| crate::model::errors::UNKNOWN_CODE.to_owned());
                (id, code, message)
            })
            .collect())
    }
    /// Re-queue rows for classification and activation: the given
    /// quarantined, blocked or retry rows plus every row blocked on one of
    /// them (transitively: rows written by an older build may be blocked on
    /// an intermediate blocked row); `None` re-queues every such row.
    /// Returns how many were reset.
    pub fn revalidate(&mut self, ids: Option<&[String]>) -> Result<usize> {
        self.with_write("inbox_revalidate", |store| {
            let targets: BTreeSet<String> = match ids {
                None => store
                    .db
                    .prepare(
                        "SELECT id FROM incoming_events WHERE state IN ('quarantined','blocked','retry') OR (state IS NULL AND error IS NOT NULL)",
                    )?
                    .query_map([], |r| r.get(0))?
                    .collect::<rusqlite::Result<_>>()?,
                Some(ids) => {
                    let mut targets = BTreeSet::new();
                    for id in ids {
                        let listed: bool = store.db.query_row(
                            "SELECT EXISTS(SELECT 1 FROM incoming_events WHERE id=? AND (state IN ('quarantined','blocked','retry') OR (state IS NULL AND error IS NOT NULL)))",
                            [id],
                            |r| r.get(0),
                        )?;
                        if listed {
                            targets.insert(id.clone());
                        }
                    }
                    let mut todo = ids.to_vec();
                    let mut seen = BTreeSet::new();
                    while let Some(id) = todo.pop() {
                        if !seen.insert(id.clone()) {
                            continue;
                        }
                        let blocked: Vec<String> = store
                            .db
                            .prepare_cached(
                                "SELECT id FROM incoming_events WHERE blocked_on=? AND state='blocked'",
                            )?
                            .query_map([&id], |r| r.get(0))?
                            .collect::<rusqlite::Result<_>>()?;
                        todo.extend(blocked.iter().cloned());
                        targets.extend(blocked);
                    }
                    targets
                }
            };
            for id in &targets {
                store
                    .db
                    .execute("DELETE FROM incoming_waits WHERE child=?", [id])?;
                store.db.execute(
                    "UPDATE incoming_events SET state=NULL,error=NULL,code=NULL,error_class=NULL,blocked_on=NULL,attempts=0,retry_at_ms=NULL WHERE id=?",
                    [id],
                )?;
            }
            Ok(targets.len())
        })
    }
    /// A ready (or retry) row goes to `retry`, due in `delay_ms` (used for a
    /// `Stale` slow commit). Other rows are left alone.
    pub fn requeue(&mut self, id: &str, delay_ms: u64) -> Result<()> {
        self.db.execute(
            "UPDATE incoming_events SET state='retry',retry_at_ms=? WHERE id=? AND state IN ('ready','retry')",
            params![(time().saturating_add(delay_ms)) as i64, id],
        )?;
        Ok(())
    }
}

/// `BEGIN IMMEDIATE` (from autocommit) plus `SAVEPOINT name`. Returns whether
/// this call owns the outer transaction.
fn begin_write(db: &Connection, name: &str) -> Result<bool> {
    let outer = db.is_autocommit();
    if outer {
        db.execute_batch("BEGIN IMMEDIATE")?;
    }
    if let Err(error) = db.execute_batch(&format!("SAVEPOINT {name}")) {
        if outer {
            let _ = db.execute_batch("ROLLBACK");
        }
        return Err(error.into());
    }
    Ok(outer)
}

/// Commit `result`'s writes (`RELEASE`, then `COMMIT` when `outer`), or roll
/// them back (including after a failed `COMMIT`).
fn finish_write<T>(db: &Connection, name: &str, outer: bool, result: Result<T>) -> Result<T> {
    let committed = result.and_then(|value| {
        db.execute_batch(&format!("RELEASE {name}"))?;
        if outer {
            db.execute_batch("COMMIT")?;
        }
        Ok(value)
    });
    if committed.is_err() {
        if !outer {
            let _ = db.execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"));
        } else if !db.is_autocommit() {
            // SQLite may already have rolled back (for example after an I/O
            // error); then there is nothing left to discard.
            let _ = db.execute_batch("ROLLBACK");
        }
    }
    committed
}

/// Every revision `event` references: dependencies, then change parents.
fn references(event: &Event) -> impl Iterator<Item = &String> {
    event
        .dependencies
        .iter()
        .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
}

/// Direct-link validation against a known parent (as `validate_direct_links`).
fn check_link(
    event: &Event,
    revision: &str,
    change: Option<&Change>,
    parent: &Links,
) -> Result<()> {
    match change {
        None => ensure!(
            parent.branch == event.branch,
            "INVALID_CAUSAL_DEPENDENCY: {revision}"
        ),
        Some(change) => ensure!(
            parent.branch == event.branch
                && parent
                    .changes
                    .iter()
                    .any(|c| c.entity == change.entity && c.field == change.field),
            "INVALID_CAUSAL_PARENT: {revision}"
        ),
    }
    Ok(())
}

/// Transient failures are retried, never quarantined: SQLite busy, locked,
/// I/O, full, out-of-memory and cannot-open errors, and any `std::io` error.
/// Everything else (validation, malformed payloads) is a quarantine.
fn is_transient(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(rusqlite::Error::SqliteFailure(failure, _)) = cause.downcast_ref() {
            return matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy
                    | rusqlite::ErrorCode::DatabaseLocked
                    | rusqlite::ErrorCode::SystemIoFailure
                    | rusqlite::ErrorCode::DiskFull
                    | rusqlite::ErrorCode::OutOfMemory
                    | rusqlite::ErrorCode::CannotOpen
            );
        }
        cause.downcast_ref::<std::io::Error>().is_some()
    })
}

/// A legacy `error` text that recorded a transient failure (the old drain
/// quarantined everything, RC11).
fn legacy_transient(text: &str) -> bool {
    [
        "database is locked",
        "database table is locked",
        "disk I/O error",
        "database or disk is full",
        "unable to open database file",
        "SQLITE_BUSY",
    ]
    .iter()
    .any(|marker| text.contains(marker))
}

/// The class recorded for a quarantine: the §8 class when it is one of the
/// non-retrying classes, else Fatal (a scoped quarantine).
fn quarantine_class(code: &str) -> ErrorClass {
    match explicit_class(code) {
        Some(class @ (ErrorClass::NeedsUserAction | ErrorClass::Incompatible)) => class,
        _ => ErrorClass::Fatal,
    }
}

fn quarantine_wire(error: &anyhow::Error) -> ErrorWire {
    let code = code_of(error);
    ErrorWire {
        class: quarantine_class(&code),
        code,
        message: format!("{error:#}"),
        retry_after_ms: None,
    }
}

/// `QUARANTINED_CAUSAL_ANCESTOR: <id>` → `<id>`.
fn blocked_root(text: &str) -> Option<String> {
    let rest = text.strip_prefix(ANCESTOR)?.strip_prefix(':')?.trim_start();
    let id: String = rest
        .chars()
        .take_while(|c| !c.is_whitespace() && *c != ':')
        .collect();
    (!id.is_empty()).then_some(id)
}

fn retry_delay_ms(attempts: u64) -> u64 {
    1000u64
        .saturating_mul(1u64 << attempts.min(20))
        .min(RETRY_CAP_MS)
}

pub(super) fn validate_known_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
) -> Result<()> {
    validate_incoming_links(event, known, quarantined, &BTreeSet::new())
}
fn validate_incoming_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
    committed: &BTreeSet<String>,
) -> Result<()> {
    validate_direct_links(event, known, quarantined)?;
    let mut visited = BTreeSet::new();
    let mut todo = event.dependencies.clone();
    todo.extend(event.changes.iter().flat_map(|c| c.parents.iter().cloned()));
    while let Some(revision) = todo.pop() {
        ensure!(revision != event.id, "CAUSAL_CYCLE");
        if committed.contains(&revision) {
            continue;
        }
        if visited.insert(revision.clone())
            && let Some(parent) = known.get(&revision)
        {
            todo.extend(parent.dependencies.iter().cloned());
            todo.extend(
                parent
                    .changes
                    .iter()
                    .flat_map(|c| c.parents.iter().cloned()),
            );
        }
    }
    Ok(())
}
pub(super) fn validate_direct_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
) -> Result<()> {
    for dependency in &event.dependencies {
        ensure!(
            !quarantined.contains(dependency),
            "QUARANTINED_CAUSAL_ANCESTOR: {dependency}"
        );
        if let Some(parent) = known.get(dependency) {
            ensure!(
                parent.branch == event.branch,
                "INVALID_CAUSAL_DEPENDENCY: {dependency}"
            );
        }
    }
    for change in &event.changes {
        for revision in &change.parents {
            ensure!(
                !quarantined.contains(revision),
                "QUARANTINED_CAUSAL_ANCESTOR: {revision}"
            );
            if let Some(parent) = known.get(revision) {
                ensure!(
                    parent.branch == event.branch
                        && parent
                            .changes
                            .iter()
                            .any(|c| c.entity == change.entity && c.field == change.field),
                    "INVALID_CAUSAL_PARENT: {revision}"
                );
            }
        }
    }
    Ok(())
}
