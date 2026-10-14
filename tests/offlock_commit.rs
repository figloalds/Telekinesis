//! WP2.4: the off-lock slow path and the event cache (REVAMP_PLAN §4.4,
//! §14 WP2.4), plus the CCRs applied with it (CCR-WP1.INT-4, CCR-WP1.6-1,
//! CCR-WP1.6-5).
//!
//! - **Differential fuzz** (`prepared_commits_equal_inline_commits_and_full_replay`,
//!   300 seeds in release with `prepared_commits_300_seeds_release`): 2–4
//!   replicas make random creates, mkdirs, renames, directory moves, deletes,
//!   resurrections, saves, stale-base saves, metadata changes, resolutions
//!   and recoveries, and deliver each other's events in random pages, random
//!   orders and with duplicates. A delivery is only staged; the receiver then
//!   activates like the background activator: slices of 64 events / 25 ms,
//!   every slow event prepared off-lock on a long-lived read handle and
//!   committed with `commit_prepared`. Before each prepared commit, either a
//!   concurrent local write lands on the receiver (the ticket must go
//!   `Stale`, be re-queued and converge later), or a byte copy of the store
//!   commits the same ticket inline and every derived table must come out
//!   identical (projection, `current_state`, `current_parts`, conflict rows
//!   with `resolved_by`, frontier, startup snapshot, journal, outbox, inbox).
//!   Every commit is also checked against a derivation from no rows inside
//!   its transaction (`set_thread_commit_checks`), the strict oracle
//!   (`verify_derived_state_strict`, every row re-derived from no rows) must
//!   be empty after every step, and at the end every replica converges to the
//!   same events, projection and conflict rows. Seeds alternate append and
//!   verify mode, so off-lock derivation is also compared with full mode.
//! - Staleness, re-validation under the lock (`OBJECT_SIZE_CHANGED`,
//!   `EVENT_ID_REUSED`, `BRANCH_DESCRIPTOR_CHANGED`), recorded refusals, the
//!   event cache against `events()`, the slow-commit delta through the
//!   prepared path, crash points of a prepared commit and of the periodic
//!   seal, and the `NAMESPACE_HISTORY_LIMIT` count of successor commits at
//!   the 4,096-cut boundary.
mod support;

use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
};
use support::{causal_sort, clone_replica, devices, linear_history};
use tempfile::TempDir;
use tkfs::core::{
    derive::{
        ConflictMode, DeriveCounters, set_thread_commit_checks, set_thread_conflict_mode,
        thread_derive_counters,
    },
    inbox::{CommitOutcome, Prepared, SliceBudget, SlowTicket, StagedEvent},
    *,
};

const REPO: &str = "offlock-commit";
const NAMES: &[&str] = &["a", "A", "b", "c.txt", "C.TXT", "d"];

/// Sets the calling thread's conflict mode; restores the previous one on drop.
struct ModeGuard(Option<ConflictMode>);
impl ModeGuard {
    fn new(mode: ConflictMode) -> Self {
        Self(set_thread_conflict_mode(Some(mode)))
    }
}
impl Drop for ModeGuard {
    fn drop(&mut self) {
        set_thread_conflict_mode(self.0);
    }
}

/// Checks every commit of the calling thread against the reference.
struct ChecksGuard(bool);
impl ChecksGuard {
    fn new() -> Self {
        Self(set_thread_commit_checks(true))
    }
}
impl Drop for ChecksGuard {
    fn drop(&mut self) {
        set_thread_commit_checks(self.0);
    }
}

/// Every derived or bookkept table a commit writes, as sorted text rows.
const TABLES: &[(&str, &str)] = &[
    (
        "current_state",
        "SELECT branch,payload FROM current_state ORDER BY branch",
    ),
    (
        "current_parts",
        "SELECT branch,entity,entry,heads FROM current_parts ORDER BY branch,entity",
    ),
    (
        "conflicts",
        "SELECT id,branch,payload,coalesce(resolved_by,'') FROM conflicts ORDER BY id",
    ),
    (
        "event_frontier",
        "SELECT branch,event FROM event_frontier ORDER BY branch,event",
    ),
    (
        "startup_snapshots",
        "SELECT branch,cursor,payload,checksum FROM startup_snapshots ORDER BY branch",
    ),
    (
        "journal_sequence",
        "SELECT seq,event FROM journal_sequence ORDER BY seq",
    ),
    ("events", "SELECT id,branch,payload FROM events ORDER BY id"),
    (
        "outbox",
        "SELECT event,acknowledged FROM outbox ORDER BY event",
    ),
    (
        "incoming_events",
        "SELECT id,coalesce(state,''),coalesce(error,''),coalesce(code,''),coalesce(blocked_on,'') FROM incoming_events ORDER BY id",
    ),
    (
        "incoming_waits",
        "SELECT child,parent FROM incoming_waits ORDER BY child,parent",
    ),
    (
        "object_references",
        "SELECT hash FROM object_references ORDER BY hash",
    ),
    (
        "object_sizes",
        "SELECT hash,size FROM object_sizes ORDER BY hash",
    ),
    (
        "branches",
        "SELECT id,name,shared FROM branches ORDER BY id",
    ),
];

fn table(s: &Store, sql: &str) -> Vec<Vec<String>> {
    let mut statement = s.db.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |r| {
            (0..columns)
                .map(|i| {
                    Ok(match r.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => String::new(),
                        rusqlite::types::ValueRef::Integer(v) => v.to_string(),
                        rusqlite::types::ValueRef::Real(v) => v.to_string(),
                        rusqlite::types::ValueRef::Text(v) | rusqlite::types::ValueRef::Blob(v) => {
                            String::from_utf8_lossy(v).into_owned()
                        }
                    })
                })
                .collect()
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// The tables that differ between two stores, with the first differing row.
fn table_differences(a: &Store, b: &Store) -> Vec<String> {
    let mut out = vec![];
    for (name, sql) in TABLES {
        let (left, right) = (table(a, sql), table(b, sql));
        if left != right {
            let first = left
                .iter()
                .zip(&right)
                .position(|(l, r)| l != r)
                .unwrap_or(left.len().min(right.len()));
            let show = |rows: &[Vec<String>]| {
                rows.get(first)
                    .map(|row| {
                        row.iter()
                            .map(|v| v.chars().take(160).collect::<String>())
                            .collect::<Vec<_>>()
                            .join(" | ")
                    })
                    .unwrap_or_else(|| "<none>".into())
            };
            out.push(format!(
                "{name}: {} vs {} rows; first difference at {first}:\n  {}\n  {}",
                left.len(),
                right.len(),
                show(&left),
                show(&right)
            ));
        }
    }
    out
}

/// A byte copy of `s` (`VACUUM INTO`), opened as a plain secondary handle:
/// no recovery, no replay, nothing activated, so its tables are exactly `s`'s.
fn copy_of(s: &Store) -> (TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("objects")).unwrap();
    let path = dir.path().join("metadata.sqlite");
    s.db.execute("VACUUM INTO ?", [path.to_str().unwrap()])
        .unwrap();
    let copy = Store::open_secondary(dir.path(), &s.repo, &s.device).unwrap();
    (dir, copy)
}

/// The startup snapshot of `branch` is valid as `snapshot.rs` checks it on
/// open: checksum, a cursor within the branch's journal, the journal hash up
/// to it, and a sealed projection equal to the replay of the journal up to
/// the cursor (and to the stored projection when the cursor is the tip).
/// Returns whether the cursor is the branch's tip.
#[track_caller]
fn assert_snapshot_valid(s: &Store, branch: &str) -> bool {
    let (cursor, payload, checksum): (i64, String, String) =
        s.db.query_row(
            "SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?",
            [branch],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(hash(payload.as_bytes()), checksum, "snapshot checksum");
    let snapshot: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(snapshot["cursor"], json!(cursor));
    let mut digest = Sha256::new();
    let mut prefix: Vec<Event> = vec![];
    let mut tip = 0;
    let mut statement = s.db.prepare("SELECT s.seq,e.id,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? ORDER BY s.seq").unwrap();
    let mut rows = statement.query([branch]).unwrap();
    while let Some(row) = rows.next().unwrap() {
        let seq: i64 = row.get(0).unwrap();
        tip = seq;
        if seq > cursor {
            continue;
        }
        digest.update(seq.to_le_bytes());
        for column in [1, 2] {
            let value: String = row.get(column).unwrap();
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value.as_bytes());
        }
        prefix.push(serde_json::from_str(&row.get::<_, String>(2).unwrap()).unwrap());
    }
    assert!(cursor <= tip, "snapshot cursor {cursor} past the tip {tip}");
    assert_eq!(
        snapshot["journal_hash"],
        json!(hex::encode(digest.finalize())),
        "journal hash"
    );
    let projection: Projection = serde_json::from_value(snapshot["projection"].clone()).unwrap();
    assert_eq!(
        projection,
        project(branch, &prefix).unwrap(),
        "sealed projection replays the journal up to the cursor"
    );
    if cursor == tip {
        assert_eq!(
            projection,
            s.projection(branch).unwrap(),
            "sealed projection"
        );
    }
    cursor == tip
}

#[track_caller]
fn assert_oracle_empty(s: &mut Store, step: &str) {
    let diff = s.verify_derived_state_strict().unwrap();
    assert!(diff.is_empty(), "{step}: {diff:#?}");
}

fn staged(event: Event) -> StagedEvent {
    StagedEvent {
        event,
        source: None,
    }
}

/// Shared events of `from` that `to` lacks, causally sorted.
fn missing(from: &Store, to: &Store) -> Vec<Event> {
    let mut bundle = from
        .shared_bundle(&to.shared_event_inventory().unwrap())
        .unwrap();
    causal_sort(&mut bundle.events);
    bundle.events
}

fn write(s: &mut Store, path: &str, bytes: &[u8]) -> String {
    let entry = s.lookup(path).unwrap();
    s.write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
        .unwrap()
}

/// One activator pass's statistics.
#[derive(Debug, Default, Clone, Copy)]
struct Activation {
    tickets: usize,
    computed: usize,
    compared: usize,
    stale: usize,
    failed: usize,
    committed_slow: usize,
    /// Waits for a `retry` row that was not due yet (a re-queued `Stale`
    /// ticket behind a wall-clock step, or a transient backoff).
    waits: usize,
}

impl Activation {
    fn add(&mut self, other: &Activation) {
        self.tickets += other.tickets;
        self.computed += other.computed;
        self.compared += other.compared;
        self.stale += other.stale;
        self.failed += other.failed;
        self.committed_slow += other.committed_slow;
        self.waits += other.waits;
    }
}

/// Every inbox row of `s`, one per line (failure messages).
fn inbox_dump(s: &Store) -> String {
    let rows: Vec<String> = s
        .db
        .prepare(
            "SELECT id,coalesce(state,'NULL'),coalesce(code,''),coalesce(error_class,''),coalesce(attempts,0),coalesce(retry_at_ms,0),coalesce(error,'') FROM incoming_events ORDER BY received_ms,rowid",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(format!(
                "{} state={} code={} class={} attempts={} retry_at_ms={} error={}",
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, String>(6)?
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    format!(
        "now_ms={} inbox ({} rows):\n  {}",
        time(),
        rows.len(),
        rows.join("\n  ")
    )
}

/// The `retry` rows of `s`: `(id, retry_at_ms, code, error_class)`, the
/// earliest due first.
fn retry_rows(s: &Store) -> Vec<(String, i64, String, String)> {
    s.db.prepare(
        "SELECT id,coalesce(retry_at_ms,0),coalesce(code,''),coalesce(error_class,'') FROM incoming_events WHERE state='retry' ORDER BY coalesce(retry_at_ms,0)",
    )
    .unwrap()
    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

/// How long one activation may wait, in total, for `retry` rows to fall due.
const RETRY_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);

/// What happened between `prepare_slow` and `commit_prepared`.
enum Between {
    /// Nothing: commit as computed.
    Nothing,
    /// Commit the same ticket inline on a copy first; the tables must match.
    Compare,
    /// The callback ran a concurrent local write on the store (the engine
    /// lock is free while a slow event is prepared); `true` if it committed.
    Wrote(bool),
}

/// Activate `s` like the background activator (§4.4) until nothing is
/// ready: slices of `SliceBudget::ACTIVATOR` under the "lock", slow events
/// prepared off-lock on `read`, committed with `commit_prepared`, `Stale`
/// re-queued. `between` runs between prepare and commit of each ticket.
///
/// A re-queued row (`requeue(.., 0)`) is due at once by the wall clock the
/// store uses, which can step back (WSL resynchronizes it with the host):
/// a `retry` row that is not due yet is waited for (fix round 1; the loop
/// used to spin on it until its round cap). A transient backoff
/// (`retry()`, `error_class = 'transient'`) is waited for too, except
/// `CAUSAL_PREREQUISITES_PENDING`: in this single-branch fuzz every parent
/// of an event is committed on its branch before it is ready, so a pending
/// projection means the slow path's history was incomplete, a defect.
fn activate(
    s: &mut Store,
    read: &Store,
    between: &mut dyn FnMut(&SlowTicket, &mut Store) -> Between,
) -> Activation {
    let mut stats = Activation::default();
    let mut waited = std::time::Duration::ZERO;
    for round in 0.. {
        assert!(
            round < 20_000,
            "activation does not converge: {stats:?}\n{}",
            inbox_dump(s)
        );
        let slice = s.activate_slice(SliceBudget::ACTIVATOR).unwrap();
        let Some(ticket) = slice.slow_ticket else {
            if slice.more {
                continue;
            }
            let retry = retry_rows(s);
            let Some((_, due, _, _)) = retry.first() else {
                return stats;
            };
            if let Some((id, ..)) = retry
                .iter()
                .find(|(_, _, code, _)| code == "CAUSAL_PREREQUISITES_PENDING")
            {
                panic!(
                    "{id}: the slow path found its history incomplete (single branch, every parent committed)\n{}",
                    inbox_dump(s)
                );
            }
            let now = time() as i64;
            if *due > now {
                // Not due yet: wait for it (bounded).
                let wait = std::time::Duration::from_millis((*due - now).min(2_000) as u64 + 1);
                waited += wait;
                assert!(
                    waited <= RETRY_WAIT_BUDGET,
                    "retry rows not due after {waited:?}: {stats:?}\n{}",
                    inbox_dump(s)
                );
                stats.waits += 1;
                std::thread::sleep(wait);
            }
            continue;
        };
        stats.tickets += 1;
        let prepared = Store::prepare_slow(read, &ticket).unwrap();
        let Prepared::Computed(computed) = &prepared else {
            panic!("prepare_slow computes off-lock");
        };
        stats.computed += usize::from(computed.is_computed());
        let mut wrote = false;
        let outcome = match between(&ticket, s) {
            Between::Nothing => s.commit_prepared(prepared).unwrap(),
            Between::Wrote(committed) => {
                wrote = committed;
                s.commit_prepared(prepared).unwrap()
            }
            Between::Compare => {
                let (_dir, mut copy) = copy_of(s);
                let inline = copy
                    .commit_prepared(Prepared::Inline(ticket.clone()))
                    .unwrap();
                let outcome = s.commit_prepared(prepared).unwrap();
                assert_eq!(outcome, inline, "outcome of {}", ticket.id);
                let differences = table_differences(s, &copy);
                assert!(
                    differences.is_empty(),
                    "prepared commit of {} differs from the inline commit:\n{}",
                    ticket.id,
                    differences.join("\n")
                );
                assert_eq!(
                    s.projection(&ticket.branch).unwrap(),
                    copy.projection(&ticket.branch).unwrap()
                );
                stats.compared += 1;
                outcome
            }
        };
        match outcome {
            CommitOutcome::Stale => {
                stats.stale += 1;
                s.requeue(&ticket.id, 0).unwrap();
            }
            CommitOutcome::Committed { slow, .. } => {
                assert!(
                    !wrote,
                    "{}: committed over a concurrent local commit",
                    ticket.id
                );
                stats.committed_slow += usize::from(slow);
            }
            CommitOutcome::Failed(_) => stats.failed += 1,
        }
    }
    unreachable!()
}

/// The visible path of an entry, if it and every ancestor are visible.
fn path_of(p: &Projection, id: &str) -> Option<String> {
    let mut parts = vec![];
    let mut cursor = id;
    while cursor != ROOT {
        let e = p.entries.get(cursor).filter(|e| e.alive)?;
        parts.push(e.name.clone());
        cursor = &e.parent;
        if parts.len() > 64 {
            return None;
        }
    }
    parts.reverse();
    Some(parts.join("/"))
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.into()
    } else {
        format!("{dir}/{name}")
    }
}

/// One random local operation on `s` (the derive_oracle repertoire). Returns
/// a description, or `None` when nothing applied. Expected refusals are
/// fine; anything else fails the test.
fn local_op(rng: &mut StdRng, s: &mut Store, serial: usize) -> Option<String> {
    let pick = |rng: &mut StdRng, n: usize| rng.gen_range(0..n.max(1));
    let p = s.projection(&s.active).unwrap();
    let visible: Vec<(String, String)> = p
        .entries
        .keys()
        .filter_map(|id| path_of(&p, id).map(|path| (id.clone(), path)))
        .collect();
    let mut dirs: Vec<String> = visible
        .iter()
        .filter(|(id, _)| p.entries[id].kind == "directory")
        .map(|(_, path)| path.clone())
        .collect();
    dirs.push(String::new());
    let dir = dirs[pick(rng, dirs.len())].clone();
    let name = NAMES[pick(rng, NAMES.len())];
    let target = visible.get(pick(rng, visible.len())).cloned();
    let files: Vec<Entry> = p
        .entries
        .values()
        .filter(|e| e.kind == "file")
        .cloned()
        .collect();
    let file = files.get(pick(rng, files.len())).cloned();
    let (what, result, expected): (String, anyhow::Result<()>, &[&str]) = match rng
        .gen_range(0..100)
    {
        0..=24 => {
            let kind = if rng.gen_bool(0.6) {
                "file"
            } else {
                "directory"
            };
            let path = join(&dir, name);
            (
                format!("create {kind} {path}"),
                s.create(&path, kind).map(|_| ()),
                &["NAME_COLLISION", "NOT_DIRECTORY", "FILE_NOT_FOUND"],
            )
        }
        25..=44 => {
            let (id, path) = target?;
            let to = join(&dir, name);
            let replace = rng.gen_bool(0.3);
            (
                format!("rename {path} -> {to} replace={replace}"),
                s.rename(&id, &to, replace),
                &[
                    "NAME_COLLISION",
                    "DIRECTORY_CYCLE",
                    "NOT_DIRECTORY",
                    "FILE_NOT_FOUND",
                ],
            )
        }
        45..=54 => {
            let (id, path) = target?;
            (
                format!("delete {path}"),
                s.delete(&id),
                &["DIRECTORY_NOT_EMPTY"],
            )
        }
        55..=69 => {
            let file = file?;
            (
                format!("write {} alive={}", file.id, file.alive),
                s.write_revision(
                    &file.id,
                    file.revisions.get("content").cloned(),
                    format!("bytes {serial}").as_bytes(),
                )
                .map(|_| ()),
                &[],
            )
        }
        70..=74 => {
            let file = file?;
            let history: Vec<Event> = s
                .events()
                .unwrap()
                .into_iter()
                .filter(|e| e.changes.iter().any(|c| c.entity == file.id))
                .collect();
            let base = |field: &str, rng: &mut StdRng| {
                let versions: Vec<_> = history
                    .iter()
                    .filter(|e| {
                        e.changes
                            .iter()
                            .any(|c| c.entity == file.id && c.field == field)
                    })
                    .map(|e| e.id.clone())
                    .collect();
                versions.choose(rng).cloned()
            };
            let content = base("content", rng);
            let alive = base("alive", rng);
            (
                format!("stale write {}", file.id),
                s.write_with_base(
                    &file.id,
                    content,
                    alive,
                    format!("stale {serial}").as_bytes(),
                )
                .map(|_| ()),
                &[],
            )
        }
        75..=79 => {
            let (id, path) = target?;
            let entry = &p.entries[&id];
            let mut basic = entry.basic_info();
            basic.attributes = BasicInfo::attributes(&entry.kind, basic.attributes ^ 0x1).unwrap();
            (format!("set_basic {path}"), s.set_basic(&id, basic), &[])
        }
        80..=89 => {
            let open: Vec<Conflict> = s
                .conflicts()
                .unwrap()
                .into_iter()
                .filter(|c| c.resolved_by.is_none() && c.field != "namespace")
                .collect();
            let c = open.choose(rng).cloned()?;
            let selected = c.alternatives.choose(rng).unwrap().revision.clone();
            (
                format!("resolve {} {}/{} -> {selected}", c.id, c.entity, c.field),
                s.resolve(std::slice::from_ref(&c.id), &selected)
                    .map(|_| ()),
                &["CONFLICT_NOT_FOUND", "REVISION_NOT_IN_REVIEW"],
            )
        }
        _ => {
            let open: Vec<Conflict> = s
                .conflicts()
                .unwrap()
                .into_iter()
                .filter(|c| c.resolved_by.is_none() && c.field == "namespace")
                .collect();
            let c = open.choose(rng).cloned()?;
            let entities: Vec<&str> = c.entity.split(',').collect();
            let entity = entities.choose(rng).unwrap().to_string();
            let to = format!("recovered-{serial}");
            (
                format!("recover {entity} -> {to} reviewing {}", c.id),
                s.recover(&entity, &to, std::slice::from_ref(&c.id))
                    .map(|_| ()),
                &[
                    "EXPLICIT_EXISTENCE_RESOLUTION_REQUIRED",
                    "REVIEW_HAS_NO_ENTRY_LOCATION",
                    "RECOVERY_NOT_VISIBLE",
                    "REVIEW_SCOPE_MISMATCH",
                    "ENTRY_NOT_FOUND",
                    "CONFLICT_NOT_FOUND",
                ],
            )
        }
    };
    match result {
        Ok(()) => Some(what),
        Err(error) => {
            let text = format!("{error:#}");
            assert!(
                expected.iter().any(|code| text.contains(code))
                    || text.contains("NAMESPACE_HISTORY_LIMIT"),
                "{what}: unexpected error {text}"
            );
            None
        }
    }
}

struct Replica {
    /// `None` once kept on disk for a post-mortem (`Fuzz`'s drop).
    dir: Option<TempDir>,
    store: Option<Store>,
    /// The off-lock read handle, kept across preparations (its event cache).
    read: Option<Store>,
}
impl Replica {
    /// A new replica with the given device id (from the seed).
    fn new(device: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::initialize(dir.path(), REPO, device).unwrap());
        let mut replica = Self {
            dir: Some(dir),
            store: None,
            read: None,
        };
        replica.restart();
        replica
    }
    fn restart(&mut self) {
        self.read = None;
        self.store = None;
        let path = self.dir.as_ref().unwrap().path();
        let store = Store::open(path, Some(REPO)).unwrap();
        self.read = Some(Store::open_secondary(&store.root, &store.repo, &store.device).unwrap());
        self.store = Some(store);
    }
    fn store(&mut self) -> &mut Store {
        self.store.as_mut().unwrap()
    }
}

/// A device id drawn from `rng`, shaped like `core::id()`'s.
fn seeded_id(rng: &mut StdRng) -> String {
    let hex = format!("{:032x}", rng.r#gen::<u128>());
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

struct Fuzz {
    seed: u64,
    rng: StdRng,
    replicas: Vec<Replica>,
    allowed: BTreeSet<String>,
    /// One counter for every local operation of the seed, between
    /// activations and between prepare and commit: names made from it
    /// (`recovered-{serial}`) never repeat (fix round 1).
    serial: usize,
    log: Vec<String>,
    totals: Activation,
    /// Exchanging everything at the end: no concurrent local writes then
    /// (they would leave new events behind the last round).
    converging: bool,
}

impl Fuzz {
    fn new(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let count = rng.gen_range(2..=4);
        // Device ids from the seed. (Event ids are `core::id()`'s random
        // UUIDs, which a test cannot seed: a failing seed is not replayed
        // exactly, so a failure keeps its replicas on disk instead.)
        let mut ids = StdRng::seed_from_u64(seed ^ 0x0ff1_0c4c_0de5_eed5);
        let mut replicas: Vec<_> = (0..count)
            .map(|_| Replica::new(&seeded_id(&mut ids)))
            .collect();
        let allowed = replicas
            .iter_mut()
            .map(|r| r.store().device.clone())
            .collect();
        Self {
            seed,
            rng,
            replicas,
            allowed,
            serial: 0,
            log: vec![],
            totals: Activation::default(),
            converging: false,
        }
    }

    fn context(&self) -> String {
        let tail: Vec<_> = self.log.iter().rev().take(30).rev().cloned().collect();
        format!("seed {}, last steps:\n  {}", self.seed, tail.join("\n  "))
    }

    fn check(&mut self, r: usize, after: &str) {
        let diff = self.replicas[r].store().verify_derived_state_strict();
        match diff {
            Ok(diff) if diff.is_empty() => {}
            other => panic!(
                "replica {r} after {after}: oracle {other:#?}\n{}",
                self.context()
            ),
        }
        let quarantined: Vec<(String, String)> = self.replicas[r]
            .store()
            .db
            .prepare("SELECT id,error FROM incoming_events WHERE error IS NOT NULL")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        for (id, error) in &quarantined {
            assert!(
                error.contains("NAMESPACE_HISTORY_LIMIT")
                    || error.starts_with("QUARANTINED_CAUSAL_ANCESTOR"),
                "replica {r} after {after}: {id} quarantined: {error}\n{}",
                self.context()
            );
        }
    }

    fn step(&mut self) {
        let r = self.rng.gen_range(0..self.replicas.len());
        match self.rng.gen_range(0..100) {
            0..=49 => self.local(r),
            50..=89 => {
                let mut to = self.rng.gen_range(0..self.replicas.len() - 1);
                if to >= r {
                    to += 1;
                }
                self.deliver(r, to, false);
            }
            90..=94 => {
                self.log.push(format!("restart {r}"));
                self.replicas[r].restart();
                self.check(r, "restart");
            }
            _ => {
                let to = (r + 1) % self.replicas.len();
                self.deliver(r, to, true);
            }
        }
    }

    fn local(&mut self, r: usize) {
        self.serial += 1;
        let serial = self.serial;
        let what = local_op(&mut self.rng, self.replicas[r].store(), serial);
        if let Some(what) = what {
            self.log.push(format!("[{r}] {what}"));
            self.check(r, "local operation");
        }
    }

    /// Stage some (or all) of `from`'s missing shared events on `to`, in
    /// shuffled pages with duplicates, then activate off-lock.
    fn deliver(&mut self, from: usize, to: usize, everything: bool) {
        let source: Vec<Event> = self.replicas[from]
            .store()
            .events()
            .unwrap()
            .into_iter()
            .filter(|e| e.branch.shared)
            .collect();
        let target = self.replicas[to].store();
        let known: BTreeSet<String> = target
            .events()
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .chain(
                target
                    .db
                    .prepare("SELECT id FROM incoming_events")
                    .unwrap()
                    .query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .map(Result::unwrap),
            )
            .collect();
        let (known, mut events): (Vec<Event>, Vec<Event>) =
            source.into_iter().partition(|e| known.contains(&e.id));
        if !everything && !events.is_empty() && self.rng.gen_bool(0.7) {
            events.shuffle(&mut self.rng);
            let keep = self.rng.gen_range(1..=events.len());
            events.truncate(keep);
        }
        let duplicates = self.rng.gen_range(0..=2usize).min(known.len());
        events.extend(known.into_iter().take(duplicates));
        if events.is_empty() {
            return;
        }
        events.shuffle(&mut self.rng);
        while !events.is_empty() {
            let take = self.rng.gen_range(1..=events.len().min(4));
            let page: Vec<Event> = events.drain(..take).collect();
            self.log.push(format!(
                "stage {from}->{to} {}",
                page.iter()
                    .map(|e| e.id.as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
            let allowed = self.allowed.clone();
            let outcome = self.replicas[to]
                .store()
                .stage_incoming(page.into_iter().map(staged).collect(), &allowed)
                .unwrap();
            assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);
            if self.rng.gen_bool(0.6) {
                self.activate(to);
            }
        }
        self.activate(to);
    }

    /// Activate `r` off-lock; per ticket, compare with the inline commit on a
    /// copy, or race a local write, or just commit.
    fn activate(&mut self, r: usize) {
        let seed = self.rng.r#gen::<u64>();
        let mut rng = StdRng::seed_from_u64(seed);
        // The seed's one counter (not `serial * 1000`, which every call of
        // one delivery restarted, so a between-operation `recover` reused
        // `recovered-{serial}`: NAME_COLLISION).
        let mut serial = self.serial;
        let quiet = self.converging;
        let replica = &mut self.replicas[r];
        let read = replica.read.take().unwrap();
        let store = replica.store.as_mut().unwrap();
        let mut writes = vec![];
        let mut between = |_: &SlowTicket, store: &mut Store| -> Between {
            match rng.gen_range(0..100) {
                0..=59 => Between::Compare,
                n if n <= 79 || quiet => Between::Nothing,
                _ => {
                    serial += 1;
                    let what = local_op(&mut rng, store, serial);
                    writes.push(format!("[{r}] between prepare and commit: {what:?}"));
                    Between::Wrote(what.is_some())
                }
            }
        };
        let stats = activate(store, &read, &mut between);
        replica.read = Some(read);
        self.serial = serial;
        self.log.extend(writes);
        self.log.push(format!("activate {r}: {stats:?}"));
        self.totals.add(&stats);
        self.check(r, "activation");
    }

    /// Exchange everything until every replica holds the same events, then
    /// require identical projections and conflict rows everywhere.
    fn converge(&mut self) {
        self.converging = true;
        for _ in 0..4 {
            for from in 0..self.replicas.len() {
                for to in 0..self.replicas.len() {
                    if from != to {
                        self.deliver(from, to, true);
                    }
                }
            }
        }
        let mut reference: Option<(BTreeSet<String>, Projection, Vec<Conflict>)> = None;
        for r in 0..self.replicas.len() {
            let s = self.replicas[r].store();
            let refused: i64 =
                s.db.query_row(
                    "SELECT count(*) FROM incoming_events WHERE error IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let staged: i64 =
                s.db.query_row("SELECT count(*) FROM incoming_events", [], |row| row.get(0))
                    .unwrap();
            assert!(
                staged == 0 || refused > 0,
                "replica {r} did not drain\n{}",
                self.context()
            );
            if refused > 0 {
                continue;
            }
            let state = (
                s.events().unwrap().into_iter().map(|e| e.id).collect(),
                s.projection(&s.active).unwrap(),
                s.conflicts().unwrap(),
            );
            match &reference {
                None => reference = Some(state),
                Some(reference) => {
                    let context = self.context();
                    assert_eq!(
                        reference.0, state.0,
                        "replica {r} events
{context}"
                    );
                    assert_eq!(
                        reference.1, state.1,
                        "replica {r} projection
{context}"
                    );
                    assert_eq!(
                        reference.2, state.2,
                        "replica {r} conflict rows
{context}"
                    );
                }
            }
        }
        for r in 0..self.replicas.len() {
            let s = self.replicas[r].store();
            let active = s.active.clone();
            assert_snapshot_valid(s, &active);
        }
    }
}

impl Drop for Fuzz {
    /// A failing seed is not replayed exactly (its event ids are random):
    /// keep its replicas' directories for a post-mortem.
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        for (r, replica) in self.replicas.iter_mut().enumerate() {
            if let Some(dir) = replica.dir.take() {
                eprintln!(
                    "seed {}: replica {r} kept at {}",
                    self.seed,
                    dir.keep().display()
                );
            }
        }
    }
}

fn run_seed(seed: u64, steps: std::ops::RangeInclusive<usize>) -> Activation {
    let mode = if seed.is_multiple_of(3) {
        ConflictMode::Verify
    } else {
        ConflictMode::Append
    };
    let _mode = ModeGuard::new(mode);
    let _checks = ChecksGuard::new();
    let mut fuzz = Fuzz::new(seed);
    let steps = fuzz.rng.gen_range(steps);
    for _ in 0..steps {
        fuzz.step();
    }
    fuzz.converge();
    fuzz.totals
}

fn run_seeds(seeds: impl Iterator<Item = u64>, steps: std::ops::RangeInclusive<usize>) {
    let mut total = Activation::default();
    let started = std::time::Instant::now();
    let mut count = 0;
    for seed in seeds {
        total.add(&run_seed(seed, steps.clone()));
        count += 1;
    }
    println!(
        "OFFLOCK_FUZZ {count} seeds in {} ms: {total:?}",
        started.elapsed().as_millis()
    );
    assert!(
        total.compared >= count * 2,
        "too few prepared commits compared: {total:?}"
    );
    assert!(total.stale > 0, "no stale ticket exercised: {total:?}");
    assert!(total.committed_slow > 0, "{total:?}");
}

/// The differential fuzz (module docs), debug size.
#[test]
fn prepared_commits_equal_inline_commits_and_full_replay() {
    let seeds: u64 = std::env::var("TKFS_OFFLOCK_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    run_seeds(0..seeds, 20..=60);
}

/// The acceptance run: 300 seeds in release
/// (`cargo test --release --test offlock_commit -- --ignored`), seeds
/// 1000–1299. `TKFS_OFFLOCK_START` and `TKFS_OFFLOCK_COUNT` select a shard
/// (for example four processes of 75 seeds each).
#[test]
#[ignore = "release: 300 seeds"]
fn prepared_commits_300_seeds_release() {
    let env = |name: &str, default: u64| -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let start = env("TKFS_OFFLOCK_START", 1000);
    let count = env("TKFS_OFFLOCK_COUNT", 300);
    run_seeds(start..start + count, 30..=90);
}

/// A two-way edit on B: B's unseen local save and A's concurrent save of the
/// same file. Returns (A, B, A's event, allowed devices).
fn two_way(dir: &Path) -> (Store, Store, Event, BTreeSet<String>) {
    let repo = id();
    let mut a = Store::initialize(&dir.join("a"), &repo, &id()).unwrap();
    let mut b = Store::initialize(&dir.join("b"), &repo, &id()).unwrap();
    let allowed = devices([&a, &b]);
    a.create("shared.txt", "file").unwrap();
    b.receive(
        Bundle {
            repo: repo.clone(),
            branches: vec![],
            events: missing(&a, &b),
            objects: BTreeMap::new(),
        },
        &allowed,
    )
    .unwrap();
    write(&mut a, "shared.txt", b"from a");
    write(&mut b, "shared.txt", b"from b");
    let event = missing(&a, &b).remove(0);
    (a, b, event, allowed)
}

/// A local commit between prepare and commit moves the branch: the ticket is
/// `Stale`, nothing of the computation is written, the row stays staged, and
/// after `requeue` it commits off-lock exactly as an inline activation of
/// the same state does.
#[test]
fn stale_when_the_branch_moves_between_prepare_and_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (_a, mut b, event, allowed) = two_way(dir.path());
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    b.stage_incoming(vec![staged(event.clone())], &allowed)
        .unwrap();
    let ticket = b
        .activate_slice(SliceBudget::ACTIVATOR)
        .unwrap()
        .slow_ticket
        .expect("a two-way save is slow");
    let prepared = Store::prepare_slow(&read, &ticket).unwrap();
    b.create("local-between.txt", "file").unwrap();
    let tables = |s: &Store| -> Vec<Vec<Vec<String>>> {
        TABLES
            .iter()
            .filter(|(name, _)| !name.starts_with("incoming"))
            .map(|(_, sql)| table(s, sql))
            .collect()
    };
    let before = tables(&b);
    assert_eq!(b.commit_prepared(prepared).unwrap(), CommitOutcome::Stale);
    assert_eq!(before, tables(&b), "a stale commit writes nothing");
    assert!(!b.events().unwrap().iter().any(|e| e.id == event.id));
    assert_eq!(
        b.db.query_row(
            "SELECT state FROM incoming_events WHERE id=?",
            [&event.id],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "ready"
    );
    b.requeue(&ticket.id, 0).unwrap();
    let (_reference_dir, mut reference) = copy_of(&b);
    let stats = activate(&mut b, &read, &mut |_, _| Between::Nothing);
    assert_eq!((stats.tickets, stats.stale), (1, 0), "{stats:?}");
    assert!(b.events().unwrap().iter().any(|e| e.id == event.id));
    assert_oracle_empty(&mut b, "after the re-queued commit");
    // The same state activated inline.
    while reference
        .activate_slice(SliceBudget::UNBOUNDED)
        .unwrap()
        .more
    {}
    let differences = table_differences(&b, &reference);
    assert!(
        differences.is_empty(),
        "{}",
        differences.join(
            "
"
        )
    );
}

/// Stage `event` on `b`, issue its ticket and prepare it off-lock.
fn prepare_one(
    b: &mut Store,
    read: &Store,
    event: &Event,
    allowed: &BTreeSet<String>,
) -> (SlowTicket, Prepared) {
    b.stage_incoming(vec![staged(event.clone())], allowed)
        .unwrap();
    let ticket = b
        .activate_slice(SliceBudget::ACTIVATOR)
        .unwrap()
        .slow_ticket
        .expect("a slow event");
    assert_eq!(ticket.id, event.id);
    let prepared = Store::prepare_slow(read, &ticket).unwrap();
    assert!(matches!(&prepared, Prepared::Computed(p) if p.is_computed()));
    (ticket, prepared)
}

#[track_caller]
fn assert_refused(outcome: CommitOutcome, code: &str) {
    match outcome {
        CommitOutcome::Failed(error) => assert_eq!(error.code, code, "{error:?}"),
        other => panic!("expected {code}, got {other:?}"),
    }
}

/// `commit_prepared` re-runs the commit validations that read global tables
/// (I7, invariants 6 and 7): a size recorded for the event's object, a
/// different payload committed under its id, or a changed branch descriptor
/// between prepare and commit quarantines the event; nothing is committed.
#[test]
fn size_change_between_prepare_and_commit_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (_a, mut b, event, allowed) = two_way(dir.path());
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let (object, size) = event.object_sizes.iter().next().expect("a sized save");
    let (_ticket, prepared) = prepare_one(&mut b, &read, &event, &allowed);
    b.db.execute(
        "INSERT INTO object_sizes VALUES(?,?)",
        rusqlite::params![object, size + 1],
    )
    .unwrap();
    assert_refused(b.commit_prepared(prepared).unwrap(), "OBJECT_SIZE_CHANGED");
    assert!(!b.events().unwrap().iter().any(|e| e.id == event.id));
    assert_eq!(b.inbox_counts().unwrap().quarantined, 1);
    assert_oracle_empty(&mut b, "after the refusal");
}

#[test]
fn event_id_reuse_and_descriptor_change_between_prepare_and_commit_rejected() {
    // A different payload under the event's id, committed on another branch
    // (the event's own branch does not move, so the ticket is not stale).
    let dir = tempfile::tempdir().unwrap();
    let (_a, mut b, event, allowed) = two_way(dir.path());
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let (_ticket, prepared) = prepare_one(&mut b, &read, &event, &allowed);
    let mut other = event.clone();
    other.branch.id = "elsewhere".into();
    other.created_ms += 1;
    b.db.execute(
        "INSERT INTO events VALUES(?,?,?)",
        rusqlite::params![
            other.id,
            other.branch.id,
            serde_json::to_string(&other).unwrap()
        ],
    )
    .unwrap();
    assert_refused(b.commit_prepared(prepared).unwrap(), "EVENT_ID_REUSED");
    b.db.execute(
        "DELETE FROM events WHERE id=? AND branch='elsewhere'",
        [&other.id],
    )
    .unwrap();
    // The branch descriptor changed under the event.
    let dir = tempfile::tempdir().unwrap();
    let (_a, mut b, event, allowed) = two_way(dir.path());
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let (_ticket, prepared) = prepare_one(&mut b, &read, &event, &allowed);
    b.db.execute(
        "UPDATE branches SET name='renamed' WHERE id=?",
        [&event.branch.id],
    )
    .unwrap();
    assert_refused(
        b.commit_prepared(prepared).unwrap(),
        "BRANCH_DESCRIPTOR_CHANGED",
    );
    assert!(!b.events().unwrap().iter().any(|e| e.id == event.id));
    b.db.execute(
        "UPDATE branches SET name=? WHERE id=?",
        [&event.branch.name, &event.branch.id],
    )
    .unwrap();
    assert_oracle_empty(&mut b, "after the refusals");
}

/// A refusal of the slow path found off-lock (here `INVALID_CONFLICT_REVIEW`:
/// the event reviews a record that does not exist) is applied under the
/// lock exactly as the inline commit applies it.
#[test]
fn refusals_found_off_lock_are_applied_as_inline() {
    let dir = tempfile::tempdir().unwrap();
    let (a, mut b, save, allowed) = two_way(dir.path());
    let entry = a.lookup("shared.txt").unwrap();
    let branch = a.branch(&a.active).unwrap();
    let mut event = a.event(
        &branch,
        vec![
            a.current_change(
                &entry.id,
                "location",
                json!({"parent": ROOT, "name": "moved.txt"}),
            )
            .unwrap(),
        ],
    );
    event.reviewed = vec!["no-such-record".into()];
    // A's save first (its parent), then the review.
    b.stage_incoming(vec![staged(save), staged(event.clone())], &allowed)
        .unwrap();
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let stats = activate(&mut b, &read, &mut |_, _| Between::Compare);
    assert_eq!((stats.compared, stats.failed), (2, 1), "{stats:?}");
    let error: String =
        b.db.query_row(
            "SELECT error FROM incoming_events WHERE id=?",
            [&event.id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(error.contains("INVALID_CONFLICT_REVIEW"), "{error}");
    assert_oracle_empty(&mut b, "after the refusal");
}

/// The lock holder's part does no derivation: projecting, deriving and
/// serializing happen in `prepare_slow` (counted there), `commit_prepared`
/// only writes. The slow commit's derivation is counted once.
#[test]
fn commit_prepared_derives_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (_a, mut b, event, allowed) = two_way(dir.path());
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let start = thread_derive_counters();
    let (_ticket, prepared) = prepare_one(&mut b, &read, &event, &allowed);
    let prepared_counters = thread_derive_counters();
    let derived = |c: DeriveCounters| c.append + c.full + c.fallbacks + c.verified;
    assert_eq!(derived(prepared_counters) - derived(start), 1);
    assert!(
        b.commit_prepared(prepared)
            .unwrap()
            .eq(&CommitOutcome::Committed {
                id: event.id.clone(),
                slow: true
            })
    );
    let after = thread_derive_counters();
    assert_eq!(derived(after), derived(prepared_counters));
    assert_eq!(after.commit_micros, prepared_counters.commit_micros);
    assert_eq!(b.commit_path_counters().slow, 1);
    assert_snapshot_valid(&b, &event.branch.id);
    assert_oracle_empty(&mut b, "after the prepared commit");
}

/// The event cache is `events()` filtered by branch, in the same order,
/// after commits, on a read handle, across branches, across a rolled-back
/// transaction whose sequence numbers are reused, and after deletions.
#[test]
fn event_cache_equals_events_after_inserts_and_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some(REPO)).unwrap();
    let read = Store::open_secondary(&s.root, &s.repo, &s.device).unwrap();
    let check = |s: &Store, step: &str| {
        let events = s.events().unwrap();
        for branch in s.branches().unwrap() {
            let expected: Vec<Event> = events
                .iter()
                .filter(|e| e.branch.id == branch.id)
                .cloned()
                .collect();
            assert_eq!(
                s.cached_branch_events(&branch.id).unwrap(),
                expected,
                "{step}: branch {}",
                branch.name
            );
        }
    };
    check(&s, "empty");
    for i in 0..20 {
        s.create(&format!("f-{i:02}"), "file").unwrap();
        if i % 5 == 0 {
            check(&s, "commits");
            check(&read, "read handle");
        }
    }
    let main = s.active.clone();
    s.fork("side", true).unwrap();
    s.checkout("side").unwrap();
    s.create("on-side", "file").unwrap();
    check(&s, "two branches");
    check(&read, "read handle, two branches");
    s.checkout(&main).unwrap();
    // A rolled-back transaction: its sequence numbers are reused.
    s.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    s.create("rolled-back", "file").unwrap();
    check(&s, "inside a transaction");
    s.db.execute_batch("ROLLBACK").unwrap();
    s.create("reuses-the-sequence", "file").unwrap();
    check(&s, "after a rollback");
    check(&read, "read handle after a rollback");
    // Deletions (repair tools, evacuation) reload.
    let victim = s
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.branch.id == main)
        .unwrap();
    s.db.execute("DELETE FROM events WHERE id=?", [&victim.id])
        .unwrap();
    check(&s, "after a deletion");
    check(&read, "read handle after a deletion");
    s.create("after-the-deletion", "file").unwrap();
    check(&s, "after a deletion and a commit");
}

/// `events()` filtered by branch: what the event cache must hold.
fn branch_events(s: &Store, branch: &str) -> Vec<Event> {
    s.events()
        .unwrap()
        .into_iter()
        .filter(|e| e.branch.id == branch)
        .collect()
}

/// The journal's newest sequence number.
fn journal_tip(s: &Store) -> i64 {
    s.db.query_row(
        "SELECT coalesce(max(seq),0) FROM journal_sequence",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

/// WP2.4 fix round 1 (blocker): a write transaction commits a row on another
/// branch, uses `main`'s cache (which reads that row), and rolls back; the
/// next commit, on `main`, takes the freed sequence number. The cache must
/// see that commit (every row read inside a transaction is tentative,
/// whatever its branch), and the slow commits computed from it must keep it:
/// an inline slow commit of a concurrent remote event and a local one, also
/// after a restart. Before the fix the cache missed `y.txt`: the remote
/// event's slow commit stored and sealed a projection without it, and the
/// local slow save failed with `CAUSAL_PREREQUISITES_PENDING`.
#[test]
fn event_cache_sees_a_sequence_reused_after_another_branchs_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let file = a.create("base.txt", "file").unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    let before = b.shared_event_inventory().unwrap();
    a.create("remote.txt", "file").unwrap();
    let remote = a.shared_bundle(&before).unwrap().events;
    let main = b.active.clone();
    b.fork("side", false).unwrap();
    b.checkout("side").unwrap();
    b.create("s1", "file").unwrap();
    b.checkout(&main).unwrap();
    assert_eq!(
        b.cached_branch_events(&main).unwrap(),
        branch_events(&b, &main)
    );
    // The transaction: a row of `side`, read by `main`'s cache, rolled back.
    b.checkout("side").unwrap();
    b.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    b.create("s2", "file").unwrap();
    let freed = journal_tip(&b);
    assert_eq!(
        b.cached_branch_events(&main).unwrap(),
        branch_events(&b, &main),
        "inside the transaction"
    );
    b.db.execute_batch("ROLLBACK").unwrap();
    assert_eq!(journal_tip(&b), freed - 1);
    b.checkout(&main).unwrap();
    b.create("y.txt", "file").unwrap();
    assert_eq!(
        journal_tip(&b),
        freed,
        "y.txt took the freed sequence number"
    );
    assert_eq!(
        b.cached_branch_events(&main).unwrap(),
        branch_events(&b, &main),
        "after the reused sequence number"
    );
    // An inline slow commit of A's concurrent create (B's y.txt is unseen).
    let slow = b.commit_path_counters().slow;
    b.receive(
        Bundle {
            repo: repo.clone(),
            branches: vec![],
            events: remote,
            objects: BTreeMap::new(),
        },
        &allowed,
    )
    .unwrap();
    assert_eq!(b.commit_path_counters().slow, slow + 1, "a slow commit");
    b.lookup("y.txt").unwrap();
    b.lookup("remote.txt").unwrap();
    assert_oracle_empty(&mut b, "after the remote slow commit");
    assert!(assert_snapshot_valid(&b, &main), "sealed at the tip");
    // A local slow commit: two saves from one base.
    let entry = b.entry(&file.id).unwrap();
    let (content, alive) = (
        entry.revisions.get("content").cloned(),
        entry.revisions.get("alive").cloned(),
    );
    b.write_with_base(&file.id, content.clone(), alive.clone(), b"one")
        .unwrap();
    b.write_with_base(&file.id, content, alive, b"two").unwrap();
    assert_eq!(
        b.commit_path_counters().slow,
        slow + 2,
        "a local slow commit"
    );
    assert_eq!(
        b.cached_branch_events(&main).unwrap(),
        branch_events(&b, &main)
    );
    assert_oracle_empty(&mut b, "after the local slow commit");
    // The sealed projection is the one a restart resumes.
    let root = b.root.clone();
    drop(b);
    let mut b = Store::open(&root, Some(&repo)).unwrap();
    b.lookup("y.txt").unwrap();
    b.lookup("remote.txt").unwrap();
    assert_oracle_empty(&mut b, "after a restart");
}

/// WP2.4 fix round 1: the event cache keeps several branches, and a branch
/// loaded inside a write transaction is kept (its rows above the last
/// committed tip are tentative), so alternating uses of two branches, inside
/// and outside transactions, load each branch once. A rolled-back
/// transaction whose sequence numbers are reused still reloads both.
#[test]
fn event_cache_keeps_branches_and_loads_made_inside_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some(REPO)).unwrap();
    let main = s.active.clone();
    for i in 0..5 {
        s.create(&format!("m{i}"), "file").unwrap();
    }
    let side = s.fork("side", true).unwrap().id;
    s.checkout(&side).unwrap();
    for i in 0..5 {
        s.create(&format!("s{i}"), "file").unwrap();
    }
    s.checkout(&main).unwrap();
    // A fresh handle: empty caches.
    let root = s.root.clone();
    drop(s);
    let mut s = Store::open(&root, Some(REPO)).unwrap();
    let check = |s: &Store, branch: &str, step: &str| {
        assert_eq!(
            s.cached_branch_events(branch).unwrap(),
            branch_events(s, branch),
            "{step}: branch {branch}"
        );
    };
    let loads = s.event_cache_loads();
    // Loaded inside a transaction, then used alternately: one load each.
    s.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    check(&s, &main, "first use, in a transaction");
    check(&s, &side, "first use, in a transaction");
    for i in 0..4 {
        s.create(&format!("t{i}"), "file").unwrap();
        check(&s, &main, "in a transaction");
        check(&s, &side, "in a transaction");
    }
    s.db.execute_batch("COMMIT").unwrap();
    check(&s, &side, "after the commit");
    check(&s, &main, "after the commit");
    for round in 0..3 {
        s.db.execute_batch("BEGIN IMMEDIATE").unwrap();
        s.create(&format!("r{round}"), "file").unwrap();
        check(&s, &side, "in a later transaction");
        check(&s, &main, "in a later transaction");
        s.db.execute_batch("COMMIT").unwrap();
        s.create(&format!("a{round}"), "file").unwrap();
        check(&s, &main, "in autocommit");
        check(&s, &side, "in autocommit");
    }
    assert_eq!(s.event_cache_loads() - loads, 2, "each branch loaded once");
    // A rolled-back row of `side`, its sequence number reused on `main`.
    s.checkout(&side).unwrap();
    s.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    s.create("rolled-back", "file").unwrap();
    check(&s, &main, "before the rollback");
    check(&s, &side, "before the rollback");
    s.db.execute_batch("ROLLBACK").unwrap();
    s.checkout(&main).unwrap();
    s.create("reuses-the-sequence", "file").unwrap();
    check(&s, &main, "after the rollback");
    check(&s, &side, "after the rollback");
    assert_eq!(
        s.event_cache_loads() - loads,
        4,
        "the rollback reloads both branches"
    );
}

/// WP2.4 fix round 1 (the reviewer's case): an activation slice commits
/// remote namespace successors on two shared branches in turn, each
/// counting its competing cuts over its branch's cached events, inside the
/// slice's write transaction. Each branch is loaded once (kept across the
/// transaction and the next slices), not twice per slice.
#[test]
fn activation_slices_alternating_branches_load_each_branch_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(REPO)).unwrap();
    let main = a.active.clone();
    for i in 0..10 {
        a.create(&format!("m{i}"), "file").unwrap();
    }
    let side = a.fork("side", true).unwrap().id;
    a.checkout(&side).unwrap();
    for i in 0..10 {
        a.create(&format!("s{i}"), "file").unwrap();
    }
    a.checkout(&main).unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    let loads = b.event_cache_loads();
    for round in 0..3 {
        let before = b.shared_event_inventory().unwrap();
        // Renames in place, alternating branches: namespace successors.
        for i in 0..8 {
            for (branch, prefix) in [(&main, "m"), (&side, "s")] {
                a.checkout(branch).unwrap();
                let name = format!("{prefix}{i}{}", "x".repeat(round));
                let entry = a.lookup(&name).unwrap();
                a.rename(&entry.id, &format!("{name}x"), false).unwrap();
            }
        }
        let mut events = a.shared_bundle(&before).unwrap().events;
        causal_sort(&mut events);
        b.stage_incoming(events.into_iter().map(staged).collect(), &allowed)
            .unwrap();
        while b
            .activate_slice(SliceBudget::ACTIVATOR)
            .map(|slice| {
                assert!(slice.slow_ticket.is_none(), "successors only");
                slice.more
            })
            .unwrap()
        {}
        assert_eq!(b.inbox_counts().unwrap().pending_active(), 0);
    }
    for branch in [&main, &side] {
        assert_eq!(
            b.cached_branch_events(branch).unwrap(),
            branch_events(&b, branch)
        );
    }
    assert_eq!(
        b.event_cache_loads() - loads,
        2,
        "one load per branch for 48 alternating successors"
    );
    assert_oracle_empty(&mut b, "after the slices");
}

/// WP2.4 fix round 1: a commit validation that refuses the event on the
/// read store (a size recorded for its object after the ticket was issued
/// and before the preparation) is decided by the re-run under the lock:
/// `commit_prepared` quarantines the event at once instead of returning
/// `Stale` (a wasted round and a backoff).
#[test]
fn validation_refused_off_lock_is_decided_under_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let (_a, mut b, event, allowed) = two_way(dir.path());
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let (object, size) = event.object_sizes.iter().next().expect("a sized save");
    b.stage_incoming(vec![staged(event.clone())], &allowed)
        .unwrap();
    let ticket = b
        .activate_slice(SliceBudget::ACTIVATOR)
        .unwrap()
        .slow_ticket
        .expect("a slow event");
    b.db.execute(
        "INSERT INTO object_sizes VALUES(?,?)",
        rusqlite::params![object, size + 1],
    )
    .unwrap();
    let prepared = Store::prepare_slow(&read, &ticket).unwrap();
    let Prepared::Computed(computed) = &prepared else {
        panic!("prepare_slow computes off-lock");
    };
    assert!(!computed.is_computed(), "refused on the read store");
    assert_eq!(computed.event(), Some(&event));
    assert_refused(b.commit_prepared(prepared).unwrap(), "OBJECT_SIZE_CHANGED");
    assert!(!b.events().unwrap().iter().any(|e| e.id == event.id));
    assert_eq!(b.inbox_counts().unwrap().quarantined, 1);
    assert_oracle_empty(&mut b, "after the refusal");
}

/// WP2.4 fix round 1: a slow event on a shared branch whose newest row is
/// far below the journal's tip (other branches committed since). The
/// ticket's `base_seq` is the branch's newest sequence (read from its
/// frontier, without walking the journal from its tip); the commit is not
/// stale, and its forced seal is written from the parts computed off-lock
/// (the check that the branch's journal is the hashed prefix plus the event
/// also reads the frontier). The seal is valid and the oracle empty.
#[test]
fn side_branch_slow_commit_far_below_the_tip_seals_from_prepared_parts() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::initialize(&dir.path().join("a"), &repo, &id()).unwrap();
    let mut b = Store::initialize(&dir.path().join("b"), &repo, &id()).unwrap();
    let allowed = devices([&a, &b]);
    let main = a.active.clone();
    let side = a.fork("side", true).unwrap();
    a.checkout(&side.id).unwrap();
    a.create("side.txt", "file").unwrap();
    let deliver = |from: &Store, to: &mut Store| {
        to.receive(
            Bundle {
                repo: repo.clone(),
                branches: vec![],
                events: missing(from, to),
                objects: BTreeMap::new(),
            },
            &allowed,
        )
        .unwrap();
    };
    deliver(&a, &mut b);
    b.checkout(&side.id).unwrap();
    // Concurrent saves of side.txt; B's stays unseen by A.
    write(&mut a, "side.txt", b"from a");
    write(&mut b, "side.txt", b"from b");
    let event = missing(&a, &b).remove(0);
    // Many rows of `main` above `side`'s newest.
    b.checkout(&main).unwrap();
    for i in 0..60 {
        b.create(&format!("main-{i}"), "file").unwrap();
    }
    let newest: i64 =
        b.db.query_row(
            "SELECT max(s.seq) FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=?",
            [&side.id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(journal_tip(&b) >= newest + 60);
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let (ticket, prepared) = prepare_one(&mut b, &read, &event, &allowed);
    assert_eq!(ticket.base_seq, newest, "the ticket's branch sequence");
    let Prepared::Computed(computed) = &prepared else {
        unreachable!()
    };
    assert_eq!(
        computed.base_seq(),
        newest,
        "the preparation's branch sequence"
    );
    let seals = b.prepared_seals();
    assert_eq!(
        b.commit_prepared(prepared).unwrap(),
        CommitOutcome::Committed {
            id: event.id.clone(),
            slow: true
        }
    );
    assert_eq!(
        b.prepared_seals(),
        seals + 1,
        "sealed from the prepared parts"
    );
    assert!(
        assert_snapshot_valid(&b, &side.id),
        "sealed at the side's tip"
    );
    assert_oracle_empty(&mut b, "after the side commit");
}

/// The rows a slow commit leaves for its projection: `current_state`, how
/// many `current_parts` rows, and how many entities the projection has.
fn stored_layout(store: &Store) -> (String, usize, usize) {
    let payload: String = store
        .db
        .query_row(
            "SELECT payload FROM current_state WHERE branch=?",
            [&store.active],
            |r| r.get(0),
        )
        .unwrap();
    let parts: i64 = store
        .db
        .query_row(
            "SELECT count(*) FROM current_parts WHERE branch=?",
            [&store.active],
            |r| r.get(0),
        )
        .unwrap();
    let entities = store.projection(&store.active).unwrap().entries.len();
    (payload, parts as usize, entities)
}

/// CCR-WP1.INT-4 through the off-lock path: prepared slow commits keep
/// writing the `current_parts` delta (not the whole projection), fold the
/// parts back with a full rewrite (serialized off-lock) as the entries grow,
/// seal from the projection they hold, and survive a restart.
#[test]
fn prepared_slow_commits_store_their_projection_as_a_delta() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    linear_history(&mut a, 60).unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    // B's unseen local edit: every event from A is concurrent with it.
    b.create("b-local-dir", "directory").unwrap();
    let before = b.shared_event_inventory().unwrap();
    for i in 0..30 {
        a.create(&format!("ns-dir-{i:02}"), "directory").unwrap();
        let file = a.lookup(&format!("lin-{i:06}.txt")).unwrap();
        a.rename(&file.id, &format!("ns-dir-{i:02}/moved-{i:02}.txt"), false)
            .unwrap();
    }
    let mut events = a.shared_bundle(&before).unwrap().events;
    causal_sort(&mut events);
    assert_eq!(events.len(), 60);
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let (mut state, _, _) = stored_layout(&b);
    let (mut deltas, mut rewrites) = (0, 0);
    for event in events {
        b.stage_incoming(vec![staged(event)], &allowed).unwrap();
        let slow = b.commit_path_counters().slow;
        let stats = activate(&mut b, &read, &mut |_, _| Between::Nothing);
        assert_eq!(stats.committed_slow, 1, "{stats:?}");
        assert_eq!(b.commit_path_counters().slow, slow + 1);
        let (now, parts, entities) = stored_layout(&b);
        if now == state {
            deltas += 1;
            assert!(parts > 0, "a delta writes parts");
        } else {
            rewrites += 1;
            assert_eq!(parts, 0, "a full rewrite folds the parts back");
            state = now;
        }
        assert!(
            parts <= entities / 2,
            "{parts} parts for {entities} entities"
        );
        assert!(
            assert_snapshot_valid(&b, &b.active.clone()),
            "forced seal at the tip"
        );
        assert_oracle_empty(&mut b, "after a prepared slow commit");
    }
    assert!(
        deltas >= 40 && rewrites >= 1,
        "{deltas} delta commits, {rewrites} full rewrites"
    );
    let root = b.root.clone();
    drop(read);
    drop(b);
    let mut b = Store::open(&root, Some(&repo)).unwrap();
    assert!(b.lookup("ns-dir-29/moved-29.txt").is_ok());
    assert!(b.lookup("b-local-dir").is_ok());
    assert_oracle_empty(&mut b, "after a restart");
}

/// CCR-WP1.6-1 adapter: a periodic seal of a fast commit serializes the
/// projection the commit holds (the patched view) instead of reading it back;
/// the sealed payload is what a seal from the stored projection gives (a
/// valid snapshot of the tip) and the next open resumes from it.
#[test]
fn periodic_seals_of_fast_commits_use_the_projection_they_hold() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some(REPO)).unwrap();
    let entry = s.create("file", "file").unwrap();
    let seals = s.known_projection_seals();
    let mut sealed_at_tip = 0;
    for i in 0..300 {
        let current = s.entry(&entry.id).unwrap();
        s.write_revision(
            &entry.id,
            current.revisions.get("content").cloned(),
            format!("v{i}").as_bytes(),
        )
        .unwrap();
        if i % 7 == 0 {
            s.create(&format!("other-{i}"), "file").unwrap();
        }
        let active = s.active.clone();
        if assert_snapshot_valid(&s, &active) {
            sealed_at_tip += 1;
        }
    }
    assert!(
        s.known_projection_seals() >= seals + 2,
        "{} known-projection seals",
        s.known_projection_seals() - seals
    );
    assert!(sealed_at_tip >= 2, "{sealed_at_tip}");
    let expected = s.projection(&s.active).unwrap();
    drop(s);
    let s = Store::open(dir.path(), Some(REPO)).unwrap();
    assert_eq!(s.projection(&s.active).unwrap(), expected);
}

const CRASH_DIR: &str = "TKFS_OFFLOCK_CRASH_DIR";

/// Child: open the staged store at `TKFS_OFFLOCK_CRASH_DIR` without
/// activating, prepare its slow event off-lock and commit it; `TKFS_FAULT`
/// ends the process (exit 86) at that point. A no-op unless the variable is
/// set.
#[test]
fn crash_child() {
    let Ok(root) = std::env::var(CRASH_DIR) else {
        return;
    };
    let mut s = Store::open_existing_with(
        Path::new(&root),
        OpenOptions {
            defer_activation: true,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let read = Store::open_secondary(&s.root, &s.repo, &s.device).unwrap();
    let ticket = s
        .activate_slice(SliceBudget::ACTIVATOR)
        .unwrap()
        .slow_ticket
        .unwrap();
    let prepared = Store::prepare_slow(&read, &ticket).unwrap();
    s.commit_prepared(prepared).unwrap();
}

/// A crash at any point of a prepared commit (before its first statement,
/// before and after the seal is written, after the savepoint is released
/// inside the not yet committed transaction) leaves the state as it was:
/// the event still staged, every derived table and the startup snapshot
/// unchanged. The event then commits off-lock and the next open resumes.
#[test]
fn crashes_during_a_prepared_commit_keep_the_committed_state() {
    for point in [
        "before_sql",
        "before_startup_snapshot",
        "startup_snapshot_written",
        "sql_committed",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (_a, b, event, allowed) = two_way(dir.path());
        b.stage_incoming(vec![staged(event.clone())], &allowed)
            .unwrap();
        let root = b.root.clone();
        drop(b);
        // Opening resumes the startup snapshot (folding `current_parts` and
        // sealing the replayed suffix); the child opens the same way, so the
        // state to compare with is that of an open.
        let opened = |root: &Path| {
            Store::open_existing_with(
                root,
                OpenOptions {
                    defer_activation: true,
                    ..OpenOptions::default()
                },
            )
            .unwrap()
        };
        // An open may also fold `current_parts` into `current_state`: the
        // stored projection is compared, not its layout.
        let state = |b: &Store| -> (Projection, Vec<Vec<Vec<String>>>) {
            (
                b.projection(&event.branch.id).unwrap(),
                TABLES
                    .iter()
                    .filter(|(name, _)| !name.starts_with("current_"))
                    .map(|(_, sql)| table(b, sql))
                    .collect(),
            )
        };
        let b = opened(&root);
        let before = state(&b);
        drop(b);
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "crash_child",
                "--exact",
                "--nocapture",
                "--test-threads",
                "1",
            ])
            .env(CRASH_DIR, &root)
            .env("TKFS_FAULT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut b = opened(&root);
        let after = state(&b);
        assert_eq!(before.0, after.0, "{point}: the projection changed");
        let names = TABLES
            .iter()
            .filter(|(name, _)| !name.starts_with("current_"));
        for (((name, _), before), after) in names.zip(&before.1).zip(&after.1) {
            assert_eq!(before, after, "{point}: {name} changed by the crash");
        }
        assert!(!b.events().unwrap().iter().any(|e| e.id == event.id));
        let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
        let stats = activate(&mut b, &read, &mut |_, _| Between::Nothing);
        assert_eq!(stats.committed_slow, 1, "{point}: {stats:?}");
        assert!(assert_snapshot_valid(&b, &event.branch.id), "{point}");
        assert_oracle_empty(&mut b, point);
        let expected = b.projection(&event.branch.id).unwrap();
        drop(read);
        drop(b);
        let b = Store::open(&root, None).unwrap();
        assert_eq!(b.projection(&event.branch.id).unwrap(), expected, "{point}");
    }
}

/// `count` concurrent renames of `entity` (to fresh root names), authored by
/// fake devices from `s`'s current state: mutually concurrent location
/// changes, each parented on the entity's current location head.
fn concurrent_renames(s: &Store, entity: &str, prefix: &str, count: usize) -> Vec<Event> {
    let branch = s.branch(&s.active).unwrap();
    (0..count)
        .map(|k| {
            let change = s
                .current_change(
                    entity,
                    "location",
                    json!({"parent": ROOT, "name": format!("{prefix}-{k}")}),
                )
                .unwrap();
            let mut event = s.event(&branch, vec![change]);
            event.device = format!("device-{prefix}-{k}");
            event
        })
        .collect()
}

/// Two directories D1 and D2 each renamed concurrently by `left` and `right`
/// devices (then both register conflicts resolved), and a file X in D1. Every
/// cone of the history stays within `MAX_NAMESPACE_CUTS` (2^left - 1 - left
/// and 2^right - 1 - right competing cuts). Moving X into D2 joins both
/// cones: 2^(left+right) - 1 - (left+right) competing cuts.
fn joined_cone_history(dir: &Path, left: usize, right: usize) -> (Store, String, String) {
    let mut s = Store::open(dir, Some(REPO)).unwrap();
    let d1 = s.create("d1", "directory").unwrap();
    let d2 = s.create("d2", "directory").unwrap();
    let x = s.create("d1/x", "file").unwrap();
    let mut events = concurrent_renames(&s, &d1.id, "d1", left);
    events.extend(concurrent_renames(&s, &d2.id, "d2", right));
    let allowed: BTreeSet<String> = events.iter().map(|e| e.device.clone()).collect();
    s.receive(
        Bundle {
            repo: s.repo.clone(),
            branches: vec![],
            events,
            objects: BTreeMap::new(),
        },
        &allowed,
    )
    .unwrap();
    assert_eq!(s.inbox_counts().unwrap().pending_active(), 0);
    for entity in [&d1.id, &d2.id] {
        // One review of every pair of concurrent renames.
        let pairs: Vec<Conflict> = s
            .conflicts()
            .unwrap()
            .into_iter()
            .filter(|c| &c.entity == entity && c.field == "location" && c.resolved_by.is_none())
            .collect();
        assert!(!pairs.is_empty(), "concurrent renames conflict");
        let selected = pairs[0].alternatives[0].revision.clone();
        let ids: Vec<String> = pairs.into_iter().map(|c| c.id).collect();
        s.resolve(&ids, &selected).unwrap();
    }
    assert!(
        !s.branch_has_unresolved_conflicts(&s.active.clone())
            .unwrap()
    );
    assert_oracle_empty(&mut s, "joined cone history");
    let d2_name = s.entry(&d2.id).unwrap().name;
    (s, x.id, d2_name)
}

/// CCR-WP1.6-5: a namespace successor commit counts the competing cuts
/// full replay would count and refuses where full replay refuses. At 6 + 6
/// concurrent renames the joined cone has 4,083 competing cuts (accepted, on
/// the fast path, counted); at 7 + 6 it has 8,178: refused with
/// `NAMESPACE_HISTORY_LIMIT` (nothing committed, the history still
/// rebuilds), and full replay of the same history plus the event refuses it
/// too. Before WP2.4 the successor committed it and the next rebuild failed.
#[test]
fn successor_commits_count_competing_cuts_like_full_replay() {
    // Accepted at the boundary, through the successor, with the count run.
    let dir = tempfile::tempdir().unwrap();
    let (mut s, x, d2) = joined_cone_history(dir.path(), 6, 6);
    let (fast, checks) = (s.commit_path_counters().fast, s.successor_cut_checks());
    s.rename(&x, &format!("{d2}/x"), false).unwrap();
    assert_eq!(
        s.commit_path_counters().fast,
        fast + 1,
        "a successor commit"
    );
    assert_eq!(
        s.successor_cut_checks(),
        checks + 1,
        "its cuts were counted"
    );
    assert_oracle_empty(&mut s, "after the joining move at 4,083 cuts");
    // A rename in place joins nothing: no count.
    let checks = s.successor_cut_checks();
    s.rename(&x, &format!("{d2}/y"), false).unwrap();
    assert_eq!(s.successor_cut_checks(), checks);
    // Refused past the boundary, exactly where full replay refuses.
    let dir = tempfile::tempdir().unwrap();
    let (mut s, x, d2) = joined_cone_history(dir.path(), 7, 6);
    let events_before = s.events().unwrap().len();
    let error = s.rename(&x, &format!("{d2}/x"), false).unwrap_err();
    assert!(
        format!("{error:#}").contains("NAMESPACE_HISTORY_LIMIT"),
        "{error:#}"
    );
    assert_eq!(
        s.events().unwrap().len(),
        events_before,
        "nothing committed"
    );
    assert_oracle_empty(&mut s, "after the refusal");
    // Full replay of the history plus that event refuses it too.
    let branch = s.branch(&s.active).unwrap();
    let change = s
        .current_change(
            &x,
            "location",
            json!({"parent": s.lookup(&d2).unwrap().id, "name": "x"}),
        )
        .unwrap();
    let event = s.event(&branch, vec![change]);
    let (_copy_dir, mut copy) = copy_of(&s);
    copy.db
        .execute(
            "INSERT INTO events VALUES(?,?,?)",
            rusqlite::params![
                event.id,
                event.branch.id,
                serde_json::to_string(&event).unwrap()
            ],
        )
        .unwrap();
    let refused = copy.verify_derived_state().unwrap_err();
    assert!(
        format!("{refused:#}").contains("NAMESPACE_HISTORY_LIMIT"),
        "{refused:#}"
    );
    // The same event from a peer is quarantined, not committed.
    let mut remote = event.clone();
    remote.device = "a-peer".into();
    s.stage_incoming(
        vec![staged(remote.clone())],
        &BTreeSet::from(["a-peer".to_owned()]),
    )
    .unwrap();
    let slice = s.activate_slice(SliceBudget::ACTIVATOR).unwrap();
    assert!(slice.slow_ticket.is_none() && slice.activated.is_empty());
    assert_eq!(slice.quarantined.len(), 1);
    assert_eq!(slice.quarantined[0].1.code, "NAMESPACE_HISTORY_LIMIT");
    assert_eq!(s.inbox_counts().unwrap().pending_active(), 0);
    assert_oracle_empty(&mut s, "after the remote refusal");
}
