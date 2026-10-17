//! WP4.INT: a seeded fuzz of the metadata feed, format 7 (REVAMP_PLAN §6.3,
//! §14 WP4.INT task 2: "release oracle with 1,000 seeds (`derive_oracle`,
//! `feed_v7` fuzz)").
//!
//! Two to four replicas of one repository exchange through the three
//! store-level feed functions, as two workers do through `feed-offer` →
//! `feed-exchange` → `feed-apply` (§6.3.5; `tests/feed_v7.rs` drives the same
//! functions one scenario at a time). Each seed picks a connected topology
//! (dialer → acceptor routes: a relay chain, a star or a full mesh, some
//! routes mutual), then runs random steps:
//!
//! * local operations on the shared branch (creates, directories, saves,
//!   renames, deletes, stale-base saves that conflict), and now and then a
//!   private branch with a canary file that must never leave its replica;
//! * an exchange over a random route, delivered whole, or with its reply
//!   lost (the acceptor handled the request, the dialer never applies), or
//!   its request lost, or a stale reply applied again later (it must be
//!   refused or harmless: replies bind to their request);
//! * partial activation (none, one activator slice, or a drain);
//! * restarts, and restores of a replica from an earlier backup (a feed
//!   regression: its peers must notice and re-send what it lost). A
//!   replica with a backup pending authors nothing until it is restored:
//!   its own events, once lost, would come back in its name and be refused
//!   `FORGED_EVENT_ORIGIN`, by design (invariant 14).
//!
//! After every step: private events and branches are on their author only
//! (invariant 12), nothing is rejected or quarantined (an honest workload),
//! and on every replica the step touched the derived-state oracle is empty
//! (invariant 11). At the end the faults stop and every route exchanges
//! until nothing moves: every replica holds the same shared journal and
//! projection, nothing is staged, every outbox row is acknowledged, each
//! route's peers know each other's whole shared journal (per-id evidence,
//! invariant 10) and see each other caught up, and every oracle is empty.
//!
//! `fuzz_seeds_debug` runs 8 seeds in the debug suite;
//! `feed_fuzz_release` is the qualification run (`TKFS_FEED_SEEDS`, default
//! 1000, from `TKFS_FEED_SEED_START`, default 10000):
//! `cargo test --release --test feed_fuzz -- --ignored --nocapture`.
mod support;

use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
use support::copy_dir;
use tkfs::{
    core::{Projection, ROOT, Store, id, inbox::SliceBudget},
    model::protocol::feed::{MetaExchangeReply, MetaExchangeRequest},
    paired_sync::SyncRoute,
};

struct Replica {
    root: PathBuf,
    repo: String,
    store: Option<Store>,
    /// A backup taken earlier (restored later); the replica authors nothing
    /// meanwhile.
    backup: Option<PathBuf>,
    /// Private event ids and branch ids this replica authored.
    private: BTreeSet<String>,
}

impl Replica {
    fn store(&mut self) -> &mut Store {
        self.store.as_mut().unwrap()
    }
    fn get(&self) -> &Store {
        self.store.as_ref().unwrap()
    }
    fn device(&self) -> String {
        self.get().device.clone()
    }
    fn restart(&mut self) {
        self.store = None;
        self.store = Some(Store::open(&self.root, Some(&self.repo)).unwrap());
    }
}

/// `from`'s route to `to` (keyed by `to`'s replica, as orchestrators bind it).
fn route(from: &Store, to: &Store) -> SyncRoute {
    SyncRoute {
        peer: format!("installation-of-{}", to.device),
        repo: from.repo.clone(),
        local_replica: from.device.clone(),
        remote_replica: to.device.clone(),
    }
}

/// A reply kept to be applied again later (a stale, duplicated delivery).
struct Kept {
    dialer: usize,
    acceptor: usize,
    request: String,
    reply: MetaExchangeReply,
}

struct Fuzz {
    seed: u64,
    rng: StdRng,
    dir: tempfile::TempDir,
    replicas: Vec<Replica>,
    /// Dialer → acceptor.
    routes: Vec<(usize, usize)>,
    kept: Vec<Kept>,
    log: Vec<String>,
    stats: BTreeMap<&'static str, usize>,
    serial: usize,
    /// An exchange since the flag was last cleared did something: entries,
    /// needs or served events either way, a cursor mismatch or reset, a
    /// probe page or a missing cursor (a side about to re-read the other).
    /// `converge` stops only after rounds in which no exchange did.
    moved: bool,
    /// Apply kept replies again (`TKFS_FEED_STALE_REPLIES=1`): off by
    /// default, since the session layer delivers each reply once, to its own
    /// request, and the orchestrator never replays `feed-apply` (invariant
    /// 16); the store-level binding check alone accepts a stale pair.
    stale_replies: bool,
}

impl Fuzz {
    fn new(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let dir = tempfile::tempdir().unwrap();
        let repo = id();
        let count = rng.gen_range(2..=4usize);
        let replicas = (0..count)
            .map(|i| {
                let root = dir.path().join(format!("r{i}"));
                let store = Store::initialize(&root, &repo, &id()).unwrap();
                Replica {
                    root,
                    repo: repo.clone(),
                    store: Some(store),
                    backup: None,
                    private: BTreeSet::new(),
                }
            })
            .collect();
        // A connected topology: every replica after the first dials (or is
        // dialed by) an earlier one; then extra routes, some mutual.
        let mut routes = BTreeSet::new();
        let shape = rng.gen_range(0..3);
        for i in 1..count {
            let other = match shape {
                0 => i - 1,               // relay chain
                1 => 0,                   // star
                _ => rng.gen_range(0..i), // random tree
            };
            if rng.gen_bool(0.5) {
                routes.insert((i, other));
            } else {
                routes.insert((other, i));
            }
        }
        for _ in 0..rng.gen_range(0..=count) {
            let (a, b) = (rng.gen_range(0..count), rng.gen_range(0..count));
            if a != b {
                routes.insert((a, b));
            }
        }
        let mut fuzz = Self {
            seed,
            rng,
            dir,
            replicas,
            routes: routes.into_iter().collect(),
            kept: Vec::new(),
            log: Vec::new(),
            stats: BTreeMap::new(),
            serial: 0,
            moved: false,
            stale_replies: std::env::var("TKFS_FEED_STALE_REPLIES").as_deref() == Ok("1"),
        };
        let routes = fuzz.routes.clone();
        fuzz.log
            .push(format!("seed {seed}: {count} replicas, routes {routes:?}"));
        fuzz
    }

    fn fail(&self, what: impl AsRef<str>) -> ! {
        // `TKFS_FEED_FULL_LOG=1`: every step, not the last 40.
        let keep = if std::env::var("TKFS_FEED_FULL_LOG").as_deref() == Ok("1") {
            usize::MAX
        } else {
            40
        };
        let tail: Vec<&String> = self.log.iter().rev().take(keep).collect();
        panic!(
            "feed fuzz seed {}: {}\nlast steps (newest first):\n{:#?}",
            self.seed,
            what.as_ref(),
            tail
        );
    }

    fn count(&mut self, what: &'static str) {
        *self.stats.entry(what).or_default() += 1;
    }

    fn next_name(&mut self, prefix: &str) -> String {
        self.serial += 1;
        format!("{prefix}-{}", self.serial)
    }

    // -- local operations -------------------------------------------------

    fn local(&mut self, r: usize) {
        if self.replicas[r].backup.is_some() {
            return;
        }
        let pick = self.rng.gen_range(0..100);
        let name = self.next_name("n");
        let outcome: anyhow::Result<&'static str> = (|| {
            let replica = &mut self.replicas[r];
            let s = replica.store();
            let p = s.projection(&s.active)?;
            let alive: Vec<_> = p.entries.values().filter(|e| e.alive).cloned().collect();
            let files: Vec<_> = alive.iter().filter(|e| e.kind == "file").cloned().collect();
            let dirs: Vec<_> = alive
                .iter()
                .filter(|e| e.kind == "directory")
                .cloned()
                .collect();
            Ok(match pick {
                0..=24 => {
                    s.create(&format!("{name}.txt"), "file")?;
                    "create"
                }
                25..=31 => {
                    s.create(&name, "directory")?;
                    "mkdir"
                }
                32..=36 if !dirs.is_empty() => {
                    let d = &dirs[pick % dirs.len()];
                    let path = path_of(&p, &d.id).ok_or_else(|| anyhow::anyhow!("hidden"))?;
                    s.create(&format!("{path}/{name}.txt"), "file")?;
                    "create in a directory"
                }
                37..=64 if !files.is_empty() => {
                    let f = &files[pick % files.len()];
                    s.write_revision(&f.id, f.revisions.get("content").cloned(), name.as_bytes())?;
                    "save"
                }
                65..=72 if !files.is_empty() => {
                    // A stale-base save: conflicts with any newer revision.
                    let f = &files[pick % files.len()];
                    let base = s.events()?.into_iter().find_map(|e| {
                        e.changes
                            .iter()
                            .find(|c| c.entity == f.id && c.field == "content")
                            .map(|_| e.id.clone())
                    });
                    s.write_with_base(
                        &f.id,
                        base,
                        f.revisions.get("alive").cloned(),
                        name.as_bytes(),
                    )?;
                    "stale save"
                }
                73..=82 if !files.is_empty() => {
                    let f = &files[pick % files.len()];
                    s.rename(&f.id, &format!("{name}.txt"), false)?;
                    "rename"
                }
                83..=90 if !files.is_empty() => {
                    let f = &files[pick % files.len()];
                    s.delete(&f.id)?;
                    "delete"
                }
                91..=93 => {
                    // A private branch with a canary: it never leaves.
                    let main = s.active.clone();
                    let branch = s.fork(&format!("private-{name}"), false)?;
                    s.checkout(&branch.id)?;
                    let canary = s.create(&format!("canary-{name}"), "file")?;
                    s.write_revision(
                        &canary.id,
                        canary.revisions.get("content").cloned(),
                        format!("PRIVATE-CANARY-{name}").as_bytes(),
                    )?;
                    s.checkout(&main)?;
                    let ids: Vec<String> = s
                        .events()?
                        .into_iter()
                        .filter(|e| !e.branch.shared)
                        .map(|e| e.id)
                        .collect();
                    replica.private.insert(branch.id);
                    replica.private.extend(ids);
                    "private"
                }
                _ => {
                    s.create(&format!("{name}.txt"), "file")?;
                    "create"
                }
            })
        })();
        match outcome {
            Ok(kind) => {
                self.count(kind);
                self.log.push(format!("local r{r}: {kind}"));
            }
            Err(error) => {
                // A refused local operation (an occupied name, a moved
                // entry) changes nothing; the store must stay exact.
                self.count("local refused");
                self.log.push(format!("local r{r}: refused {error:#}"));
            }
        }
        self.check_oracle(r, "local");
    }

    // -- exchanges --------------------------------------------------------

    fn exchange(&mut self, dialer: usize, acceptor: usize, fault: u32) {
        let (d, a) = (&self.replicas[dialer], &self.replicas[acceptor]);
        let request: MetaExchangeRequest =
            match d.get().build_exchange_request(&route(d.get(), a.get())) {
                Ok(request) => request,
                Err(error) => self.fail(format!("r{dialer} build_exchange_request: {error:#}")),
            };
        if fault == 1 {
            // The request never arrived.
            self.count("request lost");
            self.log
                .push(format!("exchange r{dialer}->r{acceptor}: request lost"));
            return;
        }
        let handled = a
            .get()
            .handle_exchange_request(&route(a.get(), d.get()), &request);
        let (reply, _) = match handled {
            Ok(handled) => handled,
            Err(error) => self.fail(format!("r{acceptor} handle_exchange_request: {error:#}")),
        };
        if fault == 2 {
            // The reply was lost: the acceptor's side happened.
            self.count("reply lost");
            self.log
                .push(format!("exchange r{dialer}->r{acceptor}: reply lost"));
            self.check_oracle(acceptor, "reply lost");
            return;
        }
        let applied =
            d.get()
                .apply_exchange_reply(&route(d.get(), a.get()), &request.request, &reply);
        if let Err(error) = applied {
            self.fail(format!("r{dialer} apply_exchange_reply: {error:#}"));
        }
        self.count("exchange");
        use tkfs::model::protocol::feed::PageStatus;
        self.moved |= !request.page.entries.is_empty()
            || !reply.page.entries.is_empty()
            || !request.need.is_empty()
            || !request.need_served.is_empty()
            || !reply.need_served.is_empty()
            || !reply.unknown.is_empty()
            || reply.page_status != PageStatus::Accepted
            || request.page.reset
            || reply.page.reset
            || request.page.after.is_none()
            || request.cursor.is_none()
            || reply.cursor.epoch.is_empty();
        self.log.push(format!(
            "exchange r{dialer}->r{acceptor}: {} up, {} down",
            request.page.entries.len(),
            reply.page.entries.len()
        ));
        if std::env::var("TKFS_FEED_TRACE").as_deref() == Ok("1") {
            // Every id moved (8 characters; `+` with its payload).
            let short = |id: &str| id.chars().take(8).collect::<String>();
            let entries = |page: &tkfs::model::protocol::feed::FeedPage| {
                page.entries
                    .iter()
                    .map(|e| {
                        format!(
                            "{}{}",
                            short(&e.id),
                            if e.event.is_some() { "+" } else { "" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            };
            let events = |list: &[tkfs::core::Event]| {
                list.iter()
                    .map(|e| short(&e.id))
                    .collect::<Vec<_>>()
                    .join(" ")
            };
            self.log.push(format!(
                "  up [after {:?} reset {}]: {} | cursor {:?} | need [{}] | served [{}]",
                request.page.after.as_ref().map(|c| c.seq),
                request.page.reset,
                entries(&request.page),
                request.cursor.as_ref().map(|c| c.seq),
                request
                    .need
                    .iter()
                    .map(|id| short(id))
                    .collect::<Vec<_>>()
                    .join(" "),
                events(&request.need_served),
            ));
            self.log.push(format!(
                "  down [{:?} after {:?} reset {}]: {} | cursor {} | served [{}] | unknown {}",
                reply.page_status,
                reply.page.after.as_ref().map(|c| c.seq),
                reply.page.reset,
                entries(&reply.page),
                reply.cursor.seq,
                events(&reply.need_served),
                reply.unknown.len(),
            ));
        }
        if self.rng.gen_bool(0.1) {
            self.kept.push(Kept {
                dialer,
                acceptor,
                request: request.request.clone(),
                reply,
            });
        }
        self.check_oracle(dialer, "exchange");
        self.check_oracle(acceptor, "exchange");
    }

    /// A reply applied again, later (a duplicate or delayed delivery): bound
    /// to its request, it is refused or changes nothing that matters.
    fn stale_reply(&mut self) {
        if self.kept.is_empty() {
            return;
        }
        let index = self.rng.gen_range(0..self.kept.len());
        let kept = self.kept.swap_remove(index);
        let (d, a) = (&self.replicas[kept.dialer], &self.replicas[kept.acceptor]);
        let outcome =
            d.get()
                .apply_exchange_reply(&route(d.get(), a.get()), &kept.request, &kept.reply);
        self.count(if outcome.is_ok() {
            "stale reply applied"
        } else {
            "stale reply refused"
        });
        self.log.push(format!(
            "stale reply r{}<-r{}: {}",
            kept.dialer,
            kept.acceptor,
            match &outcome {
                Ok(_) => "applied".to_owned(),
                Err(error) => format!("refused {error:#}"),
            }
        ));
        self.check_oracle(kept.dialer, "stale reply");
    }

    // -- activation, restarts, restores -----------------------------------

    fn activate(&mut self, r: usize, budget: SliceBudget, slices: usize) -> usize {
        let mut activated = 0;
        for _ in 0..slices {
            match self.replicas[r].store().activate_slice(budget) {
                Ok(slice) => {
                    activated += slice.activated.len();
                    if !slice.more {
                        break;
                    }
                }
                Err(error) => self.fail(format!("r{r} activate_slice: {error:#}")),
            }
        }
        activated
    }

    fn drain(&mut self, r: usize) -> usize {
        self.activate(r, SliceBudget::UNBOUNDED, usize::MAX)
    }

    fn backup_or_restore(&mut self, r: usize) {
        let replica = &mut self.replicas[r];
        if let Some(backup) = replica.backup.take() {
            replica.store = None;
            std::fs::remove_dir_all(&replica.root).unwrap();
            copy_dir(&backup, &replica.root).unwrap();
            std::fs::remove_dir_all(&backup).unwrap();
            replica.restart();
            self.count("restore");
            self.log.push(format!("restore r{r}"));
        } else {
            let backup = self.dir.path().join(format!("backup-{r}-{}", self.serial));
            replica.store = None;
            copy_dir(&replica.root, &backup).unwrap();
            replica.restart();
            replica.backup = Some(backup);
            self.count("backup");
            self.log.push(format!("backup r{r}"));
        }
        self.check_oracle(r, "restore");
    }

    // -- invariants -------------------------------------------------------

    fn check_oracle(&mut self, r: usize, step: &str) {
        let diff = match self.replicas[r].store().verify_derived_state() {
            Ok(diff) => diff,
            Err(error) => self.fail(format!("r{r} oracle after {step}: {error:#}")),
        };
        if !diff.is_empty() {
            self.fail(format!("r{r} oracle after {step}: {diff:#?}"));
        }
    }

    /// Private events and branches stay with their author; an honest
    /// workload has no rejected or quarantined row.
    fn check_isolation(&self) {
        for (r, replica) in self.replicas.iter().enumerate() {
            let s = replica.get();
            let events: BTreeSet<String> = s.events().unwrap().into_iter().map(|e| e.id).collect();
            let branches: BTreeSet<String> =
                s.db.prepare("SELECT id FROM branches")
                    .unwrap()
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
            let staged: BTreeSet<String> =
                s.db.prepare("SELECT id FROM incoming_events")
                    .unwrap()
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
            for (o, other) in self.replicas.iter().enumerate() {
                if o == r {
                    continue;
                }
                for secret in &other.private {
                    if events.contains(secret)
                        || branches.contains(secret)
                        || staged.contains(secret)
                    {
                        self.fail(format!("r{o}'s private {secret} reached r{r}"));
                    }
                }
            }
            let refused: Vec<(String, Option<String>)> =
                s.db.prepare("SELECT id,error FROM incoming_events WHERE error IS NOT NULL")
                    .unwrap()
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
            if !refused.is_empty() {
                self.fail(format!("r{r} quarantined rows: {refused:?}"));
            }
            for (peer, other) in self.replicas.iter().enumerate() {
                if peer == r {
                    continue;
                }
                let rejected = s.feed_rejections(&other.device()).unwrap();
                if !rejected.is_empty() {
                    let codes: Vec<(String, String)> =
                        rejected.into_iter().map(|row| (row.id, row.code)).collect();
                    self.fail(format!("r{r} rejected entries of r{peer}: {codes:?}"));
                }
            }
        }
    }

    fn step(&mut self) {
        let count = self.replicas.len();
        let choice = self.rng.gen_range(0..100);
        match choice {
            0..=39 => {
                let r = self.rng.gen_range(0..count);
                self.local(r);
            }
            40..=79 => {
                let (dialer, acceptor) = *self.routes.choose(&mut self.rng).unwrap();
                let fault = match self.rng.gen_range(0..100) {
                    0..=7 => 1,
                    8..=17 => 2,
                    _ => 0,
                };
                self.exchange(dialer, acceptor, fault);
            }
            80..=91 => {
                let r = self.rng.gen_range(0..count);
                let n = match self.rng.gen_range(0..3) {
                    0 => self.activate(r, SliceBudget::ACTIVATOR, 1),
                    1 => self.activate(r, SliceBudget::ACTIVATOR, 4),
                    _ => self.drain(r),
                };
                self.log.push(format!("activate r{r}: {n}"));
                self.check_oracle(r, "activation");
            }
            92..=94 if self.stale_replies => self.stale_reply(),
            92..=94 => {}
            95..=97 => {
                let r = self.rng.gen_range(0..count);
                self.replicas[r].restart();
                self.count("restart");
                self.log.push(format!("restart r{r}"));
                self.check_oracle(r, "restart");
            }
            _ => {
                let r = self.rng.gen_range(0..count);
                self.backup_or_restore(r);
            }
        }
        self.check_isolation();
    }

    /// No more faults: pending restores happen, then every route exchanges
    /// and every replica activates until nothing moves.
    fn converge(&mut self) {
        for r in 0..self.replicas.len() {
            if self.replicas[r].backup.is_some() {
                self.backup_or_restore(r);
            }
        }
        let routes = self.routes.clone();
        let mut quiet_rounds = 0;
        for round in 0..400 {
            let before: Vec<(BTreeSet<String>, i64, u64)> = self
                .replicas
                .iter()
                .map(|replica| {
                    let s = replica.get();
                    (
                        s.shared_event_inventory().unwrap(),
                        s.outgoing_unacknowledged().unwrap(),
                        s.inbox_counts().unwrap().total(),
                    )
                })
                .collect();
            self.moved = false;
            for &(dialer, acceptor) in &routes {
                self.exchange(dialer, acceptor, 0);
            }
            let mut activated = 0;
            for r in 0..self.replicas.len() {
                activated += self.drain(r);
            }
            let after: Vec<(BTreeSet<String>, i64, u64)> = self
                .replicas
                .iter()
                .map(|replica| {
                    let s = replica.get();
                    (
                        s.shared_event_inventory().unwrap(),
                        s.outgoing_unacknowledged().unwrap(),
                        s.inbox_counts().unwrap().total(),
                    )
                })
                .collect();
            // A round whose exchanges only re-synchronized cursors (a probe
            // after a restore) changes no inventory, but the next one moves
            // events (WP4.INT: seed 10631 stopped there).
            quiet_rounds = if activated == 0 && before == after && !self.moved {
                quiet_rounds + 1
            } else {
                0
            };
            if quiet_rounds >= 2 {
                self.log.push(format!("converged after {round} rounds"));
                return;
            }
        }
        self.fail("the feed did not settle in 400 rounds");
    }

    fn check_converged(&mut self) {
        self.check_isolation();
        let first = self.replicas[0].get();
        let shared = first.shared_event_inventory().unwrap();
        let projection = first.projection(&first.active).unwrap();
        for r in 0..self.replicas.len() {
            let s = self.replicas[r].get();
            if s.shared_event_inventory().unwrap() != shared {
                self.fail(format!("r{r} holds another shared journal"));
            }
            let p = s.projection(&s.active).unwrap();
            if p.entries != projection.entries {
                self.fail(format!("r{r} projects differently"));
            }
            if s.inbox_counts().unwrap().total() != 0 {
                self.fail(format!("r{r} left rows staged"));
            }
            if s.outgoing_unacknowledged().unwrap() != 0 {
                let devices: Vec<String> = self.replicas.iter().map(Replica::device).collect();
                let name = |device: &str| {
                    devices
                        .iter()
                        .position(|d| d == device)
                        .map_or(device.to_owned(), |i| format!("r{i}"))
                };
                let authors: BTreeMap<String, String> = s
                    .events()
                    .unwrap()
                    .into_iter()
                    .map(|e| (e.id, name(&e.device)))
                    .collect();
                let rows: Vec<String> =
                    s.db.prepare("SELECT event FROM outbox WHERE acknowledged=0")
                        .unwrap()
                        .query_map([], |row| row.get(0))
                        .unwrap()
                        .collect::<rusqlite::Result<_>>()
                        .unwrap();
                let unacked: Vec<(String, String)> = rows
                    .into_iter()
                    .map(|id| {
                        let author = authors.get(&id).cloned().unwrap_or_default();
                        (id, author)
                    })
                    .collect();
                self.fail(format!(
                    "r{r} has {} unacknowledged outbox rows (id, author): {unacked:?}",
                    s.outgoing_unacknowledged().unwrap()
                ));
            }
        }
        for &(dialer, acceptor) in &self.routes.clone() {
            for (x, y) in [(dialer, acceptor), (acceptor, dialer)] {
                let (sx, sy) = (self.replicas[x].get(), self.replicas[y].get());
                let known = sx.known_by_peer(&sy.device).unwrap();
                if known != shared {
                    // Which ones, and whose (the replica that authored each).
                    let devices: Vec<String> = self.replicas.iter().map(Replica::device).collect();
                    let authors: BTreeMap<String, String> = sx
                        .events()
                        .unwrap()
                        .into_iter()
                        .map(|e| {
                            let author = devices
                                .iter()
                                .position(|d| *d == e.device)
                                .map_or(e.device.clone(), |i| format!("r{i}"));
                            (e.id, author)
                        })
                        .collect();
                    let missing: Vec<(String, String)> = shared
                        .difference(&known)
                        .map(|id| (id.clone(), authors.get(id).cloned().unwrap_or_default()))
                        .collect();
                    self.fail(format!(
                        "r{x} has evidence of {} of r{y}'s {} shared events; missing (id, author): {missing:?}",
                        known.len(),
                        shared.len()
                    ));
                }
            }
            let (d, a) = (self.replicas[dialer].get(), self.replicas[acceptor].get());
            if !d.feed_gate(&a.device).unwrap().caught_up {
                self.fail(format!("r{dialer} does not see r{acceptor} caught up"));
            }
        }
        for r in 0..self.replicas.len() {
            self.check_oracle(r, "convergence");
        }
    }
}

fn run_seed(seed: u64, steps: std::ops::RangeInclusive<usize>) -> BTreeMap<&'static str, usize> {
    let mut fuzz = Fuzz::new(seed);
    let steps = fuzz.rng.gen_range(steps);
    for _ in 0..steps {
        fuzz.step();
    }
    fuzz.converge();
    fuzz.check_converged();
    let mut stats = std::mem::take(&mut fuzz.stats);
    stats.insert("replicas", fuzz.replicas.len());
    stats.insert(
        "events",
        fuzz.replicas[0]
            .get()
            .shared_event_inventory()
            .unwrap()
            .len(),
    );
    stats
}

fn run_seeds(seeds: impl Iterator<Item = u64>, steps: std::ops::RangeInclusive<usize>) {
    let mut total: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut seeds_run = 0;
    for seed in seeds {
        let started = std::time::Instant::now();
        let stats = run_seed(seed, steps.clone());
        eprintln!(
            "FEED_SEED {seed}: {} ms, {} replicas, {} events",
            started.elapsed().as_millis(),
            stats.get("replicas").copied().unwrap_or(0),
            stats.get("events").copied().unwrap_or(0)
        );
        for (k, v) in stats {
            *total.entry(k).or_default() += v;
        }
        seeds_run += 1;
    }
    println!("FEED_FUZZ {}", json!({"seeds": seeds_run, "stats": total}));
    // The fuzz must exercise what it is for.
    for kind in ["exchange", "reply lost", "save", "create"] {
        assert!(
            total.get(kind).copied().unwrap_or(0) > 0,
            "no {kind}: {total:?}"
        );
    }
}

/// Debug suite: 8 seeds of 40–80 steps.
#[test]
fn fuzz_seeds_debug() {
    run_seeds(0..8, 40..=80);
}

/// Seeds the release run found (WP4.INT), kept in the debug suite: 10023
/// (an entry a peer's feed listed while we lacked the event became evidence
/// only once `Store`'s receiver kept it, `evidence_from_needs`; it failed
/// before with a short known set) and 10631 (the convergence loop stopped
/// after a round that only re-synchronized cursors; a harness fix).
#[test]
fn seeds_found_by_the_release_run() {
    for seed in [10023, 10631] {
        run_seed(seed, 100..=200);
    }
}

/// Qualification: `cargo test --release --test feed_fuzz -- --ignored`.
/// `TKFS_FEED_SEEDS` (default 1000) seeds from `TKFS_FEED_SEED_START`
/// (default 10000, disjoint from the debug seeds), 100–200 steps each.
#[test]
#[ignore = "WP4.INT release qualification (1,000 seeds)"]
fn feed_fuzz_release() {
    let env = |key: &str, default: u64| {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let start = env("TKFS_FEED_SEED_START", 10_000);
    let count = env("TKFS_FEED_SEEDS", 1000);
    run_seeds(start..start + count, 100..=200);
}

/// The visible path of an entry, if it and every ancestor are alive.
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
