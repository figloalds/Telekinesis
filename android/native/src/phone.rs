//! What the phone made and the PC has not yet confirmed. `phone.sqlite` keeps one row per file,
//! folder or name this device changed, from the journal's own events, so the Folders screen can say
//! "1 file waiting to send" with no PC to ask, and still say it after the app is killed.
//!
//! A row leaves only when one peer both received the file's latest event (`sent` has passed it)
//! and, for content this device wrote, holds every digest of the head (`objects.have`). Anything
//! else leaves the row: an error, an unreachable peer, a peer that predates `objects.have`, a
//! digest the peer lacks, a budget that ran out. Clearing too early would tell the owner a file
//! reached the PC when it may not have, so every doubt is a row that stays.
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};
use tkfs::{
    model::content::Digest,
    orchestrator::{
        catalog::Catalog,
        hub::{Daemon, Held},
        sync::HAVE_BATCH,
    },
    runtime::Engine,
};

/// The working name a shared-in file has until it is complete (`PART_PREFIX` in the share code).
/// It is not a file the owner made, so it is never listed or counted.
pub const PART_PREFIX: &str = ".tkfs-part-";

/// How much one `confirm` may ask of the PC. What it does not reach waits for the next call.
#[derive(Clone, Copy, Debug)]
pub struct Budget {
    pub digests: usize,
    pub time: Duration,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            digests: 4 * HAVE_BATCH,
            time: Duration::from_secs(20),
        }
    }
}

/// What the PCs say. `sent` is the journal position each peer has durably received from this
/// device for `repo`; `held` is which of the digests a peer durably holds.
pub trait Confirmer {
    fn sent(&self, repo: &str) -> Vec<(String, u64)>;
    fn held(&self, installation: &str, digests: &[Digest]) -> Held;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Waiting {
    pub entity: String,
    pub path: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Confirmed {
    /// Files still waiting.
    pub waiting: usize,
    /// Of those, the ones a peer that has their events could not be asked about, because it
    /// predates `objects.have`. Only updating the PC can clear them.
    pub unsupported: usize,
}

pub struct Phone {
    db: Mutex<Connection>,
    /// One confirmation at a time: two would ask the PC the same questions.
    confirming: Mutex<()>,
    budget: Budget,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

const SCAN_BATCH: usize = 512;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS scanned(
    folder TEXT PRIMARY KEY,
    journal TEXT NOT NULL,
    position INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS unconfirmed(
    folder TEXT NOT NULL,
    entity TEXT NOT NULL,
    seq INTEGER NOT NULL,
    path TEXT NOT NULL,
    -- How much of this event's digests `peer` already confirmed, of `total`.
    peer TEXT NOT NULL DEFAULT '',
    checked INTEGER NOT NULL DEFAULT 0,
    total INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(folder, entity)
);";

struct Row {
    entity: String,
    seq: u64,
    path: String,
    peer: String,
    checked: usize,
    total: usize,
}

fn hidden(path: &str) -> bool {
    path.rsplit('/').next().is_some_and(|name| name.starts_with(PART_PREFIX))
}

enum Blocked {
    NoFeature,
    Unreachable,
}

impl Phone {
    pub fn open(path: &Path) -> Result<Phone> {
        let db = Connection::open(path)
            .with_context(|| format!("PHONE_DB_UNAVAILABLE: {}", path.display()))?;
        // A write that returns is on disk: the app may be killed right after.
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        db.execute_batch(SCHEMA)?;
        Ok(Phone {
            db: Mutex::new(db),
            confirming: Mutex::new(()),
            budget: Budget::default(),
        })
    }

    pub fn with_budget(mut self, budget: Budget) -> Phone {
        self.budget = budget;
        self
    }

    /// Takes this device's events since the last scan into the table. A folder whose journal is
    /// not the one scanned (it started over) loses its rows and is scanned again from the start,
    /// since positions in one journal mean nothing in another.
    pub fn scan(&self, folder: &str, engine: &Engine) -> Result<()> {
        let journal = lock(&engine.store).device.clone();
        let mut position = {
            let db = lock(&self.db);
            let saved: Option<(String, u64)> = db
                .query_row(
                    "SELECT journal,position FROM scanned WHERE folder=?1",
                    [folder],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            match saved {
                Some((saved, position)) if saved == journal => position,
                Some(_) => {
                    db.execute("DELETE FROM unconfirmed WHERE folder=?1", [folder])?;
                    db.execute("DELETE FROM scanned WHERE folder=?1", [folder])?;
                    0
                }
                None => 0,
            }
        };
        loop {
            // Read before the events: all of them up to here are looked at, whatever comes later.
            let top = lock(&engine.store).feed_position()?;
            let batch = engine.own_events_after(position, SCAN_BATCH)?;
            let full = batch.len() == SCAN_BATCH;
            let next = match batch.last() {
                Some((last, _)) if full => *last,
                Some((last, _)) => top.max(*last),
                None => top,
            };
            let paths: Vec<String> = {
                let store = lock(&engine.store);
                batch
                    .iter()
                    .map(|(_, event)| {
                        store
                            .projection()
                            .path(&event.entity)
                            .unwrap_or_else(|_| format!("/{}", event.state.name))
                    })
                    .collect()
            };
            let db = lock(&self.db);
            let tx = db.unchecked_transaction()?;
            for ((seq, event), path) in batch.iter().zip(&paths) {
                // A newer event of the same entity starts it over: it is a different thing to send.
                tx.execute(
                    "INSERT INTO unconfirmed(folder,entity,seq,path) VALUES(?1,?2,?3,?4) \
                     ON CONFLICT(folder,entity) DO UPDATE SET \
                        seq=excluded.seq, path=excluded.path, peer='', checked=0, total=0 \
                     WHERE excluded.seq > unconfirmed.seq",
                    params![folder, event.entity, seq, path],
                )?;
            }
            tx.execute(
                "INSERT INTO scanned(folder,journal,position) VALUES(?1,?2,?3) \
                 ON CONFLICT(folder) DO UPDATE SET journal=excluded.journal, position=excluded.position",
                params![folder, journal, next],
            )?;
            tx.commit()?;
            drop(db);
            if !full {
                return Ok(());
            }
            position = next;
        }
    }

    fn rows(&self, folder: &str) -> Result<Vec<Row>> {
        let db = lock(&self.db);
        let mut query = db.prepare_cached(
            "SELECT entity,seq,path,peer,checked,total FROM unconfirmed WHERE folder=?1 ORDER BY seq",
        )?;
        Ok(query
            .query_map([folder], |r| {
                Ok(Row {
                    entity: r.get(0)?,
                    seq: r.get(1)?,
                    path: r.get(2)?,
                    peer: r.get(3)?,
                    checked: r.get(4)?,
                    total: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn remember(&self, folder: &str, row: &Row, peer: &str, checked: usize, total: usize) -> Result<()> {
        lock(&self.db).execute(
            "UPDATE unconfirmed SET peer=?4, checked=?5, total=?6 \
             WHERE folder=?1 AND entity=?2 AND seq=?3",
            params![folder, row.entity, row.seq, peer, checked, total],
        )?;
        Ok(())
    }

    /// Takes the paths of renamed folders from the journal; what a row last said stays when the
    /// journal no longer knows the entity.
    fn refresh_paths(&self, folder: &str, rows: &[Row], engine: &Engine) -> Result<()> {
        let current: Vec<Option<String>> = {
            let store = lock(&engine.store);
            rows.iter()
                .map(|row| store.projection().path(&row.entity).ok())
                .collect()
        };
        let db = lock(&self.db);
        for (row, path) in rows.iter().zip(current) {
            if let Some(path) = path.filter(|path| *path != row.path) {
                db.execute(
                    "UPDATE unconfirmed SET path=?3 WHERE folder=?1 AND entity=?2 AND seq=?4",
                    params![folder, row.entity, path, row.seq],
                )?;
            }
        }
        Ok(())
    }

    /// Scans, asks the peers about what is left, and takes out what they confirmed. The work is
    /// bounded by the budget: whatever it does not reach stays for the next call.
    pub fn confirm(
        &self,
        folder: &str,
        repo: &str,
        engine: &Engine,
        peers: &dyn Confirmer,
    ) -> Result<Confirmed> {
        let _one = lock(&self.confirming);
        let deadline = Instant::now() + self.budget.time;
        let mut left = self.budget.digests;
        self.scan(folder, engine)?;
        let rows = self.rows(folder)?;
        self.refresh_paths(folder, &rows, engine)?;
        let sent = peers.sent(repo);
        let mut blocked: BTreeMap<String, Blocked> = BTreeMap::new();
        let mut cleared: Vec<(String, u64)> = Vec::new();
        let mut unsupported = BTreeSet::new();
        'rows: for row in &rows {
            // Out of digests is not out of rows: what needs no bytes still clears.
            if Instant::now() >= deadline {
                break;
            }
            let candidates: Vec<&str> = sent
                .iter()
                .filter(|(_, sent)| *sent >= row.seq)
                .map(|(peer, _)| peer.as_str())
                .collect();
            if candidates.is_empty() {
                continue;
            }
            // A working file is no one's to confirm. It clears once exchanged; the file it
            // becomes is a new event with a row of its own.
            let digests = if hidden(&row.path) {
                Vec::new()
            } else {
                match engine.head_closure(&row.entity) {
                    Ok(closure) => closure.digests,
                    // Not knowing what the head needs is not a reason to say it was sent.
                    Err(_) => continue,
                }
            };
            if digests.is_empty() {
                cleared.push((row.entity.clone(), row.seq));
                continue;
            }
            let total = digests.len();
            for peer in candidates.iter().copied() {
                if blocked.contains_key(peer) {
                    continue;
                }
                let mut at = if row.peer == peer && row.total == total {
                    row.checked.min(total)
                } else {
                    0
                };
                let before = at;
                while at < total {
                    if left == 0 || Instant::now() >= deadline {
                        break;
                    }
                    let take = (total - at).min(HAVE_BATCH).min(left);
                    let page = &digests[at..at + take];
                    match peers.held(peer, page) {
                        Held::Known(known) => {
                            left -= take;
                            if !page.iter().all(|digest| known.contains(digest)) {
                                break;
                            }
                            at += take;
                        }
                        Held::NoFeature => {
                            blocked.insert(peer.to_owned(), Blocked::NoFeature);
                            break;
                        }
                        Held::Unreachable(_) => {
                            blocked.insert(peer.to_owned(), Blocked::Unreachable);
                            break;
                        }
                    }
                }
                if at > before {
                    self.remember(folder, row, peer, at, total)?;
                }
                if at == total {
                    cleared.push((row.entity.clone(), row.seq));
                    continue 'rows;
                }
            }
            if candidates
                .iter()
                .any(|peer| matches!(blocked.get(*peer), Some(Blocked::NoFeature)))
            {
                unsupported.insert((row.entity.clone(), row.seq));
            }
        }
        // An edit made while the PC was being asked has a newer event now. The scan finds it
        // and the guard on the sequence keeps its row: the answers above were about the old one.
        self.scan(folder, engine)?;
        {
            let db = lock(&self.db);
            let tx = db.unchecked_transaction()?;
            for (entity, seq) in &cleared {
                tx.execute(
                    "DELETE FROM unconfirmed WHERE folder=?1 AND entity=?2 AND seq=?3",
                    params![folder, entity, seq],
                )?;
            }
            tx.commit()?;
        }
        let waiting = self.waiting(folder)?;
        let waiting_now: BTreeSet<_> = waiting.iter().map(|w| w.entity.as_str()).collect();
        let unsupported = unsupported
            .iter()
            .filter(|(entity, _)| waiting_now.contains(entity.as_str()))
            .count();
        Ok(Confirmed {
            waiting: waiting.len(),
            unsupported,
        })
    }

    /// The files waiting, oldest first. Working files of a share in progress are not among them.
    pub fn waiting(&self, folder: &str) -> Result<Vec<Waiting>> {
        Ok(self
            .rows(folder)?
            .into_iter()
            .filter(|row| !hidden(&row.path))
            .map(|row| Waiting {
                entity: row.entity,
                path: row.path,
            })
            .collect())
    }
}

/// The peers `repo`'s cursors name that count for this folder now: the cursor is of the
/// folder's current journal (`journal`, the device of its store), and the peer is still paired.
/// Cursors outlive a removed folder, linger after a start over until its first page saves, and
/// an unpair can lose a race with a driver that saves one more.
pub fn counted_cursors(catalog: &Catalog, repo: &str, journal: &str) -> Result<Vec<(String, u64)>> {
    let paired: BTreeSet<String> = catalog
        .peers()?
        .into_iter()
        .map(|peer| peer.installation)
        .collect();
    Ok(catalog
        .feed_cursors_of_repo(repo)?
        .into_iter()
        .filter(|(installation, cursor)| {
            cursor.local_journal == journal && paired.contains(installation)
        })
        .map(|(installation, cursor)| (installation, cursor.sent))
        .collect())
}

/// The orchestrator's peers as a [`Confirmer`] for one folder.
pub struct DaemonPeers<'a> {
    pub daemon: &'a Daemon,
    pub folder: &'a str,
    /// The journal of the folder's store now: its `device`.
    pub journal: &'a str,
}

impl Confirmer for DaemonPeers<'_> {
    fn sent(&self, repo: &str) -> Vec<(String, u64)> {
        // Not being able to say is the same as no peer having it.
        counted_cursors(&self.daemon.catalog, repo, self.journal).unwrap_or_default()
    }

    fn held(&self, installation: &str, digests: &[Digest]) -> Held {
        // An error says nothing about the digests: it is never "held", and never "lost".
        self.daemon
            .held_by(self.folder, installation, digests)
            .unwrap_or_else(|error| Held::Unreachable(format!("{error:#}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::RefCell, sync::Arc};
    use tkfs::{
        core::Operation,
        model::content::{CHUNK_SIZE, ContentKind, FileContentRef},
        orchestrator::catalog::SavedCursor,
    };

    const FOLDER: &str = "folder-id";
    const REPO: &str = "repo-id";
    const PC: &str = "pc";

    /// A PC that has received some of this device's journal and holds some digests.
    #[derive(Default)]
    struct FakePeers {
        sent: RefCell<BTreeMap<String, u64>>,
        held: RefCell<BTreeMap<String, BTreeSet<Digest>>>,
        feature: RefCell<BTreeMap<String, bool>>,
        asked: RefCell<Vec<usize>>,
    }

    impl FakePeers {
        fn sent(&self, peer: &str, sent: u64) {
            self.sent.borrow_mut().insert(peer.into(), sent);
        }
        fn holds(&self, peer: &str, digests: &[Digest]) {
            self.held
                .borrow_mut()
                .entry(peer.into())
                .or_default()
                .extend(digests.iter().copied());
        }
        fn without_the_feature(&self, peer: &str) {
            self.feature.borrow_mut().insert(peer.into(), false);
        }
        fn calls(&self) -> usize {
            self.asked.borrow().len()
        }
    }

    impl Confirmer for FakePeers {
        fn sent(&self, _: &str) -> Vec<(String, u64)> {
            self.sent.borrow().iter().map(|(p, s)| (p.clone(), *s)).collect()
        }
        fn held(&self, installation: &str, digests: &[Digest]) -> Held {
            self.asked.borrow_mut().push(digests.len());
            if self.feature.borrow().get(installation) == Some(&false) {
                return Held::NoFeature;
            }
            let held = self.held.borrow();
            let own = held.get(installation);
            Held::Known(
                digests
                    .iter()
                    .filter(|digest| own.is_some_and(|own| own.contains(digest)))
                    .copied()
                    .collect(),
            )
        }
    }

    struct Fixture {
        _home: tempfile::TempDir,
        engine: Arc<Engine>,
        phone: Phone,
        peers: FakePeers,
    }

    fn fixture() -> Fixture {
        let home = tempfile::tempdir().unwrap();
        let engine = Engine::open(&home.path().join("folder"), None).unwrap();
        let phone = Phone::open(&home.path().join("phone.sqlite")).unwrap();
        Fixture {
            _home: home,
            engine,
            phone,
            peers: FakePeers::default(),
        }
    }

    impl Fixture {
        fn write(&self, path: &str, bytes: &[u8]) {
            let token = self.engine.open_file(path, true, false, true).unwrap();
            self.engine.write(token, 0, bytes, false, false).unwrap();
            self.engine.flush(Some(token)).unwrap();
            self.engine.close(token).unwrap();
        }

        fn rewrite(&self, path: &str, bytes: &[u8]) {
            let token = self.engine.open_to_replace(path).unwrap();
            self.engine.truncate(token, 0, false).unwrap();
            self.engine.write(token, 0, bytes, false, false).unwrap();
            self.engine.flush(Some(token)).unwrap();
            self.engine.close(token).unwrap();
        }

        fn rename(&self, from: &str, to: &str) {
            let token = self
                .engine
                .open_native_file(from, false, false, false, None, true)
                .unwrap();
            self.engine.rename(token, to, false).unwrap();
            self.engine.close(token).unwrap();
            self.engine.store.lock().unwrap().sync_metadata().unwrap();
        }

        fn delete(&self, path: &str) {
            let id = self.engine.lookup(path).unwrap().id;
            self.engine.store.lock().unwrap().delete(&id).unwrap();
        }

        fn top(&self) -> u64 {
            self.engine.store.lock().unwrap().feed_position().unwrap()
        }

        fn digests(&self, path: &str) -> Vec<Digest> {
            let id = self.engine.lookup(path).unwrap().id;
            self.engine.head_closure(&id).unwrap().digests
        }

        fn confirm(&self) -> Confirmed {
            self.phone.confirm(FOLDER, REPO, &self.engine, &self.peers).unwrap()
        }

        fn paths(&self) -> Vec<String> {
            self.phone
                .waiting(FOLDER)
                .unwrap()
                .into_iter()
                .map(|w| w.path)
                .collect()
        }

        /// An event of another device on top of `path`'s head, as a feed brings it.
        fn peer_writes(&self, path: &str, bytes: &[u8]) {
            let id = self.engine.lookup(path).unwrap().id;
            let content = self.engine.working.content().import(bytes).unwrap();
            let mut event = {
                let mut store = self.engine.store.lock().unwrap();
                let mut state = store.entry(&id).unwrap();
                state.content = Some(content);
                let heads = store.projection().heads[&id].iter().cloned().collect();
                let branch = store.active.clone();
                store.event(&branch, state, heads, Operation::Content)
            };
            event.author = "a peer".into();
            self.engine.apply_remote(&[event]).unwrap();
        }
    }

    #[test]
    fn a_new_file_waits_until_sent_and_held() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        assert_eq!(f.confirm(), Confirmed { waiting: 1, unsupported: 0 });
        assert_eq!(f.paths(), ["/a.txt"]);
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &f.digests("/a.txt"));
        assert_eq!(f.confirm(), Confirmed::default());
        assert!(f.paths().is_empty());
    }

    #[test]
    fn sent_alone_does_not_clear_a_file() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.peers.sent(PC, f.top());
        assert_eq!(f.confirm().waiting, 1);
        assert_eq!(f.paths(), ["/a.txt"]);
    }

    #[test]
    fn held_alone_does_not_clear_a_file() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.peers.holds(PC, &f.digests("/a.txt"));
        // No peer has the event yet, even one that holds the bytes.
        assert_eq!(f.confirm().waiting, 1);
        // And one that has passed only the events before this file's does not count either.
        f.peers.sent(PC, 0);
        assert_eq!(f.confirm().waiting, 1);
    }

    #[test]
    fn a_file_edited_again_waits_again() {
        let f = fixture();
        f.write("/a.txt", b"first");
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &f.digests("/a.txt"));
        assert_eq!(f.confirm().waiting, 0);

        f.rewrite("/a.txt", b"second, and longer");
        assert_eq!(f.confirm().waiting, 1);
        assert_eq!(f.paths(), ["/a.txt"]);
        // Its new event has not gone, and its new bytes are not there.
        f.peers.sent(PC, f.top());
        assert_eq!(f.confirm().waiting, 1);
        f.peers.holds(PC, &f.digests("/a.txt"));
        assert_eq!(f.confirm().waiting, 0);
    }

    #[test]
    fn a_delete_waits_for_the_exchange_only() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &f.digests("/a.txt"));
        assert_eq!(f.confirm().waiting, 0);

        f.delete("/a.txt");
        assert_eq!(f.confirm().waiting, 1, "the delete has not gone");
        f.peers.sent(PC, f.top());
        let asked = f.peers.calls();
        assert_eq!(f.confirm().waiting, 0, "no bytes to wait for");
        assert_eq!(f.peers.calls(), asked, "the PC was not asked about bytes");
    }

    #[test]
    fn a_rename_waits_for_the_exchange_only() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &f.digests("/a.txt"));
        assert_eq!(f.confirm().waiting, 0);

        f.rename("/a.txt", "/b.txt");
        assert_eq!(f.confirm().waiting, 1, "the rename has not gone");
        assert_eq!(f.paths(), ["/b.txt"]);
        f.peers.sent(PC, f.top());
        assert_eq!(f.confirm().waiting, 0);
    }

    #[test]
    fn a_rename_of_a_file_whose_manifest_is_not_here_asks_for_the_manifest_only() {
        let f = fixture();
        // A file the PC made, known here by name only: its manifest was never downloaded, and
        // the PC is the one that holds it.
        let absent = FileContentRef::new(
            ContentKind::Manifest,
            Digest::bytes(b"never here"),
            4 * CHUNK_SIZE,
        )
        .unwrap();
        f.write("/theirs.bin", b"x");
        let id = f.engine.lookup("/theirs.bin").unwrap().id;
        {
            let mut store = f.engine.store.lock().unwrap();
            let mut state = store.entry(&id).unwrap();
            state.content = Some(absent.clone());
            let heads = store.projection().heads[&id].iter().cloned().collect();
            let branch = store.active.clone();
            let mut event = store.event(&branch, state, heads, Operation::Content);
            event.author = "the pc".into();
            store.apply(&[event]).unwrap();
        }
        f.rename("/theirs.bin", "/mine.bin");
        f.peers.sent(PC, f.top());
        // The head is this device's rename, and the manifest is all that can be asked about.
        assert_eq!(f.confirm().waiting, 1);
        f.peers.holds(PC, &[absent.digest]);
        assert_eq!(f.confirm().waiting, 0);
    }

    #[test]
    fn a_head_written_by_a_peer_needs_no_bytes() {
        let f = fixture();
        f.write("/a.txt", b"mine");
        f.peer_writes("/a.txt", b"the peer's, which this phone never wrote");
        f.peers.sent(PC, f.top());
        // The PC holds none of this phone's bytes, and is not asked.
        assert_eq!(f.confirm().waiting, 0);
        assert_eq!(f.peers.calls(), 0);
    }

    #[test]
    fn one_confirming_peer_is_enough() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.peers.sent("slow", 0);
        f.peers.sent("pc", f.top());
        f.peers.sent("other", f.top());
        f.peers.holds("pc", &f.digests("/a.txt"));
        assert_eq!(f.confirm().waiting, 0);
    }

    #[test]
    fn a_peer_without_the_feature_leaves_files_waiting_and_says_so() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.write("/b.txt", b"there");
        f.delete("/b.txt");
        f.peers.sent(PC, f.top());
        f.peers.without_the_feature(PC);
        // Even if it did hold the bytes, nothing can say so.
        f.peers.holds(PC, &f.digests("/a.txt"));
        let confirmed = f.confirm();
        assert_eq!(confirmed, Confirmed { waiting: 1, unsupported: 1 });
        assert_eq!(f.paths(), ["/a.txt"]);
        // Another PC that can say so clears it.
        f.peers.sent("new", f.top());
        f.peers.holds("new", &f.digests("/a.txt"));
        assert_eq!(f.confirm(), Confirmed::default());
    }

    #[test]
    fn an_unreachable_peer_clears_nothing() {
        struct Down;
        impl Confirmer for Down {
            fn sent(&self, _: &str) -> Vec<(String, u64)> {
                vec![("pc".into(), u64::MAX)]
            }
            fn held(&self, _: &str, _: &[Digest]) -> Held {
                Held::Unreachable("no lane".into())
            }
        }
        let f = fixture();
        f.write("/a.txt", b"hello");
        let confirmed = f.phone.confirm(FOLDER, REPO, &f.engine, &Down).unwrap();
        assert_eq!(confirmed, Confirmed { waiting: 1, unsupported: 0 });
    }

    #[test]
    fn a_peer_that_lacks_one_digest_does_not_confirm() {
        let f = fixture();
        let bytes: Vec<u8> = (0..2 * CHUNK_SIZE as usize + 5).map(|i| (i / 3 % 251) as u8).collect();
        f.write("/big", &bytes);
        let digests = f.digests("/big");
        assert_eq!(digests.len(), 4);
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &digests[..3]);
        assert_eq!(f.confirm().waiting, 1);
        f.peers.holds(PC, &digests[3..]);
        assert_eq!(f.confirm().waiting, 0);
    }

    #[test]
    fn work_per_call_is_bounded_and_a_large_file_still_confirms_across_calls() {
        let mut f = fixture();
        f.phone = Phone::open(&f._home.path().join("phone.sqlite"))
            .unwrap()
            .with_budget(Budget {
                digests: 2,
                time: Duration::from_secs(60),
            });
        let bytes: Vec<u8> = (0..4 * CHUNK_SIZE as usize).map(|i| (i / 5 % 251) as u8).collect();
        f.write("/big", &bytes);
        f.write("/small", b"s");
        let big = f.digests("/big");
        assert_eq!(big.len(), 5);
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &big);
        f.peers.holds(PC, &f.digests("/small"));

        // 5 digests and then 1, at 2 a call, the big file first: it is not asked twice about any.
        let asked = || f.peers.asked.borrow().iter().sum::<usize>();
        assert_eq!(f.confirm().waiting, 2);
        assert_eq!(asked(), 2);
        assert_eq!(f.confirm().waiting, 2);
        assert_eq!(asked(), 4);
        assert_eq!(f.confirm().waiting, 0);
        assert_eq!(asked(), 6);
    }

    #[test]
    fn progress_on_an_old_edit_is_not_credited_to_the_new_one() {
        let mut f = fixture();
        f.phone = Phone::open(&f._home.path().join("phone.sqlite"))
            .unwrap()
            .with_budget(Budget {
                digests: 2,
                time: Duration::from_secs(60),
            });
        let pattern = |salt: usize| -> Vec<u8> {
            (0..4 * CHUNK_SIZE as usize).map(|i| ((i / 5 + salt) % 251) as u8).collect()
        };
        f.write("/big", &pattern(0));
        let old = f.digests("/big");
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &old);
        assert_eq!(f.confirm().waiting, 1);

        // Same number of digests, none the same. The PC holds the tail of the new ones only.
        f.rewrite("/big", &pattern(100));
        let new = f.digests("/big");
        assert_eq!(new.len(), old.len());
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &new[2..]);
        for _ in 0..4 {
            assert_eq!(f.confirm().waiting, 1);
        }
        f.peers.holds(PC, &new[..2]);
        for _ in 0..3 {
            f.confirm();
        }
        assert_eq!(f.confirm().waiting, 0);
    }

    #[test]
    fn a_file_whose_head_cannot_be_read_stays_waiting() {
        let f = fixture();
        let snapshot = f.engine.store.lock().unwrap().checkpoint().unwrap();
        let other = f.engine.store.lock().unwrap().fork(&snapshot, "other").unwrap();
        f.write("/a.txt", b"hello");
        assert_eq!(f.confirm().waiting, 1);
        // On a branch that has never heard of the file there is no head to read.
        f.engine.store.lock().unwrap().checkout(&other).unwrap();
        f.peers.sent(PC, f.top());
        assert_eq!(f.confirm().waiting, 1);
        assert_eq!(f.peers.calls(), 0);
    }

    #[test]
    fn a_spent_budget_does_not_hold_back_what_needs_no_bytes() {
        let mut f = fixture();
        f.phone = Phone::open(&f._home.path().join("phone.sqlite"))
            .unwrap()
            .with_budget(Budget {
                digests: 1,
                time: Duration::from_secs(60),
            });
        let bytes: Vec<u8> = (0..2 * CHUNK_SIZE as usize).map(|i| (i / 5 % 251) as u8).collect();
        f.write("/big", &bytes);
        f.write("/gone.txt", b"gone");
        f.delete("/gone.txt");
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &f.digests("/big"));
        // The big file spends the whole budget; the delete behind it is still cleared.
        let confirmed = f.confirm();
        assert_eq!(confirmed.waiting, 1);
        assert_eq!(f.paths(), ["/big"]);
    }

    #[test]
    fn a_confirmation_out_of_time_stops_and_clears_nothing_unasked() {
        let mut f = fixture();
        f.phone = Phone::open(&f._home.path().join("phone.sqlite"))
            .unwrap()
            .with_budget(Budget {
                digests: 100,
                time: Duration::ZERO,
            });
        f.write("/a.txt", b"hello");
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &f.digests("/a.txt"));
        assert_eq!(f.confirm().waiting, 1);
        assert_eq!(f.peers.calls(), 0);
    }

    #[test]
    fn a_share_in_progress_is_not_listed_or_counted() {
        let f = fixture();
        f.write("/.tkfs-part-1234", b"half a photo");
        f.write("/sub-name.txt", b"mine");
        assert_eq!(f.confirm().waiting, 1);
        assert_eq!(f.paths(), ["/sub-name.txt"]);
        // Finished, it is a file the owner made: renamed, it waits under its own name.
        f.rename("/.tkfs-part-1234", "/photo.jpg");
        assert_eq!(f.confirm().waiting, 2);
        assert_eq!(f.paths(), ["/sub-name.txt", "/photo.jpg"]);
    }

    #[test]
    fn a_working_file_that_goes_away_leaves_the_table_once_exchanged() {
        let f = fixture();
        f.write("/.tkfs-part-1", b"abandoned");
        f.delete("/.tkfs-part-1");
        f.peers.sent(PC, f.top());
        assert_eq!(f.confirm().waiting, 0);
        assert!(f.phone.rows(FOLDER).unwrap().is_empty());
    }

    #[test]
    fn a_change_made_while_the_pc_was_asked_keeps_its_row() {
        // The peer answers "held" for the old bytes, and the file is edited as it answers.
        struct EditsOnAnswer<'a>(&'a Fixture);
        impl Confirmer for EditsOnAnswer<'_> {
            fn sent(&self, _: &str) -> Vec<(String, u64)> {
                vec![("pc".into(), self.0.top())]
            }
            fn held(&self, _: &str, digests: &[Digest]) -> Held {
                let answer = Held::Known(digests.iter().copied().collect());
                self.0.rewrite("/a.txt", b"edited just now");
                answer
            }
        }
        let f = fixture();
        f.write("/a.txt", b"before");
        let confirmed = f
            .phone
            .confirm(FOLDER, REPO, &f.engine, &EditsOnAnswer(&f))
            .unwrap();
        assert_eq!(confirmed.waiting, 1);
        assert_eq!(f.paths(), ["/a.txt"]);
    }

    #[test]
    fn a_folder_that_started_over_does_not_inherit_its_old_rows() {
        let f = fixture();
        f.write("/old.txt", b"old");
        assert_eq!(f.confirm().waiting, 1);
        // The same folder id, a new store: another journal.
        let other = tempfile::tempdir().unwrap();
        let fresh = Engine::open(other.path(), None).unwrap();
        let confirmed = f.phone.confirm(FOLDER, REPO, &fresh, &f.peers).unwrap();
        assert_eq!(confirmed.waiting, 0);
        assert!(f.paths().is_empty());
    }

    #[test]
    fn folders_do_not_share_rows() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.phone.scan("one", &f.engine).unwrap();
        f.peers.sent(PC, f.top());
        f.phone.confirm("one", REPO, &f.engine, &f.peers).unwrap();
        f.peers.holds(PC, &f.digests("/a.txt"));
        assert_eq!(f.phone.confirm("one", REPO, &f.engine, &f.peers).unwrap().waiting, 0);
        assert_eq!(f.phone.waiting("two").unwrap(), vec![]);
        f.phone.scan(FOLDER, &f.engine).unwrap();
        assert_eq!(f.paths(), ["/a.txt"]);
    }

    #[test]
    fn a_renamed_directory_moves_the_paths_of_what_waits_in_it() {
        let f = fixture();
        let directory = f.engine.open_file("/d", true, true, false).unwrap();
        f.engine.close(directory).unwrap();
        f.write("/d/a.txt", b"hello");
        assert_eq!(f.confirm().waiting, 2);
        f.rename("/d", "/e");
        f.confirm();
        let mut paths = f.paths();
        paths.sort();
        assert_eq!(paths, ["/e", "/e/a.txt"]);
    }

    #[test]
    fn waiting_survives_reopening_the_database() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.write("/b.txt", b"there");
        assert_eq!(f.confirm().waiting, 2);
        let path = f._home.path().join("phone.sqlite");
        let Fixture { engine, _home, .. } = f;
        let reopened = Phone::open(&path).unwrap();
        let mut paths: Vec<_> = reopened
            .waiting(FOLDER)
            .unwrap()
            .into_iter()
            .map(|w| w.path)
            .collect();
        paths.sort();
        assert_eq!(paths, ["/a.txt", "/b.txt"]);
        // And what it scanned is not scanned twice: a new file is the only new row.
        let token = engine.open_file("/c.txt", true, false, true).unwrap();
        engine.close(token).unwrap();
        reopened.scan(FOLDER, &engine).unwrap();
        assert_eq!(reopened.waiting(FOLDER).unwrap().len(), 3);
        engine.shutdown().unwrap();
    }

    /// The share code names its working files in Kotlin; a rename of that prefix there must not
    /// leave them in the table here.
    #[test]
    fn the_working_prefix_is_the_one_the_share_code_uses() {
        // Tests run in the package directory.
        let importer =
            std::fs::read_to_string("../app/src/main/java/dev/tkfs/share/Importer.kt").unwrap();
        assert!(importer.contains(&format!("PART_PREFIX = \"{PART_PREFIX}\"")));
    }

    fn catalog_with(journal: &str) -> (tempfile::TempDir, Arc<Catalog>) {
        let home = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(home.path()).unwrap();
        catalog.save_peer("ws://pc:1", "pc", [1; 32], None).unwrap();
        save_cursor(&catalog, "pc", journal, 7);
        (home, catalog)
    }

    fn save_cursor(catalog: &Catalog, peer: &str, journal: &str, sent: u64) {
        let cursor = SavedCursor {
            sent,
            received: 3,
            local_journal: journal.into(),
            peer_journal: "theirs".into(),
        };
        catalog.save_feed_cursor(peer, REPO, &cursor).unwrap();
    }

    #[test]
    fn a_cursor_of_the_folders_journal_from_a_paired_peer_counts() {
        let (_home, catalog) = catalog_with("device-1");
        assert_eq!(
            counted_cursors(&catalog, REPO, "device-1").unwrap(),
            vec![("pc".to_owned(), 7)]
        );
    }

    #[test]
    fn a_cursor_of_another_journal_does_not_count() {
        // A removed folder's row, or a start over before its first page saved.
        let (_home, catalog) = catalog_with("device-1");
        assert!(counted_cursors(&catalog, REPO, "device-2").unwrap().is_empty());
        assert!(counted_cursors(&catalog, "another repo", "device-1").unwrap().is_empty());
    }

    #[test]
    fn a_cursor_of_a_peer_no_longer_paired_does_not_count() {
        let (_home, catalog) = catalog_with("device-1");
        catalog.unpair("pc").unwrap();
        assert!(counted_cursors(&catalog, REPO, "device-1").unwrap().is_empty());
        // The race: a driver saves one more cursor after the unpair.
        save_cursor(&catalog, "pc", "device-1", 9);
        assert!(counted_cursors(&catalog, REPO, "device-1").unwrap().is_empty());
    }
}
