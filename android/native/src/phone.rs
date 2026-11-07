//! What the phone made and the PC has not yet confirmed. `phone.sqlite` keeps one row per file,
//! folder or name this device changed, from the journal's own events, so the Folders screen can say
//! "1 file waiting to send" with no PC to ask, and still say it after the app is killed.
//!
//! A row leaves only when one peer both received the file's latest event (`sent` has passed it)
//! and, for content this device wrote, holds every digest of the head (`objects.have`). Anything
//! else leaves the row: an error, an unreachable peer, a peer that predates `objects.have`, a
//! digest the peer lacks, a budget that ran out. Clearing too early would tell the owner a file
//! reached the PC when it may not have, so every doubt is a row that stays.
//!
//! The same file holds what the owner marked to keep and when each file was last opened, for what
//! leaves when space runs low: dead objects, then deleted files, and then the files opened longest
//! ago, only if a PC has just said it holds them. A file with a row here has not been confirmed, and
//! does not leave.
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};
use tkfs::{
    core::{Entry, EntryKind, ROOT},
    model::{content::Digest, errors::code_of},
    orchestrator::{
        catalog::Catalog,
        hub::{Daemon, Held},
        sync::HAVE_BATCH,
    },
    runtime::{Engine, Presence},
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
    /// The table is still catching up with a long journal: the count is of what was reached so far,
    /// so it says nothing about the rest.
    pub scanning: bool,
}

/// What freeing space did: what left the packs, and why it could not do what was asked.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Freed {
    pub bytes: u64,
    /// [`PC_UNREACHABLE`], [`PC_TOO_OLD`] or [`NOT_ON_PC`].
    pub blocked: Option<String>,
}

/// No PC answered, so nothing it holds can be counted on.
pub const PC_UNREACHABLE: &str = "PC_UNREACHABLE";
/// The PC cannot say what it holds until it is updated.
pub const PC_TOO_OLD: &str = "PC_TOO_OLD";
/// The PC answered, and it does not hold all of the file.
pub const NOT_ON_PC: &str = "NOT_ON_PC";

pub struct Phone {
    db: Mutex<Connection>,
    /// One confirmation at a time: two would ask the PC the same questions.
    confirming: Mutex<()>,
    /// One freeing at a time: two would rewrite the same segments, for the same bytes.
    freeing: Mutex<()>,
    budget: Budget,
    /// About how much one evict is asked to free.
    batch_bytes: u64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Own events taken into the table at a time, and the journal pages read for them.
const SCAN_BATCH: usize = 512;
const SCAN_PAGES: usize = 8;

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
);
CREATE TABLE IF NOT EXISTS marks(
    folder TEXT NOT NULL,
    entity TEXT NOT NULL,
    PRIMARY KEY(folder, entity)
);
CREATE TABLE IF NOT EXISTS opened(
    folder TEXT NOT NULL,
    entity TEXT NOT NULL,
    at_ms INTEGER NOT NULL,
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
    path.rsplit('/')
        .next()
        .is_some_and(|name| name.starts_with(PART_PREFIX))
}

enum Blocked {
    NoFeature,
    Unreachable,
}

impl Phone {
    pub fn open(path: &Path) -> Result<Phone> {
        let opened = (|| -> Result<Connection> {
            let db = Connection::open(path)?;
            // A write that returns is on disk: the app may be killed right after.
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
            db.execute_batch(SCHEMA)?;
            Ok(db)
        })();
        let db = opened.with_context(|| format!("PHONE_DB_UNAVAILABLE: {}", path.display()))?;
        Ok(Phone {
            db: Mutex::new(db),
            confirming: Mutex::new(()),
            freeing: Mutex::new(()),
            budget: Budget::default(),
            batch_bytes: EVICT_BATCH_BYTES,
        })
    }

    pub fn with_budget(mut self, budget: Budget) -> Phone {
        self.budget = budget;
        self
    }

    #[cfg(test)]
    fn with_batch_bytes(mut self, bytes: u64) -> Phone {
        self.batch_bytes = bytes;
        self
    }

    /// Takes all of this device's events since the last scan into the table.
    pub fn scan(&self, folder: &str, engine: &Engine) -> Result<()> {
        self.scan_until(folder, engine, Instant::now() + Duration::from_secs(86_400))?;
        Ok(())
    }

    /// Takes this device's events since the last scan into the table, until `deadline`. False when
    /// that came first: the table is behind, and the position reached is kept for the next call.
    ///
    /// Only events the journal has made durable are taken (the store keeps recent ones in an open
    /// transaction that a kill takes back, and their sequences are then used again), and never
    /// those past the `top` read first. A saved position beyond `top` is of a journal that lost
    /// events: the folder starts over, as it does when its journal is another one, since positions
    /// in one journal mean nothing in another.
    pub fn scan_until(&self, folder: &str, engine: &Engine, deadline: Instant) -> Result<bool> {
        let journal = lock(&engine.store).device.clone();
        let top = engine.durable_position()?;
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
                Some((saved, position)) if saved == journal && position <= top => position,
                Some(_) => {
                    db.execute("DELETE FROM unconfirmed WHERE folder=?1", [folder])?;
                    db.execute("DELETE FROM scanned WHERE folder=?1", [folder])?;
                    0
                }
                None => 0,
            }
        };
        while position < top {
            if Instant::now() >= deadline {
                return Ok(false);
            }
            let read = engine.own_events_between(position, top, SCAN_BATCH, SCAN_PAGES)?;
            if read.reached <= position {
                // No progress is no reason to loop: the next call asks again.
                return Ok(false);
            }
            let paths: Vec<String> = {
                let store = lock(&engine.store);
                read.events
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
            for ((seq, event), path) in read.events.iter().zip(&paths) {
                // A newer event of the same entity starts it over: it is a different thing to send.
                tx.execute(
                    "INSERT INTO unconfirmed(folder,entity,seq,path) VALUES(?1,?2,?3,?4)                      ON CONFLICT(folder,entity) DO UPDATE SET                         seq=excluded.seq, path=excluded.path, peer='', checked=0, total=0                      WHERE excluded.seq > unconfirmed.seq",
                    params![folder, event.entity, seq, path],
                )?;
            }
            tx.execute(
                "INSERT INTO scanned(folder,journal,position) VALUES(?1,?2,?3)                  ON CONFLICT(folder) DO UPDATE SET journal=excluded.journal, position=excluded.position",
                params![folder, journal, read.reached],
            )?;
            tx.commit()?;
            drop(db);
            position = read.reached;
        }
        // An empty journal still has a position to keep.
        let db = lock(&self.db);
        db.execute(
            "INSERT INTO scanned(folder,journal,position) VALUES(?1,?2,?3)              ON CONFLICT(folder) DO UPDATE SET journal=excluded.journal, position=excluded.position",
            params![folder, journal, position],
        )?;
        Ok(true)
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

    fn remember(
        &self,
        folder: &str,
        row: &Row,
        peer: &str,
        checked: usize,
        total: usize,
    ) -> Result<()> {
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
        if !self.scan_until(folder, engine, deadline)? {
            return Ok(Confirmed {
                waiting: self.waiting(folder)?.len(),
                unsupported: 0,
                scanning: true,
            });
        }
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
                    // Not knowing what the head needs is not a reason to say it was sent. But an
                    // event the journal no longer holds (taken back by a kill) is nothing to send.
                    Err(_) => {
                        if matches!(engine.has_event(row.seq, &row.entity), Ok(false)) {
                            cleared.push((row.entity.clone(), row.seq));
                        }
                        continue;
                    }
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
        // If it cannot finish, nothing is cleared: an event it did not reach could be newer.
        if !self.scan_until(folder, engine, Instant::now() + self.budget.time)? {
            return Ok(Confirmed {
                waiting: self.waiting(folder)?.len(),
                unsupported: 0,
                scanning: true,
            });
        }
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
            scanning: false,
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

    /// [`Self::scan_until`] with no confirmation running: a scan beside one could put back a row it
    /// has just cleared.
    fn scan_alone(&self, folder: &str, engine: &Engine, deadline: Instant) -> Result<bool> {
        let _one = lock(&self.confirming);
        self.scan_until(folder, engine, deadline)
    }

    /// Keeps a file or directory on this phone, or stops. A mark goes by entity, so a rename keeps
    /// it, and a directory's covers what later arrives in it.
    pub fn mark(&self, folder: &str, entity: &str, keep: bool) -> Result<()> {
        let db = lock(&self.db);
        if keep {
            db.execute(
                "INSERT OR IGNORE INTO marks(folder,entity) VALUES(?1,?2)",
                params![folder, entity],
            )?;
        } else {
            db.execute(
                "DELETE FROM marks WHERE folder=?1 AND entity=?2",
                params![folder, entity],
            )?;
        }
        Ok(())
    }

    /// What the owner marked in the folder, by entity.
    pub fn marks(&self, folder: &str) -> Result<BTreeSet<String>> {
        let db = lock(&self.db);
        let mut query = db.prepare_cached("SELECT entity FROM marks WHERE folder=?1")?;
        Ok(query
            .query_map([folder], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    /// Whether `entity` is marked, or is in a directory that is. `ancestors` are the directories it
    /// is in.
    pub fn is_kept(&self, folder: &str, entity: &str, ancestors: &[String]) -> Result<bool> {
        let marks = self.marks(folder)?;
        Ok(marks.contains(entity) || ancestors.iter().any(|id| marks.contains(id)))
    }

    /// The owner opened the file now. Losing this to a kill costs only a little of the order.
    pub fn opened(&self, folder: &str, entity: &str, now_ms: u64) -> Result<()> {
        lock(&self.db).execute(
            "INSERT INTO opened(folder,entity,at_ms) VALUES(?1,?2,?3) \
             ON CONFLICT(folder,entity) DO UPDATE SET at_ms=excluded.at_ms",
            params![folder, entity, now_ms],
        )?;
        Ok(())
    }

    fn opened_times(&self, folder: &str) -> Result<HashMap<String, u64>> {
        let db = lock(&self.db);
        let mut query = db.prepare_cached("SELECT entity,at_ms FROM opened WHERE folder=?1")?;
        Ok(query
            .query_map([folder], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }

    /// Every file of the folder that shows, and what freeing has to know of it.
    fn files(&self, folder: &str, engine: &Engine) -> Result<Vec<FileInFolder>> {
        let marks = self.marks(folder)?;
        let store = lock(&engine.store);
        let projection = store.projection();
        Ok(projection
            .entries
            .values()
            .filter(|entry| entry.kind == EntryKind::File && !entry.deleted)
            .filter(|entry| entry.content.is_some())
            .map(|entry| FileInFolder {
                entity: entry.id.clone(),
                kept: !marks.is_empty() && is_covered(&marks, &projection.entries, entry),
                conflict: projection
                    .heads
                    .get(&entry.id)
                    .is_some_and(|heads| heads.len() > 1),
            })
            .collect())
    }

    /// The chunks that stay whatever else leaves: those of a kept file, and those of a file a PC has
    /// not confirmed, which has yet to send them from here.
    fn chunks_that_stay(
        &self,
        folder: &str,
        files: &[FileInFolder],
        engine: &Engine,
    ) -> Result<BTreeSet<Digest>> {
        let waiting = self.rows(folder)?;
        let kept = files.iter().filter(|file| file.kept).map(|f| &f.entity);
        let mut stay = BTreeSet::new();
        for entity in kept.chain(waiting.iter().map(|row| &row.entity)) {
            match engine.head_chunks(entity) {
                Ok(Some(chunks)) => stay.extend(chunks),
                // A file whose manifest is not here has none here to name.
                Ok(None) => {}
                // A row can be of a directory, or of something the journal took back.
                Err(error)
                    if matches!(code_of(&error).as_str(), "IS_A_DIRECTORY" | "NOT_FOUND") => {}
                // Not knowing what must stay is no reason to go on as if nothing must.
                Err(error) => return Err(error),
            }
        }
        Ok(stay)
    }

    /// Which of `digests` a PC holds now, and why none could say. What one PC does not name, the
    /// next one is asked about.
    fn ask_peers(
        &self,
        sent: &[(String, u64)],
        digests: &[Digest],
        peers: &dyn Confirmer,
        deadline: Instant,
    ) -> (BTreeSet<Digest>, Option<&'static str>) {
        let mut known = BTreeSet::new();
        let (mut answered, mut too_old) = (false, false);
        'peers: for (peer, _) in sent {
            let left: Vec<Digest> = digests
                .iter()
                .filter(|digest| !known.contains(*digest))
                .copied()
                .collect();
            for page in left.chunks(HAVE_BATCH) {
                if Instant::now() >= deadline {
                    break 'peers;
                }
                match peers.held(peer, page) {
                    Held::Known(held) => {
                        answered = true;
                        known.extend(held);
                    }
                    Held::NoFeature => {
                        too_old = true;
                        continue 'peers;
                    }
                    Held::Unreachable(_) => continue 'peers,
                }
            }
        }
        let blocked = match (answered, too_old) {
            (true, _) => None,
            (false, true) => Some(PC_TOO_OLD),
            (false, false) => Some(PC_UNREACHABLE),
        };
        (known, blocked)
    }

    /// Frees the chunks of one file, if no doubt holds them here: it has not been confirmed, it is
    /// kept, or the PC does not say it holds them all. Those it shares with a kept file stay.
    /// What an open handle, a draft or an unresolved conflict needs stays too, inside `evict`.
    pub fn remove_download(
        &self,
        folder: &str,
        repo: &str,
        entity: &str,
        engine: &Engine,
        peers: &dyn Confirmer,
    ) -> Result<Freed> {
        let _one = lock(&self.freeing);
        let deadline = Instant::now() + self.budget.time;
        let not_on_pc = || Freed {
            bytes: 0,
            blocked: Some(NOT_ON_PC.to_owned()),
        };
        // A table still catching up says nothing about this file, which may be on what it has not read.
        if !self.scan_alone(folder, engine, deadline)? {
            return Ok(not_on_pc());
        }
        if self.rows(folder)?.iter().any(|row| row.entity == entity) {
            return Ok(not_on_pc());
        }
        let files = self.files(folder, engine)?;
        if files.iter().any(|file| file.entity == entity && file.kept) {
            return Ok(Freed::default());
        }
        let Some(chunks) = engine.head_chunks(entity)? else {
            return Ok(Freed::default());
        };
        let stay = self.chunks_that_stay(folder, &files, engine)?;
        let mut named = Vec::new();
        for digest in chunks {
            if !stay.contains(&digest) && takes_room(engine, digest)? > 0 {
                named.push(digest);
            }
        }
        if named.is_empty() {
            return Ok(Freed::default());
        }
        let (held, blocked) = self.ask_peers(&peers.sent(repo), &named, peers, deadline);
        if let Some(blocked) = blocked {
            return Ok(Freed {
                bytes: 0,
                blocked: Some(blocked.to_owned()),
            });
        }
        if !named.iter().all(|digest| held.contains(digest)) {
            return Ok(not_on_pc());
        }
        let mut failure = None;
        let bytes = evict_counted(engine, &named.into_iter().collect(), &mut failure)?;
        match failure {
            Some(error) if bytes == 0 => Err(error),
            _ => Ok(Freed {
                bytes,
                blocked: None,
            }),
        }
    }

    /// Gives back at least `need_bytes` of the packs if it can. First what is dead, which is nobody's
    /// and asks no one. Then what only a deleted file holds, which the owner deleted everywhere. Then
    /// the files opened longest ago, those never opened first, whose chunks a PC has just said it
    /// holds. The bytes are what left the disk, whatever each step said, as an evict that failed may
    /// have freed other segments. Out of time, it stops with what it has.
    pub fn free_oldest(
        &self,
        folder: &str,
        repo: &str,
        engine: &Engine,
        peers: &dyn Confirmer,
        need_bytes: u64,
    ) -> Result<Freed> {
        let _one = lock(&self.freeing);
        let deadline = Instant::now() + self.budget.time;
        let packs = &engine.working.content().packs;
        let before = packs.disk_bytes()?;
        let so_far = || -> Result<u64> { Ok(before.saturating_sub(packs.disk_bytes()?)) };
        let mut failure = None;
        // A collect seals every draft, and half of a file that is still being written is no version.
        if engine.writable_tokens().is_empty()
            && let Err(error) = engine.collect()
        {
            failure.get_or_insert(error);
        }
        let mut freed = Freed {
            bytes: so_far()?,
            blocked: None,
        };
        if freed.bytes < need_bytes {
            let need = need_bytes - freed.bytes;
            if let Err(error) = free_deleted(engine, need, self.batch_bytes, deadline, &mut failure)
            {
                failure.get_or_insert(error);
            }
            freed.bytes = so_far()?;
        }
        if freed.bytes < need_bytes {
            let files = self.free_files(
                folder,
                repo,
                engine,
                peers,
                need_bytes,
                deadline,
                &so_far,
                &mut failure,
            );
            match files {
                Ok(blocked) => freed.blocked = blocked,
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
            freed.bytes = so_far()?;
        }
        match failure {
            Some(error) if freed.bytes == 0 && freed.blocked.is_none() => Err(error),
            Some(error) => {
                crate::log_error(&format!("freeing {folder}: {error:#}"));
                Ok(freed)
            }
            None => Ok(freed),
        }
    }

    /// The last step of [`Self::free_oldest`]. Each batch is asked about and evicted as one, since
    /// many small evicts would copy what is left of a segment again each time, and it has a size,
    /// since one evict holds the store and the packs for as long as it copies. This can go on for a
    /// while, so what stays is worked out again for each batch: a file edited or marked by then is
    /// not what it was when this began.
    #[allow(clippy::too_many_arguments)]
    fn free_files(
        &self,
        folder: &str,
        repo: &str,
        engine: &Engine,
        peers: &dyn Confirmer,
        need_bytes: u64,
        deadline: Instant,
        so_far: &dyn Fn() -> Result<u64>,
        failure: &mut Option<anyhow::Error>,
    ) -> Result<Option<String>> {
        let opened = self.opened_times(folder)?;
        let mut queue: Vec<(u64, String)> = self
            .files(folder, engine)?
            .into_iter()
            .map(|file| (opened.get(&file.entity).copied().unwrap_or(0), file.entity))
            .collect();
        queue.sort();
        let mut queue = queue.into_iter().map(|(_, entity)| entity);
        let sent = peers.sent(repo);
        loop {
            // What has not been read of the table may be what has not been confirmed.
            if !self.scan_alone(folder, engine, deadline)? {
                return Ok(None);
            }
            let files = self.files(folder, engine)?;
            let stay = self.chunks_that_stay(folder, &files, engine)?;
            let mut held_back: BTreeSet<String> =
                self.rows(folder)?.into_iter().map(|r| r.entity).collect();
            held_back.extend(
                files
                    .into_iter()
                    .filter(|file| file.kept || file.conflict)
                    .map(|file| file.entity),
            );
            let want = need_bytes.saturating_sub(so_far()?).min(self.batch_bytes);
            let mut batch: Vec<Vec<Digest>> = Vec::new();
            let mut planned = 0;
            while planned < want && Instant::now() < deadline {
                let Some(entity) = queue.next() else {
                    break;
                };
                if held_back.contains(&entity) {
                    continue;
                }
                let mut own = Vec::new();
                for digest in engine.head_chunks(&entity)?.unwrap_or_default() {
                    let len = takes_room(engine, digest)?;
                    if !stay.contains(&digest) && len > 0 {
                        planned += len;
                        own.push(digest);
                    }
                }
                if !own.is_empty() {
                    batch.push(own);
                }
            }
            if batch.is_empty() {
                return Ok(None);
            }
            let digests: Vec<Digest> = batch.iter().flatten().copied().collect();
            let (held, blocked) = self.ask_peers(&sent, &digests, peers, deadline);
            let named: BTreeSet<Digest> = batch
                .into_iter()
                .filter(|own| own.iter().all(|digest| held.contains(digest)))
                .flatten()
                .collect();
            if !named.is_empty() {
                evict_counted(engine, &named, failure)?;
            }
            if blocked.is_some() {
                return Ok(blocked.map(str::to_owned));
            }
            if so_far()? >= need_bytes || Instant::now() >= deadline {
                return Ok(None);
            }
        }
    }

    /// Downloads what is kept and is not all here. One file a PC cannot give stops only itself, up
    /// to a few, which is how a PC that went away shows. What came stays. `cancel` is looked at
    /// between steps. It returns how many bytes came.
    pub fn hydrate_kept(
        &self,
        folder: &str,
        engine: &Engine,
        cancel: &dyn Fn() -> bool,
    ) -> Result<u64> {
        let mut brought = 0;
        let mut failures = 0;
        let mut first_failure = None;
        for file in self.files(folder, engine)? {
            if !file.kept {
                continue;
            }
            if cancel() {
                break;
            }
            let hydrated = presence_of(engine, &file.entity).and_then(|before| {
                if before.is_whole() {
                    return Ok(0);
                }
                let after = engine.hydrate(&file.entity, cancel)?;
                Ok(after.local_bytes.saturating_sub(before.local_bytes))
            });
            match hydrated {
                Ok(bytes) => brought += bytes,
                Err(error) => {
                    first_failure.get_or_insert(error);
                    failures += 1;
                    if failures == HYDRATE_FAILURES {
                        break;
                    }
                }
            }
        }
        match first_failure {
            Some(error) if brought == 0 => Err(error),
            _ => Ok(brought),
        }
    }

    /// Whether anything kept is not all here: whether [`Self::hydrate_kept`] has work. It asks no one.
    pub fn kept_missing(&self, folder: &str, engine: &Engine) -> Result<bool> {
        for file in self.files(folder, engine)? {
            if !file.kept {
                continue;
            }
            if !presence_of(engine, &file.entity)?.is_whole() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// About how much one evict is asked to free. It holds the store and the packs while it copies what
/// else is in the segments it touches, and everything that reads waits for that.
const EVICT_BATCH_BYTES: u64 = 256 * 1024 * 1024;
/// How many kept files may fail in one [`Phone::hydrate_kept`], each after a wait of its own.
const HYDRATE_FAILURES: usize = 3;
/// Deleted files looked at for one call of [`Engine::deleted_heads`].
const DELETED_PAGE: usize = 256;

/// One file of a folder, for [`Phone::free_oldest`].
struct FileInFolder {
    entity: String,
    kept: bool,
    conflict: bool,
}

/// Whether the file is marked, or is in a directory that is.
fn is_covered(marks: &BTreeSet<String>, entries: &BTreeMap<String, Entry>, file: &Entry) -> bool {
    let mut current = file;
    // No cycle lasts past this: names are at most so deep, and it says "not kept" of what cannot be walked.
    for _ in 0..4096 {
        if marks.contains(&current.id) {
            return true;
        }
        if current.id == ROOT {
            return false;
        }
        match entries.get(&current.parent) {
            Some(parent) => current = parent,
            None => return false,
        }
    }
    false
}

/// The directories `entity` is in, nearest first.
pub fn ancestors_of(engine: &Engine, entity: &str) -> Vec<String> {
    let store = engine.store.lock().unwrap_or_else(PoisonError::into_inner);
    let entries = &store.projection().entries;
    let mut found = Vec::new();
    let mut current = entries.get(entity);
    while let Some(entry) = current {
        if entry.id == ROOT || found.len() == 4096 {
            break;
        }
        found.push(entry.parent.clone());
        current = entries.get(&entry.parent);
    }
    found
}

/// What the object takes in the packs: nothing if it is not here. An empty file's one chunk takes
/// nothing either, so it is worth neither a question nor a rewrite.
fn takes_room(engine: &Engine, digest: Digest) -> Result<u64> {
    let packs = &engine.working.content().packs;
    Ok(packs.object_len(digest)?.unwrap_or(0))
}

/// How much of the file is here now. The store is not held while it looks in the packs.
fn presence_of(engine: &Engine, entity: &str) -> Result<Presence> {
    let entry = engine
        .store
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(entity)?;
    engine.entry_presence(&entry)
}

/// Evicts `named` once and says what left the disk. An evict that fails may still have freed other
/// segments, so its failure goes in `failure` and the bytes are counted all the same.
fn evict_counted(
    engine: &Engine,
    named: &BTreeSet<Digest>,
    failure: &mut Option<anyhow::Error>,
) -> Result<u64> {
    let packs = &engine.working.content().packs;
    let before = packs.disk_bytes()?;
    if let Err(error) = engine.evict(named) {
        failure.get_or_insert(error);
    }
    Ok(before.saturating_sub(packs.disk_bytes()?))
}

/// Frees what only deleted files hold, up to about `need_bytes`, with one evict for each batch. The
/// owner deleted them everywhere, so no PC is asked. A chunk a live file names stays: a copy that was
/// then deleted shares its chunks with a file that may be the only one holding them. What is live
/// here only saves naming those, and [`evict_deleted`] is what counts.
fn free_deleted(
    engine: &Engine,
    need_bytes: u64,
    batch_bytes: u64,
    deadline: Instant,
    failure: &mut Option<anyhow::Error>,
) -> Result<()> {
    let packs = &engine.working.content().packs;
    let before = packs.disk_bytes()?;
    let mut after: Option<String> = None;
    loop {
        let freed = before.saturating_sub(packs.disk_bytes()?);
        let want = need_bytes.saturating_sub(freed).min(batch_bytes);
        let live = engine.live_head_objects()?;
        let mut named = BTreeSet::new();
        let mut planned = 0;
        let more = 'pages: loop {
            let page = engine.deleted_heads(after.as_deref(), DELETED_PAGE)?;
            for entity in page.entities {
                for digest in engine.head_chunks(&entity)?.unwrap_or_default() {
                    let len = takes_room(engine, digest)?;
                    if !live.contains(&digest) && len > 0 && named.insert(digest) {
                        planned += len;
                    }
                }
                after = Some(entity);
                if planned >= want {
                    break 'pages true;
                }
            }
            match page.next {
                Some(next) => after = Some(next),
                None => break false,
            }
            if Instant::now() >= deadline {
                break true;
            }
        };
        if !named.is_empty() {
            evict_deleted(engine, &named, failure);
        }
        let freed = before.saturating_sub(packs.disk_bytes()?);
        if !more || freed >= need_bytes || Instant::now() >= deadline {
            return Ok(());
        }
    }
}

/// One evict of what only deleted files hold. What `named` was worked out from may be out of date by
/// now, and no PC has been asked about any of it, so this goes by what is live as it runs.
fn evict_deleted(engine: &Engine, named: &BTreeSet<Digest>, failure: &mut Option<anyhow::Error>) {
    if let Err(error) = engine.evict_unless_live(named) {
        failure.get_or_insert(error);
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

/// [`DaemonPeers`] for freeing, which may come long after the last burst, when the lanes may be
/// waiting out a backoff. The first PC with no lane gets one bounded wait for them, and from then
/// on what it answers is the answer. Another call waits again.
pub struct ReachingPeers<'a> {
    pub peers: DaemonPeers<'a>,
    /// How long to wait for a PC, and then for its bulk lane.
    pub within: Duration,
    waited: Cell<bool>,
}

impl<'a> ReachingPeers<'a> {
    pub fn new(peers: DaemonPeers<'a>, within: Duration) -> Self {
        Self {
            peers,
            within,
            waited: Cell::new(false),
        }
    }

    fn wait_for_lanes(&self, installation: &str) {
        // Neither says anything: what counts is what the PC answers after them.
        let _ = self
            .peers
            .daemon
            .reach_a_peer(self.peers.folder, self.within);
        let end = Instant::now() + self.within;
        while matches!(self.peers.held(installation, &[]), Held::Unreachable(_))
            && Instant::now() < end
        {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Confirmer for ReachingPeers<'_> {
    fn sent(&self, repo: &str) -> Vec<(String, u64)> {
        self.peers.sent(repo)
    }

    fn held(&self, installation: &str, digests: &[Digest]) -> Held {
        let answer = self.peers.held(installation, digests);
        if !matches!(answer, Held::Unreachable(_)) || self.waited.replace(true) {
            return answer;
        }
        self.wait_for_lanes(installation);
        self.peers.held(installation, digests)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::RefCell,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    use tkfs::{
        core::{EntryKind, Operation},
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
            self.sent
                .borrow()
                .iter()
                .map(|(p, s)| (p.clone(), *s))
                .collect()
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

    /// A replacement cut short is recovered beside its file by the next start, as a new file of this
    /// device: it waits to send like any other, until a PC holds its bytes.
    #[test]
    fn a_recovered_replacement_waits_to_send_like_any_file() {
        let home = tempfile::tempdir().unwrap();
        let folder = home.path().join("folder");
        let phone = Phone::open(&home.path().join("phone.sqlite")).unwrap();
        {
            let engine = Engine::open(&folder, None).unwrap();
            let token = engine.open_file("file.txt", true, false, true).unwrap();
            engine
                .write(token, 0, b"the good bytes", false, false)
                .unwrap();
            engine.flush(Some(token)).unwrap();
            engine.close(token).unwrap();
            let writer = engine.open_to_replace("file.txt").unwrap();
            engine.truncate(writer, 0, false).unwrap();
            engine.write(writer, 0, b"half", false, false).unwrap();
            engine.shutdown().unwrap();
        }
        let engine = Engine::open(&folder, None).unwrap();
        phone.scan(FOLDER, &engine).unwrap();
        let waiting = phone.waiting(FOLDER).unwrap();
        let recovered = waiting
            .iter()
            .find(|row| row.path == "/file (recovered).txt")
            .unwrap_or_else(|| panic!("{waiting:?}"));
        assert!(
            waiting.iter().any(|row| row.path == "/file.txt"),
            "{waiting:?}"
        );

        let peers = FakePeers::default();
        let top = engine.store.lock().unwrap().feed_position().unwrap();
        peers.sent(PC, top);
        let closure = engine.head_closure(&recovered.entity).unwrap();
        assert!(!closure.digests.is_empty());
        let still = phone.confirm(FOLDER, REPO, &engine, &peers).unwrap();
        assert!(
            phone
                .waiting(FOLDER)
                .unwrap()
                .iter()
                .any(|row| row.path == "/file (recovered).txt"),
            "no PC holds its bytes yet: {still:?}"
        );
        peers.holds(PC, &closure.digests);
        phone.confirm(FOLDER, REPO, &engine, &peers).unwrap();
        assert!(
            !phone
                .waiting(FOLDER)
                .unwrap()
                .iter()
                .any(|row| row.path == "/file (recovered).txt")
        );
        engine.shutdown().unwrap();
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
            self.phone
                .confirm(FOLDER, REPO, &self.engine, &self.peers)
                .unwrap()
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
        fn id(&self, path: &str) -> String {
            self.engine.lookup(path).unwrap().id
        }

        /// How much of the file is here.
        fn here(&self, path: &str) -> u64 {
            self.engine.presence(path).unwrap().local_bytes
        }

        fn is_whole(&self, path: &str) -> bool {
            self.engine.presence(path).unwrap().is_whole()
        }

        /// Everything so far has reached the PC and left the table.
        fn on_the_pc(&self, paths: &[&str]) {
            self.peers.sent(PC, self.top());
            for path in paths {
                self.peers.holds(PC, &self.digests(path));
            }
            assert_eq!(self.confirm().waiting, 0);
        }

        fn free(&self, need: u64) -> Freed {
            self.phone
                .free_oldest(FOLDER, REPO, &self.engine, &self.peers, need)
                .unwrap()
        }

        fn remove(&self, path: &str) -> Freed {
            self.phone
                .remove_download(FOLDER, REPO, &self.id(path), &self.engine, &self.peers)
                .unwrap()
        }

        /// A file's chunks go, as if freed, so that a test starts with them gone.
        fn evicted(&self, path: &str) -> Vec<u8> {
            let bytes = self.read(path);
            let chunks = self.engine.head_chunks(&self.id(path)).unwrap().unwrap();
            let named = chunks.into_iter().collect();
            self.engine.evict(&named).unwrap();
            bytes
        }

        fn read(&self, path: &str) -> Vec<u8> {
            let token = self.engine.open_file(path, false, false, false).unwrap();
            let (_, len) = self.engine.info(token).unwrap();
            let mut bytes = vec![0; len as usize];
            assert_eq!(
                self.engine.read(token, 0, &mut bytes).unwrap(),
                bytes.len(),
                "a short read of {path}"
            );
            self.engine.close(token).unwrap();
            // A draft that a reader left holds what it was made from for a while.
            self.engine.working.reap_released().unwrap();
            bytes
        }

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
        assert_eq!(
            f.confirm(),
            Confirmed {
                waiting: 1,
                unsupported: 0,
                scanning: false,
            }
        );
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
    fn a_rename_of_a_file_whose_bytes_a_peer_wrote_needs_the_exchange_only() {
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
        // The head is this device's rename, over bytes the PC wrote: nothing here to hold.
        assert_eq!(f.confirm().waiting, 0);
        assert_eq!(f.peers.calls(), 0);
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
    fn a_peers_rename_over_bytes_only_this_phone_has_does_not_clear_the_file() {
        // A metered network: the file's event reached the PC and its bytes did not, and the
        // owner renamed it on the PC. That event carries this phone's content.
        let f = fixture();
        f.write("/shared.bin", b"only the phone has these bytes");
        let digests = f.digests("/shared.bin");
        let id = f.engine.lookup("/shared.bin").unwrap().id;
        let mut event = {
            let mut store = f.engine.store.lock().unwrap();
            let mut state = store.entry(&id).unwrap();
            state.name = "renamed.bin".into();
            let heads = store.projection().heads[&id].iter().cloned().collect();
            let branch = store.active.clone();
            store.event(&branch, state, heads, Operation::Rename)
        };
        event.author = "the pc".into();
        f.engine.apply_remote(&[event]).unwrap();
        f.peers.sent(PC, f.top());
        assert_eq!(f.confirm().waiting, 1, "the PC does not hold the bytes");
        assert_eq!(f.paths(), ["/renamed.bin"]);
        f.peers.holds(PC, &digests);
        assert_eq!(f.confirm().waiting, 0);
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
        assert_eq!(
            confirmed,
            Confirmed {
                waiting: 1,
                unsupported: 1,
                scanning: false,
            }
        );
        assert_eq!(f.paths(), ["/a.txt"]);
        // Another PC that can say so clears it.
        f.peers.sent("new", f.top());
        f.peers.holds("new", &f.digests("/a.txt"));
        assert_eq!(f.confirm(), Confirmed::default());
    }

    #[test]
    fn an_unreachable_peer_clears_nothing() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        let confirmed = f.phone.confirm(FOLDER, REPO, &f.engine, &Down).unwrap();
        assert_eq!(
            confirmed,
            Confirmed {
                waiting: 1,
                unsupported: 0,
                scanning: false,
            }
        );
    }

    #[test]
    fn a_peer_that_lacks_one_digest_does_not_confirm() {
        let f = fixture();
        let bytes: Vec<u8> = (0..2 * CHUNK_SIZE as usize + 5)
            .map(|i| (i / 3 % 251) as u8)
            .collect();
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
        let bytes: Vec<u8> = (0..4 * CHUNK_SIZE as usize)
            .map(|i| (i / 5 % 251) as u8)
            .collect();
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
            (0..4 * CHUNK_SIZE as usize)
                .map(|i| ((i / 5 + salt) % 251) as u8)
                .collect()
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
        let other = f
            .engine
            .store
            .lock()
            .unwrap()
            .fork(&snapshot, "other")
            .unwrap();
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
        let bytes: Vec<u8> = (0..2 * CHUNK_SIZE as usize)
            .map(|i| (i / 5 % 251) as u8)
            .collect();
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
        f.phone.scan(FOLDER, &f.engine).unwrap();
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &f.digests("/a.txt"));
        assert_eq!(f.confirm().waiting, 1);
        assert_eq!(f.peers.calls(), 0);
    }

    #[test]
    fn a_table_still_catching_up_says_so_and_clears_nothing() {
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
        let confirmed = f.confirm();
        assert!(confirmed.scanning);
        assert_eq!(f.peers.calls(), 0);
        // With time to finish it counts everything, and the file is what it says.
        f.phone = Phone::open(&f._home.path().join("phone.sqlite")).unwrap();
        let confirmed = f.confirm();
        assert!(!confirmed.scanning);
        assert_eq!(confirmed.waiting, 0);
    }

    #[test]
    fn a_long_journal_is_taken_in_steps_that_resume() {
        let f = fixture();
        {
            let mut store = f.engine.store.lock().unwrap();
            for index in 0..1500 {
                store
                    .create(&format!("/d{index}"), EntryKind::Directory)
                    .unwrap();
            }
        }
        let past = Instant::now() - Duration::from_secs(1);
        // No time at all: nothing is read, and it says so.
        assert!(!f.phone.scan_until(FOLDER, &f.engine, past).unwrap());
        assert!(f.phone.waiting(FOLDER).unwrap().is_empty());
        f.phone.scan(FOLDER, &f.engine).unwrap();
        assert_eq!(f.phone.waiting(FOLDER).unwrap().len(), 1500);
    }

    #[test]
    fn a_position_beyond_the_journal_starts_the_folder_over() {
        // Events taken back by a kill: their sequences are used again by what comes next.
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.write("/b.txt", b"there");
        f.phone.scan(FOLDER, &f.engine).unwrap();
        let top = f.top();
        lock(&f.phone.db)
            .execute(
                "UPDATE scanned SET position=?2 WHERE folder=?1",
                params![FOLDER, top + 10],
            )
            .unwrap();
        f.phone.scan(FOLDER, &f.engine).unwrap();
        let mut paths = f.paths();
        paths.sort();
        assert_eq!(paths, ["/a.txt", "/b.txt"]);
        let position: u64 = lock(&f.phone.db)
            .query_row(
                "SELECT position FROM scanned WHERE folder=?1",
                [FOLDER],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(position, top);
    }

    #[test]
    fn a_row_for_an_event_the_journal_lost_clears_and_one_for_a_live_event_does_not() {
        let f = fixture();
        f.write("/a.txt", b"hello");
        f.phone.scan(FOLDER, &f.engine).unwrap();
        // A ghost: an entity and sequence that never were, as after a rolled-back create.
        lock(&f.phone.db)
            .execute(
                "INSERT INTO unconfirmed(folder,entity,seq,path) VALUES(?1,'no-such-entity',1,'/ghost')",
                [FOLDER],
            )
            .unwrap();
        f.peers.sent(PC, f.top());
        let confirmed = f.confirm();
        assert_eq!(f.paths(), ["/a.txt"]);
        assert_eq!(confirmed.waiting, 1);
    }

    #[test]
    fn a_database_that_cannot_open_fails_with_its_code() {
        let home = tempfile::tempdir().unwrap();
        // A directory where the file should be.
        let error = Phone::open(home.path())
            .err()
            .expect("a directory is not a database");
        assert_eq!(
            tkfs::model::errors::ErrorWire::from_error(&error).code,
            "PHONE_DB_UNAVAILABLE"
        );
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
        assert_eq!(
            f.phone
                .confirm("one", REPO, &f.engine, &f.peers)
                .unwrap()
                .waiting,
            0
        );
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

    /// `chunks` chunks and a little, none of them like another's or another `seed`'s.
    fn distinct(seed: u8, chunks: usize) -> Vec<u8> {
        let mut bytes = vec![seed; chunks * CHUNK_SIZE as usize + 10];
        for (index, chunk) in bytes.chunks_mut(CHUNK_SIZE as usize).enumerate() {
            chunk[..8].copy_from_slice(&(index as u64).to_le_bytes());
        }
        bytes
    }

    const CHUNKS: usize = 8;
    /// What a file of [`CHUNKS`] chunks gives back.
    const FILE_BYTES: u64 = CHUNKS as u64 * CHUNK_SIZE + 10;

    /// No PC there at all, with its cursor where it was.
    struct Down;
    impl Confirmer for Down {
        fn sent(&self, _: &str) -> Vec<(String, u64)> {
            vec![("pc".into(), u64::MAX)]
        }
        fn held(&self, _: &str, _: &[Digest]) -> Held {
            Held::Unreachable("no lane".into())
        }
    }

    #[test]
    fn oldest_opened_goes_first_and_stops_when_enough_is_freed() {
        let f = fixture();
        for (seed, path) in ["/a", "/b", "/c", "/never"].iter().enumerate() {
            f.write(path, &distinct(seed as u8 + 1, CHUNKS));
        }
        f.on_the_pc(&["/a", "/b", "/c", "/never"]);
        f.phone.opened(FOLDER, &f.id("/a"), 100).unwrap();
        f.phone.opened(FOLDER, &f.id("/b"), 300).unwrap();
        f.phone.opened(FOLDER, &f.id("/c"), 200).unwrap();
        // More than one file's worth and less than two.
        let need = FILE_BYTES + CHUNK_SIZE;
        let freed = f.free(need);
        assert_eq!(freed.blocked, None);
        assert!(freed.bytes >= need, "{freed:?}");
        assert!(freed.bytes < 2 * FILE_BYTES + CHUNK_SIZE, "{freed:?}");
        assert_eq!(f.here("/never"), 0, "one never opened goes first");
        assert_eq!(f.here("/a"), 0, "then the one opened longest ago");
        assert!(f.is_whole("/c"));
        assert!(f.is_whole("/b"));
        // A file opened again moves to the end, and the next to go is the one left oldest.
        f.phone.opened(FOLDER, &f.id("/c"), 400).unwrap();
        f.free(FILE_BYTES);
        assert_eq!(f.here("/b"), 0);
        assert!(f.is_whole("/c"));
    }

    /// A freed file comes back from the PC by the demand path, and reads the same.
    #[test]
    fn a_freed_file_reads_back_the_same_from_what_the_pc_holds() {
        let f = fixture();
        let bytes = distinct(1, CHUNKS);
        f.write("/a", &bytes);
        f.on_the_pc(&["/a"]);
        let supplier = Supplier::of(&f, &bytes);
        assert!(f.free(FILE_BYTES).bytes >= FILE_BYTES);
        assert_eq!(f.here("/a"), 0);
        f.engine.working.content().set_fetcher(supplier);
        assert!(f.read("/a") == bytes);
    }

    #[test]
    fn a_marked_file_is_never_freed() {
        let f = fixture();
        f.write("/kept", &distinct(1, CHUNKS));
        f.write("/other", &distinct(2, CHUNKS));
        f.on_the_pc(&["/kept", "/other"]);
        f.phone.mark(FOLDER, &f.id("/kept"), true).unwrap();
        let freed = f.free(10 * FILE_BYTES);
        assert_eq!(f.here("/other"), 0);
        assert!(f.is_whole("/kept"));
        assert!(freed.bytes >= FILE_BYTES && freed.bytes < 2 * FILE_BYTES);
        // With the mark gone it is a file like any other.
        f.phone.mark(FOLDER, &f.id("/kept"), false).unwrap();
        f.free(FILE_BYTES);
        assert_eq!(f.here("/kept"), 0);
    }

    #[test]
    fn a_marked_folder_covers_files_that_arrive_later() {
        let f = fixture();
        let directory = f.engine.open_file("/d", true, true, false).unwrap();
        f.engine.close(directory).unwrap();
        let inner = f.engine.open_file("/d/e", true, true, false).unwrap();
        f.engine.close(inner).unwrap();
        f.phone.mark(FOLDER, &f.id("/d"), true).unwrap();
        f.write("/d/e/later", &distinct(1, CHUNKS));
        f.write("/outside", &distinct(2, CHUNKS));
        f.on_the_pc(&["/d/e/later", "/outside"]);
        let later = f.id("/d/e/later");
        assert_eq!(
            ancestors_of(&f.engine, &later),
            vec![f.id("/d/e"), f.id("/d"), ROOT.to_owned()]
        );
        let ancestors = ancestors_of(&f.engine, &later);
        assert!(f.phone.is_kept(FOLDER, &later, &ancestors).unwrap());
        assert!(
            !f.phone
                .is_kept(FOLDER, &f.id("/outside"), &[ROOT.to_owned()])
                .unwrap()
        );
        f.free(10 * FILE_BYTES);
        assert_eq!(f.here("/outside"), 0);
        assert!(f.is_whole("/d/e/later"));
    }

    #[test]
    fn a_mark_follows_a_rename() {
        let f = fixture();
        f.write("/before", &distinct(1, CHUNKS));
        let id = f.id("/before");
        f.phone.mark(FOLDER, &id, true).unwrap();
        f.rename("/before", "/after");
        f.on_the_pc(&["/after"]);
        assert!(f.phone.is_kept(FOLDER, &id, &[]).unwrap());
        assert_eq!(f.free(10 * FILE_BYTES).bytes, 0);
        assert!(f.is_whole("/after"));
    }

    #[test]
    fn a_chunk_shared_with_a_marked_file_stays() {
        let f = fixture();
        let kept = distinct(1, CHUNKS);
        // The same chunks, and then some of its own.
        let mut freed_file = kept[..CHUNKS * CHUNK_SIZE as usize].to_vec();
        freed_file.extend(distinct(2, CHUNKS));
        f.write("/kept", &kept);
        f.write("/freed", &freed_file);
        f.on_the_pc(&["/kept", "/freed"]);
        f.phone.mark(FOLDER, &f.id("/kept"), true).unwrap();
        f.free(10 * FILE_BYTES);
        assert!(f.is_whole("/kept"));
        assert!(f.read("/kept") == kept);
        let left = f.here("/freed");
        assert_eq!(left, CHUNKS as u64 * CHUNK_SIZE, "the shared part is here");
    }

    #[test]
    fn a_file_the_peer_does_not_hold_is_not_freed() {
        let f = fixture();
        f.write("/lost", &distinct(1, CHUNKS));
        f.write("/there", &distinct(2, CHUNKS));
        f.on_the_pc(&["/lost", "/there"]);
        // The PC lost most of one of them: none of it goes, and the other does.
        let digests = f.digests("/lost");
        f.peers
            .held
            .borrow_mut()
            .get_mut(PC)
            .unwrap()
            .remove(&digests[1]);
        let freed = f.free(10 * FILE_BYTES);
        assert_eq!(freed.blocked, None);
        assert!(f.is_whole("/lost"));
        assert_eq!(f.here("/there"), 0);
    }

    #[test]
    fn an_unreachable_peer_frees_nothing_and_says_why() {
        let f = fixture();
        f.write("/a", &distinct(1, CHUNKS));
        f.on_the_pc(&["/a"]);
        let freed = f
            .phone
            .free_oldest(FOLDER, REPO, &f.engine, &Down, FILE_BYTES)
            .unwrap();
        assert_eq!(
            freed,
            Freed {
                bytes: 0,
                blocked: Some("PC_UNREACHABLE".into())
            }
        );
        assert!(f.is_whole("/a"));
    }

    #[test]
    fn no_peer_with_the_folder_frees_nothing_and_says_why() {
        let f = fixture();
        f.write("/a", &distinct(1, CHUNKS));
        f.on_the_pc(&["/a"]);
        f.peers.sent.borrow_mut().clear();
        let freed = f.free(FILE_BYTES);
        assert_eq!(freed.blocked.as_deref(), Some("PC_UNREACHABLE"));
        assert!(f.is_whole("/a"));
    }

    #[test]
    fn a_peer_without_the_feature_frees_nothing_and_says_why() {
        let f = fixture();
        f.write("/a", &distinct(1, CHUNKS));
        f.on_the_pc(&["/a"]);
        f.peers.without_the_feature(PC);
        let freed = f.free(FILE_BYTES);
        assert_eq!(
            freed,
            Freed {
                bytes: 0,
                blocked: Some("PC_TOO_OLD".into())
            }
        );
        assert!(f.is_whole("/a"));
    }

    /// One PC that is away says nothing of what another holds.
    #[test]
    fn a_second_peer_answers_for_one_that_cannot() {
        let f = fixture();
        f.write("/a", &distinct(1, CHUNKS));
        f.on_the_pc(&["/a"]);
        f.peers.without_the_feature(PC);
        f.peers.sent("new", f.top());
        f.peers.holds("new", &f.digests("/a"));
        let freed = f.free(FILE_BYTES);
        assert_eq!(freed.blocked, None);
        assert_eq!(f.here("/a"), 0);
    }

    /// Its row says so without a question, and whatever a PC might say of its bytes.
    #[test]
    fn a_file_waiting_to_send_is_not_asked_about_and_stays() {
        let f = fixture();
        f.write("/sent", &distinct(1, CHUNKS));
        f.on_the_pc(&["/sent"]);
        f.write("/waiting", &distinct(2, CHUNKS));
        f.peers.holds(PC, &f.digests("/waiting"));
        let asked = f.peers.asked.borrow().iter().sum::<usize>();
        f.free(10 * FILE_BYTES);
        assert!(f.is_whole("/waiting"));
        assert_eq!(f.here("/sent"), 0);
        let digests = f.peers.asked.borrow().iter().sum::<usize>() - asked;
        assert_eq!(digests, CHUNKS + 1, "only the sent file was asked about");
    }

    /// A change the table has not read yet is one nobody has confirmed.
    #[test]
    fn a_file_written_just_now_is_waiting_before_anything_asks() {
        let f = fixture();
        f.write("/a", &distinct(1, CHUNKS));
        f.peers.sent(PC, f.top());
        f.peers.holds(PC, &f.digests("/a"));
        // No `confirm` has run since it was written.
        assert_eq!(f.free(10 * FILE_BYTES).bytes, 0);
        assert!(f.is_whole("/a"));
    }

    #[test]
    fn a_file_held_open_stays_until_it_is_closed() {
        let f = fixture();
        f.write("/open", &distinct(1, CHUNKS));
        let token = f.engine.open_file("/open", false, false, false).unwrap();
        f.on_the_pc(&["/open"]);
        f.free(10 * FILE_BYTES);
        assert!(f.is_whole("/open"));
        f.engine.close(token).unwrap();
        f.engine.working.reap_released().unwrap();
        f.free(10 * FILE_BYTES);
        assert_eq!(f.here("/open"), 0);
    }

    /// Evict would refuse it anyway, so no question is spent on it.
    #[test]
    fn a_conflict_is_not_asked_about() {
        let f = fixture();
        f.write("/settle", &distinct(1, CHUNKS));
        f.on_the_pc(&["/settle"]);
        let id = f.id("/settle");
        let parents: Vec<String> = f.engine.store.lock().unwrap().projection().heads[&id]
            .iter()
            .cloned()
            .collect();
        // Two edits by peers, of the same version.
        for seed in [2, 3] {
            let content = f
                .engine
                .working
                .content()
                .import(&distinct(seed, CHUNKS))
                .unwrap();
            let mut event = {
                let mut store = f.engine.store.lock().unwrap();
                let mut state = store.entry(&id).unwrap();
                state.content = Some(content);
                let branch = store.active.clone();
                store.event(&branch, state, parents.clone(), Operation::Content)
            };
            event.author = format!("peer {seed}");
            f.engine.apply_remote(&[event]).unwrap();
        }
        assert_eq!(
            f.engine.store.lock().unwrap().projection().heads[&id].len(),
            2
        );
        f.on_the_pc(&[]);
        let asked = f.peers.calls();
        f.free(10 * FILE_BYTES);
        assert_eq!(f.peers.calls(), asked);
        assert!(f.is_whole("/settle"));
    }

    /// Superseded versions cannot be named by a head, and the phone has no other collection.
    #[test]
    fn what_is_dead_goes_first_and_asks_no_one() {
        let f = fixture();
        f.write("/a", &distinct(1, 40));
        f.rewrite("/a", b"small now");
        f.write("/b", &distinct(2, 1));
        f.on_the_pc(&["/a", "/b"]);
        // A collect passes over the segment still being written.
        let packs = &f.engine.working.content().packs;
        let named = f.digests("/b").into_iter().collect();
        packs.close_segment_holding(&named).unwrap();
        let asked = f.peers.calls();
        let freed = f.free(10 * CHUNK_SIZE);
        assert!(freed.bytes >= 40 * CHUNK_SIZE, "{freed:?}");
        assert_eq!(f.peers.calls(), asked);
        assert!(f.is_whole("/a"));
        assert!(f.is_whole("/b"));
    }

    #[test]
    fn deleted_files_go_before_any_live_one_and_ask_no_one() {
        let f = fixture();
        f.write("/gone", &distinct(1, CHUNKS));
        f.write("/old", &distinct(2, CHUNKS));
        f.on_the_pc(&["/gone", "/old"]);
        f.delete("/gone");
        let asked = f.peers.calls();
        let freed = f.free(FILE_BYTES);
        assert!(freed.bytes >= FILE_BYTES, "{freed:?}");
        assert_eq!(f.peers.calls(), asked);
        assert!(f.is_whole("/old"));
    }

    /// A copy that was then deleted. The copy that stays has not gone anywhere, and this is all
    /// there is of it.
    #[test]
    fn a_deleted_files_chunk_shared_with_a_live_unsent_file_stays() {
        let f = fixture();
        let bytes = distinct(1, CHUNKS);
        f.write("/copy", &bytes);
        f.write("/original", &bytes);
        f.delete("/copy");
        let freed = f
            .phone
            .free_oldest(FOLDER, REPO, &f.engine, &Down, 10 * FILE_BYTES)
            .unwrap();
        assert_eq!(freed.bytes, 0, "{freed:?}");
        assert!(f.is_whole("/original"));
        assert!(f.read("/original") == bytes);
    }

    /// What cannot leave does not stand in front of what can.
    #[test]
    fn a_deleted_file_that_must_stay_does_not_hide_those_after_it() {
        let f = fixture();
        let shared = distinct(1, 1);
        f.write("/live", &shared);
        // More than a page's worth that stays, whatever order they list in.
        for index in 0..DELETED_PAGE + 20 {
            let path = format!("/copy-{index}");
            f.write(&path, &shared);
            f.delete(&path);
        }
        f.write("/gone", &distinct(2, CHUNKS));
        let gone = f.id("/gone");
        f.delete("/gone");
        let named: BTreeSet<Digest> = f
            .engine
            .head_chunks(&gone)
            .unwrap()
            .unwrap()
            .into_iter()
            .collect();
        let freed = f
            .phone
            .free_oldest(FOLDER, REPO, &f.engine, &Down, FILE_BYTES)
            .unwrap();
        assert!(freed.bytes >= FILE_BYTES, "{freed:?}");
        let packs = &f.engine.working.content().packs;
        for digest in named {
            assert!(!packs.contains(digest).unwrap());
        }
        assert!(f.read("/live") == shared);
    }

    /// With the PC away there may still be bytes nobody needs.
    #[test]
    fn with_the_pc_away_what_needs_no_pc_still_goes_and_it_says_why_no_more_did() {
        let f = fixture();
        f.write("/gone", &distinct(1, CHUNKS));
        f.write("/a", &distinct(2, CHUNKS));
        f.on_the_pc(&["/gone", "/a"]);
        f.delete("/gone");
        let freed = f
            .phone
            .free_oldest(FOLDER, REPO, &f.engine, &Down, 2 * FILE_BYTES)
            .unwrap();
        assert!(freed.bytes >= FILE_BYTES, "{freed:?}");
        assert_eq!(freed.blocked.as_deref(), Some("PC_UNREACHABLE"));
        assert!(f.is_whole("/a"));
    }

    /// Out of time before it starts on the files: nothing asked, and the next call goes on.
    #[test]
    fn with_no_time_left_it_stops_with_what_it_has() {
        let mut f = fixture();
        f.write("/a", &distinct(1, CHUNKS));
        f.on_the_pc(&["/a"]);
        f.phone = Phone::open(&f._home.path().join("phone.sqlite"))
            .unwrap()
            .with_budget(Budget {
                digests: 100,
                time: Duration::ZERO,
            });
        let asked = f.peers.calls();
        assert_eq!(f.free(FILE_BYTES), Freed::default());
        assert_eq!(f.peers.calls(), asked);
        assert!(f.is_whole("/a"));
        f.phone = Phone::open(&f._home.path().join("phone.sqlite")).unwrap();
        assert!(f.free(FILE_BYTES).bytes >= FILE_BYTES);
    }

    /// One free gives back all that is asked, however many files that takes.
    #[test]
    fn a_large_need_is_met_in_one_call() {
        let f = fixture();
        let paths: Vec<String> = (0..12).map(|index| format!("/f{index}")).collect();
        for (index, path) in paths.iter().enumerate() {
            f.write(path, &distinct(index as u8 + 1, CHUNKS));
        }
        f.on_the_pc(&paths.iter().map(String::as_str).collect::<Vec<_>>());
        let need = 11 * FILE_BYTES;
        assert!(f.free(need).bytes >= need);
    }

    #[test]
    fn remove_download_frees_a_file_the_pc_holds() {
        let f = fixture();
        f.write("/a", &distinct(1, CHUNKS));
        f.write("/b", &distinct(2, CHUNKS));
        f.on_the_pc(&["/a", "/b"]);
        let freed = f.remove("/a");
        assert_eq!(freed.blocked, None);
        assert!(
            freed.bytes >= FILE_BYTES && freed.bytes < 2 * FILE_BYTES,
            "{freed:?}"
        );
        assert_eq!(f.here("/a"), 0);
        assert!(f.is_whole("/b"));
        // Nothing left of it to remove, and no question to ask about it.
        let asked = f.peers.calls();
        assert_eq!(f.remove("/a"), Freed::default());
        assert_eq!(f.peers.calls(), asked);
    }

    #[test]
    fn remove_download_of_a_file_not_on_the_pc_says_so() {
        let f = fixture();
        f.write("/waiting", &distinct(1, CHUNKS));
        let asked = f.peers.calls();
        let freed = f.remove("/waiting");
        assert_eq!(freed.blocked.as_deref(), Some("NOT_ON_PC"));
        assert_eq!(f.peers.calls(), asked, "a row needs no question");
        assert!(f.is_whole("/waiting"));
        // No row, and the PC does not hold all of it.
        f.on_the_pc(&["/waiting"]);
        let digests = f.digests("/waiting");
        f.peers
            .held
            .borrow_mut()
            .get_mut(PC)
            .unwrap()
            .remove(&digests[1]);
        assert_eq!(f.remove("/waiting").blocked.as_deref(), Some("NOT_ON_PC"));
        assert!(f.is_whole("/waiting"));
    }

    #[test]
    fn remove_download_with_the_pc_away_or_too_old_says_which() {
        let f = fixture();
        f.write("/a", &distinct(1, CHUNKS));
        f.on_the_pc(&["/a"]);
        let id = f.id("/a");
        let away = f
            .phone
            .remove_download(FOLDER, REPO, &id, &f.engine, &Down)
            .unwrap();
        assert_eq!(away.blocked.as_deref(), Some("PC_UNREACHABLE"));
        f.peers.without_the_feature(PC);
        assert_eq!(f.remove("/a").blocked.as_deref(), Some("PC_TOO_OLD"));
        assert!(f.is_whole("/a"));
    }

    #[test]
    fn remove_download_leaves_a_kept_file_and_what_it_shares_with_one() {
        let f = fixture();
        let kept = distinct(1, CHUNKS);
        let mut other = kept[..CHUNKS * CHUNK_SIZE as usize].to_vec();
        other.extend(distinct(2, CHUNKS));
        f.write("/kept", &kept);
        f.write("/other", &other);
        f.on_the_pc(&["/kept", "/other"]);
        f.phone.mark(FOLDER, &f.id("/kept"), true).unwrap();
        assert_eq!(f.remove("/kept"), Freed::default());
        assert!(f.is_whole("/kept"));
        f.remove("/other");
        assert!(f.is_whole("/kept"));
        assert_eq!(f.here("/other"), CHUNKS as u64 * CHUNK_SIZE);
    }

    /// What a free of deleted files named is checked again as it goes: a file saved since then has
    /// the same chunks, no copy of its own, and no PC has been asked about it.
    #[test]
    fn a_file_saved_while_a_deleted_one_is_freed_keeps_its_chunks() {
        let f = fixture();
        let bytes = distinct(1, CHUNKS);
        f.write("/copy", &bytes);
        let id = f.id("/copy");
        f.delete("/copy");
        let named: BTreeSet<Digest> = f
            .engine
            .head_chunks(&id)
            .unwrap()
            .unwrap()
            .into_iter()
            .collect();
        f.write("/saved", &bytes);
        let mut failure = None;
        evict_deleted(&f.engine, &named, &mut failure);
        assert!(failure.is_none(), "{failure:?}");
        assert!(f.is_whole("/saved"));
        assert!(f.read("/saved") == bytes);
    }

    /// A kept file's manifest that will not read says nothing of what must stay. A row may name a
    /// directory or something the journal took back, and those have no chunks to keep.
    #[test]
    fn what_stays_tells_a_file_with_no_chunks_from_one_it_cannot_read() {
        let f = fixture();
        let directory = f.engine.open_file("/d", true, true, false).unwrap();
        f.engine.close(directory).unwrap();
        f.write("/kept", b"x");
        assert!(f.confirm().waiting >= 2);
        lock(&f.phone.db)
            .execute(
                "INSERT INTO unconfirmed(folder,entity,seq,path) VALUES(?1,'no-such-entity',1,'/ghost')",
                [FOLDER],
            )
            .unwrap();
        let files = f.phone.files(FOLDER, &f.engine).unwrap();
        assert!(f.phone.chunks_that_stay(FOLDER, &files, &f.engine).is_ok());

        let content = f.engine.working.content();
        let garbage = content
            .packs
            .put(tkfs::content::Kind::Manifest, b"not a manifest")
            .unwrap();
        let id = f.id("/kept");
        let mut event = {
            let mut store = f.engine.store.lock().unwrap();
            let mut state = store.entry(&id).unwrap();
            state.content =
                Some(FileContentRef::new(ContentKind::Manifest, garbage, 4 * CHUNK_SIZE).unwrap());
            let heads = store.projection().heads[&id].iter().cloned().collect();
            let branch = store.active.clone();
            store.event(&branch, state, heads, Operation::Content)
        };
        event.author = "the pc".into();
        f.engine.apply_remote(&[event]).unwrap();
        f.phone.mark(FOLDER, &id, true).unwrap();
        let files = f.phone.files(FOLDER, &f.engine).unwrap();
        assert!(f.phone.chunks_that_stay(FOLDER, &files, &f.engine).is_err());
    }

    /// Answers as the PC does, and does something else the first time it is asked.
    struct ThenDoes<'a, T: Fn()> {
        peers: &'a FakePeers,
        then: T,
        done: std::cell::Cell<bool>,
    }
    impl<T: Fn()> Confirmer for ThenDoes<'_, T> {
        fn sent(&self, repo: &str) -> Vec<(String, u64)> {
            Confirmer::sent(self.peers, repo)
        }
        fn held(&self, installation: &str, digests: &[Digest]) -> Held {
            let answer = self.peers.held(installation, digests);
            if !self.done.replace(true) {
                (self.then)();
            }
            answer
        }
    }

    fn in_batches_of_one_file(f: &mut Fixture) {
        f.phone = Phone::open(&f._home.path().join("phone.sqlite"))
            .unwrap()
            .with_batch_bytes(FILE_BYTES);
    }

    /// A free can last a while. What stays is worked out again for each batch, or a mark made after
    /// the first would not count.
    #[test]
    fn a_mark_made_while_a_free_runs_counts_for_the_next_batch() {
        let mut f = fixture();
        in_batches_of_one_file(&mut f);
        for (seed, path) in ["/first", "/second", "/third"].iter().enumerate() {
            f.write(path, &distinct(seed as u8 + 1, CHUNKS));
        }
        f.on_the_pc(&["/first", "/second", "/third"]);
        for (at, path) in ["/first", "/second", "/third"].iter().enumerate() {
            f.phone.opened(FOLDER, &f.id(path), at as u64 + 1).unwrap();
        }
        let second = f.id("/second");
        let peers = ThenDoes {
            peers: &f.peers,
            then: || f.phone.mark(FOLDER, &second, true).unwrap(),
            done: Default::default(),
        };
        f.phone
            .free_oldest(FOLDER, REPO, &f.engine, &peers, 3 * FILE_BYTES)
            .unwrap();
        assert_eq!(f.here("/first"), 0);
        assert!(f.is_whole("/second"), "it was marked after the first batch");
        assert_eq!(f.here("/third"), 0);
    }

    /// So is a file saved in the meantime, which is waiting from then on.
    #[test]
    fn a_file_edited_while_a_free_runs_stays_for_the_next_batch() {
        let mut f = fixture();
        in_batches_of_one_file(&mut f);
        f.write("/first", &distinct(1, CHUNKS));
        f.write("/second", &distinct(2, CHUNKS));
        f.on_the_pc(&["/first", "/second"]);
        f.phone.opened(FOLDER, &f.id("/first"), 1).unwrap();
        f.phone.opened(FOLDER, &f.id("/second"), 2).unwrap();
        // The PC already holds these bytes: only the row says the event has not gone.
        let edited = distinct(9, CHUNKS);
        let digests: Vec<Digest> = edited
            .chunks(CHUNK_SIZE as usize)
            .map(Digest::bytes)
            .collect();
        f.peers.holds(PC, &digests);
        let peers = ThenDoes {
            peers: &f.peers,
            then: || f.rewrite("/second", &edited),
            done: Default::default(),
        };
        f.phone
            .free_oldest(FOLDER, REPO, &f.engine, &peers, 2 * FILE_BYTES)
            .unwrap();
        assert!(f.is_whole("/second"), "it has a row now");
    }

    /// An evict's locks are held for as long as it copies. Each batch is still one evict.
    #[test]
    fn a_free_of_files_evicts_in_batches_of_its_size() {
        let mut f = fixture();
        in_batches_of_one_file(&mut f);
        let paths: Vec<String> = (0..3).map(|index| format!("/f{index}")).collect();
        for (index, path) in paths.iter().enumerate() {
            f.write(path, &distinct(index as u8 + 1, CHUNKS));
        }
        f.on_the_pc(&paths.iter().map(String::as_str).collect::<Vec<_>>());
        let packs = &f.engine.working.content().packs;
        let before = packs.generation();
        assert!(f.free(3 * FILE_BYTES).bytes >= 3 * FILE_BYTES);
        // One for the collect that comes first, and one for each evict.
        assert_eq!(packs.generation() - before, 1 + 3);
    }

    #[test]
    fn a_free_of_deleted_files_evicts_in_batches_of_its_size() {
        let mut f = fixture();
        in_batches_of_one_file(&mut f);
        for (index, path) in ["/a", "/b", "/c"].iter().enumerate() {
            f.write(path, &distinct(index as u8 + 1, CHUNKS));
            f.delete(path);
        }
        let packs = &f.engine.working.content().packs;
        let before = packs.generation();
        assert!(f.free(3 * FILE_BYTES).bytes >= 3 * FILE_BYTES);
        assert_eq!(packs.generation() - before, 1 + 3);
    }

    /// One evict for what does fit in a batch, however many files that is.
    #[test]
    fn a_batch_is_one_evict() {
        let f = fixture();
        let paths: Vec<String> = (0..3).map(|index| format!("/f{index}")).collect();
        for (index, path) in paths.iter().enumerate() {
            f.write(path, &distinct(index as u8 + 1, CHUNKS));
        }
        f.on_the_pc(&paths.iter().map(String::as_str).collect::<Vec<_>>());
        let packs = &f.engine.working.content().packs;
        let before = packs.generation();
        assert!(f.free(3 * FILE_BYTES).bytes >= 3 * FILE_BYTES);
        assert_eq!(packs.generation() - before, 1 + 1);
    }

    /// One chunk that takes no room: no question, and nothing to rewrite.
    #[test]
    fn an_empty_file_is_not_asked_about() {
        let f = fixture();
        f.write("/empty", b"");
        f.on_the_pc(&["/empty"]);
        let asked = f.peers.calls();
        assert_eq!(f.free(FILE_BYTES).bytes, 0);
        assert_eq!(f.remove("/empty"), Freed::default());
        assert_eq!(f.peers.calls(), asked);
    }

    /// Gives back what it was told of, as a PC would, and counts the fetches.
    struct Supplier {
        packs: tkfs::content::PackStore,
        objects: BTreeMap<Digest, Vec<u8>>,
        fetches: AtomicUsize,
        /// Whether it has any of it: a PC that went away does not.
        away: AtomicBool,
    }

    impl Supplier {
        fn of(f: &Fixture, bytes: &[u8]) -> Arc<Supplier> {
            let objects = bytes
                .chunks(CHUNK_SIZE as usize)
                .map(|chunk| (Digest::bytes(chunk), chunk.to_vec()))
                .collect();
            Arc::new(Supplier {
                packs: f.engine.working.content().packs.clone(),
                objects,
                fetches: AtomicUsize::new(0),
                away: AtomicBool::new(false),
            })
        }
    }

    impl tkfs::content::ObjectFetcher for Supplier {
        fn fetch(&self, objects: &[(Digest, tkfs::content::Kind)]) -> Result<()> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            anyhow::ensure!(!self.away.load(Ordering::SeqCst), "the PC is away");
            for (digest, kind) in objects {
                if let Some(bytes) = self.objects.get(digest) {
                    self.packs.put(*kind, bytes)?;
                }
            }
            Ok(())
        }
    }

    #[test]
    fn what_is_kept_downloads_and_says_how_much_came() {
        let f = fixture();
        let bytes = distinct(1, CHUNKS);
        f.write("/kept", &bytes);
        f.write("/other", &distinct(2, CHUNKS));
        f.on_the_pc(&["/kept", "/other"]);
        let supplier = Supplier::of(&f, &bytes);
        f.evicted("/kept");
        f.evicted("/other");
        f.engine.working.content().set_fetcher(supplier);
        assert!(!f.phone.kept_missing(FOLDER, &f.engine).unwrap());
        assert_eq!(
            f.phone.hydrate_kept(FOLDER, &f.engine, &|| false).unwrap(),
            0
        );
        f.phone.mark(FOLDER, &f.id("/kept"), true).unwrap();
        assert!(f.phone.kept_missing(FOLDER, &f.engine).unwrap());
        let brought = f.phone.hydrate_kept(FOLDER, &f.engine, &|| false).unwrap();
        assert_eq!(brought, FILE_BYTES);
        assert!(f.is_whole("/kept"));
        assert_eq!(f.here("/other"), 0, "only what is marked comes");
        assert!(!f.phone.kept_missing(FOLDER, &f.engine).unwrap());
        // Nothing left to do is no work.
        assert_eq!(
            f.phone.hydrate_kept(FOLDER, &f.engine, &|| false).unwrap(),
            0
        );
        assert!(f.read("/kept") == bytes);
    }

    #[test]
    fn a_marked_folder_downloads_what_arrives_in_it() {
        let f = fixture();
        let directory = f.engine.open_file("/d", true, true, false).unwrap();
        f.engine.close(directory).unwrap();
        f.phone.mark(FOLDER, &f.id("/d"), true).unwrap();
        let bytes = distinct(1, CHUNKS);
        f.write("/d/later", &bytes);
        f.on_the_pc(&["/d/later"]);
        let supplier = Supplier::of(&f, &bytes);
        f.evicted("/d/later");
        f.engine.working.content().set_fetcher(supplier);
        f.phone.hydrate_kept(FOLDER, &f.engine, &|| false).unwrap();
        assert!(f.is_whole("/d/later"));
    }

    #[test]
    fn a_cancel_stops_a_hydrate_between_steps_and_what_came_stays() {
        let f = fixture();
        let bytes = distinct(1, CHUNKS);
        f.write("/kept", &bytes);
        f.on_the_pc(&["/kept"]);
        let supplier = Supplier::of(&f, &bytes);
        f.evicted("/kept");
        f.engine.working.content().set_fetcher(supplier.clone());
        f.phone.mark(FOLDER, &f.id("/kept"), true).unwrap();
        assert_eq!(
            f.phone.hydrate_kept(FOLDER, &f.engine, &|| true).unwrap(),
            0
        );
        assert_eq!(supplier.fetches.load(Ordering::SeqCst), 0);
        assert_eq!(f.here("/kept"), 0);
        assert_eq!(
            f.phone.hydrate_kept(FOLDER, &f.engine, &|| false).unwrap(),
            FILE_BYTES
        );
    }

    #[test]
    fn with_the_pc_away_a_hydrate_fails_and_says_why_after_a_few_tries() {
        let f = fixture();
        for index in 0..HYDRATE_FAILURES + 2 {
            let path = format!("/kept-{index}");
            f.write(&path, &distinct(index as u8 + 1, 1));
            f.phone.mark(FOLDER, &f.id(&path), true).unwrap();
        }
        let paths: Vec<String> = (0..HYDRATE_FAILURES + 2)
            .map(|i| format!("/kept-{i}"))
            .collect();
        f.on_the_pc(&paths.iter().map(String::as_str).collect::<Vec<_>>());
        let supplier = Supplier::of(&f, &[]);
        supplier.away.store(true, Ordering::SeqCst);
        for path in &paths {
            f.evicted(path);
        }
        f.engine.working.content().set_fetcher(supplier.clone());
        let failed = f
            .phone
            .hydrate_kept(FOLDER, &f.engine, &|| false)
            .unwrap_err();
        assert!(
            format!("{failed:#}").contains("the PC is away"),
            "{failed:#}"
        );
        assert_eq!(supplier.fetches.load(Ordering::SeqCst), HYDRATE_FAILURES);
    }

    #[test]
    fn marks_and_times_survive_reopening_the_database() {
        let f = fixture();
        f.write("/a", &distinct(1, 1));
        let id = f.id("/a");
        f.phone.mark(FOLDER, &id, true).unwrap();
        f.phone.opened(FOLDER, &id, 7).unwrap();
        let reopened = Phone::open(&f._home.path().join("phone.sqlite")).unwrap();
        assert!(reopened.is_kept(FOLDER, &id, &[]).unwrap());
        assert_eq!(reopened.opened_times(FOLDER).unwrap()[&id], 7);
        assert!(!reopened.is_kept("another folder", &id, &[]).unwrap());
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
        assert!(
            counted_cursors(&catalog, REPO, "device-2")
                .unwrap()
                .is_empty()
        );
        assert!(
            counted_cursors(&catalog, "another repo", "device-1")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_cursor_of_a_peer_no_longer_paired_does_not_count() {
        let (_home, catalog) = catalog_with("device-1");
        catalog.unpair("pc").unwrap();
        assert!(
            counted_cursors(&catalog, REPO, "device-1")
                .unwrap()
                .is_empty()
        );
        // The race: a driver saves one more cursor after the unpair.
        save_cursor(&catalog, "pc", "device-1", 9);
        assert!(
            counted_cursors(&catalog, REPO, "device-1")
                .unwrap()
                .is_empty()
        );
    }
}
