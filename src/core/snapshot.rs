//! Disposable startup snapshots. The semantic journal remains authoritative.
use super::*;

// Bump when projection/validation semantics change, even without a SQL migration.
const FORMAT: u32 = 1;
const INTERVAL: i64 = 128;

#[derive(Serialize, Deserialize)]
struct Snapshot {
    format: u32,
    repo: String,
    device: String,
    branch: Branch,
    cursor: i64,
    journal_hash: String,
    projection: Projection,
    conflicts: Vec<Conflict>,
    frontier: Vec<String>,
}

pub(super) fn initialize(db: &Connection) -> Result<()> {
    // Triggers also cover writes by older executables and repair tools. Sequence
    // numbers are local acceptance order, never CRDT version precedence.
    db.execute_batch("SAVEPOINT snapshot_schema;
        CREATE TABLE IF NOT EXISTS startup_snapshots(branch TEXT PRIMARY KEY,cursor INTEGER NOT NULL,payload TEXT NOT NULL,checksum TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS journal_sequence(seq INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL UNIQUE);
        INSERT INTO journal_sequence(event) SELECT id FROM events e WHERE NOT EXISTS(SELECT 1 FROM journal_sequence s WHERE s.event=e.id) ORDER BY rowid;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_insert AFTER INSERT ON events BEGIN
            INSERT INTO journal_sequence(event) VALUES(NEW.id);
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_delete AFTER DELETE ON events BEGIN
            DELETE FROM startup_snapshots;
            DELETE FROM journal_sequence WHERE event=OLD.id;
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_update AFTER UPDATE ON events BEGIN
            DELETE FROM startup_snapshots;
            UPDATE journal_sequence SET event=NEW.id WHERE event=OLD.id;
        END;
        RELEASE snapshot_schema;")?;
    Ok(())
}

pub(super) fn load_projection(db: &Connection, branch: &str) -> Result<Projection> {
    let payload: String = db.query_row(
        "SELECT payload FROM current_state WHERE branch=?",
        [branch],
        |r| r.get(0),
    )?;
    let mut p: Projection = serde_json::from_str(&payload)?;
    let mut statement = db.prepare("SELECT entry,heads FROM current_parts WHERE branch=?")?;
    let rows = statement.query_map([branch], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (entry, heads) = row?;
        let entry: Entry = serde_json::from_str(&entry)?;
        p.entries.insert(entry.id.clone(), entry);
        let heads: BTreeMap<String, Vec<Version>> = serde_json::from_str(&heads)?;
        p.heads.extend(heads);
    }
    Ok(p)
}

fn cursor(db: &Connection, branch: &str) -> Result<i64> {
    Ok(db.query_row("SELECT coalesce(max(s.seq),0) FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=?", [branch], |r| r.get(0))?)
}

fn journal_hash(db: &Connection, branch: &str, cursor: i64) -> Result<String> {
    let mut digest = Sha256::new();
    let mut statement = db.prepare("SELECT s.seq,e.id,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? AND s.seq<=? ORDER BY s.seq")?;
    let mut rows = statement.query(params![branch, cursor])?;
    while let Some(row) = rows.next()? {
        let seq: i64 = row.get(0)?;
        digest.update(seq.to_le_bytes());
        for column in [1, 2] {
            let value: String = row.get(column)?;
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value.as_bytes());
        }
        crate::startup::progress("checking snapshot journal");
    }
    Ok(hex::encode(digest.finalize()))
}

pub(super) fn maybe_persist(db: &Connection, branch: &str, full: bool) -> Result<()> {
    let saved: Option<i64> = db
        .query_row(
            "SELECT cursor FROM startup_snapshots WHERE branch=?",
            [branch],
            |r| r.get(0),
        )
        .optional()?;
    // O(1) on each commit. The branch-specific cursor/digest is computed only
    // when sealing; activity on another branch may merely seal this one sooner.
    let tip: i64 = db.query_row(
        "SELECT coalesce(max(seq),0) FROM journal_sequence",
        [],
        |r| r.get(0),
    )?;
    if full || saved.is_none_or(|saved| tip - saved >= INTERVAL) {
        persist(db, branch)?;
    }
    Ok(())
}

pub(super) fn persist(db: &Connection, branch: &str) -> Result<()> {
    let cursor = cursor(db, branch)?;
    let projection = load_projection(db, branch)?;
    ensure!(projection.pending.is_empty(), "INCOMPLETE_SNAPSHOT");
    let mut conflicts = Vec::new();
    let mut statement =
        db.prepare("SELECT payload,resolved_by FROM conflicts WHERE branch=? ORDER BY id")?;
    let mut rows = statement.query([branch])?;
    while let Some(row) = rows.next()? {
        let mut conflict: Conflict = serde_json::from_str(&row.get::<_, String>(0)?)?;
        conflict.resolved_by = row.get(1)?;
        conflicts.push(conflict);
    }
    let snapshot = Snapshot {
        format: FORMAT,
        repo: db.query_row("SELECT value FROM config WHERE key='repo'", [], |r| {
            r.get(0)
        })?,
        device: db.query_row("SELECT value FROM config WHERE key='device'", [], |r| {
            r.get(0)
        })?,
        branch: db.query_row(
            "SELECT id,name,shared FROM branches WHERE id=?",
            [branch],
            |r| {
                Ok(Branch {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    shared: r.get(2)?,
                })
            },
        )?,
        cursor,
        journal_hash: journal_hash(db, branch, cursor)?,
        projection,
        conflicts,
        frontier: db
            .prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")?
            .query_map([branch], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?,
    };
    let payload = serde_json::to_string(&snapshot)?;
    fault("before_startup_snapshot");
    db.execute("INSERT INTO startup_snapshots VALUES(?,?,?,?) ON CONFLICT(branch) DO UPDATE SET cursor=excluded.cursor,payload=excluded.payload,checksum=excluded.checksum", params![branch, cursor, payload, hash(payload.as_bytes())])?;
    fault("startup_snapshot_written");
    Ok(())
}

impl Store {
    pub(super) fn resume_snapshot(&mut self, branch: &Branch) -> Result<bool> {
        // Restoring the base and applying the suffix is atomic. In particular,
        // an unsupported concurrent/namespace event must leave no partial view.
        self.db.execute_batch("SAVEPOINT startup_replay")?;
        let result = self.try_resume_snapshot(branch);
        self.view_cache.borrow_mut().take();
        match result {
            Ok(true) => {
                self.db.execute_batch("RELEASE startup_replay")?;
                Ok(true)
            }
            result => {
                self.db
                    .execute_batch("ROLLBACK TO startup_replay; RELEASE startup_replay")?;
                if let Err(error) = result {
                    eprintln!("Startup snapshot fallback for {}: {error:#}", branch.id);
                }
                Ok(false)
            }
        }
    }

    fn try_resume_snapshot(&mut self, branch: &Branch) -> Result<bool> {
        let saved: Option<(i64, String, String)> = self
            .db
            .query_row(
                "SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?",
                [&branch.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((saved_cursor, payload, checksum)) = saved else {
            return Ok(false);
        };
        crate::startup::progress("loading startup snapshot");
        ensure!(hash(payload.as_bytes()) == checksum, "SNAPSHOT_CHECKSUM");
        let snapshot: Snapshot = serde_json::from_str(&payload)?;
        ensure!(
            snapshot.format == FORMAT
                && snapshot.repo == self.repo
                && snapshot.device == self.device
                && snapshot.branch == *branch,
            "SNAPSHOT_VERSION_OR_IDENTITY"
        );
        ensure!(
            snapshot.cursor == saved_cursor
                && saved_cursor >= 0
                && saved_cursor <= cursor(&self.db, &branch.id)?,
            "SNAPSHOT_CURSOR"
        );
        ensure!(
            snapshot.journal_hash == journal_hash(&self.db, &branch.id, saved_cursor)?,
            "SNAPSHOT_JOURNAL_CHANGED"
        );
        ensure!(
            snapshot.projection.pending.is_empty(),
            "INCOMPLETE_SNAPSHOT"
        );
        let tail: Vec<(i64, String)> = self.db.prepare("SELECT s.seq,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? AND s.seq>? ORDER BY s.seq")?
            .query_map(params![branch.id, saved_cursor], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        // Bound startup work even when an older writer never consolidated.
        if tail.len() > INTERVAL as usize {
            return Ok(false);
        }
        self.db
            .execute("DELETE FROM current_parts WHERE branch=?", [&branch.id])?;
        self.db.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload", params![branch.id, serde_json::to_string(&snapshot.projection)?])?;
        self.db
            .execute("DELETE FROM conflicts WHERE branch=?", [&branch.id])?;
        for c in &snapshot.conflicts {
            self.db.execute(
                "INSERT INTO conflicts VALUES(?,?,?,?)",
                params![c.id, branch.id, serde_json::to_string(c)?, c.resolved_by],
            )?;
        }
        self.db
            .execute("DELETE FROM event_frontier WHERE branch=?", [&branch.id])?;
        for id in &snapshot.frontier {
            self.db.execute(
                "INSERT INTO event_frontier VALUES(?,?)",
                params![branch.id, id],
            )?;
        }
        self.view_cache.borrow_mut().take();
        for (seq, payload) in &tail {
            crate::startup::progress("replaying snapshot suffix");
            let event: Event = serde_json::from_str(payload)?;
            ensure!(
                event.repo == self.repo && event.branch == *branch,
                "SNAPSHOT_EVENT_IDENTITY"
            );
            // A cursor orders acceptance, not causality. Legacy journals with a
            // prerequisite later in acceptance order require full reconstruction.
            for parent in event
                .dependencies
                .iter()
                .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
            {
                let parent_seq: Option<i64> = self
                    .db
                    .query_row(
                        "SELECT seq FROM journal_sequence WHERE event=?",
                        [parent],
                        |r| r.get(0),
                    )
                    .optional()?;
                ensure!(
                    parent_seq.is_some_and(|parent| parent < *seq),
                    "SNAPSHOT_NON_TOPOLOGICAL_SUFFIX"
                );
            }
            // Reuse only proven incremental rules. Conflicting edits, reviews
            // and directory changes that suppress descendants use full replay.
            let mut delta = self.content_successor(&event)?;
            // Namespace successor helpers consult the active branch's review
            // history. Other branches conservatively use full reconstruction.
            if delta.is_none() && event.branch.id == self.active {
                delta = self.creation_successor(&event)?;
            }
            if delta.is_none() && event.branch.id == self.active {
                delta = self.namespace_successor(&event)?;
            }
            let Some(delta) = delta else { return Ok(false) };
            for (entity, entry) in &delta.entries {
                let heads: BTreeMap<_, _> = delta
                    .heads
                    .iter()
                    .filter(|(key, _)| key.rsplit_once('/').is_some_and(|(id, _)| id == entity))
                    .collect();
                self.db.execute("INSERT INTO current_parts VALUES(?,?,?,?) ON CONFLICT(branch,entity) DO UPDATE SET entry=excluded.entry,heads=excluded.heads", params![branch.id, entity, serde_json::to_string(entry)?, serde_json::to_string(&heads)?])?;
            }
            for parent in event
                .dependencies
                .iter()
                .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
            {
                self.db.execute(
                    "DELETE FROM event_frontier WHERE branch=? AND event=?",
                    params![branch.id, parent],
                )?;
            }
            self.db.execute(
                "INSERT INTO event_frontier VALUES(?,?)",
                params![branch.id, event.id],
            )?;
            fault("startup_suffix_applied");
        }
        if !tail.is_empty() {
            persist(&self.db, &branch.id)?;
        }
        Ok(true)
    }
}
