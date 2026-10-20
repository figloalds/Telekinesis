//! Durable metadata-to-object root handoff. A metadata commit never traverses a
//! manifest or waits for content. Object consumers own their durable cursor.
use super::*;
use crate::model::{
    content::FileContentRef,
    workspace::{AdmissionAction, AdmissionChange},
};

pub const ADMISSION_PAGE: usize = 256;

pub(super) fn ensure_schema(db: &Connection) -> Result<()> {
    db.execute_batch("SAVEPOINT content_admission_schema;
        CREATE TABLE IF NOT EXISTS content_root_metadata(
            reference TEXT PRIMARY KEY,event TEXT NOT NULL,entity TEXT NOT NULL,
            branch TEXT NOT NULL,root BLOB NOT NULL CHECK(length(root)=48),
            generation INTEGER NOT NULL CHECK(generation>0),granted INTEGER NOT NULL,
            shared INTEGER NOT NULL,publishable INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS content_roots_by_event ON content_root_metadata(event);
        CREATE TABLE IF NOT EXISTS content_root_outbox(
            seq INTEGER PRIMARY KEY AUTOINCREMENT,repository TEXT NOT NULL,branch TEXT NOT NULL,
            reference TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),
            root BLOB NOT NULL CHECK(length(root)=48),action TEXT NOT NULL CHECK(action IN ('grant','withdraw')),
            shared INTEGER NOT NULL,publishable INTEGER NOT NULL,UNIQUE(reference,generation));
        CREATE TABLE IF NOT EXISTS object_admission_cursor(consumer TEXT PRIMARY KEY,cursor INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS object_root_admission(
            reference TEXT PRIMARY KEY,branch TEXT NOT NULL,root BLOB NOT NULL CHECK(length(root)=48),
            generation INTEGER NOT NULL,granted INTEGER NOT NULL,shared INTEGER NOT NULL,publishable INTEGER NOT NULL);
        RELEASE content_admission_schema;")?;
    Ok(())
}

/// Stable per-event register identity, including unambiguous length-delimited
/// JSON components. It is never a provider path or an authorization by hash.
pub fn reference_id(event: &str, entity: &str) -> String {
    hash(&serde_json::to_vec(&("tkfs:content-reference:v1", event, entity)).unwrap())
}

pub(super) fn append_event(db: &Connection, event: &Event, local: bool) -> Result<()> {
    ensure!(
        !db.is_autocommit(),
        "ADMISSION_OUTSIDE_METADATA_TRANSACTION"
    );
    for change in event
        .changes
        .iter()
        .filter(|change| change.field == "content" && change.value.is_object())
    {
        let root: FileContentRef = serde_json::from_value(change.value.clone())?;
        let encoded = root.encode()?;
        let reference = reference_id(&event.id, &change.entity);
        let publishable = event.branch.shared && local && event.reviewed.is_empty();
        let inserted = db.execute(
            "INSERT OR IGNORE INTO content_root_metadata
            (reference,event,entity,branch,root,generation,granted,shared,publishable)
            VALUES(?1,?2,?3,?4,?5,1,1,?6,?7)",
            params![
                reference,
                event.id,
                change.entity,
                event.branch.id,
                encoded.as_slice(),
                event.branch.shared,
                publishable
            ],
        )?;
        if inserted != 0 {
            db.execute(
                "INSERT INTO content_root_outbox
                (repository,branch,reference,generation,root,action,shared,publishable)
                VALUES(?1,?2,?3,1,?4,'grant',?5,?6)",
                params![
                    event.repo,
                    event.branch.id,
                    reference,
                    encoded.as_slice(),
                    event.branch.shared,
                    publishable
                ],
            )?;
        }
    }
    crate::util::fault::trigger("content.admission.appended")?;
    Ok(())
}

impl Store {
    /// Before activation, typed events are permitted only in explicitly marked
    /// isolated fixture stores. WP6.6 replaces this fixture gate with negotiated
    /// repository activation and the deployment fence. No CLI enables it.
    pub(super) fn check_content_capability(&self, event: &Event) -> Result<()> {
        if !event
            .changes
            .iter()
            .any(|change| change.field == "content" && change.value.is_object())
        {
            return Ok(());
        }
        let enabled: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM config WHERE key='fixture:content_v1' AND value='1')",
            [],
            |row| row.get(0),
        )?;
        ensure!(
            enabled,
            "PEER_NEEDS_UPGRADE: content-v1 is not active for this repository"
        );
        for change in event
            .changes
            .iter()
            .filter(|change| change.field == "content" && change.value.is_object())
        {
            // A future valid format is an incompatibility, never corruption.
            if change
                .value
                .get("format")
                .and_then(Value::as_u64)
                .is_some_and(|format| format > 1)
                || change
                    .value
                    .get("algorithm")
                    .and_then(Value::as_str)
                    .is_some_and(|algorithm| algorithm != "sha256")
            {
                bail!("PEER_NEEDS_UPGRADE: unsupported content representation");
            }
            let root: FileContentRef =
                serde_json::from_value(change.value.clone()).context("INVALID_CONTENT")?;
            root.validate()?;
        }
        Ok(())
    }

    /// Read-only bounded outbox replay. No manifest or object access occurs.
    pub fn content_admission_page(
        &self,
        cursor: u64,
        limit: usize,
    ) -> Result<Vec<AdmissionChange>> {
        ensure!(
            cursor <= i64::MAX as u64 && (1..=ADMISSION_PAGE).contains(&limit),
            "ADMISSION_PAGE_LIMIT"
        );
        let mut statement = self.db.prepare_cached(
            "SELECT seq,repository,branch,reference,generation,root,action,shared,publishable
            FROM content_root_outbox WHERE seq>?1 ORDER BY seq LIMIT ?2",
        )?;
        let mut rows = statement.query(params![cursor, limit as i64])?;
        let mut changes = vec![];
        while let Some(row) = rows.next()? {
            let cursor: i64 = row.get(0)?;
            let generation: i64 = row.get(4)?;
            ensure!(cursor > 0 && generation > 0, "INVALID_ADMISSION_CURSOR");
            let root: Vec<u8> = row.get(5)?;
            let action: String = row.get(6)?;
            changes.push(AdmissionChange {
                cursor: cursor as u64,
                repository: row.get(1)?,
                branch: row.get(2)?,
                reference: row.get(3)?,
                generation: generation as u64,
                root: FileContentRef::decode(&root)?,
                action: match action.as_str() {
                    "grant" => AdmissionAction::Grant,
                    "withdraw" => AdmissionAction::Withdraw,
                    _ => bail!("INVALID_ADMISSION_ACTION"),
                },
                shared: row.get(7)?,
                publishable: row.get(8)?,
            });
        }
        Ok(changes)
    }

    pub fn content_admission_cursor(&self, consumer: &str) -> Result<u64> {
        let cursor: i64 = self
            .db
            .query_row(
                "SELECT cursor FROM object_admission_cursor WHERE consumer=?",
                [consumer],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        ensure!(cursor >= 0, "INVALID_ADMISSION_CURSOR");
        Ok(cursor as u64)
    }

    /// Effects and cursor advance commit together. Notifications can be lost;
    /// the same page can be consumed repeatedly without changing newer grants.
    pub fn consume_content_admission(&mut self, consumer: &str, limit: usize) -> Result<usize> {
        ensure!(
            !consumer.is_empty() && consumer.len() <= 128,
            "INVALID_ADMISSION_CONSUMER"
        );
        self.with_write("content_admission_consume", |store| {
            let cursor = store.content_admission_cursor(consumer)?;
            let changes = store.content_admission_page(cursor, limit)?;
            for change in &changes {
                ensure!(change.repository == store.repo, "REPOSITORY_MISMATCH");
                let root = change.root.encode()?;
                let granted = change.action == AdmissionAction::Grant;
                store.db.execute("INSERT INTO object_root_admission
                    (reference,branch,root,generation,granted,shared,publishable) VALUES(?1,?2,?3,?4,?5,?6,?7)
                    ON CONFLICT(reference) DO UPDATE SET branch=excluded.branch,root=excluded.root,
                        generation=excluded.generation,granted=excluded.granted,
                        shared=excluded.shared,publishable=excluded.publishable
                    WHERE object_root_admission.generation<excluded.generation",
                    params![change.reference,change.branch,root.as_slice(),change.generation,
                            granted,change.shared,change.publishable])?;
                if granted {
                    super::content_objects::seed_root(&store.db, &change.reference,
                        change.generation, &change.root)?;
                }
            }
            crate::util::fault::trigger("content.admission.applied")?;
            if let Some(last) = changes.last() {
                store.db.execute("INSERT INTO object_admission_cursor VALUES(?1,?2)
                    ON CONFLICT(consumer) DO UPDATE SET cursor=excluded.cursor", params![consumer,last.cursor])?;
            }
            crate::util::fault::trigger("content.admission.cursor")?;
            Ok(changes.len())
        })
    }

    /// Trusted local authorization mutation. The metadata commit is the local
    /// revocation linearization point; serving rechecks this state per frame.
    /// An old generation cannot withdraw or regrant a newer reference.
    pub fn set_content_admission(
        &mut self,
        reference: &str,
        expected: u64,
        action: AdmissionAction,
    ) -> Result<u64> {
        ensure!(
            expected > 0 && expected < i64::MAX as u64,
            "INVALID_ADMISSION_GENERATION"
        );
        self.with_write("content_admission_change", |store| {
            let (branch, root, generation, granted, shared, publishable): (
                String,
                Vec<u8>,
                u64,
                bool,
                bool,
                bool,
            ) = store.db.query_row(
                "SELECT branch,root,generation,granted,shared,publishable
                    FROM content_root_metadata WHERE reference=?",
                [reference],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )?;
            let desired = action == AdmissionAction::Grant;
            if generation == expected + 1 && granted == desired {
                return Ok(generation);
            }
            ensure!(generation == expected, "STALE_ADMISSION_GENERATION");
            if granted == desired {
                return Ok(generation);
            }
            let next = generation + 1;
            store.db.execute(
                "UPDATE content_root_metadata SET generation=?1,granted=?2 WHERE reference=?3",
                params![next, desired, reference],
            )?;
            store.db.execute(
                "INSERT INTO content_root_outbox
                (repository,branch,reference,generation,root,action,shared,publishable)
                VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    store.repo,
                    branch,
                    reference,
                    next,
                    root,
                    if desired { "grant" } else { "withdraw" },
                    shared,
                    publishable
                ],
            )?;
            Ok(next)
        })
    }

    /// Root authority only. Descendant proof, byte verification and disclosure
    /// provenance remain additional conditions in the object engine.
    pub fn content_reference_authorized(&self, reference: &str, generation: u64) -> Result<bool> {
        ensure!(
            generation <= i64::MAX as u64,
            "INVALID_ADMISSION_GENERATION"
        );
        Ok(self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM content_root_metadata
            WHERE reference=?1 AND generation=?2 AND granted=1 AND shared=1)",
            params![reference, generation],
            |row| row.get(0),
        )?)
    }
}
