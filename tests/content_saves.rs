//! Metadata publication uses real WAL transactions and actual prepared CAS roots.
//! These tests do not acknowledge mounted fsync or retire a private write session.
use std::{
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};
use tkfs::core::content_saves::SaveContinuation;
use tkfs::{
    content::{
        ContentProvider, LocalContent, ReadContext,
        write::{PreparedSave, SaveCheckpoint, WriteJournal, WriteSession},
    },
    core::{Change, Store, id},
    model::content::{ContentKind, Digest, FileContentRef},
    util::{
        Cancel,
        fault::{self, FaultAction, FaultConfig},
    },
};
use uuid::Uuid;

fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(60),
        cancel: Cancel::new(),
    }
}
fn error<T: std::fmt::Debug>(result: anyhow::Result<T>, expected: &str) {
    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains(expected), "expected {expected}: {error}");
}
struct Fixture {
    store: Store,
    local: Arc<LocalContent>,
    journal: Arc<WriteJournal>,
    session: Arc<WriteSession>,
    checkpoint: SaveCheckpoint,
    prepared: PreparedSave,
    entity: String,
    // Close every SQLite owner before removing its directory on Windows.
    dir: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        Self::with_selection(true)
    }
    fn unselected() -> Self {
        Self::with_selection(false)
    }
    fn with_selection(select: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(dir.path(), "content-save-publication", &id()).unwrap();
        store
            .db
            .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
            .unwrap();
        let entry = store.create("file", "file").unwrap();
        let local = Arc::new(LocalContent::open(dir.path()).unwrap());
        let journal = WriteJournal::open(dir.path()).unwrap();
        let intent = store.begin_content_save(&entry.id, &context()).unwrap();
        let session = Arc::new(journal.create(intent, local.clone(), &context()).unwrap());
        session
            .write_at(0, b"durable local bytes", &context())
            .unwrap();
        let checkpoint = session.checkpoint(&context()).unwrap().unwrap();
        let prepared = session.prepare(&local, &checkpoint, &context()).unwrap();
        if select {
            store
                .select_content_checkpoint(&checkpoint, &context())
                .unwrap();
        }
        Self {
            dir,
            store,
            local,
            journal,
            session,
            checkpoint,
            prepared,
            entity: entry.id,
        }
    }
    fn event_count(&self) -> i64 {
        self.store
            .db
            .query_row(
                "SELECT count(*) FROM events WHERE id=?",
                [self.session.operation().to_string()],
                |r| r.get(0),
            )
            .unwrap()
    }
    fn outbox_count(&self) -> usize {
        self.store.content_admission_page(0, 256).unwrap().len()
    }
    fn pending(&self) {
        let pending = self.journal.pending(None, 256, &context()).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].intent.operation, self.session.operation());
    }
    fn oracle(&mut self) {
        assert!(self.store.verify_derived_state().unwrap().is_empty());
    }
}

fn owned_basic(store: &Store, entity: &str) -> tkfs::core::BasicInfo {
    let mut basic = store.entry(entity).unwrap().basic_info();
    basic.posix_mode = Some(0o640);
    basic.write_time = 101;
    basic.change_time = 202;
    basic
}

#[test]
fn owned_basic_rebases_metadata_without_replacing_private_bytes_or_content_parents() {
    let mut f = Fixture::unselected();
    let before = f.store.entry(&f.entity).unwrap();
    let intent = f.checkpoint.intent.clone();
    let mutation = Uuid::new_v4();
    let basic = owned_basic(&f.store, &f.entity);
    let receipt = f
        .store
        .apply_content_save_basic(
            &intent,
            mutation,
            basic,
            f.checkpoint.sequence,
            6,
            &context(),
        )
        .unwrap();
    assert_eq!(receipt.operation, intent.operation);
    assert_eq!(f.store.entry(&f.entity).unwrap().content, before.content);
    assert_eq!(
        f.store.entry(&f.entity).unwrap().content_ref,
        before.content_ref
    );
    assert_eq!(f.store.entry(&f.entity).unwrap().basic_info(), basic);
    assert_eq!(
        f.journal.pending(None, 64, &context()).unwrap(),
        vec![f.checkpoint.clone()]
    );
    assert_eq!(read_view(&f.session, 19), b"durable local bytes");
    assert_eq!(
        f.store
            .apply_content_save_basic(
                &intent,
                mutation,
                basic,
                f.checkpoint.sequence,
                6,
                &context()
            )
            .unwrap(),
        receipt
    );
    f.store
        .select_content_checkpoint(&f.checkpoint, &context())
        .unwrap();
    let saved = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let event = f
        .store
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.id == saved.event())
        .unwrap();
    assert_eq!(
        event.changes[0].parents,
        intent.content_parent.into_iter().collect::<Vec<_>>()
    );
    assert_eq!(
        event.changes[1].parents,
        intent.alive_parent.into_iter().collect::<Vec<_>>()
    );
    assert_eq!(event.changes[2].parents, vec![receipt.event]);
    assert_eq!(f.store.entry(&f.entity).unwrap().basic_info(), basic);
    f.oracle();
    println!(
        "WAVE6_OWNED_BASIC {}",
        serde_json::json!({"scenario":"metadata_only_then_save","stable_operation":true,"content_unchanged_before_publication":true,"journal_saved_unchanged":true,"private_bytes_equal":true,"timestamps_preserved":true})
    );
}

#[test]
fn owned_basic_time_retention_expires_after_new_bytes_even_when_selection_is_superseded() {
    for select_first in [false, true] {
        let mut f = Fixture::unselected();
        let basic = owned_basic(&f.store, &f.entity);
        f.store
            .apply_content_save_basic(
                &f.checkpoint.intent,
                Uuid::new_v4(),
                basic,
                f.checkpoint.sequence,
                6,
                &context(),
            )
            .unwrap();
        if select_first {
            f.store
                .select_content_checkpoint(&f.checkpoint, &context())
                .unwrap();
        }
        f.session.write_at(0, b"new", &context()).unwrap();
        let newer = f.session.checkpoint(&context()).unwrap().unwrap();
        let prepared = f.session.prepare(&f.local, &newer, &context()).unwrap();
        f.store
            .select_content_checkpoint(&newer, &context())
            .unwrap();
        error(
            f.store.publish_content_save(&f.prepared, &context()),
            "STALE_WRITE_CHECKPOINT",
        );
        f.store.publish_content_save(&prepared, &context()).unwrap();
        let actual = f.store.entry(&f.entity).unwrap().basic_info();
        assert!(actual.write_time > basic.write_time && actual.change_time > basic.change_time);
        assert_eq!(actual.posix_mode, basic.posix_mode);
        f.oracle();
    }
}

#[test]
fn owned_basic_rejects_selected_capture_and_ordinary_metadata_still_fences_ownership() {
    let mut f = Fixture::new();
    let basic = owned_basic(&f.store, &f.entity);
    let mutation = Uuid::new_v4();
    error(
        f.store.apply_content_save_basic(
            &f.checkpoint.intent,
            mutation,
            basic,
            f.checkpoint.sequence,
            6,
            &context(),
        ),
        "CONTENT_METADATA_SAVE_IN_PROGRESS",
    );
    let count: u64 = f
        .store
        .db
        .query_row(
            "SELECT count(*) FROM content_save_metadata_changes",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    f.store.set_basic(&f.entity, basic).unwrap();
    error(
        f.store.apply_content_save_basic(
            &f.checkpoint.intent,
            mutation,
            basic,
            f.checkpoint.sequence,
            6,
            &context(),
        ),
        "STALE_CONTENT_SAVE",
    );
    error(
        f.store.publish_content_save(&f.prepared, &context()),
        "STALE_CONTENT_SAVE",
    );
    f.pending();
    f.oracle();
}

#[test]
fn owned_basic_newer_private_sequence_cannot_be_overwritten_by_an_older_checkpoint() {
    let mut f = Fixture::unselected();
    f.session.write_at(0, b"new", &context()).unwrap();
    let newer = f.session.checkpoint(&context()).unwrap().unwrap();
    let basic = owned_basic(&f.store, &f.entity);
    f.store
        .apply_content_save_basic(
            &newer.intent,
            Uuid::new_v4(),
            basic,
            newer.sequence,
            6,
            &context(),
        )
        .unwrap();
    error(
        f.store.select_content_checkpoint(&f.checkpoint, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    assert_eq!(f.store.entry(&f.entity).unwrap().basic_info(), basic);
    let prepared = f.session.prepare(&f.local, &newer, &context()).unwrap();
    f.store
        .select_content_checkpoint(&newer, &context())
        .unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.store.entry(&f.entity).unwrap().basic_info(), basic);
    f.oracle();
}

#[test]
fn owned_basic_successor_replay_and_lost_reply_survive_later_publication() {
    let mut f = Fixture::new();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let handoff = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    let session = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    session.write_at(0, b"next", &context()).unwrap();
    let checkpoint = session.checkpoint(&context()).unwrap().unwrap();
    let basic = owned_basic(&f.store, &f.entity);
    let mutation = Uuid::new_v4();
    let receipt = f
        .store
        .apply_content_save_basic(
            &checkpoint.intent,
            mutation,
            basic,
            checkpoint.sequence,
            6,
            &context(),
        )
        .unwrap();
    assert_eq!(
        f.store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap()
            .intent(),
        handoff.intent()
    );
    assert!(
        f.session
            .adopt_continuation(&handoff, f.local.clone(), &context())
            .unwrap()
            .adoption_work()
            .unwrap()
            .replay
    );
    let prepared = session.prepare(&f.local, &checkpoint, &context()).unwrap();
    f.store
        .select_content_checkpoint(&checkpoint, &context())
        .unwrap();
    // Selecting timestamps must also preserve historical successor lookup.
    assert_eq!(
        f.store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap()
            .intent(),
        handoff.intent()
    );
    f.store.publish_content_save(&prepared, &context()).unwrap();
    let next = f
        .store
        .continue_content_save(&checkpoint, &context())
        .unwrap();
    session
        .adopt_continuation(&next, f.local.clone(), &context())
        .unwrap();
    assert_eq!(
        f.store
            .apply_content_save_basic(
                &checkpoint.intent,
                mutation,
                basic,
                checkpoint.sequence,
                6,
                &context()
            )
            .unwrap(),
        receipt
    );
    error(
        f.store.apply_content_save_basic(
            &checkpoint.intent,
            mutation,
            basic,
            checkpoint.sequence + 1,
            6,
            &context(),
        ),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    assert_eq!(f.store.entry(&f.entity).unwrap().basic_info(), basic);
    f.oracle();
}

#[test]
fn owned_basic_metadata_event_cannot_reuse_the_unpublished_byte_event_identity() {
    let mut f = Fixture::unselected();
    let basic = owned_basic(&f.store, &f.entity);
    error(
        f.store.apply_content_save_basic(
            &f.checkpoint.intent,
            f.session.operation(),
            basic,
            f.checkpoint.sequence,
            6,
            &context(),
        ),
        "CONTENT_METADATA_OWNER_MISMATCH",
    );
    assert_eq!(f.event_count(), 0);
    f.store
        .select_content_checkpoint(&f.checkpoint, &context())
        .unwrap();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    f.oracle();
}

#[test]
fn owned_basic_rebase_preserves_a_remote_content_competitor() {
    let mut f = Fixture::unselected();
    let root = FileContentRef::new(
        ContentKind::Direct,
        Digest::bytes(b"absent remote bytes"),
        19,
    )
    .unwrap();
    let mut remote = f.store.event(
        &f.store.branch(&f.store.active).unwrap(),
        vec![Change {
            entity: f.entity.clone(),
            field: "content".into(),
            parents: f
                .checkpoint
                .intent
                .content_parent
                .clone()
                .into_iter()
                .collect(),
            value: serde_json::json!(root),
        }],
    );
    remote.device = "remote-peer".into();
    let remote_id = remote.id.clone();
    f.store.commit(remote, false).unwrap();
    let basic = owned_basic(&f.store, &f.entity);
    f.store
        .apply_content_save_basic(
            &f.checkpoint.intent,
            Uuid::new_v4(),
            basic,
            f.checkpoint.sequence,
            6,
            &context(),
        )
        .unwrap();
    f.store
        .select_content_checkpoint(&f.checkpoint, &context())
        .unwrap();
    let receipt = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let local = f
        .store
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.id == receipt.event())
        .unwrap();
    assert!(!local.changes[0].parents.contains(&remote_id));
    assert!(
        f.store
            .conflicts()
            .unwrap()
            .iter()
            .any(|c| c.field == "content"
                && c.alternatives.iter().any(|a| a.revision == remote_id)
                && c.alternatives.iter().any(|a| a.revision == receipt.event()))
    );
    f.oracle();
}

#[test]
fn owned_basic_process_exit_keeps_event_capture_and_idempotent_receipt_atomic() {
    for point in [
        "content.save.before_basic",
        "content.save.basic_written",
        "content.save.basic_committed",
    ] {
        let f = Fixture::unselected();
        let mutation = Uuid::new_v4();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "child_process_publication",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_SAVE_CHILD_ROOT", f.dir.path())
            .env(
                "TKFS_SAVE_CHILD_OPERATION",
                f.session.operation().to_string(),
            )
            .env("TKFS_SAVE_CHILD_POINT", point)
            .env("TKFS_SAVE_CHILD_CONTINUE", "3")
            .env(
                "TKFS_SAVE_CHILD_BASIC_INTENT",
                serde_json::to_string(&f.checkpoint.intent).unwrap(),
            )
            .env("TKFS_SAVE_CHILD_MUTATION", mutation.to_string())
            .env(
                "TKFS_SAVE_CHILD_SEQUENCE",
                f.checkpoint.sequence.to_string(),
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{point}");
        let mut recovered = Store::open_existing(f.dir.path()).unwrap();
        let count: u64 = recovered
            .db
            .query_row(
                "SELECT count(*) FROM content_save_metadata_changes",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, u64::from(point == "content.save.basic_committed"));
        let basic = owned_basic(&recovered, &f.entity);
        let receipt = recovered
            .apply_content_save_basic(
                &f.checkpoint.intent,
                mutation,
                basic,
                f.checkpoint.sequence,
                6,
                &context(),
            )
            .unwrap();
        assert_eq!(
            recovered
                .apply_content_save_basic(
                    &f.checkpoint.intent,
                    mutation,
                    basic,
                    f.checkpoint.sequence,
                    6,
                    &context()
                )
                .unwrap(),
            receipt
        );
        recovered
            .select_content_checkpoint(&f.checkpoint, &context())
            .unwrap();
        recovered
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        assert_eq!(recovered.entry(&f.entity).unwrap().basic_info(), basic);
        assert!(recovered.verify_derived_state().unwrap().is_empty());
        f.pending();
    }
}

#[test]
fn owned_basic_silent_sql_receipt_and_capture_writes_cannot_acknowledge_success() {
    for (label, sql) in [
        (
            "receipt_ignore",
            "CREATE TEMP TRIGGER reject_metadata BEFORE INSERT ON content_save_metadata_changes BEGIN SELECT RAISE(IGNORE); END",
        ),
        (
            "receipt_delete",
            "CREATE TEMP TRIGGER reject_metadata AFTER INSERT ON content_save_metadata_changes BEGIN DELETE FROM content_save_metadata_changes WHERE mutation=NEW.mutation; END",
        ),
        (
            "capture_revert",
            "CREATE TEMP TRIGGER reject_metadata AFTER UPDATE OF capture ON content_save_operations BEGIN UPDATE content_save_operations SET capture=OLD.capture WHERE operation=NEW.operation; END",
        ),
        (
            "successor_revert",
            "CREATE TEMP TRIGGER reject_metadata AFTER UPDATE OF capture ON content_save_successors BEGIN UPDATE content_save_successors SET capture=OLD.capture WHERE operation=NEW.operation AND checkpoint=NEW.checkpoint; END",
        ),
    ] {
        let mut f = Fixture::unselected();
        let (intent, sequence) = if label == "successor_revert" {
            f.store
                .select_content_checkpoint(&f.checkpoint, &context())
                .unwrap();
            f.store
                .publish_content_save(&f.prepared, &context())
                .unwrap();
            (
                f.store
                    .continue_content_save(&f.checkpoint, &context())
                    .unwrap()
                    .intent()
                    .clone(),
                0,
            )
        } else {
            (f.checkpoint.intent.clone(), f.checkpoint.sequence)
        };
        let before = f.store.entry(&f.entity).unwrap();
        let basic = owned_basic(&f.store, &f.entity);
        let mutation = Uuid::new_v4();
        f.store.db.execute_batch(sql).unwrap();
        assert!(
            f.store
                .apply_content_save_basic(&intent, mutation, basic, sequence, 6, &context())
                .is_err(),
            "{label} acknowledged a silently lost write"
        );
        assert!(f.store.db.is_autocommit());
        assert_eq!(f.store.entry(&f.entity).unwrap(), before, "{label}");
        let count: u64 = f
            .store
            .db
            .query_row(
                "SELECT count(*) FROM events WHERE id=?",
                [mutation.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "{label}");
        f.store
            .db
            .execute_batch("DROP TRIGGER reject_metadata")
            .unwrap();
        f.store
            .apply_content_save_basic(&intent, mutation, basic, sequence, 6, &context())
            .unwrap();
        assert_eq!(f.store.entry(&f.entity).unwrap().basic_info(), basic);
        f.oracle();
    }
}

#[test]
fn owned_basic_selection_detects_a_valid_but_silently_reverted_metadata_template() {
    let mut f = Fixture::unselected();
    let basic = owned_basic(&f.store, &f.entity);
    f.store
        .apply_content_save_basic(
            &f.checkpoint.intent,
            Uuid::new_v4(),
            basic,
            f.checkpoint.sequence,
            0,
            &context(),
        )
        .unwrap();
    f.store.db.execute_batch("CREATE TEMP TABLE previous_metadata AS SELECT operation,capture FROM content_save_operations;
        CREATE TEMP TRIGGER revert_selection AFTER UPDATE OF selected ON content_save_operations BEGIN
        UPDATE content_save_operations SET capture=(SELECT capture FROM previous_metadata WHERE operation=NEW.operation) WHERE operation=NEW.operation; END").unwrap();
    assert!(
        f.store
            .select_content_checkpoint(&f.checkpoint, &context())
            .is_err(),
        "selection acknowledged a different metadata template"
    );
    assert!(f.store.db.is_autocommit());
    f.store
        .db
        .execute_batch("DROP TRIGGER revert_selection")
        .unwrap();
    f.store
        .select_content_checkpoint(&f.checkpoint, &context())
        .unwrap();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    assert!(f.store.entry(&f.entity).unwrap().basic_info().write_time > basic.write_time);
    f.oracle();
}

/// Independent fixture journal for the next metadata generation. This does not
/// implement/admit a rollover of the original private journal; it stays intact.
struct Generation {
    session: WriteSession,
    checkpoint: SaveCheckpoint,
    prepared: PreparedSave,
    _journal: Arc<WriteJournal>,
    _dir: tempfile::TempDir,
}

fn journal_db(f: &Fixture) -> rusqlite::Connection {
    rusqlite::Connection::open(f.dir.path().join("write-sessions-v1.sqlite")).unwrap()
}
fn read_view(session: &WriteSession, len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len];
    session.read_range(0, &mut bytes, &context()).unwrap();
    bytes
}
fn finish_collection(journal: &WriteJournal) {
    let mut cursor = None;
    loop {
        let page = journal.reclaim_orphans(cursor, &context()).unwrap();
        assert!(page.validated + page.examined + page.nodes_examined <= 256);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
}

#[test]
fn same_journal_adoption_fences_old_handles_and_continues_the_event_chain() {
    let mut f = Fixture::new();
    let first = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let handoff = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    let next = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    assert!(!next.adoption_work().unwrap().replay);
    assert_eq!(next.operation(), f.session.operation());
    assert_eq!(read_view(&next, 19), b"durable local bytes");
    assert!(next.checkpoint(&context()).unwrap().is_none());
    assert!(f.journal.pending(None, 256, &context()).unwrap().is_empty());
    let raw: Vec<u8> = journal_db(&f)
        .query_row("SELECT saved FROM sessions", [], |r| r.get(0))
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let baseline: SaveCheckpoint = serde_json::from_value(value["value"].clone()).unwrap();
    assert!(!baseline.needs_publication());
    error(
        f.store.select_content_checkpoint(&baseline, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    error(
        f.session.write_at(0, b"stale", &context()),
        "STALE_WRITE_SESSION",
    );
    error(
        f.session.read_range(0, &mut [0], &context()),
        "STALE_WRITE_SESSION",
    );
    let replay = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    assert!(replay.adoption_work().unwrap().replay);
    assert_eq!(
        journal_db(&f)
            .query_row("SELECT count(*) FROM retained_overlays", [], |r| r
                .get::<_, usize>(0))
            .unwrap(),
        1
    );
    next.write_at(0, b"next", &context()).unwrap();
    assert_eq!(read_view(&replay, 4), b"next");
    let saved = next.checkpoint(&context()).unwrap().unwrap();
    assert_eq!(saved.intent, *handoff.intent());
    assert!(saved.needs_publication());
    let prepared = next.prepare(&f.local, &saved, &context()).unwrap();
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let second = f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_ne!(first.event(), second.event());
    assert_eq!(
        f.store
            .content_checkpoint_receipt(&f.checkpoint, &context())
            .unwrap(),
        Some(first)
    );
    let handoff2 = f.store.continue_content_save(&saved, &context()).unwrap();
    let third = next
        .adopt_continuation(&handoff2, f.local.clone(), &context())
        .unwrap();
    assert_eq!(read_view(&third, 4), b"next");
    error(
        f.session
            .adopt_continuation(&handoff, f.local.clone(), &context())
            .map(|_| ()),
        "STALE_WRITE_SESSION",
    );
    error(next.write_at(0, b"late", &context()), "STALE_WRITE_SESSION");
    finish_collection(&f.journal);
    f.oracle();
}

#[test]
fn adoption_rebases_later_saved_and_buffered_shrink_grow_views_independently() {
    let mut f = Fixture::new();
    f.session.write_at(19, &[b'x'; 81], &context()).unwrap();
    let parent = f.session.checkpoint(&context()).unwrap().unwrap();
    let prepared = f.session.prepare(&f.local, &parent, &context()).unwrap();
    f.store
        .select_content_checkpoint(&parent, &context())
        .unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    f.session.set_len(50, &context()).unwrap();
    f.session.set_len(100, &context()).unwrap();
    let later = f.session.checkpoint(&context()).unwrap().unwrap();
    let expected_saved = read_view(&f.session, 100);
    f.session.set_len(25, &context()).unwrap();
    f.session.set_len(100, &context()).unwrap();
    f.session.write_at(75, b"live", &context()).unwrap();
    let expected_working = read_view(&f.session, 100);
    assert_eq!(&expected_saved[50..], &[0; 50]);
    assert_eq!(&expected_working[25..75], &[0; 50]);
    let queued = f.journal.queued_patch_bytes(&context()).unwrap();
    let handoff = f.store.continue_content_save(&parent, &context()).unwrap();
    let next = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    assert_eq!(read_view(&next, 100), expected_working);
    assert_eq!(f.journal.queued_patch_bytes(&context()).unwrap(), queued);
    let rebased = f.journal.pending(None, 256, &context()).unwrap().remove(0);
    assert_eq!(rebased.sequence, later.sequence);
    assert_eq!(rebased.base_visible_len, 50);
    assert_eq!(rebased.intent, *handoff.intent());
    let mut saved_bytes = vec![0; 100];
    next.read_checkpoint(&rebased, 0, &mut saved_bytes, &context())
        .unwrap();
    assert_eq!(saved_bytes, expected_saved);
    error(
        next.read_checkpoint(&later, 0, &mut [0], &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    finish_collection(&f.journal);
    // Resume intentionally selects the promised Saved view, not ordinary writes.
    let reopened = WriteJournal::open(f.dir.path()).unwrap();
    let recovered = reopened
        .resume(next.operation(), f.local.clone(), &context())
        .unwrap();
    assert_eq!(read_view(&recovered, 100), expected_saved);
    error(
        f.session
            .adopt_continuation(&handoff, f.local.clone(), &context())
            .map(|_| ()),
        "STALE_WRITE_SESSION",
    );
    error(next.write_at(0, b"old", &context()), "STALE_WRITE_SESSION");
    assert_eq!(
        journal_db(&f)
            .query_row("SELECT count(*) FROM retained_overlays", [], |r| r
                .get::<_, usize>(0))
            .unwrap(),
        1
    );
}

#[test]
fn adoption_failures_and_a_lost_committed_reply_are_idempotent() {
    for point in [
        "content.write.before_adoption",
        "content.write.adoption_page",
        "content.write.adoption_staged",
        "content.write.adoption_archived",
        "content.write.adoption_written",
        "content.write.adopted",
    ] {
        let mut f = Fixture::new();
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        let handoff = f
            .store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap();
        fault::configure(Some(FaultConfig::new(point, FaultAction::Fail)));
        let result = f
            .session
            .adopt_continuation(&handoff, f.local.clone(), &context());
        fault::configure(None);
        assert!(result.is_err(), "{point}");
        let committed = point == "content.write.adopted";
        assert_eq!(
            journal_db(&f)
                .query_row("SELECT count(*) FROM retained_overlays", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            usize::from(committed)
        );
        assert_eq!(
            f.journal.pending(None, 256, &context()).unwrap().is_empty(),
            committed
        );
        let next = f
            .session
            .adopt_continuation(&handoff, f.local.clone(), &context())
            .unwrap();
        assert_eq!(next.adoption_work().unwrap().replay, committed);
        assert_eq!(read_view(&next, 19), b"durable local bytes");
        assert_eq!(
            journal_db(&f)
                .query_row("SELECT count(*) FROM retained_overlays", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            1
        );
    }
}

#[test]
fn adoption_staging_defers_node_sweep_until_the_new_roots_are_committed() {
    use tkfs::model::content::CHUNK_SIZE;
    let mut f = Fixture::new();
    let parent = vec![11; 2 * CHUNK_SIZE as usize];
    f.session.write_at(0, &parent, &context()).unwrap();
    f.checkpoint = f.session.checkpoint(&context()).unwrap().unwrap();
    f.prepared = f
        .session
        .prepare(&f.local, &f.checkpoint, &context())
        .unwrap();
    f.store
        .select_content_checkpoint(&f.checkpoint, &context())
        .unwrap();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    f.session
        .write_at(CHUNK_SIZE + 7, b"later", &context())
        .unwrap();
    f.session.checkpoint(&context()).unwrap();
    f.session
        .write_at(CHUNK_SIZE + 19, b"buffered", &context())
        .unwrap();
    let expected = read_view(&f.session, parent.len());
    let handoff = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    let mut config = FaultConfig::new("content.write.adoption_page", FaultAction::Hang);
    config.ms = Some(1200);
    config.count = Some(1);
    fault::configure(Some(config));
    let session = f.session.clone();
    let native = f.local.clone();
    let job = std::thread::spawn(move || session.adopt_continuation(&handoff, native, &context()));
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let mut cursor = None;
    let mut deferred = false;
    for _ in 0..8 {
        let page = f.journal.reclaim_orphans(cursor, &context()).unwrap();
        assert!(page.validated + page.examined + page.nodes_examined <= 256);
        if page.nodes_deferred {
            assert_eq!(page.nodes_removed, 0);
            assert_eq!(page.nodes_examined, 0);
            deferred = true;
            break;
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    let result = job.join().unwrap();
    fault::configure(None);
    assert!(deferred);
    let next = result.unwrap();
    assert!(next.adoption_work().unwrap().map.nodes_written > 0);
    finish_collection(&f.journal);
    assert_eq!(read_view(&next, parent.len()), expected);
    let saved = f.journal.pending(None, 256, &context()).unwrap().remove(0);
    next.prepare(&f.local, &saved, &context()).unwrap();
}

#[test]
fn concurrent_mutation_during_adoption_staging_is_preserved_and_fences_the_result() {
    let mut f = Fixture::new();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let handoff = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    let mut config = FaultConfig::new("content.write.adoption_page", FaultAction::Hang);
    config.ms = Some(250);
    config.count = Some(1);
    fault::configure(Some(config));
    let session = f.session.clone();
    let local = f.local.clone();
    let cloned = handoff.clone();
    let job = std::thread::spawn(move || session.adopt_continuation(&cloned, local, &context()));
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    f.session.write_at(0, b"concurrent", &context()).unwrap();
    let later = f.session.checkpoint(&context()).unwrap().unwrap();
    let result = job.join().unwrap();
    fault::configure(None);
    error(result.map(|_| ()), "STALE_WRITE_CHECKPOINT");
    assert_eq!(read_view(&f.session, 10), b"concurrent");
    assert_eq!(
        f.journal.pending(None, 256, &context()).unwrap(),
        vec![later]
    );
    let next = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    assert_eq!(read_view(&next, 10), b"concurrent");
    assert_eq!(
        journal_db(&f)
            .query_row("SELECT count(*) FROM retained_overlays", [], |r| r
                .get::<_, usize>(0))
            .unwrap(),
        1
    );
}

#[test]
fn retained_roots_survive_unavailable_canonical_bytes_and_missing_archive_fails_closed() {
    for missing_archive in [false, true] {
        let mut f = Fixture::new();
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        let handoff = f
            .store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap();
        let next = f
            .session
            .adopt_continuation(&handoff, f.local.clone(), &context())
            .unwrap();
        std::fs::remove_file(
            f.dir
                .path()
                .join("objects")
                .join(f.prepared.content().digest.to_string()),
        )
        .unwrap();
        assert!(next.read_range(0, &mut [0], &context()).is_err());
        if missing_archive {
            journal_db(&f)
                .execute("DELETE FROM retained_overlays", [])
                .unwrap();
            assert!(f.journal.reclaim_orphans(None, &context()).is_err());
        } else {
            finish_collection(&f.journal);
        }
        assert_eq!(
            journal_db(&f)
                .query_row("SELECT count(*) FROM payloads", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            1
        );
    }
}

#[test]
fn missing_checkpoint_or_discarded_retention_cannot_authorize_adoption() {
    for damage in [
        "DELETE FROM checkpoint_records",
        "CREATE TRIGGER discard BEFORE INSERT ON retained_overlays BEGIN SELECT RAISE(IGNORE); END",
    ] {
        let mut f = Fixture::new();
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        let handoff = f
            .store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap();
        journal_db(&f).execute_batch(damage).unwrap();
        error(
            f.session
                .adopt_continuation(&handoff, f.local.clone(), &context())
                .map(|_| ()),
            "CORRUPT_WRITE_JOURNAL",
        );
        assert_eq!(read_view(&f.session, 19), b"durable local bytes");
        f.pending();
    }
}

#[test]
fn wrong_native_provider_or_journal_scope_cannot_adopt_a_continuation() {
    let mut f = Fixture::new();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let handoff = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let other = Arc::new(LocalContent::open(dir.path()).unwrap());
    error(
        f.session
            .adopt_continuation(&handoff, other.clone(), &context())
            .map(|_| ()),
        "CONTENT_SAVE_WRONG_STORE",
    );
    let foreign = WriteJournal::open(dir.path())
        .unwrap()
        .create(f.checkpoint.intent.clone(), f.local.clone(), &context())
        .unwrap();
    error(
        foreign
            .adopt_continuation(&handoff, f.local.clone(), &context())
            .map(|_| ()),
        "CONTENT_SAVE_WRONG_STORE",
    );
    assert_eq!(read_view(&f.session, 19), b"durable local bytes");
}

#[test]
fn process_exit_at_private_adoption_boundaries_recovers_both_databases() {
    for overlap in [false, true] {
        for point in [
            "content.write.before_adoption",
            "content.write.adoption_page",
            "content.write.adoption_staged",
            "content.write.adoption_archived",
            "content.write.adoption_written",
            "content.write.adopted",
        ] {
            let mut f = Fixture::new();
            if overlap {
                f.session.write_at(19, &[b'x'; 81], &context()).unwrap();
                f.checkpoint = f.session.checkpoint(&context()).unwrap().unwrap();
                f.prepared = f
                    .session
                    .prepare(&f.local, &f.checkpoint, &context())
                    .unwrap();
                f.store
                    .select_content_checkpoint(&f.checkpoint, &context())
                    .unwrap();
            }
            let receipt = f
                .store
                .publish_content_save(&f.prepared, &context())
                .unwrap();
            let expected = if overlap {
                f.session.set_len(50, &context()).unwrap();
                f.session.set_len(100, &context()).unwrap();
                f.session.checkpoint(&context()).unwrap();
                read_view(&f.session, 100)
            } else {
                b"durable local bytes".to_vec()
            };
            let saved_before = f.journal.pending(None, 256, &context()).unwrap().remove(0);
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "child_process_publication",
                    "--ignored",
                    "--nocapture",
                ])
                .env("TKFS_SAVE_CHILD_ROOT", f.dir.path())
                .env(
                    "TKFS_SAVE_CHILD_OPERATION",
                    f.session.operation().to_string(),
                )
                .env("TKFS_SAVE_CHILD_POINT", point)
                .env("TKFS_SAVE_CHILD_CONTINUE", "2")
                .env(
                    "TKFS_SAVE_CHILD_PARENT",
                    serde_json::to_string(&f.checkpoint).unwrap(),
                )
                .env("TKFS_SAVE_CHILD_BUFFERED", if overlap { "1" } else { "0" })
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{point} overlap={overlap}");
            let committed = point == "content.write.adopted";
            let db = journal_db(&f);
            assert_eq!(
                db.query_row("SELECT count(*) FROM retained_overlays", [], |r| r
                    .get::<_, usize>(0))
                    .unwrap(),
                usize::from(committed)
            );
            let journal = WriteJournal::open(f.dir.path()).unwrap();
            assert_eq!(
                journal.pending(None, 256, &context()).unwrap().len(),
                usize::from(overlap || !committed)
            );
            let mut fresh = Store::open_existing(f.dir.path()).unwrap();
            let handoff = fresh
                .continue_content_save(&f.checkpoint, &context())
                .unwrap();
            let raw: Vec<u8> = db
                .query_row(
                    "SELECT working FROM sessions WHERE id=?",
                    [f.session.operation().to_string()],
                    |r| r.get(0),
                )
                .unwrap();
            let record: serde_json::Value = serde_json::from_slice(&raw).unwrap();
            let value = &record["value"];
            assert_eq!(
                value["intent"],
                serde_json::to_value(if committed {
                    handoff.intent()
                } else {
                    &f.checkpoint.intent
                })
                .unwrap()
            );
            assert_eq!(
                value["roots"]["floor"].as_u64().unwrap(),
                if committed { f.checkpoint.sequence } else { 0 }
            );
            if overlap {
                assert_eq!(
                    value["sequence"].as_u64().unwrap(),
                    saved_before.sequence + 2
                );
                assert_eq!(
                    value["base_visible_len"].as_u64().unwrap(),
                    if committed { 50 } else { 0 }
                );
                let root: tkfs::content::overlay_map::MapRoot =
                    serde_json::from_value(value["roots"]["patches"].clone()).unwrap();
                let patch = tkfs::content::overlay_map::OverlayMap::new(
                    &db,
                    f.session.operation(),
                    tkfs::content::overlay_map::MapKind::Patches,
                )
                .get(
                    root,
                    0,
                    value["roots"]["floor"].as_u64().unwrap(),
                    &mut tkfs::content::overlay_map::MapWork::default(),
                    &context(),
                )
                .unwrap()
                .unwrap();
                let tkfs::content::overlay_map::MapValue::Patch {
                    hash,
                    valid,
                    sequence,
                } = patch
                else {
                    panic!("expected buffered patch")
                };
                assert_eq!(valid as usize, 75 + CHILD_BUFFER.len());
                assert_eq!(sequence, saved_before.sequence + 2);
                let payload: Vec<u8> = db
                    .query_row(
                        "SELECT data FROM payloads WHERE hash=?",
                        [hash.to_string()],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(Digest::bytes(&payload), hash);
                assert_eq!(&payload[75..75 + CHILD_BUFFER.len()], CHILD_BUFFER);
                for i in 50..75 {
                    assert_eq!(
                        payload[tkfs::model::content::CHUNK_SIZE as usize + i / 8] & (1 << (i % 8)),
                        0
                    );
                }
                for i in 75..75 + CHILD_BUFFER.len() {
                    assert_ne!(
                        payload[tkfs::model::content::CHUNK_SIZE as usize + i / 8] & (1 << (i % 8)),
                        0
                    );
                }
                // Product audit validates the typed header/seal, complete root set
                // and inventory before resume intentionally discards ordinary work.
                finish_collection(&journal);
            }
            let resumed = journal
                .resume(f.session.operation(), f.local.clone(), &context())
                .unwrap();
            let recovered = if committed {
                error(
                    resumed
                        .adopt_continuation(&handoff, f.local.clone(), &context())
                        .map(|_| ()),
                    "STALE_WRITE_SESSION",
                );
                resumed
            } else {
                resumed
                    .adopt_continuation(&handoff, f.local.clone(), &context())
                    .unwrap()
            };
            assert_eq!(read_view(&recovered, expected.len()), expected);
            assert_eq!(recovered.checkpoint(&context()).unwrap().is_some(), overlap);
            assert_eq!(
                fresh
                    .content_checkpoint_receipt(&f.checkpoint, &context())
                    .unwrap(),
                Some(receipt)
            );
            assert_eq!(
                db.query_row("SELECT count(*) FROM retained_overlays", [], |r| r
                    .get::<_, usize>(0))
                    .unwrap(),
                1
            );
            if overlap {
                let saved = journal.pending(None, 256, &context()).unwrap().remove(0);
                assert_eq!(saved.sequence, saved_before.sequence);
                assert_eq!(saved.base_visible_len, 50);
                let prepared = recovered.prepare(&f.local, &saved, &context()).unwrap();
                let oracle = f
                    .local
                    .import(&mut expected.as_slice(), expected.len() as u64, &context())
                    .unwrap();
                assert_eq!(prepared.content(), &oracle);
                fresh.select_content_checkpoint(&saved, &context()).unwrap();
                let receipt = fresh.publish_content_save(&prepared, &context()).unwrap();
                assert_eq!(
                    fresh.publish_content_save(&prepared, &context()).unwrap(),
                    receipt
                );
                assert_eq!(fresh.content_admission_page(0, 256).unwrap().len(), 2);
                assert!(fresh.verify_derived_state().unwrap().is_empty());
            }
            finish_collection(&journal);
            println!(
                "WAVE6_ADOPTION_CRASH {}",
                serde_json::json!({"point":point,"overlap":overlap,
            "parent_sequence":f.checkpoint.sequence,"saved_sequence":saved_before.sequence,"committed":committed,
            "saved_bytes":expected.len(),"working_checked_before_resume":overlap,"durable_saved_republished_once":overlap,
            "ordinary_work_intentionally_discarded_on_resume":overlap})
            );
        }
    }
}

#[test]
fn cancellation_and_sqlite_deadline_before_adoption_leave_the_old_save_recoverable() {
    let mut f = Fixture::new();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let handoff = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    let ctx = context();
    let cancel = ctx.cancel.clone();
    let mut config = FaultConfig::new("content.write.adoption_page", FaultAction::Hang);
    config.ms = Some(250);
    config.count = Some(1);
    fault::configure(Some(config));
    let session = f.session.clone();
    let local = f.local.clone();
    let cloned = handoff.clone();
    let job = std::thread::spawn(move || session.adopt_continuation(&cloned, local, &ctx));
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    cancel.cancel();
    let result = job.join().unwrap();
    fault::configure(None);
    error(result.map(|_| ()), "CONTENT_CANCELLED");
    f.pending();
    let db = journal_db(&f);
    db.execute_batch("CREATE TRIGGER stall_adoption BEFORE INSERT ON retained_overlays BEGIN
        SELECT sum(x) FROM (WITH RECURSIVE nums(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM nums WHERE x<100000000) SELECT x FROM nums); END").unwrap();
    let ctx = ReadContext {
        deadline: Instant::now() + Duration::from_millis(80),
        cancel: Cancel::new(),
    };
    error(
        f.session
            .adopt_continuation(&handoff, f.local.clone(), &ctx)
            .map(|_| ()),
        "CONTENT_DEADLINE",
    );
    assert!(!f.journal.connection_failed());
    f.pending();
    assert_eq!(read_view(&f.session, 19), b"durable local bytes");
    db.execute_batch("DROP TRIGGER stall_adoption").unwrap();
    let next = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    assert_eq!(read_view(&next, 19), b"durable local bytes");
}
impl Generation {
    fn new(f: &Fixture, continuation: &SaveContinuation, bytes: &[u8]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let journal = WriteJournal::open(dir.path()).unwrap();
        let session = journal
            .create(continuation.intent().clone(), f.local.clone(), &context())
            .unwrap();
        session.write_at(0, bytes, &context()).unwrap();
        let checkpoint = session.checkpoint(&context()).unwrap().unwrap();
        let prepared = session.prepare(&f.local, &checkpoint, &context()).unwrap();
        Self {
            session,
            checkpoint,
            prepared,
            _journal: journal,
            _dir: dir,
        }
    }
}

#[test]
fn continuation_preserves_exact_receipts_and_newer_original_journal_work() {
    let mut f = Fixture::new();
    let first = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    f.session.write_at(0, b"later", &context()).unwrap();
    let later = f.session.checkpoint(&context()).unwrap().unwrap();
    let handoff = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    assert_eq!(handoff.intent().operation, f.session.operation());
    assert_eq!(handoff.intent().base.content, *first.content());
    assert_eq!(
        handoff.intent().base.generation,
        f.checkpoint.intent.base.generation + 2
    );
    assert_eq!(
        handoff.intent().content_parent.as_deref(),
        Some(first.event())
    );
    assert_eq!(
        handoff.intent().alive_parent.as_deref(),
        Some(first.event())
    );
    assert_eq!(handoff.store_root(), f.dir.path().canonicalize().unwrap());
    assert_eq!(handoff.base_pin().provider(), f.local.identity());
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap()[0], later);
    let mut bytes = vec![0; 5];
    f.session.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(bytes, b"later");
    assert_eq!(
        f.store
            .content_checkpoint_receipt(&f.checkpoint, &context())
            .unwrap(),
        Some(first.clone())
    );
    assert!(
        f.store
            .content_checkpoint_receipt(&later, &context())
            .unwrap()
            .is_none()
    );
    error(
        f.store.continue_content_save(&later, &context()),
        "CONTENT_SAVE_NOT_COMMITTED",
    );
    assert!(
        f.store
            .content_save_receipt(f.session.operation(), &context())
            .unwrap()
            .is_none()
    );
    let retry = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    assert_eq!(retry.intent(), handoff.intent());
    let next = Generation::new(&f, &handoff, b"next generation");
    f.store
        .select_content_checkpoint(&next.checkpoint, &context())
        .unwrap();
    let second = f
        .store
        .publish_content_save(&next.prepared, &context())
        .unwrap();
    assert_ne!(first.event(), second.event());
    assert_eq!(first.operation(), second.operation());
    assert_eq!(
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap(),
        first
    );
    assert_eq!(
        f.store
            .content_checkpoint_receipt(&f.checkpoint, &context())
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        f.store
            .content_checkpoint_receipt(&next.checkpoint, &context())
            .unwrap(),
        Some(second)
    );
    assert_eq!(f.store.content_admission_page(0, 256).unwrap().len(), 2);
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap()[0], later);
    f.oracle();
}

#[test]
fn continuation_chain_replays_older_generations_without_changing_latest_selection() {
    let mut f = Fixture::new();
    let first = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let one = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    let second = Generation::new(&f, &one, b"second");
    f.store
        .select_content_checkpoint(&second.checkpoint, &context())
        .unwrap();
    let second_receipt = f
        .store
        .publish_content_save(&second.prepared, &context())
        .unwrap();
    let two = f
        .store
        .continue_content_save(&second.checkpoint, &context())
        .unwrap();
    let third = Generation::new(&f, &two, b"third");
    f.store
        .select_content_checkpoint(&third.checkpoint, &context())
        .unwrap();
    let before: Vec<u8> = f
        .store
        .db
        .query_row("SELECT selected FROM content_save_operations", [], |r| {
            r.get(0)
        })
        .unwrap();
    f.store
        .select_content_checkpoint(&f.checkpoint, &context())
        .unwrap();
    f.store
        .select_content_checkpoint(&second.checkpoint, &context())
        .unwrap();
    assert_eq!(
        before,
        f.store
            .db
            .query_row("SELECT selected FROM content_save_operations", [], |r| r
                .get::<_, Vec<u8>>(0))
            .unwrap()
    );
    assert_eq!(
        f.store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap()
            .intent(),
        one.intent()
    );
    assert_eq!(
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap(),
        first
    );
    assert_eq!(
        f.store
            .publish_content_save(&second.prepared, &context())
            .unwrap(),
        second_receipt
    );
    let third_receipt = f
        .store
        .publish_content_save(&third.prepared, &context())
        .unwrap();
    assert_ne!(third_receipt.event(), second_receipt.event());
    assert_eq!(
        f.store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                .get::<_, usize>(0))
            .unwrap(),
        3
    );
    assert_eq!(
        f.store
            .db
            .query_row("SELECT count(*) FROM content_save_successors", [], |r| {
                r.get::<_, usize>(0)
            })
            .unwrap(),
        2
    );
    assert_eq!(second.session.operation(), f.session.operation());
    f.oracle();
    f.pending();
}

#[test]
fn continuation_does_not_parent_a_remote_competing_content_revision() {
    let mut f = Fixture::new();
    let first = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let root = FileContentRef::new(ContentKind::Direct, Digest::bytes(b"remote"), 6).unwrap();
    let mut remote = f.store.event(
        &f.store.branch(&f.store.active).unwrap(),
        vec![Change {
            entity: f.entity.clone(),
            field: "content".into(),
            parents: vec![first.event().into()],
            value: serde_json::json!(root),
        }],
    );
    remote.device = "other-peer".into();
    let remote_id = remote.id.clone();
    f.store.commit(remote, false).unwrap();
    let handoff = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    assert_eq!(
        handoff.intent().content_parent.as_deref(),
        Some(first.event())
    );
    assert_eq!(handoff.intent().base.content, *first.content());
    let next = Generation::new(&f, &handoff, b"local next");
    f.store
        .select_content_checkpoint(&next.checkpoint, &context())
        .unwrap();
    let receipt = f
        .store
        .publish_content_save(&next.prepared, &context())
        .unwrap();
    assert!(
        f.store
            .conflicts()
            .unwrap()
            .iter()
            .any(|c| c.field == "content"
                && c.alternatives.iter().any(|a| a.revision == remote_id)
                && c.alternatives.iter().any(|a| a.revision == receipt.event()))
    );
    f.oracle();
    f.pending();
}

#[test]
fn later_local_mutation_or_checkout_fences_new_continuation() {
    for action in ["rename", "delete", "permission", "operation", "checkout"] {
        let mut f = Fixture::new();
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        let mut foreign = Store::open_existing(f.dir.path()).unwrap();
        match action {
            "rename" => foreign.rename(&f.entity, "other", false).unwrap(),
            "delete" => foreign.delete(&f.entity).unwrap(),
            "permission" => {
                let mut basic = foreign.entry(&f.entity).unwrap().basic_info();
                basic.posix_mode = Some(0o400);
                foreign.set_basic(&f.entity, basic).unwrap();
            }
            "operation" => {
                foreign.begin_content_save(&f.entity, &context()).unwrap();
            }
            "checkout" => foreign.checkout(&foreign.active.clone()).unwrap(),
            _ => unreachable!(),
        }
        error(
            f.store.continue_content_save(&f.checkpoint, &context()),
            if action == "checkout" {
                "STALE_CONTENT_VIEW"
            } else {
                "STALE_CONTENT_SAVE"
            },
        );
        assert_eq!(
            f.store
                .db
                .query_row("SELECT count(*) FROM content_save_successors", [], |r| {
                    r.get::<_, usize>(0)
                })
                .unwrap(),
            0
        );
        f.pending();
        f.oracle();
    }
}

#[test]
fn continuation_lost_reply_and_precommit_failures_are_idempotent() {
    for point in [
        "content.save.before_successor",
        "content.save.successor_written",
        "content.save.successor_committed",
    ] {
        let mut f = Fixture::new();
        let receipt = f
            .store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        fault::configure(Some(FaultConfig::new(point, FaultAction::Fail)));
        let result = f.store.continue_content_save(&f.checkpoint, &context());
        fault::configure(None);
        assert!(result.is_err());
        let committed = point == "content.save.successor_committed";
        let count: usize = f
            .store
            .db
            .query_row("SELECT count(*) FROM content_save_successors", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, usize::from(committed));
        let handoff = f
            .store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap();
        let after: Vec<u8> = f
            .store
            .db
            .query_row("SELECT capture FROM content_save_successors", [], |r| {
                r.get(0)
            })
            .unwrap();
        let retried = f
            .store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap();
        assert_eq!(handoff.intent(), retried.intent());
        assert_eq!(
            after,
            f.store
                .db
                .query_row("SELECT capture FROM content_save_successors", [], |r| r
                    .get::<_, Vec<u8>>(0))
                .unwrap()
        );
        assert_eq!(
            f.store
                .content_checkpoint_receipt(&f.checkpoint, &context())
                .unwrap(),
            Some(receipt)
        );
        f.pending();
        f.oracle();
    }
}

#[test]
fn archive_and_successor_writes_cannot_be_silently_discarded() {
    for target in ["archive", "capture", "successor", "old-history"] {
        let mut f = Fixture::new();
        if target == "archive" {
            f.store.db.execute_batch("CREATE TEMP TRIGGER discard_archive BEFORE INSERT ON content_save_history BEGIN SELECT RAISE(IGNORE); END").unwrap();
            error(
                f.store.publish_content_save(&f.prepared, &context()),
                "CONTENT_SAVE_PUBLICATION_LOST",
            );
            assert_eq!(f.event_count(), 0);
            assert_eq!(f.outbox_count(), 0);
        } else {
            f.store
                .publish_content_save(&f.prepared, &context())
                .unwrap();
            let sql = match target {
                "capture" => {
                    "CREATE TEMP TRIGGER discard_successor AFTER UPDATE OF capture ON content_save_operations BEGIN UPDATE content_save_operations SET capture=OLD.capture WHERE operation=NEW.operation; END"
                }
                "successor" => {
                    "CREATE TEMP TRIGGER discard_successor BEFORE INSERT ON content_save_successors BEGIN SELECT RAISE(IGNORE); END"
                }
                "old-history" => {
                    "CREATE TEMP TRIGGER discard_successor AFTER INSERT ON content_save_successors BEGIN DELETE FROM content_save_history; END"
                }
                _ => unreachable!(),
            };
            f.store.db.execute_batch(sql).unwrap();
            assert!(
                f.store
                    .continue_content_save(&f.checkpoint, &context())
                    .is_err()
            );
            f.store
                .db
                .execute_batch("DROP TRIGGER discard_successor")
                .unwrap();
            f.store
                .continue_content_save(&f.checkpoint, &context())
                .unwrap();
            assert_eq!(
                f.store
                    .db
                    .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                        .get::<_, usize>(0))
                    .unwrap(),
                1
            );
        }
        f.pending();
        f.oracle();
    }
}

#[test]
fn missing_or_corrupt_history_and_successor_records_keep_original_recovery() {
    for target in ["history", "successor", "successor-missing"] {
        let mut f = Fixture::new();
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        f.store
            .continue_content_save(&f.checkpoint, &context())
            .unwrap();
        let sql = match target {
            "history" => "DELETE FROM content_save_history",
            "successor" => "UPDATE content_save_successors SET capture=x'00'",
            _ => "DELETE FROM content_save_successors",
        };
        f.store.db.execute_batch(sql).unwrap();
        error(
            f.store.continue_content_save(&f.checkpoint, &context()),
            "CORRUPT_CONTENT_SAVE",
        );
        f.pending();
    }
}

#[test]
fn pre_continuation_capture_encoding_and_receipts_remain_readable() {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct OldCapture {
        intent: tkfs::content::write::SaveIntent,
        view_generation: u64,
        event: tkfs::core::Event,
    }
    let mut f = Fixture::new();
    let receipt = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let raw: Vec<u8> = f
        .store
        .db
        .query_row("SELECT capture FROM content_save_operations", [], |r| {
            r.get(0)
        })
        .unwrap();
    let record: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let old: OldCapture = serde_json::from_value(record["value"].clone()).unwrap();
    let checksum = tkfs::core::hash(&serde_json::to_vec(&("capture:v1", &old)).unwrap());
    #[derive(serde::Serialize)]
    struct OldRecord<'a> {
        value: &'a OldCapture,
        checksum: String,
    }
    let encoded = serde_json::to_vec(&OldRecord {
        value: &old,
        checksum,
    })
    .unwrap();
    assert_eq!(raw, encoded);
    f.store
        .db
        .execute("DELETE FROM content_save_history", [])
        .unwrap();
    let next = f
        .store
        .continue_content_save(&f.checkpoint, &context())
        .unwrap();
    assert_eq!(
        f.store
            .db
            .query_row("SELECT capture FROM content_save_history", [], |r| r
                .get::<_, Vec<u8>>(0))
            .unwrap(),
        raw
    );
    assert_eq!(next.intent().base.content, *receipt.content());
    assert_eq!(
        f.store
            .content_checkpoint_receipt(&f.checkpoint, &context())
            .unwrap(),
        Some(receipt)
    );
    f.pending();
}

#[test]
fn process_exit_during_archive_and_continuation_keeps_exact_replay() {
    for point in [
        "content.save.archived",
        "content.save.before_successor",
        "content.save.successor_written",
        "content.save.successor_committed",
    ] {
        let f = Fixture::new();
        let continuation = point != "content.save.archived";
        let mut initial = Store::open_existing(f.dir.path()).unwrap();
        if continuation {
            initial
                .publish_content_save(&f.prepared, &context())
                .unwrap();
        }
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "child_process_publication",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_SAVE_CHILD_ROOT", f.dir.path())
            .env(
                "TKFS_SAVE_CHILD_OPERATION",
                f.session.operation().to_string(),
            )
            .env("TKFS_SAVE_CHILD_POINT", point)
            .env(
                "TKFS_SAVE_CHILD_CONTINUE",
                if continuation { "1" } else { "0" },
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{point}");
        let mut fresh = Store::open_existing(f.dir.path()).unwrap();
        let had_successor = point == "content.save.successor_committed";
        assert_eq!(
            fresh
                .db
                .query_row("SELECT count(*) FROM content_save_successors", [], |r| {
                    r.get::<_, usize>(0)
                })
                .unwrap(),
            usize::from(had_successor)
        );
        if !continuation {
            assert!(
                fresh
                    .content_checkpoint_receipt(&f.checkpoint, &context())
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                fresh
                    .db
                    .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                        .get::<_, usize>(0))
                    .unwrap(),
                0
            );
            fresh.publish_content_save(&f.prepared, &context()).unwrap();
        }
        let handoff = fresh
            .continue_content_save(&f.checkpoint, &context())
            .unwrap();
        assert_eq!(
            handoff.intent().base.generation,
            f.checkpoint.intent.base.generation + 2
        );
        assert_eq!(
            fresh
                .continue_content_save(&f.checkpoint, &context())
                .unwrap()
                .intent(),
            handoff.intent()
        );
        assert_eq!(
            fresh
                .db
                .query_row("SELECT count(*) FROM content_save_successors", [], |r| {
                    r.get::<_, usize>(0)
                })
                .unwrap(),
            1
        );
        assert!(fresh.verify_derived_state().unwrap().is_empty());
        f.pending();
    }
}

#[test]
fn continuation_unwind_and_failed_rollback_preserve_the_committed_parent() {
    for denied in [false, true] {
        let mut f = Fixture::new();
        let receipt = f
            .store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        if denied {
            deny_rollback(&f.store.db);
        }
        fault::configure(Some(FaultConfig::new(
            "content.save.successor_written",
            FaultAction::Panic,
        )));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f.store.continue_content_save(&f.checkpoint, &context())
        }));
        fault::configure(None);
        assert!(result.is_err());
        if denied {
            assert!(f.store.content_save_connection_failed());
        } else {
            assert!(f.store.db.is_autocommit());
        }
        let mut fresh = Store::open_existing(f.dir.path()).unwrap();
        assert_eq!(
            fresh
                .content_checkpoint_receipt(&f.checkpoint, &context())
                .unwrap(),
            Some(receipt)
        );
        assert_eq!(
            fresh
                .db
                .query_row("SELECT count(*) FROM content_save_successors", [], |r| {
                    r.get::<_, usize>(0)
                })
                .unwrap(),
            0
        );
        fresh
            .continue_content_save(&f.checkpoint, &context())
            .unwrap();
        f.pending();
    }
}

#[test]
fn publication_receipt_and_outbox_survive_lost_reply_and_restart() {
    let mut f = Fixture::new();
    let receipt = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    assert_eq!(receipt.operation(), f.session.operation());
    assert_eq!(receipt.sequence(), f.checkpoint.sequence);
    assert_eq!(receipt.content(), f.prepared.content());
    assert_eq!(
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap(),
        receipt
    );
    assert_eq!(f.event_count(), 1);
    assert_eq!(f.outbox_count(), 1);
    assert_eq!(
        f.store.entry(&f.entity).unwrap().content_ref.as_ref(),
        Some(receipt.content())
    );
    assert!(f.store.content_admission_page(0, 256).unwrap()[0].publishable);
    f.oracle();
    f.pending();
    drop(f.store);
    let store = Store::open_existing(f.dir.path()).unwrap();
    assert_eq!(
        store
            .content_save_receipt(f.session.operation(), &context())
            .unwrap(),
        Some(receipt.clone())
    );
    let mut bytes = vec![0; receipt.content().total_len as usize];
    f.local
        .read_range(f.prepared.pin(), 0, &mut bytes, &context())
        .unwrap();
    assert_eq!(bytes, b"durable local bytes");
}

#[test]
fn receipt_replay_after_checkout_and_delete_never_reapplies_the_event() {
    let mut f = Fixture::new();
    let receipt = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    f.store.checkout(&f.store.active.clone()).unwrap();
    f.store.delete(&f.entity).unwrap();
    assert!(!f.store.entry(&f.entity).unwrap().alive);
    assert_eq!(
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap(),
        receipt
    );
    assert!(!f.store.entry(&f.entity).unwrap().alive);
    assert!(
        f.store
            .content_save_receipt(Uuid::new_v4(), &context())
            .unwrap()
            .is_none()
    );
    assert_eq!(f.event_count(), 1);
    f.pending();
    f.oracle();
}

#[test]
fn slow_metadata_publication_does_not_hold_the_private_journal_lock() {
    let f = Fixture::new();
    let prepared = f.prepared.clone();
    let mut config = FaultConfig::new("content.save.receipt_written", FaultAction::Hang);
    config.ms = Some(500);
    config.count = Some(1);
    fault::configure(Some(config));
    let worker = std::thread::spawn(move || {
        let mut store = f.store;
        store.publish_content_save(&prepared, &context())
    });
    let until = Instant::now() + Duration::from_secs(5);
    while fault::fired() == 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    let fired = fault::fired();
    let started = Instant::now();
    let mutation = f.session.write_at(0, b"newer", &context());
    let mut old = vec![0; f.checkpoint.length as usize];
    let read = f
        .session
        .read_checkpoint(&f.checkpoint, 0, &mut old, &context());
    let elapsed = started.elapsed();
    let published = worker.join().unwrap();
    fault::configure(None);
    assert_eq!(fired, 1);
    mutation.unwrap();
    read.unwrap();
    published.unwrap();
    assert_eq!(old, b"durable local bytes");
    assert!(elapsed < Duration::from_millis(250), "{elapsed:?}");
    println!(
        "SAVE_JOURNAL_DURING_METADATA elapsed_ms={:.4}",
        elapsed.as_secs_f64() * 1000.0
    );
    let newer = f.session.checkpoint(&context()).unwrap().unwrap();
    assert!(newer.sequence > f.checkpoint.sequence);
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap()[0], newer);
}

#[test]
fn capture_frontier_and_generation_limits_fail_without_partial_ownership() {
    let mut f = Fixture::new();
    let branch = f.store.active.clone();
    for i in 0..257 {
        f.store
            .db
            .execute(
                "INSERT INTO event_frontier(branch,event) VALUES(?,?)",
                rusqlite::params![branch, format!("fixture-extra-{i}")],
            )
            .unwrap();
    }
    error(
        f.store.begin_content_save(&f.entity, &context()),
        "CONTENT_SAVE_CAPTURE_LIMIT",
    );
    assert!(f.store.db.is_autocommit());
    f.store
        .db
        .execute(
            "DELETE FROM event_frontier WHERE event LIKE 'fixture-extra-%'",
            [],
        )
        .unwrap();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    f.store
        .db
        .execute("UPDATE content_save_entities SET generation=?", [i64::MAX])
        .unwrap();
    assert!(f.store.begin_content_save(&f.entity, &context()).is_err());
    let count: i64 = f
        .store
        .db
        .query_row("SELECT count(*) FROM content_save_operations", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
    assert!(f.store.db.is_autocommit());
    f.pending();
    f.oracle();
}

#[test]
fn paired_metadata_publication_records_receipt_cost_without_canonical_io() {
    // Current-build control: the same captured typed event and admission outbox,
    // without the coordinator's durable receipt. Capture, checkpoint, selection
    // and canonical preparation are deliberately outside both timed regions.
    // This does not compare the old executable, mounted builds or full saves.
    for iteration in 0..6 {
        let mut control = Fixture::new();
        let raw: Vec<u8> = control
            .store
            .db
            .query_row("SELECT capture FROM content_save_operations", [], |r| {
                r.get(0)
            })
            .unwrap();
        let capture: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let mut event: tkfs::core::Event =
            serde_json::from_value(capture["value"]["event"].clone()).unwrap();
        event.changes[0].value = serde_json::json!(control.prepared.content());
        let mut candidate = Fixture::new();
        assert_eq!(candidate.prepared.content(), control.prepared.content());
        let before = candidate.local.counters.snapshot();
        let baseline = Instant::now();
        control.store.commit(event, true).unwrap();
        let baseline = baseline.elapsed().as_secs_f64() * 1000.0;
        let started = Instant::now();
        candidate
            .store
            .publish_content_save(&candidate.prepared, &context())
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let after = candidate.local.counters.snapshot();
        assert_eq!(
            serde_json::to_value(after).unwrap(),
            serde_json::to_value(before).unwrap(),
            "publication performed canonical byte work"
        );
        assert_eq!(control.event_count(), 1);
        assert_eq!(candidate.event_count(), 1);
        assert_eq!(control.outbox_count(), 1);
        assert_eq!(candidate.outbox_count(), 1);
        control.oracle();
        candidate.oracle();
        println!(
            "WAVE6_SAVE_METADATA {}",
            serde_json::json!({"warmup":iteration==0,
            "control_event_outbox_ms":baseline,"candidate_with_receipt_ms":elapsed,
            "canonical_counters_unchanged":true,"full_wal":true,"same_descriptor":true})
        );
    }
}

#[test]
fn newer_selected_checkpoint_fences_old_preparation() {
    let mut f = Fixture::new();
    f.session.write_at(0, b"newer", &context()).unwrap();
    let checkpoint = f.session.checkpoint(&context()).unwrap().unwrap();
    let prepared = f
        .session
        .prepare(&f.local, &checkpoint, &context())
        .unwrap();
    f.store
        .select_content_checkpoint(&checkpoint, &context())
        .unwrap();
    error(
        f.store.select_content_checkpoint(&f.checkpoint, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    error(
        f.store.publish_content_save(&f.prepared, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    assert_eq!(f.event_count(), 0);
    let receipt = f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(receipt.sequence(), checkpoint.sequence);
    assert_eq!(f.event_count(), 1);
    f.pending();
    f.oracle();
}

#[test]
fn buffered_and_newer_durable_work_are_retained_after_older_publication() {
    let mut f = Fixture::new();
    f.session
        .write_at(0, b"newer buffered", &context())
        .unwrap();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let mut bytes = vec![0; 14];
    f.session.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(bytes, b"newer buffered");
    let newer = f.session.checkpoint(&context()).unwrap().unwrap();
    assert!(newer.sequence > f.checkpoint.sequence);
    error(
        f.store.select_content_checkpoint(&newer, &context()),
        "CONTENT_SAVE_ALREADY_COMMITTED",
    );
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap()[0], newer);
    // Rollover must retain/rebase this newer checkpoint before retiring the old
    // operation. A metadata receipt alone is deliberately not a retire API.
    f.oracle();
}

#[test]
fn newer_foreign_local_operation_fences_old_save_but_keeps_recovery() {
    let mut f = Fixture::new();
    let mut other = Store::open_existing(f.dir.path()).unwrap();
    let newer = other.begin_content_save(&f.entity, &context()).unwrap();
    assert_eq!(newer.base.workspace, f.checkpoint.intent.base.workspace);
    assert!(newer.base.generation > f.checkpoint.intent.base.generation);
    error(
        f.store.publish_content_save(&f.prepared, &context()),
        "STALE_CONTENT_SAVE",
    );
    error(
        f.store.select_content_checkpoint(&f.checkpoint, &context()),
        "STALE_CONTENT_SAVE",
    );
    assert_eq!(f.event_count(), 0);
    f.pending();
    f.oracle();
}

#[test]
fn recreated_numeric_generation_cannot_transfer_an_old_operations_ownership() {
    let mut f = Fixture::new();
    let mut foreign = Store::open_existing(f.dir.path()).unwrap();
    foreign
        .db
        .execute("DELETE FROM content_save_entities", [])
        .unwrap();
    let newer = foreign.begin_content_save(&f.entity, &context()).unwrap();
    assert_eq!(newer.base.generation, f.checkpoint.intent.base.generation);
    assert_ne!(newer.operation, f.checkpoint.intent.operation);
    error(
        f.store.publish_content_save(&f.prepared, &context()),
        "STALE_CONTENT_SAVE",
    );
    assert_eq!(f.event_count(), 0);
    f.pending();
    f.oracle();
}

#[test]
fn ignored_or_discarded_selection_write_cannot_fence_a_new_checkpoint() {
    for action in [
        "SELECT RAISE(IGNORE);",
        "UPDATE content_save_operations SET selected=OLD.selected WHERE operation=NEW.operation;",
    ] {
        let mut f = Fixture::new();
        f.session.write_at(0, b"newer", &context()).unwrap();
        let newer = f.session.checkpoint(&context()).unwrap().unwrap();
        let timing = if action.starts_with("SELECT") {
            "BEFORE"
        } else {
            "AFTER"
        };
        f.store.db.execute_batch(&format!("CREATE TEMP TRIGGER discard_selection {timing} UPDATE OF selected ON content_save_operations BEGIN {action} END")).unwrap();
        error(
            f.store.select_content_checkpoint(&newer, &context()),
            "CONTENT_SAVE_SELECTION_LOST",
        );
        f.store
            .db
            .execute_batch("DROP TRIGGER discard_selection")
            .unwrap();
        f.store
            .select_content_checkpoint(&newer, &context())
            .unwrap();
        error(
            f.store.publish_content_save(&f.prepared, &context()),
            "STALE_WRITE_CHECKPOINT",
        );
        assert_eq!(f.event_count(), 0);
        f.pending();
        f.oracle();
    }
}

#[test]
fn ordinary_local_namespace_and_content_mutations_fence_pending_publication() {
    for operation in ["rename", "delete", "replace", "legacy-write", "permissions"] {
        let mut f = Fixture::new();
        let mut foreign = Store::open_existing(f.dir.path()).unwrap();
        match operation {
            "rename" => foreign.rename(&f.entity, "renamed", false).unwrap(),
            "delete" => foreign.delete(&f.entity).unwrap(),
            "replace" => {
                let replacement = foreign.create("replacement", "file").unwrap();
                foreign.rename(&replacement.id, "file", true).unwrap();
            }
            "legacy-write" => {
                let entry = foreign.entry(&f.entity).unwrap();
                foreign
                    .write_revision(
                        &f.entity,
                        entry.revisions.get("content").cloned(),
                        b"new local save",
                    )
                    .unwrap();
            }
            "permissions" => {
                let mut basic = foreign.entry(&f.entity).unwrap().basic_info();
                basic.posix_mode = Some(0o400);
                foreign.set_basic(&f.entity, basic).unwrap();
            }
            _ => unreachable!(),
        }
        error(
            f.store.publish_content_save(&f.prepared, &context()),
            "STALE_CONTENT_SAVE",
        );
        assert_eq!(f.event_count(), 0);
        f.pending();
        f.oracle();
    }
}

#[test]
fn checkout_epoch_is_read_from_the_database_not_a_stale_store_handle() {
    let mut f = Fixture::new();
    let mut foreign = Store::open_existing(f.dir.path()).unwrap();
    foreign.checkout(&foreign.active.clone()).unwrap();
    assert_ne!(f.store.generation, foreign.generation);
    error(
        f.store.publish_content_save(&f.prepared, &context()),
        "STALE_CONTENT_VIEW",
    );
    assert_eq!(f.event_count(), 0);
    f.pending();
}

#[test]
fn unrelated_local_changes_do_not_reconstruct_causal_parents_or_frontier() {
    let mut f = Fixture::new();
    let unrelated = f.store.create("other", "file").unwrap();
    let latest = unrelated.revisions.get("alive").unwrap();
    let receipt = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let event = f
        .store
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.id == receipt.event())
        .unwrap();
    assert!(!event.dependencies.contains(latest));
    assert_eq!(
        event.changes[0].parents,
        f.checkpoint
            .intent
            .content_parent
            .clone()
            .into_iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        event.changes[1].parents,
        f.checkpoint
            .intent
            .alive_parent
            .clone()
            .into_iter()
            .collect::<Vec<_>>()
    );
    f.oracle();
}

#[test]
fn remote_competing_revision_is_preserved_and_not_silently_reviewed() {
    let mut f = Fixture::new();
    let root = FileContentRef::new(
        ContentKind::Direct,
        Digest::bytes(b"absent remote bytes"),
        19,
    )
    .unwrap();
    let mut remote = f.store.event(
        &f.store.branch(&f.store.active).unwrap(),
        vec![Change {
            entity: f.entity.clone(),
            field: "content".into(),
            parents: f
                .checkpoint
                .intent
                .content_parent
                .clone()
                .into_iter()
                .collect(),
            value: serde_json::json!(root),
        }],
    );
    remote.device = "remote-peer".into();
    let remote_id = remote.id.clone();
    f.store.commit(remote, false).unwrap();
    let receipt = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    let local = f
        .store
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.id == receipt.event())
        .unwrap();
    assert!(!local.dependencies.contains(&remote_id));
    assert!(!local.changes[0].parents.contains(&remote_id));
    let conflicts = f.store.conflicts().unwrap();
    assert!(conflicts.iter().any(|c| c.field == "content"
        && c.alternatives.iter().any(|a| a.revision == remote_id)
        && c.alternatives.iter().any(|a| a.revision == receipt.event())));
    f.oracle();
    f.pending();
}

#[test]
fn event_outbox_receipt_and_generation_rollback_together() {
    for table in ["content_root_outbox", "content_save_operations"] {
        let mut f = Fixture::new();
        let before = f.store.projection(&f.store.active).unwrap();
        let sql = if table == "content_root_outbox" {
            "CREATE TEMP TRIGGER reject_save BEFORE INSERT ON content_root_outbox BEGIN SELECT RAISE(ABORT,'outbox rejected'); END"
        } else {
            "CREATE TEMP TRIGGER reject_save BEFORE UPDATE OF receipt ON content_save_operations WHEN NEW.receipt IS NOT NULL BEGIN SELECT RAISE(ABORT,'receipt rejected'); END"
        };
        f.store.db.execute_batch(sql).unwrap();
        assert!(
            f.store
                .publish_content_save(&f.prepared, &context())
                .is_err()
        );
        assert!(f.store.db.is_autocommit());
        assert_eq!(f.event_count(), 0);
        assert_eq!(f.outbox_count(), 0);
        assert_eq!(f.store.projection(&f.store.active).unwrap(), before);
        assert!(
            f.store
                .content_save_receipt(f.session.operation(), &context())
                .unwrap()
                .is_none()
        );
        f.store
            .db
            .execute_batch("DROP TRIGGER reject_save")
            .unwrap();
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        assert_eq!(f.event_count(), 1);
        assert_eq!(f.outbox_count(), 1);
        f.oracle();
        f.pending();
    }
}

#[test]
fn silently_ignored_receipt_write_cannot_acknowledge_publication() {
    for action in [
        "SELECT RAISE(IGNORE);",
        "UPDATE content_save_operations SET receipt=NULL WHERE operation=NEW.operation;",
    ] {
        let mut f = Fixture::new();
        let timing = if action.starts_with("SELECT") {
            "BEFORE"
        } else {
            "AFTER"
        };
        f.store.db.execute_batch(&format!("CREATE TEMP TRIGGER ignore_receipt {timing} UPDATE OF receipt ON content_save_operations WHEN NEW.receipt IS NOT NULL BEGIN {action} END")).unwrap();
        error(
            f.store.publish_content_save(&f.prepared, &context()),
            "CONTENT_SAVE_PUBLICATION_LOST",
        );
        assert_eq!(f.event_count(), 0);
        assert_eq!(f.outbox_count(), 0);
        assert!(
            f.store
                .content_save_receipt(f.session.operation(), &context())
                .unwrap()
                .is_none()
        );
        f.store
            .db
            .execute_batch("DROP TRIGGER ignore_receipt")
            .unwrap();
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        f.pending();
        f.oracle();
    }
}

#[test]
fn injected_failures_before_and_after_commit_keep_exactly_once_evidence() {
    for point in [
        "content.save.before_metadata",
        "content.save.event_written",
        "content.save.receipt_written",
        "content.save.committed",
    ] {
        let mut f = Fixture::new();
        fault::configure(Some(FaultConfig::new(point, FaultAction::Fail)));
        let result = f.store.publish_content_save(&f.prepared, &context());
        fault::configure(None);
        assert!(result.is_err(), "{point}");
        let committed = point == "content.save.committed";
        assert_eq!(f.event_count(), i64::from(committed));
        assert_eq!(f.outbox_count(), usize::from(committed));
        assert_eq!(
            f.store
                .content_save_receipt(f.session.operation(), &context())
                .unwrap()
                .is_some(),
            committed
        );
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        assert_eq!(f.event_count(), 1);
        assert_eq!(f.outbox_count(), 1);
        f.pending();
        f.oracle();
    }
}

#[test]
fn publication_unwind_releases_writer_and_preserves_recovery() {
    for point in [
        "content.save.before_metadata",
        "content.save.event_written",
        "content.save.receipt_written",
        "content.save.committed",
    ] {
        let mut f = Fixture::new();
        let old_timeout: u64 = f
            .store
            .db
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        let ctx = context();
        fault::configure(Some(FaultConfig::new(point, FaultAction::Panic)));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f.store.publish_content_save(&f.prepared, &ctx)
        }));
        fault::configure(None);
        assert!(result.is_err(), "{point}");
        assert!(f.store.db.is_autocommit(), "writer leaked after {point}");
        ctx.cancel.cancel();
        let sum: u64=f.store.db.query_row("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n",[],|r|r.get(0)).unwrap();
        assert_eq!(
            sum, 50_005_000,
            "publication progress handler survived unwind"
        );
        assert_eq!(
            f.store
                .db
                .query_row("PRAGMA busy_timeout", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            old_timeout
        );
        let other = rusqlite::Connection::open(f.dir.path().join("metadata.sqlite")).unwrap();
        other.busy_timeout(Duration::from_millis(100)).unwrap();
        other.execute_batch("BEGIN IMMEDIATE; ROLLBACK").unwrap();
        let committed = point == "content.save.committed";
        assert_eq!(f.event_count(), i64::from(committed));
        assert_eq!(f.outbox_count(), usize::from(committed));
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        assert_eq!(f.event_count(), 1);
        f.pending();
        f.oracle();
    }
}

#[test]
fn sqlite_rejected_commit_rolls_back_receipt_and_cached_visibility() {
    let mut f = Fixture::new();
    let before = f.store.entry(&f.entity).unwrap();
    f.store.db.commit_hook(Some(|| true));
    let failure = f
        .store
        .publish_content_save(&f.prepared, &context())
        .unwrap_err();
    assert!(failure.downcast_ref::<rusqlite::Error>().is_some());
    assert_ne!(
        tkfs::model::errors::code_of(&failure),
        "CORRUPT_CONTENT_SAVE"
    );
    assert!(f.store.db.is_autocommit());
    assert!(!f.store.content_save_connection_failed());
    assert_eq!(f.store.entry(&f.entity).unwrap(), before);
    assert_eq!(f.event_count(), 0);
    assert_eq!(f.outbox_count(), 0);
    f.store.db.commit_hook(None::<fn() -> bool>);
    assert!(
        f.store
            .content_save_receipt(f.session.operation(), &context())
            .unwrap()
            .is_none()
    );
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    f.pending();
    f.oracle();
}

fn deny_rollback(db: &rusqlite::Connection) {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
    db.authorizer(Some(|ctx: AuthContext<'_>| {
        if matches!(
            ctx.action,
            AuthAction::Transaction {
                operation: TransactionOperation::Rollback
            }
        ) {
            Authorization::Deny
        } else {
            Authorization::Allow
        }
    }));
}

#[test]
fn failed_rollback_quarantines_handle_and_releases_its_physical_writer() {
    for panic in [false, true] {
        let mut f = Fixture::new();
        deny_rollback(&f.store.db);
        fault::configure(Some(FaultConfig::new(
            "content.save.event_written",
            if panic {
                FaultAction::Panic
            } else {
                FaultAction::Fail
            },
        )));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f.store.publish_content_save(&f.prepared, &context())
        }));
        fault::configure(None);
        if panic {
            assert!(result.is_err());
        } else {
            error(result.unwrap(), "CONTENT_SAVE_ROLLBACK_FAILED");
        }
        assert!(f.store.content_save_connection_failed());
        error(
            f.store.publish_content_save(&f.prepared, &context()),
            "CONTENT_SAVE_CONNECTION_FAILED",
        );
        error(
            f.store
                .content_save_receipt(f.session.operation(), &context()),
            "CONTENT_SAVE_CONNECTION_FAILED",
        );
        error(
            f.store.begin_content_save(&f.entity, &context()),
            "CONTENT_SAVE_CONNECTION_FAILED",
        );
        // Exercise ordinary metadata too: the dead handle must not retain a
        // cached view or silently commit into an automatically reopened store.
        assert!(f.store.entry(&f.entity).is_err());
        let mut fresh = Store::open_existing(f.dir.path()).unwrap();
        fresh.db.busy_timeout(Duration::from_millis(100)).unwrap();
        fresh.db.execute_batch("BEGIN IMMEDIATE; ROLLBACK").unwrap();
        assert!(
            fresh
                .content_save_receipt(f.session.operation(), &context())
                .unwrap()
                .is_none()
        );
        let session = f
            .journal
            .resume(f.session.operation(), f.local.clone(), &context())
            .unwrap();
        let prepared = session
            .prepare(&f.local, &f.checkpoint, &context())
            .unwrap();
        fresh.publish_content_save(&prepared, &context()).unwrap();
        assert!(fresh.verify_derived_state().unwrap().is_empty());
        f.pending();
    }
}

#[test]
fn pool_discards_a_quarantined_connection_and_checks_out_a_fresh_store() {
    let f = Fixture::new();
    let pool = tkfs::runtime::ctx::SyncStorePool::new(f.dir.path(), &f.store.repo, &f.store.device);
    let mut store = pool.checkout().unwrap();
    deny_rollback(&store.db);
    fault::configure(Some(FaultConfig::new(
        "content.save.before_metadata",
        FaultAction::Fail,
    )));
    let result = store.publish_content_save(&f.prepared, &context());
    fault::configure(None);
    error(result, "CONTENT_SAVE_ROLLBACK_FAILED");
    assert!(store.content_save_connection_failed());
    drop(store);
    assert_eq!(pool.idle(), 0);
    let mut fresh = pool.checkout().unwrap();
    assert!(!fresh.content_save_connection_failed());
    fresh.publish_content_save(&f.prepared, &context()).unwrap();
    drop(fresh);
    assert_eq!(pool.idle(), 1);
    f.pending();
}

#[test]
fn missing_and_malformed_required_records_have_a_scoped_corruption_code() {
    use tkfs::model::errors::{ErrorClass, classify, code_of};
    for damaged in [
        "operation",
        "capture-type",
        "capture-size",
        "owner",
        "workspace",
        "epoch",
        "event",
    ] {
        let mut f = Fixture::new();
        if damaged == "event" {
            f.store
                .publish_content_save(&f.prepared, &context())
                .unwrap();
        }
        match damaged {
            "operation" => {
                f.store
                    .db
                    .execute("DELETE FROM content_save_operations", [])
                    .unwrap();
            }
            "capture-type" => {
                f.store
                    .db
                    .execute("UPDATE content_save_operations SET capture=23", [])
                    .unwrap();
            }
            "capture-size" => {
                f.store.db.execute_batch("PRAGMA ignore_check_constraints=ON; UPDATE content_save_operations SET capture=zeroblob(65537); PRAGMA ignore_check_constraints=OFF").unwrap();
            }
            "owner" => {
                f.store
                    .db
                    .execute("DELETE FROM content_save_entities", [])
                    .unwrap();
            }
            "workspace" => {
                f.store
                    .db
                    .execute("DELETE FROM config WHERE key='content_save_workspace'", [])
                    .unwrap();
            }
            "epoch" => {
                f.store
                    .db
                    .execute("UPDATE config SET value='bad' WHERE key='generation'", [])
                    .unwrap();
            }
            "event" => {
                f.store
                    .db
                    .execute(
                        "DELETE FROM events WHERE id=?",
                        [f.session.operation().to_string()],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let result = if damaged == "event" {
            f.store
                .content_save_receipt(f.session.operation(), &context())
                .map(|_| ())
        } else {
            f.store
                .publish_content_save(&f.prepared, &context())
                .map(|_| ())
        };
        let failure = result.unwrap_err();
        assert_eq!(
            code_of(&failure),
            "CORRUPT_CONTENT_SAVE",
            "{damaged}: {failure:#}"
        );
        assert_eq!(classify(&code_of(&failure)), ErrorClass::Fatal);
        assert!(!f.store.content_save_connection_failed());
        f.pending();
    }
}

#[test]
fn sqlite_authorization_failure_is_not_relabelled_as_corrupt_work() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    let f = Fixture::new();
    f.store.db.authorizer(Some(|ctx: AuthContext<'_>| {
        if matches!(
            ctx.action,
            AuthAction::Read {
                table_name: "content_save_operations",
                ..
            }
        ) {
            Authorization::Deny
        } else {
            Authorization::Allow
        }
    }));
    let error = f
        .store
        .content_save_receipt(f.session.operation(), &context())
        .unwrap_err();
    assert_ne!(tkfs::model::errors::code_of(&error), "CORRUPT_CONTENT_SAVE");
    assert!(error.downcast_ref::<rusqlite::Error>().is_some());
    f.store
        .db
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    assert!(
        f.store
            .content_save_receipt(f.session.operation(), &context())
            .unwrap()
            .is_none()
    );
    f.pending();
}

#[test]
fn private_branch_publication_preserves_private_admission_and_base_permissions() {
    let mut f = Fixture::new();
    let private = f.store.fork("private-save", false).unwrap();
    f.store.checkout(&private.id).unwrap();
    let before = f.store.entry(&f.entity).unwrap().basic_info();
    let intent = f.store.begin_content_save(&f.entity, &context()).unwrap();
    let session = f
        .journal
        .create(intent, f.local.clone(), &context())
        .unwrap();
    session
        .write_at(0, b"durable local bytes", &context())
        .unwrap();
    let checkpoint = session.checkpoint(&context()).unwrap().unwrap();
    let prepared = session.prepare(&f.local, &checkpoint, &context()).unwrap();
    assert_eq!(
        prepared.content(),
        f.prepared.content(),
        "deduplication must not grant disclosure"
    );
    f.store
        .select_content_checkpoint(&checkpoint, &context())
        .unwrap();
    let receipt = f.store.publish_content_save(&prepared, &context()).unwrap();
    let page = f.store.content_admission_page(0, 256).unwrap();
    let reference = tkfs::core::admission::reference_id(receipt.event(), &f.entity);
    let change = page.iter().find(|c| c.reference == reference).unwrap();
    assert!(!change.shared && !change.publishable);
    let after = f.store.entry(&f.entity).unwrap().basic_info();
    assert_eq!(before.posix_mode, after.posix_mode);
    assert_eq!(before.creation_time, after.creation_time);
    assert_eq!(
        f.store
            .content_save_receipt(session.operation(), &context())
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap().len(), 2);
    f.oracle();
}

#[test]
fn publication_refuses_outer_transactions_and_weakened_durability() {
    let mut f = Fixture::new();
    f.store.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    error(
        f.store.publish_content_save(&f.prepared, &context()),
        "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION",
    );
    f.store
        .db
        .execute_batch("ROLLBACK; PRAGMA synchronous=NORMAL")
        .unwrap();
    error(
        f.store.publish_content_save(&f.prepared, &context()),
        "CONTENT_SAVE_REQUIRES_FULL_WAL",
    );
    f.store
        .db
        .execute_batch("PRAGMA synchronous=FULL; DELETE FROM config WHERE key='fixture:content_v1'")
        .unwrap();
    error(
        f.store.publish_content_save(&f.prepared, &context()),
        "PEER_NEEDS_UPGRADE",
    );
    assert_eq!(f.event_count(), 0);
    f.pending();
}

#[test]
fn prepared_content_from_another_cas_cannot_be_published() {
    let mut f = Fixture::new();
    let other = tempfile::tempdir().unwrap();
    let other = LocalContent::open(other.path()).unwrap();
    let prepared = f
        .session
        .prepare(&other, &f.checkpoint, &context())
        .unwrap();
    error(
        f.store.publish_content_save(&prepared, &context()),
        "CONTENT_SAVE_WRONG_STORE",
    );
    assert_eq!(f.event_count(), 0);
    f.pending();
}

#[test]
fn writer_acquisition_obeys_deadline_and_cancel_and_restores_timeout() {
    let mut f = Fixture::new();
    let blocker = rusqlite::Connection::open(f.dir.path().join("metadata.sqlite")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let timeout: u64 = f
        .store
        .db
        .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
        .unwrap();
    for cancelled in [false, true] {
        let mut ctx = context();
        let worker = if cancelled {
            let cancel = ctx.cancel.clone();
            Some(std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(25));
                cancel.cancel();
            }))
        } else {
            ctx.deadline = Instant::now() + Duration::from_millis(25);
            None
        };
        let start = Instant::now();
        assert!(f.store.publish_content_save(&f.prepared, &ctx).is_err());
        let elapsed = start.elapsed();
        if let Some(worker) = worker {
            worker.join().unwrap();
        }
        assert!(elapsed < Duration::from_millis(250), "{elapsed:?}");
        println!(
            "SAVE_WRITER_WAIT cancelled={cancelled} elapsed_ms={:.4}",
            elapsed.as_secs_f64() * 1000.0
        );
        assert!(f.store.db.is_autocommit());
        assert_eq!(
            f.store
                .db
                .query_row("PRAGMA busy_timeout", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            timeout
        );
    }
    blocker.execute_batch("ROLLBACK").unwrap();
    assert_eq!(f.event_count(), 0);
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    f.oracle();
}

#[test]
fn sql_cancellation_rolls_back_event_outbox_and_receipt() {
    let mut f = Fixture::new();
    f.store.db.execute_batch("CREATE TEMP TRIGGER slow_receipt BEFORE UPDATE OF receipt ON content_save_operations WHEN NEW.receipt IS NOT NULL
        BEGIN SELECT sum(x) FROM (WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT x FROM n); END").unwrap();
    let mut ctx = context();
    ctx.deadline = Instant::now() + Duration::from_millis(60);
    let start = Instant::now();
    error(f.store.publish_content_save(&f.prepared, &ctx), "DEADLINE");
    assert!(start.elapsed() < Duration::from_millis(250));
    assert!(f.store.db.is_autocommit());
    assert_eq!(f.event_count(), 0);
    assert_eq!(f.outbox_count(), 0);
    f.store
        .db
        .execute_batch("DROP TRIGGER slow_receipt")
        .unwrap();
    f.store
        .publish_content_save(&f.prepared, &context())
        .unwrap();
    f.oracle();
    f.pending();
}

#[test]
fn damaged_receipt_or_outbox_is_not_accepted_as_recovery_authority() {
    for corruption in ["receipt", "outbox", "event", "capture-shape"] {
        let mut f = Fixture::new();
        f.store
            .publish_content_save(&f.prepared, &context())
            .unwrap();
        match corruption {
            "receipt" => {
                f.store
                    .db
                    .execute("UPDATE content_save_operations SET receipt=x'00'", [])
                    .unwrap();
            }
            "outbox" => {
                f.store
                    .db
                    .execute("DELETE FROM content_root_outbox", [])
                    .unwrap();
            }
            "event" => {
                f.store
                    .db
                    .execute(
                        "UPDATE events SET payload='{}' WHERE id=?",
                        [f.session.operation().to_string()],
                    )
                    .unwrap();
            }
            "capture-shape" => {
                #[derive(serde::Serialize)]
                struct Captured {
                    intent: tkfs::content::write::SaveIntent,
                    view_generation: u64,
                    event: tkfs::core::Event,
                }
                let raw: Vec<u8> = f
                    .store
                    .db
                    .query_row("SELECT capture FROM content_save_operations", [], |r| {
                        r.get(0)
                    })
                    .unwrap();
                let mut record: serde_json::Value = serde_json::from_slice(&raw).unwrap();
                record["value"]["event"]["changes"] = serde_json::json!([]);
                let captured = Captured {
                    intent: serde_json::from_value(record["value"]["intent"].clone()).unwrap(),
                    view_generation: record["value"]["view_generation"].as_u64().unwrap(),
                    event: serde_json::from_value(record["value"]["event"].clone()).unwrap(),
                };
                record["checksum"] = serde_json::json!(tkfs::core::hash(
                    &serde_json::to_vec(&("capture:v1", &captured)).unwrap()
                ));
                f.store
                    .db
                    .execute(
                        "UPDATE content_save_operations SET capture=?",
                        [serde_json::to_vec(&record).unwrap()],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            f.store
                .content_save_receipt(f.session.operation(), &context())
                .is_err(),
            "{corruption}"
        );
        f.pending();
    }
}

#[test]
fn process_exit_at_publication_boundaries_recovers_once() {
    for point in [
        "content.save.before_metadata",
        "content.save.event_written",
        "content.save.receipt_written",
        "content.save.committed",
    ] {
        let f = Fixture::new();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "child_process_publication",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_SAVE_CHILD_ROOT", f.dir.path())
            .env(
                "TKFS_SAVE_CHILD_OPERATION",
                f.session.operation().to_string(),
            )
            .env("TKFS_SAVE_CHILD_POINT", point)
            .env("TKFS_SAVE_CHILD_CONTINUE", "0")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "{point}");
        let mut recovered = Store::open_existing(f.dir.path()).unwrap();
        let committed = point == "content.save.committed";
        assert_eq!(
            recovered
                .content_save_receipt(f.session.operation(), &context())
                .unwrap()
                .is_some(),
            committed
        );
        // Revalidate the journal under a fresh owner after the actual process
        // exit. Canonical preparation is repeated, never inferred from a receipt.
        let resumed = f
            .journal
            .resume(f.session.operation(), f.local.clone(), &context())
            .unwrap();
        let prepared = resumed
            .prepare(&f.local, &f.checkpoint, &context())
            .unwrap();
        let receipt = recovered
            .publish_content_save(&prepared, &context())
            .unwrap();
        assert_eq!(receipt.content(), f.prepared.content());
        assert!(recovered.verify_derived_state().unwrap().is_empty());
        assert_eq!(f.event_count(), 1);
        assert_eq!(f.outbox_count(), 1);
        f.pending();
        // Keep the original connection's view refresh exercised as well.
        assert_eq!(
            f.store.entry(&f.entity).unwrap().content_ref.as_ref(),
            Some(receipt.content())
        );
    }
}

const CHILD_BUFFER: &[u8] = b"child-buffered";
#[test]
#[ignore = "subprocess helper invoked by process_exit_at_publication_boundaries_recovers_once"]
fn child_process_publication() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_SAVE_CHILD_ROOT").unwrap());
    let operation = Uuid::parse_str(&std::env::var("TKFS_SAVE_CHILD_OPERATION").unwrap()).unwrap();
    let mut store = Store::open_existing(&root).unwrap();
    if std::env::var("TKFS_SAVE_CHILD_CONTINUE").as_deref() == Ok("3") {
        let intent = serde_json::from_str::<tkfs::content::write::SaveIntent>(
            &std::env::var("TKFS_SAVE_CHILD_BASIC_INTENT").unwrap(),
        )
        .unwrap();
        let mutation =
            Uuid::parse_str(&std::env::var("TKFS_SAVE_CHILD_MUTATION").unwrap()).unwrap();
        let sequence = std::env::var("TKFS_SAVE_CHILD_SEQUENCE")
            .unwrap()
            .parse()
            .unwrap();
        let basic = owned_basic(&store, &intent.base.entity);
        fault::configure(Some(FaultConfig::new(
            &std::env::var("TKFS_SAVE_CHILD_POINT").unwrap(),
            FaultAction::Exit,
        )));
        store
            .apply_content_save_basic(&intent, mutation, basic, sequence, 6, &context())
            .unwrap();
        panic!("owned basic mutation did not hit requested process exit");
    }
    let local = Arc::new(LocalContent::open(&root).unwrap());
    let journal = WriteJournal::open(&root).unwrap();
    let session = journal
        .resume(operation, local.clone(), &context())
        .unwrap();
    let checkpoint = journal
        .pending(None, 256, &context())
        .unwrap()
        .into_iter()
        .find(|c| c.intent.operation == operation)
        .unwrap();
    let prepared = session.prepare(&local, &checkpoint, &context()).unwrap();
    let point = std::env::var("TKFS_SAVE_CHILD_POINT").unwrap();
    fault::configure(Some(FaultConfig::new(&point, FaultAction::Exit)));
    if std::env::var("TKFS_SAVE_CHILD_CONTINUE").as_deref() == Ok("2") {
        let parent = std::env::var("TKFS_SAVE_CHILD_PARENT")
            .ok()
            .map(|s| serde_json::from_str::<SaveCheckpoint>(&s).unwrap())
            .unwrap_or_else(|| checkpoint.clone());
        let handoff = store.continue_content_save(&parent, &context()).unwrap();
        if std::env::var("TKFS_SAVE_CHILD_BUFFERED").as_deref() == Ok("1") {
            session.write_at(75, CHILD_BUFFER, &context()).unwrap();
        }
        session
            .adopt_continuation(&handoff, local, &context())
            .unwrap();
    } else if std::env::var("TKFS_SAVE_CHILD_CONTINUE").as_deref() == Ok("1") {
        store
            .continue_content_save(&checkpoint, &context())
            .unwrap();
    } else {
        store.publish_content_save(&prepared, &context()).unwrap();
    }
    panic!("publication did not hit requested process exit");
}
