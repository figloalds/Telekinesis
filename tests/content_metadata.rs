//! Partial metadata commits use real native Saved clocks and FULL Core WAL.
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        LocalContent, ReadContext,
        write::{SaveCheckpoint, WriteJournal, WriteSession},
    },
    core::{BasicInfo, Store, content_saves::ContentBasicUpdate, id},
    util::{
        Cancel,
        fault::{self, FaultAction, FaultConfig},
    },
};
use uuid::Uuid;
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    }
}
fn error<T: std::fmt::Debug>(result: anyhow::Result<T>, expected: &str) {
    let actual = format!("{:#}", result.unwrap_err());
    assert!(actual.contains(expected), "{actual}");
}
struct Fixture {
    store: Store,
    local: Arc<LocalContent>,
    _journal: Arc<WriteJournal>,
    session: WriteSession,
    entity: String,
    saved: SaveCheckpoint,
    _dir: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(dir.path(), "checkpoint-metadata", &id()).unwrap();
        store
            .db
            .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
            .unwrap();
        let entry = store.create("file", "file").unwrap();
        let local = Arc::new(LocalContent::open(dir.path()).unwrap());
        let empty = local.import(&mut [].as_slice(), 0, &context()).unwrap();
        let change = store
            .current_change(&entry.id, "content", serde_json::json!(empty))
            .unwrap();
        let event = store.event(&store.branch(&store.active).unwrap(), vec![change]);
        store.commit(event, true).unwrap();
        let intent = store.begin_content_save(&entry.id, &context()).unwrap();
        let journal = WriteJournal::open(dir.path()).unwrap();
        let session = journal.create(intent, local.clone(), &context()).unwrap();
        let basic = store.entry(&entry.id).unwrap().basic_info();
        session
            .write_at_with_times(
                0,
                b"private",
                0,
                [basic.write_time, basic.change_time],
                &context(),
            )
            .unwrap();
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        store.select_content_checkpoint(&saved, &context()).unwrap();
        Self {
            store,
            local,
            _journal: journal,
            session,
            entity: entry.id,
            saved,
            _dir: dir,
        }
    }
    fn newer(&self, bytes: &[u8], prior: BasicInfo) -> SaveCheckpoint {
        self.session
            .write_at_with_times(
                0,
                bytes,
                0,
                [prior.write_time, prior.change_time],
                &context(),
            )
            .unwrap();
        self.session.checkpoint(&context()).unwrap().unwrap()
    }
    fn basic(&self) -> BasicInfo {
        self.store.entry(&self.entity).unwrap().basic_info()
    }
    fn publications(&self) -> u64 {
        self.store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                r.get(0)
            })
            .unwrap()
    }
    fn oracle(&mut self) {
        assert!(self.store.verify_derived_state().unwrap().is_empty());
    }
}

#[test]
fn partial_metadata_supersedes_an_unpublished_selection_without_publishing_bytes() {
    let mut f = Fixture::new();
    let old = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    let before = f.store.entry(&f.entity).unwrap();
    let saved = f.newer(b"newer", before.basic_info());
    let prepared = f.session.prepare(&f.local, &saved, &context()).unwrap();
    let fields = ContentBasicUpdate {
        write_time: Some(101),
        attributes: Some(0x80),
        posix_mode: Some(0o640),
        ..Default::default()
    };
    let mutation = Uuid::new_v4();
    let receipt = f
        .store
        .apply_content_checkpoint_basic(
            &saved.intent,
            mutation,
            Some(&saved),
            fields.clone(),
            &context(),
        )
        .unwrap();
    assert_eq!(f.publications(), 0);
    assert_eq!(
        f.store.entry(&f.entity).unwrap().content_ref,
        before.content_ref
    );
    let basic = f.basic();
    assert_eq!(basic.write_time, 101);
    assert_eq!(basic.change_time, saved.native_times().unwrap().change_time);
    assert_eq!(basic.attributes, 0x80);
    assert_eq!(basic.creation_time, before.basic_info().creation_time);
    error(
        f.store.publish_content_save(&old, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.basic(), basic);
    assert_eq!(f.publications(), 1);
    assert_eq!(
        f.store
            .apply_content_checkpoint_basic(
                &saved.intent,
                mutation,
                Some(&saved),
                fields,
                &context()
            )
            .unwrap(),
        receipt
    );
    error(
        f.store.apply_content_checkpoint_basic(
            &saved.intent,
            mutation,
            Some(&saved),
            ContentBasicUpdate {
                write_time: Some(102),
                ..Default::default()
            },
            &context(),
        ),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    f.oracle();
}

#[test]
fn partial_metadata_accumulates_same_sequence_fields_and_later_bytes_expire_overrides() {
    let mut f = Fixture::new();
    let saved = f.saved.clone();
    let first = ContentBasicUpdate {
        write_time: Some(101),
        creation_time: Some(707),
        access_time: Some(808),
        attributes: Some(0x80),
        ..Default::default()
    };
    let mutation = Uuid::new_v4();
    let receipt = f
        .store
        .apply_content_checkpoint_basic(
            &saved.intent,
            mutation,
            Some(&saved),
            first.clone(),
            &context(),
        )
        .unwrap();
    f.store
        .apply_content_checkpoint_basic(
            &saved.intent,
            Uuid::new_v4(),
            Some(&saved),
            ContentBasicUpdate {
                change_time: Some(202),
                posix_mode: Some(0o640),
                ..Default::default()
            },
            &context(),
        )
        .unwrap();
    f.store
        .apply_content_checkpoint_basic(
            &saved.intent,
            Uuid::new_v4(),
            Some(&saved),
            ContentBasicUpdate {
                attributes: Some(1),
                ..Default::default()
            },
            &context(),
        )
        .unwrap();
    let basic = f.basic();
    assert_eq!(
        [
            basic.write_time,
            basic.change_time,
            basic.creation_time,
            basic.access_time
        ],
        [101, 202, 707, 808]
    );
    assert_eq!(basic.posix_mode, Some(0o640));
    assert_eq!(basic.attributes, 1);
    assert_eq!(f.publications(), 0);
    let newer = f.newer(b"later", basic);
    f.store
        .select_content_checkpoint(&newer, &context())
        .unwrap();
    error(
        f.store.apply_content_checkpoint_basic(
            &saved.intent,
            Uuid::new_v4(),
            Some(&saved),
            ContentBasicUpdate::default(),
            &context(),
        ),
        "STALE_WRITE_CHECKPOINT",
    );
    let prepared = f.session.prepare(&f.local, &newer, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    let published = f.basic();
    assert_eq!(
        published.write_time,
        newer.native_times().unwrap().write_time
    );
    assert_eq!(
        published.change_time,
        newer.native_times().unwrap().change_time
    );
    assert_eq!([published.creation_time, published.access_time], [707, 808]);
    assert_eq!(published.posix_mode, Some(0o640));
    assert_eq!(published.attributes, 0x21);
    assert_eq!(
        f.store
            .apply_content_checkpoint_basic(
                &saved.intent,
                mutation,
                Some(&saved),
                first,
                &context()
            )
            .unwrap(),
        receipt
    );
    assert_eq!(f.publications(), 1);
    f.oracle();
}

#[test]
fn partial_metadata_lost_reply_replays_after_later_byte_publication() {
    let mut f = Fixture::new();
    let saved = f.saved.clone();
    let mutation = Uuid::new_v4();
    let fields = ContentBasicUpdate {
        write_time: Some(101),
        ..Default::default()
    };
    fault::configure(Some(FaultConfig::new(
        "content.save.basic_committed",
        FaultAction::Fail,
    )));
    let result = f.store.apply_content_checkpoint_basic(
        &saved.intent,
        mutation,
        Some(&saved),
        fields.clone(),
        &context(),
    );
    fault::configure(None);
    error(result, "FAULT_INJECTED");
    assert_eq!(f.basic().write_time, 101);
    assert_eq!(f.publications(), 0);
    let prepared = f.session.prepare(&f.local, &saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    let receipt = f
        .store
        .apply_content_checkpoint_basic(&saved.intent, mutation, Some(&saved), fields, &context())
        .unwrap();
    assert_eq!(receipt.event, mutation.to_string());
    assert_eq!(f.publications(), 1);
    assert_eq!(
        f.store
            .db
            .query_row(
                "SELECT count(*) FROM content_save_metadata_changes",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        1
    );
    assert_eq!(f.basic().write_time, 101);
    f.oracle();
}

#[test]
fn partial_metadata_rejects_committed_bytes_until_adoption_then_updates_the_clean_floor() {
    let mut f = Fixture::new();
    let saved = f.saved.clone();
    let prepared = f.session.prepare(&f.local, &saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    let fields = ContentBasicUpdate {
        write_time: Some(101),
        attributes: Some(0x80),
        ..Default::default()
    };
    let payload: String = f
        .store
        .db
        .query_row(
            "SELECT payload FROM events WHERE id=?",
            [saved.intent.operation.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    error(
        f.store.apply_content_checkpoint_basic(
            &saved.intent,
            Uuid::new_v4(),
            Some(&saved),
            fields.clone(),
            &context(),
        ),
        "CONTENT_METADATA_SAVE_IN_PROGRESS",
    );
    let handoff = f.store.continue_content_save(&saved, &context()).unwrap();
    let next = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    assert!(next.checkpoint(&context()).unwrap().is_none());
    let next_intent = next.intent(&context()).unwrap();
    f.store
        .apply_content_checkpoint_basic(&next_intent, Uuid::new_v4(), None, fields, &context())
        .unwrap();
    assert_eq!(f.basic().write_time, 101);
    assert_eq!(f.basic().attributes, 0x80);
    let after: String = f
        .store
        .db
        .query_row(
            "SELECT payload FROM events WHERE id=?",
            [saved.intent.operation.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(payload, after);
    assert_eq!(f.publications(), 1);
    assert!(
        f.store
            .db
            .query_row(
                "SELECT selected IS NULL FROM content_save_operations",
                [],
                |r| r.get::<_, bool>(0)
            )
            .unwrap()
    );
    f.oracle();
}

#[test]
fn partial_metadata_rejects_a_different_checkpoint_with_the_same_sequence() {
    let mut f = Fixture::new();
    let before = f.basic();
    let mut raw = serde_json::to_value(&f.saved).unwrap();
    raw["native_times"]["write_time"] = serde_json::json!(before.write_time + 1);
    let forged: SaveCheckpoint = serde_json::from_value(raw).unwrap();
    assert_ne!(forged, f.saved);
    let mutation = Uuid::new_v4();
    error(
        f.store.apply_content_checkpoint_basic(
            &forged.intent,
            mutation,
            Some(&forged),
            ContentBasicUpdate {
                write_time: Some(101),
                ..Default::default()
            },
            &context(),
        ),
        "STALE_WRITE_CHECKPOINT",
    );
    error(
        f.store.apply_content_checkpoint_basic(
            &f.saved.intent,
            mutation,
            None,
            ContentBasicUpdate::default(),
            &context(),
        ),
        "CONTENT_METADATA_SAVE_IN_PROGRESS",
    );
    assert_eq!(f.basic(), before);
    assert_eq!(f.publications(), 0);
    assert_eq!(
        f.store
            .db
            .query_row(
                "SELECT count(*) FROM content_save_metadata_changes",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        0
    );
    f.oracle();
}

#[test]
fn partial_metadata_detects_capture_or_selection_reversion_after_receipt_insertion() {
    for target in ["capture", "selected"] {
        let mut f = Fixture::new();
        let before = f.basic();
        let saved = f.newer(b"newer", before);
        let old: (Vec<u8>, Vec<u8>) = f
            .store
            .db
            .query_row(
                "SELECT capture,selected FROM content_save_operations",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let value = if target == "capture" { &old.0 } else { &old.1 };
        f.store.db.execute_batch(&format!("CREATE TRIGGER revert_metadata AFTER INSERT ON content_save_metadata_changes BEGIN UPDATE content_save_operations SET {target}=X'{}'; END;",hex::encode(value))).unwrap();
        let mutation = Uuid::new_v4();
        error(
            f.store.apply_content_checkpoint_basic(
                &saved.intent,
                mutation,
                Some(&saved),
                ContentBasicUpdate {
                    write_time: Some(101),
                    ..Default::default()
                },
                &context(),
            ),
            "CONTENT_METADATA_CONTINUATION_LOST",
        );
        let after: (Vec<u8>, Vec<u8>) = f
            .store
            .db
            .query_row(
                "SELECT capture,selected FROM content_save_operations",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(after, old);
        assert_eq!(f.basic(), before);
        assert_eq!(
            f.store
                .db
                .query_row(
                    "SELECT count(*) FROM events WHERE id=?",
                    [mutation.to_string()],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            0
        );
        assert_eq!(
            f.store
                .db
                .query_row(
                    "SELECT count(*) FROM content_save_metadata_changes",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            0
        );
        assert_eq!(f.publications(), 0);
        f.oracle();
    }
}

#[test]
fn partial_metadata_before_any_byte_revision_keeps_the_operation_owned() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(dir.path(), "empty-owned-metadata", &id()).unwrap();
    store
        .db
        .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
        .unwrap();
    let entity = store.create("file", "file").unwrap().id;
    let local = Arc::new(LocalContent::open(dir.path()).unwrap());
    let journal = WriteJournal::open(dir.path()).unwrap();
    let intent = store.begin_content_save(&entity, &context()).unwrap();
    let session = journal
        .create(intent.clone(), local.clone(), &context())
        .unwrap();
    store
        .apply_content_checkpoint_basic(
            &intent,
            Uuid::new_v4(),
            None,
            ContentBasicUpdate {
                write_time: Some(101),
                creation_time: Some(707),
                ..Default::default()
            },
            &context(),
        )
        .unwrap();
    assert!(journal.pending(None, 64, &context()).unwrap().is_empty());
    let basic = store.entry(&entity).unwrap().basic_info();
    session
        .write_at_with_times(
            0,
            b"later",
            6,
            [basic.write_time, basic.change_time],
            &context(),
        )
        .unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    store.select_content_checkpoint(&saved, &context()).unwrap();
    let prepared = session.prepare(&local, &saved, &context()).unwrap();
    store.publish_content_save(&prepared, &context()).unwrap();
    let published = store.entry(&entity).unwrap().basic_info();
    assert_eq!(published.write_time, 101);
    assert_eq!(published.creation_time, 707);
    assert!(store.verify_derived_state().unwrap().is_empty());
}

#[test]
fn recovered_basic_observation_uses_the_byte_revision_and_clean_floor_not_owner_fences() {
    let mut f = Fixture::new();
    let saved = f.saved.clone();
    let mutation = Uuid::new_v4();
    let fields = ContentBasicUpdate {
        write_time: Some(101),
        change_time: Some(202),
        attributes: Some(0x80),
        ..Default::default()
    };
    assert!(
        f.store
            .content_checkpoint_metadata_receipt(
                &saved.intent,
                mutation,
                Some(&saved),
                &fields,
                &context()
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(f.publications(), 0);
    let receipt = f
        .store
        .apply_content_checkpoint_basic(
            &saved.intent,
            mutation,
            Some(&saved),
            fields.clone(),
            &context(),
        )
        .unwrap();
    assert_eq!(
        f.store
            .content_checkpoint_metadata_receipt(
                &saved.intent,
                mutation,
                Some(&saved),
                &fields,
                &context()
            )
            .unwrap(),
        Some(receipt)
    );
    let recovered = f
        ._journal
        .resume(saved.intent.operation, f.local.clone(), &context())
        .unwrap();
    let state = recovered.native_basic_state();
    assert_eq!(
        state.sequence, saved.sequence,
        "owner recovery is not another byte mutation"
    );
    assert!(state.has_changes);
    let visible = f
        .store
        .content_save_basic_info(saved.intent.operation, state, &context())
        .unwrap();
    assert_eq!(
        [
            visible.write_time,
            visible.change_time,
            visible.attributes as u64
        ],
        [101, 202, 0x80]
    );
    assert_eq!(f.publications(), 0);
    let prepared = recovered.prepare(&f.local, &saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    let handoff = f.store.continue_content_save(&saved, &context()).unwrap();
    f.store
        .apply_content_checkpoint_basic(
            handoff.intent(),
            Uuid::new_v4(),
            None,
            ContentBasicUpdate {
                write_time: Some(250),
                attributes: Some(1),
                ..Default::default()
            },
            &context(),
        )
        .unwrap();
    let visible = f
        .store
        .content_save_basic_info(saved.intent.operation, state, &context())
        .unwrap();
    assert_eq!(
        visible.write_time, 250,
        "a published byte revision cannot overwrite metadata while private adoption is pending"
    );
    assert_eq!(visible.attributes, 1);
    let next = recovered
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    let state = next.native_basic_state();
    assert!(!state.has_changes);
    assert!(next.checkpoint(&context()).unwrap().is_none());
    assert_eq!(
        f.store
            .content_save_basic_info(saved.intent.operation, state, &context())
            .unwrap(),
        f.basic()
    );
    let intent = next.intent(&context()).unwrap();
    f.store
        .apply_content_checkpoint_basic(
            &intent,
            Uuid::new_v4(),
            None,
            ContentBasicUpdate {
                write_time: Some(303),
                attributes: Some(1),
                ..Default::default()
            },
            &context(),
        )
        .unwrap();
    let visible = f
        .store
        .content_save_basic_info(intent.operation, state, &context())
        .unwrap();
    assert_eq!(
        visible.write_time, 303,
        "a clean baseline cannot restore its old Saved clock over newer metadata"
    );
    assert_eq!(visible.attributes, 1);
    f.oracle();
}
