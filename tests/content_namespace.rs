//! Owned namespace changes preserve exact private save identities and FULL WAL.
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        LocalContent, ReadContext,
        write::{SaveCheckpoint, WriteJournal, WriteSession},
    },
    core::{
        BasicInfo, Store,
        content_saves::{ContentBasicUpdate, NamespaceDestination},
        id,
    },
    util::{
        Cancel,
        fault::{self, FaultAction, FaultConfig},
    },
};
use uuid::Uuid;
#[path = "content_namespace/create.rs"]
mod create;
#[path = "content_namespace/deletion.rs"]
mod deletion;
#[path = "content_namespace/directory.rs"]
mod directory;
#[path = "content_namespace/directory_changes.rs"]
mod directory_changes;
#[path = "content_namespace/retirement.rs"]
mod retirement;
#[path = "content_namespace/unlink.rs"]
mod unlink;
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    }
}

#[test]
fn owned_rename_keeps_private_bytes_basic_policy_and_the_open_operation_after_publication() {
    let mut f = Fixture::new();
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    let before = f.store.entry(&f.entity).unwrap().content_ref;
    f.store
        .apply_content_checkpoint_basic(
            &f.saved.intent,
            Uuid::new_v4(),
            Some(&f.saved),
            ContentBasicUpdate {
                write_time: Some(101),
                attributes: Some(0x80),
                ..Default::default()
            },
            &context(),
        )
        .unwrap();
    let basic = f.basic();
    let mutation = Uuid::new_v4();
    let receipt = f
        .store
        .apply_content_checkpoint_rename(
            &f.saved.intent,
            mutation,
            Some(&f.saved),
            "moved",
            false,
            &context(),
        )
        .unwrap();
    assert_eq!(receipt.operation, f.saved.intent.operation);
    assert_eq!(f.store.lookup("moved").unwrap().id, f.entity);
    assert!(f.store.lookup("file").is_err());
    assert_eq!(f.store.entry(&f.entity).unwrap().content_ref, before);
    assert_eq!(
        f.session.checkpoint(&context()).unwrap().as_ref(),
        Some(&f.saved)
    );
    assert_eq!(f.basic(), basic);
    assert_eq!(f.publications(), 0);
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.basic(), basic);
    let handoff = f.store.continue_content_save(&f.saved, &context()).unwrap();
    let next = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    next.write_at(0, b"further", &context()).unwrap();
    let saved = next.checkpoint(&context()).unwrap().unwrap();
    assert_eq!(saved.intent.operation, f.saved.intent.operation);
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = next.prepare(&f.local, &saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.store.lookup("moved").unwrap().id, f.entity);
    assert_eq!(f.publications(), 2);
    let mut bytes = [0; 7];
    next.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(&bytes, b"further");
    f.oracle();
}

#[test]
fn owned_rename_rebinds_newer_saved_and_fences_an_older_preparation() {
    let mut f = Fixture::new();
    let old = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    let newer = f.newer(b"changed", f.basic());
    f.store
        .apply_content_checkpoint_rename(
            &newer.intent,
            Uuid::new_v4(),
            Some(&newer),
            "moved",
            false,
            &context(),
        )
        .unwrap();
    error(
        f.store.publish_content_save(&old, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    let prepared = f.session.prepare(&f.local, &newer, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.publications(), 1);
    assert_eq!(f.store.lookup("moved").unwrap().id, f.entity);
    f.oracle();
}

#[test]
fn owned_rename_rejects_a_changed_same_sequence_checkpoint_and_the_reserved_byte_event_id() {
    let mut f = Fixture::new();
    let mut changed = f.saved.clone();
    changed.length += 1;
    error(
        f.store.apply_content_checkpoint_rename(
            &changed.intent,
            Uuid::new_v4(),
            Some(&changed),
            "moved",
            false,
            &context(),
        ),
        "STALE_WRITE_CHECKPOINT",
    );
    error(
        f.store.apply_content_checkpoint_rename(
            &f.saved.intent,
            f.saved.intent.operation,
            Some(&f.saved),
            "moved",
            false,
            &context(),
        ),
        "CONTENT_METADATA_OWNER_MISMATCH",
    );
    assert_eq!(f.store.lookup("file").unwrap().id, f.entity);
    assert!(f.store.lookup("moved").is_err());
    assert_eq!(
        f.store
            .db
            .query_row(
                "SELECT count(*) FROM content_save_namespace_changes",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        0
    );
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.publications(), 1);
    f.oracle();
}

#[test]
fn replaced_destination_saved_bytes_can_be_committed_without_resurrecting_its_name() {
    let mut f = Fixture::new();
    let target = f.store.create("target", "file").unwrap();
    let intent = f.store.begin_content_save(&target.id, &context()).unwrap();
    let other = f
        ._journal
        .create(intent, f.local.clone(), &context())
        .unwrap();
    other.write_at(0, b"kept", &context()).unwrap();
    let saved = other.checkpoint(&context()).unwrap().unwrap();
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = other.prepare(&f.local, &saved, &context()).unwrap();
    f.store
        .apply_content_checkpoint_replace(
            &f.saved.intent,
            Uuid::new_v4(),
            Some(&f.saved),
            "target",
            NamespaceDestination {
                intent: saved.intent.clone(),
                checkpoint: Some(saved.clone()),
            },
            &context(),
        )
        .unwrap();
    error(
        f.store.publish_content_save(&prepared, &context()),
        "STALE_WRITE_CHECKPOINT",
    );
    let handoff = f
        .store
        .content_namespace_continuation(&saved.intent, &context())
        .unwrap()
        .unwrap();
    let retired = other
        .adopt_namespace_continuation(&handoff, &context())
        .unwrap();
    error(other.write_at(0, b"bad", &context()), "STALE_WRITE_SESSION");
    let mut bytes = [0; 4];
    retired.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(&bytes, b"kept");
    let retired_saved = retired.checkpoint(&context()).unwrap().unwrap();
    assert_eq!(retired_saved.sequence, saved.sequence);
    assert_eq!(retired_saved.patch_map, saved.patch_map);
    assert_eq!(retired_saved.native_times(), saved.native_times());
    let prepared = retired
        .prepare(&f.local, &retired_saved, &context())
        .unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.store.lookup("target").unwrap().id, f.entity);
    assert!(!f.store.entry(&target.id).unwrap().alive);
    let next = f
        .store
        .continue_content_save(&retired_saved, &context())
        .unwrap();
    let retired = retired
        .adopt_continuation(&next, f.local.clone(), &context())
        .unwrap();
    retired.write_at(0, b"more", &context()).unwrap();
    let saved2 = retired.checkpoint(&context()).unwrap().unwrap();
    f.store
        .select_content_checkpoint(&saved2, &context())
        .unwrap();
    let prepared = retired.prepare(&f.local, &saved2, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.store.lookup("target").unwrap().id, f.entity);
    assert!(!f.store.entry(&target.id).unwrap().alive);
    assert_eq!(f.publications(), 2);
    let source = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    f.store.publish_content_save(&source, &context()).unwrap();
    assert_eq!(f.publications(), 3);
    f.oracle();
}

#[test]
fn owned_replacement_preserves_the_source_owner_and_fences_the_replaced_writer() {
    let mut f = Fixture::new();
    let target = f.store.create("target", "file").unwrap();
    let intent = f.store.begin_content_save(&target.id, &context()).unwrap();
    let other = f
        ._journal
        .create(intent, f.local.clone(), &context())
        .unwrap();
    other.write_at(0, b"kept", &context()).unwrap();
    let saved = other.checkpoint(&context()).unwrap().unwrap();
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let old_target = other.prepare(&f.local, &saved, &context()).unwrap();
    f.store
        .apply_content_checkpoint_rename(
            &f.saved.intent,
            Uuid::new_v4(),
            Some(&f.saved),
            "target",
            true,
            &context(),
        )
        .unwrap();
    assert_eq!(f.store.lookup("target").unwrap().id, f.entity);
    assert!(!f.store.entry(&target.id).unwrap().alive);
    error(
        f.store.publish_content_save(&old_target, &context()),
        "STALE_CONTENT_SAVE",
    );
    let mut private = [0; 4];
    other.read_range(0, &mut private, &context()).unwrap();
    assert_eq!(&private, b"kept");
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.publications(), 1);
    f.oracle();
}

#[test]
fn owned_rename_lost_reply_replays_after_later_rename_without_moving_the_file_back() {
    let mut f = Fixture::new();
    let mutation = Uuid::new_v4();
    let mut config = FaultConfig::new("content.save.rename_committed", FaultAction::Fail);
    config.count = Some(1);
    fault::configure(Some(config));
    let result = f.store.apply_content_checkpoint_rename(
        &f.saved.intent,
        mutation,
        Some(&f.saved),
        "first",
        false,
        &context(),
    );
    let fired = fault::fired();
    fault::configure(None);
    assert_eq!(fired, 1);
    error(result, "FAULT_INJECTED");
    let receipt = f
        .store
        .content_checkpoint_rename_receipt(
            &f.saved.intent,
            mutation,
            Some(&f.saved),
            "first",
            false,
            &context(),
        )
        .unwrap()
        .unwrap();
    f.store
        .apply_content_checkpoint_rename(
            &f.saved.intent,
            Uuid::new_v4(),
            Some(&f.saved),
            "second",
            false,
            &context(),
        )
        .unwrap();
    assert_eq!(
        f.store
            .apply_content_checkpoint_rename(
                &f.saved.intent,
                mutation,
                Some(&f.saved),
                "first",
                false,
                &context()
            )
            .unwrap(),
        receipt
    );
    assert_eq!(f.store.lookup("second").unwrap().id, f.entity);
    assert!(f.store.lookup("first").is_err());
    error(
        f.store.apply_content_checkpoint_rename(
            &f.saved.intent,
            mutation,
            Some(&f.saved),
            "other",
            false,
            &context(),
        ),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.publications(), 1);
    f.oracle();
}

#[test]
fn owned_rename_detects_silent_capture_selection_and_receipt_reversion_and_rolls_back_the_name() {
    for reverted in ["receipt", "capture", "selected"] {
        let mut f = Fixture::new();
        let saved = if reverted == "selected" {
            f.newer(b"changed", f.basic())
        } else {
            f.saved.clone()
        };
        let trigger = if reverted != "receipt" {
            let old: Vec<u8> = f
                .store
                .db
                .query_row(
                    &format!("SELECT {reverted} FROM content_save_operations WHERE operation=?"),
                    [f.saved.intent.operation.to_string()],
                    |r| r.get(0),
                )
                .unwrap();
            let hex: String = old.iter().map(|b| format!("{b:02x}")).collect();
            format!(
                "CREATE TRIGGER revert_namespace AFTER INSERT ON content_save_namespace_changes BEGIN UPDATE content_save_operations SET {reverted}=X'{hex}' WHERE operation='{}'; END;",
                f.saved.intent.operation
            )
        } else {
            "CREATE TRIGGER revert_namespace AFTER INSERT ON content_save_namespace_changes BEGIN DELETE FROM content_save_namespace_changes WHERE mutation=NEW.mutation; END;".to_owned()
        };
        f.store.db.execute_batch(&trigger).unwrap();
        error(
            f.store.apply_content_checkpoint_rename(
                &saved.intent,
                Uuid::new_v4(),
                Some(&saved),
                "moved",
                false,
                &context(),
            ),
            if reverted != "receipt" {
                "CONTENT_METADATA_CONTINUATION_LOST"
            } else {
                "CONTENT_METADATA_RECEIPT_LOST"
            },
        );
        assert_eq!(f.store.lookup("file").unwrap().id, f.entity);
        assert!(f.store.lookup("moved").is_err());
        assert_eq!(f.publications(), 0);
        f.store
            .select_content_checkpoint(&saved, &context())
            .unwrap();
        let prepared = f.session.prepare(&f.local, &saved, &context()).unwrap();
        f.store.publish_content_save(&prepared, &context()).unwrap();
        assert_eq!(f.publications(), 1);
        f.oracle();
    }
}

#[test]
fn owned_rename_requires_committed_save_adoption_then_accepts_the_clean_operation() {
    let mut f = Fixture::new();
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    error(
        f.store.apply_content_checkpoint_rename(
            &f.saved.intent,
            Uuid::new_v4(),
            Some(&f.saved),
            "moved",
            false,
            &context(),
        ),
        "CONTENT_METADATA_SAVE_IN_PROGRESS",
    );
    let handoff = f.store.continue_content_save(&f.saved, &context()).unwrap();
    let next = f
        .session
        .adopt_continuation(&handoff, f.local.clone(), &context())
        .unwrap();
    let intent = next.intent(&context()).unwrap();
    assert!(next.checkpoint(&context()).unwrap().is_none());
    f.store
        .apply_content_checkpoint_rename(&intent, Uuid::new_v4(), None, "moved", false, &context())
        .unwrap();
    assert_eq!(f.store.lookup("moved").unwrap().id, f.entity);
    next.write_at(0, b"further", &context()).unwrap();
    let saved = next.checkpoint(&context()).unwrap().unwrap();
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = next.prepare(&f.local, &saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.publications(), 2);
    f.oracle();
}

#[test]
fn owned_rename_replay_keeps_the_resolved_parent_identity_after_that_directory_moves() {
    let mut f = Fixture::new();
    let directory = f.store.create("parent", "directory").unwrap();
    let mutation = Uuid::new_v4();
    let receipt = f
        .store
        .apply_content_checkpoint_rename(
            &f.saved.intent,
            mutation,
            Some(&f.saved),
            "parent/moved",
            false,
            &context(),
        )
        .unwrap();
    f.store.rename(&directory.id, "elsewhere", false).unwrap();
    assert_eq!(
        f.store
            .apply_content_checkpoint_rename(
                &f.saved.intent,
                mutation,
                Some(&f.saved),
                "parent/moved",
                false,
                &context()
            )
            .unwrap(),
        receipt
    );
    assert_eq!(f.store.lookup("elsewhere/moved").unwrap().id, f.entity);
    assert!(f.store.lookup("parent").is_err());
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.store.lookup("elsewhere/moved").unwrap().id, f.entity);
    f.oracle();
}

#[test]
fn owned_rename_and_byte_publication_keep_remote_location_and_content_alternatives() {
    let mut f = Fixture::new();
    let original = f.store.entry(&f.entity).unwrap();
    let branch = f.store.branch(&f.store.active).unwrap();
    let location = |name: &str| tkfs::core::Change {
        entity: f.entity.clone(),
        field: "location".into(),
        parents: original
            .revisions
            .get("location")
            .cloned()
            .into_iter()
            .collect(),
        value: serde_json::json!({"parent":tkfs::core::ROOT,"name":name}),
    };
    let remote_root = tkfs::model::content::FileContentRef::new(
        tkfs::model::content::ContentKind::Direct,
        tkfs::model::content::Digest::bytes(b"absent remote bytes"),
        19,
    )
    .unwrap();
    let mut first = f.store.event(
        &branch,
        vec![
            location("remote-a"),
            tkfs::core::Change {
                entity: f.entity.clone(),
                field: "content".into(),
                parents: f.saved.intent.content_parent.clone().into_iter().collect(),
                value: serde_json::json!(remote_root),
            },
        ],
    );
    let mut second = f.store.event(&branch, vec![location("remote-b")]);
    first.device = "remote-a".into();
    second.device = "remote-b".into();
    let remote_content = first.id.clone();
    f.store.commit(first, false).unwrap();
    f.store.commit(second, false).unwrap();
    f.store
        .apply_content_checkpoint_rename(
            &f.saved.intent,
            Uuid::new_v4(),
            Some(&f.saved),
            "local",
            false,
            &context(),
        )
        .unwrap();
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    let receipt = f.store.publish_content_save(&prepared, &context()).unwrap();
    let event = f
        .store
        .events()
        .unwrap()
        .into_iter()
        .find(|event| event.id == receipt.event())
        .unwrap();
    assert!(!event.changes[0].parents.contains(&remote_content));
    let conflicts = f.store.conflicts().unwrap();
    assert!(
        conflicts
            .iter()
            .any(|c| c.entity == f.entity && c.field == "content" && c.resolved_by.is_none())
    );
    assert!(
        conflicts
            .iter()
            .any(|c| c.entity == f.entity && c.field == "location" && c.resolved_by.is_none())
    );
    assert_eq!(f.publications(), 1);
    f.oracle();
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

#[test]
#[ignore = "actual process-exit helper for owned_rename_process_exit_recovers_the_name_and_private_bytes_once"]
fn owned_rename_crash_child() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_NAMESPACE_CHILD_ROOT").unwrap());
    let operation =
        Uuid::parse_str(&std::env::var("TKFS_NAMESPACE_CHILD_OPERATION").unwrap()).unwrap();
    let mutation =
        Uuid::parse_str(&std::env::var("TKFS_NAMESPACE_CHILD_MUTATION").unwrap()).unwrap();
    let mut store = Store::open_existing(&root).unwrap();
    let local = Arc::new(LocalContent::open(&root).unwrap());
    let journal = WriteJournal::open(&root).unwrap();
    let session = journal.resume(operation, local, &context()).unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    fault::configure(Some(FaultConfig::new(
        &std::env::var("TKFS_NAMESPACE_CHILD_POINT").unwrap(),
        FaultAction::Exit,
    )));
    store
        .apply_content_checkpoint_rename(
            &saved.intent,
            mutation,
            Some(&saved),
            "moved",
            false,
            &context(),
        )
        .unwrap();
    panic!("rename did not reach the requested crash boundary");
}

#[test]
fn owned_rename_process_exit_recovers_the_name_and_private_bytes_once() {
    for point in [
        "content.save.before_rename",
        "content.save.rename_written",
        "content.save.rename_committed",
    ] {
        let f = Fixture::new();
        let Fixture {
            store,
            local,
            _journal,
            session,
            entity,
            saved,
            _dir,
        } = f;
        drop(store);
        drop(local);
        drop(session);
        drop(_journal);
        let mutation = Uuid::new_v4();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "owned_rename_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_NAMESPACE_CHILD_ROOT", _dir.path())
            .env(
                "TKFS_NAMESPACE_CHILD_OPERATION",
                saved.intent.operation.to_string(),
            )
            .env("TKFS_NAMESPACE_CHILD_MUTATION", mutation.to_string())
            .env("TKFS_NAMESPACE_CHILD_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let store = Store::open_existing(_dir.path()).unwrap();
        let committed = point == "content.save.rename_committed";
        let path = if committed { "moved" } else { "file" };
        assert_eq!(store.lookup(path).unwrap().id, entity);
        assert!(
            store
                .lookup(if committed { "file" } else { "moved" })
                .is_err()
        );
        assert_eq!(
            store
                .content_checkpoint_rename_receipt(
                    &saved.intent,
                    mutation,
                    Some(&saved),
                    "moved",
                    false,
                    &context()
                )
                .unwrap()
                .is_some(),
            committed
        );
        let shared = Arc::new(std::sync::Mutex::new(tkfs::runtime::Engine::new(store)));
        let writes =
            tkfs::runtime::content_writes::NativeWrites::install(&shared, &context()).unwrap();
        let handle = tkfs::runtime::lock_engine(&shared)
            .open(path, None, false)
            .unwrap()
            .0;
        let bytes = tkfs::runtime::content_reads::read_mount_vec(
            &shared,
            handle,
            0,
            7,
            tkfs::runtime::content::ContentWait::fixed(Duration::from_secs(30)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(&bytes, b"private");
        assert_eq!(
            tkfs::runtime::lock_engine(&shared)
                .store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        tkfs::runtime::engine::close_handle(&shared, handle).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        assert_eq!(e.store.lookup(path).unwrap().id, entity);
        assert_eq!(
            e.store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            1
        );
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        drop(e);
        writes.shutdown(&context()).unwrap();
    }
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(dir.path(), "checkpoint-namespace", &id()).unwrap();
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
fn observed_parent_is_revalidated_in_the_transaction_and_bound_to_the_exact_receipt() {
    use tkfs::core::content_saves::NamespaceTarget;
    let mut f = Fixture::new();
    let old = f.store.create("dir", "directory").unwrap().id;
    let new = f.store.create("other", "directory").unwrap().id;
    let observed = NamespaceTarget {
        path: "dir/moved".into(),
        parent: old,
    };
    f.store.rename(&observed.parent, "away", false).unwrap();
    f.store.rename(&new, "dir", false).unwrap();
    let mutation = Uuid::new_v4();
    error(
        f.store.apply_content_checkpoint_rename_at(
            &f.saved.intent,
            mutation,
            Some(&f.saved),
            &observed,
            None,
            &context(),
        ),
        "STALE_CONTENT_DESTINATION",
    );
    assert_eq!(f.store.lookup("file").unwrap().id, f.entity);
    assert!(f.store.lookup("dir/moved").is_err());
    let corrected = NamespaceTarget {
        path: observed.path.clone(),
        parent: new,
    };
    let receipt = f
        .store
        .apply_content_checkpoint_rename_at(
            &f.saved.intent,
            mutation,
            Some(&f.saved),
            &corrected,
            None,
            &context(),
        )
        .unwrap();
    f.store.rename(&corrected.parent, "gone", false).unwrap();
    assert!(f.store.lookup("dir").is_err());
    assert_eq!(
        f.store
            .content_checkpoint_rename_at_receipt(
                &f.saved.intent,
                mutation,
                Some(&f.saved),
                &corrected,
                None,
                &context()
            )
            .unwrap(),
        Some(receipt)
    );
    error(
        f.store.content_checkpoint_rename_at_receipt(
            &f.saved.intent,
            mutation,
            Some(&f.saved),
            &observed,
            None,
            &context(),
        ),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.store.lookup("gone/moved").unwrap().id, f.entity);
    f.oracle();
}
