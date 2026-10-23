use super::*;

struct Replacement {
    f: Fixture,
    target: String,
    other: WriteSession,
    saved: SaveCheckpoint,
    mutation: Uuid,
}
impl Replacement {
    fn new(after_publication: bool) -> Self {
        let mut f = Fixture::new();
        let target = f.store.create("target", "file").unwrap().id;
        let intent = f.store.begin_content_save(&target, &context()).unwrap();
        let mut other = f
            ._journal
            .create(intent, f.local.clone(), &context())
            .unwrap();
        other.write_at(0, b"kept", &context()).unwrap();
        if after_publication {
            let saved = other.checkpoint(&context()).unwrap().unwrap();
            f.store
                .select_content_checkpoint(&saved, &context())
                .unwrap();
            let prepared = other.prepare(&f.local, &saved, &context()).unwrap();
            f.store.publish_content_save(&prepared, &context()).unwrap();
            let continuation = f.store.continue_content_save(&saved, &context()).unwrap();
            other = other
                .adopt_continuation(&continuation, f.local.clone(), &context())
                .unwrap();
            other.write_at(0, b"kept", &context()).unwrap();
        }
        let saved = other.checkpoint(&context()).unwrap().unwrap();
        Self {
            f,
            target,
            other,
            saved,
            mutation: Uuid::new_v4(),
        }
    }
    fn destination(&self) -> NamespaceDestination {
        NamespaceDestination {
            intent: self.saved.intent.clone(),
            checkpoint: Some(self.saved.clone()),
        }
    }
    fn replace(&mut self) -> anyhow::Result<tkfs::core::content_saves::ContentMetadataReceipt> {
        let destination = self.destination();
        self.f.store.apply_content_checkpoint_replace(
            &self.f.saved.intent,
            self.mutation,
            Some(&self.f.saved),
            "target",
            destination,
            &context(),
        )
    }
    fn continuation(&self) -> tkfs::core::content_saves::NamespaceContinuation {
        self.f
            .store
            .content_namespace_continuation(&self.saved.intent, &context())
            .unwrap()
            .unwrap()
    }
    fn invariant(&mut self) {
        assert_eq!(self.f.store.lookup("target").unwrap().id, self.f.entity);
        assert!(!self.f.store.entry(&self.target).unwrap().alive);
        self.f.oracle();
    }
}

#[test]
fn retirement_after_a_published_generation_preserves_old_receipt_replay_and_later_saves() {
    let mut r = Replacement::new(true);
    let old = r.f.store.db.query_row("SELECT selected FROM content_save_history WHERE operation=? ORDER BY generation LIMIT 1",[r.saved.intent.operation.to_string()],|row|row.get::<_,Vec<u8>>(0)).unwrap();
    let old: serde_json::Value = serde_json::from_slice(&old).unwrap();
    let old: SaveCheckpoint = serde_json::from_value(old["value"].clone()).unwrap();
    r.replace().unwrap();
    let replay = r.f.store.continue_content_save(&old, &context()).unwrap();
    assert_eq!(replay.intent(), &r.saved.intent);
    let retired = r
        .other
        .adopt_namespace_continuation(&r.continuation(), &context())
        .unwrap();
    let saved = retired.checkpoint(&context()).unwrap().unwrap();
    // Rebind the first retired generation through the owned metadata path.
    r.f.store
        .apply_content_checkpoint_basic(
            &saved.intent,
            Uuid::new_v4(),
            Some(&saved),
            ContentBasicUpdate {
                write_time: Some(777),
                ..Default::default()
            },
            &context(),
        )
        .unwrap();
    r.f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = retired.prepare(&r.f.local, &saved, &context()).unwrap();
    r.f.store
        .publish_content_save(&prepared, &context())
        .unwrap();
    assert_eq!(
        r.f.store.entry(&r.target).unwrap().basic_info().write_time,
        777
    );
    assert_eq!(
        r.f.store
            .continue_content_save(&old, &context())
            .unwrap()
            .intent(),
        &r.saved.intent
    );
    let next = r.f.store.continue_content_save(&saved, &context()).unwrap();
    let next = retired
        .adopt_continuation(&next, r.f.local.clone(), &context())
        .unwrap();
    next.write_at(0, b"tail", &context()).unwrap();
    let saved = next.checkpoint(&context()).unwrap().unwrap();
    r.f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = next.prepare(&r.f.local, &saved, &context()).unwrap();
    r.f.store
        .publish_content_save(&prepared, &context())
        .unwrap();
    r.invariant();
}

#[test]
fn retirement_lost_namespace_and_private_replies_are_exact_and_never_reapply_a_move() {
    let mut r = Replacement::new(false);
    let mut config = FaultConfig::new("content.save.rename_committed", FaultAction::Fail);
    config.count = Some(1);
    fault::configure(Some(config));
    let result = r.replace();
    let fired = fault::fired();
    fault::configure(None);
    assert_eq!(fired, 1);
    error(result, "FAULT_INJECTED");
    let destination = r.destination();
    let receipt =
        r.f.store
            .content_checkpoint_replace_receipt(
                &r.f.saved.intent,
                r.mutation,
                Some(&r.f.saved),
                "target",
                destination,
                &context(),
            )
            .unwrap()
            .unwrap();
    r.f.store
        .apply_content_checkpoint_rename(
            &r.f.saved.intent,
            Uuid::new_v4(),
            Some(&r.f.saved),
            "later",
            false,
            &context(),
        )
        .unwrap();
    assert_eq!(r.replace().unwrap(), receipt);
    assert!(r.f.store.lookup("target").is_err());
    assert_eq!(r.f.store.lookup("later").unwrap().id, r.f.entity);
    let handoff = r.continuation();
    let mut config = FaultConfig::new("content.write.namespace_adopted", FaultAction::Fail);
    config.count = Some(1);
    fault::configure(Some(config));
    let result = r.other.adopt_namespace_continuation(&handoff, &context());
    let fired = fault::fired();
    fault::configure(None);
    assert_eq!(fired, 1);
    assert!(format!("{:#}", result.err().unwrap()).contains("FAULT_INJECTED"));
    let next = r
        .other
        .adopt_namespace_continuation(&handoff, &context())
        .unwrap();
    next.write_at(0, b"more", &context()).unwrap();
    let replay = r
        .other
        .adopt_namespace_continuation(&handoff, &context())
        .unwrap();
    let mut bytes = [0; 4];
    replay.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(&bytes, b"more");
    let saved = replay.checkpoint(&context()).unwrap().unwrap();
    r.f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = replay.prepare(&r.f.local, &saved, &context()).unwrap();
    r.f.store
        .publish_content_save(&prepared, &context())
        .unwrap();
    assert!(r.f.store.lookup("target").is_err());
    assert!(!r.f.store.entry(&r.target).unwrap().alive);
    r.f.oracle();
}

#[test]
fn retirement_rejects_wrong_destination_and_reserved_identity_without_changing_either_owner() {
    for reserved in [false, true] {
        let mut r = Replacement::new(false);
        if !reserved {
            r.f.store.create("different", "file").unwrap();
            let destination = r.destination();
            let error =
                r.f.store
                    .apply_content_checkpoint_replace(
                        &r.f.saved.intent,
                        r.mutation,
                        Some(&r.f.saved),
                        "different",
                        destination,
                        &context(),
                    )
                    .unwrap_err();
            let wire = tkfs::model::errors::ErrorWire::from_error(&error);
            assert_eq!(wire.code, "STALE_CONTENT_DESTINATION");
            assert_eq!(wire.class, tkfs::model::errors::ErrorClass::Contention);
            assert_eq!(r.f.store.lookup("target").unwrap().id, r.target);
        }
        if reserved {
            r.mutation = r.saved.intent.operation;
        } else {
            r.f.store.rename(&r.target, "elsewhere", false).unwrap();
        }
        let result = r.replace();
        assert!(result.is_err());
        assert_eq!(r.f.store.lookup("file").unwrap().id, r.f.entity);
        assert!(
            r.f.store
                .content_namespace_continuation(&r.saved.intent, &context())
                .unwrap()
                .is_none()
        );
        assert!(r.other.checkpoint(&context()).unwrap().is_some());
        assert_eq!(r.f.publications(), 0);
        r.f.oracle();
    }
}

#[test]
fn retirement_final_checks_reject_silent_destination_record_reversions_and_rollback_the_rename() {
    for revert in ["capture", "selection", "owner", "continuation"] {
        let mut r = Replacement::new(false);
        r.f.store
            .select_content_checkpoint(&r.saved, &context())
            .unwrap();
        r.f.store.db.execute_batch("CREATE TEMP TABLE retirement_backup AS SELECT * FROM content_save_operations; CREATE TEMP TABLE retirement_owners AS SELECT * FROM content_save_entities;").unwrap();
        let op = r.saved.intent.operation;
        let sql = match revert {
            "capture" => format!(
                "UPDATE content_save_operations SET capture=(SELECT capture FROM retirement_backup WHERE operation='{op}') WHERE operation='{op}';"
            ),
            "selection" => format!(
                "UPDATE content_save_operations SET selected=(SELECT selected FROM retirement_backup WHERE operation='{op}') WHERE operation='{op}';"
            ),
            "owner" => format!(
                "UPDATE content_save_entities SET generation=(SELECT generation FROM retirement_owners WHERE entity='{}'),owner=NULL WHERE entity='{}';",
                r.target, r.target
            ),
            _ => "DELETE FROM content_save_namespace_continuations;".to_owned(),
        };
        r.f.store.db.execute_batch(&format!("CREATE TEMP TRIGGER revert_retirement AFTER INSERT ON content_save_namespace_changes BEGIN {sql} END;")).unwrap();
        assert!(r.replace().is_err(), "{revert}");
        assert_eq!(r.f.store.lookup("file").unwrap().id, r.f.entity);
        assert_eq!(r.f.store.lookup("target").unwrap().id, r.target);
        assert!(
            r.f.store
                .content_namespace_continuation(&r.saved.intent, &context())
                .unwrap()
                .is_none()
        );
        assert_eq!(r.f.publications(), 0);
        r.f.store
            .db
            .execute_batch("DROP TRIGGER revert_retirement;")
            .unwrap();
        r.replace().unwrap();
        r.invariant();
    }
}

#[test]
fn retired_save_remains_fenced_by_an_ordinary_local_restore_and_retains_its_private_bytes() {
    let mut r = Replacement::new(false);
    r.replace().unwrap();
    let next = r
        .other
        .adopt_namespace_continuation(&r.continuation(), &context())
        .unwrap();
    let saved = next.checkpoint(&context()).unwrap().unwrap();
    let prepared = next.prepare(&r.f.local, &saved, &context()).unwrap();
    let change =
        r.f.store
            .current_change(&r.target, "alive", serde_json::json!(true))
            .unwrap();
    let event =
        r.f.store
            .event(&r.f.store.branch(&r.f.store.active).unwrap(), vec![change]);
    r.f.store.commit(event, true).unwrap();
    error(
        r.f.store.publish_content_save(&prepared, &context()),
        "STALE_CONTENT_SAVE",
    );
    let mut bytes = [0; 4];
    next.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(&bytes, b"kept");
    assert_eq!(r.f.publications(), 0);
    r.f.oracle();
}

#[test]
fn replacement_of_a_clean_destination_keeps_later_native_writes_unnamed() {
    let mut f = Fixture::new();
    let target = f.store.create("target", "file").unwrap().id;
    let intent = f.store.begin_content_save(&target, &context()).unwrap();
    let other = f
        ._journal
        .create(intent.clone(), f.local.clone(), &context())
        .unwrap();
    assert!(other.checkpoint(&context()).unwrap().is_none());
    f.store
        .apply_content_checkpoint_replace(
            &f.saved.intent,
            Uuid::new_v4(),
            Some(&f.saved),
            "target",
            NamespaceDestination {
                intent: intent.clone(),
                checkpoint: None,
            },
            &context(),
        )
        .unwrap();
    let handoff = f
        .store
        .content_namespace_continuation(&intent, &context())
        .unwrap()
        .unwrap();
    let other = other
        .adopt_namespace_continuation(&handoff, &context())
        .unwrap();
    assert!(other.checkpoint(&context()).unwrap().is_none());
    other
        .write_at_with_times(0, b"late", 6, [555, 666], &context())
        .unwrap();
    let saved = other.checkpoint(&context()).unwrap().unwrap();
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = other.prepare(&f.local, &saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    let entry = f.store.entry(&target).unwrap();
    assert!(!entry.alive);
    assert_eq!(entry.basic_info().write_time, 555);
    assert_eq!(entry.basic_info().change_time, 666);
    assert_eq!(f.store.lookup("target").unwrap().id, f.entity);
    f.oracle();
}

#[test]
fn replacement_binds_newer_source_and_destination_native_clocks_and_expires_older_basic_policy() {
    let mut r = Replacement::new(false);
    for saved in [&r.f.saved, &r.saved] {
        r.f.store
            .apply_content_checkpoint_basic(
                &saved.intent,
                Uuid::new_v4(),
                Some(saved),
                ContentBasicUpdate {
                    write_time: Some(777),
                    attributes: Some(0x80),
                    ..Default::default()
                },
                &context(),
            )
            .unwrap();
    }
    r.other
        .write_at_with_times(0, b"dest", 6, [555, 666], &context())
        .unwrap();
    r.saved = r.other.checkpoint(&context()).unwrap().unwrap();
    r.f.session
        .write_at_with_times(0, b"source!", 6, [111, 222], &context())
        .unwrap();
    r.f.saved = r.f.session.checkpoint(&context()).unwrap().unwrap();
    r.replace().unwrap();
    let other = r
        .other
        .adopt_namespace_continuation(&r.continuation(), &context())
        .unwrap();
    let saved = other.checkpoint(&context()).unwrap().unwrap();
    let prepared = other.prepare(&r.f.local, &saved, &context()).unwrap();
    r.f.store
        .publish_content_save(&prepared, &context())
        .unwrap();
    let prepared =
        r.f.session
            .prepare(&r.f.local, &r.f.saved, &context())
            .unwrap();
    r.f.store
        .publish_content_save(&prepared, &context())
        .unwrap();
    let source = r.f.store.entry(&r.f.entity).unwrap().basic_info();
    let dest = r.f.store.entry(&r.target).unwrap().basic_info();
    assert_eq!((source.write_time, source.change_time), (111, 222));
    assert_eq!((dest.write_time, dest.change_time), (555, 666));
    assert_ne!(source.attributes & 0x20, 0);
    assert_ne!(dest.attributes & 0x20, 0);
    r.invariant();
}

#[test]
fn namespace_adoption_rollback_and_replay_preserve_saved_and_newer_buffered_maps_without_pruning() {
    let mut r = Replacement::new(false);
    r.replace().unwrap();
    r.other.set_len(6, &context()).unwrap();
    r.other.write_at(2, b"xy", &context()).unwrap();
    let handoff = r.continuation();
    let mut config = FaultConfig::new(
        "content.write.namespace_adoption_written",
        FaultAction::Fail,
    );
    config.count = Some(1);
    fault::configure(Some(config));
    let result = r.other.adopt_namespace_continuation(&handoff, &context());
    let fired = fault::fired();
    fault::configure(None);
    assert_eq!(fired, 1);
    assert!(result.is_err());
    assert_eq!(r.other.intent(&context()).unwrap(), r.saved.intent);
    let next = r
        .other
        .adopt_namespace_continuation(&handoff, &context())
        .unwrap();
    let mut bytes = [0; 6];
    next.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(&bytes, b"kexy\0\0");
    let saved = next.checkpoint(&context()).unwrap().unwrap();
    assert!(saved.sequence > r.saved.sequence);
    assert_eq!(saved.length, 6);
    r.f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = next.prepare(&r.f.local, &saved, &context()).unwrap();
    r.f.store
        .publish_content_save(&prepared, &context())
        .unwrap();
    r.invariant();
}

#[test]
#[ignore = "actual process-exit child for retired_destination_process_exit_recovery"]
fn retired_destination_crash_child() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_RETIRE_ROOT").unwrap());
    let operation = Uuid::parse_str(&std::env::var("TKFS_RETIRE_SOURCE").unwrap()).unwrap();
    let destination = Uuid::parse_str(&std::env::var("TKFS_RETIRE_TARGET").unwrap()).unwrap();
    let mutation = Uuid::parse_str(&std::env::var("TKFS_RETIRE_MUTATION").unwrap()).unwrap();
    let point = std::env::var("TKFS_RETIRE_POINT").unwrap();
    let mut store = Store::open_existing(&root).unwrap();
    let local = Arc::new(LocalContent::open(&root).unwrap());
    let journal = WriteJournal::open(&root).unwrap();
    let source = journal
        .resume(operation, local.clone(), &context())
        .unwrap();
    let saved = source.checkpoint(&context()).unwrap().unwrap();
    let other = journal
        .resume(destination, local.clone(), &context())
        .unwrap();
    let target = other.checkpoint(&context()).unwrap().unwrap();
    fault::configure(Some(FaultConfig::new(&point, FaultAction::Exit)));
    store
        .apply_content_checkpoint_replace(
            &saved.intent,
            mutation,
            Some(&saved),
            "target",
            NamespaceDestination {
                intent: target.intent.clone(),
                checkpoint: Some(target.clone()),
            },
            &context(),
        )
        .unwrap();
    let handoff = store
        .content_namespace_continuation(&target.intent, &context())
        .unwrap()
        .unwrap();
    let next = other
        .adopt_namespace_continuation(&handoff, &context())
        .unwrap();
    let saved = next.checkpoint(&context()).unwrap().unwrap();
    let prepared = next.prepare(&local, &saved, &context()).unwrap();
    store.publish_content_save(&prepared, &context()).unwrap();
    panic!("did not reach crash point {point}");
}

#[test]
fn retired_destination_process_exit_recovery() {
    for point in [
        "content.save.before_rename",
        "content.save.rename_written",
        "content.save.rename_committed",
        "content.write.before_namespace_adoption",
        "content.write.namespace_adoption_written",
        "content.write.namespace_adopted",
        "content.save.before_metadata",
        "content.save.event_written",
        "content.save.committed",
    ] {
        let r = Replacement::new(false);
        let root = r.f._dir.path().to_owned();
        let source = r.f.saved.clone();
        let target = r.saved.clone();
        let entity = r.target.clone();
        let source_entity = r.f.entity.clone();
        let mutation = r.mutation;
        let Replacement { f, other, .. } = r;
        let Fixture {
            store,
            local,
            _journal,
            session,
            _dir,
            ..
        } = f;
        drop(store);
        drop(local);
        drop(_journal);
        drop(session);
        drop(other);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "retirement::retired_destination_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_RETIRE_ROOT", &root)
            .env("TKFS_RETIRE_SOURCE", source.intent.operation.to_string())
            .env("TKFS_RETIRE_TARGET", target.intent.operation.to_string())
            .env("TKFS_RETIRE_MUTATION", mutation.to_string())
            .env("TKFS_RETIRE_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut store = Store::open_existing(&root).unwrap();
        let local = Arc::new(LocalContent::open(&root).unwrap());
        let journal = WriteJournal::open(&root).unwrap();
        let other = journal
            .resume(target.intent.operation, local.clone(), &context())
            .unwrap();
        let committed = !matches!(
            point,
            "content.save.before_rename" | "content.save.rename_written"
        );
        assert_eq!(
            store.lookup("target").unwrap().id,
            if committed { &source_entity } else { &entity }.clone()
        );
        assert_eq!(
            store
                .content_namespace_continuation(&target.intent, &context())
                .unwrap()
                .is_some(),
            committed
        );
        if !committed {
            store
                .apply_content_checkpoint_replace(
                    &source.intent,
                    mutation,
                    Some(&source),
                    "target",
                    NamespaceDestination {
                        intent: target.intent.clone(),
                        checkpoint: Some(target.clone()),
                    },
                    &context(),
                )
                .unwrap();
        }
        let handoff = store
            .content_namespace_continuation(&target.intent, &context())
            .unwrap()
            .unwrap();
        let other = if other.intent(&context()).unwrap() == *handoff.previous() {
            other
                .adopt_namespace_continuation(&handoff, &context())
                .unwrap()
        } else {
            assert_eq!(&other.intent(&context()).unwrap(), handoff.intent());
            other
        };
        let mut bytes = [0; 4];
        other.read_range(0, &mut bytes, &context()).unwrap();
        assert_eq!(&bytes, b"kept");
        let saved = other.checkpoint(&context()).unwrap().unwrap();
        store.select_content_checkpoint(&saved, &context()).unwrap();
        let prepared = other.prepare(&local, &saved, &context()).unwrap();
        store.publish_content_save(&prepared, &context()).unwrap();
        assert_eq!(store.lookup("target").unwrap().id, source_entity);
        assert!(!store.entry(&entity).unwrap().alive);
        assert_eq!(
            store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |row| row
                    .get::<_, u64>(
                    0
                ))
                .unwrap(),
            1
        );
        assert!(store.verify_derived_state().unwrap().is_empty());
    }
}
