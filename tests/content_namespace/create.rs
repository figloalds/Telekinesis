use super::*;
use tkfs::core::content_saves::{ContentCreateRequest, NamespaceTarget};

fn request(f: &Fixture, path: &str) -> ContentCreateRequest {
    let parent = tkfs::core::split_path(path).unwrap().0;
    ContentCreateRequest {
        mutation: Uuid::new_v4(),
        entity: Uuid::new_v4(),
        operation: Uuid::new_v4(),
        branch: f.store.active.clone(),
        view_generation: f.store.generation,
        target: NamespaceTarget {
            path: path.into(),
            parent: f.store.lookup(&parent).unwrap().id,
        },
        basic: f.basic(),
    }
}
#[test]
fn typed_creation_and_initial_owner_are_atomic_and_receipt_replay_never_recreates_the_name() {
    let mut f = Fixture::new();
    let req = request(&f, "created");
    let empty = f.local.import(&mut [].as_slice(), 0, &context()).unwrap();
    let intent = f.store.apply_content_create(&req, &context()).unwrap();
    assert_eq!(intent.base.content, empty);
    let entry = f.store.lookup("created").unwrap();
    assert_eq!(entry.id, req.entity.to_string());
    assert!(entry.content.is_none());
    assert_eq!(entry.basic_info(), req.basic);
    assert_eq!(f.publications(), 0);
    let session = f
        ._journal
        .create(intent.clone(), f.local.clone(), &context())
        .unwrap();
    session.write_at(0, b"created", &context()).unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = session.prepare(&f.local, &saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    f.store.rename(&entry.id, "moved", false).unwrap();
    f.store.delete(&entry.id).unwrap();
    assert_eq!(
        f.store.content_create_receipt(&req, &context()).unwrap(),
        Some(intent.clone())
    );
    assert_eq!(
        f.store.apply_content_create(&req, &context()).unwrap(),
        intent
    );
    assert!(f.store.lookup("created").is_err());
    assert!(f.store.lookup("moved").is_err());
    let mut altered = req.clone();
    altered.target.path = "other".into();
    error(
        f.store.content_create_receipt(&altered, &context()),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    assert_eq!(f.publications(), 1);
    f.oracle();
}
#[test]
fn typed_creation_fences_parent_view_collision_and_reserved_event_identity_before_commit() {
    let mut f = Fixture::new();
    let req = request(&f, "created");
    let mut wrong = req.clone();
    wrong.target.parent = "another-directory".into();
    error(
        f.store.apply_content_create(&wrong, &context()),
        "STALE_CONTENT_DESTINATION",
    );
    wrong = req.clone();
    wrong.view_generation += 1;
    error(
        f.store.apply_content_create(&wrong, &context()),
        "STALE_CONTENT_VIEW",
    );
    wrong = req.clone();
    wrong.target.path = "file".into();
    error(
        f.store.apply_content_create(&wrong, &context()),
        "NAME_COLLISION",
    );
    wrong = req.clone();
    wrong.operation = wrong.mutation;
    error(
        f.store.apply_content_create(&wrong, &context()),
        "CONTENT_SAVE_INTENT_MISMATCH",
    );
    wrong = req.clone();
    wrong.operation = f.saved.intent.operation;
    error(
        f.store.apply_content_create(&wrong, &context()),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    assert!(f.store.lookup("created").is_err());
    assert!(
        f.store
            .content_create_receipt(&req, &context())
            .unwrap()
            .is_none()
    );
    f.store.apply_content_create(&req, &context()).unwrap();
    f.oracle();
}
#[test]
fn silent_creation_receipt_capture_and_owner_writes_cannot_acknowledge_a_file() {
    for sql in [
        "CREATE TRIGGER sabotaged BEFORE INSERT ON content_save_create_changes BEGIN SELECT RAISE(IGNORE); END",
        "CREATE TRIGGER sabotaged AFTER INSERT ON content_save_create_changes BEGIN DELETE FROM content_save_create_changes WHERE mutation=NEW.mutation; END",
        "CREATE TRIGGER sabotaged AFTER INSERT ON content_save_create_changes BEGIN DELETE FROM content_save_operations WHERE operation='$OP'; END",
        "CREATE TRIGGER sabotaged BEFORE INSERT ON content_save_operations BEGIN SELECT RAISE(IGNORE); END",
        "CREATE TRIGGER sabotaged AFTER INSERT ON content_save_entities BEGIN UPDATE content_save_entities SET owner=NULL WHERE branch=NEW.branch AND entity=NEW.entity; END",
    ] {
        let mut f = Fixture::new();
        let req = request(&f, "created");
        let sql = sql.replace("$OP", &req.operation.to_string());
        f.store.db.execute_batch(&sql).unwrap();
        assert!(f.store.apply_content_create(&req, &context()).is_err());
        assert!(f.store.lookup("created").is_err());
        assert!(
            f.store
                .content_create_receipt(&req, &context())
                .unwrap()
                .is_none()
        );
        let count: u64 = f
            .store
            .db
            .query_row(
                "SELECT count(*) FROM events WHERE id=?",
                [req.mutation.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
        f.store.db.execute_batch("DROP TRIGGER sabotaged").unwrap();
        f.store.apply_content_create(&req, &context()).unwrap();
        f.oracle();
    }
}
#[test]
fn creation_failures_and_lost_replies_leave_one_exact_namespace_receipt() {
    for point in [
        "content.create.before_event",
        "content.create.event_written",
        "content.create.written",
        "content.create.committed",
    ] {
        let mut f = Fixture::new();
        let req = request(&f, "created");
        fault::configure(Some(FaultConfig::new(point, FaultAction::Fail)));
        error(
            f.store.apply_content_create(&req, &context()),
            "FAULT_INJECTED",
        );
        fault::configure(None);
        let committed = point == "content.create.committed";
        assert_eq!(f.store.lookup("created").is_ok(), committed);
        assert_eq!(
            f.store
                .content_create_receipt(&req, &context())
                .unwrap()
                .is_some(),
            committed
        );
        let intent = f.store.apply_content_create(&req, &context()).unwrap();
        assert_eq!(intent.operation, req.operation);
        assert_eq!(f.publications(), 0);
        f.oracle();
    }
}

#[test]
fn creation_rejects_a_valid_initial_capture_silently_substituted_before_ack() {
    #[derive(serde::Serialize)]
    struct Captured {
        intent: tkfs::content::write::SaveIntent,
        view_generation: u64,
        event: tkfs::core::Event,
    }
    #[derive(serde::Serialize)]
    struct Record<'a> {
        value: &'a Captured,
        checksum: String,
    }
    for table in ["content_save_operations", "content_save_create_changes"] {
        let mut f = Fixture::new();
        let req = request(&f, "created");
        let content = f.local.import(&mut [].as_slice(), 0, &context()).unwrap();
        let intent = tkfs::content::write::SaveIntent {
            operation: req.operation,
            branch: req.branch.clone(),
            content_parent: Some(req.mutation.to_string()),
            alive_parent: Some(req.mutation.to_string()),
            base: tkfs::model::workspace::FileRevision {
                workspace: f.saved.intent.base.workspace.clone(),
                entity: req.entity.to_string(),
                generation: 1,
                content: content.clone(),
            },
        };
        let mut altered = req.basic;
        altered.attributes = BasicInfo::attributes("file", altered.attributes | 0x20).unwrap();
        altered.access_time += 17;
        let changes = [
            ("content", serde_json::json!(content)),
            ("alive", serde_json::json!(true)),
            ("basic", serde_json::json!(altered)),
        ]
        .into_iter()
        .map(|(field, value)| tkfs::core::Change {
            entity: req.entity.to_string(),
            field: field.into(),
            parents: vec![req.mutation.to_string()],
            value,
        })
        .collect();
        let mut event = f
            .store
            .event(&f.store.branch(&req.branch).unwrap(), changes);
        event.id = req.operation.to_string();
        event.dependencies = vec![req.mutation.to_string()];
        let capture = Captured {
            intent,
            view_generation: req.view_generation,
            event,
        };
        let raw = serde_json::to_vec(&Record {
            value: &capture,
            checksum: tkfs::core::hash(&serde_json::to_vec(&("capture:v1", &capture)).unwrap()),
        })
        .unwrap();
        f.store.db.execute_batch(&format!("CREATE TRIGGER valid_substitution AFTER INSERT ON {table} BEGIN UPDATE content_save_operations SET capture=x'{}' WHERE operation='{}'; END",hex::encode(raw),req.operation)).unwrap();
        error(
            f.store.apply_content_create(&req, &context()),
            "CONTENT_METADATA_CONTINUATION_LOST",
        );
        assert!(f.store.lookup("created").is_err());
        assert!(
            f.store
                .content_create_receipt(&req, &context())
                .unwrap()
                .is_none()
        );
        f.oracle();
    }
}
