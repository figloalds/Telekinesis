use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        ContentProvider, LocalContent, ReadContext,
        receipt::{self, ReceiptRecovery, ReceivedFrame},
    },
    core::{Bundle, Change, Store, admission::reference_id},
    model::content::{CHUNK_SIZE, FileContentRef, ObjectKey, ObjectKind},
    util::Cancel,
};

fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    }
}

fn store(root: &std::path::Path) -> (Store, Arc<LocalContent>) {
    let store = Store::initialize(root, "content-objects", &tkfs::core::id()).unwrap();
    store
        .db
        .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
        .unwrap();
    let provider = Arc::new(LocalContent::open(root).unwrap());
    (store, provider)
}

fn publish(
    store: &mut Store,
    provider: &LocalContent,
    name: &str,
    bytes: &[u8],
) -> (String, FileContentRef) {
    let entry = store.create(name, "file").unwrap();
    let root = provider
        .import(&mut &bytes[..], bytes.len() as u64, &context())
        .unwrap();
    let event = store.event(
        &store.branch(&store.active).unwrap(),
        vec![Change {
            entity: entry.id.clone(),
            field: "content".into(),
            parents: entry
                .revisions
                .get("content")
                .cloned()
                .into_iter()
                .collect(),
            value: serde_json::json!(root),
        }],
    );
    let reference = reference_id(&event.id, &entry.id);
    store.commit(event, true).unwrap();
    while store.consume_content_admission("objects-v1", 256).unwrap() != 0 {}
    expand(store, provider);
    (reference, root)
}

fn metadata(source: &Store, dest: &mut Store) {
    dest.receive(
        Bundle {
            repo: source.repo.clone(),
            branches: vec![],
            objects: BTreeMap::new(),
            events: source.events().unwrap(),
        },
        &BTreeSet::from([source.device.clone()]),
    )
    .unwrap();
    while dest.consume_content_admission("objects-v1", 256).unwrap() != 0 {}
}

fn expand(store: &mut Store, provider: &LocalContent) {
    loop {
        let items = store.content_candidates(256, true).unwrap();
        if items.is_empty() {
            break;
        }
        for item in items {
            store
                .expand_content_edge(provider, &item, &context())
                .unwrap();
        }
    }
}

fn transfer(
    source: (&Store, &LocalContent),
    dest: (&mut Store, &LocalContent),
    reference: &str,
    key: ObjectKey,
) -> anyhow::Result<()> {
    let (_, bytes) = source
        .0
        .read_content_frame(source.1, reference, key, &context())?;
    let generation = dest.0.content_root(reference)?.0;
    receipt::receive(
        dest.0,
        dest.1,
        ReceivedFrame {
            reference,
            generation,
            key,
            bytes: &bytes,
            source: &source.0.device,
        },
        &context(),
    )
}

#[test]
fn parents_admit_children_and_private_deduplication_does_not_publish_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(&temp.path().join("a"));
    let (mut b, bp) = store(&temp.path().join("b"));
    let data: Vec<u8> = (0..150_000)
        .map(|i| ((i / 65536) * 73 + i % 251) as u8)
        .collect();
    let (reference, root) = publish(&mut a, &ap, "file", &data);
    metadata(&a, &mut b);
    let key = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: tkfs::model::content::Digest::bytes(&data[..65536]),
    };
    bp.install(key, &data[..65536], &context()).unwrap(); // private-only physical copy
    assert!(b.content_edge(&reference, 1, key).is_err());
    assert!(
        b.read_content_frame(&bp, &key.digest.to_string(), key, &context())
            .is_err()
    );
    transfer((&a, &ap), (&mut b, &bp), &reference, ObjectKey::root(&root)).unwrap();
    expand(&mut b, &bp);
    let edge = b.content_edge(&reference, 1, key).unwrap();
    b.verify_content_edge(&bp, &edge, &context()).unwrap();
    assert!(
        b.read_content_frame(&bp, &reference, key, &context())
            .is_err(),
        "a hash/index hit promoted private bytes to sendable content"
    );
    transfer((&a, &ap), (&mut b, &bp), &reference, key).unwrap();
    assert_eq!(
        b.read_content_frame(&bp, &reference, key, &context())
            .unwrap()
            .1,
        &data[..65536]
    );
    let pin = bp.pin(&root).unwrap();
    let mut out = [0; 17];
    bp.read_range(&pin, 7, &mut out, &context()).unwrap();
    assert_eq!(out, data[7..24]);
    assert!(
        bp.read_range(&pin, CHUNK_SIZE, &mut out, &context())
            .is_err(),
        "unrelated unavailable chunk was silently materialized"
    );
}

#[test]
fn lost_receipt_index_write_recovers_from_durable_verified_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(&temp.path().join("a"));
    let (mut b, bp) = store(&temp.path().join("b"));
    let (reference, root) = publish(&mut a, &ap, "file", b"received content");
    metadata(&a, &mut b);
    let key = ObjectKey::root(&root);
    // Fresh creation also admitted the canonical empty revision. Verify its
    // existing local bytes before isolating the failed receipt for this root.
    for item in b.content_candidates(256, false).unwrap() {
        if item.edge.key != key {
            assert_eq!(item.edge.len, 0);
            assert_eq!(
                item.edge.key,
                ObjectKey {
                    kind: ObjectKind::Bytes,
                    digest: tkfs::model::content::Digest::bytes(&[]),
                }
            );
            b.verify_content_edge(&bp, &item, &context()).unwrap();
        }
    }
    assert_eq!(
        b.content_candidates(256, false)
            .unwrap()
            .into_iter()
            .map(|item| item.edge.key)
            .collect::<Vec<_>>(),
        vec![key]
    );
    b.db.execute_batch(
        "CREATE TEMP TRIGGER reject_receipt BEFORE INSERT ON content_public_receipts
        BEGIN SELECT RAISE(ABORT,'lost receipt index write'); END",
    )
    .unwrap();
    assert!(transfer((&a, &ap), (&mut b, &bp), &reference, key).is_err());
    let edge = b.content_edge(&reference, 1, key).unwrap();
    b.verify_content_edge(&bp, &edge, &context()).unwrap();
    assert!(
        b.content_candidates(256, false).unwrap().is_empty(),
        "installed bytes were wanted again solely because the index write failed"
    );
    assert!(
        b.read_content_frame(&bp, &reference, key, &context())
            .is_err()
    );
    let root_path = b.root.clone();
    drop(b);
    let mut b = Store::open_existing(&root_path).unwrap();
    let mut recovery = ReceiptRecovery::open(&bp).unwrap().unwrap();
    let (completed, done) = recovery.step(&mut b, &bp, 256, &context()).unwrap();
    assert_eq!(completed, 1);
    assert!(done);
    assert_eq!(
        b.read_content_frame(&bp, &reference, key, &context())
            .unwrap()
            .1,
        b"received content"
    );
}

#[test]
fn stale_or_forged_edge_expansion_cannot_admit_children() {
    use tkfs::model::workspace::AdmissionAction;
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(temp.path());
    let (reference, root) = publish(&mut a, &ap, "file", &vec![1; 150_000]);
    let item = a
        .content_edge(&reference, 1, ObjectKey::root(&root))
        .unwrap();
    let before: i64 =
        a.db.query_row("SELECT count(*) FROM content_manifest_edges", [], |row| {
            row.get(0)
        })
        .unwrap();
    let mut forged = item.clone();
    forged.edge.offset = CHUNK_SIZE;
    assert!(a.expand_content_edge(&ap, &forged, &context()).is_err());
    a.set_content_admission(&reference, 1, AdmissionAction::Withdraw)
        .unwrap();
    assert!(a.expand_content_edge(&ap, &item, &context()).is_err());
    assert!(
        a.read_content_frame(&ap, &reference, item.edge.key, &context())
            .is_err()
    );
    let after: i64 =
        a.db.query_row("SELECT count(*) FROM content_manifest_edges", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(before, after);
}

#[test]
fn a_forged_derived_index_cannot_disclose_a_private_chunk() {
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(temp.path());
    let (reference, root) = publish(&mut a, &ap, "public", &vec![1; 150_000]);
    let private = vec![7; 65536];
    let key = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: tkfs::model::content::Digest::bytes(&private),
    };
    ap.install(key, &private, &context()).unwrap();
    let mut forged = a.content_edge_at(&reference, 1, 0, None).unwrap().edge;
    forged.key = key;
    a.db.execute(
        "UPDATE content_manifest_edges SET digest=?1 WHERE parent=?2 AND ordinal=0",
        rusqlite::params![key.digest.to_string(), root.digest.to_string()],
    )
    .unwrap();
    assert!(
        a.read_content_frame_at(&ap, &reference, &forged, &context())
            .is_err()
    );
    assert!(
        a.read_content_frame(&ap, &reference, key, &context())
            .is_err(),
        "derived membership was trusted without checking the authenticated root"
    );
}

#[test]
fn a_sender_proves_a_position_without_waiting_for_its_derived_graph() {
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(temp.path());
    let (reference, _) = publish(&mut a, &ap, "file", &vec![3; 150_000]);
    let edge = a
        .content_edge_at(&reference, 1, CHUNK_SIZE, None)
        .unwrap()
        .edge;
    a.db.execute_batch("DELETE FROM content_object_jobs; DELETE FROM content_manifest_edges;")
        .unwrap();
    assert_eq!(
        a.read_content_frame_at(&ap, &reference, &edge, &context())
            .unwrap()
            .1,
        vec![3; 65536]
    );
}

#[test]
fn revoked_alias_history_is_paged_instead_of_scanned_in_one_slice() {
    use tkfs::model::workspace::AdmissionAction;
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(temp.path());
    let (first, root) = publish(&mut a, &ap, "first", b"pending bytes");
    let (second, _) = publish(&mut a, &ap, "second", b"pending bytes");
    a.set_content_admission(&second, 1, AdmissionAction::Withdraw)
        .unwrap();
    for generation in 10..310 {
        a.db.execute("INSERT INTO content_page_aliases(kind,digest,reference,generation,offset) VALUES(0,?1,?2,?3,0)",
            rusqlite::params![root.digest.to_string(),first,generation]).unwrap();
    }
    assert!(a.content_candidates(1, false).unwrap().is_empty());
    let cursor: u64 =
        a.db.query_row(
            "SELECT before_id FROM content_alias_cursor WHERE kind=0 AND digest=?1",
            [root.digest.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert!(cursor > 0, "the first slice scanned all historical aliases");
    a.db.execute("UPDATE content_object_jobs SET retry_at_ms=0", [])
        .unwrap();
    let resumed = a.content_candidates(1, false).unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].reference, first);
}

#[test]
fn a_reused_subtree_survives_withdrawal_of_its_original_root_witness() {
    use tkfs::model::workspace::AdmissionAction;
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(temp.path());
    let mut bytes = vec![0; CHUNK_SIZE as usize * 512];
    for (index, chunk) in bytes.chunks_mut(CHUNK_SIZE as usize).enumerate() {
        chunk.fill(index as u8);
        chunk[..8].copy_from_slice(&(index as u64).to_le_bytes());
    }
    let (first, root) = publish(&mut a, &ap, "file", &bytes);
    let entity: String =
        a.db.query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?1",
            [&first],
            |r| r.get(0),
        )
        .unwrap();
    let entry = a.entry(&entity).unwrap();
    let updated = ap
        .overwrite(&ap.pin(&root).unwrap(), 0, &[7; 4096], &context())
        .unwrap();
    let event = a.event(
        &a.branch(&a.active).unwrap(),
        vec![Change {
            entity: entity.clone(),
            field: "content".into(),
            parents: entry
                .revisions
                .get("content")
                .cloned()
                .into_iter()
                .collect(),
            value: serde_json::json!(updated),
        }],
    );
    let second = reference_id(&event.id, &entity);
    a.commit(event, true).unwrap();
    while a.consume_content_admission("objects-v1", 256).unwrap() != 0 {}
    expand(&mut a, &ap);
    let before: i64 =
        a.db.query_row("SELECT count(*) FROM content_manifest_edges", [], |r| {
            r.get(0)
        })
        .unwrap();
    a.set_content_admission(&first, 1, AdmissionAction::Withdraw)
        .unwrap();
    let items = a.content_candidates(256, false).unwrap();
    assert!(!items.is_empty());
    assert!(items.iter().all(|item| item.reference == second));
    let start = CHUNK_SIZE as usize * 300;
    let key = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: tkfs::model::content::Digest::bytes(&bytes[start..start + 65536]),
    };
    let item = a.content_edge(&second, 1, key).unwrap();
    assert_eq!(item.edge.offset, start as u64);
    assert_eq!(
        a.read_content_frame_at(&ap, &second, &item.edge, &context())
            .unwrap()
            .1,
        &bytes[start..start + 65536]
    );
    let after: i64 =
        a.db.query_row("SELECT count(*) FROM content_manifest_edges", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(before, after, "revocation copied a reused subtree");
}

#[test]
fn withdrawing_one_reference_keeps_another_valid_reference_sendable() {
    use tkfs::model::workspace::AdmissionAction;
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(&temp.path().join("a"));
    let (mut b, bp) = store(&temp.path().join("b"));
    let (first, root) = publish(&mut a, &ap, "first", b"same public bytes");
    let (second, _) = publish(&mut a, &ap, "second", b"same public bytes");
    metadata(&a, &mut b);
    transfer((&a, &ap), (&mut b, &bp), &first, ObjectKey::root(&root)).unwrap();
    b.set_content_admission(&first, 1, AdmissionAction::Withdraw)
        .unwrap();
    assert!(
        b.read_content_frame(&bp, &first, ObjectKey::root(&root), &context())
            .is_err()
    );
    assert_eq!(
        b.read_content_frame(&bp, &second, ObjectKey::root(&root), &context())
            .unwrap()
            .1,
        b"same public bytes"
    );
}

#[test]
fn range_work_admits_under_cross_repository_saturation_and_retries_another_source() {
    use tkfs::{
        content::{
            scheduler::{ContentScheduler, TransferKey},
            work::prepare_transfer,
        },
        model::workspace::{ByteRange, Demand, DemandPriority, FileRevision, WorkspaceId},
        util::phases::Phase,
    };
    use uuid::Uuid;
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(&temp.path().join("a"));
    let (mut b, bp) = store(&temp.path().join("b"));
    let (reference, root) = publish(&mut a, &ap, "file", b"foreground bytes");
    b.receive(
        Bundle {
            repo: a.repo.clone(),
            branches: vec![],
            objects: BTreeMap::new(),
            events: a.events().unwrap(),
        },
        &BTreeSet::from([a.device.clone()]),
    )
    .unwrap();
    let jobs: i64 =
        b.db.query_row("SELECT count(*) FROM content_object_jobs", [], |r| r.get(0))
            .unwrap();
    assert_eq!(jobs, 0, "fixture unexpectedly consumed object admission");
    let entity: String =
        b.db.query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?",
            [&reference],
            |row| row.get(0),
        )
        .unwrap();
    let scheduler = Arc::new(ContentScheduler::default());
    let mut background = vec![];
    for i in 0..6_u64 {
        background.push(
            scheduler
                .try_acquire(
                    TransferKey {
                        provider: Uuid::nil(),
                        object: ObjectKey {
                            kind: ObjectKind::Bytes,
                            digest: tkfs::model::content::Digest::bytes(&i.to_le_bytes()),
                        },
                    },
                    &format!("other-repo{}", i / 2),
                    &format!("other-peer{}", i / 2),
                    DemandPriority::Background,
                    65536,
                )
                .unwrap()
                .unwrap(),
        );
    }
    let pending = scheduler
        .demand_on(
            bp.identity(),
            Demand {
                waiter: Uuid::new_v4(),
                revision: FileRevision {
                    workspace: WorkspaceId {
                        repository: b.repo.clone(),
                        workspace: Uuid::new_v4(),
                    },
                    entity,
                    generation: 1,
                    content: root.clone(),
                },
                reference: reference.clone(),
                range: ByteRange {
                    offset: 0,
                    len: root.total_len,
                },
                priority: DemandPriority::Foreground,
                eligible_peers: vec![a.device.clone(), "alternate".into()],
            },
            &context(),
        )
        .unwrap();
    let started = Instant::now();
    let first = prepare_transfer(&mut b, &bp, &scheduler, &a.device, &context())
        .unwrap()
        .unwrap();
    assert_eq!(first.priority, DemandPriority::Foreground);
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(
        scheduler.counters.snapshot()[Phase::DemandAdmission as usize].operations,
        1
    );
    first.failed(&mut b).unwrap();
    assert!(
        prepare_transfer(&mut b, &bp, &scheduler, &a.device, &context())
            .unwrap()
            .is_none()
    );
    let second = prepare_transfer(&mut b, &bp, &scheduler, "alternate", &context())
        .unwrap()
        .unwrap();
    let bytes = a
        .read_content_frame(&ap, &reference, ObjectKey::root(&root), &context())
        .unwrap()
        .1;
    second.complete(&mut b, &bp, &bytes, &context()).unwrap();
    assert_eq!(scheduler.usage().0, 6);
    drop(pending);
    drop(background);
    assert_eq!(scheduler.usage().0, 0);
}

#[test]
fn structural_range_demand_uses_reserved_class_without_fetching_its_data_chunk() {
    use tkfs::{
        content::{
            scheduler::ContentScheduler,
            work::{TransferSelection, prepare_transfer_scoped},
        },
        model::{
            content_wire::PrepareMode,
            protocol::LaneSpec,
            workspace::{ByteRange, Demand, DemandPriority, FileRevision, WorkspaceId},
        },
    };
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(&temp.path().join("a"));
    let (mut b, bp) = store(&temp.path().join("b"));
    let (reference, root) = publish(&mut a, &ap, "file", &vec![75; 3 * 65536]);
    metadata(&a, &mut b);
    let entity: String =
        b.db.query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?",
            [&reference],
            |r| r.get(0),
        )
        .unwrap();
    let scheduler = Arc::new(ContentScheduler::default());
    let mut demand = Demand {
        waiter: uuid::Uuid::new_v4(),
        reference: reference.clone(),
        revision: FileRevision {
            workspace: WorkspaceId {
                repository: b.repo.clone(),
                workspace: uuid::Uuid::new_v4(),
            },
            entity,
            generation: 1,
            content: root.clone(),
        },
        range: ByteRange {
            offset: 65536,
            len: 1,
        },
        priority: DemandPriority::Structural,
        eligible_peers: vec![],
    };
    let lease = scheduler
        .demand_on(bp.identity(), demand.clone(), &context())
        .unwrap();
    let foreground = TransferSelection {
        mode: PrepareMode::Foreground,
        lane: LaneSpec::new(0, 1),
    };
    let structural = TransferSelection {
        mode: PrepareMode::Structural,
        lane: LaneSpec::new(0, 1),
    };
    assert!(
        prepare_transfer_scoped(&mut b, &bp, &scheduler, &a.device, &foreground, &context())
            .unwrap()
            .is_none()
    );
    let transfer =
        prepare_transfer_scoped(&mut b, &bp, &scheduler, &a.device, &structural, &context())
            .unwrap()
            .unwrap();
    assert_eq!(transfer.priority, DemandPriority::Structural);
    assert_eq!(transfer.item.edge.key.kind, ObjectKind::Manifest);
    let body = a
        .read_content_frame(&ap, &reference, transfer.item.edge.key, &context())
        .unwrap()
        .1;
    transfer.complete(&mut b, &bp, &body, &context()).unwrap();
    for _ in 0..4 {
        assert!(
            prepare_transfer_scoped(&mut b, &bp, &scheduler, &a.device, &structural, &context())
                .unwrap()
                .is_none()
        );
    }
    let data = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: tkfs::model::content::Digest::bytes(&vec![75; 65536]),
    };
    assert!(!b.content_object_verified(data).unwrap());
    drop(lease);
    demand.waiter = uuid::Uuid::new_v4();
    demand.priority = DemandPriority::Foreground;
    let lease = scheduler
        .demand_on(bp.identity(), demand, &context())
        .unwrap();
    let transfer =
        prepare_transfer_scoped(&mut b, &bp, &scheduler, &a.device, &foreground, &context())
            .unwrap()
            .unwrap();
    assert_eq!(transfer.priority, DemandPriority::Foreground);
    assert_eq!(transfer.item.edge.key, data);
    let body = a
        .read_content_frame(&ap, &reference, data, &context())
        .unwrap()
        .1;
    transfer.complete(&mut b, &bp, &body, &context()).unwrap();
    drop(lease);
    assert_eq!(scheduler.usage().0, 0);
}

#[test]
fn receipt_recovery_never_serves_corruption_and_verified_repair_preserves_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(&temp.path().join("a"));
    let (mut b, bp) = store(&temp.path().join("b"));
    let (reference, root) = publish(&mut a, &ap, "file", b"correct bytes");
    metadata(&a, &mut b);
    let key = ObjectKey::root(&root);
    b.db.execute_batch(
        "CREATE TEMP TRIGGER reject_receipt BEFORE INSERT ON content_public_receipts
        BEGIN SELECT RAISE(ABORT,'lost receipt index'); END",
    )
    .unwrap();
    assert!(transfer((&a, &ap), (&mut b, &bp), &reference, key).is_err());
    b.db.execute_batch("DROP TRIGGER reject_receipt").unwrap();
    std::fs::write(
        b.root.join("objects").join(key.digest.to_string()),
        b"corrupt bytes",
    )
    .unwrap();
    let mut recovery = ReceiptRecovery::open(&bp).unwrap().unwrap();
    assert!(recovery.step(&mut b, &bp, 256, &context()).is_err());
    assert!(
        b.read_content_frame(&bp, &reference, key, &context())
            .is_err()
    );
    bp.install(key, b"correct bytes", &context()).unwrap();
    assert_eq!(
        std::fs::read_dir(b.root.join("quarantine-content"))
            .unwrap()
            .count(),
        1
    );
    let mut recovery = ReceiptRecovery::open(&bp).unwrap().unwrap();
    assert_eq!(recovery.step(&mut b, &bp, 256, &context()).unwrap().0, 1);
    assert_eq!(
        b.read_content_frame(&bp, &reference, key, &context())
            .unwrap()
            .1,
        b"correct bytes"
    );
}

#[test]
fn receipt_crash_worker() {
    let Some(root) = std::env::var_os("TKFS_RECEIPT_CHILD_ROOT") else {
        return;
    };
    let point = std::env::var("TKFS_RECEIPT_CHILD_POINT").unwrap();
    let reference = std::env::var("TKFS_RECEIPT_CHILD_REFERENCE").unwrap();
    let mut store = Store::open_existing(std::path::Path::new(&root)).unwrap();
    let provider = LocalContent::open(std::path::Path::new(&root)).unwrap();
    let key = ObjectKey::root(&store.content_root(&reference).unwrap().1);
    tkfs::util::fault::configure(Some(tkfs::util::fault::FaultConfig::new(
        &point,
        tkfs::util::fault::FaultAction::Exit,
    )));
    receipt::receive(
        &mut store,
        &provider,
        ReceivedFrame {
            reference: &reference,
            generation: 1,
            key,
            bytes: b"crash bytes",
            source: "source",
        },
        &context(),
    )
    .unwrap();
    panic!("armed crash boundary was not reached");
}

#[test]
fn process_crashes_around_receipt_boundaries_preserve_verified_recovery() {
    for point in [
        "content.receipt.prepared",
        "content.receipt.installed",
        "content.receipt.committed",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (mut a, ap) = store(&temp.path().join("a"));
        let (mut b, bp) = store(&temp.path().join("b"));
        let (reference, root) = publish(&mut a, &ap, "file", b"crash bytes");
        metadata(&a, &mut b);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["receipt_crash_worker", "--exact", "--test-threads=1"])
            .env("TKFS_RECEIPT_CHILD_ROOT", &b.root)
            .env("TKFS_RECEIPT_CHILD_POINT", point)
            .env("TKFS_RECEIPT_CHILD_REFERENCE", &reference)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(tkfs::util::fault::EXIT_CODE),
            "{point}: {:?}",
            output
        );
        let mut recovery = ReceiptRecovery::open(&bp).unwrap().unwrap();
        if point == "content.receipt.prepared" {
            assert!(recovery.step(&mut b, &bp, 256, &context()).is_err());
            transfer((&a, &ap), (&mut b, &bp), &reference, ObjectKey::root(&root)).unwrap();
        } else {
            assert_eq!(recovery.step(&mut b, &bp, 256, &context()).unwrap().0, 1);
        }
        assert_eq!(
            b.read_content_frame(&bp, &reference, ObjectKey::root(&root), &context())
                .unwrap()
                .1,
            b"crash bytes"
        );
    }
}

#[test]
fn an_unchanged_manifest_subtree_is_not_reexpanded_for_a_small_revision() {
    use tkfs::model::content::ManifestNode;
    use tkfs::util::phases::Phase;
    let temp = tempfile::tempdir().unwrap();
    let (mut source, provider) = store(temp.path());
    let entry = source.create("file", "file").unwrap();
    let len = CHUNK_SIZE * 1024;
    let first = provider
        .import(&mut std::io::repeat(1), len, &context())
        .unwrap();
    let save = |source: &mut Store, root: &FileContentRef| {
        let old = source.entry(&entry.id).unwrap();
        let event = source.event(
            &source.branch(&source.active).unwrap(),
            vec![Change {
                entity: entry.id.clone(),
                field: "content".into(),
                parents: old.revisions.get("content").cloned().into_iter().collect(),
                value: serde_json::json!(root),
            }],
        );
        source.commit(event, true).unwrap();
        while source.consume_content_admission("objects-v1", 256).unwrap() != 0 {}
    };
    save(&mut source, &first);
    expand(&mut source, &provider);
    let pin = provider.pin(&first).unwrap();
    let second = provider.overwrite(&pin, 0, &[2; 4096], &context()).unwrap();
    save(&mut source, &second);
    let before = provider.counters.snapshot()[Phase::ManifestRead as usize].operations;
    expand(&mut source, &provider);
    let pages = provider.counters.snapshot()[Phase::ManifestRead as usize].operations - before;
    assert!(
        pages <= u64::from(ManifestNode::root_level(len).unwrap()) + 1,
        "a 4 KiB revision reread {pages} manifest pages, including unchanged subtrees"
    );
}

#[test]
fn retired_descriptor_cannot_enter_metadata_or_chunk_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let (mut store, _) = store(temp.path());
    let entry = store.create("file", "file").unwrap();
    let before = store.events().unwrap();
    let admission = store.content_admission_page(0, 256).unwrap();
    let event = store.event(
        &store.branch(&store.active).unwrap(),
        vec![Change {
            entity: entry.id.clone(),
            field: "content".into(),
            parents: entry
                .revisions
                .get("content")
                .cloned()
                .into_iter()
                .collect(),
            value: serde_json::json!({
                "kind":"legacy", "format":0, "algorithm":"sha256",
                "digest":tkfs::model::content::Digest::bytes(b"retired").to_string(),
                "total_len":70000
            }),
        }],
    );
    assert!(store.commit(event, true).is_err());
    assert_eq!(store.events().unwrap(), before);
    assert_eq!(store.entry(&entry.id).unwrap(), entry);
    assert_eq!(store.content_admission_page(0, 256).unwrap(), admission);
}

#[test]
fn a_backed_off_prefix_does_not_hide_later_objects_from_the_same_source() {
    use tkfs::{
        content::{
            scheduler::ContentScheduler,
            work::{TransferSelection, prepare_transfer_scoped},
        },
        model::{content_wire::PrepareMode, protocol::LaneSpec},
    };
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(&temp.path().join("a"));
    let (mut b, bp) = store(&temp.path().join("b"));
    for i in 0..40_u64 {
        publish(&mut a, &ap, &format!("file-{i}"), &i.to_le_bytes());
    }
    metadata(&a, &mut b);
    let digests: Vec<String> = b
        .db
        .prepare("SELECT digest FROM content_object_jobs WHERE kind=0 ORDER BY digest LIMIT 16")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    for digest in &digests {
        b.backoff_content_source(
            ObjectKey {
                kind: ObjectKind::Bytes,
                digest: tkfs::model::content::Digest::parse(digest).unwrap(),
            },
            &a.device,
        )
        .unwrap();
    }
    let scheduler = Arc::new(ContentScheduler::default());
    let selection = TransferSelection {
        mode: PrepareMode::Background,
        lane: LaneSpec::new(0, 1),
    };
    assert!(
        prepare_transfer_scoped(&mut b, &bp, &scheduler, &a.device, &selection, &context())
            .unwrap()
            .is_none()
    );
    let next = prepare_transfer_scoped(&mut b, &bp, &scheduler, &a.device, &selection, &context())
        .unwrap()
        .unwrap();
    assert!(!digests.contains(&next.item.edge.key.digest.to_string()));
}

#[test]
fn idle_transfer_scans_do_not_write_sqlite_after_the_initial_cursor() {
    use tkfs::model::{content_wire::PrepareMode, protocol::LaneSpec};
    let temp = tempfile::tempdir().unwrap();
    let (mut a, _) = store(temp.path());
    a.content_transfer_candidates("source", LaneSpec::new(0, 1), PrepareMode::Mixed)
        .unwrap();
    let before = a.db.total_changes();
    for _ in 0..50 {
        assert!(
            a.content_transfer_candidates("source", LaneSpec::new(0, 1), PrepareMode::Mixed)
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(
        before,
        a.db.total_changes(),
        "idle polling dirtied SQLite/WAL"
    );
}

#[test]
fn an_unaligned_one_mib_demand_reaches_its_seventeenth_chunk() {
    use tkfs::{
        content::{scheduler::ContentScheduler, work::prepare_transfer},
        model::workspace::{ByteRange, Demand, DemandPriority, FileRevision, WorkspaceId},
    };
    let temp = tempfile::tempdir().unwrap();
    let (mut a, ap) = store(&temp.path().join("a"));
    let (mut b, bp) = store(&temp.path().join("b"));
    let mut bytes = vec![0; 1024 * 1024 + 1];
    for (i, chunk) in bytes.chunks_mut(CHUNK_SIZE as usize).enumerate() {
        chunk.fill(i as u8);
    }
    let (reference, root) = publish(&mut a, &ap, "file", &bytes);
    metadata(&a, &mut b);
    let entity: String =
        b.db.query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?1",
            [&reference],
            |r| r.get(0),
        )
        .unwrap();
    let scheduler = Arc::new(ContentScheduler::default());
    let guard = scheduler
        .demand_on(
            bp.identity(),
            Demand {
                waiter: uuid::Uuid::new_v4(),
                reference: reference.clone(),
                revision: FileRevision {
                    workspace: WorkspaceId {
                        repository: b.repo.clone(),
                        workspace: uuid::Uuid::new_v4(),
                    },
                    entity,
                    generation: 1,
                    content: root.clone(),
                },
                range: ByteRange {
                    offset: 1,
                    len: 1024 * 1024,
                },
                priority: DemandPriority::Foreground,
                eligible_peers: vec![a.device.clone()],
            },
            &context(),
        )
        .unwrap();
    let mut out = vec![0; 1024 * 1024];
    let mut complete = false;
    for _ in 0..32 {
        if let Some(job) = prepare_transfer(&mut b, &bp, &scheduler, &a.device, &context()).unwrap()
        {
            let bytes = a
                .read_content_frame_at(&ap, &reference, &job.item.edge, &context())
                .unwrap()
                .1;
            job.complete(&mut b, &bp, &bytes, &context()).unwrap();
        }
        if bp
            .read_range(&bp.pin(&root).unwrap(), 1, &mut out, &context())
            .is_ok()
        {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "a verified prefix must not consume every preparation slice"
    );
    assert_eq!(out, &bytes[1..]);
    // A later real read invalidates precisely its missing object. Cached
    // availability must not hide a new failure or restart the verified prefix.
    let _ = prepare_transfer(&mut b, &bp, &scheduler, &a.device, &context()).unwrap();
    let tail = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: tkfs::model::content::Digest::bytes(&bytes[1024 * 1024..]),
    };
    std::fs::remove_file(b.root.join("objects").join(tail.digest.to_string())).unwrap();
    let error = bp
        .read_range(&bp.pin(&root).unwrap(), 1, &mut out, &context())
        .unwrap_err();
    assert_eq!(LocalContent::unavailable_key(&error), Some(tail));
    b.observe_content_unavailable(tail).unwrap();
    drop(guard);
    let entity: String =
        b.db.query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?1",
            [&reference],
            |r| r.get(0),
        )
        .unwrap();
    let retry = scheduler
        .demand_on(
            bp.identity(),
            Demand {
                waiter: uuid::Uuid::new_v4(),
                reference: reference.clone(),
                revision: FileRevision {
                    workspace: WorkspaceId {
                        repository: b.repo.clone(),
                        workspace: uuid::Uuid::new_v4(),
                    },
                    entity,
                    generation: 1,
                    content: root.clone(),
                },
                range: ByteRange {
                    offset: 1,
                    len: 1024 * 1024,
                },
                priority: DemandPriority::Foreground,
                eligible_peers: vec![a.device.clone()],
            },
            &context(),
        )
        .unwrap();
    let repair = prepare_transfer(&mut b, &bp, &scheduler, &a.device, &context())
        .unwrap()
        .unwrap();
    assert_eq!(repair.item.edge.key, tail);
    repair
        .complete(&mut b, &bp, &bytes[1024 * 1024..], &context())
        .unwrap();
    bp.read_range(&bp.pin(&root).unwrap(), 1, &mut out, &context())
        .unwrap();
    assert_eq!(out, &bytes[1..]);
    drop(retry);
    let root_key = ObjectKey::root(&root);
    std::fs::remove_file(b.root.join("manifests-v1").join(root.digest.to_string())).unwrap();
    let error = bp
        .read_range(&bp.pin(&root).unwrap(), 1, &mut out, &context())
        .unwrap_err();
    assert_eq!(LocalContent::unavailable_key(&error), Some(root_key));
    b.observe_content_unavailable(root_key).unwrap();
    assert!(
        !b.content_range_indexed(
            &reference,
            ByteRange {
                offset: 1,
                len: 1024 * 1024
            }
        )
        .unwrap()
    );
    let entity: String =
        b.db.query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?1",
            [&reference],
            |r| r.get(0),
        )
        .unwrap();
    let _page_retry = scheduler
        .demand_on(
            bp.identity(),
            Demand {
                waiter: uuid::Uuid::new_v4(),
                reference: reference.clone(),
                revision: FileRevision {
                    workspace: WorkspaceId {
                        repository: b.repo.clone(),
                        workspace: uuid::Uuid::new_v4(),
                    },
                    entity,
                    generation: 1,
                    content: root.clone(),
                },
                range: ByteRange {
                    offset: 1,
                    len: 1024 * 1024,
                },
                priority: DemandPriority::Foreground,
                eligible_peers: vec![a.device.clone()],
            },
            &context(),
        )
        .unwrap();
    let page = prepare_transfer(&mut b, &bp, &scheduler, &a.device, &context())
        .unwrap()
        .unwrap();
    assert_eq!(page.item.edge.key, root_key);
    let data = a
        .read_content_frame_at(&ap, &reference, &page.item.edge, &context())
        .unwrap()
        .1;
    page.complete(&mut b, &bp, &data, &context()).unwrap();
    bp.read_range(&bp.pin(&root).unwrap(), 1, &mut out, &context())
        .unwrap();
    assert_eq!(out, &bytes[1..]);
}
