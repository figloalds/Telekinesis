use serde_json::json;
use std::collections::BTreeSet;
use tkfs::{
    core::*,
    paired_sync::SyncRoute,
    paired_sync::{self, SyncPage},
    runtime::Engine,
};
fn fixture() -> (tempfile::TempDir, Engine, Engine, SyncRoute, SyncRoute) {
    let temp = tempfile::tempdir().unwrap();
    let repo = id();
    let da = id();
    let db = id();
    let a = Engine::new(Store::initialize(&temp.path().join("a"), &repo, &da).unwrap());
    let b = Engine::new(Store::initialize(&temp.path().join("b"), &repo, &db).unwrap());
    let ga = SyncRoute {
        peer: id(),
        repo: repo.clone(),
        local_replica: da.clone(),
        remote_replica: db.clone(),
    };
    let gb = SyncRoute {
        peer: id(),
        repo,
        local_replica: db,
        remote_replica: da,
    };
    (temp, a, b, ga, gb)
}
fn write(engine: &mut Engine, name: &str, bytes: &[u8]) {
    let file = engine.store.create(name, "file").unwrap();
    engine
        .store
        .write_revision(&file.id, file.revisions.get("content").cloned(), bytes)
        .unwrap();
}

#[test]
fn legacy_missing_size_does_not_block_status_or_metadata_and_content_can_recover() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    write(&mut a, "legacy.txt", b"legacy content");
    let object = hash(b"legacy content");
    // Reproduce a pre-descriptor journal whose original content is unavailable.
    for mut event in a.store.events().unwrap() {
        event.object_sizes.clear();
        a.store
            .db
            .execute(
                "UPDATE events SET payload=? WHERE id=?",
                rusqlite::params![serde_json::to_string(&event).unwrap(), event.id],
            )
            .unwrap();
    }
    a.store.db.execute("DELETE FROM object_sizes", []).unwrap();
    std::fs::remove_file(a.store.root.join("objects").join(&object)).unwrap();
    let status = a.control("status", &json!({"op":"status"})).unwrap();
    assert_eq!(status["content"]["current_missing"], 1);
    assert_eq!(
        status["content"]["items"][0]["size"],
        serde_json::Value::Null
    );
    assert_eq!(status["content"]["items"][0]["size_known"], false);
    let before = a.store.events().unwrap();
    let page = paired_sync::worker_control(
        &mut a,
        &json!({"op":"published-offer","route":ga,"metadata_only":true}),
    )
    .unwrap();
    assert!(page["object_sizes"].get(&object).is_none());
    paired_sync::worker_control(
        &mut b,
        &json!({"op":"published-exchange","route":gb,"page":page,"metadata_only":true}),
    )
    .unwrap();
    assert!(b.store.lookup("legacy.txt").is_ok());
    assert_eq!(b.store.object_size_if_known(&object).unwrap(), None);
    assert_eq!(b.store.events().unwrap(), before);
    assert_eq!(
        roundtrip(&mut a, &mut b, &ga, &gb)["metadata_caught_up"],
        true
    );
    assert_eq!(b.store.content_status().unwrap()["complete"], false);
    a.store.put_object(b"legacy content").unwrap();
    roundtrip(&mut a, &mut b, &ga, &gb);
    assert_eq!(b.store.read_object(&object).unwrap(), b"legacy content");
    assert_eq!(
        b.store.events().unwrap(),
        before,
        "descriptors must not rewrite journal events"
    );
}

#[test]
fn bounded_metadata_pages_make_progress_with_reverse_sorted_dependencies() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    a.store.create("chain", "file").unwrap();
    let mut base = a.store.events().unwrap().pop().unwrap();
    // Explicit descending IDs force children before parents in UUID/string order.
    let mut parent = base.id.clone();
    let entity = base.changes[0].entity.clone();
    for index in (0..24).rev() {
        base.id = format!("00000000-0000-4000-8000-{index:012}");
        base.dependencies = vec![parent.clone()];
        base.changes = vec![Change {
            entity: entity.clone(),
            field: "alive".into(),
            parents: vec![parent],
            value: json!(true),
        }];
        base.object_sizes.clear();
        a.store.commit(base.clone(), true).unwrap();
        parent = base.id.clone();
    }
    let mut rounds = 0;
    while a.store.shared_event_inventory().unwrap() != b.store.shared_event_inventory().unwrap() {
        let known = b.store.shared_event_inventory().unwrap();
        let bundle = a.store.shared_metadata_page(&known, 6000).unwrap();
        assert!(!bundle.events.is_empty());
        let mut page: SyncPage = serde_json::from_value(
            paired_sync::worker_control(
                &mut a,
                &json!({"op":"published-offer","route":ga,"metadata_only":true}),
            )
            .unwrap(),
        )
        .unwrap();
        let referenced: BTreeSet<_> = bundle.events.iter().flat_map(Event::objects).collect();
        page.object_sizes.retain(|h, _| referenced.contains(h));
        page.bundle = bundle;
        paired_sync::receive(&mut b, &gb, &page).unwrap();
        assert!(
            b.store.shared_event_inventory().unwrap().len() > known.len(),
            "each page must activate events"
        );
        assert_eq!(b.store.status().unwrap()["incoming_pending"], 0);
        rounds += 1;
        assert!(rounds <= 25);
    }
    assert!(rounds > 1, "fixture must exercise pagination");
    assert_eq!(
        a.store.projection(&a.store.active).unwrap(),
        b.store.projection(&b.store.active).unwrap()
    );
}
fn roundtrip(a: &mut Engine, b: &mut Engine, ga: &SyncRoute, gb: &SyncRoute) -> serde_json::Value {
    let page = paired_sync::offer(a, ga, None).unwrap();
    let sent: BTreeSet<_> = page
        .bundle
        .events
        .iter()
        .map(|event| event.id.clone())
        .collect();
    let response = paired_sync::worker_control(
        b,
        &json!({"op":"published-exchange","route":gb,"page":page}),
    )
    .unwrap();
    paired_sync::worker_control(a,&json!({"op":"published-apply","route":ga,"page":response,"request":page.request,"sent":sent})).unwrap()
}
#[test]
fn published_only_sync_is_idempotent_and_durable_after_restart() {
    let (temp, mut a, mut b, ga, gb) = fixture();
    write(&mut a, "public.txt", b"shared");
    a.store.fork("secret", false).unwrap();
    a.store.checkout("secret").unwrap();
    write(&mut a, "private.txt", b"private payload");
    let page = paired_sync::offer(&a, &ga, None).unwrap();
    assert!(page.bundle.branches.iter().all(|branch| branch.shared));
    assert!(!page.known_objects.contains(&hash(b"private payload")));
    assert!(!page.bundle.objects.contains_key(&hash(b"private payload")));
    roundtrip(&mut a, &mut b, &ga, &gb);
    roundtrip(&mut a, &mut b, &ga, &gb);
    assert!(b.store.lookup("public.txt").is_ok());
    assert!(b.store.lookup("private.txt").is_err());
    let count = b.store.events().unwrap().len();
    drop(b);
    let mut b = Engine::new(Store::open_existing(&temp.path().join("b")).unwrap());
    assert!(
        roundtrip(&mut a, &mut b, &ga, &gb)["caught_up"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(b.store.events().unwrap().len(), count);
    write(&mut b, "other.txt", b"reverse direction");
    roundtrip(&mut a, &mut b, &ga, &gb);
    a.store.checkout("main").unwrap();
    assert!(a.store.lookup("other.txt").is_ok());
}
#[test]
fn wrong_repo_replica_forged_origin_and_private_object_requests_fail() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    write(&mut a, "public.txt", b"shared");
    let page = paired_sync::offer(&a, &ga, None).unwrap();
    let mut wrong = page.clone();
    wrong.sender = id();
    assert!(paired_sync::receive(&mut b, &gb, &wrong).is_err());
    let mut wrong = page.clone();
    wrong.repo = id();
    assert!(paired_sync::receive(&mut b, &gb, &wrong).is_err());
    let mut wrong = page.clone();
    wrong.bundle.events[0].device = gb.local_replica.clone();
    assert!(
        paired_sync::receive(&mut b, &gb, &wrong)
            .unwrap_err()
            .to_string()
            .contains("FORGED_EVENT_ORIGIN")
    );
    let secret = b.store.put_object(b"private-only local CAS").unwrap();
    let mut wrong = page.clone();
    wrong
        .bundle
        .objects
        .insert(secret.clone(), hex::encode(b"private-only local CAS"));
    assert!(
        paired_sync::receive(&mut b, &gb, &wrong)
            .unwrap_err()
            .to_string()
            .contains("UNAUTHORIZED_OBJECT")
    );
    assert!(!b.store.shared_object_inventory().unwrap().contains(&secret));
    let mut wrong = page.clone();
    wrong.bundle.events.iter_mut().for_each(|event| {
        for change in &mut event.changes {
            if change.field == "content" {
                change.value = json!(secret);
            }
        }
    });
    wrong.bundle.objects.clear();
    wrong.object_sizes.clear();
    for event in &mut wrong.bundle.events {
        event.object_sizes.clear();
    }
    let ack = paired_sync::receive(&mut b, &gb, &wrong).unwrap();
    assert_eq!(ack.len(), wrong.bundle.events.len());
    assert!(!b.store.shared_object_inventory().unwrap().contains(&secret));
    assert!(b.store.lookup("public.txt").is_ok());
}
#[test]
fn metadata_is_acked_without_content_but_unbound_ack_cannot_clear_outbox() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    write(&mut a, "large.txt", b"whole object required");
    let mut page = paired_sync::offer(&a, &ga, None).unwrap();
    page.bundle.objects.clear();
    page.object_parts.clear();
    let response: SyncPage = serde_json::from_value(
        paired_sync::worker_control(
            &mut b,
            &json!({"op":"published-exchange","route":gb,"page":page}),
        )
        .unwrap(),
    )
    .unwrap();
    let content_event = page
        .bundle
        .events
        .iter()
        .find(|event| !event.objects().is_empty())
        .unwrap();
    assert!(response.ack.contains(&content_event.id));
    assert!(response.known.contains(&content_event.id));
    assert!(response.known_objects.is_empty());
    let original = a.store.outgoing_unacknowledged().unwrap();
    let mut wrong = response.clone();
    wrong.ack.push(id());
    assert!(paired_sync::worker_control(&mut a,&json!({"op":"published-apply","route":ga,"page":wrong,"request":page.request,"sent":[content_event.id]})).is_err());
    assert_eq!(a.store.outgoing_unacknowledged().unwrap(), original);
    let mut wrong = response;
    wrong.reply_to = Some(id());
    assert!(paired_sync::worker_control(&mut a,&json!({"op":"published-apply","route":ga,"page":wrong,"request":page.request,"sent":[]})).is_err());
    assert_eq!(a.store.outgoing_unacknowledged().unwrap(), original);
}

#[test]
fn reserved_lane_transports_events_without_content_or_rewinding_bulk_cursors() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    write(&mut a, "payload.bin", &vec![7; 2 * 1024 * 1024]);
    b.store.create("remote-directory", "directory").unwrap();
    let page: SyncPage = serde_json::from_value(paired_sync::worker_control(
        &mut a,
        &json!({"op":"published-offer","route":ga,"metadata_only":true,"max_frame":4*1024*1024}),
    ).unwrap()).unwrap();
    assert!(!page.bundle.events.is_empty());
    assert!(page.bundle.objects.is_empty());
    assert!(page.object_parts.is_empty());
    let response: SyncPage = serde_json::from_value(
        paired_sync::worker_control(
            &mut b,
            &json!({"op":"published-exchange","route":gb,"page":page,"metadata_only":true}),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(response.bundle.objects.is_empty());
    assert!(response.object_parts.is_empty());
    assert_eq!(response.pending_events, 0);
    assert!(b.store.lookup("payload.bin").is_ok());
    assert_eq!(b.store.content_status().unwrap()["complete"], false);
    for event in page
        .bundle
        .events
        .iter()
        .filter(|e| !e.objects().is_empty())
    {
        assert!(response.ack.contains(&event.id));
        assert!(response.known.contains(&event.id));
    }
    let object = hash(&vec![7; 2 * 1024 * 1024]);
    let offsets = [(object.clone(), 1024)].into_iter().collect();
    a.store
        .remember_peer_partials(&ga.remote_replica, &offsets)
        .unwrap();
    let sent: BTreeSet<_> = page.bundle.events.iter().map(|e| e.id.clone()).collect();
    let result = paired_sync::worker_control(
        &mut a,
        &json!({
            "op":"published-apply","route":ga,"page":response,"request":page.request,
            "sent":sent,"metadata_only":true
        }),
    )
    .unwrap();
    assert_eq!(result["metadata_caught_up"], true);
    assert!(a.store.lookup("remote-directory").is_ok());
    assert_eq!(
        a.store.peer_partial_objects(&ga.remote_replica).unwrap(),
        offsets
    );

    // Clear the synthetic cursor, then let the bulk transport finish normally.
    a.store
        .remember_peer_partials(&ga.remote_replica, &Default::default())
        .unwrap();
    for _ in 0..4 {
        roundtrip(&mut a, &mut b, &ga, &gb);
    }
    assert_eq!(
        b.store.read_object(&object).unwrap(),
        vec![7; 2 * 1024 * 1024]
    );
    assert_eq!(a.store.outgoing_unacknowledged().unwrap(), 0);
}

#[test]
fn reserved_lane_rejects_bulk_before_applying_events() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    write(&mut a, "file", b"bulk payload");
    let page = paired_sync::offer(&a, &ga, None).unwrap();
    assert!(!page.bundle.objects.is_empty());
    let error = paired_sync::worker_control(
        &mut b,
        &json!({
            "op":"published-exchange","route":gb,"page":page,"metadata_only":true
        }),
    )
    .unwrap_err();
    assert!(error.to_string().contains("BULK_ON_CONTROL_LANE"));
    assert!(b.store.lookup("file").is_err());
}

#[test]
fn apply_requires_an_explicit_valid_request_binding() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    write(
        &mut b,
        "remote.txt",
        b"must not be applied without a request",
    );
    let page = paired_sync::offer(&b, &gb, None).unwrap();
    let before = a.store.events().unwrap();
    for request in [
        serde_json::Value::Null,
        json!(42),
        json!("not-a-uuid"),
        json!(id()),
    ] {
        assert!(
            paired_sync::worker_control(
                &mut a,
                &json!({
                    "op":"published-apply", "route":ga, "page":page,
                    "request":request, "sent":[]
                })
            )
            .is_err()
        );
        assert_eq!(a.store.events().unwrap(), before);
    }
}

#[test]
fn invalid_inventory_cannot_apply_events_or_change_durable_receipts() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    write(&mut a, "local.txt", b"local unacknowledged save");
    write(&mut b, "remote.txt", b"remote save");
    let mut page = paired_sync::offer(&b, &gb, None).unwrap();
    page.known = a.store.shared_event_inventory().unwrap();
    let before = a.store.events().unwrap();
    let outbox = a.store.outgoing_unacknowledged().unwrap();
    let mut malformed_hash = page.clone();
    malformed_hash.known_objects.insert("invalid".into());
    let mut malformed_partial = page.clone();
    malformed_partial
        .partial_objects
        .insert("invalid".into(), 0);
    let mut oversized_partial = page.clone();
    oversized_partial
        .partial_objects
        .insert(hash(b"partial"), MAX_CONTENT + 1);
    let mut malformed_reply = page;
    malformed_reply.reply_to = Some("invalid".into());
    for invalid in [
        malformed_hash,
        malformed_partial,
        oversized_partial,
        malformed_reply,
    ] {
        assert!(paired_sync::receive(&mut a, &ga, &invalid).is_err());
        assert_eq!(a.store.events().unwrap(), before);
        assert_eq!(a.store.outgoing_unacknowledged().unwrap(), outbox);
        assert!(
            a.store
                .known_by_peer(&ga.remote_replica)
                .unwrap()
                .is_empty()
        );
        assert!(
            a.store
                .known_objects_by_peer(&ga.remote_replica)
                .unwrap()
                .is_empty()
        );
    }
}
