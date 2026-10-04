use serde_json::json;
use std::collections::BTreeSet;
use tkfs::{
    core::*,
    paired_sync::{self, SyncPage},
    pairing::SyncConfiguration,
    runtime::Engine,
};
fn fixture() -> (
    tempfile::TempDir,
    Engine,
    Engine,
    SyncConfiguration,
    SyncConfiguration,
) {
    let temp = tempfile::tempdir().unwrap();
    let repo = id();
    let da = id();
    let db = id();
    let a = Engine::new(Store::initialize(&temp.path().join("a"), &repo, &da).unwrap());
    let b = Engine::new(Store::initialize(&temp.path().join("b"), &repo, &db).unwrap());
    let ga = SyncConfiguration {
        peer: id(),
        repo: repo.clone(),
        local_runtime: temp.path().join("a/runtime.json"),
        local_replica: da.clone(),
        remote_replica: db.clone(),
        enabled: true,
    };
    let gb = SyncConfiguration {
        peer: id(),
        repo,
        local_runtime: temp.path().join("b/runtime.json"),
        local_replica: db,
        remote_replica: da,
        enabled: true,
    };
    (temp, a, b, ga, gb)
}
fn write(engine: &mut Engine, name: &str, bytes: &[u8]) {
    let file = engine.store.create(name, "file").unwrap();
    engine.store.write_revision(&file.id, None, bytes).unwrap();
}
fn roundtrip(
    a: &mut Engine,
    b: &mut Engine,
    ga: &SyncConfiguration,
    gb: &SyncConfiguration,
) -> serde_json::Value {
    let page = paired_sync::offer(a, ga, None).unwrap();
    let sent: BTreeSet<_> = page
        .bundle
        .events
        .iter()
        .map(|event| event.id.clone())
        .collect();
    let response = paired_sync::worker_control(
        b,
        &json!({"op":"published-exchange","grant":gb,"page":page}),
    )
    .unwrap();
    paired_sync::worker_control(a,&json!({"op":"published-apply","grant":ga,"page":response,"request":page.request,"sent":sent})).unwrap()
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
    let ack = paired_sync::receive(&mut b, &gb, &wrong).unwrap();
    assert!(ack.len() < wrong.bundle.events.len());
    assert!(!b.store.shared_object_inventory().unwrap().contains(&secret));
    assert!(b.store.lookup("public.txt").is_err());
}
#[test]
fn missing_object_is_not_acked_and_unbound_or_early_ack_cannot_clear_outbox() {
    let (_temp, mut a, mut b, ga, gb) = fixture();
    write(&mut a, "large.txt", b"whole object required");
    let mut page = paired_sync::offer(&a, &ga, None).unwrap();
    page.bundle.objects.clear();
    page.object_parts.clear();
    let response: SyncPage = serde_json::from_value(
        paired_sync::worker_control(
            &mut b,
            &json!({"op":"published-exchange","grant":gb,"page":page}),
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
    assert!(!response.ack.contains(&content_event.id));
    assert!(!response.known.contains(&content_event.id));
    let original = a.store.outgoing_unacknowledged().unwrap();
    let mut wrong = response.clone();
    wrong.ack.push(content_event.id.clone());
    assert!(paired_sync::worker_control(&mut a,&json!({"op":"published-apply","grant":ga,"page":wrong,"request":page.request,"sent":[content_event.id]})).is_err());
    assert_eq!(a.store.outgoing_unacknowledged().unwrap(), original);
    let mut wrong = response;
    wrong.reply_to = Some(id());
    assert!(paired_sync::worker_control(&mut a,&json!({"op":"published-apply","grant":ga,"page":wrong,"request":page.request,"sent":[]})).is_err());
    assert_eq!(a.store.outgoing_unacknowledged().unwrap(), original);
}
