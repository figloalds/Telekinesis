use serde_json::json;
use std::collections::BTreeSet;
use tkfs::{core::*, runtime::Engine};

fn store(path: &std::path::Path) -> Store {
    Store::open(path, Some("ack-regressions")).unwrap()
}
fn inventory(s: &Store) -> BTreeSet<String> {
    s.events()
        .unwrap()
        .into_iter()
        .filter(|e| e.branch.shared)
        .map(|e| e.id)
        .collect()
}
fn outgoing(s: &Store) -> i64 {
    s.status().unwrap()["outgoing_unacknowledged"]
        .as_i64()
        .unwrap()
}
fn status(engine: &mut Engine) -> serde_json::Value {
    engine.control(&id(), &json!({"op":"status"})).unwrap()
}
#[test]
fn durable_inventory_repairs_received_echoes_and_lost_acks_after_restart() {
    let adir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let mut a = store(adir.path());
    let mut b = store(bdir.path());
    let fa = a.create("from-a", "file").unwrap();
    a.write_revision(&fa.id, fa.revisions.get("content").cloned(), b"A")
        .unwrap();
    let fb = b.create("from-b", "file").unwrap();
    b.write_revision(&fb.id, fb.revisions.get("content").cloned(), b"B")
        .unwrap();
    let allowed = BTreeSet::from([a.device.clone(), b.device.clone()]);
    let a_initial = inventory(&a);
    let b_initial = inventory(&b);
    // Only A -> B connectivity works. B durably accepts A's events, but its
    // reply/explicit acknowledgements are lost. Reconnect must use inventory.
    b.receive(a.shared_bundle(&BTreeSet::new()).unwrap(), &allowed)
        .unwrap();
    drop(b);
    let mut b = store(bdir.path());
    b.remember_peer_inventory(&a.device, &a_initial).unwrap();
    assert_eq!(
        outgoing(&b),
        b_initial.len() as i64,
        "received events already durable on their sender must not remain queued"
    );
    assert_eq!(
        outgoing(&a),
        a_initial.len() as i64,
        "lost replies cannot acknowledge A's own queue"
    );
    // No explicit ack survives. The next complete inventory repairs both sides.
    a.receive(b.shared_bundle(&a_initial).unwrap(), &allowed)
        .unwrap();
    a.remember_peer_inventory(&b.device, &inventory(&b))
        .unwrap();
    b.remember_peer_inventory(&a.device, &inventory(&a))
        .unwrap();
    for _ in 0..3 {
        assert!(
            a.shared_bundle(&a.known_by_peer(&b.device).unwrap())
                .unwrap()
                .events
                .is_empty()
        );
        assert!(
            b.shared_bundle(&b.known_by_peer(&a.device).unwrap())
                .unwrap()
                .events
                .is_empty()
        );
        a.remember_peer_inventory(&b.device, &inventory(&b))
            .unwrap();
        b.remember_peer_inventory(&a.device, &inventory(&a))
            .unwrap();
        assert_eq!((outgoing(&a), outgoing(&b)), (0, 0));
    }
    drop(a);
    drop(b);
    let a = store(adir.path());
    let b = store(bdir.path());
    assert_eq!((outgoing(&a), outgoing(&b)), (0, 0));
    assert_eq!(
        a.projection(&a.active).unwrap(),
        b.projection(&b.active).unwrap()
    );
    assert_eq!(inventory(&a).len(), a_initial.len() + b_initial.len());
    assert_eq!(a.read_object(&hash(b"B")).unwrap(), b"B");
    assert_eq!(b.read_object(&hash(b"A")).unwrap(), b"A");
}
#[test]
fn caught_up_checks_current_queue_inventory_and_unflushed_work() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::new(store(dir.path()));
    let file = e.store.create("code", "file").unwrap();
    e.sync_peers.insert("configured-peer".into());
    let known = inventory(&e.store);
    e.store
        .acknowledge(&known.iter().cloned().collect::<Vec<_>>())
        .unwrap();
    e.store
        .remember_peer_inventory("configured-peer", &known)
        .unwrap();
    e.last_sync = Some(json!({"caught_up":true}));
    assert_eq!(status(&mut e)["caught_up"], true);
    let (h, _) = e.open("code", None, true).unwrap();
    e.write(h, 0, b"new durable work", false, false).unwrap();
    assert_eq!(
        status(&mut e)["caught_up"],
        false,
        "a cached roundtrip cannot cover unflushed bytes"
    );
    e.flush(h).unwrap();
    e.close(h).unwrap();
    assert_eq!(status(&mut e)["caught_up"], false);
    assert_eq!(status(&mut e)["upload_state"], "queued");
    e.store
        .remember_peer_inventory("configured-peer", &inventory(&e.store))
        .unwrap();
    assert_eq!(status(&mut e)["caught_up"], true);
    // A durable peer restore may withdraw an event previously acknowledged.
    // The last successful boolean and historical ack bit are insufficient.
    e.store
        .remember_peer_inventory("configured-peer", &known)
        .unwrap();
    assert_eq!(status(&mut e)["caught_up"], false);
    assert!(outgoing(&e.store) > 0);
    assert_eq!(status(&mut e)["upload_state"], "queued");
    assert_eq!(e.store.lookup("code").unwrap().id, file.id);
}

#[test]
fn object_inventory_pending_quarantine_and_private_ids_are_not_event_receipts() {
    let adir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let mut a = store(adir.path());
    let mut b = store(bdir.path());
    let main = a.active.clone();
    let file = a.create("code", "file").unwrap();
    a.write_revision(&file.id, file.revisions.get("content").cloned(), b"shared")
        .unwrap();
    let shared = inventory(&a);
    let allowed = BTreeSet::from([a.device.clone(), b.device.clone()]);
    let private = a.fork("private", false).unwrap();
    a.checkout(&private.id).unwrap();
    let canary = a.create("canary", "file").unwrap();
    a.write_revision(
        &canary.id,
        canary.revisions.get("content").cloned(),
        b"PRIVATE-ACK-CANARY",
    )
    .unwrap();
    a.checkout(&main).unwrap();
    let mut missing = a.shared_bundle(&BTreeSet::new()).unwrap();
    let objects = missing.objects.keys().cloned().collect();
    missing.objects.clear();
    assert!(b.receive(missing, &allowed).unwrap().is_empty());
    b.remember_peer_inventory(&a.device, &shared).unwrap();
    assert!(inventory(&b).is_empty());
    assert_eq!(b.status().unwrap()["incoming_pending"], 2);
    assert_eq!(outgoing(&b), 0);
    a.remember_peer_objects(&b.device, &objects).unwrap();
    assert_eq!(
        outgoing(&a),
        2,
        "object receipts cannot acknowledge metadata"
    );
    b.receive(a.shared_bundle(&BTreeSet::new()).unwrap(), &allowed)
        .unwrap();
    b.remember_peer_inventory(&a.device, &shared).unwrap();
    assert_eq!(outgoing(&b), 0);
    let invalid = a.event(
        &a.branch(&main).unwrap(),
        vec![
            a.current_change(&file.id, "kind", json!("bad-kind"))
                .unwrap(),
        ],
    );
    let mut advertised = shared.clone();
    advertised.insert(invalid.id.clone());
    assert!(
        b.receive(
            Bundle {
                repo: b.repo.clone(),
                branches: vec![],
                events: vec![invalid.clone()],
                objects: Default::default()
            },
            &allowed
        )
        .unwrap()
        .is_empty()
    );
    b.remember_peer_inventory(&a.device, &advertised).unwrap();
    b.acknowledge(&[invalid.id]).unwrap();
    assert_eq!(b.status().unwrap()["incoming_quarantined"], 1);
    assert_eq!(inventory(&b), shared);
    assert_eq!(outgoing(&b), 0);
    advertised.extend(
        a.events()
            .unwrap()
            .iter()
            .filter(|e| !e.branch.shared)
            .map(|e| e.id.clone()),
    );
    a.remember_peer_inventory(&b.device, &advertised).unwrap();
    a.acknowledge(&advertised.into_iter().collect::<Vec<_>>())
        .unwrap();
    assert_eq!(outgoing(&a), 0);
    assert!(b.branch(&private.id).is_err());
    assert!(b.read_object(&hash(b"PRIVATE-ACK-CANARY")).is_err());
    assert!(
        !a.shared_object_inventory()
            .unwrap()
            .contains(&hash(b"PRIVATE-ACK-CANARY"))
    );
    assert!(a.shared_bundle(&shared).unwrap().events.is_empty());
}
