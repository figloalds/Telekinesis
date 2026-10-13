//! Qualification scenarios for WP1.8 (object indexes, startup markers,
//! per-object outcomes). REVAMP_PLAN §14.6; floors in §15 and
//! `tests/qualification-floors.json`.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP1.8"]`.
//! `cargo test --release --test qual_wp1_8_objects -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use support::*;
use tkfs::core::{ObjectPart, Store, hash, id};

const FILE: &str = "qual_wp1_8_objects";

/// A page whose middle part is corrupt keeps the completed objects before and
/// after it (registered and shareable), instead of discarding the whole page.
#[test]
#[ignore = "WP1.8"]
fn corrupt_mid_page_keeps_completed() {
    let mut s = Scenario::new(FILE, "corrupt_mid_page_keeps_completed");
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let mut b = Store::open(&dir.path().join("b"), Some(&repo)).unwrap();
    let allowed = devices([&a, &b]);
    let mut contents = BTreeMap::new();
    for i in 0..3u64 {
        let bytes = seeded_bytes(100 + i, 64 * 1024);
        let e = a.create(&format!("part-{i}.bin"), "file").unwrap();
        a.write_revision(&e.id, e.revisions.get("content").cloned(), &bytes)
            .unwrap();
        contents.insert(hash(&bytes), bytes);
    }
    b.receive(metadata_bundle(&a, &b).unwrap(), &allowed)
        .unwrap();
    let hashes: Vec<String> = contents.keys().cloned().collect();
    let parts: BTreeMap<String, ObjectPart> = hashes
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let mut bytes = contents[h].clone();
            if i == 1 {
                bytes[1000] ^= 0xff;
            }
            (
                h.clone(),
                ObjectPart {
                    offset: 0,
                    total: bytes.len() as u64,
                    hex: hex::encode(&bytes),
                },
            )
        })
        .collect();
    let result = b.receive_parts(parts, &[], &allowed);
    s.note(format!("receive_parts: {result:?}"));
    let shared = b.shared_object_inventory().unwrap();
    s.check(
        shared.contains(&hashes[0]),
        "object before the corrupt part kept",
    );
    s.check(
        shared.contains(&hashes[2]),
        "object after the corrupt part kept",
    );
    s.check(
        !shared.contains(&hashes[1]),
        "corrupt object not registered",
    );
    s.check(
        !b.object_available(&hashes[1]),
        "corrupt object not installed",
    );
    s.metric(
        "kept",
        [&hashes[0], &hashes[2]]
            .iter()
            .filter(|h| shared.contains(**h))
            .count() as f64,
        "objects",
    );
    s.finish();
}

/// Restart to Ready with 20K stored objects performs no re-hash.
#[test]
#[ignore = "WP1.8"]
fn restart_without_rehash_20k() {
    let mut s = Scenario::new(FILE, "restart_without_rehash_20k");
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    {
        let mut store = Store::initialize(&state, &id(), &id()).unwrap();
        let (_, ms) =
            time_ms(|| small_objects_in(&mut store, "objects", scaled(20_000), 1024, 3).unwrap());
        s.note(format!("{} objects stored in {ms:.0} ms", scaled(20_000)));
    }
    // First start may verify once (after an upgrade, or objects written without markers).
    let mut first = WorkerProc::spawn(&state, &[], &[]).unwrap();
    let first_ready = first.wait_ready(Duration::from_secs(600));
    s.check(first_ready.is_ok(), format!("first start: {first_ready:?}"));
    s.metric(
        "first_ready_ms",
        first.spawned_at.elapsed().as_secs_f64() * 1000.0,
        "ms",
    );
    let _ = first.stop();
    s.measure_from_now();
    let mut second = WorkerProc::spawn(&state, &[], &[]).unwrap();
    match second.wait_ready(s.budget) {
        Ok(_) => {
            s.metric(
                "ready_ms",
                second.spawned_at.elapsed().as_secs_f64() * 1000.0,
                "ms",
            );
        }
        Err(e) => s.timeout(format!("second start: {e:#}")),
    }
    let _ = second.stop();
    s.finish();
}

/// 12,000 tiny objects pending on the receiver produce valid bulk pages and all
/// arrive (baseline: a page above the 10,000-part cap is rejected).
#[test]
#[ignore = "WP1.8"]
fn tiny_pending_objects_12k() {
    let mut s = Scenario::new(FILE, "tiny_pending_objects_12k");
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let (a_state, b_state) = (dir.path().join("a"), dir.path().join("b"));
    let count = scaled(12_000);
    let (a_device, b_device) = {
        let mut a = Store::initialize(&a_state, &repo, &id()).unwrap();
        small_objects_in(&mut a, "tiny", count, 64, 5).unwrap();
        // B has committed the metadata but holds none of the objects.
        let (a, b) = metadata_replica(a, &b_state).unwrap();
        (a.device.clone(), b.device.clone())
    };
    let mut wa = WorkerProc::spawn(&a_state, &[], &[]).unwrap();
    let mut wb = WorkerProc::spawn(&b_state, &[], &[]).unwrap();
    wa.wait_ready(Duration::from_secs(300)).unwrap();
    wb.wait_ready(Duration::from_secs(300)).unwrap();
    let link = WorkerLink::new(&wb, &wa, &repo, &b_device, &a_device);
    // A must learn that B committed the events before it offers their objects.
    for _ in 0..3 {
        link.metadata_round().unwrap();
    }
    s.measure_from_now();
    let lane = json!({"index": 0, "count": 1});
    let mut inventory = Value::Null;
    let mut received = 0u64;
    let mut error = None;
    let watch = Stopwatch::start();
    let complete = |w: &WorkerProc| {
        w.rpc(json!({"op":"status"}))
            .is_ok_and(|s| s["content"]["complete"] == true)
    };
    while !complete(&wb) {
        if s.over_budget() {
            s.timeout("objects still missing");
            break;
        }
        match link.bulk_round(&lane, &mut inventory) {
            Ok((bytes, _, _)) => received += bytes,
            Err(e) => {
                error = Some(format!("{e:#}"));
                break;
            }
        }
    }
    let available = wb
        .rpc(json!({"op":"status"}))
        .map(|s| s["content"]["shared_available"].clone())
        .unwrap_or(Value::Null);
    let done = complete(&wb);
    s.metric_with(
        "completed",
        if done { 1.0 } else { 0.0 },
        "bool",
        json!({"objects": count, "available": available, "error": error,
            "received_bytes": received}),
    );
    s.metric(
        "objects_per_s",
        available.as_f64().unwrap_or(0.0) / watch.elapsed().as_secs_f64().max(1e-3),
        "objects/s",
    );
    s.check(error.is_none(), format!("bulk error: {error:?}"));
    s.finish();
}

/// `content_status` with 50K current objects.
#[test]
#[ignore = "WP1.8"]
fn content_status_50k() {
    let mut s = Scenario::new(FILE, "content_status_50k");
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some(&id())).unwrap();
    let (_, ms) =
        time_ms(|| small_objects_in(&mut store, "objects", scaled(50_000), 32, 9).unwrap());
    s.note(format!("{} objects stored in {ms:.0} ms", scaled(50_000)));
    s.measure_from_now();
    let mut samples = vec![];
    for _ in 0..5 {
        if s.over_budget() {
            break;
        }
        let (status, ms) = time_ms(|| store.content_status().unwrap());
        s.check(status["complete"] == true, "content complete");
        samples.push(ms);
    }
    let summary = summarize(&samples);
    s.metric_with("content_status_ms", summary.p50, "ms", summary.json());
    s.finish();
}
