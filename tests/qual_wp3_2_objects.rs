//! Qualification scenarios for WP3.2 (objects v3, worker side).
//! REVAMP_PLAN §14.6, §6.4.
//!
//! Created by WP0.5, `#[ignore = "WP3.2"]`. Two real worker processes; the
//! baseline pump is the legacy bulk path (`published-bulk-offer/exchange/apply`,
//! bulk protocol 1) driven exactly as an orchestrator lane drives it. WP3.2
//! replaces `pump` with the `objects.want/read/write` loopback-session pump.
//! `cargo test --release --test qual_wp3_2_objects -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{fs, path::Path, time::Duration};
use support::*;
use tkfs::core::{Store, id};

const FILE: &str = "qual_wp3_2_objects";

struct Setup {
    dir: tempfile::TempDir,
    repo: String,
    a_device: String,
    b_device: String,
}

/// A holds the objects written by `author`; B has the metadata only.
fn setup(author: impl FnOnce(&mut Store)) -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::initialize(&dir.path().join("a"), &repo, &id()).unwrap();
    author(&mut a);
    // B has committed the metadata but holds none of the objects.
    let (a, b) = metadata_replica(a, &dir.path().join("b")).unwrap();
    Setup {
        repo,
        a_device: a.device.clone(),
        b_device: b.device.clone(),
        dir,
    }
}

fn complete(w: &WorkerProc) -> bool {
    w.rpc(json!({"op":"status"}))
        .is_ok_and(|s| s["content"]["complete"] == true)
}

/// Pump objects from A to B until B is complete, an error, or `deadline`.
/// Returns the object bytes B received.
fn pump(link: &WorkerLink, s: &Scenario, every: usize) -> anyhow::Result<u64> {
    for _ in 0..3 {
        link.metadata_round()?;
    }
    let lane = json!({"index": 0, "count": 1});
    let mut inventory = Value::Null;
    let mut received = 0;
    let mut rounds = 0;
    loop {
        if rounds % every == 0 && complete(link.dialer) {
            return Ok(received);
        }
        if s.over_budget() {
            return Ok(received);
        }
        let (bytes, _, _) = link.bulk_round(&lane, &mut inventory)?;
        received += bytes;
        rounds += 1;
    }
}

fn workers(setup: &Setup) -> (WorkerProc, WorkerProc) {
    let mut a = WorkerProc::spawn(&setup.dir.path().join("a"), &[], &[]).unwrap();
    let mut b = WorkerProc::spawn(&setup.dir.path().join("b"), &[], &[]).unwrap();
    a.wait_ready(Duration::from_secs(600)).unwrap();
    b.wait_ready(Duration::from_secs(600)).unwrap();
    (a, b)
}

/// One 256 MiB object worker to worker over loopback ≥ 100 MiB/s.
#[test]
#[ignore = "WP3.2"]
fn object_256mib_worker_to_worker() {
    let mut s = Scenario::new(FILE, "object_256mib_worker_to_worker");
    let size = scaled(256) as u64 * 1024 * 1024;
    let setup = setup(|a| {
        large_object_in(a, "big.bin", size, 11).unwrap();
    });
    let (wa, wb) = workers(&setup);
    let link = WorkerLink::new(&wb, &wa, &setup.repo, &setup.b_device, &setup.a_device);
    s.measure_from_now();
    let result = pump(&link, &s, 8);
    let secs = s.started.elapsed().as_secs_f64();
    let done = complete(&wb);
    let received = result.as_ref().copied().unwrap_or(0);
    s.metric_with(
        "mib_per_s",
        received as f64 / 1048576.0 / secs,
        "MiB/s",
        json!({"bytes": size, "received": received, "complete": done}),
    );
    s.check(result.is_ok(), format!("pump: {result:?}"));
    if !done {
        s.timeout(format!("{received} of {size} bytes in budget"));
    }
    s.finish();
}

/// 10K tiny objects (64 B) ≥ 300 objects/s.
#[test]
#[ignore = "WP3.2"]
fn tiny_objects_10k() {
    let mut s = Scenario::new(FILE, "tiny_objects_10k");
    let count = scaled(10_000);
    let setup = setup(|a| {
        small_objects_in(a, "tiny", count, 64, 12).unwrap();
    });
    let (wa, wb) = workers(&setup);
    let link = WorkerLink::new(&wb, &wa, &setup.repo, &setup.b_device, &setup.a_device);
    s.measure_from_now();
    let result = pump(&link, &s, 4);
    let secs = s.started.elapsed().as_secs_f64();
    let available = wb
        .rpc(json!({"op":"status"}))
        .map(|st| st["content"]["shared_available"].as_f64().unwrap_or(0.0))
        .unwrap_or(0.0);
    s.metric_with(
        "objects_per_s",
        available / secs,
        "objects/s",
        json!({"objects": count, "available": available}),
    );
    s.check(result.is_ok(), format!("pump: {result:?}"));
    if !complete(&wb) {
        s.timeout(format!("{available} of {count} objects in budget"));
    }
    s.finish();
}

fn spool_bytes(state: &Path) -> u64 {
    fs::read_dir(state.join("incoming-objects"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

/// A receiver crash mid-object resumes from the durable offset (no blind replay).
#[test]
#[ignore = "WP3.2"]
fn crash_resume() {
    let mut s = Scenario::new(FILE, "crash_resume");
    let size = scaled(64) as u64 * 1024 * 1024;
    let setup = setup(|a| {
        large_object_in(a, "resume.bin", size, 13).unwrap();
    });
    let (wa, mut wb) = workers(&setup);
    let b_state = setup.dir.path().join("b");
    let lane = json!({"index": 0, "count": 1});
    {
        let link = WorkerLink::new(&wb, &wa, &setup.repo, &setup.b_device, &setup.a_device);
        for _ in 0..3 {
            link.metadata_round().unwrap();
        }
        let mut inventory = Value::Null;
        while spool_bytes(&b_state) < size / 4 && !s.over_budget() {
            link.bulk_round(&lane, &mut inventory).unwrap();
        }
    }
    wb.kill();
    let durable = spool_bytes(&b_state);
    s.note(format!("killed with {durable} spool bytes"));
    let mut wb = WorkerProc::spawn(&b_state, &[], &[]).unwrap();
    wb.wait_ready(Duration::from_secs(120)).unwrap();
    s.measure_from_now();
    let link = WorkerLink::new(&wb, &wa, &setup.repo, &setup.b_device, &setup.a_device);
    let result = pump(&link, &s, 4);
    let after = result.as_ref().copied().unwrap_or(0);
    let replayed = (after + durable).saturating_sub(size);
    s.metric_with(
        "replayed_bytes",
        replayed as f64,
        "bytes",
        json!({"durable_at_crash": durable, "received_after": after}),
    );
    s.check(result.is_ok(), format!("pump: {result:?}"));
    s.check(complete(&wb), "object complete and verified after resume");
    s.check(
        replayed <= 8 * 1024 * 1024,
        format!("{replayed} bytes replayed"),
    );
    s.finish();
}
