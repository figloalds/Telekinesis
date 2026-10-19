//! TEMPORARY probe (WP5.INT, not committed): the drain rate of a remote
//! catch-up during and after local writes through the mount.
mod support;
use serde_json::{Value, json};
use std::{collections::{BTreeMap, BTreeSet}, fs, path::PathBuf, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}, thread, time::{Duration, Instant}};
use support::*;
use tkfs::core::{Event, Store, id, inbox::StagedEvent};


/// A worker state B and `n` remote creates authored by A that B has not seen
/// (qual_wp2_3's catch-up).
struct CatchUp {
    _dir: tempfile::TempDir,
    state: PathBuf,
    repo: String,
    b_device: String,
    a_device: String,
    events: Vec<Event>,
}

fn catch_up(n: usize) -> CatchUp {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let state = dir.path().join("b");
    let mut b = Store::initialize(&state, &repo, &id()).unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    a.create("base", "directory").unwrap();
    for i in 0..20 {
        let e = a.create(&format!("base/f{i:02}.txt"), "file").unwrap();
        a.write_revision(
            &e.id,
            e.revisions.get("content").cloned(),
            format!("file {i}").as_bytes(),
        )
        .unwrap();
    }
    let allowed = devices([&a, &b]);
    sync_into(&a, &mut b, &allowed).unwrap();
    linear_history_named(&mut a, "catchup", n).unwrap();
    let events = metadata_bundle(&a, &b).unwrap().events;
    CatchUp {
        state,
        repo: repo.clone(),
        b_device: b.device.clone(),
        a_device: a.device.clone(),
        events,
        _dir: dir,
    }
}

/// Stages the catch-up on a secondary connection in pages of 64 (the way
/// feed exchanges stage), poking the activator, then waits until nothing is
/// pending.
fn stage(c: &CatchUp, stop: &AtomicBool, staged: &AtomicUsize) -> anyhow::Result<()> {
    let store = Store::open_secondary(&c.state, &c.repo, &c.b_device)?;
    let allowed: BTreeSet<String> = [c.a_device.clone(), c.b_device.clone()].into();
    for page in c.events.chunks(64) {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        let outcome = store.stage_incoming(
            page.iter()
                .cloned()
                .map(|event| StagedEvent {
                    event,
                    source: Some((c.a_device.clone(), 0)),
                })
                .collect(),
            &allowed,
        )?;
        anyhow::ensure!(
            outcome.rejected.is_empty(),
            "rejected: {:?}",
            outcome.rejected
        );
        staged.fetch_add(page.len(), Ordering::Relaxed);
        let _ = rpc_at(&c.state, json!({"op":"revalidate","ids":[]}));
    }
    while !stop.load(Ordering::Relaxed) {
        let pending = rpc_at(&c.state, json!({"op":"status"}))
            .ok()
            .and_then(|s| s["incoming_pending"].as_i64());
        if pending == Some(0) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}


#[test]
#[ignore]
fn probe_drain() {
    let writes_s: u64 = std::env::var("PROBE_WRITES_S").ok().and_then(|v| v.parse().ok()).unwrap_or(180);
    let c = Arc::new(catch_up(2000));
    let mount = c.state.parent().unwrap().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&c.state, &["--mount", &mount_arg], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(120)).unwrap();
    if worker.rpc(json!({"op":"mount-status"})).is_ok() {
        let _ = worker.rpc(json!({"op":"mount-attach","path":mount_arg}));
    }
    let t0 = Instant::now();
    while !mount.join("base").is_dir() { assert!(t0.elapsed() < Duration::from_secs(30)); thread::sleep(Duration::from_millis(50)); }
    fs::create_dir(mount.join("work")).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let staged = Arc::new(AtomicUsize::new(0));
    let feeder = { let (c, stop, staged) = (c.clone(), stop.clone(), staged.clone()); thread::spawn(move || stage(&c, &stop, &staged)) };
    let start = Instant::now();
    let sample = |worker: &WorkerProc, label: &str| {
        let s: Value = worker.rpc(json!({"op":"status"})).unwrap_or(Value::Null);
        eprintln!("PROBE {label} t={:.0}s pending={} activation={}", start.elapsed().as_secs_f64(), s["incoming_pending"], s["activation"]);
    };
    let mut round = 0usize;
    let mut last = Instant::now();
    while start.elapsed() < Duration::from_secs(writes_s) {
        let name = format!("work/w{round:05}.bin");
        let bytes = seeded_bytes(round as u64, 1024 + (round * 4099) % (60 * 1024));
        let target = mount.join(&name);
        fs::write(&target, &bytes).unwrap();
        if round % 4 == 0 { fs::rename(&target, mount.join(format!("work/r{round:05}.bin"))).unwrap(); }
        thread::sleep(Duration::from_millis(40));
        round += 1;
        if last.elapsed() >= Duration::from_secs(10) { sample(&worker, "writing"); last = Instant::now(); }
    }
    eprintln!("PROBE writes stopped after {round} files");
    for _ in 0..120 {
        sample(&worker, "after");
        let s: Value = worker.rpc(json!({"op":"status"})).unwrap_or(Value::Null);
        if s["incoming_pending"].as_i64() == Some(0) { break; }
        thread::sleep(Duration::from_secs(10));
    }
    stop.store(true, Ordering::Relaxed);
    let _ = feeder.join();
}
