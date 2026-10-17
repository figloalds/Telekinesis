//! Qualification scenarios for WP5.2 (the local filesystem path).
//! REVAMP_PLAN §14.5b WP5.2 acceptance.
//!
//! * `copy_2000_small_files_into_a_mount`: 2,000 files of 1–64 KiB in 100
//!   directories copied into a mount served by this build and, when set, by
//!   the 6298d0b baseline (`TKFS_BASELINE_BIN`) and another build to compare
//!   (`TKFS_COMPARE_BIN`, for example the WP4.INT one), best of
//!   `TKFS_WP5_2_RUNS` (3) runs each, interleaved. Each copy opens the
//!   source, creates the destination, writes it and closes it, file by file,
//!   like the lab's `copy_in.ps1`. Checked: at least 2x faster than the
//!   baseline; every file committed with its bytes (this build).
//! * `fs_p99_during_catch_up`: stats, reads (also of files saved through
//!   the mount) and listings while 2K remote events activate. Filesystem p99
//!   under 50 ms. `fs_writes_during_catch_up` adds creates, writes, closes
//!   and renames (evidence; checked: nothing fails, everything is committed).
//! * `mounted_read_of_a_512mib_object_while_it_downloads`: a read through the
//!   mount started while the object downloads (the receiving lane's spool
//!   grows) waits for it and returns its bytes.
//!
//! `cargo test --release --test qual_wp5_2_local -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::core::{Event, ObjectPart, Store, id, inbox::StagedEvent};

const FILE: &str = "qual_wp5_2_local";

/// The dataset: `(relative path, bytes)`, 1–64 KiB, in `dirs` directories.
fn dataset(files: usize, dirs: usize) -> Vec<(String, Vec<u8>)> {
    (0..files)
        .map(|i| {
            let size = 1024
                + (seeded_bytes(i as u64 * 31 + 7, 8)[0] as usize * 251 + i * 977)
                    % (63 * 1024 + 1);
            (
                format!("d{:03}/f{i:04}.bin", i % dirs),
                seeded_bytes(i as u64 + 1, size),
            )
        })
        .collect()
}

/// One worker of `exe` with a mount, started standalone
/// (`daemon --state S --mount M`, as the lab's builds all accept).
struct Mounted {
    exe: PathBuf,
    state: PathBuf,
    mount: PathBuf,
    child: Child,
}

impl Mounted {
    fn start(exe: &Path, root: &Path) -> anyhow::Result<Self> {
        let state = root.join("state");
        let mount = root.join("mnt");
        let init = Command::new(exe)
            .args(["init", "--state"])
            .arg(&state)
            .output()?;
        anyhow::ensure!(
            init.status.success(),
            "init: {}",
            String::from_utf8_lossy(&init.stderr)
        );
        let log = fs::File::create(root.join("worker.log"))?;
        let child = Command::new(exe)
            .arg("daemon")
            .arg("--state")
            .arg(&state)
            .arg("--mount")
            .arg(&mount)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()?;
        let mut mounted = Self {
            exe: exe.to_owned(),
            state,
            mount,
            child,
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        while !mounted.mount.join(".tkfs-runtime.json").is_file() {
            anyhow::ensure!(
                mounted.child.try_wait()?.is_none(),
                "the worker exited: {}",
                tail(&root.join("worker.log"), 4096)
            );
            anyhow::ensure!(Instant::now() < deadline, "the mount never appeared");
            thread::sleep(Duration::from_millis(50));
        }
        Ok(mounted)
    }

    /// Stops gracefully, retrying while files are unsaved (a graceful stop
    /// is refused until every file is committed); the time it took.
    fn stop(&mut self) -> Duration {
        let started = Instant::now();
        let runtime = self.state.join("runtime.json");
        while started.elapsed() < Duration::from_secs(120) {
            let stopped = Command::new(&self.exe)
                .arg("--runtime")
                .arg(&runtime)
                .arg("stop")
                .output()
                .is_ok_and(|output| output.status.success());
            if stopped {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let took = started.elapsed();
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        took
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            self.stop();
        }
    }
}

/// The lab's copy: directories first, then each file opened, created,
/// written and closed in order.
fn copy_in(files: &[(String, PathBuf)], mount: &Path) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let dirs: BTreeSet<&str> = files
        .iter()
        .filter_map(|(relative, _)| relative.rsplit_once('/').map(|(dir, _)| dir))
        .collect();
    for dir in dirs {
        fs::create_dir_all(mount.join(dir))?;
    }
    let mut buffer = vec![0; 4 * 1024 * 1024];
    for (relative, source) in files {
        let mut input = fs::File::open(source)?;
        let mut output = fs::File::create(mount.join(relative))?;
        loop {
            let n = input.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            output.write_all(&buffer[..n])?;
        }
        drop(output);
    }
    Ok(started.elapsed())
}

#[test]
#[ignore = "WP5.2"]
fn copy_2000_small_files_into_a_mount() {
    let s = Scenario::with_budget(FILE, "copy_2000_small_files_into_a_mount", budget_or(3600));
    if !mount_supported() {
        return s.skip("no mount support");
    }
    let mut s = s;
    let files = scaled(2000);
    let data = tempfile::tempdir().unwrap();
    let set = dataset(files, 100);
    let mut sources = vec![];
    for (relative, bytes) in &set {
        let path = data.path().join("src").join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        sources.push((relative.clone(), path));
    }
    let total: usize = set.iter().map(|(_, b)| b.len()).sum();
    let runs: usize = std::env::var("TKFS_WP5_2_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let mut builds: Vec<(&str, PathBuf)> = vec![("this", bin())];
    if let Some(baseline) = std::env::var_os("TKFS_BASELINE_BIN") {
        builds.push(("baseline", PathBuf::from(baseline)));
    }
    if let Some(compare) = std::env::var_os("TKFS_COMPARE_BIN") {
        builds.push(("compare", PathBuf::from(compare)));
    }
    let mut results: BTreeMap<&str, Vec<(f64, f64)>> = BTreeMap::new();
    s.measure_from_now();
    for round in 0..runs {
        for (label, exe) in &builds {
            let root = tempfile::tempdir().unwrap();
            let mut worker = match Mounted::start(exe, root.path()) {
                Ok(worker) => worker,
                Err(error) => {
                    s.check(false, format!("{label}: {error:#}"));
                    continue;
                }
            };
            let copied = copy_in(&sources, &worker.mount);
            let stop = worker.stop();
            match copied {
                Ok(took) => {
                    let ms = took.as_secs_f64() * 1000.0;
                    let stop_ms = stop.as_secs_f64() * 1000.0;
                    s.note(format!(
                        "{label} round {round}: copy {ms:.0} ms, committed {stop_ms:.0} ms later"
                    ));
                    results.entry(label).or_default().push((ms, stop_ms));
                }
                Err(error) => {
                    s.check(false, format!("{label} round {round}: {error:#}"));
                }
            }
            if *label == "this" && round == 0 {
                // Every file is committed with its bytes.
                let store = Store::open_existing(&worker.state).unwrap();
                let wrong: Vec<&String> = set
                    .iter()
                    .filter(|(relative, bytes)| {
                        store
                            .lookup(relative)
                            .ok()
                            .and_then(|entry| entry.content)
                            .and_then(|object| store.read_object(&object).ok())
                            .as_deref()
                            != Some(bytes.as_slice())
                    })
                    .map(|(relative, _)| relative)
                    .collect();
                s.check(
                    wrong.is_empty(),
                    format!(
                        "{} files not committed with their bytes: {:?}",
                        wrong.len(),
                        wrong.iter().take(5).collect::<Vec<_>>()
                    ),
                );
                let conflicts = store
                    .conflicts()
                    .unwrap()
                    .into_iter()
                    .filter(|c| c.resolved_by.is_none())
                    .count();
                s.check(conflicts == 0, format!("{conflicts} conflicts"));
            }
        }
    }
    let best = |label: &str| {
        results
            .get(label)
            .and_then(|runs| runs.iter().map(|(ms, _)| *ms).min_by(|a, b| a.total_cmp(b)))
    };
    let this = best("this").unwrap_or(f64::NAN);
    let extra = json!({"files": files, "bytes": total, "runs": runs,
        "results": results.iter().map(|(label, runs)| (label.to_string(), json!(runs))).collect::<serde_json::Map<String, Value>>()});
    s.metric_with("copy_ms", this, "ms", extra.clone());
    if let Some(compare) = best("compare") {
        s.metric_with("compare_copy_ms", compare, "ms", extra.clone());
    }
    match best("baseline") {
        Some(baseline) => {
            let speedup = baseline / this;
            s.metric_with("baseline_copy_ms", baseline, "ms", extra.clone());
            s.metric_with("speedup_vs_baseline", speedup, "x", extra);
            s.check(
                speedup >= 2.0,
                format!("{speedup:.2}x faster than the baseline, at least 2x required"),
            );
        }
        None => s.note("TKFS_BASELINE_BIN (a 6298d0b tkfs binary) is not set: no speedup measured"),
    }
    s.finish();
}

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

/// Filesystem operations through the mount while 2K remote events activate
/// (the rule of the floor `fs_p99_during_catchup`, < 50 ms): stats, reads of
/// base files and of files saved through the mount before the catch-up, and
/// listings.
#[test]
#[ignore = "WP5.2"]
fn fs_p99_during_catch_up() {
    fs_during_catch_up("fs_p99_during_catch_up", false);
}

/// The same with the application writing (create, write, close, rename)
/// during the catch-up. Evidence: each local commit makes the next remote
/// event concurrent with it, so the catch-up runs the activator's slow path
/// (its cost is WP5.1's, CCR-WP4.INT-3), whose commits hold the engine.
/// Checked: no operation fails, and every file written is committed with
/// its bytes once the catch-up ends.
#[test]
#[ignore = "WP5.2"]
fn fs_writes_during_catch_up() {
    fs_during_catch_up("fs_writes_during_catch_up", true);
}

fn fs_during_catch_up(name: &str, writes: bool) {
    let s = Scenario::with_budget(FILE, name, budget_or(120));
    if !mount_supported() {
        return s.skip("no mount support");
    }
    let mut s = s;
    let c = Arc::new(catch_up(scaled(2000)));
    let mount = c.state.parent().unwrap().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&c.state, &["--mount", &mount_arg], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(120)).unwrap();
    if worker.rpc(json!({"op":"mount-status"})).is_ok() {
        let _ = worker.rpc(json!({"op":"mount-attach","path":mount_arg}));
    }
    wait_for("the mount", Duration::from_secs(30), || {
        mount.join("base").is_dir()
    });
    // Files saved through the mount before the catch-up, read during it.
    fs::create_dir(mount.join("saved")).unwrap();
    let mut written = BTreeMap::new();
    for i in 0..200u64 {
        let bytes = seeded_bytes(10_000 + i, 1024 + (i as usize * 997) % (60 * 1024));
        fs::write(mount.join(format!("saved/s{i:03}.bin")), &bytes).unwrap();
        written.insert(format!("saved/s{i:03}.bin"), bytes);
    }
    wait_for("the saved files committed", Duration::from_secs(60), || {
        worker
            .rpc(json!({"op":"status"}))
            .ok()
            .and_then(|status| status["unflushed_files"].as_u64())
            == Some(0)
    });
    if writes {
        fs::create_dir(mount.join("work")).unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let staged = Arc::new(AtomicUsize::new(0));
    s.measure_from_now();
    let feeder = {
        let (c, stop, staged) = (c.clone(), stop.clone(), staged.clone());
        thread::spawn(move || stage(&c, &stop, &staged))
    };
    let mut samples = BTreeMap::<&str, Vec<f64>>::new();
    let mut errors = 0usize;
    let mut last_error = None;
    let mut round = 0usize;
    while round == 0 || (!feeder.is_finished() && !s.over_budget()) {
        let mut op = |kind: &'static str, f: &mut dyn FnMut() -> std::io::Result<()>| {
            let (result, ms) = time_ms(f);
            match result {
                Ok(()) => samples.entry(kind).or_default().push(ms),
                Err(error) => {
                    errors += 1;
                    last_error = Some(format!("{kind}: {error}"));
                }
            }
        };
        let base = mount.join(format!("base/f{:02}.txt", round % 20));
        op("stat", &mut || fs::metadata(&base).map(|_| ()));
        op("read", &mut || fs::read(&base).map(|_| ()));
        let saved = format!("saved/s{:03}.bin", round % 200);
        let expected = written[&saved].clone();
        op("read_saved", &mut || {
            let read = fs::read(mount.join(&saved))?;
            if read == expected {
                Ok(())
            } else {
                Err(std::io::Error::other("a saved file read back wrong"))
            }
        });
        if round % 10 == 0 {
            op("list", &mut || {
                fs::read_dir(mount.join("base")).map(|d| {
                    let _ = d.count();
                })
            });
        }
        if writes {
            let name = format!("work/w{round:05}.bin");
            let bytes = seeded_bytes(round as u64, 1024 + (round * 4099) % (60 * 1024));
            let target = mount.join(&name);
            op("create_write_close", &mut || fs::write(&target, &bytes));
            if round % 4 == 0 {
                let renamed = format!("work/r{round:05}.bin");
                op("rename", &mut || fs::rename(&target, mount.join(&renamed)));
                written.insert(renamed, bytes);
            } else {
                written.insert(name, bytes);
            }
        }
        round += 1;
        thread::sleep(Duration::from_millis(10));
    }
    let timed_out = !feeder.is_finished();
    stop.store(true, Ordering::Relaxed);
    let fed = feeder.join().unwrap();
    let all: Vec<f64> = samples.values().flatten().copied().collect();
    let summary = summarize(&all);
    let per_kind: serde_json::Map<String, Value> = samples
        .iter()
        .map(|(kind, v)| (kind.to_string(), summarize(v).json()))
        .collect();
    let engine = worker
        .rpc(json!({"op":"watch","after_seq":0,"wait_ms":0}))
        .map(|hb| hb["engine"].clone())
        .unwrap_or(Value::Null);
    // The writes are committed whatever the activation does; it may finish
    // after the measured phase (the writes made it slow), within a bound.
    let saved = wait_until(Duration::from_secs(120), || {
        worker
            .rpc(json!({"op":"status"}))
            .ok()
            .and_then(|status| status["unflushed_files"].as_u64())
            == Some(0)
    });
    let caught_up = wait_until(Duration::from_secs(600), || {
        worker
            .rpc(json!({"op":"status"}))
            .ok()
            .and_then(|status| status["incoming_pending"].as_i64())
            == Some(0)
    });
    s.metric_with(
        "fs_p99_ms",
        summary.p99,
        "ms",
        json!({"fs": summary.json(), "per_kind": per_kind, "fs_errors": errors,
            "staged": staged.load(Ordering::Relaxed), "events": c.events.len(),
            "files": written.len(), "writes": writes, "engine": engine,
            "catch_up_finished_in_budget": !timed_out, "caught_up_after": caught_up}),
    );
    s.check(
        errors == 0,
        format!("{errors} FS operations failed, last: {last_error:?}"),
    );
    s.check(fed.is_ok(), format!("staging failed: {fed:?}"));
    s.check(saved, "every write through the mount is saved");
    if writes {
        s.note(format!(
            "with writes: p99 {:.1} ms (no floor: the activator's slow path, CCR-WP4.INT-3)",
            summary.p99
        ));
    } else {
        s.check(caught_up, "the catch-up finishes");
        s.check(
            summary.p99 < 50.0,
            format!(
                "filesystem p99 {:.1} ms during the catch-up, under 50 ms required",
                summary.p99
            ),
        );
        if timed_out {
            s.timeout("catch-up not finished in budget");
        }
    }
    // Every file written through the mount is committed with its bytes.
    let _ = worker.stop();
    let store = Store::open_existing_with(
        &c.state,
        tkfs::core::OpenOptions {
            defer_activation: true,
            ..Default::default()
        },
    )
    .unwrap();
    let wrong = written
        .iter()
        .filter(|(path, bytes)| {
            store
                .lookup(path)
                .ok()
                .and_then(|entry| entry.content)
                .and_then(|object| store.read_object(&object).ok())
                .as_deref()
                != Some(bytes.as_slice())
        })
        .count();
    s.check(
        wrong == 0,
        format!("{wrong} files not committed with their bytes"),
    );
    s.finish();
}

#[test]
#[ignore = "WP5.2"]
fn mounted_read_of_a_512mib_object_while_it_downloads() {
    let s = Scenario::with_budget(
        FILE,
        "mounted_read_of_a_512mib_object_while_it_downloads",
        budget_or(900),
    );
    if !mount_supported() {
        return s.skip("no mount support");
    }
    let mut s = s;
    let size: u64 = std::env::var("TKFS_WP5_2_DOWNLOAD_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(512)
        * 1024
        * 1024;
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let object = large_object_in(&mut a, "big.bin", size, 11).unwrap();
    a.create("small.txt", "file").unwrap();
    let state = dir.path().join("b");
    let mut b = Store::initialize(&state, &repo, &id()).unwrap();
    let allowed = devices([&a, &b]);
    b.receive(metadata_bundle(&a, &b).unwrap(), &allowed)
        .unwrap();
    drop(b);
    let engine = Arc::new(Mutex::new(tkfs::runtime::Engine::new(
        Store::open_existing(&state).unwrap(),
    )));
    let controller = tkfs::worker::mount_ctl::MountController::new(engine.clone(), None);
    let mount = dir.path().join("mnt");
    use tkfs::model::health::MountControl;
    if let Err(error) = controller.attach(&mount) {
        if error.code == "WINFSP_RUNTIME_UNAVAILABLE" || error.code == "FUSE_MOUNT_FAILED" {
            return s.skip("no mount support");
        }
        panic!("attach: {error}");
    }
    s.measure_from_now();
    // The read starts before any byte arrived.
    let reader = {
        let path = mount.join("big.bin");
        thread::spawn(move || {
            let started = Instant::now();
            let mut file = fs::File::open(&path)?;
            let mut digest = <sha2::Sha256 as sha2::Digest>::new();
            let mut buffer = vec![0; 1024 * 1024];
            let mut first = None;
            loop {
                let n = file.read(&mut buffer)?;
                first.get_or_insert_with(|| started.elapsed());
                if n == 0 {
                    break;
                }
                sha2::Digest::update(&mut digest, &buffer[..n]);
            }
            Ok::<_, std::io::Error>((
                hex::encode(sha2::Digest::finalize(digest)),
                first,
                started.elapsed(),
            ))
        })
    };
    thread::sleep(Duration::from_millis(500));
    // The download: 4 MiB parts through the receive path (spool, durable
    // offset, verified install), while the read waits.
    let source = a.root.join("objects").join(&object);
    let mut input = fs::File::open(&source).unwrap();
    let mut offset = 0u64;
    let mut buffer = vec![0; 4 * 1024 * 1024];
    let mut stats = vec![];
    let fed = Instant::now();
    while offset < size {
        let n = input.read(&mut buffer).unwrap();
        let part = ObjectPart {
            offset,
            total: size,
            hex: hex::encode(&buffer[..n]),
        };
        engine
            .lock()
            .unwrap()
            .store
            .receive_parts(
                BTreeMap::from([(object.clone(), part)]),
                &[],
                &BTreeSet::new(),
            )
            .unwrap();
        offset += n as u64;
        // Other operations through the mount do not wait for the download.
        let (result, ms) = time_ms(|| fs::metadata(mount.join("small.txt")));
        if result.is_ok() {
            stats.push(ms);
        }
    }
    let fed_ms = fed.elapsed().as_secs_f64() * 1000.0;
    let read = reader.join().unwrap();
    let stat = summarize(&stats);
    match read {
        Ok((digest, first, total)) => {
            s.metric_with(
                "read_ms",
                total.as_secs_f64() * 1000.0,
                "ms",
                json!({"bytes": size, "download_ms": fed_ms,
                    "first_bytes_ms": first.map(|d| d.as_secs_f64() * 1000.0),
                    "stat_during_download": stat.json()}),
            );
            s.check(digest == object, "the read returned the downloaded bytes");
        }
        Err(error) => {
            s.check(
                false,
                format!(
                    "the read failed: {error} (kind {:?}, os error {:?})",
                    error.kind(),
                    error.raw_os_error()
                ),
            );
        }
    }
    s.check(
        stat.p99 < 50.0,
        format!("stat p99 {:.1} ms during the download", stat.p99),
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while controller.detach(false).is_err() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    let _ = controller.detach(true);
    s.finish();
}
