//! Qualification scenarios for WP3.2 (objects v3, worker side).
//! REVAMP_PLAN §14.6, §6.4.
//!
//! Created by WP0.5, `#[ignore = "WP3.2"]`, against the legacy bulk path.
//! WP3.2 replaced the pump with the objects v3 test pump: two real worker
//! processes, one v3 loopback session to each (`session::dial_loopback`, as
//! the orchestrator's `SessionWorkerClient` dials them), and the receiver's
//! `objects.want` driving the sender's `objects.read` (a reader thread keeps
//! up to `READS_AHEAD` 1 MiB reads ahead) into the receiver's `objects.write`
//! (tiny objects batched up to 1 MiB and `MAX_BATCH` items per call, up to
//! `WRITES_IN_FLIGHT` calls in flight, as §6.4's download pipeline keeps
//! them; parts of one object that overtake each other wait in the worker for
//! their turn). They stay `#[ignore]`: `scripts/qualify.ps1` runs the release
//! qualification with `--ignored`.
//! `cargo test --release --test qual_wp3_2_objects -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    core::{Store, id},
    model::protocol::objects::{MAX_READ_LEN, MAX_WANT_ITEMS, PULL_BUDGET_BYTES},
    session::{self, Conn, ConnOptions, Priority},
};

const FILE: &str = "qual_wp3_2_objects";
/// Reads kept in flight ahead of the writer (§6.4: ≤ 4 per transfer).
const READS_AHEAD: usize = 4;
/// Items per `objects.write` call (the header stays far below 64 KiB).
const MAX_BATCH: usize = 64;
/// `objects.write` calls in flight (§6.4: ≤ 4).
const WRITES_IN_FLIGHT: usize = 4;
const DEADLINE: Duration = Duration::from_secs(60);

/// Time spent in each worker op by the pump (µs), for the scenario notes.
static WANT_US: AtomicU64 = AtomicU64::new(0);
static READ_US: AtomicU64 = AtomicU64::new(0);
static WRITE_US: AtomicU64 = AtomicU64::new(0);

fn timed<T>(counter: &AtomicU64, f: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let value = f();
    counter.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
    value
}

/// `want`/`read`/`write` time (s) so far, as a note.
fn op_times() -> String {
    let s = |c: &AtomicU64| c.load(Ordering::Relaxed) as f64 / 1e6;
    format!(
        "time in objects.want {:.1} s, objects.read {:.1} s, objects.write {:.1} s (summed over threads)",
        s(&WANT_US),
        s(&READ_US),
        s(&WRITE_US)
    )
}

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
    // The content counters refresh in the background (§4.5): give them a
    // moment to catch up with the last registrations.
    wait_until(Duration::from_secs(15), || {
        w.rpc(json!({"op":"status"}))
            .is_ok_and(|s| s["content"]["complete"] == true)
    })
}

fn workers(setup: &Setup) -> (WorkerProc, WorkerProc) {
    let mut a = WorkerProc::spawn(&setup.dir.path().join("a"), &[], &[]).unwrap();
    let mut b = WorkerProc::spawn(&setup.dir.path().join("b"), &[], &[]).unwrap();
    a.wait_ready(Duration::from_secs(600)).unwrap();
    b.wait_ready(Duration::from_secs(600)).unwrap();
    (a, b)
}

/// A loopback session to the worker of `state` (orchestrator role).
fn dial(state: &Path) -> Conn {
    let address = discovery_at(state).unwrap().address;
    session::dial_loopback(&address, None, ConnOptions::default()).unwrap()
}

/// The test pump: B (receiver) pulls from A (sender) over loopback sessions.
struct Pump {
    a: Conn,
    b: Conn,
    route_a: Value,
    route_b: Value,
    session: String,
}

/// One chunk read from A, in transfer order.
struct Chunk {
    hash: String,
    offset: u64,
    total: u64,
    bytes: Vec<u8>,
}

impl Pump {
    fn new(setup: &Setup) -> Self {
        let route_b = json!({"peer": id(), "repo": setup.repo, "local_replica": setup.b_device,
            "remote_replica": setup.a_device});
        let route_a = json!({"peer": id(), "repo": setup.repo, "local_replica": setup.a_device,
            "remote_replica": setup.b_device});
        Self {
            a: dial(&setup.dir.path().join("a")),
            b: dial(&setup.dir.path().join("b")),
            route_a,
            route_b,
            session: id(),
        }
    }

    /// Another handle on the same sessions (clones of the connections).
    fn clone_handles(&self) -> Pump {
        Pump {
            a: self.a.clone(),
            b: self.b.clone(),
            route_a: self.route_a.clone(),
            route_b: self.route_b.clone(),
            session: self.session.clone(),
        }
    }

    fn want(&self) -> anyhow::Result<Vec<Value>> {
        let reply = timed(&WANT_US, || {
            self.b.call(
                "objects.want",
                json!({"route": self.route_b, "lane": {"index": 0, "count": 1},
                    "max_items": MAX_WANT_ITEMS, "max_bytes": PULL_BUDGET_BYTES, "session": self.session}),
                vec![],
                Priority::Control,
                DEADLINE,
            )
        })
        .map_err(|e| anyhow::anyhow!("objects.want: {e}"))?;
        Ok(reply.header["items"]
            .as_array()
            .cloned()
            .unwrap_or_default())
    }

    /// One write call; returns the bytes the call made durable or completed.
    fn write(&self, chunks: &[Chunk]) -> anyhow::Result<u64> {
        let items: Vec<Value> = chunks
            .iter()
            .map(|c| {
                let len = c.bytes.len() as u64;
                json!({"hash": c.hash, "offset": c.offset, "total": c.total, "len": len,
                    "last": c.offset + len == c.total})
            })
            .collect();
        let body: Vec<u8> = chunks
            .iter()
            .flat_map(|c| c.bytes.iter().copied())
            .collect();
        let reply = timed(&WRITE_US, || {
            self.b.call(
                "objects.write",
                json!({"route": self.route_b, "session": self.session, "items": items}),
                body,
                Priority::Data,
                DEADLINE,
            )
        })
        .map_err(|e| anyhow::anyhow!("objects.write: {e}"))?;
        let mut written = 0;
        for (result, chunk) in reply.header["results"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .zip(chunks)
        {
            match &result["outcome"] {
                Value::String(s) if s == "completed" => written += chunk.bytes.len() as u64,
                Value::Object(o) if o.contains_key("progressed") => {
                    written += chunk.bytes.len() as u64
                }
                other => anyhow::bail!("objects.write {}: {other}", chunk.hash),
            }
        }
        Ok(written)
    }

    /// Pull every wanted object until B wants nothing, an error, or the
    /// budget. `stop_after`: stop once that many bytes were written (a crash
    /// scenario's first leg). Returns the object bytes B received.
    fn run(&self, s: &Scenario, stop_after: Option<u64>) -> anyhow::Result<u64> {
        let mut received = 0u64;
        loop {
            if s.over_budget() || stop_after.is_some_and(|n| received >= n) {
                return Ok(received);
            }
            let items = self.want()?;
            if items.is_empty() {
                return Ok(received);
            }
            // The reader streams the list's chunks, in order, ahead of the writer.
            let (tx, rx) = mpsc::sync_channel::<anyhow::Result<Chunk>>(READS_AHEAD);
            let a = self.a.clone();
            let route = self.route_a.clone();
            let reader = thread::spawn(move || {
                for item in items {
                    let hash = item["hash"].as_str().unwrap_or_default().to_owned();
                    let size = item["size"].as_u64().unwrap_or(0);
                    let mut offset = item["offset"].as_u64().unwrap_or(0);
                    loop {
                        let read = timed(&READ_US, || {
                            a.call(
                                "objects.read",
                                json!({"route": route, "hash": hash, "offset": offset, "len": MAX_READ_LEN}),
                                vec![],
                                Priority::Control,
                                DEADLINE,
                            )
                        });
                        let chunk = match read {
                            Ok(reply) => Chunk {
                                hash: hash.clone(),
                                offset,
                                total: reply.header["total"].as_u64().unwrap_or(size),
                                bytes: reply.body,
                            },
                            Err(e) => {
                                let _ = tx.send(Err(anyhow::anyhow!("objects.read {hash}: {e}")));
                                return;
                            }
                        };
                        offset += chunk.bytes.len() as u64;
                        let total = chunk.total;
                        if tx.send(Ok(chunk)).is_err() {
                            return;
                        }
                        if offset >= total {
                            break;
                        }
                    }
                }
            });
            // Writers: up to WRITES_IN_FLIGHT calls in flight.
            let (batches, queue) = mpsc::sync_channel::<Vec<Chunk>>(WRITES_IN_FLIGHT);
            let queue = Arc::new(Mutex::new(queue));
            let written = Arc::new(Mutex::new(Ok::<u64, anyhow::Error>(0)));
            let writers: Vec<_> = (0..WRITES_IN_FLIGHT)
                .map(|_| {
                    let (queue, written) = (queue.clone(), written.clone());
                    let pump = self.clone_handles();
                    thread::spawn(move || {
                        loop {
                            let next = queue.lock().unwrap().recv();
                            let Ok(batch) = next else { return };
                            let result = pump.write(&batch);
                            let mut total = written.lock().unwrap();
                            match (result, total.as_mut()) {
                                (Ok(n), Ok(sum)) => *sum += n,
                                (Err(e), Ok(_)) => *total = Err(e),
                                (_, Err(_)) => {}
                            }
                        }
                    })
                })
                .collect();
            let done = |written: &Arc<Mutex<anyhow::Result<u64>>>| {
                written.lock().unwrap().as_ref().map_or(true, |n| {
                    stop_after.is_some_and(|stop| received + n >= stop)
                })
            };
            let mut batch: Vec<Chunk> = vec![];
            let mut batch_bytes = 0usize;
            let mut outcome = Ok(());
            for chunk in rx.iter() {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(e) => {
                        outcome = Err(e);
                        break;
                    }
                };
                if !batch.is_empty()
                    && (batch_bytes + chunk.bytes.len() > MAX_READ_LEN as usize
                        || batch.len() >= MAX_BATCH)
                {
                    if batches.send(std::mem::take(&mut batch)).is_err() {
                        break;
                    }
                    batch_bytes = 0;
                    if s.over_budget() || done(&written) {
                        break;
                    }
                }
                batch_bytes += chunk.bytes.len();
                batch.push(chunk);
            }
            if outcome.is_ok() && !batch.is_empty() && !done(&written) {
                let _ = batches.send(batch);
            }
            drop(batches);
            for writer in writers {
                let _ = writer.join();
            }
            let written = std::mem::replace(&mut *written.lock().unwrap(), Ok(0));
            match written {
                Ok(n) => received += n,
                Err(e) => outcome = outcome.and(Err(e)),
            }
            drop(rx);
            let _ = reader.join();
            outcome?;
        }
    }
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
    let (_wa, wb) = workers(&setup);
    let pump = Pump::new(&setup);
    s.measure_from_now();
    let result = pump.run(&s, None);
    let secs = s.started.elapsed().as_secs_f64();
    let done = complete(&wb);
    let received = result.as_ref().copied().unwrap_or(0);
    s.metric_with(
        "mib_per_s",
        received as f64 / 1048576.0 / secs,
        "MiB/s",
        json!({"bytes": size, "received": received, "complete": done}),
    );
    s.note(op_times());
    s.check(result.is_ok(), format!("pump: {result:?}"));
    s.check(
        received == size,
        format!("{received} of {size} bytes, no replay"),
    );
    if !done {
        s.timeout(format!("{received} of {size} bytes in budget"));
    }
    s.finish();
}

/// The directory fsync of `raw_fs_rate` (as `core::objects::sync_dir`).
#[cfg(windows)]
fn sync_dir(dir: &Path) {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    fs::OpenOptions::new()
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)
        .and_then(|dir| dir.sync_all())
        .unwrap();
}
#[cfg(not(windows))]
fn sync_dir(dir: &Path) {
    fs::File::open(dir).and_then(|dir| dir.sync_all()).unwrap();
}

/// The bare filesystem rate of the receiver's literal data plane on this disk
/// (objects/s), measured next to the scenario as evidence for its floor:
/// `count` 64-byte files, each created, written and fsynced (one data fsync
/// per object, §6.4), then renamed into another directory, with one
/// directory fsync per 64 renames (one write call's install barrier). No
/// database, no hashing, no sessions: the worker cannot beat it.
fn raw_fs_rate(dir: &Path, count: usize) -> f64 {
    let (spool, objects) = (dir.join("raw-spool"), dir.join("raw-objects"));
    fs::create_dir_all(&spool).unwrap();
    fs::create_dir_all(&objects).unwrap();
    let started = Instant::now();
    for i in 0..count {
        let name = format!("{i:064x}");
        let path = spool.join(&name);
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(&[i as u8; 64]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        fs::rename(&path, objects.join(&name)).unwrap();
        if i % 64 == 63 || i + 1 == count {
            sync_dir(&objects);
        }
    }
    let rate = count as f64 / started.elapsed().as_secs_f64();
    let _ = fs::remove_dir_all(&spool);
    let _ = fs::remove_dir_all(&objects);
    rate
}

/// 10K tiny objects (64 B) ≥ 300 objects/s. The bare filesystem rate of the
/// same data plane is measured right before and after (`fs_objects_per_s`,
/// no floor): on a disk that cannot do 300 fsynced creates per second the
/// floor cannot be met by any worker, and the two numbers side by side say
/// which of the two a miss is.
#[test]
#[ignore = "WP3.2"]
fn tiny_objects_10k() {
    let mut s = Scenario::new(FILE, "tiny_objects_10k");
    let count = scaled(10_000);
    let setup = setup(|a| {
        small_objects_in(a, "tiny", count, 64, 12).unwrap();
    });
    let (_wa, wb) = workers(&setup);
    let pump = Pump::new(&setup);
    let raw_before = raw_fs_rate(setup.dir.path(), 1000);
    s.measure_from_now();
    let result = pump.run(&s, None);
    let secs = s.started.elapsed().as_secs_f64();
    let raw_after = raw_fs_rate(setup.dir.path(), 1000);
    s.metric_with(
        "fs_objects_per_s",
        (raw_before + raw_after) / 2.0,
        "objects/s",
        json!({"before": raw_before, "after": raw_after, "files": 1000}),
    );
    let done = complete(&wb);
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
    s.note(op_times());
    s.check(result.is_ok(), format!("pump: {result:?}"));
    if !done {
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

/// The committed durable offset of the (only) spool of `state`.
fn durable(state: &Path) -> u64 {
    let db = rusqlite::Connection::open(state.join("metadata.sqlite")).unwrap();
    db.query_row(
        "SELECT coalesce(sum(durable),0) FROM spool_state",
        [],
        |r| r.get::<_, i64>(0),
    )
    .unwrap() as u64
}

/// A receiver killed mid-object resumes from the durable offset (no blind replay).
#[test]
#[ignore = "WP3.2"]
fn crash_resume() {
    let mut s = Scenario::new(FILE, "crash_resume");
    let size = scaled(64) as u64 * 1024 * 1024;
    let setup = setup(|a| {
        large_object_in(a, "resume.bin", size, 13).unwrap();
    });
    let (_wa, mut wb) = workers(&setup);
    let b_state = setup.dir.path().join("b");
    // The first leg streams; once a quarter of the object is durable the
    // receiver is killed with writes in flight (the pump then fails).
    let first = Instant::now();
    let pump = Pump::new(&setup);
    let (streamed, reached) = thread::scope(|scope| {
        let leg = scope.spawn(|| pump.run(&s, None));
        let reached = wait_until(Duration::from_secs(60), || durable(&b_state) >= size / 4);
        wb.kill();
        (leg.join().unwrap(), reached)
    });
    s.check(reached, "a quarter of the object became durable");
    s.check(
        streamed.is_err(),
        format!("the kill cut the first leg: {streamed:?}"),
    );
    s.note(format!("first leg: {streamed:?} in {:?}", first.elapsed()));
    let on_disk = spool_bytes(&b_state);
    let durable_at_crash = durable(&b_state);
    s.check(
        durable_at_crash > 0 && durable_at_crash < size,
        format!("killed mid-object: durable {durable_at_crash} of {size}"),
    );
    s.check(
        on_disk >= durable_at_crash,
        format!("spool {on_disk} holds the durable {durable_at_crash}"),
    );
    s.note(format!(
        "killed with {on_disk} spool bytes, {durable_at_crash} durable"
    ));
    let mut wb = WorkerProc::spawn(&b_state, &[], &[]).unwrap();
    wb.wait_ready(Duration::from_secs(120)).unwrap();
    s.check(
        spool_bytes(&b_state) == durable_at_crash,
        "the restart cut the spool back to its durable offset",
    );
    s.measure_from_now();
    let pump = Pump::new(&setup);
    let result = pump.run(&s, None);
    let after = result.as_ref().copied().unwrap_or(0);
    let replayed = (after + durable_at_crash).saturating_sub(size);
    s.metric_with(
        "replayed_bytes",
        replayed as f64,
        "bytes",
        json!({"durable_at_crash": durable_at_crash, "spool_at_crash": on_disk, "received_after": after}),
    );
    s.check(result.is_ok(), format!("pump: {result:?}"));
    s.check(complete(&wb), "object complete and verified after resume");
    s.check(replayed == 0, format!("{replayed} bytes replayed"));
    s.finish();
}
