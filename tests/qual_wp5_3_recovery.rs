//! Qualification scenarios for WP5.3 (network recovery and demand), REVAMP_PLAN
//! §14.5b "WP5.3 Network recovery and demand".
//!
//! Two daemons, A (the source and the listener) and B (the dialer and the
//! receiver), with B's lanes through a [`FaultProxy`] capped at a Wi-Fi-like
//! rate. A 512 MiB object moves A → B; once a good part of it is durable at
//! B the link goes down for 45 s, then comes back. Two kinds of outage:
//!
//! * **clean close**: every connection is shut down at once and new ones are
//!   closed right after accept (the lab's `ssh -L` kill: the dialer's
//!   sockets see EOF/RST immediately, and redials fail at once);
//! * **silent blackhole**: connections stay open and every byte is dropped,
//!   new ones included (a Wi-Fi drop: nothing tells either side; sessions
//!   die by their heartbeat deadline, redials hang in the handshake).
//!
//! After the restore the scenario measures, from the moment the link is back:
//! every lane of B's peer `Ready` (`peers` op, ≤ 5 s), bytes flowing again
//! (B's durable spool offset advancing, ≤ 10 s; the wire too), and that the
//! transfer resumed from the durable offset (the offset never went back,
//! and what crossed the wire after the restore is the remainder, not the
//! object again). A timeline of every lane transition (the `status` op) and
//! of the durable offset is emitted as notes, which is how the causes were
//! found (`docs/revamp/notes/wp5-3.md`).
//!
//! The mutual-pairing variant pairs both ways (each daemon dials the other
//! through its own proxy, both proxies fail together) and checks that no
//! object is transferred twice: object bytes on all wires ≤ 1.1 × the size.
//!
//! `cargo test --release --test qual_wp5_3_recovery -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;

const FILE: &str = "qual_wp5_3_recovery";
const SETUP: Duration = Duration::from_secs(900);
const MIB: u64 = 1024 * 1024;
/// The lab's tunnel ran at about 7 MB/s; 8 MiB/s keeps a 512 MiB transfer
/// longer than a minute.
const LINK_RATE: u64 = 8 * MIB;
/// Lanes per peer, as in the lab (1 control + 3 bulk).
const CONNECTIONS: usize = 4;
/// How long the link stays down (the lab's outage).
fn outage() -> Duration {
    Duration::from_secs(
        std::env::var("TKFS_WP5_3_OUTAGE_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(45),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    CleanClose,
    Blackhole,
}

impl Fault {
    fn name(self) -> &'static str {
        match self {
            Fault::CleanClose => "clean_close",
            Fault::Blackhole => "blackhole",
        }
    }
    fn begin(self, proxy: &FaultProxy) {
        match self {
            Fault::CleanClose => {
                proxy.refuse(true);
                proxy.reset_all();
            }
            Fault::Blackhole => proxy.blackhole(true),
        }
    }
    fn end(self, proxy: &FaultProxy) {
        match self {
            Fault::CleanClose => proxy.refuse(false),
            Fault::Blackhole => proxy.blackhole(false),
        }
    }
}

/// The receiver's spool of `hash`: `(durable offset, spool file length)`,
/// `None` once the object is installed (or before its spool exists).
fn spool(state: &Path, hash: &str) -> Option<(u64, u64)> {
    let len = std::fs::metadata(state.join("incoming-objects").join(hash))
        .ok()?
        .len();
    let db = rusqlite::Connection::open_with_flags(
        state.join("metadata.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    let _ = db.busy_timeout(Duration::from_millis(500));
    let durable: i64 = db
        .query_row(
            "SELECT durable FROM spool_state WHERE hash=?",
            [hash],
            |row| row.get(0),
        )
        .ok()?;
    Some((u64::try_from(durable).unwrap_or(0), len))
}

fn installed(state: &Path, hash: &str, size: u64) -> bool {
    std::fs::metadata(state.join("objects").join(hash)).is_ok_and(|m| m.len() == size)
}

/// `(ready lanes, lanes)` of `node`'s outbound peer whose endpoint is `endpoint`.
fn lanes_ready(node: &Node, endpoint: &str) -> (usize, usize) {
    let Ok(peers) = node.raw_call(json!({"op":"peers"})) else {
        return (0, 0);
    };
    let list = peers
        .as_array()
        .cloned()
        .or_else(|| peers["peers"].as_array().cloned())
        .unwrap_or_default();
    list.iter()
        .find(|peer| peer["endpoint"] == endpoint)
        .and_then(|peer| peer["lanes"].as_array())
        .map(|lanes| {
            (
                lanes
                    .iter()
                    .filter(|lane| lane["connected"] == true)
                    .count(),
                lanes.len(),
            )
        })
        .unwrap_or((0, 0))
}

/// Lane states of `node`'s outbound peer `installation` from the `status` op:
/// `lane → (state, backoff_until_ms, error code)`.
fn lane_states(node: &Node, installation: &str) -> Vec<(u64, String, Option<u64>, String)> {
    let Ok(status) = node.raw_call(json!({"op":"status"})) else {
        return vec![];
    };
    status["peers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|peer| peer["installation"] == installation)
        .flat_map(|peer| peer["lanes"].as_array().cloned().unwrap_or_default())
        .filter(|lane| lane["direction"] == "outbound")
        .map(|lane| {
            (
                lane["lane"].as_u64().unwrap_or(0),
                lane["state"].as_str().unwrap_or("?").to_owned(),
                lane["backoff_until_ms"].as_u64(),
                lane["error"]["code"].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect()
}

/// What the sampler saw, relative to the scenario's clock.
#[derive(Default)]
struct Timeline {
    /// `(t_s, text)` of every lane transition and offset milestone.
    events: Vec<(f64, String)>,
    /// `(t_s, durable)` samples.
    durable: Vec<(f64, u64)>,
}

/// Samples the outbound lane states of every `(label, daemon, peer
/// installation)` and B's durable offset every 100 ms until stopped.
fn sampler(
    dialers: Vec<(&'static str, Arc<Node>, String)>,
    state: PathBuf,
    hash: String,
    clock: Instant,
    stop: Arc<AtomicBool>,
    timeline: Arc<Mutex<Timeline>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut last: std::collections::BTreeMap<
            (&'static str, u64),
            (String, String, Option<u64>),
        > = Default::default();
        while !stop.load(Ordering::Relaxed) {
            let t = clock.elapsed().as_secs_f64();
            let lanes = dialers.iter().flat_map(|(label, node, installation)| {
                lane_states(node, installation)
                    .into_iter()
                    .map(move |lane| (*label, lane))
            });
            for (label, (lane, state, until, code)) in lanes {
                // A new backoff deadline is a new attempt.
                let key = (state.clone(), code.clone(), until);
                if last.get(&(label, lane)) != Some(&key) {
                    let backoff = until
                        .map(|until| {
                            let now = tkfs::core::time();
                            format!(" backoff {} ms", until.saturating_sub(now))
                        })
                        .unwrap_or_default();
                    let code = if code.is_empty() {
                        String::new()
                    } else {
                        format!(" ({code})")
                    };
                    lock(&timeline)
                        .events
                        .push((t, format!("{label} lane {lane} {state}{code}{backoff}")));
                    last.insert((label, lane), key);
                }
            }
            if let Some((durable, _)) = spool(&state, &hash) {
                lock(&timeline).durable.push((t, durable));
            }
            thread::sleep(Duration::from_millis(100));
        }
    })
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|p| p.into_inner())
}

/// One outage scenario (module docs). `mutual`: A pairs with B too.
fn recovery(fault: Fault, mutual: bool) {
    let name = format!(
        "{}{}_outage_mid_512mib",
        if mutual { "mutual_" } else { "" },
        fault.name()
    );
    let mut s = Scenario::with_budget(FILE, &name, budget_or(600));
    let size = scaled(512) as u64 * MIB;
    let config = NodeConfig::default().connections(CONNECTIONS);
    let a = Arc::new(Node::start(config.clone()));
    let b = Arc::new(Node::start(config));
    let pa = a.create("Recovery", true);
    let hash = a
        .with_stopped_folder(&pa, SETUP, |st| large_object_in(st, "big.bin", size, 53))
        .unwrap();
    let to_a = FaultProxy::start(&a.endpoint());
    to_a.set_bandwidth(LINK_RATE);
    let to_b = mutual.then(|| {
        let proxy = FaultProxy::start(&b.endpoint());
        proxy.set_bandwidth(LINK_RATE);
        proxy
    });
    b.pair_endpoint(&to_a.endpoint(), &a.code(), Some(CONNECTIONS));
    if let Some(to_b) = &to_b {
        a.pair_endpoint(&to_b.endpoint(), &b.code(), Some(CONNECTIONS));
    }
    s.measure_from_now();
    let clock = Instant::now();
    let pb = b.receive_from(&to_a.endpoint(), &pa);
    let state = state_dir(&pb);
    let timeline = Arc::new(Mutex::new(Timeline::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut dialers = vec![("B", b.clone(), a.installation())];
    if mutual {
        dialers.push(("A", a.clone(), b.installation()));
    }
    let sampling = sampler(
        dialers,
        state.clone(),
        hash.clone(),
        clock,
        stop.clone(),
        timeline.clone(),
    );
    let wire = |to_a: &FaultProxy, to_b: &Option<FaultProxy>| {
        to_a.bytes_up()
            + to_a.bytes_down()
            + to_b.as_ref().map_or(0, |p| p.bytes_up() + p.bytes_down())
    };

    // 1. A good part of the object is durable at B.
    let cut_at = size / 5;
    let started = wait_until(s.remaining(), || {
        spool(&state, &hash).is_some_and(|(durable, _)| durable >= cut_at)
    });
    if !s.check(
        started,
        format!("{} MiB durable before the outage", cut_at / MIB),
    ) {
        stop.store(true, Ordering::Relaxed);
        let _ = sampling.join();
        s.finish();
        return;
    }
    let (ready_before, lanes) = lanes_ready(&b, &to_a.endpoint());
    s.note(format!(
        "before the outage: {ready_before}/{lanes} lanes ready"
    ));

    // 2. The outage.
    let down_at = clock.elapsed().as_secs_f64();
    fault.begin(&to_a);
    if let Some(to_b) = &to_b {
        fault.begin(to_b);
    }
    let durable_at_cut = spool(&state, &hash).map_or(0, |(durable, _)| durable);
    lock(&timeline).events.push((
        down_at,
        format!("outage begins at durable {durable_at_cut}"),
    ));
    thread::sleep(outage());
    let durable_before_restore = spool(&state, &hash).map_or(0, |(durable, _)| durable);

    // 3. The restore.
    let restored = Instant::now();
    let restore_at = clock.elapsed().as_secs_f64();
    let wire_at_restore = wire(&to_a, &to_b);
    let pulled_at_restore = to_a.bytes_down();
    let pushed_at_restore = to_b.as_ref().map_or(0, FaultProxy::bytes_up);
    fault.end(&to_a);
    if let Some(to_b) = &to_b {
        fault.end(to_b);
    }
    lock(&timeline).events.push((
        restore_at,
        format!("link restored at durable {durable_before_restore}"),
    ));
    let mut first_lane: Option<f64> = None;
    let mut all_lanes: Option<f64> = None;
    let mut wire_flowing: Option<f64> = None;
    let mut carrier = "none";
    let mut durable_flowing: Option<f64> = None;
    let mut min_after = u64::MAX;
    let budget = s.remaining();
    let complete = wait_until(budget, || {
        let t = restored.elapsed().as_secs_f64();
        if all_lanes.is_none() {
            let (ready, lanes) = lanes_ready(&b, &to_a.endpoint());
            if ready > 0 && first_lane.is_none() {
                first_lane = Some(t);
            }
            if lanes > 0 && ready == lanes {
                all_lanes = Some(t);
            }
        }
        if wire_flowing.is_none() {
            // Object bytes A → B: B's pulls (down through `to_a`) or, under
            // mutual pairing, A's pushes (up through `to_b`).
            let pushed = to_b.as_ref().map_or(0, FaultProxy::bytes_up);
            if to_a.bytes_down() > pulled_at_restore + MIB {
                wire_flowing = Some(t);
                carrier = "pull";
            } else if pushed > pushed_at_restore + MIB {
                wire_flowing = Some(t);
                carrier = "push";
            }
        }
        match spool(&state, &hash) {
            Some((durable, _)) => {
                min_after = min_after.min(durable);
                if durable_flowing.is_none() && durable > durable_before_restore {
                    durable_flowing = Some(t);
                }
            }
            None if installed(&state, &hash, size) => {
                durable_flowing.get_or_insert(t);
                return all_lanes.is_some() || mutual;
            }
            None => {}
        }
        false
    });
    let complete_s = restored.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    let _ = sampling.join();
    {
        let timeline = lock(&timeline);
        for (t, text) in &timeline.events {
            s.note(format!("t={t:.2}s {text}"));
        }
        // The last durable sample before the cut and the first one that moved
        // after the restore.
        let moved = timeline
            .durable
            .iter()
            .find(|(t, durable)| *t > restore_at && *durable > durable_before_restore);
        if let Some((t, durable)) = moved {
            s.note(format!(
                "t={t:.2}s durable moved to {durable} ({:.2} s after the restore)",
                t - restore_at
            ));
        }
    }
    let wire_after = wire(&to_a, &to_b).saturating_sub(wire_at_restore);
    let remainder = size.saturating_sub(durable_before_restore);
    s.metric_with(
        "restore_to_lanes_ready_s",
        all_lanes.unwrap_or(f64::NAN),
        "s",
        json!({"first_lane_s": first_lane, "lanes": CONNECTIONS, "fault": fault.name()}),
    );
    s.metric_with(
        "restore_to_bytes_s",
        durable_flowing.unwrap_or(f64::NAN),
        "s",
        json!({"wire_s": wire_flowing, "carrier": carrier, "fault": fault.name()}),
    );
    s.metric_with(
        "restore_to_complete_s",
        if complete { complete_s } else { f64::NAN },
        "s",
        json!({"outage_s": outage().as_secs(), "size": size}),
    );
    s.metric_with(
        "resume_wire_ratio",
        wire_after as f64 / remainder.max(1) as f64,
        "ratio",
        json!({"wire_after_restore": wire_after, "remainder": remainder,
               "durable_at_cut": durable_at_cut, "durable_at_restore": durable_before_restore}),
    );
    if mutual {
        let total = wire(&to_a, &to_b);
        s.metric_with(
            "wire_ratio",
            total as f64 / size as f64,
            "ratio",
            json!({"wire_bytes": total, "object_bytes": size}),
        );
        s.check(
            total as f64 <= size as f64 * 1.1,
            format!("mutual pairing moved the object once: {total} wire bytes for {size}"),
        );
    }
    if !complete {
        s.timeout("transfer incomplete after the restore");
    }
    s.check(
        all_lanes.is_some_and(|t| t <= 5.0),
        format!("lanes Ready within 5 s of the restore ({all_lanes:?})"),
    );
    s.check(
        durable_flowing.is_some_and(|t| t <= 10.0),
        format!("bytes flowing within 10 s of the restore ({durable_flowing:?})"),
    );
    s.check(
        min_after == u64::MAX || min_after >= durable_at_cut,
        format!(
            "the transfer resumed from the durable offset: {min_after} after the restore, \
             {durable_at_cut} at the cut"
        ),
    );
    s.check(
        wire_after as f64 <= remainder as f64 * 1.1 + 32.0 * MIB as f64,
        format!("no blind replay: {wire_after} wire bytes after the restore for {remainder} left"),
    );
    if complete {
        s.check(has_hash(&pb, "big.bin", &hash), "big.bin arrived whole");
    }
    s.finish();
}

/// The lab's fault: the tunnel's sockets close at once and redials are refused.
#[test]
#[ignore = "WP5.3"]
fn clean_close_outage_mid_512mib() {
    recovery(Fault::CleanClose, false);
}

/// A silent link loss: sessions die by their heartbeat deadline and redials
/// hang in their handshake until the link is back.
#[test]
#[ignore = "WP5.3"]
fn blackhole_outage_mid_512mib() {
    recovery(Fault::Blackhole, false);
}

/// Mutual pairing through an outage: the object still moves once.
#[test]
#[ignore = "WP5.3"]
fn mutual_clean_close_outage_mid_512mib() {
    recovery(Fault::CleanClose, true);
}

/// Mutual pairing through a silent outage.
#[test]
#[ignore = "WP5.3"]
fn mutual_blackhole_outage_mid_512mib() {
    recovery(Fault::Blackhole, true);
}
