//! WP4.INT task 4: the soak (REVAMP_PLAN §14 WP4.INT, `docs/VALIDATION.md`
//! *Soak*). Two real daemons paired both ways, each through its own
//! `FaultProxy` (A dials B through one, B dials A through the other), share
//! one folder. For `TKFS_SOAK_MINUTES` (default 120) both sides edit
//! continuously through their workers (new files, saves of existing ones,
//! directories, renames, deletes, a few larger files), an operation every
//! `TKFS_SOAK_PAUSE_MS_MIN`..`TKFS_SOAK_PAUSE_MS_MAX` ms on each side
//! (default 1000..4000). Each side edits names of its own, and
//! `TKFS_SOAK_SHARED_PERCENT` (default 10) of its operations go to a name
//! pool both sides edit, so concurrent edits conflict now and then. The
//! first WP4.INT runs used 50..400 ms with 100 % (a conflict storm) and with
//! 10 % shared: neither converged, activation fell behind (CCR-WP4.INT-3,
//! `docs/revamp/notes/wp4-int.md`).
//! Meanwhile the links cycle through
//! fault phases: healthy, latency changes (5 to 100 ms one way), bandwidth
//! caps (256 KiB/s to 20 MiB/s), stalls, connection resets, a blackhole and
//! refused connections. Every minute the scenario samples both daemons'
//! status (recorded in the evidence).
//!
//! Then the edits stop, the links are healthy again, and it passes when,
//! within `TKFS_SOAK_SETTLE_MINUTES` (default 30):
//!
//! * both folders read `up_to_date` (never stalled, blocked or needing
//!   attention at the end), every lane of both peers is ready, nothing is
//!   staged and every outbox row is acknowledged on both;
//! * the two replicas hold the same shared journal (equal feeds) and the same
//!   projection, every object referenced by a shared event is available on
//!   both (`content.complete`);
//! * the derived-state oracle (`Store::verify_derived_state`) is empty on
//!   both stores, read with the folders stopped.
//!
//! `cargo test --release --test qual_wp4_int_soak -- --ignored --nocapture`
mod support;

use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::core::Store;

const FILE: &str = "qual_wp4_int_soak";

fn minutes(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Edits through one side's worker, until `stop`.
struct Editor {
    edits: AtomicU64,
    refused: AtomicU64,
    bytes: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl Editor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            edits: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            last_error: Mutex::new(None),
        })
    }

    fn run(
        self: Arc<Self>,
        folder: Value,
        seed: u64,
        side: &'static str,
        shared_percent: u32,
        pause_ms: (u64, u64),
        stop: Arc<AtomicBool>,
    ) {
        let mut rng = StdRng::seed_from_u64(seed);
        let dirs = ["", "docs/", "src/", "media/"];
        while !stop.load(Ordering::Relaxed) {
            let dir = dirs[rng.gen_range(0..dirs.len())];
            // The shared pool (`f…`, both sides edit it) or this side's own.
            let pool = if rng.gen_range(0..100) < shared_percent {
                "f"
            } else {
                side
            };
            let name = format!("{dir}{pool}{:03}.dat", rng.gen_range(0..160));
            let op = rng.gen_range(0..100);
            let result = match op {
                0..=64 => {
                    let size = match rng.gen_range(0..100) {
                        0..=79 => rng.gen_range(16..16 * 1024),
                        80..=97 => rng.gen_range(16 * 1024..512 * 1024),
                        _ => rng.gen_range(1024 * 1024..4 * 1024 * 1024),
                    };
                    let mut bytes = vec![0u8; size];
                    rng.fill(&mut bytes[..]);
                    self.bytes.fetch_add(size as u64, Ordering::Relaxed);
                    try_rpc(
                        &folder,
                        json!({"op":"import","path":name,"hex":hex::encode(&bytes)}),
                    )
                }
                65..=72 => {
                    let target = format!(
                        "{}{pool}{:03}.dat",
                        dirs[rng.gen_range(0..dirs.len())],
                        rng.gen_range(0..160)
                    );
                    try_rpc(
                        &folder,
                        json!({"op":"rename","path":name,"target":target,"replace":false}),
                    )
                }
                73..=80 => try_rpc(&folder, json!({"op":"delete","path":name})),
                81..=83 => {
                    let path = dir.trim_end_matches('/');
                    if path.is_empty() {
                        continue;
                    }
                    try_rpc(&folder, json!({"op":"mkdir","path":path}))
                }
                _ => try_rpc(&folder, json!({"op":"status"})),
            };
            match result {
                Ok(_) => {
                    self.edits.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    // A missing name, an occupied target, a busy worker while
                    // it activates: refused edits change nothing.
                    self.refused.fetch_add(1, Ordering::Relaxed);
                    *self.last_error.lock().unwrap() = Some(describe_error(&error));
                }
            }
            thread::sleep(Duration::from_millis(
                rng.gen_range(pause_ms.0..=pause_ms.1),
            ));
        }
    }
}

/// One fault phase of both links.
fn phase(proxies: &[&FaultProxy], index: u64, rng: &mut StdRng) -> String {
    for proxy in proxies {
        proxy.clear();
    }
    match index % 9 {
        0 => "healthy".into(),
        1 => {
            let ms = [5, 25, 50, 100][rng.gen_range(0..4)];
            for proxy in proxies {
                proxy.set_latency(Duration::from_millis(ms));
            }
            format!("latency {ms} ms one way")
        }
        2 => {
            let rate =
                [256 * 1024, 1024 * 1024, 4 * 1024 * 1024, 20 * 1024 * 1024][rng.gen_range(0..4)];
            for proxy in proxies {
                proxy.set_bandwidth(rate);
                proxy.set_latency(Duration::from_millis(10));
            }
            format!("bandwidth {} KiB/s", rate / 1024)
        }
        3 => {
            let s = rng.gen_range(3..20);
            for proxy in proxies {
                proxy.stall_for(Duration::from_secs(s));
            }
            format!("stall {s} s")
        }
        4 => {
            for proxy in proxies {
                proxy.reset_all();
            }
            "reset".into()
        }
        5 => {
            for proxy in proxies {
                proxy.reset_after(rng.gen_range(64 * 1024..4 * 1024 * 1024));
                proxy.set_latency(Duration::from_millis(15));
            }
            "reset after some bytes".into()
        }
        6 => {
            for proxy in proxies {
                proxy.blackhole(true);
            }
            "blackhole".into()
        }
        7 => {
            for proxy in proxies {
                proxy.refuse(true);
                proxy.reset_all();
            }
            "refuse new connections".into()
        }
        _ => {
            for proxy in proxies {
                proxy.stall_after(
                    rng.gen_range(64 * 1024..2 * 1024 * 1024),
                    Duration::from_secs(5),
                );
                proxy.set_latency(Duration::from_millis(rng.gen_range(1..60)));
            }
            "stall after some bytes, jittery latency".into()
        }
    }
}

fn folder_status(node: &Node, state: &str) -> Value {
    node.raw_call(json!({"op":"status"}))
        .ok()
        .and_then(|status| {
            status["folders"]
                .as_array()
                .and_then(|folders| folders.iter().find(|f| f["state"] == state).cloned())
        })
        .unwrap_or(Value::Null)
}

fn peers_ready(node: &Node) -> bool {
    node.raw_call(json!({"op":"status"})).is_ok_and(|status| {
        status["peers"].as_array().is_some_and(|peers| {
            !peers.is_empty()
                && peers.iter().all(|peer| {
                    peer["lanes"].as_array().is_some_and(|lanes| {
                        lanes
                            .iter()
                            .filter(|lane| lane["direction"] == "outbound")
                            .all(|lane| lane["state"] == "ready")
                    })
                })
        })
    })
}

/// A short record of a folder's status for the evidence.
fn brief(status: &Value) -> Value {
    json!({
        "sync": status["sync"]["state"],
        "summary": status["sync"]["summary"],
        "events": status["journal"]["events"],
        "unacknowledged": status["journal"]["outgoing_unacknowledged"],
        "conflicts": status["journal"]["unresolved_conflicts"],
        "inbox": status["activation"]["inbox"],
        "complete": status["content"]["complete"],
        "missing_bytes": status["content"]["shared_missing_bytes"],
        // Open errors only (the list also keeps resolved ones).
        "errors": status["errors"].as_array().map(|errors| errors.iter()
            .filter(|e| e["resolved_at_ms"].is_null())
            .map(|e| e["error"]["code"].clone()).collect::<Vec<_>>()),
    })
}

fn settled(status: &Value) -> bool {
    status["sync"]["state"] == "up_to_date"
        && status["journal"]["outgoing_unacknowledged"] == 0
        && status["content"]["complete"] == true
        && [
            "ready",
            "waiting",
            "retry",
            "blocked",
            "quarantined",
            "unclassified",
        ]
        .iter()
        .all(|state| status["activation"]["inbox"][state].as_u64().unwrap_or(0) == 0)
}

#[test]
#[ignore = "WP4.INT soak (release, two hours by default)"]
fn soak_two_daemons_through_faulty_links() {
    let duration = Duration::from_secs(60 * minutes("TKFS_SOAK_MINUTES", 120));
    let settle = Duration::from_secs(60 * minutes("TKFS_SOAK_SETTLE_MINUTES", 30));
    let shared_percent = minutes("TKFS_SOAK_SHARED_PERCENT", 10).min(100) as u32;
    let pause_min = minutes("TKFS_SOAK_PAUSE_MS_MIN", 1000);
    let pause_ms = (
        pause_min,
        minutes("TKFS_SOAK_PAUSE_MS_MAX", 4000).max(pause_min),
    );
    let mut s = Scenario::with_budget(FILE, "soak", duration + settle + Duration::from_secs(900));
    // A short anti-entropy interval, as the harness daemons use: more
    // exchanges per minute through the faults.
    let a = Node::start(NodeConfig::default().interval(200).connections(3));
    let b = Node::start(NodeConfig::default().interval(200).connections(3));
    let pa = a.create("Soak", true);
    for i in 0..20 {
        write_file(&pa, &format!("f{i:03}.dat"), format!("seed {i}").as_bytes());
    }
    let to_a = FaultProxy::start(&a.endpoint());
    let to_b = FaultProxy::start(&b.endpoint());
    b.pair_endpoint(&to_a.endpoint(), &a.code(), Some(3));
    a.pair_endpoint(&to_b.endpoint(), &b.code(), Some(3));
    let pb = b.receive_from(&to_a.endpoint(), &pa);
    let (state_a, state_b) = (state_id(&pa), state_id(&pb));
    b.wait_for("the first sync", Duration::from_secs(300), || {
        has_file(&pb, "f019.dat", b"seed 19")
    });
    s.measure_from_now();

    let stop = Arc::new(AtomicBool::new(false));
    let editors = [Editor::new(), Editor::new()];
    let edit_threads: Vec<_> = [(&pa, 1u64, "a"), (&pb, 2u64, "b")]
        .into_iter()
        .zip(editors.iter())
        .map(|((folder, seed, side), editor)| {
            let (editor, folder, stop) = (editor.clone(), folder.clone(), stop.clone());
            thread::spawn(move || editor.run(folder, seed, side, shared_percent, pause_ms, stop))
        })
        .collect();

    let mut rng = StdRng::seed_from_u64(7);
    let started = Instant::now();
    let mut phases = Vec::new();
    let mut samples = Vec::new();
    let mut next_sample = Instant::now();
    let mut index = 0u64;
    while started.elapsed() < duration {
        let name = phase(&[&to_a, &to_b], index, &mut rng);
        let length = Duration::from_secs(rng.gen_range(20..60));
        phases.push(
            json!({"at_s": started.elapsed().as_secs(), "phase": name, "s": length.as_secs()}),
        );
        let until = Instant::now() + length;
        while Instant::now() < until && started.elapsed() < duration {
            if Instant::now() >= next_sample {
                next_sample += Duration::from_secs(60);
                samples.push(json!({
                    "at_s": started.elapsed().as_secs(),
                    "a": brief(&folder_status(&a, &state_a)),
                    "b": brief(&folder_status(&b, &state_b)),
                    "resets": to_a.resets() + to_b.resets(),
                }));
            }
            thread::sleep(Duration::from_millis(500));
        }
        index += 1;
    }
    stop.store(true, Ordering::Relaxed);
    for handle in edit_threads {
        handle.join().unwrap();
    }
    to_a.clear();
    to_b.clear();
    let edited = started.elapsed();

    // Settle: up to date on both sides, every lane ready.
    let settling = Instant::now();
    let converged = wait_until(settle, || {
        settled(&folder_status(&a, &state_a))
            && settled(&folder_status(&b, &state_b))
            && peers_ready(&a)
            && peers_ready(&b)
            && {
                let (sa, sb) = (folder_status(&a, &state_a), folder_status(&b, &state_b));
                sa["journal"]["shared_events"] == sb["journal"]["shared_events"]
            }
    });
    let settle_s = settling.elapsed().as_secs_f64();
    let final_a = folder_status(&a, &state_a);
    let final_b = folder_status(&b, &state_b);
    s.check(
        converged,
        format!(
            "both folders up to date, settled and ready within {settle:?}: A {} / B {}",
            brief(&final_a),
            brief(&final_b)
        ),
    );
    for (who, status) in [("A", &final_a), ("B", &final_b)] {
        let state = status["sync"]["state"].as_str().unwrap_or("?");
        s.check(
            !["stalled", "blocked", "needs_attention", "backoff"].contains(&state),
            format!("{who} ends {state}"),
        );
    }

    // Equal feeds, equal projections, empty oracles: on the stopped stores.
    a.stop_folder_exited(&pa, STOP_EXIT_TIMEOUT).unwrap();
    b.stop_folder_exited(&pb, STOP_EXIT_TIMEOUT).unwrap();
    let mut journals = Vec::new();
    for (who, folder) in [("A", &pa), ("B", &pb)] {
        let mut store = Store::open_existing(&state_dir(folder)).unwrap();
        let diff = store.verify_derived_state().unwrap();
        s.check(diff.is_empty(), format!("{who}'s oracle: {diff:?}"));
        s.check(
            store.inbox_counts().unwrap().total() == 0,
            format!("{who} has staged rows"),
        );
        s.check(
            store.outgoing_unacknowledged().unwrap() == 0,
            format!("{who} has unacknowledged outbox rows"),
        );
        let shared: BTreeSet<String> = store.shared_event_inventory().unwrap();
        let projection = store.projection(&store.active).unwrap();
        journals.push((shared, projection.entries));
    }
    s.check(
        journals[0].0 == journals[1].0,
        format!(
            "equal shared journals: A {} events, B {} events",
            journals[0].0.len(),
            journals[1].0.len()
        ),
    );
    s.check(journals[0].1 == journals[1].1, "equal projections");

    let edits: Vec<u64> = editors
        .iter()
        .map(|e| e.edits.load(Ordering::Relaxed))
        .collect();
    let refused: Vec<u64> = editors
        .iter()
        .map(|e| e.refused.load(Ordering::Relaxed))
        .collect();
    s.metric_with(
        "settle_s",
        if converged { settle_s } else { f64::NAN },
        "s",
        json!({
            "edit_minutes": edited.as_secs_f64() / 60.0,
            "shared_percent": shared_percent,
            "pause_ms": [pause_ms.0, pause_ms.1],
            "edits": edits,
            "refused_edits": refused,
            "last_refusal": editors.iter().map(|e| e.last_error.lock().unwrap().clone()).collect::<Vec<_>>(),
            "edit_bytes": editors.iter().map(|e| e.bytes.load(Ordering::Relaxed)).collect::<Vec<_>>(),
            "shared_events": journals[0].0.len(),
            "entries": journals[0].1.len(),
            "unresolved_conflicts": final_a["journal"]["unresolved_conflicts"],
            "proxy_resets": to_a.resets() + to_b.resets(),
            "proxy_connections": to_a.connections() + to_b.connections(),
            "proxy_bytes": to_a.bytes_up() + to_a.bytes_down() + to_b.bytes_up() + to_b.bytes_down(),
            "phases": phases.len(),
            "final": {"a": brief(&final_a), "b": brief(&final_b)},
        }),
    );
    s.metric("edits", (edits[0] + edits[1]) as f64, "count");
    s.note(format!(
        "phases: {}",
        serde_json::to_string(&phases).unwrap()
    ));
    s.note(format!(
        "samples: {}",
        serde_json::to_string(&samples).unwrap()
    ));
    s.finish();
}
