//! WP3.4: the desktop view model (`tkfs::ui_model`, REVAMP_PLAN §7.4).
//!
//! * Golden texts for every `FolderSyncState`, `FolderPeerState`,
//!   `PeerConnState`, lifecycle phase, mount state and gate kind.
//! * A non-sharing peer is ignored; a needs-upgrade peer and a stalled
//!   folder render as specified; errors disappear when absent from a
//!   snapshot; every open error has its class, time and a suggestion.
//! * The background loops: commands answer in < 200 ms while a slow
//!   subscription is in flight; the subscription reconnects with backoff,
//!   respawns a daemon that is gone (never after a desktop Shutdown) and
//!   follows a restarted daemon's generations.
//! * The CCR-WP2.1-7 display adapter (`MountSmoother`).
//! * The four unit tests that lived in `src/gui.rs`, moved here against the
//!   new model (§14 WP3.4 task 4).
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    model::{
        errors::{ErrorClass, ErrorRecord, ErrorWire, Scope},
        health::{
            ActivationCounters, ContentCounters, InboxCounts, JournalCounters, MountIntent,
            MountState, MountStatus, StoreHealth,
        },
        orchestrator::ExitRecord,
        status::{
            ActivityEntry, DaemonStatus, Direction, FolderPeerState, FolderPeerSync, FolderStatus,
            FolderSync, FolderSyncState, GateKind, GateStatus, InflightOp, Intents, LaneRole,
            LaneState, LaneStatus, LifecyclePhase, LifecycleStatus, MetaProgress, PeerConnState,
            PeerStatus, StartupProgress, StatusSnapshot, TransferProgress,
        },
    },
    orchestrator::Action,
    ui_model::{
        self, Busy, Command, CommandOutcome, CommandPool, CommandTransport, DaemonStarter, Feed,
        FeedOptions, FeedUpdate, FolderAction, MountSmoother, Names, Now, RateMeter,
        StatusTransport, Tone, TransportError,
    },
};

/// 2026-10-14 13:20:00 UTC.
const NOW: u64 = 1_792_070_400_000;
const SEC: u64 = 1000;
const MIN: u64 = 60 * SEC;
const GIB: u64 = 1 << 30;
const MIB: u64 = 1 << 20;

// -- builders ----------------------------------------------------------------

fn wire(code: &str) -> ErrorWire {
    ErrorWire::coded(code, "detail")
}

fn record(code: &str, scope: Scope, at: u64) -> ErrorRecord {
    ErrorRecord::new(wire(code), scope, at)
}

/// A running, mounted, shared, up-to-date folder.
fn folder(state: &str, label: &str) -> FolderStatus {
    FolderStatus {
        state: state.into(),
        label: label.into(),
        repo: format!("repo-{state}"),
        device: format!("device-{state}"),
        lifecycle: LifecycleStatus {
            phase: LifecyclePhase::Running,
            since_ms: NOW - 3 * 3600 * SEC,
            pid: Some(4242),
            instance: Some(format!("inst-{state}")),
            ..LifecycleStatus::default()
        },
        intents: Intents {
            desired_running: true,
            sync_enabled: true,
            mount_enabled: true,
        },
        mount: MountStatus {
            intent: MountIntent::Attach,
            path: Some(PathBuf::from(format!("C:\\Telekinesis\\{label}"))),
            state: MountState::Attached {
                since_ms: NOW - 3 * 3600 * SEC,
            },
        },
        sync: FolderSync {
            state: FolderSyncState::UpToDate,
            summary: "Up to date · 1 of 1 peer".into(),
            peers: vec![pair("office", FolderPeerState::UpToDate)],
        },
        journal: JournalCounters {
            events: 120,
            shared_events: 100,
            tip: 120,
            ..JournalCounters::default()
        },
        content: ContentCounters {
            current_objects: 10,
            current_available: 10,
            complete: true,
            ..ContentCounters::default()
        },
        ..FolderStatus::default()
    }
}

fn pair(installation: &str, state: FolderPeerState) -> FolderPeerSync {
    FolderPeerSync {
        installation: installation.into(),
        endpoint: Some(format!("ws://{installation}:43180")),
        label: None,
        direction: Direction::Outbound,
        state,
        metadata: MetaProgress {
            caught_up: state == FolderPeerState::UpToDate,
            ..MetaProgress::default()
        },
        ..FolderPeerSync::default()
    }
}

fn peer(installation: &str, state: PeerConnState) -> PeerStatus {
    PeerStatus {
        installation: installation.into(),
        endpoint: Some(format!("ws://{installation}:43180")),
        label: Some(installation.into()),
        direction: Direction::Outbound,
        state,
        ..PeerStatus::default()
    }
}

fn snapshot(folders: Vec<FolderStatus>, peers: Vec<PeerStatus>) -> StatusSnapshot {
    StatusSnapshot {
        schema: 2,
        generation: 7,
        at_ms: NOW,
        daemon: DaemonStatus {
            installation: "installation-1234".into(),
            version: "0.1.0".into(),
            pid: 999,
            started_at_ms: NOW - 2 * 3600 * SEC - 5 * MIN,
            control: "ws://127.0.0.1:5000".into(),
            network: Some("ws://0.0.0.0:43180".into()),
            ..DaemonStatus::default()
        },
        folders,
        peers,
        activity: Vec::new(),
    }
}

fn one(f: FolderStatus) -> StatusSnapshot {
    snapshot(vec![f], vec![peer("office", PeerConnState::Online)])
}

fn row(s: &StatusSnapshot) -> ui_model::FolderRowModel {
    ui_model::folder_rows(s).remove(0)
}

fn sync_text(f: FolderStatus) -> (String, Tone) {
    let r = row(&one(f));
    (r.sync.text, r.sync.tone)
}

fn with_sync(
    mut f: FolderStatus,
    state: FolderSyncState,
    peers: Vec<FolderPeerSync>,
) -> FolderStatus {
    f.sync.state = state;
    f.sync.peers = peers;
    f
}

// -- formats -----------------------------------------------------------------

#[test]
fn number_and_time_formats() {
    assert_eq!(ui_model::format_bytes(512), "512 B");
    assert_eq!(ui_model::format_bytes(3 * GIB + GIB / 10), "3.1 GiB");
    assert_eq!(ui_model::format_rate(3 * MIB + 400 * 1024), "3.4 MiB/s");
    assert_eq!(
        ui_model::format_eta(Some(360)).as_deref(),
        Some("ETA 6 min")
    );
    assert_eq!(ui_model::format_eta(Some(45)).as_deref(), Some("ETA 45 s"));
    assert_eq!(ui_model::format_eta(None), None);
    assert_eq!(ui_model::group(1_234_567), "1,234,567");
    assert_eq!(ui_model::count(1, "file", "files"), "1 file");
    assert_eq!(ui_model::count(1200, "file", "files"), "1,200 files");
    let now = Now::at(NOW);
    assert_eq!(ui_model::ago(now, NOW - 4 * MIN), "4 min ago");
    assert_eq!(ui_model::ago(now, NOW + 5 * SEC), "just now");
    // 1970-01-02 01:00:00 UTC is 1970-01-01 22:00:00 at UTC-3.
    let ms = 86_400_000 + 3_600_000;
    let local = Now::at(ms).with_offset(-180);
    assert_eq!(ui_model::clock(local, ms), "22:00:00");
    assert_eq!(ui_model::clock(Now::at(ms), ms), "01:00:00");
    let next_day = Now::at(ms + 86_400_000).with_offset(-180);
    assert_eq!(ui_model::clock(next_day, ms), "1970-01-01 22:00");
    assert_eq!(ui_model::clip("a  b\n c", 10), "a b c");
    assert_eq!(ui_model::clip("abcdefghijkl", 4), "abcd...");
}

// -- every FolderSyncState ---------------------------------------------------

#[test]
fn every_folder_sync_state_has_its_chip() {
    use FolderPeerState as P;
    use FolderSyncState as F;

    // Stopped (not desired) / not running.
    let mut f = folder("a", "Docs");
    f.lifecycle.phase = LifecyclePhase::Stopped;
    f.intents.desired_running = false;
    f.mount.state = MountState::Detached;
    let r = row(&one(with_sync(f.clone(), F::Stopped, vec![])));
    assert_eq!(
        (r.sync.text.as_str(), r.sync.tone),
        ("Stopped", Tone::Muted)
    );
    assert_eq!(r.sync.action, Some(FolderAction::Start));
    let mut starting = f.clone();
    starting.lifecycle.phase = LifecyclePhase::Starting;
    starting.intents.desired_running = true;
    assert_eq!(
        sync_text(with_sync(starting, F::Stopped, vec![])).0,
        "Not syncing — worker starting"
    );

    assert_eq!(
        sync_text(with_sync(folder("a", "Docs"), F::NotShared, vec![])),
        ("Not shared with any peer".into(), Tone::Muted)
    );

    let mut paused = folder("a", "Docs");
    paused.intents.sync_enabled = false;
    let r = row(&one(with_sync(
        paused,
        F::Paused,
        vec![pair("office", P::Paused)],
    )));
    assert_eq!((r.sync.text.as_str(), r.sync.tone), ("Paused", Tone::Muted));
    assert_eq!(r.sync.action, Some(FolderAction::ResumeSync));

    // Offline: one peer, with how long.
    let mut s = snapshot(
        vec![with_sync(
            folder("a", "Docs"),
            F::Offline,
            vec![pair("office", P::Offline)],
        )],
        vec![PeerStatus {
            since_ms: Some(NOW - 5 * MIN),
            ..peer("office", PeerConnState::Offline)
        }],
    );
    let r = row(&s);
    assert_eq!(r.sync.text, "Waiting for office (offline 5 min)");
    assert_eq!(r.sync.tone, Tone::Warn);
    // Several peers.
    s.folders[0].sync.peers = vec![pair("office", P::Offline), pair("home", P::Incompatible)];
    assert_eq!(
        row(&s).sync.text,
        "Waiting for 2 peers (needs upgrade, offline)"
    );

    // Initial sync, with metadata n of m.
    let mut gated = folder("a", "Docs");
    gated.gate = GateStatus {
        state: GateKind::Awaiting,
        source: Some("office".into()),
        since_ms: Some(NOW - MIN),
        waiting_on: vec!["412 events to receive".into()],
    };
    gated.mount.state = MountState::Detached;
    let mut p = pair("office", P::Metadata);
    p.metadata.to_receive = 412;
    p.metadata.cursor_seq = 1588;
    p.metadata.remote_tip = 2000;
    let r = row(&one(with_sync(gated, F::InitialSync, vec![p.clone()])));
    assert_eq!(
        r.sync.text,
        "Initial sync from office · 1,588 of 2,000 events"
    );
    assert_eq!(r.sync.tone, Tone::Progress);
    assert!((r.progress.unwrap() - 0.794).abs() < 0.001);

    // Metadata: n of m and how many left.
    let r = row(&one(with_sync(
        folder("a", "Docs"),
        F::Metadata,
        vec![p.clone()],
    )));
    assert_eq!(r.sync.text, "Syncing metadata · 1,588 of 2,000 · 412 left");
    // Without a known tip.
    let mut q = pair("office", P::Metadata);
    q.metadata.to_send = 12;
    assert_eq!(
        sync_text(with_sync(folder("a", "Docs"), F::Metadata, vec![q])).0,
        "Syncing metadata · 12 left"
    );

    // Downloading: done of total, rate, ETA.
    let mut d = pair("office", P::Downloading);
    d.objects_down = TransferProgress {
        objects_remaining: 40,
        bytes_remaining: 2 * GIB - GIB / 5, // 1.8 GiB left
        total_bytes: 3 * GIB,
        rate_bps: 3 * MIB + 400 * 1024,
        ..TransferProgress::default()
    };
    let r = row(&one(with_sync(
        folder("a", "Docs"),
        F::Downloading,
        vec![d.clone()],
    )));
    assert_eq!(
        r.sync.text,
        "Downloading 1.2 GiB of 3.0 GiB · 3.4 MiB/s · ETA 10 min"
    );
    assert!((r.progress.unwrap() - 0.4).abs() < 0.01);

    // Uploading: files when only counts are known.
    let mut u = pair("office", P::Uploading);
    u.objects_up.objects_remaining = 120;
    assert_eq!(
        sync_text(with_sync(
            folder("a", "Docs"),
            F::Uploading,
            vec![u.clone()]
        ))
        .0,
        "Uploading 120 files"
    );

    // Syncing: both directions and metadata.
    let mut both = d.clone();
    both.state = P::Syncing;
    both.objects_up.objects_remaining = 120;
    both.metadata.to_send = 3;
    let text = sync_text(with_sync(folder("a", "Docs"), F::Syncing, vec![both])).0;
    assert!(text.starts_with("Syncing · ↓ 1.2 GiB of 3.0 GiB"), "{text}");
    assert!(text.contains("↑ 120 files"), "{text}");
    assert!(text.ends_with("3 events"), "{text}");

    // Blocked: why.
    let mut b = pair("office", P::Blocked);
    b.metadata.quarantined = 3;
    let r = row(&one(with_sync(folder("a", "Docs"), F::Blocked, vec![b])));
    assert_eq!(r.sync.text, "Blocked — 3 events quarantined");
    assert_eq!(r.sync.tone, Tone::Error);
    assert_eq!(r.sync.action, Some(FolderAction::Revalidate));
    assert!(r.attention);

    // Backoff: next retry and the code.
    let mut k = pair("office", P::Backoff);
    k.backoff_until_ms = Some(NOW + 12 * SEC);
    k.error = Some(record(
        "CONNECTION_LOST",
        Scope::FolderPeer {
            state: "a".into(),
            installation: "office".into(),
        },
        NOW - SEC,
    ));
    assert_eq!(
        sync_text(with_sync(folder("a", "Docs"), F::Backoff, vec![k])),
        ("Retrying in 12 s — CONNECTION_LOST".into(), Tone::Warn)
    );

    // Stalled: since and the culprit.
    let r = row(&stalled_snapshot());
    assert_eq!(r.sync.text, "Stalled 4 min — CORRUPT_OBJECT 1a2b3c4d5e6f");
    assert_eq!(r.sync.tone, Tone::Error);
    assert!(r.attention);

    // NeedsAttention: the reason and its remedy.
    let mut failed = folder("a", "Docs");
    failed.lifecycle.phase = LifecyclePhase::Failed;
    failed.lifecycle.detail = Some("CRASH_LOOP: 5 failures in 10 min".into());
    failed.mount.state = MountState::Detached;
    let r = row(&one(with_sync(failed, F::NeedsAttention, vec![])));
    assert_eq!(r.sync.text, "Needs attention: crash loop");
    assert_eq!(r.sync.action, Some(FolderAction::Retry));
    let mut mount_failed = folder("a", "Docs");
    mount_failed.mount.state = MountState::AttachFailed {
        error: wire("MOUNT_PATH_MUST_NOT_EXIST"),
        attempt: 1,
        next_retry_at_ms: None,
    };
    let r = row(&one(with_sync(mount_failed, F::NeedsAttention, vec![])));
    assert_eq!(
        r.sync.text,
        "Needs attention: mount failed — MOUNT_PATH_MUST_NOT_EXIST"
    );
    assert_eq!(r.sync.action, Some(FolderAction::Attach));

    // UpToDate, and with a peer away.
    assert_eq!(
        sync_text(folder("a", "Docs")),
        ("Up to date".into(), Tone::Ok)
    );
    let away = with_sync(
        folder("a", "Docs"),
        F::UpToDate,
        vec![pair("office", P::UpToDate), pair("home", P::Offline)],
    );
    assert_eq!(sync_text(away).0, "Up to date · 1 of 2 peers unreachable");
}

fn stalled_snapshot() -> StatusSnapshot {
    let mut p = pair("office", FolderPeerState::Stalled);
    p.objects_down.objects_remaining = 1;
    p.objects_down.bytes_remaining = 10 * MIB;
    p.last_progress_ms = Some(NOW - 4 * MIN);
    p.stalled_since_ms = Some(NOW - 3 * MIN);
    p.error = Some(record(
        "CORRUPT_OBJECT",
        Scope::Object {
            state: "a".into(),
            hash: "1a2b3c4d5e6f7a8b9c0d".into(),
        },
        NOW - 30 * SEC,
    ));
    one(with_sync(
        folder("a", "Docs"),
        FolderSyncState::Stalled,
        vec![p],
    ))
}

#[test]
fn chip_text_is_the_rows_sync_chip() {
    let s = stalled_snapshot();
    assert_eq!(
        ui_model::chip_text(&s.folders[0], &s.peers, Now::of(&s)),
        row(&s).sync.text
    );
}

// -- every FolderPeerState ---------------------------------------------------

#[test]
fn every_folder_peer_state_has_its_chip() {
    use FolderPeerState as P;
    let s = snapshot(
        vec![],
        vec![
            PeerStatus {
                since_ms: Some(NOW - 2 * MIN),
                ..peer("office", PeerConnState::Offline)
            },
            PeerStatus {
                lanes: vec![LaneStatus {
                    state: LaneState::Backoff,
                    backoff_until_ms: Some(NOW + 9 * SEC),
                    ..LaneStatus::default()
                }],
                ..peer("home", PeerConnState::Backoff)
            },
        ],
    );
    let names = Names::of(&s);
    let now = Now::of(&s);
    let chip = |p: FolderPeerSync| ui_model::folder_peer_chip(&p, &names, now);
    let mut meta = pair("office", P::Metadata);
    meta.metadata.to_receive = 10;
    meta.metadata.cursor_seq = 90;
    meta.metadata.remote_tip = 100;
    let mut down = pair("office", P::Downloading);
    down.objects_down.bytes_remaining = MIB;
    let mut up = pair("office", P::Uploading);
    up.objects_up.objects_remaining = 2;
    let mut blocked = pair("office", P::Blocked);
    blocked.metadata.rejected = 1;
    let mut stalled = pair("office", P::Stalled);
    stalled.last_progress_ms = Some(NOW - 90 * SEC);
    let cases: Vec<(FolderPeerSync, &str, Tone)> = vec![
        (pair("office", P::NotShared), "Not shared", Tone::Muted),
        (pair("office", P::Offline), "Offline 2 min", Tone::Muted),
        (pair("office", P::Connecting), "Connecting…", Tone::Progress),
        (
            pair("office", P::Incompatible),
            "Peer needs upgrade",
            Tone::Error,
        ),
        (
            pair("office", P::Unauthorized),
            "Unauthorized — re-pair",
            Tone::Error,
        ),
        (
            pair("office", P::WaitingForWorker),
            "Waiting for the local worker",
            Tone::Muted,
        ),
        (
            pair("office", P::WaitingForRemote),
            "Waiting for the peer's folder",
            Tone::Warn,
        ),
        (pair("office", P::Paused), "Paused", Tone::Muted),
        (
            meta,
            "Syncing metadata · 90 of 100 · 10 left",
            Tone::Progress,
        ),
        (down, "Downloading 1.0 MiB left", Tone::Progress),
        (up, "Uploading 2 files", Tone::Progress),
        (pair("office", P::Syncing), "Syncing", Tone::Progress),
        (blocked, "Blocked — 1 event rejected", Tone::Error),
        (
            pair("home", P::Backoff),
            "Backoff (retry in 9 s)",
            Tone::Warn,
        ),
        (stalled, "Stalled since 1 min ago", Tone::Error),
        (pair("office", P::UpToDate), "Up to date", Tone::Ok),
    ];
    assert_eq!(cases.len(), 16, "every FolderPeerState");
    for (p, text, tone) in cases {
        let state = p.state;
        let c = chip(p);
        assert_eq!((c.text.as_str(), c.tone), (text, tone), "{state:?}");
    }
}

// -- every PeerConnState -----------------------------------------------------

#[test]
fn every_peer_badge_and_the_inbound_only_peer() {
    let backoff = PeerStatus {
        lanes: vec![
            LaneStatus {
                lane: 0,
                state: LaneState::Backoff,
                backoff_until_ms: Some(NOW + 12 * SEC),
                ..LaneStatus::default()
            },
            LaneStatus {
                lane: 1,
                role: LaneRole::Bulk,
                state: LaneState::Backoff,
                backoff_until_ms: Some(NOW + 30 * SEC),
                ..LaneStatus::default()
            },
        ],
        ..peer("b", PeerConnState::Backoff)
    };
    let cases = vec![
        (peer("a", PeerConnState::Online), "Online", Tone::Ok),
        (
            peer("c", PeerConnState::Connecting),
            "Connecting…",
            Tone::Progress,
        ),
        (
            PeerStatus {
                since_ms: Some(NOW - 5 * MIN),
                ..peer("d", PeerConnState::Offline)
            },
            "Offline 5 min",
            Tone::Muted,
        ),
        (backoff, "Backoff (retry in 12 s)", Tone::Warn),
        (
            peer("e", PeerConnState::Incompatible),
            "Peer needs upgrade",
            Tone::Error,
        ),
        (
            peer("f", PeerConnState::Unauthorized),
            "Unauthorized — re-pair",
            Tone::Error,
        ),
        (peer("g", PeerConnState::Revoked), "Revoked", Tone::Error),
    ];
    for (p, text, tone) in cases {
        let badge = ui_model::peer_badge(&p, Now::at(NOW));
        assert_eq!(
            (badge.text.as_str(), badge.tone),
            (text, tone),
            "{:?}",
            p.state
        );
    }

    // An inbound-only peer is listed after the configured ones, with its
    // lanes, rates and shared folders.
    let inbound = PeerStatus {
        installation: "zzz-inbound".into(),
        endpoint: None,
        label: None,
        direction: Direction::Inbound,
        state: PeerConnState::Online,
        rtt_ms: Some(14),
        rate_up_bps: 2 * MIB,
        rate_down_bps: MIB,
        inbound_sessions: 2,
        lanes: vec![LaneStatus {
            lane: 0,
            role: LaneRole::Control,
            direction: Direction::Inbound,
            state: LaneState::Ready,
            since_ms: Some(NOW - 10 * MIN),
            inflight: vec![InflightOp {
                op: "meta.exchange".into(),
                repo: "repo-a".into(),
                since_ms: NOW - 3 * SEC,
                bytes: 1200 * 1024,
            }],
            bytes_up: 5 * MIB,
            bytes_down: 3 * GIB,
            ..LaneStatus::default()
        }],
        shared_folders: vec!["repo-a".into()],
        ..PeerStatus::default()
    };
    let s = snapshot(
        vec![folder("a", "Docs")],
        vec![inbound, peer("office", PeerConnState::Online)],
    );
    let rows = ui_model::peer_rows(&s);
    assert_eq!(rows[0].name, "office");
    let r = &rows[1];
    assert!(r.inbound_only);
    assert_eq!(r.name, "peer zzz-inbo");
    assert_eq!(r.direction, "inbound only");
    assert_eq!(r.rtt, "14 ms");
    assert_eq!(r.rates, "↑ 2.0 MiB/s · ↓ 1.0 MiB/s");
    assert_eq!(r.lanes_summary, "1 request in flight · 2 inbound sessions");
    assert_eq!(r.shared_folders, vec!["Docs".to_owned()]);
    let lane = &r.lanes[0];
    assert_eq!(lane.role, "control (inbound)");
    assert_eq!(lane.state.text, "ready");
    assert_eq!(lane.inflight, "meta.exchange · Docs · 3 s · 1.2 MiB");
    assert_eq!(lane.since, "for 10 min");
    assert_eq!(lane.bytes, "↑ 5.0 MiB · ↓ 3.0 GiB");
    assert_eq!(ui_model::overview(&s).inbound_only, 1);
}

// -- every lifecycle phase -----------------------------------------------------

#[test]
fn every_lifecycle_phase_has_its_chip() {
    let chip = |f: &FolderStatus| ui_model::lifecycle_chip(f, Now::at(NOW));
    let mut f = folder("a", "Docs");
    assert_eq!(chip(&f).text, "Running");
    f.lifecycle.adopted = true;
    assert_eq!(
        (chip(&f).text.as_str(), chip(&f).tone),
        ("Adopted", Tone::Ok)
    );
    f.lifecycle.degraded = true;
    assert_eq!(chip(&f).text, "Degraded — engine lock stalled");

    let mut f = folder("a", "Docs");
    f.lifecycle.phase = LifecyclePhase::Stopped;
    f.intents.desired_running = false;
    assert_eq!(
        (chip(&f).text.as_str(), chip(&f).tone),
        ("Stopped", Tone::Muted)
    );
    f.intents.desired_running = true;
    assert_eq!(chip(&f).text, "Start pending");

    f.lifecycle.phase = LifecyclePhase::Starting;
    f.lifecycle.startup = Some(StartupProgress {
        phase: "verifying objects".into(),
        seq: 37,
    });
    assert_eq!(
        (chip(&f).text.as_str(), chip(&f).tone),
        ("Starting — verifying objects (seq 37)", Tone::Progress)
    );
    f.lifecycle.adopted = true;
    assert_eq!(chip(&f).text, "Starting — adopting the running worker");
    f.lifecycle.adopted = false;

    f.lifecycle.phase = LifecyclePhase::StopBlocked;
    f.store.open_handles = 3;
    let c = chip(&f);
    assert_eq!(c.text, "Stopping — 3 files open");
    assert_eq!(c.action, Some(FolderAction::ForceStop));
    assert_eq!(c.action_label, "Force stop");

    f.lifecycle.phase = LifecyclePhase::Stopping;
    assert_eq!(chip(&f).text, "Stopping…");
    assert_eq!(chip(&f).action, Some(FolderAction::ForceStop));

    f.lifecycle.phase = LifecyclePhase::Backoff;
    f.lifecycle.next_retry_at_ms = Some(NOW + 32 * SEC);
    f.lifecycle.detail = Some("restart attempt 3 after exit code 70".into());
    let c = chip(&f);
    assert_eq!(c.text, "Restarting in 32 s (attempt 3)");
    assert_eq!(c.tone, Tone::Warn);
    assert_eq!(c.action, Some(FolderAction::Retry));

    f.lifecycle.phase = LifecyclePhase::Failed;
    f.lifecycle.detail = Some("CRASH_LOOP: the worker failed 5 times".into());
    let c = chip(&f);
    assert_eq!((c.text.as_str(), c.tone), ("Crash loop", Tone::Error));
    assert_eq!(
        (c.action, c.action_label.as_str()),
        (Some(FolderAction::Retry), "Retry")
    );
    f.lifecycle.detail = Some("PREVIOUS_WORKER_UNRESPONSIVE: pid 12".into());
    assert_eq!(chip(&f).action, Some(FolderAction::ForceStop));
    f.lifecycle.detail = Some("STORE_IDENTITY_MISMATCH: x".into());
    assert_eq!(chip(&f).text, "Failed — STORE_IDENTITY_MISMATCH");
}

// -- every mount state and gate kind -------------------------------------------

#[test]
fn every_mount_state_has_its_chip() {
    let chip = |f: &FolderStatus| ui_model::mount_chip(f, Now::at(NOW));
    let mut f = folder("a", "Docs");
    assert_eq!(
        (chip(&f).text.as_str(), chip(&f).tone),
        ("Mounted", Tone::Ok)
    );
    f.mount.state = MountState::Preflight;
    assert_eq!(chip(&f).text, "Mounting…");
    f.mount.state = MountState::Attaching;
    assert_eq!(chip(&f).text, "Mounting…");
    f.mount.state = MountState::Detaching;
    assert_eq!(chip(&f).text, "Unmounting…");
    f.mount.state = MountState::AttachFailed {
        error: wire("WINFSP_RUNTIME_UNAVAILABLE"),
        attempt: 2,
        next_retry_at_ms: None,
    };
    let c = chip(&f);
    assert_eq!(c.text, "Mount failed");
    assert_eq!(
        (c.tone, c.action),
        (Tone::Error, Some(FolderAction::Attach))
    );
    assert_eq!(c.action_label, "Retry");
    f.mount.state = MountState::AttachFailed {
        error: wire("OBJECT_IO_ERROR"),
        attempt: 2,
        next_retry_at_ms: Some(NOW + 8 * SEC),
    };
    assert_eq!(chip(&f).text, "Mount failed · retry in 8 s");
    f.mount.state = MountState::Lost {
        error: wire("MOUNT_LOST"),
    };
    assert_eq!(chip(&f).text, "Mount lost");
    assert_eq!(chip(&f).action, Some(FolderAction::Attach));
    // Detached by the user.
    f.mount.state = MountState::Detached;
    f.mount.intent = MountIntent::Detach;
    assert_eq!(chip(&f).text, "Detached");
    assert_eq!(chip(&f).action, Some(FolderAction::Attach));
    // Wanted but not attached.
    f.mount.intent = MountIntent::Attach;
    assert_eq!(chip(&f).text, "Not mounted");
    // Disabled.
    f.mount.state = MountState::Disabled;
    f.intents.mount_enabled = false;
    assert_eq!(chip(&f).text, "Mount off");
    // Worker not running.
    f.intents.mount_enabled = true;
    f.lifecycle.phase = LifecyclePhase::Stopped;
    assert_eq!(
        (chip(&f).text.as_str(), chip(&f).tone),
        ("Not mounted", Tone::Muted)
    );
    // Headless: no path.
    f.mount.path = None;
    assert_eq!(chip(&f).text, "Headless");
}

#[test]
fn every_gate_kind_is_shown() {
    let detail = |f: FolderStatus| {
        let s = one(f);
        ui_model::folder_detail(&s, "a").unwrap()
    };
    let gate_row = |d: &ui_model::FolderDetailModel| {
        d.overview
            .iter()
            .find(|kv| kv.key == "Initial sync")
            .map(|kv| (kv.value.clone(), kv.tone))
    };
    // None: no row.
    assert_eq!(gate_row(&detail(folder("a", "Docs"))), None);
    // Awaiting: the source, how long, the waiting_on list; Mount now.
    let mut f = folder("a", "Docs");
    f.mount.state = MountState::Detached;
    f.gate = GateStatus {
        state: GateKind::Awaiting,
        source: Some("office".into()),
        since_ms: Some(NOW - 3 * MIN),
        waiting_on: vec!["cursor behind the tip".into(), "3 inbox rows".into()],
    };
    let d = detail(f.clone());
    assert_eq!(
        gate_row(&d),
        Some((
            "Waiting for the initial sync from office for 3 min".into(),
            Tone::Progress
        ))
    );
    assert_eq!(d.gate_waiting_on.len(), 2);
    assert_eq!(d.row.mount.text, "Waiting for initial sync");
    assert_eq!(
        (d.row.mount.action, d.row.mount.action_label.as_str()),
        (Some(FolderAction::MountNow), "Mount now")
    );
    assert!(d.row.actions.contains(&FolderAction::MountNow));
    assert!(!d.row.actions.contains(&FolderAction::Attach));
    // Blocked.
    f.gate.state = GateKind::Blocked;
    f.activation.inbox.quarantined = 2;
    let d = detail(f.clone());
    assert_eq!(gate_row(&d).unwrap().1, Tone::Error);
    assert!(d.row.actions.contains(&FolderAction::Revalidate));
    assert!(d.row.actions.contains(&FolderAction::MountNow));
    // Satisfied, Overridden.
    f.gate.state = GateKind::Satisfied;
    assert_eq!(
        gate_row(&detail(f.clone())),
        Some(("Complete".into(), Tone::Ok))
    );
    f.gate.state = GateKind::Overridden;
    assert_eq!(gate_row(&detail(f)).unwrap().1, Tone::Warn);
}

// -- sharing, needs-upgrade, errors ----------------------------------------------

#[test]
fn a_non_sharing_peer_is_ignored() {
    let f = with_sync(
        folder("a", "Docs"),
        FolderSyncState::UpToDate,
        vec![
            pair("office", FolderPeerState::UpToDate),
            pair("stranger", FolderPeerState::NotShared),
        ],
    );
    let s = snapshot(
        vec![f],
        vec![
            peer("office", PeerConnState::Online),
            peer("stranger", PeerConnState::Offline),
        ],
    );
    let r = row(&s);
    assert_eq!(r.sync.text, "Up to date");
    assert_eq!(r.sync_detail, "1 peer in sync");
    let d = ui_model::folder_detail(&s, "a").unwrap();
    assert_eq!(d.peers.len(), 1);
    assert_eq!(d.peers[0].name, "office");
    // Offline counts sharing peers only.
    let mut s2 = s.clone();
    s2.folders[0].sync.state = FolderSyncState::Offline;
    s2.folders[0].sync.peers[0].state = FolderPeerState::Offline;
    assert_eq!(row(&s2).sync.text, "Waiting for office (offline)");
}

#[test]
fn a_needs_upgrade_peer_renders_as_specified() {
    let mut p = pair("old", FolderPeerState::Incompatible);
    p.error = Some(record(
        "PEER_NEEDS_UPGRADE",
        Scope::FolderPeer {
            state: "a".into(),
            installation: "old".into(),
        },
        NOW - MIN,
    ));
    let mut old = peer("old", PeerConnState::Incompatible);
    old.error = Some(record(
        "PEER_NEEDS_UPGRADE",
        Scope::Peer {
            installation: "old".into(),
        },
        NOW - MIN,
    ));
    let s = snapshot(
        vec![with_sync(
            folder("a", "Docs"),
            FolderSyncState::Offline,
            vec![p],
        )],
        vec![old],
    );
    assert_eq!(row(&s).sync.text, "Waiting for old (needs upgrade)");
    let peers = ui_model::peer_rows(&s);
    assert_eq!(peers[0].badge.text, "Peer needs upgrade");
    assert_eq!(peers[0].badge.tone, Tone::Error);
    let e = peers[0].error.as_ref().unwrap();
    assert_eq!(e.class, "incompatible");
    assert!(
        e.suggestion.contains("Upgrade Telekinesis"),
        "{}",
        e.suggestion
    );
    let d = ui_model::folder_detail(&s, "a").unwrap();
    assert_eq!(d.peers[0].state.text, "Peer needs upgrade");
    assert_eq!(
        d.peers[0].error.as_ref().unwrap().code,
        "PEER_NEEDS_UPGRADE"
    );
}

#[test]
fn a_stalled_folder_renders_as_specified() {
    let s = stalled_snapshot();
    let r = row(&s);
    assert_eq!(r.sync.text, "Stalled 4 min — CORRUPT_OBJECT 1a2b3c4d5e6f");
    let d = ui_model::folder_detail(&s, "a").unwrap();
    let p = &d.peers[0];
    assert_eq!(
        p.state.text,
        "Stalled since 4 min ago — CORRUPT_OBJECT 1a2b3c4d5e6f"
    );
    assert_eq!(p.down, "10.0 MiB left · 1 file");
    assert_eq!(p.last_progress, "4 min ago");
    let e = p.error.as_ref().unwrap();
    assert_eq!(e.scope.as_deref(), Some("object 1a2b3c4d5e6f"));
    assert_eq!(e.class, "temporary");
}

#[test]
fn errors_disappear_when_absent_from_a_snapshot() {
    let mut f = folder("a", "Docs");
    f.errors = vec![record(
        "MOUNT_PATH_MUST_NOT_EXIST",
        Scope::Mount { state: "a".into() },
        NOW - 2 * MIN,
    )];
    let mut s = one(f);
    s.daemon.errors = vec![record("CATALOG_UNAVAILABLE", Scope::Daemon, NOW - MIN)];
    let r = row(&s);
    let e = r.error.as_ref().expect("the open error is shown");
    assert_eq!(e.code, "MOUNT_PATH_MUST_NOT_EXIST");
    assert_eq!(e.class, "needs action");
    assert_eq!(e.tone, Tone::Error);
    assert!(e.when.ends_with("(2 min ago)"), "{}", e.when);
    assert!(
        e.suggestion.starts_with("Remove the existing folder"),
        "{}",
        e.suggestion
    );
    assert!(r.attention);
    assert_eq!(ui_model::daemon_model(&s, Now::of(&s)).errors.len(), 1);
    assert_eq!(ui_model::folder_detail(&s, "a").unwrap().errors.len(), 1);

    // The next snapshot no longer lists them: they are gone everywhere.
    let mut next = s.clone();
    next.generation += 1;
    next.folders[0].errors.clear();
    next.daemon.errors.clear();
    let r = row(&next);
    assert_eq!(r.error, None);
    assert_eq!(r.open_errors, 0);
    assert!(!r.attention);
    assert!(
        ui_model::daemon_model(&next, Now::of(&next))
            .errors
            .is_empty()
    );
    assert!(
        ui_model::folder_detail(&next, "a")
            .unwrap()
            .errors
            .is_empty()
    );

    // A resolved record stays in the history, muted, without a suggestion.
    let mut resolved = record("WS_CLOSED", Scope::Daemon, NOW - 5 * MIN);
    resolved.resolve(NOW - MIN);
    let mut hist = s.clone();
    hist.folders[0].errors = vec![resolved];
    let r = row(&hist);
    assert_eq!(r.error, None);
    let d = ui_model::folder_detail(&hist, "a").unwrap();
    assert_eq!(d.errors[0].tone, Tone::Muted);
    assert_eq!(d.errors[0].resolved.as_deref(), Some("resolved 1 min ago"));
    assert!(d.errors[0].suggestion.is_empty());
}

#[test]
fn error_lines_carry_class_time_count_retry_and_suggestion() {
    let s = one(folder("a", "Docs"));
    let names = Names::of(&s);
    let mut r = record(
        "REQUEST_TIMEOUT",
        Scope::Lane {
            installation: "office".into(),
            lane: 2,
        },
        NOW - 10 * MIN,
    );
    r.observe(wire("REQUEST_TIMEOUT"), NOW - 30 * SEC);
    r.observe(wire("REQUEST_TIMEOUT"), NOW - 20 * SEC);
    r.next_retry_at_ms = Some(NOW + 8 * SEC);
    let line = ui_model::error_line(&r, &names, Now::of(&s));
    assert_eq!(line.class, "temporary");
    assert_eq!(line.tone, Tone::Warn);
    assert_eq!(line.count, 3);
    assert_eq!(line.scope.as_deref(), Some("peer office lane 2"));
    assert_eq!(line.next_retry.as_deref(), Some("retry in 8 s"));
    assert!(line.first.is_some());
    assert_eq!(line.suggestion, "Retrying automatically.");
    let text = line.summary();
    assert!(
        text.starts_with("[temporary] REQUEST_TIMEOUT: detail"),
        "{text}"
    );
    assert!(text.contains("3x"), "{text}");
    assert!(text.contains("retry in 8 s"), "{text}");
    // Every class has a suggestion.
    for class in [
        ErrorClass::Contention,
        ErrorClass::Transient,
        ErrorClass::NeedsUserAction,
        ErrorClass::Incompatible,
        ErrorClass::Fatal,
    ] {
        assert!(!ui_model::suggestion("SOME_UNKNOWN_CODE", class).is_empty());
    }
}

#[test]
fn the_three_claims_are_separate_in_the_overview() {
    let mut f = folder("a", "Docs");
    f.sync.peers[0].metadata.caught_up = true;
    f.content.complete = false;
    f.content.current_missing = 2;
    f.content.current_missing_bytes = 3 * MIB;
    f.journal.outgoing_unacknowledged = 5;
    f.journal.unresolved_conflicts = 4;
    let s = one(f);
    let d = ui_model::folder_detail(&s, "a").unwrap();
    let value = |key: &str| {
        d.overview
            .iter()
            .find(|kv| kv.key == key)
            .map(|kv| kv.value.clone())
            .unwrap()
    };
    assert_eq!(value("Metadata"), "Caught up with every peer");
    assert_eq!(value("Content"), "2 of 10 files missing (3.0 MiB)");
    assert_eq!(
        value("Backed up"),
        "5 changes not yet acknowledged by a peer"
    );
    assert!(value("Journal").contains("4 unresolved conflicts"));
    let r = row(&s);
    assert_eq!(r.conflicts, 4);
    assert_eq!(
        r.sync_detail,
        "1 peer in sync · 2 files not downloaded · 5 changes not yet backed up · 4 conflicts"
    );
}

#[test]
fn paths_drop_the_verbatim_prefix() {
    use std::path::Path;
    assert_eq!(
        ui_model::display_path(Path::new(r"\\?\C:\Telekinesis\docs")),
        r"C:\Telekinesis\docs"
    );
    assert_eq!(
        ui_model::display_path(Path::new(r"\\?\UNC\nas\share\docs")),
        r"\\nas\share\docs"
    );
    assert_eq!(ui_model::display_path(Path::new(r"D:\plain")), r"D:\plain");
    let mut f = folder("a", "Docs");
    f.mount.path = Some(PathBuf::from(r"\\?\C:\T\docs"));
    f.mount.state = MountState::AttachFailed {
        error: wire("MOUNT_PATH_MUST_NOT_EXIST"),
        attempt: 2,
        next_retry_at_ms: None,
    };
    let s = one(f);
    assert_eq!(row(&s).mount_path, r"C:\T\docs");
    let d = ui_model::folder_detail(&s, "a").unwrap();
    let mount = d.overview.iter().find(|kv| kv.key == "Mount").unwrap();
    assert_eq!(
        mount.value,
        r"Mount failed · MOUNT_PATH_MUST_NOT_EXIST: detail · attempt 2 · C:\T\docs"
    );
    assert_eq!(mount.tone, Tone::Error);
}

#[test]
fn overview_shows_the_last_exit_and_store_health() {
    let mut f = folder("a", "Docs");
    f.lifecycle.phase = LifecyclePhase::Backoff;
    f.lifecycle.last_exit = Some(ExitRecord {
        code: Some(70),
        fatal: None,
        stderr_tail: "starting\nENGINE_POISONED: lock poisoned\n\n".into(),
        at_ms: NOW - 20 * SEC,
    });
    f.store.health = StoreHealth::Degraded {
        error: wire("RECOVERY_REQUIRED"),
    };
    f.store.pending_saves = 2;
    let s = one(f);
    let d = ui_model::folder_detail(&s, "a").unwrap();
    let row = |key: &str| d.overview.iter().find(|kv| kv.key == key).unwrap().clone();
    assert_eq!(
        row("Last exit").value,
        "exit code 70 · log: ENGINE_POISONED: lock poisoned · 20 s ago"
    );
    assert_eq!(
        (row("Store").value.as_str(), row("Store").tone),
        ("Degraded — RECOVERY_REQUIRED: detail", Tone::Warn)
    );
    assert_eq!(
        row("Files").value,
        "0 open files · 2 pending saves · 0 unflushed files"
    );
    // A running worker has no "last exit" row.
    let d = ui_model::folder_detail(&one(folder("a", "Docs")), "a").unwrap();
    assert!(d.overview.iter().all(|kv| kv.key != "Last exit"));
}

#[test]
fn folder_actions_follow_the_state() {
    let actions = |f: &FolderStatus| ui_model::folder_actions(f);
    let f = folder("a", "Docs");
    let a = actions(&f);
    assert!(a.contains(&FolderAction::Stop));
    assert!(a.contains(&FolderAction::Detach));
    assert!(a.contains(&FolderAction::PauseSync));
    assert!(!a.contains(&FolderAction::Start));
    assert!(!a.contains(&FolderAction::Attach));
    assert!(!a.contains(&FolderAction::Revalidate));
    assert_eq!(row(&one(f.clone())).primary, Some(FolderAction::Stop));

    let mut stopped = f.clone();
    stopped.lifecycle.phase = LifecyclePhase::Stopped;
    stopped.intents.desired_running = false;
    stopped.intents.sync_enabled = false;
    stopped.mount.state = MountState::Detached;
    let a = actions(&stopped);
    assert!(a.contains(&FolderAction::Start));
    assert!(a.contains(&FolderAction::ResumeSync));
    assert!(!a.contains(&FolderAction::Stop));
    assert!(!a.contains(&FolderAction::Detach));
    assert_eq!(row(&one(stopped)).primary, Some(FolderAction::Start));

    let mut blocked = f.clone();
    blocked.lifecycle.phase = LifecyclePhase::StopBlocked;
    assert_eq!(row(&one(blocked)).primary, Some(FolderAction::ForceStop));

    let mut failed = f.clone();
    failed.lifecycle.phase = LifecyclePhase::Failed;
    let a = actions(&failed);
    assert!(a.contains(&FolderAction::Retry));
    assert!(a.contains(&FolderAction::Stop));
    assert_eq!(row(&one(failed)).primary, Some(FolderAction::Retry));

    let mut quarantined = f.clone();
    quarantined.activation.inbox.quarantined = 1;
    assert!(actions(&quarantined).contains(&FolderAction::Revalidate));

    let mut mount_failed = f;
    mount_failed.mount.state = MountState::AttachFailed {
        error: wire("MOUNT_OVERLAPS_DATA"),
        attempt: 1,
        next_retry_at_ms: None,
    };
    assert!(actions(&mount_failed).contains(&FolderAction::Attach));

    // Routing: Force stop asks first; the confirmation sends it; Change path
    // is a dialog; everything else is sent at once.
    use ui_model::ActionRoute;
    assert_eq!(
        ui_model::route_action("force-stop"),
        Some(ActionRoute::Confirm(FolderAction::ForceStop))
    );
    assert_eq!(
        ui_model::route_action(&format!("{}force-stop", ui_model::CONFIRMED_PREFIX)),
        Some(ActionRoute::Send(FolderAction::ForceStop))
    );
    assert_eq!(
        ui_model::route_action("change-path"),
        Some(ActionRoute::ChangePath)
    );
    assert_eq!(
        ui_model::route_action("attach"),
        Some(ActionRoute::Send(FolderAction::Attach))
    );
    assert_eq!(ui_model::route_action("nonsense"), None);
    // Every action round-trips its id; the dialog-only one sends nothing.
    for action in FolderAction::ALL {
        assert_eq!(FolderAction::from_id(action.id()), Some(action));
        assert_eq!(
            action.command("s").is_none(),
            action == FolderAction::ChangePath
        );
    }
    assert!(matches!(
        FolderAction::MountNow.command("s"),
        Some(Action::OverrideGate { .. })
    ));
}

#[test]
fn detail_tabs_cover_activation_content_and_activity() {
    let mut f = folder("a", "Docs");
    f.activation.inbox = InboxCounts {
        ready: 400,
        waiting: 12,
        quarantined: 1,
        ..InboxCounts::default()
    };
    f.activation.counters = ActivationCounters {
        running: true,
        activated: 1000,
        fast: 990,
        slow: 10,
        starved_since_ms: Some(NOW - 2 * MIN),
        last_activation_at_ms: Some(NOW - 5 * SEC),
        ..ActivationCounters::default()
    };
    let mut s = one(f);
    s.activity = vec![
        ActivityEntry {
            at_ms: NOW - 3 * MIN,
            kind: "error".into(),
            folder: Some("a".into()),
            peer: Some("office".into()),
            message: "CONNECTION_LOST".into(),
        },
        ActivityEntry {
            at_ms: NOW - MIN,
            kind: "action".into(),
            folder: Some("a".into()),
            peer: None,
            message: "start".into(),
        },
        ActivityEntry {
            at_ms: NOW - 2 * MIN,
            kind: "peer".into(),
            folder: None,
            peer: Some("office".into()),
            message: "online".into(),
        },
    ];
    let d = ui_model::folder_detail_at(&s, "a", Now::of(&s), Some(12.34)).unwrap();
    let value = |rows: &[ui_model::KeyValue], key: &str| {
        rows.iter()
            .find(|kv| kv.key == key)
            .map(|kv| kv.value.clone())
            .unwrap()
    };
    assert_eq!(
        value(&d.activation, "Backlog"),
        "412 events to activate (400 ready, 12 waiting for parents)"
    );
    assert_eq!(value(&d.activation, "Rate"), "12.3 events/s");
    assert_eq!(
        value(&d.activation, "Starvation"),
        "The slow path is starved for 2 min"
    );
    assert_eq!(
        value(&d.content, "Current files"),
        "10 of 10 files available"
    );
    // The folder's activity only, newest first.
    assert_eq!(d.activity.len(), 2);
    assert_eq!(d.activity[0].message, "start");
    assert_eq!(d.activity[1].subject, "Docs / office");
    assert_eq!(d.activity[1].tone, Tone::Error);
    // The global log has all three.
    let all = ui_model::activity_rows(&s, None);
    assert_eq!(all.len(), 3);
    assert_eq!(all[1].subject, "office");
    assert_eq!(all[0].ago, "1 min ago");
    assert!(ui_model::folder_detail(&s, "missing").is_none());
}

#[test]
fn missing_items_come_from_the_worker_status() {
    let items = ui_model::missing_items(&json!({"content":{"items":[
        {"path":"photos/a.jpg","state":"downloading","downloaded":12288,"size":2097152},
        {"path":"b.txt","state":"missing","size":10,"error":"CONTENT_UNAVAILABLE: no peer has it"},
        {"state":"requested"}
    ]}}));
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].progress, "12.0 KiB of 2.0 MiB");
    assert_eq!(items[1].progress, "10 B");
    assert!(items[1].error.starts_with("CONTENT_UNAVAILABLE"));
    assert_eq!(items[2].path, "file");
    assert!(ui_model::missing_items(&Value::Null).is_empty());
}

#[test]
fn daemon_header_and_overview() {
    let mut s = snapshot(
        vec![folder("a", "Docs"), {
            let mut f = folder("b", "Archive");
            f.mount.path = None;
            f.mount.state = MountState::Disabled;
            f.sync.state = FolderSyncState::Downloading;
            f
        }],
        vec![
            PeerStatus {
                rate_down_bps: 3 * MIB,
                ..peer("office", PeerConnState::Online)
            },
            peer("home", PeerConnState::Offline),
        ],
    );
    s.daemon.log_path = Some("C:\\data\\logs\\orchestrator.log".into());
    // A peer-scoped error of a listed peer is shown under the peer, not in
    // the banner; one of an unknown peer is in the banner.
    s.daemon.errors = vec![
        record(
            "WS_CONNECT_FAILED",
            Scope::Peer {
                installation: "home".into(),
            },
            NOW - MIN,
        ),
        record(
            "WS_CONNECT_FAILED",
            Scope::Peer {
                installation: "gone".into(),
            },
            NOW - MIN,
        ),
    ];
    let d = ui_model::daemon_model(&s, Now::of(&s));
    assert_eq!(d.title, "Telekinesis 0.1.0");
    assert_eq!(d.status.text, "Running · up 2 h 5 min");
    assert_eq!(d.installation, "installa");
    assert_eq!(
        d.log_path.as_deref(),
        Some("C:\\data\\logs\\orchestrator.log")
    );
    assert_eq!(d.errors.len(), 1);
    assert_eq!(d.errors[0].scope.as_deref(), Some("peer peer gone"));
    s.daemon.stopping = true;
    assert_eq!(
        ui_model::daemon_model(&s, Now::of(&s)).status.text,
        "Stopping"
    );
    let o = ui_model::overview(&s);
    assert_eq!(
        (o.folders, o.mounted, o.up_to_date, o.syncing),
        (2, 1, 1, 1)
    );
    assert_eq!((o.peers, o.peers_online, o.rate_down_bps), (2, 1, 3 * MIB));
}

// -- moved from src/gui.rs ----------------------------------------------------

/// Was `connected_peer_does_not_hide_an_unavailable_folder_or_stale_sync`:
/// a connected peer whose folder is not serving is never shown as in sync,
/// a stall is shown as a stall, and caught-up metadata with objects left is
/// a download, not "up to date".
#[test]
fn connected_peer_does_not_hide_an_unavailable_folder_or_stale_sync() {
    let s = snapshot(
        vec![with_sync(
            folder("a", "Docs"),
            FolderSyncState::Offline,
            vec![pair("office", FolderPeerState::WaitingForRemote)],
        )],
        vec![peer("office", PeerConnState::Online)],
    );
    assert_eq!(
        row(&s).sync.text,
        "Waiting for office (its folder is not serving)"
    );
    let d = ui_model::folder_detail(&s, "a").unwrap();
    assert_eq!(d.peers[0].state.text, "Waiting for the peer's folder");
    assert_eq!(row(&stalled_snapshot()).sync.tone, Tone::Error);
    let mut p = pair("office", FolderPeerState::Downloading);
    p.metadata.caught_up = true;
    p.objects_down.objects_remaining = 3;
    let r = row(&one(with_sync(
        folder("a", "Docs"),
        FolderSyncState::Downloading,
        vec![p],
    )));
    assert_eq!(r.sync.text, "Downloading 3 files");
}

/// Was `summary_distinguishes_current_content_from_history_and_pending_metadata`.
#[test]
fn summary_distinguishes_current_content_from_history_and_pending_metadata() {
    let mut f = folder("a", "Docs");
    f.content = ContentCounters {
        current_objects: 108,
        current_available: 100,
        current_missing: 8,
        shared_objects: 291,
        shared_available: 108,
        partial_bytes: 2048,
        requested: 1,
        downloading: 2,
        ..ContentCounters::default()
    };
    f.activation.inbox.ready = 673;
    f.journal.outgoing_unacknowledged = 4;
    let s = one(f);
    let d = ui_model::folder_detail(&s, "a").unwrap();
    let value = |rows: &[ui_model::KeyValue], key: &str| {
        rows.iter().find(|kv| kv.key == key).unwrap().value.clone()
    };
    assert_eq!(
        value(&d.content, "Current files"),
        "100 of 108 files available"
    );
    assert_eq!(
        value(&d.content, "Shared history"),
        "108 of 291 objects available (0 B missing)"
    );
    assert_eq!(value(&d.content, "Downloading"), "2 files (2.0 KiB so far)");
    assert!(value(&d.activation, "Backlog").starts_with("673 events to activate"));
    assert_eq!(
        value(&d.overview, "Backed up"),
        "4 changes not yet acknowledged by a peer"
    );
}

/// Was `folder_snapshot_preserves_headless_and_failure_states`.
#[test]
fn folder_snapshot_preserves_headless_and_failure_states() {
    let design = {
        let mut f = folder("a", "Design Assets");
        f.mount.path = Some(PathBuf::from("C:\\Work\\design"));
        f
    };
    let archive = {
        let mut f = folder("b", "Archive");
        f.mount.path = None;
        f.mount.state = MountState::Disabled;
        f.mount.intent = MountIntent::Headless;
        f
    };
    let failed = {
        let mut f = folder("c", "Failed");
        f.mount.path = Some(PathBuf::from("X:"));
        f.mount.state = MountState::AttachFailed {
            error: ErrorWire::coded("WINFSP_RUNTIME_UNAVAILABLE", "Mount unavailable"),
            attempt: 1,
            next_retry_at_ms: None,
        };
        f.errors = vec![ErrorRecord::new(
            ErrorWire::coded("WINFSP_RUNTIME_UNAVAILABLE", "Mount unavailable"),
            Scope::Mount { state: "c".into() },
            NOW - SEC,
        )];
        f.sync.state = FolderSyncState::NeedsAttention;
        f
    };
    let s = snapshot(vec![failed, archive, design], vec![]);
    let rows = ui_model::folder_rows(&s);
    // Sorted by label.
    let labels: Vec<&str> = rows.iter().map(|r| r.label.as_str()).collect();
    assert_eq!(labels, ["Archive", "Design Assets", "Failed"]);
    assert_eq!(rows.iter().filter(|r| r.mounted).count(), 1);
    assert_eq!(ui_model::overview(&s).mounted, 1);
    assert_eq!(rows[0].mount.text, "Headless");
    assert_eq!(rows[0].mount_path, "");
    assert_eq!(ui_model::filter_rows(&rows, " DESIGN ").len(), 1);
    assert_eq!(ui_model::filter_rows(&rows, "c:\\work")[0].state, "a");
    assert!(ui_model::filter_rows(&rows, "missing").is_empty());
    assert_eq!(ui_model::filter_rows(&rows, "").len(), 3);
    let failed = &rows[2];
    assert_eq!(failed.mount.text, "Mount failed");
    assert_eq!(
        failed.error.as_ref().unwrap().message,
        "WINFSP_RUNTIME_UNAVAILABLE: Mount unavailable"
    );
    assert!(failed.attention);
}

/// Was `peer_is_online_when_any_lane_is_connected`: the hub decides the
/// badge from the lanes (`hub_rules::peer_conn_state`); the desktop shows
/// it, and counts online peers from it.
#[test]
fn peer_is_online_when_any_lane_is_connected() {
    let lane = |lane: u32, state: LaneState| LaneStatus {
        lane,
        state,
        ..LaneStatus::default()
    };
    let one_ready = PeerStatus {
        lanes: vec![lane(0, LaneState::Backoff), lane(1, LaneState::Ready)],
        ..peer("one", PeerConnState::Online)
    };
    let none = PeerStatus {
        lanes: vec![],
        ..peer("two", PeerConnState::Offline)
    };
    let down = PeerStatus {
        lanes: vec![lane(0, LaneState::Closed)],
        ..peer("three", PeerConnState::Offline)
    };
    let s = snapshot(vec![], vec![one_ready, none, down]);
    let rows = ui_model::peer_rows(&s);
    let online: Vec<(&str, bool)> = rows.iter().map(|r| (r.name.as_str(), r.online)).collect();
    assert_eq!(online, [("one", true), ("three", false), ("two", false)]);
    assert_eq!(rows[0].lanes_summary, "1 of 2 lanes ready");
    assert_eq!(ui_model::overview(&s).peers_online, 1);
}

// -- CCR-WP2.1-7 adapter -------------------------------------------------------

#[test]
fn mount_smoother_holds_a_settled_mount_across_one_stale_heartbeat() {
    let mut smoother = MountSmoother::default();
    let attached = one(folder("a", "Docs"));
    let mut shown = attached.clone();
    smoother.smooth(&mut shown, NOW);
    assert!(shown.folders[0].mount.state.is_attached());

    // A heartbeat requested before the attach result says "attaching".
    let mut stale = attached.clone();
    stale.folders[0].mount.state = MountState::Attaching;
    let mut shown = stale.clone();
    smoother.smooth(&mut shown, NOW + 300);
    assert!(shown.folders[0].mount.state.is_attached(), "held");
    // Idempotent on re-render, still within the window.
    let mut shown = stale.clone();
    smoother.smooth(&mut shown, NOW + 1_500);
    assert!(shown.folders[0].mount.state.is_attached(), "still held");
    // A regression that lasts is real.
    let mut shown = stale.clone();
    smoother.smooth(&mut shown, NOW + 1_900);
    assert_eq!(shown.folders[0].mount.state, MountState::Attaching);

    // A failed mount is held across a stale "detached" heartbeat, and the
    // chip keeps its Retry action.
    let mut smoother = MountSmoother::default();
    let mut failed = attached.clone();
    failed.folders[0].mount.state = MountState::AttachFailed {
        error: wire("MOUNT_PATH_MUST_NOT_EXIST"),
        attempt: 1,
        next_retry_at_ms: None,
    };
    smoother.smooth(&mut failed.clone(), NOW);
    let mut stale = attached.clone();
    stale.folders[0].mount.state = MountState::Detached;
    let mut shown = stale.clone();
    smoother.smooth(&mut shown, NOW + 500);
    assert_eq!(row(&shown).mount.text, "Mount failed");

    // A local command releases the hold at once.
    smoother.release("a");
    let mut shown = stale.clone();
    smoother.smooth(&mut shown, NOW + 600);
    assert_eq!(shown.folders[0].mount.state, MountState::Detached);

    // A user Detach, a new worker instance or a stop are never held.
    for change in 0..3 {
        let mut smoother = MountSmoother::default();
        smoother.smooth(&mut attached.clone(), NOW);
        let mut next = attached.clone();
        next.folders[0].mount.state = MountState::Detached;
        match change {
            0 => next.folders[0].mount.intent = MountIntent::Detach,
            1 => next.folders[0].lifecycle.instance = Some("new".into()),
            _ => next.folders[0].lifecycle.phase = LifecyclePhase::Stopping,
        }
        let mut shown = next.clone();
        smoother.smooth(&mut shown, NOW + 100);
        assert_eq!(
            shown.folders[0].mount.state,
            MountState::Detached,
            "{change}"
        );
    }
}

#[test]
fn an_open_mount_failure_keeps_the_mount_failed_while_retrying() {
    // CCR-WP2.1-7 adapter, part 1: the controller still wants the mount and
    // its last failure is open, so "attaching" is a retry of a failed mount.
    let mut f = folder("a", "Docs");
    f.mount.state = MountState::Attaching;
    f.errors = vec![record(
        "OBJECT_IO_ERROR",
        Scope::Mount { state: "a".into() },
        NOW - SEC,
    )];
    let c = ui_model::mount_chip(&f, Now::at(NOW));
    assert_eq!(c.text, "Mount failed · retrying");
    assert_eq!(c.action, Some(FolderAction::Attach));
    // Resolved: a plain attach.
    f.errors[0].resolve(NOW);
    assert_eq!(ui_model::mount_chip(&f, Now::at(NOW)).text, "Mounting…");
}

// -- rate meter and busy tracking ------------------------------------------------

#[test]
fn rate_meter_measures_activation() {
    let mut meter = RateMeter::default();
    let mut s = one(folder("a", "Docs"));
    s.folders[0].activation.counters.activated = 100;
    meter.observe(&s);
    assert_eq!(meter.per_second("a"), None);
    s.at_ms += 2_000;
    s.folders[0].activation.counters.activated = 150;
    meter.observe(&s);
    assert_eq!(meter.per_second("a"), Some(25.0));
    // A restarted worker resets its counter: no negative rate.
    s.at_ms += 1_000;
    s.folders[0].activation.counters.activated = 3;
    meter.observe(&s);
    assert_eq!(meter.per_second("a"), None);
    // A removed folder is forgotten.
    s.folders.clear();
    meter.observe(&s);
    assert_eq!(meter.per_second("a"), None);
}

#[test]
fn busy_allows_one_command_per_row() {
    let mut busy = Busy::default();
    assert!(busy.begin("a", 1, "Stopping"));
    assert!(!busy.begin("a", 2, "Starting"));
    assert!(busy.begin("b", 3, "Starting"));
    assert_eq!(busy.label("a"), Some("Stopping"));
    busy.finish("a", 2);
    assert!(busy.is_busy("a"), "another command's finish leaves it");
    busy.finish("a", 1);
    assert!(!busy.is_busy("a"));
    assert!(busy.any());
}

// -- background loops ----------------------------------------------------------

enum Step {
    Snap(StatusSnapshot),
    Gone,
    Fail,
    /// Blocks until the test sends on the channel, then answers the snapshot.
    Hold(mpsc::Receiver<()>, StatusSnapshot),
}

#[derive(Clone)]
struct FakeStatus {
    script: Arc<Mutex<VecDeque<Step>>>,
    sinces: Arc<Mutex<Vec<u64>>>,
    in_flight: mpsc::Sender<u64>,
}

impl StatusTransport for FakeStatus {
    fn subscribe(&mut self, since: u64, wait: Duration) -> Result<StatusSnapshot, TransportError> {
        self.sinces.lock().unwrap().push(since);
        let _ = self.in_flight.send(since);
        let step = self.script.lock().unwrap().pop_front();
        match step {
            Some(Step::Snap(s)) => Ok(s),
            Some(Step::Gone) => Err(TransportError::DaemonGone(
                "ORCHESTRATOR_NOT_RUNNING".into(),
            )),
            Some(Step::Fail) => Err(TransportError::Failed("CONNECTION_LOST".into())),
            Some(Step::Hold(release, s)) => {
                let _ = release.recv_timeout(Duration::from_secs(20));
                Ok(s)
            }
            None => {
                thread::sleep(wait.min(Duration::from_millis(50)));
                Err(TransportError::Failed("idle".into()))
            }
        }
    }
}

#[derive(Clone, Default)]
struct FakeStarter(Arc<AtomicUsize>);

impl DaemonStarter for FakeStarter {
    fn start(&mut self) -> Result<String, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok("data/logs/orchestrator.log".into())
    }
}

fn gen_snapshot(generation: u64, pid: u32) -> StatusSnapshot {
    let mut s = one(folder("a", "Docs"));
    s.generation = generation;
    s.daemon.pid = pid;
    s
}

struct Harness {
    feed: Feed,
    updates: mpsc::Receiver<FeedUpdate>,
    sinces: Arc<Mutex<Vec<u64>>>,
    in_flight: mpsc::Receiver<u64>,
    starts: Arc<AtomicUsize>,
}

fn fast() -> FeedOptions {
    FeedOptions {
        wait: Duration::from_millis(200),
        backoff_base: Duration::from_millis(10),
        backoff_cap: Duration::from_millis(40),
        start_every: Duration::from_secs(30),
    }
}

fn harness(script: Vec<Step>, options: FeedOptions) -> Harness {
    let (in_tx, in_flight) = mpsc::channel();
    let fake = FakeStatus {
        script: Arc::new(Mutex::new(script.into())),
        sinces: Arc::default(),
        in_flight: in_tx,
    };
    let sinces = Arc::clone(&fake.sinces);
    let starter = FakeStarter::default();
    let starts = Arc::clone(&starter.0);
    let (tx, updates) = mpsc::channel();
    let feed = Feed::start(
        Box::new(fake),
        Some(Box::new(starter)),
        options,
        Box::new(move |update| tx.send(update).is_ok()),
    );
    Harness {
        feed,
        updates,
        sinces,
        in_flight,
        starts,
    }
}

fn next(h: &Harness) -> FeedUpdate {
    h.updates
        .recv_timeout(Duration::from_secs(5))
        .expect("a feed update")
}

fn next_snapshot(h: &Harness) -> Arc<StatusSnapshot> {
    loop {
        if let FeedUpdate::Snapshot(s) = next(h) {
            return s;
        }
    }
}

#[test]
fn feed_respawns_a_gone_daemon_and_reconnects_with_backoff() {
    let h = harness(
        vec![
            Step::Gone,
            Step::Gone,
            Step::Fail,
            Step::Snap(gen_snapshot(3, 1)),
        ],
        fast(),
    );
    let FeedUpdate::Down(first) = next(&h) else {
        panic!("down first")
    };
    assert!(first.daemon_gone);
    assert_eq!(first.started.as_deref(), Some("data/logs/orchestrator.log"));
    assert_eq!(first.attempt, 1);
    assert_eq!(first.retry_in_ms, 10, "fast retry after a start");
    let FeedUpdate::Down(second) = next(&h) else {
        panic!("down second")
    };
    assert_eq!(second.started, None, "at most one start per interval");
    assert_eq!((second.attempt, second.retry_in_ms), (2, 20));
    let FeedUpdate::Down(third) = next(&h) else {
        panic!("down third")
    };
    assert!(!third.daemon_gone);
    assert_eq!((third.attempt, third.retry_in_ms), (3, 40));
    assert_eq!(next_snapshot(&h).generation, 3);
    assert_eq!(h.starts.load(Ordering::SeqCst), 1);
    assert_eq!(h.sinces.lock().unwrap()[..4], [0, 0, 0, 0]);
    h.feed.stop();
}

#[test]
fn feed_never_respawns_after_a_desktop_shutdown_until_asked() {
    let (release, hold) = mpsc::channel();
    let h = harness(
        vec![
            Step::Hold(hold, gen_snapshot(1, 1)),
            Step::Gone,
            Step::Gone,
            Step::Gone,
        ],
        fast(),
    );
    // The Shutdown path turns respawning off before the daemon goes away.
    h.feed.set_respawn(false);
    release.send(()).unwrap();
    assert_eq!(next_snapshot(&h).generation, 1);
    for _ in 0..2 {
        let FeedUpdate::Down(down) = next(&h) else {
            panic!("down")
        };
        assert!(down.daemon_gone && down.respawn_off);
        assert_eq!(down.started, None);
    }
    assert_eq!(h.starts.load(Ordering::SeqCst), 0);
    // "Start daemon" starts one at the next failure, whatever the interval.
    h.feed.start_daemon();
    let FeedUpdate::Down(down) = next(&h) else {
        panic!("down")
    };
    assert!(down.started.is_some());
    assert_eq!(h.starts.load(Ordering::SeqCst), 1);
    h.feed.stop();
}

#[test]
fn feed_never_restarts_a_daemon_shut_down_elsewhere() {
    // A `stopping` snapshot (a CLI `manage shutdown`), then the daemon is gone.
    let mut stopping = gen_snapshot(4, 1);
    stopping.daemon.stopping = true;
    let h = harness(vec![Step::Snap(stopping), Step::Gone, Step::Gone], fast());
    assert!(next_snapshot(&h).daemon.stopping);
    for _ in 0..2 {
        let FeedUpdate::Down(down) = next(&h) else {
            panic!("down")
        };
        assert!(down.deliberate && down.respawn_off && down.daemon_gone);
        assert_eq!(down.started, None);
    }
    assert_eq!(h.starts.load(Ordering::SeqCst), 0);
    h.feed.stop();

    // The same for a daemon that answered ORCHESTRATOR_STOPPING; a crash (no
    // such word) is restarted.
    struct Scripted(VecDeque<Result<StatusSnapshot, TransportError>>);
    impl StatusTransport for Scripted {
        fn subscribe(&mut self, _: u64, wait: Duration) -> Result<StatusSnapshot, TransportError> {
            self.0.pop_front().unwrap_or_else(|| {
                thread::sleep(wait.min(Duration::from_millis(50)));
                Err(TransportError::Failed("idle".into()))
            })
        }
    }
    for (first, expect_start) in [
        (
            TransportError::Failed("ORCHESTRATOR_STOPPING: the daemon is shutting down".into()),
            false,
        ),
        (
            TransportError::Failed("CONNECTION_LOST: reset".into()),
            true,
        ),
    ] {
        let starter = FakeStarter::default();
        let starts = Arc::clone(&starter.0);
        let (tx, rx) = mpsc::channel();
        let feed = Feed::start(
            Box::new(Scripted(
                vec![
                    Ok(gen_snapshot(1, 1)),
                    Err(first),
                    Err(TransportError::DaemonGone(
                        "ORCHESTRATOR_NOT_RUNNING".into(),
                    )),
                ]
                .into(),
            )),
            Some(Box::new(starter)),
            fast(),
            Box::new(move |update| tx.send(update).is_ok()),
        );
        let mut downs = 0;
        while downs < 2 {
            if let FeedUpdate::Down(_) = rx.recv_timeout(Duration::from_secs(5)).unwrap() {
                downs += 1;
            }
        }
        assert_eq!(starts.load(Ordering::SeqCst) == 1, expect_start);
        feed.stop();
    }
}

#[test]
fn feed_follows_generations_and_a_restarted_daemon() {
    let h = harness(
        vec![
            Step::Snap(gen_snapshot(5, 1)),
            // A long poll that timed out answers the same generation.
            Step::Snap(gen_snapshot(5, 1)),
            Step::Snap(gen_snapshot(6, 1)),
            Step::Fail,
            // The daemon restarted: lower generations, a new pid.
            Step::Snap(gen_snapshot(2, 77)),
        ],
        fast(),
    );
    assert_eq!(next_snapshot(&h).generation, 5);
    assert_eq!(
        next_snapshot(&h).generation,
        6,
        "an unchanged echo is not delivered"
    );
    let restarted = next_snapshot(&h);
    assert_eq!((restarted.generation, restarted.daemon.pid), (2, 77));
    assert_eq!(h.sinces.lock().unwrap()[..5], [0, 5, 5, 6, 0]);
    h.feed.stop();
}

/// A command transport that answers at once, except for actions on the
/// folder named `slow`, which take two seconds.
struct FakeCommands;

impl CommandTransport for FakeCommands {
    fn call(&mut self, action: &Action) -> Result<Value, String> {
        if matches!(action, Action::Stop { state, .. } if state == "slow") {
            thread::sleep(Duration::from_secs(2));
        }
        Ok(json!({"ok": true}))
    }
}

#[test]
fn commands_answer_in_200ms_while_a_slow_subscribe_is_in_flight() {
    // The subscription is parked in a long poll that does not answer.
    let (release, hold) = mpsc::channel();
    let h = harness(vec![Step::Hold(hold, gen_snapshot(1, 1))], fast());
    h.in_flight
        .recv_timeout(Duration::from_secs(5))
        .expect("the subscribe is in flight");

    let (done_tx, done_rx) = mpsc::channel::<CommandOutcome>();
    let done_tx = Mutex::new(done_tx);
    let pool = CommandPool::start(
        4,
        Arc::new(|| Box::new(FakeCommands) as Box<dyn CommandTransport>),
        Arc::new(move |outcome| {
            let _ = done_tx.lock().unwrap().send(outcome);
        }),
    );
    // A slow command on one row...
    pool.submit(Command {
        id: 1,
        key: "slow".into(),
        action: Action::Stop {
            state: "slow".into(),
            wait: None,
        },
    })
    .unwrap();
    // ...never delays a command on another row.
    for id in 2..5 {
        let submitted = Instant::now();
        pool.submit(Command {
            id,
            key: format!("row-{id}"),
            action: Action::Start {
                state: format!("row-{id}"),
            },
        })
        .unwrap();
        let outcome = done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("the command answers");
        let latency = submitted.elapsed();
        assert_eq!(outcome.command.id, id);
        assert!(outcome.result.is_ok());
        assert!(
            latency < Duration::from_millis(200),
            "command latency {latency:?} while the subscribe is in flight"
        );
    }
    let slow = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(slow.command.id, 1);
    assert!(slow.elapsed >= Duration::from_secs(2));
    // The subscription is still parked, then delivers.
    release.send(()).unwrap();
    assert_eq!(next_snapshot(&h).generation, 1);
    h.feed.stop();
}
