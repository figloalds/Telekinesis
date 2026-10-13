//! WP0.4 contract tests: golden fixtures for every wire, IPC and status shape,
//! the §8 classification table, and the `util` building blocks.
//!
//! Regenerate fixtures after an approved contract change (CCR) with
//! `TKFS_BLESS_CONTRACTS=1 cargo test --test contracts golden`.
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    core::{Branch, Change, Event},
    model::{
        errors::{self, ErrorClass, ErrorRecord, ErrorWire, Scope, classify, code_in, code_of},
        health::*,
        ipc::{self, *},
        orchestrator::*,
        protocol::{
            self, CatalogListReply, CatalogListRequest, LaneRole, LaneSpec, MetaExchangeHeader,
            PingReply, feed::*, frame::*, objects::*,
        },
        status::*,
    },
    paired_sync::SyncRoute,
    pairing_code::Verifier,
    session::{self, ConnHealth, ConnOptions, PeerInfo, Side},
    util::{
        self, Backoff, Cancel, Clock, ManualClock, Notify, Snapshot, SystemClock,
        fault::{self, FaultAction, FaultConfig},
        hmac::{ct_eq, hmac_sha256, hmac_sha256_parts},
        lock::{self, PoisonPolicy, Tracked},
        log::{self, MemorySink},
    },
};

// ---------------------------------------------------------------------------
// Golden fixtures

const T0: u64 = 1_790_000_000_000;
const HASH_A: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0";
const HASH_B: &str = "a0b1c2d3e4f5061728394a5b6c7d8e9fa0b1c2d3e4f5061728394a5b6c7d8e9f";
const REPO: &str = "5b0c2f0e-7d4a-4c51-9a3e-0e6f1d2c3b4a";
const DEVICE_A: &str = "3c9e7a10-1f2b-4c3d-8e9f-a0b1c2d3e4f5";
const DEVICE_B: &str = "8d7c6b5a-4f3e-4d2c-9b1a-0f9e8d7c6b5a";
const INSTALL_A: &str = "inst-aaaa-1111";
const INSTALL_B: &str = "inst-bbbb-2222";
const STATE: &str = "C:\\tkfs\\data\\folders\\5b0c2f0e";

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/contracts")
}

fn bless() -> bool {
    std::env::var("TKFS_BLESS_CONTRACTS").as_deref() == Ok("1")
}

/// Serializes `value`, compares it byte for byte (line endings normalized,
/// because git may check fixtures out with CRLF) with the fixture, then parses
/// the fixture back and re-serializes it.
fn golden<T: Serialize + DeserializeOwned>(name: &str, value: &T, failures: &mut Vec<String>) {
    let path = fixtures_dir().join(format!("{name}.json"));
    let actual = serde_json::to_string_pretty(value).unwrap() + "\n";
    if bless() {
        std::fs::create_dir_all(fixtures_dir()).unwrap();
        std::fs::write(&path, &actual).unwrap();
    }
    let Ok(fixture) = std::fs::read_to_string(&path) else {
        failures.push(format!("{name}: missing fixture {}", path.display()));
        return;
    };
    let fixture = fixture.replace("\r\n", "\n");
    if fixture != actual {
        failures.push(format!(
            "{name}: serialization differs from fixture\n--- fixture\n{fixture}--- actual\n{actual}"
        ));
        return;
    }
    match serde_json::from_str::<T>(&fixture) {
        Ok(back) => {
            let again = serde_json::to_string_pretty(&back).unwrap() + "\n";
            if again != actual {
                failures.push(format!("{name}: round trip changed the value\n{again}"));
            }
        }
        Err(error) => failures.push(format!("{name}: fixture does not parse: {error}")),
    }
}

/// [`golden`] plus structural equality of the parsed value.
fn golden_eq<T: Serialize + DeserializeOwned + PartialEq + Debug>(
    name: &str,
    value: &T,
    failures: &mut Vec<String>,
) {
    golden(name, value, failures);
    let text = serde_json::to_string(value).unwrap();
    let back: T = serde_json::from_str(&text).unwrap();
    if &back != value {
        failures.push(format!("{name}: parsed value differs: {back:?}"));
    }
}

fn error_wire() -> ErrorWire {
    ErrorWire::new(
        "MOUNT_PATH_MUST_NOT_EXIST",
        "MOUNT_PATH_MUST_NOT_EXIST: C:\\mounts\\photos exists and is not empty",
    )
}

fn transient_error() -> ErrorWire {
    ErrorWire::new(
        "REQUEST_TIMEOUT",
        "REQUEST_TIMEOUT: no reply after 30000 ms",
    )
    .with_retry_after(2000)
}

fn error_record() -> ErrorRecord {
    let mut record = ErrorRecord::new(
        transient_error(),
        Scope::FolderPeer {
            state: STATE.into(),
            installation: INSTALL_B.into(),
        },
        T0,
    );
    record.observe(transient_error(), T0 + 4000);
    record.next_retry_at_ms = Some(T0 + 6000);
    record
}

fn branch() -> Branch {
    Branch {
        id: "b0a1c2d3-e4f5-4061-8a9b-0c1d2e3f4a5b".into(),
        name: "main".into(),
        shared: true,
    }
}

fn event() -> Event {
    Event {
        id: "e1f2a3b4-c5d6-4e7f-8091-a2b3c4d5e6f7".into(),
        repo: REPO.into(),
        branch: branch(),
        device: DEVICE_A.into(),
        created_ms: T0,
        changes: vec![Change {
            entity: "f0e1d2c3-b4a5-4697-8879-6a5b4c3d2e1f".into(),
            field: "content".into(),
            parents: vec!["d1c2b3a4-9586-4776-8695-a4b3c2d1e0f9".into()],
            value: json!(HASH_A),
        }],
        dependencies: vec!["d1c2b3a4-9586-4776-8695-a4b3c2d1e0f9".into()],
        reviewed: vec![],
        object_sizes: BTreeMap::from([(HASH_A.to_owned(), 1_048_576)]),
    }
}

fn route() -> SyncRoute {
    SyncRoute {
        peer: INSTALL_B.into(),
        repo: REPO.into(),
        local_replica: DEVICE_A.into(),
        remote_replica: DEVICE_B.into(),
    }
}

fn mount_attached() -> MountStatus {
    MountStatus {
        intent: MountIntent::Attach,
        path: Some(PathBuf::from("C:\\mounts\\photos")),
        state: MountState::Attached { since_ms: T0 + 900 },
    }
}

fn mount_variants() -> Vec<MountStatus> {
    let path = Some(PathBuf::from("C:\\mounts\\photos"));
    [
        (MountIntent::Headless, None, MountState::Disabled),
        (MountIntent::Detach, path.clone(), MountState::Detached),
        (MountIntent::Attach, path.clone(), MountState::Preflight),
        (MountIntent::Attach, path.clone(), MountState::Attaching),
        (
            MountIntent::Attach,
            path.clone(),
            MountState::Attached { since_ms: T0 },
        ),
        (MountIntent::Detach, path.clone(), MountState::Detaching),
        (
            MountIntent::Attach,
            path.clone(),
            MountState::AttachFailed {
                error: error_wire(),
                attempt: 2,
                next_retry_at_ms: None,
            },
        ),
        (
            MountIntent::Attach,
            path.clone(),
            MountState::AttachFailed {
                error: ErrorWire::new("WINFSP_MOUNT_FAILED", "WINFSP_MOUNT_FAILED: 0xc0000034"),
                attempt: 3,
                next_retry_at_ms: Some(T0 + 8000),
            },
        ),
        (
            MountIntent::Attach,
            path,
            MountState::Lost {
                error: ErrorWire::new("REMOUNT_FAILED", "REMOUNT_FAILED: checkout remount"),
            },
        ),
    ]
    .into_iter()
    .map(|(intent, path, state)| MountStatus {
        intent,
        path,
        state,
    })
    .collect()
}

fn inbox() -> InboxCounts {
    InboxCounts {
        ready: 12,
        waiting: 3,
        retry: 1,
        blocked: 2,
        quarantined: 1,
        unclassified: 0,
    }
}

fn activation() -> ActivationCounters {
    ActivationCounters {
        running: true,
        activated: 2048,
        fast: 2040,
        slow: 8,
        quarantined: 1,
        retried: 4,
        stale: 0,
        slices: 40,
        last_slice_ms: 18,
        last_activation_at_ms: Some(T0 + 5000),
        starved_since_ms: None,
    }
}

fn content() -> ContentCounters {
    ContentCounters {
        current_objects: 1200,
        current_available: 1100,
        current_missing: 100,
        current_missing_bytes: 734_003_200,
        shared_objects: 1500,
        shared_available: 1380,
        shared_missing_bytes: 900_000_000,
        downloading: 2,
        partial_bytes: 3_145_728,
        requested: 7,
        complete: false,
    }
}

fn journal() -> JournalCounters {
    JournalCounters {
        events: 4096,
        shared_events: 4090,
        branches: 2,
        tip: 4096,
        epoch: Some("9f8e7d6c5b4a39281706f5e4d3c2b1a0".into()),
        outgoing_unacknowledged: 12,
        unresolved_conflicts: 1,
    }
}

fn generations() -> Generations {
    Generations {
        sync: 77,
        branches: 3,
        content: 41,
        mount: 2,
    }
}

fn gauges() -> EngineGauges {
    EngineGauges {
        waiters: 1,
        holds: 99_000,
        wait_p99_ms: 4,
        longest_hold_ms: 310,
        current_hold_ms: 0,
        stalled: false,
    }
}

fn peer_feed() -> PeerFeedCounters {
    PeerFeedCounters {
        replica: DEVICE_B.into(),
        cursor_seq: 3900,
        remote_tip: 4312,
        to_receive: 412,
        to_send: 5,
        staged_pending: 3,
        blocked: 0,
        quarantined: 0,
        rejected: 0,
        caught_up: false,
        last_exchange_ms: Some(T0 + 4500),
    }
}

fn tip() -> FeedTip {
    FeedTip {
        epoch: "9f8e7d6c5b4a39281706f5e4d3c2b1a0".into(),
        seq: 4096,
    }
}

fn heartbeat_basic() -> WorkerHeartbeat {
    WorkerHeartbeat {
        instance: "7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d".into(),
        seq: 42,
        at_ms: T0 + 42_000,
        uptime_ms: 42_000,
        mount: mount_attached(),
        engine: gauges(),
        store_health: StoreHealth::Healthy,
        pending_saves: 0,
        open_handles: 3,
        unflushed_files: 1,
        ..WorkerHeartbeat::default()
    }
}

fn heartbeat_full() -> WorkerHeartbeat {
    WorkerHeartbeat {
        generations: generations(),
        tip: Some(tip()),
        inbox: Some(inbox()),
        activation: Some(activation()),
        content: Some(content()),
        journal: Some(journal()),
        peers: vec![peer_feed()],
        ..heartbeat_basic()
    }
}

fn ready_line() -> ReadyLine {
    ReadyLine {
        ready: true,
        repo: REPO.into(),
        device: DEVICE_A.into(),
        runtime: PathBuf::from(format!("{STATE}\\runtime.json")),
        mounted: false,
        mount_error: Some(error_wire()),
        instance: "7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d".into(),
        pid: 1234,
        address: "ws://127.0.0.1:5555".into(),
        version: "0.1.0+6298d0b".into(),
        ipc: IPC_VERSION,
        token: None,
    }
}

fn fatal_line() -> FatalLine {
    FatalLine::new(
        "STORE_IDENTITY_MISMATCH",
        "STORE_IDENTITY_MISMATCH: metadata.sqlite belongs to another repository",
        EXIT_STORE,
    )
}

fn worker_record() -> WorkerRecord {
    WorkerRecord {
        format: WORKER_RECORD_FORMAT,
        pid: 1234,
        instance: "7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d".into(),
        address: "ws://127.0.0.1:5555".into(),
        version: "0.1.0+6298d0b".into(),
        ipc: IPC_VERSION,
        repo: REPO.into(),
        device: DEVICE_A.into(),
        started_at_ms: T0,
        process_start: 133_780_000_000_000_000,
        executable: PathBuf::from("C:\\Program Files\\tkfs\\tkfs.exe"),
        supervised: true,
        token: None,
    }
}

fn cursor(seq: i64, id: &str) -> Cursor {
    Cursor {
        epoch: "9f8e7d6c5b4a39281706f5e4d3c2b1a0".into(),
        seq,
        id: id.into(),
    }
}

fn feed_page() -> FeedPage {
    let payload = serde_json::to_string(&event()).unwrap();
    FeedPage {
        epoch: "9f8e7d6c5b4a39281706f5e4d3c2b1a0".into(),
        after: Some(cursor(3899, "c0c1c2c3-0000-4000-8000-000000003899")),
        reset: false,
        entries: vec![
            FeedEntry {
                seq: 3900,
                id: event().id,
                digest: digest(&payload),
                event: Some(event()),
                large: None,
            },
            FeedEntry {
                seq: 3901,
                id: "c0c1c2c3-0000-4000-8000-000000003901".into(),
                digest: "00112233aabbccdd".into(),
                event: None,
                large: None,
            },
            FeedEntry {
                seq: 3902,
                id: "c0c1c2c3-0000-4000-8000-000000003902".into(),
                digest: "8899aabbccddeeff".into(),
                event: None,
                large: Some(3_145_728),
            },
        ],
        through: cursor(3902, "c0c1c2c3-0000-4000-8000-000000003902"),
        tip: 4096,
        more: true,
    }
}

fn feed_state() -> FeedState {
    FeedState {
        epoch: "9f8e7d6c5b4a39281706f5e4d3c2b1a0".into(),
        tip: 4096,
        pending_active: 15,
        blocked: 2,
        quarantined: 1,
    }
}

fn exchange_request() -> MetaExchangeRequest {
    MetaExchangeRequest {
        format: FEED_FORMAT,
        request: "11111111-2222-4333-8444-555555555555".into(),
        sender: DEVICE_A.into(),
        receiver: DEVICE_B.into(),
        repo: REPO.into(),
        page: feed_page(),
        cursor: Some(cursor(120, "c0c1c2c3-0000-4000-8000-000000000120")),
        need: vec!["c0c1c2c3-0000-4000-8000-000000000077".into()],
        state: feed_state(),
    }
}

fn summary() -> ExchangeSummary {
    ExchangeSummary {
        sent_events: 3,
        received_events: 1,
        staged: 1,
        rejected: 1,
        acked: 2,
        local_tip: 4096,
        remote_tip: 130,
        cursor_in: 121,
        cursor_out: 3902,
        to_send: 194,
        to_receive: 9,
        local_pending: 15,
        remote_pending: 0,
        blocked: 2,
        quarantined: 1,
        caught_up: false,
        metadata_advanced: true,
        bytes_up: 18_000,
        bytes_down: 2_300,
    }
}

fn exchange_reply() -> MetaExchangeReply {
    MetaExchangeReply {
        format: FEED_FORMAT,
        reply_to: "11111111-2222-4333-8444-555555555555".into(),
        sender: DEVICE_B.into(),
        receiver: DEVICE_A.into(),
        repo: REPO.into(),
        page_status: PageStatus::Accepted,
        cursor: cursor(3902, "c0c1c2c3-0000-4000-8000-000000003902"),
        page: FeedPage {
            epoch: "00aa11bb22cc33dd44ee55ff66778899".into(),
            after: Some(Cursor {
                epoch: "00aa11bb22cc33dd44ee55ff66778899".into(),
                seq: 120,
                id: "c0c1c2c3-0000-4000-8000-000000000120".into(),
            }),
            reset: false,
            entries: vec![],
            through: Cursor {
                epoch: "00aa11bb22cc33dd44ee55ff66778899".into(),
                seq: 130,
                id: "c0c1c2c3-0000-4000-8000-000000000130".into(),
            },
            tip: 130,
            more: false,
        },
        staged: 1,
        rejected: vec![RejectedEvent {
            id: "c0c1c2c3-0000-4000-8000-000000003901".into(),
            seq: 3901,
            code: "FORGED_EVENT_ORIGIN".into(),
            class: ErrorClass::Fatal,
        }],
        need_served: vec![event()],
        unknown: vec!["c0c1c2c3-0000-4000-8000-000000000999".into()],
        state: FeedState {
            epoch: "00aa11bb22cc33dd44ee55ff66778899".into(),
            tip: 130,
            pending_active: 0,
            blocked: 0,
            quarantined: 0,
        },
    }
}

fn apply_result() -> FeedApplyResult {
    FeedApplyResult {
        summary: summary(),
        gate: GateView {
            caught_up: false,
            waiting_on: vec![
                "412 events remaining at source".into(),
                "2 events quarantined".into(),
            ],
            blocked: true,
        },
    }
}

fn want_item(hash: &str, priority: WantPriority) -> WantItem {
    WantItem {
        hash: hash.into(),
        size: 8_388_608,
        offset: 1_048_576,
        priority,
    }
}

fn folder_record() -> FolderRecord {
    FolderRecord {
        state: STATE.into(),
        label: "Photos".into(),
        repo: REPO.into(),
        device: DEVICE_A.into(),
        directory: PathBuf::from(STATE),
        mount: Some(PathBuf::from("C:\\mounts\\photos")),
        desired_running: true,
        sync_enabled: true,
        mount_enabled: true,
    }
}

fn peer_record() -> PeerRecord {
    PeerRecord {
        endpoint: "wss://peer.example:43180".into(),
        code: "SECRET-PAIRING-CODE-never-serialized".into(),
        installation: INSTALL_B.into(),
        connections: 4,
        revision: "rev-7".into(),
    }
}

fn exit_record() -> ExitRecord {
    ExitRecord {
        code: Some(73),
        fatal: Some(fatal_line()),
        stderr_tail: "store open failed\n".into(),
        at_ms: T0 + 1500,
    }
}

fn worker_endpoint() -> WorkerEndpoint {
    WorkerEndpoint {
        instance: "7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d".into(),
        pid: 1234,
        address: "ws://127.0.0.1:5555".into(),
        version: "0.1.0+6298d0b".into(),
        token: None,
    }
}

fn folder_view() -> FolderView {
    FolderView {
        generation: 9,
        record: folder_record(),
        phase: FolderPhase::Running {
            since_ms: T0,
            instance: "7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d".into(),
            pid: 1234,
            address: "ws://127.0.0.1:5555".into(),
            adopted: false,
        },
        gate: GateState::Awaiting {
            source: INSTALL_B.into(),
            since_ms: T0,
            waiting_on: vec!["412 events remaining at source".into()],
        },
        mount: mount_attached(),
        worker: Some(worker_endpoint()),
        heartbeat: Some(Arc::new(heartbeat_basic())),
        last_error: None,
        restarts: 1,
        degraded: false,
        published_error: Some(ErrorRecord::new(
            ErrorWire::new(
                "PUBLISHED_SUMMARY_FAILED",
                "PUBLISHED_SUMMARY_FAILED: REQUEST_TIMEOUT",
            ),
            Scope::Folder {
                state: STATE.into(),
            },
            T0 + 100,
        )),
    }
}

fn published_entry() -> PublishedEntry {
    PublishedEntry {
        repo: REPO.into(),
        replica: DEVICE_A.into(),
        label: "Photos".into(),
        branches: vec![branch()],
        state: EntryState::Running,
        tip: Some(tip()),
        sync_generation: 77,
        content: Some(content()),
        error: None,
        updated_at_ms: T0 + 2000,
    }
}

fn transfer(remaining: u64) -> TransferProgress {
    TransferProgress {
        objects_remaining: remaining,
        bytes_remaining: remaining * 8_388_608,
        in_flight: 2,
        rate_bps: 3_400_000,
        eta_s: Some(360),
        total_bytes: 3_221_225_472,
        last_progress_ms: Some(T0 + 9000),
    }
}

fn status_snapshot() -> StatusSnapshot {
    StatusSnapshot {
        schema: STATUS_SCHEMA,
        generation: 314,
        at_ms: T0 + 10_000,
        daemon: DaemonStatus {
            installation: INSTALL_A.into(),
            version: "0.1.0+6298d0b".into(),
            pid: 4321,
            started_at_ms: T0 - 60_000,
            stopping: false,
            control: "ws://127.0.0.1:43181".into(),
            network: Some("ws://0.0.0.0:43180".into()),
            log_path: Some("C:\\tkfs\\data\\logs\\orchestrator.log".into()),
            errors: vec![],
        },
        folders: vec![FolderStatus {
            state: STATE.into(),
            label: "Photos".into(),
            repo: REPO.into(),
            device: DEVICE_A.into(),
            lifecycle: LifecycleStatus {
                phase: LifecyclePhase::Running,
                detail: None,
                since_ms: T0,
                pid: Some(1234),
                instance: Some("7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d".into()),
                adopted: true,
                restarts: 1,
                next_retry_at_ms: None,
                last_exit: Some(exit_record()),
                degraded: false,
                startup: Some(StartupProgress {
                    phase: "verifying objects".into(),
                    seq: 37,
                }),
            },
            intents: Intents {
                desired_running: true,
                sync_enabled: true,
                mount_enabled: true,
            },
            mount: mount_attached(),
            gate: GateStatus {
                state: GateKind::Awaiting,
                source: Some(INSTALL_B.into()),
                since_ms: Some(T0),
                waiting_on: vec!["412 events remaining at source".into()],
            },
            store: StoreStatus {
                health: StoreHealth::Healthy,
                pending_saves: 0,
                unflushed_files: 1,
                open_handles: 3,
            },
            activation: ActivationStatus {
                inbox: inbox(),
                counters: activation(),
            },
            content: content(),
            journal: journal(),
            sync: FolderSync {
                state: FolderSyncState::Downloading,
                summary: "1 of 1 peers".into(),
                peers: vec![FolderPeerSync {
                    installation: INSTALL_B.into(),
                    endpoint: Some("wss://peer.example:43180".into()),
                    label: Some("laptop".into()),
                    direction: Direction::Outbound,
                    state: FolderPeerState::Downloading,
                    metadata: MetaProgress {
                        to_send: 5,
                        to_receive: 412,
                        local_pending: 15,
                        remote_pending: 0,
                        blocked: 2,
                        quarantined: 1,
                        rejected: 1,
                        caught_up: false,
                        cursor_seq: 3900,
                        remote_tip: 4312,
                        last_exchange_ms: Some(T0 + 4500),
                    },
                    objects_down: transfer(100),
                    objects_up: TransferProgress::default(),
                    last_progress_ms: Some(T0 + 9000),
                    stalled_since_ms: None,
                    backoff_until_ms: None,
                    error: Some(error_record()),
                }],
            },
            errors: vec![error_record()],
        }],
        peers: vec![PeerStatus {
            installation: INSTALL_B.into(),
            endpoint: Some("wss://peer.example:43180".into()),
            label: Some("laptop".into()),
            direction: Direction::Both,
            state: PeerConnState::Online,
            protocol: Some("tkfs-peer/3".into()),
            since_ms: Some(T0 - 30_000),
            rtt_ms: Some(42),
            rate_up_bps: 12_000,
            rate_down_bps: 3_400_000,
            lanes: vec![
                LaneStatus {
                    lane: 0,
                    role: LaneRole::Control,
                    direction: Direction::Outbound,
                    state: LaneState::Ready,
                    since_ms: Some(T0 - 30_000),
                    inflight: vec![InflightOp {
                        op: "meta.exchange".into(),
                        repo: REPO.into(),
                        since_ms: T0 + 9900,
                        bytes: 0,
                    }],
                    bytes_up: 1_000_000,
                    bytes_down: 2_000_000,
                    rate_up_bps: 1000,
                    rate_down_bps: 2000,
                    reconnects: 0,
                    backoff_until_ms: None,
                    error: None,
                },
                LaneStatus {
                    lane: 1,
                    role: LaneRole::Bulk,
                    direction: Direction::Outbound,
                    state: LaneState::Backoff,
                    since_ms: Some(T0 + 8000),
                    inflight: vec![],
                    bytes_up: 0,
                    bytes_down: 900_000_000,
                    rate_up_bps: 0,
                    rate_down_bps: 0,
                    reconnects: 3,
                    backoff_until_ms: Some(T0 + 12_000),
                    error: Some(error_record()),
                },
            ],
            inbound_sessions: 2,
            shared_folders: vec![REPO.into()],
            error: None,
        }],
        activity: vec![ActivityEntry {
            at_ms: T0 + 9500,
            kind: "exchange".into(),
            folder: Some(STATE.into()),
            peer: Some(INSTALL_B.into()),
            message: "received 1 event, staged 1".into(),
        }],
    }
}

fn status_events() -> Vec<StatusEvent> {
    vec![
        StatusEvent::FolderLifecycle(Box::new(folder_view())),
        StatusEvent::WorkerHeartbeat {
            state: STATE.into(),
            hb: Arc::new(heartbeat_full()),
        },
        StatusEvent::FolderRemoved {
            state: STATE.into(),
        },
        StatusEvent::PeerConfigured {
            installation: INSTALL_B.into(),
            endpoint: "wss://peer.example:43180".into(),
        },
        StatusEvent::PeerRemoved {
            installation: INSTALL_B.into(),
        },
        StatusEvent::LaneState {
            installation: INSTALL_B.into(),
            lane: 1,
            role: LaneRole::Bulk,
            direction: Direction::Outbound,
            state: LaneState::Backoff,
            error: Some(transient_error()),
            backoff_until_ms: Some(T0 + 12_000),
            rtt_ms: None,
        },
        StatusEvent::ExchangeStarted {
            installation: INSTALL_B.into(),
            repo: REPO.into(),
            lane: 0,
            op: "meta.exchange".into(),
            at_ms: T0 + 9900,
        },
        StatusEvent::ExchangeFinished {
            installation: INSTALL_B.into(),
            repo: REPO.into(),
            lane: 0,
            op: "meta.exchange".into(),
            direction: Direction::Outbound,
            ok: true,
            error: None,
            summary: Some(summary()),
            bytes_up: 18_000,
            bytes_down: 2_300,
            objects_up: 0,
            objects_down: 0,
            duration_ms: 35,
        },
        StatusEvent::PeerCatalog {
            installation: INSTALL_B.into(),
            entries: vec![published_entry()],
        },
        StatusEvent::InboundSession {
            installation: INSTALL_B.into(),
            lane: 0,
            role: LaneRole::Control,
            opened: true,
        },
        StatusEvent::InboundIncompatible {
            address: "192.0.2.10:50123".into(),
            detail: "PEER_NEEDS_UPGRADE: legacy hello version 2".into(),
        },
        StatusEvent::ObjectError {
            state: STATE.into(),
            hash: HASH_A.into(),
            error: ErrorWire::new("CORRUPT_AT_SOURCE", "CORRUPT_AT_SOURCE: hash mismatch"),
        },
        StatusEvent::LegacyLink {
            key: format!("wss://peer.example:43180\u{1f}0\u{1f}{REPO}"),
            value: json!({"connected": true, "exchanges": 12}),
        },
        StatusEvent::Error {
            scope: Scope::Peer {
                installation: INSTALL_B.into(),
            },
            error: ErrorWire::new("INVALID_PAIRING_CODE", "INVALID_PAIRING_CODE"),
        },
        StatusEvent::Resolved {
            scope: Scope::Peer {
                installation: INSTALL_B.into(),
            },
            code: "INVALID_PAIRING_CODE".into(),
        },
        StatusEvent::Activity(ActivityEntry {
            at_ms: T0,
            kind: "lifecycle".into(),
            folder: Some(STATE.into()),
            peer: None,
            message: "folder started".into(),
        }),
    ]
}

#[test]
fn golden_fixtures_round_trip_byte_for_byte() {
    let mut f = Vec::new();
    let f = &mut f;

    // errors
    golden_eq("errors.error_wire", &error_wire(), f);
    golden_eq("errors.error_record", &error_record(), f);
    golden_eq(
        "errors.error_class",
        &vec![
            ErrorClass::Contention,
            ErrorClass::Transient,
            ErrorClass::NeedsUserAction,
            ErrorClass::Incompatible,
            ErrorClass::Fatal,
        ],
        f,
    );
    golden_eq(
        "errors.scope",
        &vec![
            Scope::Daemon,
            Scope::Folder {
                state: STATE.into(),
            },
            Scope::Peer {
                installation: INSTALL_B.into(),
            },
            Scope::Lane {
                installation: INSTALL_B.into(),
                lane: 2,
            },
            Scope::FolderPeer {
                state: STATE.into(),
                installation: INSTALL_B.into(),
            },
            Scope::Object {
                state: STATE.into(),
                hash: HASH_A.into(),
            },
            Scope::Event {
                state: STATE.into(),
                id: event().id,
            },
            Scope::Mount {
                state: STATE.into(),
            },
        ],
        f,
    );

    // health
    golden_eq("health.mount_status", &mount_variants(), f);
    golden_eq(
        "health.mount_intent",
        &vec![
            MountIntent::Headless,
            MountIntent::Attach,
            MountIntent::Detach,
        ],
        f,
    );
    golden_eq(
        "health.store_health",
        &vec![
            StoreHealth::Healthy,
            StoreHealth::Degraded {
                error: ErrorWire::new("RECOVERY_REQUIRED", "RECOVERY_REQUIRED: pending save"),
            },
            StoreHealth::Failed {
                error: ErrorWire::new("INVALID_STORE_METADATA", "INVALID_STORE_METADATA"),
            },
        ],
        f,
    );
    golden_eq("health.inbox_counts", &inbox(), f);
    golden_eq("health.activation_counters", &activation(), f);
    golden_eq("health.content_counters", &content(), f);
    golden_eq("health.journal_counters", &journal(), f);
    golden_eq("health.generations", &generations(), f);
    golden_eq("health.engine_gauges", &gauges(), f);
    golden_eq("health.peer_feed_counters", &peer_feed(), f);

    // ipc
    golden_eq(
        "ipc.progress_line",
        &ProgressLine {
            startup_progress: 12,
            phase: "verifying available objects".into(),
        },
        f,
    );
    golden_eq("ipc.ready_line", &ready_line(), f);
    golden_eq("ipc.fatal_line", &fatal_line(), f);
    golden_eq(
        "ipc.startup_line",
        &vec![
            StartupLine::Progress(ProgressLine {
                startup_progress: 1,
                phase: "opening state".into(),
            }),
            StartupLine::Ready(ready_line()),
            StartupLine::Fatal(fatal_line()),
        ],
        f,
    );
    golden_eq("ipc.worker_record", &worker_record(), f);
    golden_eq(
        "ipc.hello_reply",
        &HelloReply {
            instance: "7a6b5c4d-3e2f-4a1b-9c8d-7e6f5a4b3c2d".into(),
            pid: 1234,
            repo: REPO.into(),
            device: DEVICE_A.into(),
            version: "0.1.0+6298d0b".into(),
            ipc: IPC_VERSION,
            started_at_ms: T0,
            process_start: 133_780_000_000_000_000,
        },
        f,
    );
    golden_eq(
        "ipc.watch_request",
        &WatchRequest {
            after_seq: 41,
            wait_ms: 2000,
        },
        f,
    );
    golden_eq(
        "ipc.stop_request",
        &vec![
            StopRequest {
                mode: StopMode::Graceful,
            },
            StopRequest {
                mode: StopMode::Force,
            },
        ],
        f,
    );
    golden_eq(
        "ipc.stop_reply",
        &StopReply {
            stopped: true,
            persisted_dirty: 2,
            detached: true,
        },
        f,
    );
    golden_eq(
        "ipc.mount_attach_request",
        &MountAttachRequest {
            path: PathBuf::from("C:\\mounts\\photos"),
        },
        f,
    );
    golden_eq(
        "ipc.mount_detach_request",
        &MountDetachRequest { force: true },
        f,
    );
    golden_eq(
        "ipc.revalidate_request",
        &vec![
            RevalidateRequest { ids: None },
            RevalidateRequest {
                ids: Some(vec![event().id]),
            },
        ],
        f,
    );
    golden_eq("ipc.revalidate_reply", &RevalidateReply { requeued: 3 }, f);
    golden_eq(
        "ipc.published_summary",
        &PublishedSummary {
            repo: REPO.into(),
            replica: DEVICE_A.into(),
            label: "Photos".into(),
            branches: vec![branch()],
            tip: tip(),
            sync_generation: 77,
        },
        f,
    );
    golden_eq(
        "ipc.status_extras",
        &StatusExtras {
            activation: activation(),
            mount: mount_attached(),
            store_health: StoreHealth::Healthy,
            inbox: inbox(),
            heartbeat_seq: 42,
        },
        f,
    );
    golden_eq("ipc.worker_heartbeat_basic", &heartbeat_basic(), f);
    golden_eq("ipc.worker_heartbeat_full", &heartbeat_full(), f);
    golden(
        "ipc.feed_offer_request",
        &FeedOfferRequest { route: route() },
        f,
    );
    golden(
        "ipc.feed_exchange_request",
        &FeedExchangeRequest {
            route: route(),
            request: exchange_request(),
        },
        f,
    );
    golden_eq(
        "ipc.feed_exchange_reply",
        &FeedExchangeReply {
            reply: exchange_reply(),
            local: apply_result(),
        },
        f,
    );
    golden(
        "ipc.feed_apply_request",
        &FeedApplyRequest {
            route: route(),
            request_id: "11111111-2222-4333-8444-555555555555".into(),
            reply: exchange_reply(),
        },
        f,
    );

    // protocol (top level)
    golden_eq(
        "protocol.lane_role",
        &vec![LaneRole::Control, LaneRole::Bulk, LaneRole::Probe],
        f,
    );
    golden_eq("protocol.lane_spec", &LaneSpec::new(2, 3), f);
    golden_eq("protocol.ping_reply", &PingReply { alive: true }, f);
    golden_eq(
        "protocol.catalog_list_request",
        &vec![
            CatalogListRequest { since: None },
            CatalogListRequest { since: Some(41) },
        ],
        f,
    );
    golden_eq(
        "protocol.catalog_list_reply",
        &vec![
            CatalogListReply {
                generation: 42,
                unchanged: false,
                entries: vec![published_entry()],
            },
            CatalogListReply {
                generation: 42,
                unchanged: true,
                entries: vec![],
            },
        ],
        f,
    );
    golden_eq(
        "protocol.meta_exchange_header",
        &MetaExchangeHeader { repo: REPO.into() },
        f,
    );

    // protocol::frame
    golden_eq("frame.frame_kind", &FrameKind::ALL.to_vec(), f);
    golden_eq(
        "frame.client_hello",
        &ClientHello {
            protocol: protocol::PROTOCOL_NAME.into(),
            versions: protocol::SUPPORTED_VERSIONS.to_vec(),
            installation: INSTALL_A.into(),
            role: LaneRole::Bulk,
            lane: Some(LaneSpec::new(1, 3)),
            pool: 4,
            session: "2b3c4d5e-6f70-4812-9a3b-4c5d6e7f8091".into(),
            client_nonce: "11".repeat(32),
            features: vec![],
        },
        f,
    );
    golden_eq(
        "frame.server_hello",
        &ServerHello {
            protocol: protocol::PROTOCOL_NAME.into(),
            version: protocol::PROTOCOL_VERSION,
            installation: INSTALL_B.into(),
            role: LaneRole::Bulk,
            lane: Some(LaneSpec::new(1, 3)),
            salt: "22".repeat(32),
            server_nonce: "33".repeat(32),
            verifier_generation: 5,
            features: vec![],
            limits: Limits::default(),
        },
        f,
    );
    golden_eq(
        "frame.auth",
        &Auth {
            proof: "44".repeat(32),
        },
        f,
    );
    golden_eq(
        "frame.auth_ok",
        &AuthOk {
            server_proof: "55".repeat(32),
        },
        f,
    );
    golden_eq(
        "frame.request_header",
        &vec![
            RequestHeader {
                id: 7,
                op: protocol::ops::META_EXCHANGE.into(),
                deadline_ms: Some(30_000),
                idle_ms: None,
                header: json!({"repo": REPO}),
                total: Some(3_145_728),
            },
            RequestHeader {
                id: 8,
                op: protocol::ops::OBJECTS_PULL.into(),
                deadline_ms: None,
                idle_ms: Some(30_000),
                header: Value::Null,
                total: None,
            },
        ],
        f,
    );
    golden_eq(
        "frame.response_header",
        &vec![
            ResponseHeader {
                id: 7,
                ok: true,
                header: json!({}),
                error: None,
                total: None,
            },
            ResponseHeader {
                id: 9,
                ok: false,
                header: Value::Null,
                error: Some(ErrorWire::new("BUSY", "BUSY: handler queue full")),
                total: None,
            },
        ],
        f,
    );
    golden_eq(
        "frame.chunk_header",
        &vec![
            ChunkHeader {
                id: 7,
                hash: None,
                offset: 1_048_576,
                total: 3_145_728,
                last: false,
            },
            ChunkHeader {
                id: 8,
                hash: Some(HASH_A.into()),
                offset: 7_340_032,
                total: 8_388_608,
                last: true,
            },
        ],
        f,
    );
    golden_eq(
        "frame.progress_header",
        &ProgressHeader {
            id: 8,
            bytes: Some(4_194_304),
        },
        f,
    );
    golden_eq("frame.cancel_header", &CancelHeader { id: 8 }, f);
    golden_eq(
        "frame.heartbeat",
        &vec![
            Heartbeat {
                seq: 1,
                at_ms: T0,
                echo: None,
            },
            Heartbeat {
                seq: 2,
                at_ms: T0 + 5000,
                echo: Some(HeartbeatEcho {
                    seq: 9,
                    at_ms: T0 + 4100,
                    held_ms: 870,
                }),
            },
        ],
        f,
    );
    golden_eq(
        "frame.goaway",
        &Goaway {
            code: "PAIRING_CODE_CHANGED".into(),
            message: "PAIRING_CODE_CHANGED: the verifier was rotated".into(),
        },
        f,
    );

    // protocol::feed
    golden_eq(
        "feed.cursor",
        &cursor(3902, "c0c1c2c3-0000-4000-8000-000000003902"),
        f,
    );
    golden_eq("feed.feed_page", &feed_page(), f);
    golden_eq("feed.feed_state", &feed_state(), f);
    golden_eq(
        "feed.page_status",
        &vec![
            PageStatus::Accepted,
            PageStatus::CursorMismatch {
                expected: cursor(120, "c0c1c2c3-0000-4000-8000-000000000120"),
            },
            PageStatus::Reset,
        ],
        f,
    );
    golden_eq("feed.meta_exchange_request", &exchange_request(), f);
    golden_eq("feed.meta_exchange_reply", &exchange_reply(), f);
    golden_eq("feed.exchange_summary", &summary(), f);
    golden_eq("feed.feed_apply_result", &apply_result(), f);

    // protocol::objects
    golden_eq(
        "objects.want_item",
        &vec![
            want_item(HASH_A, WantPriority::Demand),
            want_item(HASH_B, WantPriority::Current),
            WantItem {
                hash: HASH_B.into(),
                size: 4096,
                offset: 0,
                priority: WantPriority::History,
            },
        ],
        f,
    );
    golden_eq(
        "objects.objects_pull",
        &ObjectsPull {
            repo: REPO.into(),
            sender: DEVICE_A.into(),
            receiver: DEVICE_B.into(),
            lane: LaneSpec::new(1, 3),
            items: vec![want_item(HASH_A, WantPriority::Demand)],
            budget_bytes: PULL_BUDGET_BYTES,
        },
        f,
    );
    golden_eq(
        "objects.objects_wants_request",
        &ObjectsWantsRequest {
            repo: REPO.into(),
            sender: DEVICE_A.into(),
            receiver: DEVICE_B.into(),
            lane: LaneSpec::new(1, 3),
            max_items: 256,
            max_bytes: PULL_BUDGET_BYTES,
        },
        f,
    );
    golden_eq(
        "objects.objects_wants_reply",
        &ObjectsWantsReply {
            items: vec![want_item(HASH_B, WantPriority::Current)],
        },
        f,
    );
    golden_eq(
        "objects.objects_push",
        &ObjectsPush {
            repo: REPO.into(),
            sender: DEVICE_A.into(),
            receiver: DEVICE_B.into(),
            lane: LaneSpec::new(2, 3),
            items: vec![want_item(HASH_B, WantPriority::History)],
        },
        f,
    );
    golden_eq(
        "objects.objects_report",
        &ObjectsReport {
            repo: REPO.into(),
            sender: DEVICE_A.into(),
            hash: HASH_A.into(),
            code: "CORRUPT_AT_SOURCE".into(),
        },
        f,
    );
    let outcomes = vec![
        ObjectOutcome::Completed,
        ObjectOutcome::Progressed {
            durable_offset: 2_097_152,
        },
        ObjectOutcome::Skipped,
        ObjectOutcome::InFlightElsewhere,
        ObjectOutcome::NotAvailable,
        ObjectOutcome::NotPermitted,
        ObjectOutcome::CorruptAtSource,
        ObjectOutcome::SizeMismatch,
        ObjectOutcome::Unsolicited,
        ObjectOutcome::Failed(ErrorWire::new(
            "OBJECT_IO_ERROR",
            "OBJECT_IO_ERROR: disk full",
        )),
    ];
    golden_eq("objects.object_outcome", &outcomes, f);
    golden_eq(
        "objects.objects_result",
        &ObjectsResult {
            results: outcomes
                .iter()
                .enumerate()
                .map(|(n, outcome)| ObjectResult {
                    hash: if n % 2 == 0 { HASH_A } else { HASH_B }.into(),
                    outcome: outcome.clone(),
                    bytes: 1_048_576 * n as u64,
                })
                .collect(),
        },
        f,
    );
    golden(
        "objects.want_header",
        &WantHeader {
            route: route(),
            lane: LaneSpec::new(1, 3),
            max_items: 256,
            max_bytes: PULL_BUDGET_BYTES,
            session: "2b3c4d5e-6f70-4812-9a3b-4c5d6e7f8091".into(),
        },
        f,
    );
    golden_eq(
        "objects.want_reply",
        &WantReply {
            items: vec![want_item(HASH_A, WantPriority::Demand)],
        },
        f,
    );
    golden(
        "objects.read_header",
        &ReadHeader {
            route: route(),
            hash: HASH_A.into(),
            offset: 1_048_576,
            len: MAX_READ_LEN,
        },
        f,
    );
    golden_eq("objects.read_reply", &ReadReply { total: 8_388_608 }, f);
    let items = vec![
        WriteItem {
            hash: HASH_A.into(),
            offset: 1_048_576,
            total: 8_388_608,
            len: 1_048_576,
            last: false,
        },
        WriteItem {
            hash: HASH_B.into(),
            offset: 0,
            total: 4096,
            len: 4096,
            last: true,
        },
    ];
    golden_eq("objects.write_item", &items, f);
    golden(
        "objects.write_header",
        &WriteHeader {
            route: route(),
            session: "2b3c4d5e-6f70-4812-9a3b-4c5d6e7f8091".into(),
            items,
        },
        f,
    );
    golden_eq(
        "objects.write_reply",
        &WriteReply {
            results: vec![
                WriteResult {
                    hash: HASH_A.into(),
                    outcome: ObjectOutcome::Progressed {
                        durable_offset: 2_097_152,
                    },
                    durable_offset: 2_097_152,
                },
                WriteResult {
                    hash: HASH_B.into(),
                    outcome: ObjectOutcome::Completed,
                    durable_offset: 4096,
                },
            ],
        },
        f,
    );
    golden(
        "objects.report_header",
        &ReportHeader {
            route: route(),
            hash: HASH_A.into(),
            code: "CORRUPT_AT_SOURCE".into(),
        },
        f,
    );
    golden_eq(
        "objects.report_reply",
        &vec![
            ReportReply {
                action: ReportAction::Quarantined,
            },
            ReportReply {
                action: ReportAction::VerifiedOk,
            },
        ],
        f,
    );

    // status
    golden_eq("status.status_snapshot", &status_snapshot(), f);
    golden_eq("status.status_event", &status_events(), f);
    golden_eq(
        "status.lifecycle_phase",
        &vec![
            LifecyclePhase::Stopped,
            LifecyclePhase::Starting,
            LifecyclePhase::Running,
            LifecyclePhase::StopBlocked,
            LifecyclePhase::Stopping,
            LifecyclePhase::Backoff,
            LifecyclePhase::Failed,
        ],
        f,
    );
    golden_eq(
        "status.gate_kind",
        &vec![
            GateKind::None,
            GateKind::Awaiting,
            GateKind::Blocked,
            GateKind::Satisfied,
            GateKind::Overridden,
        ],
        f,
    );
    golden_eq(
        "status.folder_sync_state",
        &vec![
            FolderSyncState::Stopped,
            FolderSyncState::NotShared,
            FolderSyncState::Paused,
            FolderSyncState::Offline,
            FolderSyncState::InitialSync,
            FolderSyncState::Metadata,
            FolderSyncState::Downloading,
            FolderSyncState::Uploading,
            FolderSyncState::Syncing,
            FolderSyncState::Blocked,
            FolderSyncState::Backoff,
            FolderSyncState::Stalled,
            FolderSyncState::NeedsAttention,
            FolderSyncState::UpToDate,
        ],
        f,
    );
    golden_eq(
        "status.folder_peer_state",
        &vec![
            FolderPeerState::NotShared,
            FolderPeerState::Offline,
            FolderPeerState::Connecting,
            FolderPeerState::Incompatible,
            FolderPeerState::Unauthorized,
            FolderPeerState::WaitingForWorker,
            FolderPeerState::WaitingForRemote,
            FolderPeerState::Paused,
            FolderPeerState::Metadata,
            FolderPeerState::Downloading,
            FolderPeerState::Uploading,
            FolderPeerState::Syncing,
            FolderPeerState::Blocked,
            FolderPeerState::Backoff,
            FolderPeerState::Stalled,
            FolderPeerState::UpToDate,
        ],
        f,
    );
    golden_eq(
        "status.direction",
        &vec![Direction::Outbound, Direction::Inbound, Direction::Both],
        f,
    );
    golden_eq(
        "status.peer_conn_state",
        &vec![
            PeerConnState::Online,
            PeerConnState::Connecting,
            PeerConnState::Offline,
            PeerConnState::Backoff,
            PeerConnState::Incompatible,
            PeerConnState::Unauthorized,
            PeerConnState::Revoked,
        ],
        f,
    );
    golden_eq(
        "status.lane_state",
        &vec![
            LaneState::Idle,
            LaneState::Dialing,
            LaneState::Handshaking,
            LaneState::Ready,
            LaneState::Backoff,
            LaneState::Incompatible,
            LaneState::Unauthorized,
            LaneState::Closed,
        ],
        f,
    );

    // orchestrator
    golden_eq("orchestrator.folder_record", &folder_record(), f);
    golden("orchestrator.peer_record", &peer_record(), f);
    golden_eq(
        "orchestrator.gate_record",
        &GateRecord {
            state: STATE.into(),
            source_installation: Some(INSTALL_B.into()),
            source_endpoint: "wss://peer.example:43180".into(),
            created_ms: T0,
            override_ms: None,
        },
        f,
    );
    golden_eq(
        "orchestrator.peer_meta",
        &PeerMeta {
            installation: INSTALL_B.into(),
            last_seen_ms: Some(T0 + 100),
            display_name: Some("laptop".into()),
            incompatible: Some("PEER_NEEDS_UPGRADE".into()),
            revoked_ms: None,
        },
        f,
    );
    golden_eq(
        "orchestrator.folder_cmd",
        &vec![
            FolderCmd::Start,
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            FolderCmd::ForceStop,
            FolderCmd::Restart,
            FolderCmd::Attach,
            FolderCmd::Detach { force: true },
            FolderCmd::SetSync { enabled: false },
            FolderCmd::OverrideGate,
            FolderCmd::Retry,
            FolderCmd::Revalidate {
                ids: Some(vec![event().id]),
            },
            FolderCmd::CatalogChanged,
            FolderCmd::GateEvidence(GateEvidence {
                installation: INSTALL_B.into(),
                at_ms: T0,
                caught_up: true,
                blocked: false,
                waiting_on: vec![],
            }),
            FolderCmd::Tick,
        ],
        f,
    );
    golden_eq(
        "orchestrator.folder_phase",
        &vec![
            FolderPhase::Stopped,
            FolderPhase::Starting {
                since_ms: T0,
                attempt: 2,
                phase: "verifying objects".into(),
                progress_seq: 37,
                adopting: false,
            },
            folder_view().phase,
            FolderPhase::StopBlocked {
                since_ms: T0,
                reason: ErrorWire::new("BUSY_VIEW", "BUSY_VIEW: 2 open handles"),
                open_handles: 2,
                unsaved: 1,
            },
            FolderPhase::Stopping {
                since_ms: T0,
                stage: StopStage::AwaitExit,
                deadline_ms: T0 + 10_000,
                force: true,
            },
            FolderPhase::Backoff {
                until_ms: T0 + 4000,
                attempt: 3,
                last_exit: exit_record(),
            },
            FolderPhase::Failed {
                error: ErrorRecord::new(
                    ErrorWire::new("CRASH_LOOP", "CRASH_LOOP: 5 failures in 10 min"),
                    Scope::Folder {
                        state: STATE.into(),
                    },
                    T0,
                ),
            },
        ],
        f,
    );
    golden_eq(
        "orchestrator.stop_stage",
        &vec![
            StopStage::Requested,
            StopStage::AwaitExit,
            StopStage::Killing,
        ],
        f,
    );
    golden_eq(
        "orchestrator.gate_state",
        &vec![
            GateState::None,
            folder_view().gate,
            GateState::Blocked {
                source: INSTALL_B.into(),
                reason: "2 events quarantined".into(),
            },
            GateState::Satisfied { at_ms: T0 + 1 },
            GateState::Overridden { at_ms: T0 + 2 },
        ],
        f,
    );
    golden_eq("orchestrator.folder_view", &folder_view(), f);
    golden_eq("orchestrator.worker_endpoint", &worker_endpoint(), f);
    golden_eq("orchestrator.exit_record", &exit_record(), f);
    golden_eq(
        "orchestrator.shutdown_outcome",
        &vec![
            ShutdownOutcome {
                state: STATE.into(),
                outcome: outcomes::STOPPED.into(),
                error: None,
            },
            ShutdownOutcome {
                state: STATE.into(),
                outcome: outcomes::LEFT_RUNNING.into(),
                error: Some(ErrorWire::new("BUSY_VIEW", "BUSY_VIEW: 1 open handle")),
            },
        ],
        f,
    );
    golden_eq("orchestrator.published_entry", &published_entry(), f);
    golden_eq(
        "orchestrator.entry_state",
        &vec![
            EntryState::Running,
            EntryState::Starting,
            EntryState::Paused,
            EntryState::Unavailable,
        ],
        f,
    );
    golden_eq("orchestrator.feed_tip", &tip(), f);
    golden_eq(
        "orchestrator.launch_spec",
        &LaunchSpec {
            state: STATE.into(),
            directory: PathBuf::from(STATE),
            executable: PathBuf::from("C:\\Program Files\\tkfs\\tkfs.exe"),
            supervised: true,
            log: PathBuf::from(format!("{STATE}\\worker.log")),
            env: vec![("TKFS_FAULT_SCOPE".into(), STATE.into())],
        },
        f,
    );
    golden_eq(
        "orchestrator.process_event",
        &vec![
            ProcessEvent::Line(StartupLine::Ready(ready_line())),
            ProcessEvent::Exited {
                code: Some(73),
                fatal: Some(fatal_line()),
                stderr_tail: "store open failed\n".into(),
            },
        ],
        f,
    );
    golden_eq(
        "orchestrator.pair_result",
        &PairResult {
            endpoint: "wss://peer.example:43180".into(),
            installation: INSTALL_B.into(),
            connections: 4,
            configured_connections: 4,
            reserved_connections: 1,
        },
        f,
    );
    golden_eq(
        "orchestrator.remote_catalog",
        &RemoteCatalog {
            generation: 42,
            at_ms: T0,
            entries: vec![published_entry()],
        },
        f,
    );
    golden_eq(
        "orchestrator.daemon_endpoints",
        &DaemonEndpoints {
            control: "ws://127.0.0.1:43181".into(),
            network: Some("ws://0.0.0.0:43180".into()),
            log_path: None,
        },
        f,
    );

    // session
    golden_eq(
        "session.peer_info",
        &PeerInfo {
            installation: INSTALL_A.into(),
            role: LaneRole::Control,
            lane: None,
            session: "2b3c4d5e-6f70-4812-9a3b-4c5d6e7f8091".into(),
            verifier_generation: 5,
        },
        f,
    );
    golden_eq(
        "session.conn_health",
        &ConnHealth {
            since_ms: T0,
            rtt_ms: Some(42),
            last_rx_ms: T0 + 900,
            last_tx_ms: T0 + 950,
            bytes_up: 1000,
            bytes_down: 2000,
            inflight: 3,
            queued_bytes: 65_536,
        },
        f,
    );
    golden_eq("session.side", &vec![Side::Client, Side::Server], f);

    assert!(
        f.is_empty(),
        "{} golden fixture failure(s):\n{}",
        f.len(),
        f.join("\n")
    );
}

#[test]
fn every_fixture_file_is_covered() {
    // Fixture names referenced by the golden test, recovered from its source so
    // a stale fixture (left behind by a renamed type) fails loudly.
    let source = include_str!("contracts.rs");
    let referenced: BTreeSet<String> = source
        .split("golden")
        .filter_map(|rest| {
            let rest = rest
                .strip_prefix("_eq(")
                .or_else(|| rest.strip_prefix("("))?;
            let rest = rest.trim_start().strip_prefix('"')?;
            Some(rest[..rest.find('"')?].to_owned())
        })
        .collect();
    let on_disk: BTreeSet<String> = std::fs::read_dir(fixtures_dir())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter_map(|name| name.strip_suffix(".json").map(str::to_owned))
        .collect();
    let stale: Vec<_> = on_disk.difference(&referenced).collect();
    assert!(
        stale.is_empty(),
        "fixtures without a golden check: {stale:?}"
    );
    assert!(referenced.len() > 100, "{}", referenced.len());
}

#[test]
fn spec_examples_parse_into_the_frozen_types() {
    // §6.1 stdout lines.
    let progress =
        ipc::parse_line(r#"{"startup_progress": 12, "phase": "verifying available objects"}"#);
    assert_eq!(
        progress,
        Some(StartupLine::Progress(ProgressLine {
            startup_progress: 12,
            phase: "verifying available objects".into()
        }))
    );
    let ready = r#"{"ready": true, "repo": "r", "device": "d", "runtime": "STATE/runtime.json", "mounted": false,
 "mount_error": {"code":"MOUNT_PATH_MUST_NOT_EXIST","class":"needs_user_action","message":"..."},
 "instance": "uuid", "pid": 1234, "address": "ws://127.0.0.1:5555", "version": "0.1.0+6298d0b", "ipc": 3}"#;
    let Some(StartupLine::Ready(ready)) = ipc::parse_line(&ready.replace('\n', "")) else {
        panic!("ready line did not parse");
    };
    assert_eq!(ready.ipc, IPC_VERSION);
    assert_eq!(ready.pid, 1234);
    assert!(!ready.mounted);
    assert_eq!(ready.token, None);
    let error = ready.mount_error.unwrap();
    assert_eq!(error.class, ErrorClass::NeedsUserAction);
    assert_eq!(error.class, classify(&error.code));
    let fatal = ipc::parse_line(
        r#"{"fatal": true, "code": "STORE_IDENTITY_MISMATCH", "class": "fatal", "message": "...", "exit_code": 73}"#,
    );
    let Some(StartupLine::Fatal(fatal)) = fatal else {
        panic!("fatal line did not parse");
    };
    assert_eq!(
        (fatal.exit_code, fatal.class),
        (EXIT_STORE, ErrorClass::Fatal)
    );

    // §6.1 worker.json.
    let record: WorkerRecord = serde_json::from_str(
        r#"{"format":1,"pid":1234,"instance":"uuid","address":"ws://127.0.0.1:5555","version":"0.1.0+6298d0b",
 "ipc":3,"repo":"...","device":"...","started_at_ms":1790000000000,
 "process_start":133780000000000000,"executable":"C:\\...\\tkfs.exe","supervised":true}"#,
    )
    .unwrap();
    assert_eq!(record.format, WORKER_RECORD_FORMAT);
    assert_eq!(record.process_start, 133_780_000_000_000_000);
    assert_eq!(record.token, None);

    // A legacy (6298d0b) ready line still parses, with ipc 0 → WORKER_IPC_MISMATCH.
    let legacy = ipc::parse_line(
        r#"{"ready":true,"runtime":"C:\\s\\runtime.json","repo":"r","device":"d","mounted":true}"#,
    );
    let Some(StartupLine::Ready(legacy)) = legacy else {
        panic!("legacy ready line did not parse");
    };
    assert_eq!(legacy.ipc, 0);
    assert_eq!(
        classify("WORKER_IPC_MISMATCH"),
        ErrorClass::Incompatible,
        "the controller rejects ipc != 3 as Incompatible"
    );
    // Noise lines are not startup lines.
    for noise in [
        "",
        "hello",
        "[1,2]",
        r#"{"ready":false}"#,
        r#"{"fatal":false}"#,
    ] {
        assert_eq!(ipc::parse_line(noise), None, "{noise}");
    }

    // ReadyLine keeps the legacy keys.
    let value = serde_json::to_value(ready_line()).unwrap();
    for key in ["ready", "repo", "device", "runtime", "mounted"] {
        assert!(value.get(key).is_some(), "{key}");
    }

    // `mount.state` reads as a plain string, e.g. "attach_failed".
    for status in mount_variants() {
        let value = serde_json::to_value(&status).unwrap();
        assert_eq!(value["state"], status.state.name());
    }

    // Defaults: absent optional fields deserialize.
    let stop: StopRequest = serde_json::from_str("{}").unwrap();
    assert_eq!(stop.mode, StopMode::Graceful);
    let watch: WatchRequest = serde_json::from_str(r#"{"after_seq":3,"wait_ms":99999}"#).unwrap();
    assert_eq!(watch.wait(), Duration::from_millis(5000));
    let heartbeat: WorkerHeartbeat =
        serde_json::from_value(serde_json::to_value(heartbeat_basic()).unwrap()).unwrap();
    assert_eq!(heartbeat, heartbeat_basic());
}

#[test]
fn pairing_secrets_are_never_serialized_or_debug_printed() {
    let record = peer_record();
    let json = serde_json::to_string(&record).unwrap();
    assert!(!json.contains("SECRET"), "{json}");
    assert!(!format!("{record:?}").contains("SECRET"));
    let back: PeerRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(back.code, "");
    assert_eq!(back.installation, record.installation);

    let (_code, verifier) = Verifier::generate();
    let snapshot = CatalogSnapshot {
        peers: vec![record.clone()],
        verifier: Some(verifier.clone()),
        ..CatalogSnapshot::default()
    };
    let debug = format!("{snapshot:?}");
    assert!(
        !debug.contains(&verifier.hash) && !debug.contains("SECRET"),
        "{debug}"
    );
    let debug = format!(
        "{:?} {:?}",
        CatalogCmd::SetVerifier {
            verifier: verifier.clone()
        },
        CatalogCmd::UpsertPeer { record }
    );
    assert!(
        !debug.contains(&verifier.hash) && !debug.contains("SECRET"),
        "{debug}"
    );
    let auth = session::CodeAuth {
        code: "SECRET".into(),
    };
    assert!(!format!("{auth:?}").contains("SECRET"));
}

// ---------------------------------------------------------------------------
// §8 classification

#[test]
fn classify_matches_the_section_8_table() {
    let rows: [(&[&str], ErrorClass); 5] = [
        (errors::CONTENTION_CODES, ErrorClass::Contention),
        (errors::TRANSIENT_CODES, ErrorClass::Transient),
        (errors::NEEDS_USER_ACTION_CODES, ErrorClass::NeedsUserAction),
        (errors::INCOMPATIBLE_CODES, ErrorClass::Incompatible),
        (errors::FATAL_CODES, ErrorClass::Fatal),
    ];
    let mut seen = BTreeSet::new();
    for (codes, class) in rows {
        for code in codes {
            assert!(seen.insert(*code), "{code} appears twice in the table");
            assert_eq!(classify(code), class, "{code}");
            assert_eq!(errors::explicit_class(code), Some(class), "{code}");
        }
    }
    assert_eq!(
        (
            errors::CONTENTION_CODES.len(),
            errors::TRANSIENT_CODES.len(),
            errors::NEEDS_USER_ACTION_CODES.len(),
            errors::INCOMPATIBLE_CODES.len(),
            errors::FATAL_CODES.len()
        ),
        (4, 23, 17, 7, 39),
        "row sizes of the §8 table"
    );
    for code in errors::TRANSIENT_DEFAULT_CODES {
        assert!(
            !seen.contains(code),
            "{code} is both classified and default"
        );
        assert_eq!(classify(code), ErrorClass::Transient, "{code}");
        assert_eq!(errors::explicit_class(code), None, "{code}");
    }
    let defaults: BTreeSet<_> = errors::TRANSIENT_DEFAULT_CODES.iter().collect();
    assert_eq!(defaults.len(), errors::TRANSIENT_DEFAULT_CODES.len());
    assert_eq!(classify("SOMETHING_NEW_AND_UNKNOWN"), ErrorClass::Transient);
    // Spot checks straight from §8.
    assert_eq!(classify("WORKER_BUSY"), ErrorClass::Contention);
    assert_eq!(classify("FOLDER_SYNC_PAUSED"), ErrorClass::Transient);
    assert_eq!(classify("ALREADY_RUNNING"), ErrorClass::NeedsUserAction);
    assert_eq!(classify("PEER_NEEDS_UPGRADE"), ErrorClass::Incompatible);
    assert_eq!(classify("CONFLICT_MODE_MISMATCH"), ErrorClass::Fatal);
    assert!(!ErrorClass::Contention.counts_as_failure());
    assert!(ErrorClass::Transient.counts_as_failure());
    assert!(ErrorClass::Contention.retries_automatically());
    assert!(!ErrorClass::Fatal.retries_automatically());
    assert_eq!(ErrorClass::NeedsUserAction.to_string(), "needs_user_action");
}

/// Literal fragments matched by `"[A-Z][A-Z_]{3,}` that are not error codes:
/// SQL keywords, an OS environment variable, and substrings that error mappers
/// look for inside messages. `TKFS_*` environment variable names are skipped
/// by prefix.
const NOT_CODES: &[&str] = &[
    "ALTER",
    "BEGIN",
    "COMMIT",
    "CREATE",
    "DELETE",
    "DROP",
    "INSERT",
    "PRAGMA",
    "RELEASE",
    "REPLACE",
    "ROLLBACK",
    "SAVEPOINT",
    "SELECT",
    "UPDATE",
    "UNIQUE",
    "VALUES",
    "WITH",
    "COMPUTERNAME",
    "DUPLICATE",
    "INVALID",
    "PERMISSION",
    "UNSUPPORTED",
];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Every `"[A-Z][A-Z_]{3,}` literal in `src/`, extended over trailing
/// `[A-Z0-9_]` (so `CONFIG_VERSION_2_REQUIRED` is one code).
fn code_literals() -> BTreeMap<String, String> {
    let mut files = Vec::new();
    rust_files(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let mut found = BTreeMap::new();
    for file in files {
        let text = std::fs::read_to_string(&file).unwrap();
        let bytes = text.as_bytes();
        for (index, _) in text.match_indices('"') {
            let start = index + 1;
            let mut end = start;
            while end < bytes.len()
                && (bytes[end].is_ascii_uppercase()
                    || bytes[end].is_ascii_digit()
                    || bytes[end] == b'_')
            {
                end += 1;
            }
            let token = &text[start..end];
            let letters = token
                .bytes()
                .take_while(|b| b.is_ascii_uppercase() || *b == b'_')
                .count();
            if token
                .as_bytes()
                .first()
                .is_some_and(|b| b.is_ascii_uppercase())
                && letters >= 4
            {
                found
                    .entry(token.to_owned())
                    .or_insert_with(|| file.display().to_string());
            }
        }
    }
    found
}

#[test]
fn classify_covers_every_code_literal_in_src() {
    let literals = code_literals();
    assert!(literals.len() > 200, "grep found only {}", literals.len());
    let unknown: Vec<_> = literals
        .iter()
        .filter(|(code, _)| {
            !code.starts_with("TKFS_")
                && !NOT_CODES.contains(&code.as_str())
                && !errors::is_known(code)
        })
        .map(|(code, file)| format!("{code} ({file})"))
        .collect();
    assert!(
        unknown.is_empty(),
        "code literals that are neither in the §8 table nor in TRANSIENT_DEFAULT_CODES: {unknown:#?}"
    );
}

#[test]
fn code_of_finds_the_first_code_in_the_chain() {
    let error = anyhow::anyhow!("WS_CONNECT_FAILED: connection refused (os error 10061)");
    assert_eq!(code_of(&error), "WS_CONNECT_FAILED");
    let error = anyhow::anyhow!("WORKER_BUSY").context("LOCAL_SYNC_OFFER_FAILED");
    assert_eq!(code_of(&error), "LOCAL_SYNC_OFFER_FAILED");
    let error =
        anyhow::anyhow!("plain failure with BUSY_VIEW: 2 handles").context("could not stop");
    assert_eq!(
        code_of(&error),
        "BUSY_VIEW",
        "context without a code is skipped"
    );
    let error = anyhow::anyhow!("HTTP error 500, UUID invalid");
    assert_eq!(code_of(&error), errors::UNKNOWN_CODE, "words are not codes");
    let error = anyhow::anyhow!("remote said BUSY");
    assert_eq!(code_of(&error), "BUSY", "known single-word codes count");
    let error = anyhow::Error::new(ErrorWire::new("PEER_REVOKED", "revoked by peer")).context("x");
    assert_eq!(code_of(&error), "PEER_REVOKED");
    let error = anyhow::Error::new(CallError::Timeout { after_ms: 30 });
    assert_eq!(code_of(&error), "REQUEST_TIMEOUT");
    let busy = rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
        Some("database is locked".into()),
    );
    let error = anyhow::Error::new(busy).context("while staging");
    assert_eq!(code_of(&error), "SQLITE_BUSY");
    assert_eq!(
        code_in("x CONFIG_VERSION_2_REQUIRED y"),
        Some("CONFIG_VERSION_2_REQUIRED")
    );
    assert_eq!(code_in("lowercase_only and Mixed_Case"), None);

    let wire = ErrorWire::from_error(&anyhow::anyhow!("OBJECT_SIZE_CHANGED: {HASH_A}"));
    assert_eq!(
        (wire.code.as_str(), wire.class),
        ("OBJECT_SIZE_CHANGED", ErrorClass::Fatal)
    );
    assert_eq!(wire.to_string(), format!("OBJECT_SIZE_CHANGED: {HASH_A}"));
    assert_eq!(
        ErrorWire::new("BUSY", "queue full").to_string(),
        "BUSY: queue full"
    );
    assert_eq!(
        ErrorWire::new("BUSY", "BUSY_VIEW: 2 handles").to_string(),
        "BUSY: BUSY_VIEW: 2 handles",
        "a longer code is not the same code"
    );
    assert_eq!(ErrorWire::new("BUSY", "").to_string(), "BUSY");
    assert_eq!(
        CallError::Unavailable("connect failed: WS_CONNECT_FAILED".into()).to_string(),
        "WS_CONNECT_FAILED: connect failed: WS_CONNECT_FAILED"
    );
    assert_eq!(code_of(&wire.clone().into_anyhow()), "OBJECT_SIZE_CHANGED");
}

#[test]
fn error_records_count_and_resolve() {
    let scope = Scope::Mount {
        state: STATE.into(),
    };
    let mut record = ErrorRecord::new(transient_error(), scope.clone(), T0);
    record.observe(transient_error(), T0 + 10);
    record.observe(transient_error(), T0 + 20);
    assert_eq!(
        (record.count, record.first_at_ms, record.last_at_ms),
        (3, T0, T0 + 20)
    );
    record.resolve(T0 + 30);
    assert!(record.is_resolved());
    record.observe(transient_error(), T0 + 40);
    assert_eq!((record.count, record.first_at_ms), (1, T0 + 40), "reopened");
    record.observe(error_wire(), T0 + 50);
    assert_eq!(record.error.code, "MOUNT_PATH_MUST_NOT_EXIST");
    assert_eq!((record.count, record.scope.clone()), (1, scope));
    assert_eq!(record.class(), ErrorClass::NeedsUserAction);
}

#[test]
fn call_errors_are_classified_and_uncertainty_is_explicit() {
    let cases = [
        (
            CallError::Remote(ErrorWire::new("BUSY_VIEW", "BUSY_VIEW: open")),
            "BUSY_VIEW",
            ErrorClass::Contention,
            false,
        ),
        (
            CallError::Timeout { after_ms: 4000 },
            "REQUEST_TIMEOUT",
            ErrorClass::Transient,
            true,
        ),
        (
            CallError::ConnectionLost("reset".into()),
            "CONNECTION_LOST",
            ErrorClass::Transient,
            true,
        ),
        (
            CallError::Unavailable("refused".into()),
            "LOCAL_WORKER_UNAVAILABLE",
            ErrorClass::Transient,
            false,
        ),
        (
            CallError::Unavailable("BINARY_OPS_REQUIRE_SESSION".into()),
            "BINARY_OPS_REQUIRE_SESSION",
            ErrorClass::Transient,
            false,
        ),
        (CallError::Busy, "BUSY", ErrorClass::Contention, false),
        (
            CallError::Cancelled,
            "CANCELLED",
            ErrorClass::Transient,
            false,
        ),
    ];
    for (error, code, class, uncertain) in cases {
        assert_eq!(error.code(), code, "{error:?}");
        assert_eq!(error.class(), class, "{error:?}");
        assert_eq!(error.is_uncertain(), uncertain, "{error:?}");
        let wire = error.to_wire();
        assert_eq!((wire.code.as_str(), wire.class), (code, class));
        assert!(error.to_string().starts_with(code), "{error}");
    }
}

// ---------------------------------------------------------------------------
// Model helpers

#[test]
fn model_helpers_follow_the_spec() {
    let counts = inbox();
    assert_eq!(counts.pending_active(), 12 + 3 + 1);
    assert_eq!(counts.total(), 12 + 3 + 1 + 2 + 1);
    assert!(GateState::None.allows_attach());
    assert!(GateState::Overridden { at_ms: 1 }.allows_attach());
    assert!(!folder_view().gate.allows_attach());
    assert_eq!(folder_view().gate.kind(), GateKind::Awaiting);
    assert_eq!(folder_view().phase.lifecycle(), LifecyclePhase::Running);
    assert_eq!(
        stall_after(Duration::from_millis(500)),
        Duration::from_secs(60)
    );
    assert_eq!(
        stall_after(Duration::from_secs(10)),
        Duration::from_secs(200)
    );
    assert_eq!(
        ObjectOutcome::Unsolicited.code(),
        Some("UNSOLICITED_OBJECT")
    );
    assert_eq!(ObjectOutcome::Completed.code(), None);
    assert!(WantPriority::Demand < WantPriority::Current);
    assert!(WantPriority::Current < WantPriority::History);
    assert!(Cursor::start("e").is_start());
    assert_eq!(protocol::max_body(protocol::ops::META_EXCHANGE), 16 << 20);
    assert_eq!(protocol::max_body(protocol::ops::PING), 1 << 20);
    assert_eq!(
        protocol::op_deadline(protocol::ops::META_EXCHANGE),
        Some(Duration::from_secs(30))
    );
    assert_eq!(protocol::op_deadline(protocol::ops::OBJECTS_PULL), None);
    assert_eq!(
        protocol::op_role(protocol::ops::OBJECTS_PUSH),
        Some(LaneRole::Bulk)
    );
    assert_eq!(protocol::op_role(protocol::ops::PING), None);
    for kind in FrameKind::ALL {
        assert_eq!(FrameKind::from_u8(kind.as_u8()), Some(kind));
    }
    assert_eq!(FrameKind::from_u8(0), None);
    assert_eq!(FrameKind::from_u8(12), None);
    assert_eq!(FrameKind::Goaway.as_u8(), 11);
    assert!(FrameKind::Heartbeat.body_forbidden() && !FrameKind::Chunk.body_forbidden());
    let options = ConnOptions::default();
    assert_eq!(
        (
            options.heartbeat,
            options.dead_after,
            options.max_header,
            options.max_chunk,
            options.max_body,
            options.control_queue_bytes,
            options.data_queue_bytes
        ),
        (
            Duration::from_secs(5),
            Duration::from_secs(20),
            64 << 10,
            1 << 20,
            16 << 20,
            16 << 20,
            32 << 20
        )
    );
    assert_eq!(ConnOptions::for_role(LaneRole::Bulk).handlers, 2);
    assert_eq!(ConnOptions::for_role(LaneRole::Control).handlers, 4);
}

#[test]
fn feed_digest_is_the_first_16_hex_of_sha256() {
    use sha2::{Digest, Sha256};
    let payload = serde_json::to_string(&event()).unwrap();
    let full = hex::encode(Sha256::digest(payload.as_bytes()));
    assert_eq!(digest(&payload), full[..16]);
    assert_eq!(digest(""), "e3b0c44298fc1c14");
}

#[test]
fn lane_spec_owns_exactly_like_the_legacy_bulk_lane() {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for _ in 0..2000 {
        let hash = hex::encode(rng.r#gen::<[u8; 32]>());
        for count in 1..=15u32 {
            let owners: Vec<_> = (0..count)
                .filter(|index| LaneSpec::new(*index, count).owns(&hash))
                .collect();
            assert_eq!(owners.len(), 1, "exactly one owner");
            for index in 0..count {
                let legacy = tkfs::bulk_sync::Lane {
                    index: index as usize,
                    count: count as usize,
                };
                assert_eq!(LaneSpec::new(index, count).owns(&hash), legacy.owns(&hash));
                assert_eq!(
                    LaneSpec::new(index, count).is_valid(),
                    legacy.validate().is_ok()
                );
            }
        }
    }
    assert!(!LaneSpec::new(0, 1).owns("not-hex"));
    assert!(!LaneSpec::new(0, 0).owns(HASH_A));
    assert!(!LaneSpec::new(0, 16).is_valid());
}

#[test]
fn upgrade_kind_reads_the_sync_header() {
    use session::UpgradeKind;
    let request = |value: Option<&str>| {
        let mut builder = tungstenite::http::Request::builder().uri("ws://127.0.0.1/");
        if let Some(value) = value {
            builder = builder.header(protocol::UPGRADE_HEADER, value);
        }
        builder.body(()).unwrap()
    };
    assert_eq!(session::upgrade_kind(&request(Some("3"))), UpgradeKind::V3);
    assert_eq!(
        session::upgrade_kind(&request(Some("3-loopback"))),
        UpgradeKind::Loopback
    );
    assert_eq!(session::upgrade_kind(&request(None)), UpgradeKind::Legacy);
    assert_eq!(
        session::upgrade_kind(&request(Some("2"))),
        UpgradeKind::Legacy
    );
}

#[test]
fn session_stubs_fail_closed_with_not_implemented() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let client = std::net::TcpStream::connect(address).unwrap();
    let (server, _) = listener.accept().unwrap();

    struct Policy;
    impl session::AcceptPolicy for Policy {
        fn installation(&self) -> String {
            INSTALL_A.into()
        }
        fn verifier(&self) -> Option<(Verifier, u64)> {
            None
        }
        fn admit(&self, _: &ClientHello) -> Result<(), ErrorWire> {
            Ok(())
        }
        fn legacy_attempt(&self, _: std::net::SocketAddr, _: &Value) {}
    }
    let error = session::accept(server, Arc::new(Policy), ConnOptions::default()).unwrap_err();
    assert_eq!(error.code, "NOT_IMPLEMENTED");
    let error = session::accept_loopback(client, None, ConnOptions::default()).unwrap_err();
    assert_eq!(error.code, "NOT_IMPLEMENTED");
    let hello = ClientHello {
        protocol: protocol::PROTOCOL_NAME.into(),
        versions: vec![3],
        installation: INSTALL_A.into(),
        role: LaneRole::Probe,
        lane: None,
        pool: 2,
        session: "s".into(),
        client_nonce: "00".repeat(32),
        features: vec![],
    };
    let auth = session::CodeAuth { code: "c".into() };
    let error = session::dial(
        "ws://127.0.0.1:1",
        hello,
        &auth,
        None,
        ConnOptions::default(),
    )
    .unwrap_err();
    assert_eq!(error.to_wire().code, "NOT_IMPLEMENTED");
    let error =
        session::dial_loopback("ws://127.0.0.1:1", None, ConnOptions::default()).unwrap_err();
    assert_eq!(error.to_wire().code, "NOT_IMPLEMENTED");
    use session::ClientAuth;
    assert_eq!(auth.key("salt").unwrap_err().code, "NOT_IMPLEMENTED");
    let frame =
        session::frame::Frame::new(FrameKind::Cancel, 0, &CancelHeader { id: 1 }, vec![]).unwrap();
    assert_eq!(frame.header_as::<CancelHeader>().unwrap().id, 1);
    // WP1.9 (CCR-WP1.9-1): the frame codec is real now. It round-trips, and it
    // still fails closed on malformed input.
    let bytes = session::frame::encode(&frame, session::frame::FrameLimits::SESSION).unwrap();
    assert_eq!(
        session::frame::decode(&bytes, session::frame::FrameLimits::SESSION).unwrap(),
        frame
    );
    assert!(session::frame::decode(&[], session::frame::FrameLimits::PRE_AUTH).is_err());
    let dial_errors = [
        (session::DialError::Connect("x".into()), "WS_CONNECT_FAILED"),
        (
            session::DialError::NeedsUpgrade {
                detail: "no echo".into(),
            },
            "PEER_NEEDS_UPGRADE",
        ),
        (
            session::DialError::Unsupported {
                offered: vec![3],
                supported: vec![4],
            },
            "PEER_PROTOCOL_UNSUPPORTED",
        ),
        (
            session::DialError::Unauthorized(ErrorWire::new("INVALID_PAIRING_CODE", "")),
            "INVALID_PAIRING_CODE",
        ),
        (session::DialError::Timeout, "REQUEST_TIMEOUT"),
        (
            session::DialError::Protocol("bad".into()),
            "PROTOCOL_VIOLATION",
        ),
    ];
    for (error, code) in dial_errors {
        assert_eq!(error.to_wire().code, code);
    }
}

// ---------------------------------------------------------------------------
// Orchestrator seams are object-safe and Ctx guards the network.

struct Fakes;
impl CatalogApi for Fakes {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        Arc::new(CatalogSnapshot::default())
    }
    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        Err(ErrorWire::new("CATALOG_UNAVAILABLE", cmd.name()))
    }
    fn wait_newer(&self, _: u64, _: Duration) -> Arc<CatalogSnapshot> {
        CatalogApi::snapshot(self)
    }
}
impl FolderDirectory for Fakes {
    fn view(&self, _: &str) -> Option<Arc<FolderView>> {
        Some(Arc::new(folder_view()))
    }
    fn by_repo(&self, _: &str) -> Option<Arc<FolderView>> {
        None
    }
    fn all(&self) -> Vec<Arc<FolderView>> {
        vec![]
    }
    fn send(&self, _: &str, _: FolderCmd) -> Result<(), ErrorWire> {
        Ok(())
    }
    fn request(&self, s: &str, _: FolderCmd, _: Duration) -> Result<Arc<FolderView>, ErrorWire> {
        self.view(s)
            .ok_or_else(|| ErrorWire::new("FOLDER_NOT_FOUND", s))
    }
    fn client(&self, _: &str) -> Option<Arc<dyn WorkerClient>> {
        Some(Arc::new(Fakes))
    }
    fn generation(&self) -> u64 {
        0
    }
    fn wait_newer(&self, generation: u64, _: Duration) -> u64 {
        generation
    }
    fn shutdown(&self, _: Duration) -> Vec<ShutdownOutcome> {
        vec![]
    }
}
impl PublishedCatalogApi for Fakes {
    fn update(&self, _: &str, _: Option<PublishedEntry>) {}
    fn list(&self) -> (u64, Arc<Vec<PublishedEntry>>) {
        (0, Arc::new(vec![]))
    }
    fn wait_newer(&self, generation: u64, _: Duration) -> u64 {
        generation
    }
}
impl StatusSink for Fakes {
    fn record(&self, _: StatusEvent) {}
}
impl StatusSource for Fakes {
    fn snapshot(&self) -> Arc<StatusSnapshot> {
        Arc::new(StatusSnapshot::default())
    }
    fn wait_newer(&self, _: u64, _: Duration) -> Arc<StatusSnapshot> {
        StatusSource::snapshot(self)
    }
}
impl WorkerClient for Fakes {
    fn instance(&self) -> &str {
        "fake"
    }
    fn call(&self, op: &str, _: Value, _: Duration) -> Result<Value, CallError> {
        Err(CallError::Remote(ErrorWire::new("UNKNOWN_OPERATION", op)))
    }
    fn call_bytes(
        &self,
        _: &str,
        _: Value,
        _: &[u8],
        _: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        Err(CallError::Unavailable("BINARY_OPS_REQUIRE_SESSION".into()))
    }
}
impl WorkerProcess for Fakes {
    fn pid(&self) -> u32 {
        1
    }
    fn kill(&self) -> Result<(), ErrorWire> {
        Ok(())
    }
    fn adopted(&self) -> bool {
        false
    }
}
impl WorkerLauncher for Fakes {
    fn launch(
        &self,
        _: &LaunchSpec,
        events: ProcessSink,
    ) -> Result<Box<dyn WorkerProcess>, ErrorWire> {
        events(ProcessEvent::Line(StartupLine::Ready(ready_line())));
        Ok(Box::new(Fakes))
    }
    fn adopt(
        &self,
        _: &WorkerRecord,
        _: ProcessSink,
    ) -> Result<Option<Box<dyn WorkerProcess>>, ErrorWire> {
        Ok(None)
    }
    fn client(&self, _: &WorkerEndpoint) -> Arc<dyn WorkerClient> {
        Arc::new(Fakes)
    }
}
impl NetworkApi for Fakes {
    fn pair(&self, endpoint: &str, _: &str, _: Option<u32>) -> Result<PairResult, ErrorWire> {
        Ok(PairResult {
            endpoint: endpoint.into(),
            ..PairResult::default()
        })
    }
    fn remote_list(&self, _: &str) -> Result<Vec<PublishedEntry>, ErrorWire> {
        Ok(vec![])
    }
    fn unpair(&self, _: &str) -> Result<(), ErrorWire> {
        Ok(())
    }
    fn reconfigure(&self) {}
    fn shutdown(&self, _: Duration) {}
}
impl LegacyNetworkView for Fakes {
    fn folder_sync(&self, _: &FolderRecord) -> Value {
        json!([])
    }
    fn peers(&self) -> Value {
        json!([])
    }
}
struct FakeMount;
impl MountControl for FakeMount {
    fn attach(&self, path: &Path) -> Result<MountStatus, ErrorWire> {
        Ok(MountStatus {
            path: Some(path.to_owned()),
            ..mount_attached()
        })
    }
    fn detach(&self, _: bool) -> Result<MountStatus, ErrorWire> {
        Ok(MountStatus::default())
    }
    fn status(&self) -> MountStatus {
        MountStatus::default()
    }
}

#[test]
fn ctx_reports_network_not_ready_until_set() {
    let fakes = Arc::new(Fakes);
    let config = tkfs::orchestrator::Config {
        format_version: 2,
        data_directory: PathBuf::from("data"),
        default_mount_directory: PathBuf::from("mounts"),
        maximum_running: 8,
        network: tkfs::orchestrator::Network::default(),
    };
    let ctx = Ctx {
        installation: INSTALL_A.into(),
        config: Arc::new(config),
        catalog: fakes.clone(),
        folders: fakes.clone(),
        published: fakes.clone(),
        status: fakes.clone(),
        status_source: fakes.clone(),
        network: OnceLock::new(),
        legacy: OnceLock::new(),
        clock: Arc::new(ManualClock::new(T0)),
        shutdown: Cancel::new(),
        stopping: Arc::new(AtomicBool::new(false)),
        started_at_ms: T0,
        endpoints: Snapshot::new(DaemonEndpoints::default()),
    };
    assert_eq!(ctx.network().err().unwrap().code, "NETWORK_NOT_READY");
    assert_eq!(ctx.legacy().err().unwrap().code, "NETWORK_NOT_READY");
    let _ = ctx.network.set(fakes.clone());
    let _ = ctx.legacy.set(fakes.clone());
    assert_eq!(
        ctx.network()
            .unwrap()
            .pair("ws://x", "c", None)
            .unwrap()
            .endpoint,
        "ws://x"
    );
    assert_eq!(ctx.legacy().unwrap().peers(), json!([]));
    assert_eq!(ctx.clock.now_ms(), T0);
    assert_eq!(
        ctx.catalog
            .apply(CatalogCmd::ClearGate {
                state: STATE.into()
            })
            .unwrap_err()
            .message,
        "clear_gate"
    );
    let client = ctx.folders.client(STATE).unwrap();
    assert!(
        !client
            .call("x", json!({}), Duration::ZERO)
            .unwrap_err()
            .is_uncertain()
    );
    let seen = Arc::new(AtomicUsize::new(0));
    let sink_seen = seen.clone();
    let sink: ProcessSink = Arc::new(move |_| {
        sink_seen.fetch_add(1, Ordering::SeqCst);
    });
    let process = Fakes.launch(&LaunchSpec::default(), sink).unwrap();
    assert_eq!((process.pid(), seen.load(Ordering::SeqCst)), (1, 1));
    let mount: Arc<dyn MountControl> = Arc::new(FakeMount);
    assert!(mount.attach(Path::new("M:\\")).unwrap().state.is_attached());
    assert!(format!("{ctx:?}").contains("network_ready: true"));
    let peer = PeerContext {
        installation: INSTALL_B.into(),
        endpoint: "ws://x".into(),
        local_installation: INSTALL_A.into(),
        pool: 4,
        catalog: Snapshot::new(RemoteCatalog::default()),
    };
    assert_eq!(peer.catalog.generation(), 0);
}

#[test]
fn shared_handles_are_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>() {}
    send_sync::<Ctx>();
    send_sync::<Arc<CatalogSnapshot>>();
    send_sync::<FolderView>();
    send_sync::<StatusEvent>();
    send_sync::<CallError>();
    send_sync::<ErrorWire>();
    send_sync::<PeerContext>();
    send_sync::<session::Conn>();
    send_sync::<PeerInfo>();
    send::<session::Incoming>();
    send::<session::Responder>();
    send::<session::Upload>();
    send_sync::<Snapshot<WorkerHeartbeat>>();
    send_sync::<Notify>();
    send_sync::<Cancel>();
    send_sync::<Tracked<Vec<u8>>>();
    send_sync::<ManualClock>();
    send::<Backoff>();
    send::<Box<dyn WorkerProcess>>();
}

// ---------------------------------------------------------------------------
// util

#[test]
fn hmac_matches_rfc_4231_cases_1_to_7() {
    let cases: [(Vec<u8>, Vec<u8>, &str); 7] = [
        (
            vec![0x0b; 20],
            b"Hi There".to_vec(),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
        ),
        (
            b"Jefe".to_vec(),
            b"what do ya want for nothing?".to_vec(),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
        ),
        (
            vec![0xaa; 20],
            vec![0xdd; 50],
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe",
        ),
        (
            (1..=25).collect(),
            vec![0xcd; 50],
            "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b",
        ),
        (
            vec![0x0c; 20],
            b"Test With Truncation".to_vec(),
            // RFC 4231 truncates this case to 128 bits.
            "a3b6167473100ee06e0c796c2955552b",
        ),
        (
            vec![0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First".to_vec(),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54",
        ),
        (
            vec![0xaa; 131],
            b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm.".to_vec(),
            "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2",
        ),
    ];
    for (n, (key, data, expected)) in cases.iter().enumerate() {
        let mac = hex::encode(hmac_sha256(key, data));
        assert_eq!(&mac[..expected.len()], *expected, "RFC 4231 case {}", n + 1);
        let (left, right) = data.split_at(data.len() / 2);
        assert_eq!(
            hmac_sha256_parts(key, &[left, right, b""]),
            hmac_sha256(key, data),
            "case {}",
            n + 1
        );
    }
    assert!(ct_eq(b"abc", b"abc"));
    assert!(!ct_eq(b"abc", b"abd"));
    assert!(!ct_eq(b"abc", b"abcd"));
    assert!(ct_eq(b"", b""));
}

#[test]
fn backoff_stays_within_bounds_over_10000_samples() {
    let now = Instant::now();
    let mut samples = 0;
    let presets = [
        Backoff::worker_restart(),
        Backoff::lane(),
        Backoff::transient(),
        Backoff::contention(),
        Backoff::mount_retry(),
        Backoff::inbox(),
        Backoff::objects(),
        Backoff::new(
            Duration::from_millis(10),
            Duration::from_secs(1),
            0.5,
            Duration::ZERO,
        ),
        Backoff::new(
            Duration::from_secs(1),
            Duration::from_secs(8),
            0.0,
            Duration::ZERO,
        ),
        Backoff::new(
            Duration::from_secs(u64::MAX / 4),
            Duration::MAX,
            0.2,
            Duration::ZERO,
        ),
    ];
    while samples < 10_000 {
        for preset in &presets {
            let mut backoff = preset.clone();
            for attempt in 1..=40u32 {
                let delay = backoff.failure(now);
                samples += 1;
                assert_eq!(backoff.attempt(), attempt);
                let nominal = backoff.nominal(attempt);
                let expected = preset
                    .base
                    .checked_mul(2u32.saturating_pow(attempt.saturating_sub(1).min(31)))
                    .unwrap_or(preset.cap)
                    .min(preset.cap);
                if attempt <= 32 {
                    assert_eq!(nominal, expected, "attempt {attempt}");
                }
                let (low, high) = backoff.bounds(attempt);
                assert!(
                    low <= delay && delay <= high && delay <= preset.cap,
                    "attempt {attempt}: {delay:?} outside [{low:?}, {high:?}]"
                );
                let (jitter, nominal) = (preset.jitter, nominal.as_secs_f64());
                let slack = 1e-9 * nominal + 1e-6;
                assert!(low.as_secs_f64() >= nominal * (1.0 - jitter) - slack);
                assert!(high.as_secs_f64() <= nominal * (1.0 + jitter) + slack);
            }
        }
    }
    // The §3.3 worker schedule: 1 s · 2^(n-1), cap 300 s, ±20 %.
    let schedule = Backoff::worker_restart();
    assert_eq!(schedule.nominal(1), Duration::from_secs(1));
    assert_eq!(schedule.nominal(4), Duration::from_secs(8));
    assert_eq!(schedule.nominal(9), Duration::from_secs(256));
    assert_eq!(schedule.nominal(10), Duration::from_secs(300));
    assert_eq!(
        schedule.bounds(3),
        (Duration::from_millis(3200), Duration::from_millis(4800))
    );
    assert_eq!(schedule.bounds(20).1, Duration::from_secs(300));
}

#[test]
fn backoff_resets_only_after_a_healthy_period() {
    let start = Instant::now();
    let mut lane = Backoff::lane();
    lane.failure(start);
    lane.failure(start);
    lane.success(start);
    lane.success(start + Duration::from_secs(10));
    assert_eq!(lane.attempt(), 2, "10 s Ready is not enough");
    lane.failure(start + Duration::from_secs(20));
    assert_eq!(lane.attempt(), 3, "failure before 30 s healthy escalates");
    lane.success(start + Duration::from_secs(21));
    lane.failure(start + Duration::from_secs(52));
    assert_eq!(lane.attempt(), 1, "31 s healthy resets before counting");
    lane.success(start + Duration::from_secs(53));
    lane.success(start + Duration::from_secs(83));
    assert_eq!(lane.attempt(), 0, "a success after 30 s healthy resets");
    let mut contention = Backoff::contention();
    contention.failure(start);
    contention.success(start);
    assert_eq!(contention.attempt(), 0, "reset_after 0 resets on success");
    contention.failure(start);
    contention.reset();
    assert_eq!(contention.attempt(), 0);
}

#[test]
fn cancel_sleep_returns_within_50ms_of_cancel() {
    for _ in 0..5 {
        let cancel = Cancel::new();
        let sleeper = cancel.clone();
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let cancelled = sleeper.sleep(Duration::from_secs(10));
            tx.send((cancelled, Instant::now())).unwrap();
        });
        thread::sleep(Duration::from_millis(50));
        let at = Instant::now();
        cancel.cancel();
        let (cancelled, woke) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        handle.join().unwrap();
        assert!(cancelled);
        let latency = woke.saturating_duration_since(at);
        assert!(latency <= Duration::from_millis(50), "{latency:?}");
    }
    // Already cancelled: immediate; not cancelled: full duration.
    let cancel = Cancel::new();
    let started = Instant::now();
    assert!(!cancel.sleep(Duration::from_millis(30)));
    assert!(started.elapsed() >= Duration::from_millis(30));
    cancel.cancel();
    let started = Instant::now();
    assert!(cancel.sleep(Duration::from_secs(10)));
    assert!(started.elapsed() < Duration::from_millis(50));
    cancel.wait();
}

#[test]
fn cancel_hooks_run_once_and_children_follow_their_parent() {
    let parent = Cancel::new();
    let child = parent.child();
    let grandchild = child.child();
    let sibling = parent.child();
    let runs = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let runs = runs.clone();
        child.on_cancel(Box::new(move || {
            runs.fetch_add(1, Ordering::SeqCst);
        }));
    }
    child.on_cancel(Box::new(|| panic!("a panicking killer is contained")));
    sibling.cancel();
    assert!(sibling.is_cancelled() && !parent.is_cancelled() && !child.is_cancelled());
    parent.cancel();
    parent.cancel();
    assert!(child.is_cancelled() && grandchild.is_cancelled());
    assert_eq!(runs.load(Ordering::SeqCst), 3);
    child.cancel();
    assert_eq!(runs.load(Ordering::SeqCst), 3, "hooks run exactly once");
    let late = runs.clone();
    child.on_cancel(Box::new(move || {
        late.fetch_add(10, Ordering::SeqCst);
    }));
    assert_eq!(
        runs.load(Ordering::SeqCst),
        13,
        "late hooks run immediately"
    );
    assert!(parent.child().is_cancelled());
}

type Step = Box<dyn FnOnce() + Send>;

/// Wake latency samples: `setup` returns (a blocking wait, the trigger that
/// ends it); each sample is the time from the trigger to the waiter running.
fn wake_latencies(rounds: usize, setup: impl Fn() -> (Step, Step)) -> Vec<Duration> {
    (0..rounds)
        .map(|_| {
            let (wait, trigger) = setup();
            let (tx, rx) = mpsc::channel();
            let handle = thread::spawn(move || {
                wait();
                tx.send(Instant::now()).unwrap();
            });
            thread::sleep(Duration::from_millis(20));
            let at = Instant::now();
            trigger();
            let woke = rx.recv_timeout(Duration::from_secs(5)).unwrap();
            handle.join().unwrap();
            woke.saturating_duration_since(at)
        })
        .collect()
}

fn assert_fast(name: &str, mut latencies: Vec<Duration>, limit: Duration) {
    latencies.sort();
    // p95 within the limit (scheduler noise under a parallel test run is
    // tolerated once in 20), and nothing pathological.
    let p95 = latencies[latencies.len() * 95 / 100 - 1];
    assert!(
        p95 <= limit,
        "{name}: p95 {p95:?} > {limit:?}: {latencies:?}"
    );
    assert!(
        *latencies.last().unwrap() <= Duration::from_millis(250),
        "{name}: {latencies:?}"
    );
}

#[test]
fn snapshot_wait_newer_wakes_within_10ms() {
    let latencies = wake_latencies(20, || {
        let snapshot = Arc::new(Snapshot::new(0u32));
        let waiter = snapshot.clone();
        (
            Box::new(move || {
                assert_eq!(waiter.wait_newer(0, Duration::from_secs(5)), 1);
            }),
            Box::new(move || {
                snapshot.store(7);
            }),
        )
    });
    assert_fast("Snapshot::wait_newer", latencies, Duration::from_millis(10));

    let snapshot = Snapshot::new(String::from("a"));
    assert_eq!(snapshot.generation(), 0);
    assert_eq!(snapshot.store("b".into()), 1);
    assert_eq!(snapshot.update(|old| format!("{old}c")), 2);
    let (value, generation) = snapshot.load_versioned();
    assert_eq!((value.as_str(), generation), ("bc", 2));
    let started = Instant::now();
    assert_eq!(
        snapshot.wait_newer(2, Duration::from_millis(20)),
        2,
        "timeout"
    );
    assert!(started.elapsed() >= Duration::from_millis(20));
    assert_eq!(
        snapshot.wait_newer(1, Duration::from_secs(5)),
        2,
        "already newer"
    );
    let held = snapshot.load();
    snapshot.store("d".into());
    assert_eq!(held.as_str(), "bc", "readers keep their Arc");
    assert!(format!("{snapshot:?}").contains("generation: 3"));
}

#[test]
fn notify_wakes_within_10ms_and_never_loses_pokes() {
    let latencies = wake_latencies(20, || {
        let notify = Arc::new(Notify::new());
        let waiter = notify.clone();
        (
            Box::new(move || {
                assert_eq!(waiter.wait(0, Duration::from_secs(5)), 1);
            }),
            Box::new(move || notify.poke()),
        )
    });
    assert_fast("Notify::wait", latencies, Duration::from_millis(10));
    let notify = Notify::new();
    notify.poke();
    notify.poke();
    assert_eq!(notify.count(), 2);
    assert_eq!(
        notify.wait(1, Duration::from_secs(5)),
        2,
        "missed pokes count"
    );
    assert_eq!(notify.wait(2, Duration::from_millis(5)), 2, "timeout");
}

#[test]
fn clocks_tell_time() {
    let manual = ManualClock::new(T0);
    let instant = manual.instant();
    manual.advance(Duration::from_millis(1500));
    assert_eq!(manual.now_ms(), T0 + 1500);
    assert_eq!(manual.instant() - instant, Duration::from_millis(1500));
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let now = clock.now_ms();
    assert!(now >= T0 - 100_000_000_000 && now.abs_diff(util::now_ms()) < 1000);
    assert!(clock.instant() <= Instant::now());
}

#[test]
fn lock_is_poison_tolerant_and_lock_or_exit_panics_under_panic_policy() {
    let mutex = Arc::new(Mutex::new(5u32));
    let poisoner = mutex.clone();
    let _ = thread::spawn(move || {
        let _guard = poisoner.lock().unwrap();
        panic!("poison the mutex");
    })
    .join();
    assert!(mutex.is_poisoned());
    assert_eq!(*lock::lock(&mutex), 5);
    assert_eq!(lock::poison_policy(), PoisonPolicy::Panic, "the default");
    let hooked = AtomicBool::new(false);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = lock::lock_or_exit(&mutex, &|_| hooked.store(true, Ordering::SeqCst));
    }));
    assert!(result.is_err(), "Panic policy panics");
    assert!(
        !hooked.load(Ordering::SeqCst),
        "the hook runs only under Exit"
    );
    let clean = Mutex::new(1u32);
    *lock::lock_or_exit(&clean, &|_| unreachable!()) += 1;
    assert_eq!(*clean.lock().unwrap(), 2);
}

const CHILD_ENV: &str = "TKFS_CONTRACTS_CHILD";

/// Re-runs one test of this binary in a child process with `CHILD_ENV=mode`.
fn run_child(test: &str, mode: &str, env: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().unwrap()
}

#[test]
fn lock_or_exit_runs_the_hook_then_exits_70_under_exit_policy() {
    if std::env::var(CHILD_ENV).as_deref() == Ok("lock_or_exit") {
        let mutex = Arc::new(Mutex::new(41u32));
        let poisoner = mutex.clone();
        let _ = thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("poison");
        })
        .join();
        lock::set_poison_policy(PoisonPolicy::Exit);
        let _guard = lock::lock_or_exit(&mutex, &|value| {
            *value += 1;
            println!("HOOK_RAN value={value}");
        });
        unreachable!("lock_or_exit must exit");
    }
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let output = run_child(
        "lock_or_exit_runs_the_hook_then_exits_70_under_exit_policy",
        "lock_or_exit",
        &[],
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(70), "{stdout}\n{stderr}");
    assert!(stdout.contains("HOOK_RAN value=42"), "{stdout}");
    let fatal = stderr
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|event| event["code"] == "LOCK_POISONED")
        .unwrap_or_else(|| panic!("no fatal log line: {stderr}"));
    assert_eq!(fatal["level"], "fatal");
    assert_eq!(fatal["exit_code"], 70);
    assert_eq!(fatal["hook_panicked"], false);
}

#[test]
fn tracked_reports_holds_longer_than_50ms() {
    let _serial = log_lock();
    log::set_sink(Some(Box::new(MemorySink::new())));
    let tracked = Tracked::new("contracts.tracked", 0u32);
    assert_eq!(tracked.limit(), Duration::from_millis(50));
    {
        let mut guard = tracked.lock();
        *guard += 1;
    }
    assert_eq!((tracked.holds(), tracked.violations()), (1, 0));
    {
        let guard = tracked.lock();
        thread::sleep(Duration::from_millis(70));
        assert!(guard.held() >= Duration::from_millis(70));
    }
    assert_eq!((tracked.holds(), tracked.violations()), (2, 1));
    assert!(tracked.longest_hold() >= Duration::from_millis(70));
    let violation = lock::hold_violations()
        .into_iter()
        .find(|violation| violation.name == "contracts.tracked")
        .expect("the long hold is in the process-wide ring");
    assert!(violation.held_ms >= 70 && violation.limit_ms == 50);
    assert_eq!(tracked.into_inner(), 1);
    log::set_sink(None);
}

fn fault_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    lock::lock(&LOCK)
}

#[test]
fn fault_points_match_by_name_scope_and_count() {
    let _serial = fault_lock();
    let config = FaultConfig {
        scope: Some("alpha".into()),
        count: Some(2),
        ..FaultConfig::new("contracts.point", FaultAction::Fail)
    };
    fault::configure(Some(config));
    fault::set_scope("C:\\data\\folders\\beta");
    assert_eq!(fault::hit("contracts.point"), None, "out of scope");
    fault::set_scope("C:\\data\\folders\\alpha-1");
    assert_eq!(fault::hit("contracts.other"), None, "exact names only");
    assert_eq!(fault::hit("contracts.point").as_deref(), Some("fail"));
    let error = fault::fail("contracts.point").unwrap_err();
    assert_eq!(code_of(&error), "FAULT_INJECTED");
    assert!(error.to_string().contains("contracts.point"));
    assert_eq!(fault::hit("contracts.point"), None, "count exhausted");
    assert!(fault::fail("contracts.point").is_ok());
    assert_eq!(fault::scope(), "C:\\data\\folders\\alpha-1");

    fault::configure(Some(FaultConfig {
        ms: Some(30),
        ..FaultConfig::new("contracts.hang", FaultAction::Hang)
    }));
    let started = Instant::now();
    fault::trigger("contracts.hang").unwrap();
    assert!(started.elapsed() >= Duration::from_millis(30));
    let started = Instant::now();
    fault::hang("contracts.hang");
    assert!(started.elapsed() >= Duration::from_millis(30));
    fault::exit("contracts.not-armed");

    fault::configure(Some(FaultConfig::new(
        "contracts.panic",
        FaultAction::Panic,
    )));
    let panicked = std::panic::catch_unwind(|| fault::trigger("contracts.panic"));
    assert!(panicked.is_err());
    fault::configure(Some(FaultConfig::new("contracts.fail", FaultAction::Fail)));
    assert_eq!(
        code_of(&fault::trigger("contracts.fail").unwrap_err()),
        "FAULT_INJECTED"
    );
    assert!(
        fault::trigger("contracts.fail").is_err(),
        "no count: every hit"
    );
    fault::configure(None);
    assert_eq!(fault::current(), None);
    assert!(fault::trigger("contracts.fail").is_ok());
    assert_eq!(FaultAction::parse(" EXIT "), Some(FaultAction::Exit));
    assert_eq!(FaultAction::parse("explode"), None);
    fault::set_scope("");
}

#[test]
fn fault_environment_variables_arm_a_point_in_a_child_process() {
    if std::env::var(CHILD_ENV).as_deref() == Ok("fault_env") {
        let config = fault::current().expect("armed from the environment");
        println!(
            "CONFIG {} {:?} {:?}",
            config.point, config.scope, config.count
        );
        fault::set_scope("C:\\data\\folders\\zeta-7");
        fault::trigger("contracts.child.other").unwrap();
        fault::trigger("contracts.child").unwrap();
        unreachable!("the exit action must exit");
    }
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let output = run_child(
        "fault_environment_variables_arm_a_point_in_a_child_process",
        "fault_env",
        &[
            ("TKFS_FAULT_POINT", "contracts.child"),
            ("TKFS_FAULT_SCOPE", "zeta"),
            ("TKFS_FAULT_ACTION", "exit"),
            ("TKFS_FAULT_COUNT", "1"),
        ],
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(fault::EXIT_CODE),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("CONFIG contracts.child Some(\"zeta\") Some(1)"),
        "{stdout}"
    );
}

fn log_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    lock::lock(&LOCK)
}

#[test]
fn log_events_are_json_lines_rate_limited_per_code() {
    let _serial = log_lock();
    let sink = MemorySink::new();
    log::set_sink(Some(Box::new(sink.clone())));
    let object = HASH_A.to_owned();
    let attempts = 3u32;
    let mut written = 0;
    for n in 0..25 {
        if log::event!(warn, CONTRACTS_RATE_TEST, { object: object, "attempt": n, nested: json!({"a": [1, 2]}) })
        {
            written += 1;
        }
    }
    assert_eq!(written, log::RATE_LIMIT);
    let code = String::from("CONTRACTS_DYNAMIC_CODE");
    assert!(log::event!(info, (code.as_str()), { attempts: attempts }));
    assert!(log::event!(error, "CONTRACTS_LITERAL_CODE"));
    for _ in 0..30 {
        assert!(
            log::event!(fatal, CONTRACTS_FATAL_CODE),
            "fatal is never suppressed"
        );
    }
    log::flush_suppressed();
    log::set_sink(None);

    let events = sink.events();
    let of = |code: &str| -> Vec<&Value> {
        events
            .iter()
            .filter(|event| event["code"] == code)
            .collect()
    };
    let rate = of("CONTRACTS_RATE_TEST");
    assert_eq!(rate.len(), 21, "20 events then one suppressed summary");
    let first = rate[0];
    assert_eq!(first["level"], "warn");
    assert_eq!(first["object"], HASH_A);
    assert_eq!(first["attempt"], 0);
    assert_eq!(first["nested"], json!({"a": [1, 2]}));
    assert!(first["at_ms"].as_u64().unwrap() > T0 - 100_000_000_000);
    assert_eq!(rate[20]["suppressed"], 5);
    let line = sink
        .contents()
        .lines()
        .find(|line| line.contains("CONTRACTS_RATE_TEST"))
        .unwrap()
        .to_owned();
    assert!(
        line.starts_with(r#"{"at_ms":"#)
            && line.contains(r#","level":"warn","code":"CONTRACTS_RATE_TEST","object":"#),
        "field order at_ms, level, code, fields: {line}"
    );
    assert_eq!(of("CONTRACTS_DYNAMIC_CODE")[0]["attempts"], 3);
    assert_eq!(of("CONTRACTS_LITERAL_CODE")[0]["level"], "error");
    assert_eq!(of("CONTRACTS_FATAL_CODE").len(), 30);
}

#[test]
fn tracked_violation_is_logged_in_debug_builds() {
    let _serial = log_lock();
    let sink = MemorySink::new();
    log::set_sink(Some(Box::new(sink.clone())));
    let tracked = Tracked::with_limit("contracts.logged", (), Duration::from_millis(1));
    {
        let _guard = tracked.lock();
        thread::sleep(Duration::from_millis(10));
    }
    log::set_sink(None);
    let logged = sink
        .events()
        .into_iter()
        .any(|event| event["code"] == "LOCK_HOLD_EXCEEDED" && event["lock"] == "contracts.logged");
    assert_eq!(logged, cfg!(debug_assertions));
    assert_eq!(tracked.violations(), 1);
}
