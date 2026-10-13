//! WP0.INT: the seams landed by REVAMP_PLAN §14 WP0.INT.
//!
//! Only frozen behaviour is pinned here, so the owners can implement their
//! stubs without editing this file: exact signatures, the `WorkerCtx` shape and
//! its helpers (`SyncStorePool`, `EngineGaugeCell`), the new `Action` JSON, the
//! legacy `manage` answer to it, the unknown-op convention of the worker RPC
//! seams, and the mount invariants that hold for the stub and for WP1.5.
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64},
    },
    time::Duration,
};
use tkfs::{
    core::Store,
    model::{
        errors::{ErrorClass, ErrorWire, code_of},
        health::{MountControl, MountIntent, MountState},
        ipc::{WorkerHeartbeat, WorkerRecord},
        orchestrator::{Ctx, PeerContext},
        protocol::LaneSpec,
    },
    orchestrator::{self, Action, Config, Network, Supervisor},
    runtime::{
        self, Engine, Shared,
        ctx::{EngineGaugeCell, ObjectLease, PooledStore, SyncStorePool, WorkerCtx},
    },
    session::{AcceptedPeer, Conn, Incoming},
    util::{cancel::Cancel, notify::Notify, snapshot::Snapshot},
    worker::mount_ctl::MountController,
};

fn engine(root: &Path) -> Shared {
    Arc::new(Mutex::new(Engine::new(
        Store::open(root, Some("seam-stubs")).unwrap(),
    )))
}

/// A `WorkerCtx` built the way WP1.4 will build it (every field named).
fn worker_ctx(engine: Shared, root: &Path) -> WorkerCtx {
    let (repo, device, locks, stopping) = {
        let e = engine.lock().unwrap();
        (
            e.store.repo.clone(),
            e.store.device.clone(),
            e.transfers.clone(),
            e.stopping.clone(),
        )
    };
    WorkerCtx {
        mount: MountController::new(engine.clone(), None),
        engine,
        root: root.to_owned(),
        state: root.to_owned(),
        repo: repo.clone(),
        device: device.clone(),
        instance: "00000000-0000-4000-8000-000000000000".into(),
        started_at_ms: 1,
        supervised: false,
        version: "test".into(),
        previous: None,
        locks,
        activation: Arc::new(Notify::new()),
        activator_running: AtomicBool::new(false),
        status_generation: AtomicU64::new(0),
        heartbeat: Snapshot::new(WorkerHeartbeat::default()),
        status_cell: Snapshot::new(Value::Null),
        sync_obs: Mutex::new(Default::default()),
        sync_stores: SyncStorePool::new(root, repo, device),
        inflight_objects: Mutex::new(Default::default()),
        stopping,
        last_contact_ms: AtomicU64::new(0),
        gauges: EngineGaugeCell::new(),
    }
}

#[test]
fn stub_signatures_are_exactly_the_landed_ones() {
    let _: fn(&WorkerCtx, &str, &Value) -> anyhow::Result<Value> = runtime::sync_rpc::dispatch;
    type ObjectsDispatch =
        fn(&WorkerCtx, &str, &Value, &[u8]) -> Result<(Value, Vec<u8>), ErrorWire>;
    let _: ObjectsDispatch = runtime::objects_rpc::dispatch;
    let _: fn(Shared, Option<WorkerRecord>) -> Arc<MountController> = MountController::new;
    let _: fn(&Arc<Ctx>, Action) -> anyhow::Result<Value> = orchestrator::manage::manage;
    let _: fn(&Arc<Ctx>, Action) -> anyhow::Result<Value> = orchestrator::manage_v2::handle;
    let _: fn(Arc<Ctx>, Arc<PeerContext>, Conn, Cancel) = orchestrator::scheduler::run_control;
    let _: fn(Arc<Ctx>, Arc<PeerContext>, LaneSpec, Conn, Cancel) =
        orchestrator::objects::run_bulk_lane;
    let _: fn(&Arc<Ctx>, &AcceptedPeer, Incoming) = orchestrator::inbound_objects::handle;
    let _: fn(&Arc<Ctx>) -> anyhow::Result<Value> = orchestrator::diagnostics::collect;
    let _: fn(&SyncStorePool) -> anyhow::Result<PooledStore> = SyncStorePool::checkout;
    let _: fn(&EngineGaugeCell, u64) = EngineGaugeCell::record_wait;
    let _: fn(&EngineGaugeCell, u64) = EngineGaugeCell::record_hold;
    let _: fn(&EngineGaugeCell) -> tkfs::model::health::EngineGauges = EngineGaugeCell::snapshot;
    let _ = ObjectLease {
        session: String::new(),
        expires_ms: 0,
        offset: 0,
    };
    fn shared<T: Send + Sync>() {}
    shared::<WorkerCtx>();
    shared::<SyncStorePool>();
    shared::<EngineGaugeCell>();
    shared::<MountController>();
    fn sendable<T: Send>() {}
    sendable::<PooledStore>();
    let dir = tempfile::tempdir().unwrap();
    let ctx = Arc::new(worker_ctx(engine(dir.path()), dir.path()));
    let _: &Arc<dyn MountControl> = &ctx.mount;
}

#[test]
fn action_v2_variants_parse_the_frozen_json() {
    let parse = |value: Value| serde_json::from_value::<Action>(value).unwrap();
    assert!(matches!(parse(json!({"op":"status"})), Action::Status));
    assert!(matches!(
        parse(json!({"op":"diagnostics"})),
        Action::Diagnostics
    ));
    assert!(matches!(
        parse(json!({"op":"subscribe","since":7})),
        Action::Subscribe {
            since: 7,
            wait_ms: None
        }
    ));
    assert!(matches!(
        parse(json!({"op":"subscribe","since":7,"wait_ms":1000})),
        Action::Subscribe {
            since: 7,
            wait_ms: Some(1000)
        }
    ));
    for (op, force) in [
        (json!({"op":"detach","state":"s"}), false),
        (json!({"op":"detach","state":"s","force":true}), true),
    ] {
        assert!(
            matches!(parse(op), Action::Detach { state, force: f } if state == "s" && f == force)
        );
    }
    for (op, wait) in [
        (json!({"op":"stop","state":"s"}), None),
        (json!({"op":"stop","state":"s","wait":false}), Some(false)),
    ] {
        assert!(matches!(parse(op), Action::Stop { state, wait: w } if state == "s" && w == wait));
    }
    assert!(matches!(
        parse(json!({"op":"revalidate","state":"s"})),
        Action::Revalidate { ids: None, .. }
    ));
    assert!(matches!(
        parse(json!({"op":"revalidate","state":"s","ids":["a","b"]})),
        Action::Revalidate { ids: Some(ids), .. } if ids == ["a", "b"]
    ));
    for op in [
        "attach",
        "pause-sync",
        "resume-sync",
        "override-gate",
        "force-stop",
        "retry",
    ] {
        let value = serde_json::to_value(parse(json!({"op":op,"state":"s"}))).unwrap();
        assert_eq!(value, json!({"op":op,"state":"s"}));
    }
    assert_eq!(
        serde_json::to_value(Action::Subscribe {
            since: 3,
            wait_ms: None
        })
        .unwrap(),
        json!({"op":"subscribe","since":3,"wait_ms":null})
    );
    // `stop` without `wait` serializes exactly as before, so the new CLI and
    // GUI can still stop a 6298d0b daemon (`deny_unknown_fields`).
    let stop = |wait| {
        serde_json::to_value(Action::Stop {
            state: "s".into(),
            wait,
        })
        .unwrap()
    };
    assert_eq!(stop(None), json!({"op":"stop","state":"s"}));
    assert_eq!(
        stop(Some(true)),
        json!({"op":"stop","state":"s","wait":true})
    );
    // `deny_unknown_fields` still applies, and required fields stay required.
    assert!(
        serde_json::from_value::<Action>(json!({"op":"subscribe","since":1,"bogus":2})).is_err()
    );
    assert!(serde_json::from_value::<Action>(json!({"op":"attach"})).is_err());
    assert!(serde_json::from_value::<Action>(json!({"op":"subscribe"})).is_err());
}

#[test]
fn legacy_manage_answers_v2_actions_not_implemented() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        format_version: 2,
        data_directory: dir.path().join("data"),
        default_mount_directory: dir.path().join("mounts"),
        maximum_running: 8,
        network: Network::default(),
    };
    let shared = Arc::new(Mutex::new(Supervisor::open(config).unwrap()));
    let state = || "00000000-0000-4000-8000-000000000001".to_owned();
    for action in [
        Action::Status,
        Action::Subscribe {
            since: 0,
            wait_ms: Some(10),
        },
        Action::Attach { state: state() },
        Action::Detach {
            state: state(),
            force: true,
        },
        Action::PauseSync { state: state() },
        Action::ResumeSync { state: state() },
        Action::OverrideGate { state: state() },
        Action::ForceStop { state: state() },
        Action::Retry { state: state() },
        Action::Revalidate {
            state: state(),
            ids: None,
        },
        Action::Diagnostics,
    ] {
        let op = serde_json::to_value(&action).unwrap()["op"].clone();
        let error = orchestrator::manage(&shared, action).unwrap_err();
        assert_eq!(code_of(&error), "NOT_IMPLEMENTED", "{op}: {error:#}");
        assert!(error.to_string().contains(op.as_str().unwrap()));
    }
    // Legacy actions keep their behaviour; `wait` is accepted and ignored.
    let list = orchestrator::manage(&shared, Action::List).unwrap();
    assert_eq!(list["folders"], json!([]));
    let error = orchestrator::manage(
        &shared,
        Action::Stop {
            state: state(),
            wait: Some(false),
        },
    )
    .unwrap_err();
    assert_ne!(code_of(&error), "NOT_IMPLEMENTED");
}

#[test]
fn worker_rpc_seams_answer_unknown_ops_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = worker_ctx(engine(dir.path()), dir.path());
    let error = runtime::sync_rpc::dispatch(&ctx, "feed-unknown", &json!({})).unwrap_err();
    assert_eq!(code_of(&error), "UNSUPPORTED_OPERATION");
    assert!(error.to_string().contains("feed-unknown"));
    let error =
        runtime::objects_rpc::dispatch(&ctx, "objects.unknown", &json!({}), b"body").unwrap_err();
    assert_eq!(error.code, "UNSUPPORTED_OPERATION");
    assert_eq!(error.class, ErrorClass::Transient);
    assert!(error.message.contains("objects.unknown"));
}

#[test]
fn sync_store_pool_reuses_up_to_eight_idle_connections() {
    let dir = tempfile::tempdir().unwrap();
    let shared = engine(dir.path());
    let ctx = worker_ctx(shared.clone(), dir.path());
    let pool = &ctx.sync_stores;
    assert_eq!(pool.idle(), 0);
    {
        let store = pool.checkout().unwrap();
        assert_eq!(store.repo, ctx.repo);
        assert_eq!(store.device, ctx.device);
        assert!(
            store
                .branches()
                .unwrap()
                .iter()
                .any(|b| b.id == store.active)
        );
    }
    assert_eq!(pool.idle(), 1);
    // A pooled connection sees the engine's later commits (view freshness).
    assert!(pool.checkout().unwrap().lookup("created-later").is_err());
    {
        let mut e = shared.lock().unwrap();
        let (handle, _) = e.open("created-later", Some("file"), true).unwrap();
        e.write(handle, 0, b"bytes", false, false).unwrap();
        e.close(handle).unwrap();
    }
    assert_eq!(pool.idle(), 1);
    assert!(pool.checkout().unwrap().lookup("created-later").is_ok());
    // Checkouts never block; only MAX_IDLE connections are kept.
    let many: Vec<_> = (0..SyncStorePool::MAX_IDLE + 4)
        .map(|_| pool.checkout().unwrap())
        .collect();
    assert_eq!(pool.idle(), 0);
    drop(many);
    assert_eq!(pool.idle(), SyncStorePool::MAX_IDLE);
    // A connection returned inside a transaction is closed, never reused.
    let open = pool.checkout().unwrap();
    open.db.execute_batch("BEGIN").unwrap();
    drop(open);
    assert_eq!(pool.idle(), SyncStorePool::MAX_IDLE - 1);
}

#[test]
fn sync_store_pool_follows_the_engine_checkout() {
    let dir = tempfile::tempdir().unwrap();
    let shared = engine(dir.path());
    let ctx = worker_ctx(shared.clone(), dir.path());
    let pool = &ctx.sync_stores;
    let engine_view = || {
        let e = shared.lock().unwrap();
        (
            e.store.active.clone(),
            e.store.generation,
            e.store.status().unwrap(),
        )
    };
    let (first, generation, _) = engine_view();
    {
        let store = pool.checkout().unwrap();
        assert_eq!(store.active, first);
        assert_eq!(store.generation, generation);
    }
    assert_eq!(pool.idle(), 1);
    {
        let mut e = shared.lock().unwrap();
        e.control("fork", &json!({"op":"branch","name":"second"}))
            .unwrap();
        e.control("switch", &json!({"op":"checkout","name":"second"}))
            .unwrap();
        let (handle, _) = e.open("on-second", Some("file"), true).unwrap();
        e.write(handle, 0, b"bytes", false, false).unwrap();
        e.close(handle).unwrap();
    }
    let (second, generation, status) = engine_view();
    assert_ne!(second, first);
    // The idle connection opened on the first branch is reused, and reports
    // the engine's branch, generation and namespace.
    let reused = pool.checkout().unwrap();
    assert_eq!(pool.idle(), 0, "the idle connection was reused");
    assert_eq!(reused.active, second);
    assert_eq!(reused.generation, generation);
    let pooled = reused.status().unwrap();
    assert_eq!(pooled["branch"], status["branch"]);
    assert_eq!(pooled["generation"], status["generation"]);
    assert!(reused.lookup("on-second").is_ok());
    // A new connection agrees.
    let fresh = pool.checkout().unwrap();
    assert_eq!(fresh.active, second);
    assert_eq!(fresh.generation, generation);
}

#[test]
fn engine_gauge_cell_reports_waits_holds_and_stalls() {
    let cell = EngineGaugeCell::new();
    assert_eq!(cell.snapshot(), Default::default());
    for ms in 1..=100 {
        cell.record_wait(ms);
    }
    assert_eq!(cell.snapshot().wait_p99_ms, 99);
    // Only the most recent WAIT_SAMPLES waits count.
    for _ in 0..EngineGaugeCell::WAIT_SAMPLES {
        cell.record_wait(0);
    }
    assert_eq!(cell.snapshot().wait_p99_ms, 0);
    {
        let _a = cell.begin_wait();
        let _b = cell.begin_wait();
        assert_eq!(cell.waiters(), 2);
        assert_eq!(cell.snapshot().waiters, 2);
    }
    assert_eq!(cell.waiters(), 0);
    cell.begin_hold();
    std::thread::sleep(Duration::from_millis(30));
    let holding = cell.snapshot();
    assert!(holding.current_hold_ms >= 20, "{holding:?}");
    assert!(holding.longest_hold_ms >= holding.current_hold_ms);
    assert!(!holding.stalled);
    cell.record_hold(40);
    cell.record_hold(5);
    let released = cell.snapshot();
    assert_eq!(released.holds, 2);
    assert_eq!(released.current_hold_ms, 0);
    assert!(released.longest_hold_ms >= 40);
    assert!(!released.stalled);
    // A hold reported late, from another thread, after the next holder began
    // (here: this thread), never clears the next holder's mark.
    cell.begin_hold();
    std::thread::scope(|s| {
        s.spawn(|| cell.record_hold(1));
    });
    std::thread::sleep(Duration::from_millis(30));
    let next = cell.snapshot();
    assert_eq!(next.holds, 3);
    assert!(next.current_hold_ms >= 20, "{next:?}");
    cell.record_hold(30);
    assert_eq!(cell.snapshot().current_hold_ms, 0);
}

#[test]
fn mount_failures_stay_mount_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let shared = engine(&dir.path().join("state"));
    let controller = MountController::new(shared.clone(), None);
    assert!(controller.previous().is_none());
    assert!(!controller.status().state.is_attached());
    // Detaching an unmounted worker succeeds and never fences the engine.
    assert!(!controller.detach(false).unwrap().state.is_attached());
    assert!(!shared.lock().unwrap().switching);
    // An occupied mount path is a NeedsUserAction attach failure (after the
    // WinFsp runtime check on Windows) that never touches engine health.
    let occupied = dir.path().join("occupied");
    std::fs::create_dir_all(&occupied).unwrap();
    std::fs::write(occupied.join("file"), b"x").unwrap();
    let error = controller.attach(&occupied).unwrap_err();
    assert!(
        ["MOUNT_PATH_MUST_NOT_EXIST", "WINFSP_RUNTIME_UNAVAILABLE"].contains(&error.code.as_str()),
        "{error}"
    );
    assert_eq!(error.class, ErrorClass::NeedsUserAction);
    let status = controller.status();
    assert_eq!(status.intent, MountIntent::Attach);
    assert_eq!(status.path.as_deref(), Some(occupied.as_path()));
    match status.state {
        MountState::AttachFailed {
            error: recorded,
            attempt,
            ..
        } => {
            assert_eq!(recorded, error);
            assert!(attempt >= 1);
        }
        other => panic!("expected AttachFailed, got {other:?}"),
    }
    let e = shared.lock().unwrap();
    assert!(e.health.is_none());
    assert!(!e.mounted);
}
