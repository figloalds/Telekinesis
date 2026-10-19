//! Owned by WP2.1.
//!
//! Management dispatch over the actor orchestrator (REVAMP_PLAN §3.5, §14
//! WP2.1 task 3): the legacy actions with their compatibility shapes (§7.3),
//! and the new variants forwarded to `manage_v2::handle`.
//!
//! No action takes a lock that is held across I/O, and no action waits for
//! another folder:
//!
//! * **reads** (List, Inspect, Peers) are built from the catalog snapshot,
//!   the controllers' `FolderView`s and the hub (its legacy projection since
//!   WP3.1's cutover, `compat::HubLegacyView`), with no waiting;
//! * **writes** are `catalog.apply` (one transaction on the catalog thread),
//!   then a `FolderCmd` to that folder's controller. Start returns at once
//!   (`starting:true`); Stop waits at most 15 s for `Stopped|StopBlocked`
//!   unless `wait:false`;
//! * Create runs `Store::initialize` on the request thread;
//! * Pair, Unpair and RemoteList go through `ctx.network()`, Peers and the
//!   `sync` member through `ctx.legacy()`.
use super::{
    api::{Action, Project},
    catalog::{self, validate_mount_path},
    ctx,
    folder::initial_view,
    manage_v2, shutdown,
};
use crate::{
    core::{Store, id, name_key},
    model::{
        errors::{ErrorWire, Scope},
        health::{MountIntent, MountState},
        ipc::{StopMode, deadlines},
        orchestrator::{
            CatalogCmd, CatalogSnapshot, Ctx, EntryState, FolderCmd, FolderPhase, FolderRecord,
            FolderView, GateRecord, GateState, limits,
        },
        status::{FolderStatus, StatusEvent, StatusSnapshot},
    },
    paired_sync::PublishedState,
    pairing_code::Verifier,
    runtime::Discovery,
    util::time::now_ms,
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use serde_json::{Map, Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

/// The legacy worker ops the `Folder` passthrough allows (unchanged).
pub const FOLDER_OPS: &[&str] = &[
    "status",
    "folder-details",
    "conflicts",
    "preview-object",
    "request-object",
    "resolve",
    "recover",
    "branches",
    "history",
    "checkpoint",
    "branch",
    "restore",
];
/// How long Create waits for the registry to give a new folder its controller.
pub const CONTROLLER_WAIT: Duration = Duration::from_secs(10);
/// How long Create (and Receive) waits for its folder when the catalog
/// answered `REQUEST_TIMEOUT` for `create_folder` (the command was running
/// when `catalog::APPLY_WAIT` passed and may still be applied). The reply is
/// the folder if it appears meanwhile; otherwise `REQUEST_TIMEOUT` naming
/// its state, and the state directory is kept (WP2.INT's confirmation
/// review).
pub const CREATE_SETTLE_WAIT: Duration = Duration::from_secs(5);
/// How often Create probes the catalog while it settles.
const SETTLE_POLL: Duration = Duration::from_millis(250);
/// How long Start, Create and Receive wait for the controller to take the
/// command (it answers right after the step that handled it).
pub const COMMAND_REPLY_WAIT: Duration = Duration::from_secs(2);
/// How long Stop waits for `Stopped|StopBlocked` (§3.5: at most
/// `limits::API_STOP_WAIT`, 15 s). Two seconds less, so the reply reaches a
/// legacy client before its own 15 s socket read timeout (`ws::connect`).
pub const STOP_REPLY_WAIT: Duration = Duration::from_secs(limits::API_STOP_WAIT.as_secs() - 2);
/// Phase text while a running folder waits for its initial-sync gate (§7.3).
pub const PHASE_GATED: &str = "synchronizing remote metadata";
/// Phase text while a running folder attaches its mount (§7.3).
pub const PHASE_ATTACHING: &str = "attaching filesystem";

/// Answers one management action (§3.5). Every request first checks
/// `stopping` (`ORCHESTRATOR_STOPPING`).
pub fn manage(ctx: &Arc<Ctx>, action: Action) -> Result<Value> {
    ensure!(
        !ctx.stopping.load(Ordering::Acquire),
        "ORCHESTRATOR_STOPPING: the orchestrator is shutting down"
    );
    match action {
        Action::List => list(ctx),
        Action::Inspect { state } => inspect(ctx, &state),
        Action::Folder {
            state,
            request,
            payload,
        } => folder(ctx, &state, &request, payload),
        Action::Create {
            label,
            mount,
            headless,
        } => create(ctx, label, None, mount, headless, None),
        Action::ConfigureMount { state, mount } => configure_mount(ctx, &state, mount),
        Action::Start { state } => start(ctx, &state),
        Action::Stop { state, wait } => stop(ctx, &state, wait.unwrap_or(true)),
        Action::Shutdown => Ok(shutdown::run(ctx, limits::SHUTDOWN_FOLDERS)),
        Action::ResetPairingCode => {
            let (code, verifier) = Verifier::generate();
            apply(ctx, CatalogCmd::SetVerifier { verifier })?;
            Ok(json!({ "code": code }))
        }
        Action::Pair {
            endpoint,
            code,
            connections,
        } => {
            let connections = connections
                .map(u32::try_from)
                .transpose()
                .map_err(|_| anyhow!("INVALID_CONNECTION_COUNT"))?;
            let paired = ctx
                .network()
                .map_err(ErrorWire::into_anyhow)?
                .pair(&endpoint, &code, connections)
                .map_err(ErrorWire::into_anyhow)?;
            Ok(json!(paired))
        }
        Action::Unpair { endpoint } => {
            ctx.network()
                .map_err(ErrorWire::into_anyhow)?
                .unpair(&endpoint)
                .map_err(ErrorWire::into_anyhow)?;
            Ok(json!({ "removed": endpoint }))
        }
        Action::Peers => Ok(ctx.legacy().map_err(ErrorWire::into_anyhow)?.peers()),
        Action::RemoteList { endpoint } => Ok(json!(remote_list(ctx, &endpoint)?)),
        Action::Receive {
            endpoint,
            repo,
            label,
            mount,
            headless,
        } => {
            ensure!(
                remote_list(ctx, &endpoint)?
                    .iter()
                    .any(|published| published.repo == repo),
                "REMOTE_FOLDER_NOT_FOUND"
            );
            let installation = ctx
                .catalog
                .snapshot()
                .peer_by_endpoint(&endpoint)
                .map(|peer| peer.installation.clone());
            create(
                ctx,
                label,
                Some(repo),
                mount,
                headless,
                Some((endpoint, installation)),
            )
        }
        v2 @ (Action::Status
        | Action::Subscribe { .. }
        | Action::Attach { .. }
        | Action::Detach { .. }
        | Action::PauseSync { .. }
        | Action::ResumeSync { .. }
        | Action::OverrideGate { .. }
        | Action::ForceStop { .. }
        | Action::Retry { .. }
        | Action::Revalidate { .. }
        | Action::Diagnostics) => manage_v2::handle(ctx, v2),
    }
}

/// `catalog.apply`. A catalog that is gone (its thread exited, the handle
/// closed) or a command that failed inside it (a panic) is also a
/// daemon-scoped status error (the failure matrix's "daemon-level
/// ErrorRecord", §2.3), resolved by the next management write that
/// succeeds. A command refused because another write is slow
/// (`catalog::is_refusal`) or answered `REQUEST_TIMEOUT` is only this
/// caller's answer: the catalog is alive, and a daemon error recorded for it
/// would outlive the slow write (WP2.INT's confirmation review).
fn apply(ctx: &Arc<Ctx>, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>> {
    let extras = ctx::extras(ctx);
    match ctx.catalog.apply(cmd) {
        Ok(snapshot) => {
            if extras.catalog_error.swap(false, Ordering::AcqRel) {
                ctx.status.record(StatusEvent::Resolved {
                    scope: Scope::Daemon,
                    code: "CATALOG_UNAVAILABLE".into(),
                });
            }
            Ok(snapshot)
        }
        Err(error) => {
            if error.code == "CATALOG_UNAVAILABLE" && !catalog::is_refusal(&error) {
                ctx.status.record(StatusEvent::Error {
                    scope: Scope::Daemon,
                    error: error.clone(),
                });
                // After the record: a write that succeeds meanwhile and sees
                // the flag resolves an error the hub already holds.
                extras.catalog_error.store(true, Ordering::Release);
            }
            Err(error.into_anyhow())
        }
    }
}

/// The published folders of a paired peer, in the legacy `PublishedState`
/// shape (RemoteList's reply). As the legacy `list` did, only folders whose
/// worker serves now (`EntryState::Running`): the v3 catalog also lists
/// starting, paused and unavailable folders (WP3.1's cutover keeps the
/// legacy reply, which RemoteList and Receive rely on).
fn remote_list(ctx: &Ctx, endpoint: &str) -> Result<Vec<PublishedState>> {
    Ok(ctx
        .network()
        .map_err(ErrorWire::into_anyhow)?
        .remote_list(endpoint)
        .map_err(ErrorWire::into_anyhow)?
        .into_iter()
        .filter(|entry| entry.state == EntryState::Running)
        .map(|entry| PublishedState {
            repo: entry.repo,
            replica: entry.replica,
            label: entry.label,
            branches: entry.branches,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// The §7.3 projection

/// The legacy List/Inspect members of a folder, from its catalog record, its
/// controller's view and whether a gate row exists (§7.3, without `sync` and
/// `status`, which come from the network view and the hub):
///
/// | member | rule |
/// |---|---|
/// | `running` | `Running` && gate ∉ {awaiting, blocked} && (no mount requested ‖ `!mount_enabled` ‖ the user detached it (mount intent `Detach`) ‖ mount ∈ {Attached, AttachFailed, Lost}) |
/// | `starting` | `Starting` ‖ (`Running` && `!running`) |
/// | `mounted` | the mount is `Attached` (actual) |
/// | `initial_sync_pending` | a gate row exists, unless the folder runs and its controller is deleting it (mount `Attached`, or no mount requested and the gate satisfied or overridden) |
/// | `startup_phase` | the `Starting` phase text; `"synchronizing remote metadata"` while gated; `"attaching filesystem"` while attaching; else null |
/// | `error` | the lifecycle error (Failed, or an open controller error), else the mount `AttachFailed` message |
/// | `publication_error` | the open `PUBLISHED_SUMMARY_FAILED` error |
pub fn project(record: &FolderRecord, view: &FolderView, gate_row: bool) -> Map<String, Value> {
    let Value::Object(mut value) = json!(Project {
        state: record.state.clone(),
        label: record.label.clone(),
        repo: record.repo.clone(),
        device: record.device.clone(),
        directory: record.directory.clone(),
        mount: record.mount.clone(),
        desired_running: record.desired_running,
    }) else {
        unreachable!("a struct serializes as an object")
    };
    let gated = matches!(
        view.gate,
        GateState::Awaiting { .. } | GateState::Blocked { .. }
    );
    // A mount the user detached (WP2.2's Detach, a Wave 2 action the frozen
    // table predates) is not requested: the folder runs headless until
    // Attach, it is not "attaching" (post-integration re-review, WP2.INT).
    let mount_requested =
        record.mount.is_some() && record.mount_enabled && view.mount.intent != MountIntent::Detach;
    let mount_settled = matches!(
        view.mount.state,
        MountState::Attached { .. } | MountState::AttachFailed { .. } | MountState::Lost { .. }
    );
    let phase_running = matches!(view.phase, FolderPhase::Running { .. });
    let running = phase_running && !gated && (!mount_requested || mount_settled);
    let starting =
        matches!(view.phase, FolderPhase::Starting { .. }) || (phase_running && !running);
    let startup_phase = match &view.phase {
        FolderPhase::Starting { phase, .. } => Some(phase.clone()),
        FolderPhase::Running { .. } if gated => Some(PHASE_GATED.to_owned()),
        FolderPhase::Running { .. } if !running => Some(PHASE_ATTACHING.to_owned()),
        _ => None,
    };
    let error = lifecycle_error(view).or_else(|| match &view.mount.state {
        MountState::AttachFailed { error, .. } => Some(error.to_string()),
        _ => None,
    });
    let publication_error = view
        .published_error
        .as_ref()
        .filter(|record| !record.is_resolved())
        .map(|record| record.error.to_string());
    value.insert("running".into(), json!(running));
    value.insert("starting".into(), json!(starting));
    value.insert("mounted".into(), json!(view.mount.state.is_attached()));
    // The controller deletes the gate marker on a helper thread once the
    // folder runs complete (invariant 26, §3.3; off its thread since WP2.INT's
    // post-integration re-review): a running, attached folder never reports
    // its initial sync pending while that write is in flight, as before
    // (CCR-WP2.INT-7, WP2.INT's confirmation review).
    let marker_done = phase_running
        && (view.mount.state.is_attached()
            || (!(record.mount.is_some() && record.mount_enabled)
                && matches!(
                    view.gate,
                    GateState::Satisfied { .. } | GateState::Overridden { .. }
                )));
    value.insert(
        "initial_sync_pending".into(),
        json!(gate_row && !marker_done),
    );
    value.insert("startup_phase".into(), json!(startup_phase));
    value.insert("error".into(), json!(error));
    value.insert("publication_error".into(), json!(publication_error));
    value
}

/// The folder's lifecycle error: `Failed`'s, else the controller's open one.
fn lifecycle_error(view: &FolderView) -> Option<String> {
    if let FolderPhase::Failed { error } = &view.phase {
        return Some(error.error.to_string());
    }
    view.last_error
        .as_ref()
        .filter(|record| !record.is_resolved())
        .map(|record| record.error.to_string())
}

/// The view of `record`'s controller, or the view a folder has before its
/// controller ran.
fn view_of(ctx: &Ctx, record: &FolderRecord) -> Arc<FolderView> {
    ctx.folders
        .view(&record.state)
        .unwrap_or_else(|| Arc::new(initial_view(record)))
}

/// The full legacy shape of one folder (§7.3): the projection, the legacy
/// `sync` member, and the typed `status` member from the hub.
fn folder_value(
    ctx: &Ctx,
    snapshot: &CatalogSnapshot,
    status: &StatusSnapshot,
    record: &FolderRecord,
    view: &FolderView,
) -> Value {
    let mut value = project(record, view, snapshot.gates.contains_key(&record.state));
    let sync = ctx
        .legacy()
        .map(|legacy| legacy.folder_sync(record))
        .unwrap_or_else(|_| json!([]));
    value.insert("sync".into(), sync);
    let typed = status
        .folders
        .iter()
        .find(|folder| folder.state == record.state)
        .map_or(Value::Null, |folder| json!(current_status(folder)));
    value.insert("status".into(), typed);
    Value::Object(value)
}

/// The folder's current typed status: its open errors only. The resolved
/// history (50 entries, `resolved_at_ms`) stays in the `Status` snapshot; a
/// List row describes the folder now.
fn current_status(folder: &FolderStatus) -> FolderStatus {
    let mut folder = folder.clone();
    folder.errors.retain(|record| !record.is_resolved());
    folder
}

fn list(ctx: &Ctx) -> Result<Value> {
    let snapshot = ctx.catalog.snapshot();
    let status = ctx.status_source.snapshot();
    let folders: Vec<Value> = snapshot
        .folders
        .iter()
        .map(|record| folder_value(ctx, &snapshot, &status, record, &view_of(ctx, record)))
        .collect();
    Ok(json!({ "installation": ctx.installation, "folders": folders }))
}

fn record_of(snapshot: &CatalogSnapshot, state: &str) -> Result<FolderRecord> {
    snapshot
        .folder(state)
        .cloned()
        .with_context(|| format!("FOLDER_NOT_FOUND: no folder with state {state}"))
}

fn inspect(ctx: &Ctx, state: &str) -> Result<Value> {
    let snapshot = ctx.catalog.snapshot();
    let record = record_of(&snapshot, state)?;
    let view = view_of(ctx, &record);
    Ok(folder_value(
        ctx,
        &snapshot,
        &ctx.status_source.snapshot(),
        &record,
        &view,
    ))
}

/// The newer of a view a command returned and the registry's current one.
fn newest(ctx: &Ctx, state: &str, view: Arc<FolderView>) -> Arc<FolderView> {
    match ctx.folders.view(state) {
        Some(current) if current.generation > view.generation => current,
        _ => view,
    }
}

/// Inspect, rendered from `view` (a command's result). Its `status` member
/// is rebuilt now (the hub's snapshot is otherwise rebuilt at most 4 times a
/// second), so a command's reply shows the command's own effect.
fn inspect_view(ctx: &Arc<Ctx>, state: &str, view: &FolderView) -> Result<Value> {
    let snapshot = ctx.catalog.snapshot();
    let record = record_of(&snapshot, state)?;
    let status = match ctx::extras(ctx).hub() {
        Some(hub) => hub.snapshot_now(),
        None => ctx.status_source.snapshot(),
    };
    Ok(folder_value(ctx, &snapshot, &status, &record, view))
}

// ---------------------------------------------------------------------------
// Folder actions

/// `Folder`: the legacy worker passthrough, to the controller's endpoint
/// (never `runtime.json`), with the caller's request id (worker receipts make
/// a retried mutation idempotent) and a 30 s deadline.
fn folder(ctx: &Ctx, state: &str, request: &str, payload: Value) -> Result<Value> {
    ensure!(
        payload["op"]
            .as_str()
            .is_some_and(|op| FOLDER_OPS.contains(&op)),
        "UNSUPPORTED_FOLDER_ACTION"
    );
    let snapshot = ctx.catalog.snapshot();
    let record = record_of(&snapshot, state)?;
    let view = view_of(ctx, &record);
    let worker = match (&view.phase, &view.worker) {
        (FolderPhase::Running { .. }, Some(worker)) => worker.clone(),
        _ => bail!("WORKER_NOT_RUNNING: {} has no running worker", record.label),
    };
    let info = Discovery {
        format: 2,
        address: worker.address,
        state: record.directory.to_string_lossy().into_owned(),
        mount: None,
        repo: record.repo,
        device: record.device,
    };
    // The legacy lanes' `DeadlineLane` went with them (WP3.1): the same
    // bounded call, with the caller's request id, on a fresh connection.
    super::compat::worker_call(&info, request, payload, deadlines::FOLDER_PASSTHROUGH)
}

/// Folders that hold (or are about to hold) a worker slot: anything but
/// Stopped and Failed. The registry enforces the limit on every Start; this
/// is Create's pre-check, so a refused Create leaves nothing behind (as the
/// legacy `create` did).
fn held_slots(ctx: &Ctx) -> usize {
    ctx.folders
        .all()
        .iter()
        .filter(|view| {
            !matches!(
                view.phase,
                FolderPhase::Stopped | FolderPhase::Failed { .. }
            )
        })
        .count()
}

/// Create and Receive: validate, `Store::initialize` on this thread, the
/// catalog row (and gate), then Start through the new folder's controller.
/// The new state directory is removed only when the catalog did not apply
/// the row; when its outcome is unknown (`REQUEST_TIMEOUT`), Create waits
/// for it ([`CREATE_SETTLE_WAIT`]) and keeps the directory.
fn create(
    ctx: &Arc<Ctx>,
    label: String,
    repo: Option<String>,
    mount: Option<PathBuf>,
    headless: bool,
    source: Option<(String, Option<String>)>,
) -> Result<Value> {
    ensure!(
        !label.trim().is_empty() && label.len() <= 200,
        "INVALID_FOLDER_LABEL"
    );
    let snapshot = ctx.catalog.snapshot();
    let state = id();
    let repo = repo.unwrap_or_else(id);
    ensure!(
        snapshot.folder_by_repo(&repo).is_none(),
        "REPOSITORY_ALREADY_MANAGED"
    );
    ensure!(
        held_slots(ctx) < ctx.config.maximum_running,
        "WORKER_LIMIT: {} folders already hold a worker (maximum_running)",
        ctx.config.maximum_running
    );
    let directory = ctx.config.data_directory.join("states").join(&state);
    let mount = if headless {
        ensure!(mount.is_none(), "HEADLESS_MOUNT_CONFLICT");
        None
    } else {
        if mount.is_none() {
            name_key(&label).context("INVALID_MOUNT_FOLDER_NAME")?;
        }
        Some(mount.unwrap_or_else(|| ctx.config.default_mount_directory.join(&label)))
    };
    let mount = mount
        .map(|path| {
            validate_mount_path(&snapshot, &ctx.config.data_directory, &directory, &path)
                .map_err(ErrorWire::into_anyhow)
        })
        .transpose()?;
    let device = id();
    drop(Store::initialize(&directory, &repo, &device)?);
    let record = FolderRecord {
        state: state.clone(),
        label,
        repo,
        device,
        directory: directory.clone(),
        mount,
        desired_running: true,
        sync_enabled: true,
        mount_enabled: true,
    };
    let gate = source.map(|(endpoint, installation)| GateRecord {
        state: state.clone(),
        source_installation: installation,
        source_endpoint: endpoint,
        created_ms: now_ms(),
        override_ms: None,
    });
    match apply(ctx, CatalogCmd::CreateFolder { record, gate }) {
        Ok(_) => {}
        Err(error) if !outcome_unknown(&error) => {
            // Not applied (refused, withdrawn, rolled back): nothing refers
            // to the new state directory, do not leave it behind.
            let _ = std::fs::remove_dir_all(&directory);
            return Err(error);
        }
        // `REQUEST_TIMEOUT`: the command was still running and may commit.
        // Never delete the store of a folder the catalog may yet list: its
        // worker could not open it (a crash loop, then Failed). An orphaned
        // state directory is harmless (WP2.INT's confirmation review).
        Err(_) => match settle_create(ctx, &state) {
            Settled::Created => {}
            Settled::NotCreated => {
                let _ = std::fs::remove_dir_all(&directory);
                return Err(ErrorWire::coded(
                    "CATALOG_UNAVAILABLE",
                    format!(
                        "the catalog command create_folder ran past {} ms and was not applied; \
                         the folder was not created",
                        catalog::APPLY_WAIT.as_millis()
                    ),
                )
                .into_anyhow());
            }
            Settled::Unknown => {
                return Err(ErrorWire::coded(
                    "REQUEST_TIMEOUT",
                    format!(
                        "the catalog command create_folder did not finish within {} ms; folder \
                         {state} may still be created: look for it in List before creating it \
                         again",
                        (catalog::APPLY_WAIT + CREATE_SETTLE_WAIT).as_millis()
                    ),
                )
                .into_anyhow());
            }
        },
    }
    wait_for_controller(ctx, &state)?;
    // The controller starts a desired folder by itself; Start makes the reply
    // show that start (and keeps the legacy Create → Start order).
    let view = match ctx
        .folders
        .request(&state, FolderCmd::Start, COMMAND_REPLY_WAIT)
    {
        Ok(view) => newest(ctx, &state, view),
        // The folder is created; its controller shows why it does not run
        // (the legacy create kept the record and reported the error).
        Err(_) => ctx
            .folders
            .view(&state)
            .context("FOLDER_UNAVAILABLE: the new folder has no controller")?,
    };
    inspect_view(ctx, &state, &view)
}

/// Whether a management write's error leaves its command's outcome unknown
/// (`catalog::outcome_unknown`: `REQUEST_TIMEOUT`, it may still be applied).
fn outcome_unknown(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<ErrorWire>()
        .is_some_and(catalog::outcome_unknown)
}

/// What became of a `create_folder` the catalog answered `REQUEST_TIMEOUT`.
enum Settled {
    Created,
    NotCreated,
    Unknown,
}

/// Waits at most [`CREATE_SETTLE_WAIT`] for the outcome of a `create_folder`
/// that was still running when `catalog::APPLY_WAIT` passed: the folder
/// appears in the catalog (created), or a later command the catalog ran
/// finds no such folder (the catalog runs commands one at a time, in order,
/// so ours had ended without being applied). The probe is a no-op on the new
/// folder (`SetIntents` with nothing to change); while the slow write still
/// runs it is refused at once, and the wait goes on. Anything else leaves
/// the outcome unknown.
fn settle_create(ctx: &Ctx, state: &str) -> Settled {
    let deadline = Instant::now() + CREATE_SETTLE_WAIT;
    loop {
        let snapshot = ctx.catalog.snapshot();
        if snapshot.folder(state).is_some() {
            return Settled::Created;
        }
        let probe = CatalogCmd::SetIntents {
            state: state.to_owned(),
            sync_enabled: None,
            mount_enabled: None,
        };
        match ctx.catalog.apply(probe) {
            Ok(_) => return Settled::Created,
            Err(error) if error.code == "FOLDER_NOT_FOUND" => return Settled::NotCreated,
            Err(error) if catalog::is_refusal(&error) => {}
            Err(_) if ctx.catalog.snapshot().folder(state).is_some() => return Settled::Created,
            Err(_) => return Settled::Unknown,
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return if ctx.catalog.snapshot().folder(state).is_some() {
                Settled::Created
            } else {
                Settled::Unknown
            };
        }
        ctx.catalog
            .wait_newer(snapshot.generation, remaining.min(SETTLE_POLL));
    }
}

/// Waits until the registry has spawned the controller of a new folder.
fn wait_for_controller(ctx: &Ctx, state: &str) -> Result<()> {
    let deadline = Instant::now() + CONTROLLER_WAIT;
    loop {
        let generation = ctx.folders.generation();
        if ctx.folders.view(state).is_some() {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "FOLDER_UNAVAILABLE: no controller for {state} after {} s",
            CONTROLLER_WAIT.as_secs()
        );
        ctx.folders
            .wait_newer(generation, remaining.min(Duration::from_millis(100)));
    }
}

/// ConfigureMount: refused while the folder has a worker (legacy semantics,
/// `STOP_FOLDER_BEFORE_CONFIGURING`); the path is validated like Create's.
fn configure_mount(ctx: &Arc<Ctx>, state: &str, mount: Option<PathBuf>) -> Result<Value> {
    let snapshot = ctx.catalog.snapshot();
    let record = record_of(&snapshot, state)?;
    let busy = ctx.folders.view(state).is_some_and(|view| {
        matches!(
            view.phase,
            FolderPhase::Starting { .. }
                | FolderPhase::Running { .. }
                | FolderPhase::StopBlocked { .. }
                | FolderPhase::Stopping { .. }
        )
    });
    ensure!(!busy, "STOP_FOLDER_BEFORE_CONFIGURING");
    let mount = if mount == record.mount {
        mount
    } else {
        mount
            .map(|path| {
                validate_mount_path(
                    &snapshot,
                    &ctx.config.data_directory,
                    &record.directory,
                    &path,
                )
                .map_err(ErrorWire::into_anyhow)
            })
            .transpose()?
    };
    apply(
        ctx,
        CatalogCmd::SetMount {
            state: state.to_owned(),
            mount,
        },
    )?;
    inspect(ctx, state)
}

/// Start: `desired=1`, then `FolderCmd::Start`; returns at once
/// (`starting:true`). Beyond `maximum_running`: `WORKER_LIMIT`.
fn start(ctx: &Arc<Ctx>, state: &str) -> Result<Value> {
    apply(
        ctx,
        CatalogCmd::SetDesired {
            state: state.to_owned(),
            desired: true,
        },
    )?;
    let view = ctx
        .folders
        .request(state, FolderCmd::Start, COMMAND_REPLY_WAIT)
        .map_err(ErrorWire::into_anyhow)?;
    inspect_view(ctx, state, &newest(ctx, state, view))
}

/// Stop: `desired=0` first (invariant 26), then a graceful stop. With `wait`
/// (the default) it waits at most 15 s for `Stopped|StopBlocked`; a folder
/// still stopping or refusing (open files) adds `stopping:true` and
/// `waiting_for` to the inspect shape (§7.3).
fn stop(ctx: &Arc<Ctx>, state: &str, wait: bool) -> Result<Value> {
    apply(
        ctx,
        CatalogCmd::SetDesired {
            state: state.to_owned(),
            desired: false,
        },
    )?;
    let command = FolderCmd::Stop {
        mode: StopMode::Graceful,
    };
    let view = if wait {
        let view = ctx
            .folders
            .request(state, command, STOP_REPLY_WAIT)
            .map_err(ErrorWire::into_anyhow)?;
        newest(ctx, state, view)
    } else {
        ctx.folders
            .send(state, command)
            .map_err(ErrorWire::into_anyhow)?;
        ctx.folders
            .view(state)
            .with_context(|| format!("FOLDER_NOT_FOUND: no folder with state {state}"))?
    };
    let mut value = inspect_view(ctx, state, &view)?;
    let waiting_for = match &view.phase {
        FolderPhase::StopBlocked { reason, .. } => Some(reason.to_string()),
        FolderPhase::Stopping { .. } => Some("the worker to stop".to_owned()),
        FolderPhase::Running { .. } | FolderPhase::Starting { .. } if !wait => {
            Some("the worker to stop".to_owned())
        }
        _ => None,
    };
    if let Some(waiting_for) = waiting_for {
        value["stopping"] = json!(true);
        value["waiting_for"] = json!(waiting_for);
    }
    Ok(value)
}
