//! Owned by WP2.2.
//!
//! Management API v2 actions (REVAMP_PLAN §3.5, §7.2, §14 WP2.2): `status`,
//! `subscribe`, `attach`, `detach`, `pause-sync`, `resume-sync`,
//! `override-gate`, `force-stop`, `retry`, `revalidate` and `diagnostics`.
//! WP2.1's `manage::manage` forwards these variants to [`handle`].
//!
//! * **Reads** (`status`, `subscribe`) come from the StatusHub's snapshot and
//!   never wait on a folder, the catalog or a worker (invariant 30).
//! * **Writes** make the intent durable first (`CatalogCmd`), then tell the
//!   folder's controller (`FolderCmd`) and wait for it to settle, bounded per
//!   action (invariant 26: `desired=0` is persisted before a force stop).
//! * Each folder action returns `{state, status}`, where `status` is the
//!   folder's [`FolderStatus`] from a hub snapshot that already reflects the
//!   controller's answer (read-your-writes, waited for at most
//!   [`STATUS_SETTLE`]); it is `null` only when the hub does not know the
//!   folder. `revalidate` adds `requeued`.
//! * Every folder action is recorded as an `action` activity entry, so a
//!   `watch` shows who did what.
//!
//! This module also re-exports the client side of the API from the private
//! `api` module ([`ControlClient`], [`format_status`], [`format_folder`],
//! [`default_deadline`], [`is_read_only`]); see CCR-WP2.2-1.
use super::{api::Action, diagnostics};
use crate::model::{
    errors::ErrorWire,
    ipc::{RevalidateReply, RevalidateRequest, ops},
    orchestrator::{CatalogCmd, Ctx, FolderCmd, FolderRecord, FolderView, GateState, limits},
    status::{ActivityEntry, FolderStatus, Intents, StatusEvent, policy},
};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

pub use super::api::{ControlClient, default_deadline, format_folder, format_status, is_read_only};

/// How long a folder command that settles at once (`SetSync`,
/// `OverrideGate`, `Retry`) may take to answer.
pub const QUICK_WAIT: Duration = Duration::from_secs(5);
/// The worker `revalidate` RPC deadline (it takes the engine lock and runs a
/// bounded activation pass).
pub const REVALIDATE_DEADLINE: Duration = Duration::from_secs(30);
/// How long a folder action waits for the hub to publish a snapshot that
/// reflects the controller's answer.
pub const STATUS_SETTLE: Duration = Duration::from_secs(1);

/// Serves one Management API v2 action. Legacy actions are answered with
/// `UNSUPPORTED_OPERATION` (they belong to `manage::manage`).
pub fn handle(ctx: &Arc<Ctx>, action: Action) -> Result<Value> {
    match action {
        Action::Status => Ok(serde_json::to_value(&*ctx.status_source.snapshot())?),
        Action::Subscribe { since, wait_ms } => {
            let wait = wait_ms
                .unwrap_or(policy::SUBSCRIBE_MAX_WAIT_MS)
                .min(policy::SUBSCRIBE_MAX_WAIT_MS);
            let snapshot = ctx
                .status_source
                .wait_newer(since, Duration::from_millis(u64::from(wait)));
            Ok(serde_json::to_value(&*snapshot)?)
        }
        Action::Diagnostics => diagnostics::collect(ctx),
        Action::Attach { state } => {
            let folder = Folder::write(ctx, &state, "attach")?;
            let view = folder.request(FolderCmd::Attach, limits::ATTACH_DEADLINE)?;
            Ok(folder.reply(&view, Check::Mount))
        }
        Action::Detach { state, force } => {
            let op = if force { "detach --force" } else { "detach" };
            let folder = Folder::write(ctx, &state, op)?;
            let view = folder.request(FolderCmd::Detach { force }, limits::ATTACH_DEADLINE)?;
            Ok(folder.reply(&view, Check::Mount))
        }
        Action::PauseSync { state } => set_sync(ctx, &state, false),
        Action::ResumeSync { state } => set_sync(ctx, &state, true),
        Action::OverrideGate { state } => {
            let folder = Folder::write(ctx, &state, "override-gate")?;
            // A gateless folder has nothing to override in the catalog (the
            // command would be a silent no-op there); the controller still
            // re-arms a held attach ("Mount now").
            let gated = ctx
                .catalog
                .snapshot()
                .gates
                .get(&state)
                .is_some_and(|gate| gate.override_ms.is_none());
            if gated {
                apply(
                    ctx,
                    CatalogCmd::OverrideGate {
                        state: state.clone(),
                    },
                )?;
            }
            let view = folder.request(FolderCmd::OverrideGate, QUICK_WAIT)?;
            Ok(folder.reply(&view, Check::Lifecycle))
        }
        Action::ForceStop { state } => {
            let folder = Folder::write(ctx, &state, "force-stop")?;
            // Invariant 26: the intent is durable before anything is stopped.
            if folder.record.desired_running {
                apply(
                    ctx,
                    CatalogCmd::SetDesired {
                        state: state.clone(),
                        desired: false,
                    },
                )?;
            }
            let view = folder.request(FolderCmd::ForceStop, limits::API_STOP_WAIT)?;
            Ok(folder.reply(&view, Check::Lifecycle))
        }
        Action::Retry { state } => {
            let folder = Folder::write(ctx, &state, "retry")?;
            let view = folder.request(FolderCmd::Retry, QUICK_WAIT)?;
            Ok(folder.reply(&view, Check::Lifecycle))
        }
        Action::Revalidate { state, ids } => revalidate(ctx, &state, ids),
        legacy => {
            let op = serde_json::to_value(&legacy)?["op"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            bail!("UNSUPPORTED_OPERATION: {op} is not a management v2 action")
        }
    }
}

fn set_sync(ctx: &Arc<Ctx>, state: &str, enabled: bool) -> Result<Value> {
    let folder = Folder::write(
        ctx,
        state,
        if enabled { "resume-sync" } else { "pause-sync" },
    )?;
    if folder.record.sync_enabled != enabled {
        apply(
            ctx,
            CatalogCmd::SetIntents {
                state: state.to_owned(),
                sync_enabled: Some(enabled),
                mount_enabled: None,
            },
        )?;
    }
    let view = folder.request(FolderCmd::SetSync { enabled }, QUICK_WAIT)?;
    Ok(folder.reply(&view, Check::Lifecycle))
}

/// Worker `revalidate` (it re-queues and answers how many), then, while the
/// initial-sync gate is Blocked, `FolderCmd::Revalidate` with no ids so the
/// controller reopens the gate without re-queuing the rows twice.
fn revalidate(ctx: &Arc<Ctx>, state: &str, ids: Option<Vec<String>>) -> Result<Value> {
    let folder = Folder::write(ctx, state, "revalidate")?;
    let client = ctx.folders.client(state).ok_or_else(|| {
        ErrorWire::new(
            "WORKER_NOT_RUNNING",
            format!("WORKER_NOT_RUNNING: {state} has no running worker to revalidate"),
        )
        .into_anyhow()
    })?;
    let reply = client
        .call(
            ops::REVALIDATE,
            serde_json::to_value(RevalidateRequest { ids })?,
            REVALIDATE_DEADLINE,
        )
        .map_err(anyhow::Error::new)?;
    let RevalidateReply { requeued } =
        serde_json::from_value(reply).context("INVALID_REPLY: worker revalidate")?;
    let view = match ctx.folders.view(state) {
        Some(view) if matches!(view.gate, GateState::Blocked { .. }) => folder.request(
            FolderCmd::Revalidate {
                ids: Some(Vec::new()),
            },
            REVALIDATE_DEADLINE,
        )?,
        Some(view) => view,
        None => return Err(not_found(state)),
    };
    let mut value = folder.reply(&view, Check::Lifecycle);
    value["requeued"] = json!(requeued);
    Ok(value)
}

/// A folder a write action is about to change.
struct Folder<'a> {
    ctx: &'a Arc<Ctx>,
    state: &'a str,
    record: FolderRecord,
}

/// What a reply's status must already show (besides the intents and the
/// gate, which every action checks).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Check {
    Lifecycle,
    /// The lifecycle and the mount state (attach, detach).
    Mount,
}

impl<'a> Folder<'a> {
    /// Checks that the daemon is not stopping and the folder exists, and
    /// records the request in the activity ring.
    fn write(ctx: &'a Arc<Ctx>, state: &'a str, op: &str) -> Result<Self> {
        if ctx.stopping.load(Ordering::Acquire) {
            bail!("ORCHESTRATOR_STOPPING: the daemon is shutting down; {op} was not run");
        }
        let record = ctx
            .catalog
            .snapshot()
            .folder(state)
            .cloned()
            .ok_or_else(|| not_found(state))?;
        ctx.status.record(StatusEvent::Activity(ActivityEntry {
            at_ms: ctx.clock.now_ms(),
            kind: "action".into(),
            folder: Some(state.to_owned()),
            peer: None,
            message: format!("{op} requested"),
        }));
        Ok(Self { ctx, state, record })
    }

    fn request(&self, cmd: FolderCmd, wait: Duration) -> Result<Arc<FolderView>> {
        self.ctx
            .folders
            .request(self.state, cmd, wait)
            .map_err(ErrorWire::into_anyhow)
    }

    /// `{state, status}` for the view the controller answered with.
    fn reply(&self, view: &FolderView, check: Check) -> Value {
        json!({
            "state": self.state,
            "status": settled_status(self.ctx, self.state, view, check),
        })
    }
}

fn apply(ctx: &Ctx, cmd: CatalogCmd) -> Result<()> {
    ctx.catalog
        .apply(cmd)
        .map(|_| ())
        .map_err(ErrorWire::into_anyhow)
}

fn not_found(state: &str) -> anyhow::Error {
    ErrorWire::new(
        "FOLDER_NOT_FOUND",
        format!("FOLDER_NOT_FOUND: no folder with state {state}"),
    )
    .into_anyhow()
}

/// The folder's status from the first hub snapshot that reflects `view`
/// (the controller publishes the view, and records it to the hub, before it
/// answers), waiting at most [`STATUS_SETTLE`]; then the latest one.
fn settled_status(ctx: &Ctx, state: &str, view: &FolderView, check: Check) -> Option<FolderStatus> {
    let deadline = Instant::now() + STATUS_SETTLE;
    let mut snapshot = ctx.status_source.snapshot();
    loop {
        let folder = snapshot.folders.iter().find(|folder| folder.state == state);
        if folder.is_some_and(|folder| reflects(folder, view, check)) {
            return folder.cloned();
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return folder.cloned();
        }
        snapshot = ctx.status_source.wait_newer(snapshot.generation, remaining);
    }
}

/// Whether a hub folder status shows what `view` says.
fn reflects(status: &FolderStatus, view: &FolderView, check: Check) -> bool {
    let intents = Intents {
        desired_running: view.record.desired_running,
        sync_enabled: view.record.sync_enabled,
        mount_enabled: view.record.mount_enabled,
    };
    status.lifecycle.phase == view.phase.lifecycle()
        && status.intents == intents
        && status.gate.state == view.gate.kind()
        && (check != Check::Mount || status.mount.state.name() == view.mount.state.name())
}
