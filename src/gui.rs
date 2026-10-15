//! Owned by WP3.4.
//!
//! The desktop's Slint glue (REVAMP_PLAN §7.4, §14 WP3.4). Every text and
//! state shown is computed by `tkfs::ui_model` from the daemon's status
//! snapshot; this file maps those models onto `ui/app.slint`.
//!
//! The UI thread never does I/O, so a slow or dead daemon never freezes the
//! window:
//! * one subscription thread (`ui_model::Feed` over a `ControlClient`) long
//!   polls `subscribe(since, 10 s)` with a 15 s deadline, reconnects with
//!   backoff, and starts the daemon (its output in
//!   `DATA/logs/orchestrator.log`) when no daemon answers, except one shut
//!   down on purpose (from this window or the CLI) until the user asks for
//!   one or another daemon answers;
//! * commands run on a pool of threads, each with its own `ControlClient`
//!   (`ui_model::CommandPool`), so they never wait behind the subscription
//!   or behind each other; each row shows its own busy state.
//!
//! `TKFS_GUI_RENDER=<file.bmp>` renders one frame without a window (the
//! software renderer) and exits: with `TKFS_GUI_RENDER_SNAPSHOT=<file.json>`
//! from a saved `StatusSnapshot`, else from the running daemon's `Status`;
//! `TKFS_GUI_RENDER_VIEW` picks `folders` (default), `activity`,
//! `folder:<state>[:<tab>]` or `peer:<installation>`. It is how the manual
//! checklist's screenshots are produced.
use anyhow::{Context, Result};
use serde_json::{Value, json};
use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    process::{Command as Process, Stdio},
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};
use tkfs::{
    model::status::StatusSnapshot,
    orchestrator::{
        Action, Config,
        manage_v2::{ControlClient, default_deadline},
    },
    ui_model::{
        self, ActionRoute, Busy, CommandOutcome, CommandPool, CommandTransport, DaemonStarter,
        Feed, FeedDown, FeedOptions, FeedUpdate, FolderAction, MountSmoother, Now, RateMeter,
        StatusTransport, Tone, TransportError,
    },
    util::time::now_ms,
};
slint::include_modules!();

/// Command threads: enough that a 15 s Stop never delays another row.
const COMMAND_THREADS: usize = 4;
/// The busy key of dialog commands (create, pair, receive, …).
const GLOBAL: &str = "global";

// ---------------------------------------------------------------------------
// Transports

/// The subscription connection.
struct ControlStatus {
    config: Config,
    client: Option<ControlClient>,
}

impl StatusTransport for ControlStatus {
    fn subscribe(&mut self, since: u64, wait: Duration) -> Result<StatusSnapshot, TransportError> {
        if self.client.is_none() {
            let client = ControlClient::connect(&self.config)
                .map_err(|e| TransportError::DaemonGone(format!("{e:#}")))?;
            self.client = Some(client);
        }
        let client = self.client.as_mut().expect("connected above");
        let action = Action::Subscribe {
            since,
            wait_ms: Some(u32::try_from(wait.as_millis()).unwrap_or(u32::MAX)),
        };
        // §14 WP3.4: a 10 s long poll with a 15 s deadline.
        let value = client
            .call(&action, wait + Duration::from_secs(5))
            .map_err(|e| TransportError::Failed(format!("{e:#}")))?;
        serde_json::from_value(value)
            .map_err(|e| TransportError::Failed(format!("INVALID_REPLY: status snapshot: {e}")))
    }

    fn disconnect(&mut self) {
        if let Some(mut client) = self.client.take() {
            client.disconnect();
        }
    }
}

/// One command connection (one per pool thread).
struct ControlCommands {
    config: Config,
    client: Option<ControlClient>,
}

impl CommandTransport for ControlCommands {
    fn call(&mut self, action: &Action) -> Result<Value, String> {
        if self.client.is_none() {
            self.client = Some(ControlClient::connect(&self.config).map_err(|e| format!("{e:#}"))?);
        }
        let client = self.client.as_mut().expect("connected above");
        client
            .call(action, default_deadline(action))
            .map_err(|e| format!("{e:#}"))
    }
}

/// Starts `tkfs -f CONFIG orchestrator` with its output appended to
/// `DATA/logs/orchestrator.log`.
struct Spawner {
    config_path: PathBuf,
    log: PathBuf,
}

impl DaemonStarter for Spawner {
    fn start(&mut self) -> Result<String, String> {
        let spawn = || -> Result<()> {
            if let Some(parent) = self.log.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.log)?;
            let err = log.try_clone()?;
            let mut command = Process::new(std::env::current_exe()?);
            command
                .arg("-f")
                .arg(&self.config_path)
                .arg("orchestrator")
                .stdin(Stdio::null())
                .stdout(log)
                .stderr(err);
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
            command.spawn().context("starting the daemon")?;
            Ok(())
        };
        spawn().map_err(|e| format!("{e:#}"))?;
        Ok(self.log.display().to_string())
    }
}

fn daemon_log(config: &Config) -> PathBuf {
    config.data_directory.join("logs").join("orchestrator.log")
}

// ---------------------------------------------------------------------------
// State (UI thread only)

struct State {
    config: Config,
    feed: Option<Feed>,
    pool: Option<CommandPool>,
    snapshot: Option<Arc<StatusSnapshot>>,
    down: Option<FeedDown>,
    /// The user shut the daemon down from this window.
    shut_down: bool,
    smoother: MountSmoother,
    rates: RateMeter,
    busy: Busy,
    next_id: u64,
    utc_offset_min: i32,
    /// A fixed "now" (render-to-file), else the local clock.
    fixed_now: Option<u64>,
    /// The worker `status` reply of the folder whose Content tab is open.
    worker_status: Option<(String, Result<Value, String>)>,
    /// The worker `folder-details` reply of the open Conflicts tab.
    details: Value,
    folders: Rc<VecModel<FolderRow>>,
    peers: Rc<VecModel<PeerRow>>,
    activity: Rc<VecModel<ActivityRow>>,
    daemon_errors: Rc<VecModel<ErrorRow>>,
    detail_overview: Rc<VecModel<KV>>,
    detail_waiting: Rc<VecModel<SharedString>>,
    detail_peers: Rc<VecModel<PeerSyncRow>>,
    detail_activation: Rc<VecModel<KV>>,
    detail_content: Rc<VecModel<KV>>,
    detail_missing: Rc<VecModel<MissingRow>>,
    detail_errors: Rc<VecModel<ErrorRow>>,
    detail_activity: Rc<VecModel<ActivityRow>>,
    detail_actions: Rc<VecModel<ActionData>>,
    peer_lanes: Rc<VecModel<LaneRow>>,
}

thread_local! {
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

fn with_state(f: impl FnOnce(&mut State)) {
    STATE.with(|state| {
        if let Some(state) = state.borrow_mut().as_mut() {
            f(state);
        }
    });
}

impl State {
    fn new(config: Config) -> Self {
        Self {
            config,
            feed: None,
            pool: None,
            snapshot: None,
            down: None,
            shut_down: false,
            smoother: MountSmoother::default(),
            rates: RateMeter::default(),
            busy: Busy::default(),
            next_id: 0,
            utc_offset_min: utc_offset_min(),
            fixed_now: None,
            worker_status: None,
            details: Value::Null,
            folders: Rc::new(VecModel::default()),
            peers: Rc::new(VecModel::default()),
            activity: Rc::new(VecModel::default()),
            daemon_errors: Rc::new(VecModel::default()),
            detail_overview: Rc::new(VecModel::default()),
            detail_waiting: Rc::new(VecModel::default()),
            detail_peers: Rc::new(VecModel::default()),
            detail_activation: Rc::new(VecModel::default()),
            detail_content: Rc::new(VecModel::default()),
            detail_missing: Rc::new(VecModel::default()),
            detail_errors: Rc::new(VecModel::default()),
            detail_activity: Rc::new(VecModel::default()),
            detail_actions: Rc::new(VecModel::default()),
            peer_lanes: Rc::new(VecModel::default()),
        }
    }

    fn bind(&self, ui: &AppWindow) {
        ui.set_folders(ModelRc::from(self.folders.clone()));
        ui.set_peers(ModelRc::from(self.peers.clone()));
        ui.set_activity(ModelRc::from(self.activity.clone()));
        ui.set_daemon_errors(ModelRc::from(self.daemon_errors.clone()));
        ui.set_detail_overview(ModelRc::from(self.detail_overview.clone()));
        ui.set_detail_waiting_on(ModelRc::from(self.detail_waiting.clone()));
        ui.set_detail_peers(ModelRc::from(self.detail_peers.clone()));
        ui.set_detail_activation(ModelRc::from(self.detail_activation.clone()));
        ui.set_detail_content(ModelRc::from(self.detail_content.clone()));
        ui.set_detail_missing(ModelRc::from(self.detail_missing.clone()));
        ui.set_detail_errors(ModelRc::from(self.detail_errors.clone()));
        ui.set_detail_activity(ModelRc::from(self.detail_activity.clone()));
        ui.set_detail_actions(ModelRc::from(self.detail_actions.clone()));
        ui.set_peer_lanes(ModelRc::from(self.peer_lanes.clone()));
    }

    /// "Now" for relative times: the local clock (the daemon runs on this
    /// machine), never before the snapshot.
    fn now(&self) -> Now {
        let at = self.snapshot.as_ref().map_or(0, |s| s.at_ms);
        let now = self.fixed_now.unwrap_or_else(now_ms);
        Now::at(now.max(at)).with_offset(self.utc_offset_min)
    }

    /// Queues `action` under busy `key`; `false` when the key is busy.
    fn submit(&mut self, ui: &AppWindow, key: &str, label: &str, action: Action) -> bool {
        self.next_id += 1;
        let id = self.next_id;
        if !self.busy.begin(key, id, label) {
            return false;
        }
        let Some(pool) = &self.pool else {
            self.busy.finish(key, id);
            return false;
        };
        if pool
            .submit(ui_model::Command {
                id,
                key: key.to_owned(),
                action,
            })
            .is_err()
        {
            self.busy.finish(key, id);
            set_status(
                ui,
                true,
                "The command threads have stopped. Reopen the window.".into(),
            );
            return false;
        }
        if key != GLOBAL && !key.starts_with("detail:") {
            self.smoother.release(key);
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Model mapping

fn chip(c: &ui_model::Chip) -> Chip {
    Chip {
        text: c.text.as_str().into(),
        tone: c.tone.as_str().into(),
        action: c.action.map_or("", FolderAction::id).into(),
        action_label: c.action_label.as_str().into(),
    }
}

fn error_meta(e: &ui_model::ErrorLine) -> String {
    let mut parts = Vec::new();
    if let Some(scope) = &e.scope {
        parts.push(scope.clone());
    }
    parts.push(e.when.clone());
    if e.count > 1 {
        parts.push(match &e.first {
            Some(first) => format!("{}x since {first}", ui_model::group(e.count)),
            None => format!("{}x", ui_model::group(e.count)),
        });
    }
    if let Some(retry) = &e.next_retry {
        parts.push(retry.clone());
    }
    if let Some(resolved) = &e.resolved {
        parts.push(resolved.clone());
    }
    parts.join(" · ")
}

fn error_title(e: &ui_model::ErrorLine) -> String {
    format!("[{}] {}", e.class, e.message)
}

fn error_row(e: &ui_model::ErrorLine) -> ErrorRow {
    ErrorRow {
        title: error_title(e).into(),
        meta: error_meta(e).into(),
        hint: e.suggestion.as_str().into(),
        tone: e.tone.as_str().into(),
        open: e.open,
    }
}

/// A row's chip, without a remedy another button of the row already offers.
fn row_chip(c: &ui_model::Chip, offered: &[Option<FolderAction>]) -> Chip {
    let mut out = chip(c);
    if c.action.is_some() && offered.contains(&c.action) {
        out.action = SharedString::default();
        out.action_label = SharedString::default();
    }
    out
}

fn folder_row(r: &ui_model::FolderRowModel, busy: &Busy) -> FolderRow {
    let (error_title_text, error_meta_text, hint, tone) = match &r.error {
        Some(e) => (error_title(e), error_meta(e), e.suggestion.clone(), e.tone),
        None => (String::new(), String::new(), String::new(), Tone::Neutral),
    };
    FolderRow {
        state: r.state.as_str().into(),
        label: r.label.as_str().into(),
        mount: r.mount_path.as_str().into(),
        // The sync chip's remedy is often the mount's or the worker's (a
        // failed mount, a failed worker): one button is enough.
        sync: row_chip(&r.sync, &[r.primary, r.mount.action, r.lifecycle.action]),
        detail: r.sync_detail.as_str().into(),
        progress: r.progress.unwrap_or(0.0),
        has_progress: r.progress.is_some(),
        mount_chip: row_chip(&r.mount, &[r.primary]),
        life: row_chip(&r.lifecycle, &[r.primary]),
        conflicts: i32::try_from(r.conflicts).unwrap_or(i32::MAX),
        error_title: error_title_text.into(),
        error_meta: error_meta_text.into(),
        error_hint: hint.into(),
        error_tone: tone.as_str().into(),
        attention: r.attention,
        busy: busy.label(&r.state).unwrap_or_default().into(),
        primary: r.primary.map_or("", FolderAction::id).into(),
        primary_label: r.primary.map_or("", FolderAction::label).into(),
        running: r.running,
    }
}

fn peer_row(p: &ui_model::PeerRowModel) -> PeerRow {
    let (title, meta, hint) = match &p.error {
        Some(e) => (error_title(e), error_meta(e), e.suggestion.clone()),
        None => Default::default(),
    };
    PeerRow {
        installation: p.installation.as_str().into(),
        name: p.name.as_str().into(),
        endpoint: p.endpoint.as_str().into(),
        badge: chip(&p.badge),
        direction: p.direction.as_str().into(),
        inbound_only: p.inbound_only,
        rtt: p.rtt.as_str().into(),
        rates: p.rates.as_str().into(),
        protocol: p.protocol.as_str().into(),
        lanes_summary: p.lanes_summary.as_str().into(),
        folders: p.shared_folders.join(", ").into(),
        error_title: title.into(),
        error_meta: meta.into(),
        error_hint: hint.into(),
        online: p.online,
    }
}

fn lane_row(l: &ui_model::LaneRowModel) -> LaneRow {
    LaneRow {
        lane: l.lane.to_string().into(),
        role: l.role.as_str().into(),
        state: chip(&l.state),
        inflight: l.inflight.as_str().into(),
        since: l.since.as_str().into(),
        bytes: l.bytes.as_str().into(),
        rate: if l.reconnects > 0 {
            format!(
                "{}{}reconnected {}",
                l.rate,
                if l.rate.is_empty() { "" } else { " · " },
                ui_model::count(u64::from(l.reconnects), "time", "times")
            )
        } else {
            l.rate.clone()
        }
        .into(),
        error: l
            .error
            .as_ref()
            .map(|e| format!("{} · {}", error_title(e), error_meta(e)))
            .unwrap_or_default()
            .into(),
    }
}

fn kv(k: &ui_model::KeyValue) -> KV {
    KV {
        key: k.key.as_str().into(),
        value: k.value.as_str().into(),
        tone: k.tone.as_str().into(),
    }
}

fn activity_row(a: &ui_model::ActivityRowModel) -> ActivityRow {
    ActivityRow {
        time: a.time.as_str().into(),
        ago: a.ago.as_str().into(),
        kind: a.kind.as_str().into(),
        subject: a.subject.as_str().into(),
        message: a.message.as_str().into(),
        tone: a.tone.as_str().into(),
    }
}

fn peer_sync_row(p: &ui_model::FolderPeerRowModel) -> PeerSyncRow {
    let counts = [
        (p.to_send, "to send"),
        (p.to_receive, "to receive"),
        (p.local_pending, "local pending"),
        (p.remote_pending, "remote pending"),
        (p.blocked, "blocked"),
        (p.quarantined, "quarantined"),
        (p.rejected, "rejected"),
    ]
    .iter()
    .map(|(n, what)| format!("{} {what}", ui_model::group(*n)))
    .collect::<Vec<_>>()
    .join(" · ");
    let (title, meta, hint, tone) = match &p.error {
        Some(e) => (error_title(e), error_meta(e), e.suggestion.clone(), e.tone),
        None => (String::new(), String::new(), String::new(), Tone::Neutral),
    };
    PeerSyncRow {
        name: p.name.as_str().into(),
        direction: p.direction.as_str().into(),
        state: chip(&p.state),
        meta: p.metadata.as_str().into(),
        counts: counts.into(),
        down: p.down.as_str().into(),
        up: p.up.as_str().into(),
        progress: format!(
            "last progress {} · last exchange {}",
            p.last_progress, p.last_exchange
        )
        .into(),
        error_title: title.into(),
        error_meta: meta.into(),
        error_hint: hint.into(),
        error_tone: tone.as_str().into(),
    }
}

/// Updates `model` in place (rows that did not change are not touched, so
/// scroll positions and focus survive a refresh).
fn sync_model<T: Clone + PartialEq + 'static>(model: &VecModel<T>, items: Vec<T>) {
    if model.row_count() != items.len() {
        model.set_vec(items);
        return;
    }
    for (index, item) in items.into_iter().enumerate() {
        if model.row_data(index).as_ref() != Some(&item) {
            model.set_row_data(index, item);
        }
    }
}

fn set_status(ui: &AppWindow, error: bool, text: String) {
    ui.set_status_error(error);
    ui.set_status(text.into());
}

/// Shows `dialog` (`""` closes it). Invariant 21: the one-time pairing code
/// lives only in the host dialog that asked for it, so going anywhere else
/// clears it (the window's `changed dialog` handler is the backstop).
fn show_dialog(ui: &AppWindow, dialog: &str) {
    if dialog != "host" {
        ui.set_pairing_code(SharedString::default());
    }
    ui.set_dialog(dialog.into());
}

// ---------------------------------------------------------------------------
// Rendering

fn render(ui: &AppWindow, st: &mut State) {
    let now = st.now();
    render_banner(ui, st, now);
    ui.set_global_busy(st.busy.label(GLOBAL).unwrap_or_default().into());
    let Some(raw) = st.snapshot.clone() else {
        ui.set_connected(false);
        return;
    };
    ui.set_connected(st.down.is_none() && !raw.daemon.stopping);
    let mut snap = (*raw).clone();
    st.smoother.smooth(&mut snap, now.ms);

    let daemon = ui_model::daemon_model(&snap, now);
    ui.set_daemon_title(daemon.title.as_str().into());
    ui.set_daemon_version(snap.daemon.version.as_str().into());
    ui.set_daemon_status(match &st.down {
        Some(down) if st.shut_down || down.deliberate => {
            chip(&ui_model::Chip::new("Shut down", Tone::Muted))
        }
        Some(_) => chip(&ui_model::Chip::new("Not responding", Tone::Error)),
        None => chip(&daemon.status),
    });
    ui.set_daemon_installation(daemon.installation.as_str().into());
    ui.set_daemon_network(daemon.network.as_str().into());
    ui.set_daemon_control(daemon.control.as_str().into());
    sync_model(
        &st.daemon_errors,
        daemon.errors.iter().map(error_row).collect(),
    );

    let o = ui_model::overview(&snap);
    ui.set_folder_count(i32::try_from(o.folders).unwrap_or(i32::MAX));
    ui.set_stat_folders(format!("{} / {}", o.up_to_date, o.folders).into());
    ui.set_stat_folders_detail(
        if o.attention > 0 {
            format!("{} syncing · {} need attention", o.syncing, o.attention)
        } else if o.syncing > 0 {
            format!("{} syncing", o.syncing)
        } else if o.folders == 0 {
            "No folders yet".to_owned()
        } else {
            "Nothing needs attention".to_owned()
        }
        .into(),
    );
    ui.set_stat_folders_tone(
        if o.attention > 0 {
            "error"
        } else if o.syncing > 0 {
            "progress"
        } else if o.folders > 0 && o.up_to_date == o.folders {
            "ok"
        } else {
            "neutral"
        }
        .into(),
    );
    ui.set_stat_mounted(format!("{} / {}", o.mounted, o.folders).into());
    ui.set_stat_mounted_detail(
        if o.conflicts > 0 {
            ui_model::count(o.conflicts, "unresolved conflict", "unresolved conflicts")
        } else {
            "No unresolved conflicts".to_owned()
        }
        .into(),
    );
    ui.set_stat_peers(format!("{} / {}", o.peers_online, o.peers).into());
    ui.set_stat_peers_detail(
        if o.inbound_only > 0 {
            format!("{} inbound only", o.inbound_only)
        } else {
            "Paired hosts".to_owned()
        }
        .into(),
    );
    if o.rate_down_bps > 0 || o.rate_up_bps > 0 {
        ui.set_stat_transfer(format!("↓ {}", ui_model::format_rate(o.rate_down_bps)).into());
        ui.set_stat_transfer_detail(format!("↑ {}", ui_model::format_rate(o.rate_up_bps)).into());
    } else {
        ui.set_stat_transfer("Idle".into());
        ui.set_stat_transfer_detail("No transfer in progress".into());
    }

    let rows = ui_model::folder_rows_at(&snap, now);
    let shown = ui_model::filter_rows(&rows, &ui.get_search());
    sync_model(
        &st.folders,
        shown.iter().map(|r| folder_row(r, &st.busy)).collect(),
    );
    let peers = ui_model::peer_rows_at(&snap, now);
    sync_model(&st.peers, peers.iter().map(peer_row).collect());
    if ui.get_view() == "activity" {
        sync_model(
            &st.activity,
            ui_model::activity_rows_at(&snap, now, None, 500)
                .iter()
                .map(activity_row)
                .collect(),
        );
    }

    match ui.get_dialog().as_str() {
        "folder" => render_folder(ui, st, &snap, now),
        "peer" => {
            let selected = ui.get_selected_peer();
            if let Some(p) = peers.iter().find(|p| p.installation == selected.as_str()) {
                let row = peer_row(p);
                if ui.get_peer_detail() != row {
                    ui.set_peer_detail(row);
                }
                sync_model(&st.peer_lanes, p.lanes.iter().map(lane_row).collect());
            }
        }
        _ => {}
    }
}

fn render_banner(ui: &AppWindow, st: &State, now: Now) {
    let deliberate = st.down.as_ref().is_some_and(|down| down.deliberate);
    let (text, tone, action, label) = if (st.shut_down || deliberate)
        && st.down.as_ref().is_some_and(|down| down.started.is_none())
    {
        (
            "The daemon is shut down: folders are not syncing and their mounts are gone."
                .to_owned(),
            "warn",
            "start-daemon",
            "Start the daemon",
        )
    } else if let Some(down) = &st.down {
        let stale = st
            .snapshot
            .as_ref()
            .map(|s| format!(" Showing the status from {}.", ui_model::ago(now, s.at_ms)))
            .unwrap_or_default();
        if let Some(log) = &down.started {
            (
                format!("Starting the daemon (its output goes to {log})…{stale}"),
                "progress",
                "",
                "",
            )
        } else if let Some(error) = &down.start_error {
            (
                format!(
                    "Cannot start the daemon: {}.{stale}",
                    ui_model::clip(error, 160)
                ),
                "error",
                "start-daemon",
                "Try again",
            )
        } else if down.daemon_gone && down.respawn_off {
            (
                "The daemon is not running.".to_owned(),
                "warn",
                "start-daemon",
                "Start the daemon",
            )
        } else {
            (
                format!(
                    "No answer from the daemon (attempt {}): {} Retrying in {}.{stale}",
                    down.attempt,
                    ui_model::clip(&down.error, 160),
                    ui_model::format_duration(down.retry_in_ms)
                ),
                if down.attempt >= 3 { "error" } else { "warn" },
                "",
                "",
            )
        }
    } else if st.snapshot.is_none() {
        (
            "Connecting to the local daemon…".to_owned(),
            "progress",
            "",
            "",
        )
    } else if st.snapshot.as_ref().is_some_and(|s| s.daemon.stopping) {
        ("The daemon is shutting down.".to_owned(), "warn", "", "")
    } else {
        (String::new(), "neutral", "", "")
    };
    ui.set_banner_text(text.into());
    ui.set_banner_tone(tone.into());
    ui.set_banner_action(action.into());
    ui.set_banner_action_label(label.into());
}

fn render_folder(ui: &AppWindow, st: &mut State, snap: &StatusSnapshot, now: Now) {
    let state = ui.get_selected_state().to_string();
    let Some(d) = ui_model::folder_detail_at(snap, &state, now, st.rates.per_second(&state)) else {
        set_status(
            ui,
            true,
            "This folder is no longer listed by the daemon.".into(),
        );
        return;
    };
    let row = folder_row(&d.row, &st.busy);
    if ui.get_detail_row() != row {
        ui.set_detail_row(row);
    }
    sync_model(&st.detail_overview, d.overview.iter().map(kv).collect());
    sync_model(
        &st.detail_waiting,
        d.gate_waiting_on
            .iter()
            .map(|w| SharedString::from(w.as_str()))
            .collect(),
    );
    sync_model(
        &st.detail_peers,
        d.peers.iter().map(peer_sync_row).collect(),
    );
    sync_model(&st.detail_activation, d.activation.iter().map(kv).collect());
    sync_model(&st.detail_content, d.content.iter().map(kv).collect());
    sync_model(&st.detail_errors, d.errors.iter().map(error_row).collect());
    sync_model(
        &st.detail_activity,
        d.activity.iter().map(activity_row).collect(),
    );
    sync_model(
        &st.detail_actions,
        d.row
            .actions
            .iter()
            .map(|a| ActionData {
                id: a.id().into(),
                label: a.label().into(),
                enabled: true,
                danger: a.is_destructive(),
            })
            .collect(),
    );
    let (missing, note) = match &st.worker_status {
        Some((s, Ok(value))) if *s == state => {
            let items = ui_model::missing_items(value);
            let note = if items.is_empty() {
                "No missing files.".to_owned()
            } else {
                String::new()
            };
            (items, note)
        }
        Some((s, Err(error))) if *s == state => (
            Vec::new(),
            format!(
                "Could not read the worker's status: {}",
                ui_model::clip(error, 160)
            ),
        ),
        _ if !d.row.running => (
            Vec::new(),
            "The worker is not running; missing files are listed while it runs.".to_owned(),
        ),
        _ => (Vec::new(), "Loading…".to_owned()),
    };
    sync_model(
        &st.detail_missing,
        missing
            .iter()
            .map(|m| MissingRow {
                path: m.path.as_str().into(),
                state: m.state.as_str().into(),
                progress: m.progress.as_str().into(),
                error: m.error.as_str().into(),
            })
            .collect(),
    );
    ui.set_detail_missing_note(note.into());
}

// ---------------------------------------------------------------------------
// Events

fn on_feed(ui: &AppWindow, st: &mut State, update: FeedUpdate) {
    match update {
        FeedUpdate::Snapshot(snapshot) => {
            st.rates.observe(&snapshot);
            st.snapshot = Some(snapshot);
            st.down = None;
            st.shut_down = false;
        }
        FeedUpdate::Down(down) => st.down = Some(down),
    }
    render(ui, st);
}

fn folder_label(st: &State, state: &str) -> String {
    st.snapshot
        .as_ref()
        .and_then(|s| s.folders.iter().find(|f| f.state == state))
        .map(ui_model::folder_name)
        .unwrap_or_else(|| format!("folder {}", ui_model::short(state)))
}

fn on_folder_action(ui: &AppWindow, st: &mut State, state: &str, id: &str) {
    let Some(route) = ui_model::route_action(id) else {
        return;
    };
    let label = folder_label(st, state);
    match route {
        ActionRoute::ChangePath => {
            let mount = st
                .snapshot
                .as_ref()
                .and_then(|s| s.folders.iter().find(|f| f.state == state))
                .map(|f| {
                    (
                        f.mount.path.as_deref().map(ui_model::display_path),
                        f.lifecycle.phase != tkfs::model::status::LifecyclePhase::Stopped,
                    )
                });
            let (path, live) = mount.unwrap_or((None, false));
            ui.set_selected_state(state.into());
            ui.set_selected_label(label.into());
            ui.set_mount_path(path.unwrap_or_default().into());
            show_dialog(ui, if live { "stop-first" } else { "mount" });
        }
        ActionRoute::Confirm(action) => {
            ui.set_selected_state(state.into());
            ui.set_selected_label(label.as_str().into());
            ui.set_confirm_action(action.id().into());
            ui.set_confirm_text(
                format!(
                    "Force stop “{label}”? Unsaved files are persisted first, then the mount is \
                     detached even while files are open: programs using them may report errors."
                )
                .into(),
            );
            show_dialog(ui, "confirm");
        }
        ActionRoute::Send(action) => {
            let Some(command) = action.command(state) else {
                return;
            };
            if st.submit(ui, state, action.busy_label(), command) {
                set_status(ui, false, format!("{label}: {}…", action.busy_label()));
            }
        }
    }
    render(ui, st);
}

/// What a successful command reports.
fn done_text(st: &State, action: &Action, value: &Value) -> String {
    let folder = |state: &str| folder_label(st, state);
    match action {
        Action::Start { state } => format!("{}: start requested.", folder(state)),
        Action::Stop { state, .. } => {
            if value["stopping"] == true {
                format!(
                    "{}: still stopping ({}). Close its open files, or Force stop.",
                    folder(state),
                    value["waiting_for"].as_str().unwrap_or("files are open")
                )
            } else {
                format!("{}: stopped.", folder(state))
            }
        }
        Action::ForceStop { state } => format!("{}: force stopped.", folder(state)),
        Action::Attach { state } => format!("{}: mount requested.", folder(state)),
        Action::Detach { state, .. } => format!("{}: detached.", folder(state)),
        Action::PauseSync { state } => format!("{}: sync paused.", folder(state)),
        Action::ResumeSync { state } => format!("{}: sync resumed.", folder(state)),
        Action::OverrideGate { state } => format!(
            "{}: mounting now, before the initial sync finished.",
            folder(state)
        ),
        Action::Retry { state } => format!("{}: restarting.", folder(state)),
        Action::Revalidate { state, .. } => format!(
            "{}: {} re-queued for activation.",
            folder(state),
            ui_model::count(value["requeued"].as_u64().unwrap_or(0), "event", "events")
        ),
        Action::Create { label, .. } => format!("{label}: created; starting."),
        Action::Receive { label, .. } => format!("{label}: added; receiving its metadata."),
        Action::ConfigureMount { .. } => {
            "Mount point saved. Start the folder when you are ready.".into()
        }
        Action::Pair { .. } => "Paired. Loading the host's folders…".into(),
        Action::Unpair { .. } => "Disconnected. Local folders stay available.".into(),
        Action::RemoteList { .. } => "Remote folders loaded.".into(),
        Action::ResetPairingCode => {
            "New pairing code generated: hosts already paired must pair again.".into()
        }
        Action::Shutdown => "The daemon shut down.".into(),
        _ => "Done.".into(),
    }
}

fn on_outcome(ui: &AppWindow, st: &mut State, outcome: CommandOutcome) {
    let CommandOutcome {
        command, result, ..
    } = outcome;
    st.busy.finish(&command.key, command.id);
    let action = command.action;
    match result {
        Err(error) => {
            match &action {
                Action::RemoteList { .. } => {
                    ui.set_remote_folders(ModelRc::default());
                    ui.set_remote_message(
                        "Unable to list the host's folders. Check the address, and pair first."
                            .into(),
                    );
                }
                Action::Shutdown => {
                    if let Some(feed) = &st.feed {
                        feed.set_respawn(true);
                    }
                }
                Action::Folder { state, payload, .. } if payload["op"] == "status" => {
                    st.worker_status = Some((state.clone(), Err(error.clone())));
                }
                _ => {}
            }
            let what = match &action {
                Action::Folder { state, payload, .. } => format!(
                    "{} ({})",
                    folder_label(st, state),
                    payload["op"].as_str().unwrap_or("request")
                ),
                other => serde_json::to_value(other)
                    .ok()
                    .and_then(|v| v["op"].as_str().map(str::to_owned))
                    .unwrap_or_else(|| "request".into()),
            };
            set_status(ui, true, format!("{what} failed: {error}"));
        }
        Ok(value) => {
            let mut message = Some(done_text(st, &action, &value));
            match &action {
                Action::Folder { state, payload, .. } => {
                    message = None;
                    on_folder_reply(ui, st, state, payload, value);
                }
                Action::Create { .. } | Action::Receive { .. } | Action::ConfigureMount { .. } => {
                    show_dialog(ui, "");
                }
                Action::Stop { .. } => {
                    if ui.get_dialog() == "stop-first" {
                        show_dialog(ui, "mount");
                    }
                }
                Action::Pair { endpoint, .. } => {
                    ui.set_code("".into());
                    st.submit(
                        ui,
                        GLOBAL,
                        "Loading the host's folders",
                        Action::RemoteList {
                            endpoint: endpoint.clone(),
                        },
                    );
                }
                Action::Unpair { .. } => {
                    clear_remote(ui);
                    if ui.get_dialog() == "peer" {
                        show_dialog(ui, "");
                    }
                }
                Action::RemoteList { endpoint } => {
                    if endpoint.trim() == ui.get_endpoint().trim() {
                        ui.set_selected_repo("".into());
                        let remotes: Vec<RemoteRow> = value
                            .as_array()
                            .into_iter()
                            .flatten()
                            .map(|p| RemoteRow {
                                repo: p["repo"].as_str().unwrap_or_default().into(),
                                label: p["label"].as_str().unwrap_or_default().into(),
                            })
                            .collect();
                        ui.set_remote_message(
                            if remotes.is_empty() {
                                "This host shares no folders."
                            } else {
                                "Select a folder to receive on this host."
                            }
                            .into(),
                        );
                        ui.set_remote_folders(ModelRc::new(VecModel::from(remotes)));
                    }
                }
                Action::ResetPairingCode => {
                    // Invariant 21: the code is shown only here, once, and
                    // cleared when the window leaves the host dialog
                    // (`show_dialog`, `dismiss()`, `changed dialog`).
                    if ui.get_dialog() == "host" {
                        ui.set_pairing_code(value["code"].as_str().unwrap_or_default().into());
                    }
                }
                Action::Shutdown => {
                    st.shut_down = true;
                    show_dialog(ui, "");
                }
                _ => {}
            }
            if let Some(message) = message {
                set_status(ui, false, message);
            }
        }
    }
    render(ui, st);
}

fn on_folder_reply(ui: &AppWindow, st: &mut State, state: &str, payload: &Value, value: Value) {
    match payload["op"].as_str() {
        Some("status") => {
            st.worker_status = Some((state.to_owned(), Ok(value)));
        }
        Some("folder-details") => {
            if state != ui.get_selected_state().as_str() {
                return;
            }
            let count = value["conflict_count"].as_i64().unwrap_or(0);
            ui.set_conflict_count(i32::try_from(count).unwrap_or(i32::MAX));
            ui.set_conflict_offset(
                i32::try_from(value["offset"].as_i64().unwrap_or(0)).unwrap_or(0),
            );
            ui.set_conflict_summary(
                if count == 0 {
                    "No unresolved conflicts.".to_owned()
                } else {
                    format!(
                        "{}: pick one to review its alternatives.",
                        ui_model::count(
                            count as u64,
                            "unresolved conflict",
                            "unresolved conflicts"
                        )
                    )
                }
                .into(),
            );
            let conflicts: Vec<ConflictRow> = value["conflicts"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|c| c["resolved_by"].is_null())
                .flat_map(|c| {
                    c["entity"]
                        .as_str()
                        .unwrap_or_default()
                        .split(',')
                        .map(|entity| ConflictRow {
                            id: c["id"].as_str().unwrap_or_default().into(),
                            entity: entity.into(),
                            field: c["field"].as_str().unwrap_or_default().into(),
                            description: format!(
                                "{} · {}",
                                value["paths"][entity].as_str().unwrap_or(entity),
                                c["field"].as_str().unwrap_or_default()
                            )
                            .into(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            ui.set_conflicts(ModelRc::new(VecModel::from(conflicts)));
            ui.set_selected_conflict("".into());
            ui.set_selected_revision("".into());
            ui.set_revisions(ModelRc::default());
            ui.set_conflict_preview("Select a conflict to review its alternatives.".into());
            st.details = value;
        }
        Some("preview-object") => ui.set_conflict_preview(
            format!(
                "{} bytes{}\n\n{}",
                value["size"],
                if value["truncated"] == true {
                    " (preview truncated)"
                } else {
                    ""
                },
                value["text"].as_str().unwrap_or_default()
            )
            .into(),
        ),
        Some("resolve" | "recover") => {
            set_status(ui, false, "Resolution recorded.".into());
            st.submit(
                ui,
                &format!("detail:{state}"),
                "Loading conflicts",
                folder_op(state, json!({"op": "folder-details"})),
            );
        }
        _ => {}
    }
}

fn folder_op(state: &str, payload: Value) -> Action {
    Action::Folder {
        state: state.into(),
        request: tkfs::core::id(),
        payload,
    }
}

fn clear_remote(ui: &AppWindow) {
    ui.set_selected_repo("".into());
    ui.set_remote_folders(ModelRc::default());
    ui.set_remote_message("Browse the host to see its shared folders.".into());
}

fn path(text: &str) -> Option<PathBuf> {
    let text = text.trim();
    (!text.is_empty()).then(|| PathBuf::from(text))
}

fn endpoint_error(endpoint: &str) -> Option<&'static str> {
    tkfs::ws::endpoint_uri(endpoint)
        .is_err()
        .then_some("Enter a WebSocket address, such as ws://host:43180 or wss://host.")
}

/// Opens the daemon's log (its `log_path`, else the file this window's
/// daemon writes to).
fn open_log(ui: &AppWindow, st: &State) {
    let log = st
        .snapshot
        .as_ref()
        .and_then(|s| s.daemon.log_path.clone())
        .map(PathBuf::from)
        .unwrap_or_else(|| daemon_log(&st.config));
    if !log.is_file() {
        set_status(
            ui,
            true,
            format!(
                "No log yet at {} (the daemon writes it there when this window starts it).",
                log.display()
            ),
        );
        return;
    }
    match Process::new("explorer.exe").arg(&log).spawn() {
        Ok(_) => set_status(ui, false, format!("Opened {}.", log.display())),
        Err(error) => set_status(ui, true, format!("Cannot open {}: {error}", log.display())),
    }
}

// ---------------------------------------------------------------------------
// Wiring

fn wire(ui: &AppWindow) {
    macro_rules! on {
        ($method:ident, |$ui:ident, $st:ident $(, $arg:ident)*| $body:block) => {{
            let weak = ui.as_weak();
            ui.$method(move |$($arg),*| {
                let Some($ui) = weak.upgrade() else { return };
                with_state(|$st| $body);
            });
        }};
    }
    on!(on_folder_action, |ui, st, state, id| {
        on_folder_action(&ui, st, &state, &id);
    });
    on!(on_open_folder, |ui, st, state| {
        st.worker_status = None;
        let _ = state;
        render(&ui, st);
    });
    on!(on_open_peer, |ui, st, installation| {
        let _ = installation;
        render(&ui, st);
    });
    on!(on_detail_tab_changed, |ui, st, tab| {
        let state = ui.get_selected_state().to_string();
        match tab.as_str() {
            "content" => {
                st.worker_status = None;
                st.submit(
                    &ui,
                    &format!("detail:{state}"),
                    "Reading missing files",
                    folder_op(&state, json!({"op": "status"})),
                );
            }
            "conflicts" => {
                ui.set_conflict_summary("Loading conflicts…".into());
                st.submit(
                    &ui,
                    &format!("detail:{state}"),
                    "Loading conflicts",
                    folder_op(&state, json!({"op": "folder-details"})),
                );
            }
            _ => {}
        }
        render(&ui, st);
    });
    on!(on_refresh_content, |ui, st| {
        let state = ui.get_selected_state().to_string();
        st.submit(
            &ui,
            &format!("detail:{state}"),
            "Reading missing files",
            folder_op(&state, json!({"op": "status"})),
        );
        render(&ui, st);
    });
    on!(on_open_log, |ui, st| {
        open_log(&ui, st);
    });
    on!(on_banner_clicked, |ui, st, action| {
        if action == "start-daemon" {
            st.shut_down = false;
            if let Some(feed) = &st.feed {
                feed.start_daemon();
            }
            set_status(&ui, false, "Starting the daemon…".into());
        }
        render(&ui, st);
    });
    on!(on_search_folders, |ui, st, _query| {
        render(&ui, st);
    });
    on!(on_create_folder, |ui, st, label, mount| {
        if label.trim().is_empty() {
            set_status(&ui, true, "Enter a folder name.".into());
            return;
        }
        st.submit(
            &ui,
            GLOBAL,
            "Creating the folder",
            Action::Create {
                label: label.trim().into(),
                mount: path(&mount),
                headless: false,
            },
        );
        render(&ui, st);
    });
    on!(on_configure_mount, |ui, st, state, mount| {
        st.submit(
            &ui,
            GLOBAL,
            "Saving the mount point",
            Action::ConfigureMount {
                state: state.into(),
                mount: path(&mount),
            },
        );
        render(&ui, st);
    });
    on!(on_pair, |ui, st, endpoint, code| {
        if let Some(error) = endpoint_error(&endpoint) {
            set_status(&ui, true, error.into());
            return;
        }
        if code.trim().is_empty() {
            set_status(&ui, true, "Enter the host's pairing code.".into());
            return;
        }
        clear_remote(&ui);
        st.submit(
            &ui,
            GLOBAL,
            "Pairing",
            Action::Pair {
                endpoint: endpoint.trim().into(),
                code: code.trim().into(),
                connections: None,
            },
        );
        render(&ui, st);
    });
    on!(on_remote_list, |ui, st, endpoint| {
        if let Some(error) = endpoint_error(&endpoint) {
            set_status(&ui, true, error.into());
            return;
        }
        clear_remote(&ui);
        st.submit(
            &ui,
            GLOBAL,
            "Loading the host's folders",
            Action::RemoteList {
                endpoint: endpoint.trim().into(),
            },
        );
        render(&ui, st);
    });
    on!(on_receive_folder, |ui, st, endpoint, repo, label, mount| {
        st.submit(
            &ui,
            GLOBAL,
            "Adding the folder",
            Action::Receive {
                endpoint: endpoint.trim().into(),
                repo: repo.into(),
                label: label.into(),
                mount: path(&mount),
                headless: false,
            },
        );
        render(&ui, st);
    });
    on!(on_reset_code, |ui, st| {
        st.submit(
            &ui,
            GLOBAL,
            "Generating a pairing code",
            Action::ResetPairingCode,
        );
        render(&ui, st);
    });
    on!(on_unpair, |ui, st, endpoint| {
        if let Some(error) = endpoint_error(&endpoint) {
            set_status(&ui, true, error.into());
            return;
        }
        st.submit(
            &ui,
            GLOBAL,
            "Disconnecting",
            Action::Unpair {
                endpoint: endpoint.trim().into(),
            },
        );
        render(&ui, st);
    });
    on!(on_shutdown, |ui, st| {
        // Never respawn the daemon the user is shutting down.
        if let Some(feed) = &st.feed {
            feed.set_respawn(false);
        }
        if !st.submit(&ui, GLOBAL, "Shutting down the daemon", Action::Shutdown)
            && let Some(feed) = &st.feed
        {
            feed.set_respawn(true);
        }
        render(&ui, st);
    });
    on!(on_remote_changed, |ui, st| {
        let _ = st;
        clear_remote(&ui);
    });
    on!(on_dialog_closed, |ui, st| {
        st.worker_status = None;
        render(&ui, st);
    });
    on!(on_conflict_page, |ui, st, offset| {
        let state = ui.get_selected_state().to_string();
        st.submit(
            &ui,
            &format!("detail:{state}"),
            "Loading conflicts",
            folder_op(&state, json!({"op": "folder-details", "offset": offset})),
        );
        render(&ui, st);
    });
    on!(on_select_conflict, |ui, st, id| {
        let data = &st.details;
        let conflict = data["conflicts"]
            .as_array()
            .and_then(|cs| cs.iter().find(|c| c["id"] == id.as_str()));
        let field = ui.get_conflict_field();
        let entity = ui.get_selected_entity();
        let revisions: Vec<RevisionRow> = conflict
            .into_iter()
            .flat_map(|c| c["alternatives"].as_array().into_iter().flatten())
            .filter(|v| {
                field != "namespace"
                    || data["revision_entities"][v["revision"].as_str().unwrap_or_default()]
                        .as_array()
                        .is_some_and(|entities| entities.iter().any(|e| e == entity.as_str()))
            })
            .map(|v| RevisionRow {
                id: v["revision"].as_str().unwrap_or_default().into(),
                description: format!(
                    "{} · {} · {}",
                    v["device"].as_str().unwrap_or("unknown"),
                    v["created_ms"]
                        .as_u64()
                        .map(|ms| ui_model::clock(st.now(), ms))
                        .unwrap_or_default(),
                    v["revision"].as_str().unwrap_or_default()
                )
                .into(),
            })
            .collect();
        ui.set_revisions(ModelRc::new(VecModel::from(revisions)));
        ui.set_conflict_preview(
            conflict
                .map(|c| {
                    format!(
                        "{}\nCommon bases: {}\nSelect an alternative to inspect it.",
                        c["reason"].as_str().unwrap_or_default(),
                        c["bases"]
                    )
                })
                .unwrap_or_default()
                .into(),
        );
    });
    on!(on_preview_revision, |ui, st, revision| {
        let state = ui.get_selected_state().to_string();
        let selected = ui.get_selected_conflict();
        let Some(c) = st.details["conflicts"]
            .as_array()
            .and_then(|cs| cs.iter().find(|c| c["id"] == selected.as_str()))
            .cloned()
        else {
            return;
        };
        let Some(v) = c["alternatives"]
            .as_array()
            .and_then(|vs| vs.iter().find(|v| v["revision"] == revision.as_str()))
            .cloned()
        else {
            return;
        };
        if c["field"] == "content" {
            ui.set_conflict_preview("Loading and verifying the content…".into());
            st.submit(
                &ui,
                &format!("detail:{state}"),
                "Loading the preview",
                folder_op(
                    &state,
                    json!({"op": "preview-object", "object": v["value"]}),
                ),
            );
        } else {
            ui.set_conflict_preview(serde_json::to_string_pretty(&v).unwrap_or_default().into());
        }
        render(&ui, st);
    });
    on!(on_resolve_conflict, |ui, st| {
        let state = ui.get_selected_state().to_string();
        let payload = json!({
            "op": "resolve",
            "conflicts": [ui.get_selected_conflict().as_str()],
            "revision": ui.get_selected_revision().as_str(),
            "review_token": st.details["review_token"],
        });
        st.submit(
            &ui,
            &format!("detail:{state}"),
            "Recording the resolution",
            folder_op(&state, payload),
        );
        render(&ui, st);
    });
    on!(on_recover_conflict, |ui, st, target| {
        let state = ui.get_selected_state().to_string();
        let selected = ui.get_selected_conflict();
        if !st.details["conflicts"]
            .as_array()
            .is_some_and(|cs| cs.iter().any(|c| c["id"] == selected.as_str()))
        {
            return;
        }
        // Namespace alternatives carry entry locations; the selected
        // revision's event identifies the affected entity in the backend.
        let payload = json!({
            "op": "recover",
            "conflicts": [selected.as_str()],
            "entity": ui.get_selected_entity().as_str(),
            "revision": ui.get_selected_revision().as_str(),
            "target": target.as_str(),
            "review_token": st.details["review_token"],
        });
        st.submit(
            &ui,
            &format!("detail:{state}"),
            "Recovering the entry",
            folder_op(&state, payload),
        );
        render(&ui, st);
    });
}

pub fn run(config_path: &Path) -> Result<()> {
    let config_path = std::path::absolute(config_path)?;
    if !config_path.exists() {
        Config::initialize(&config_path)?;
    }
    let config = Config::load(&config_path)?;
    if let Some(out) = std::env::var_os("TKFS_GUI_RENDER") {
        return render_to_file(&config_path, config, Path::new(&out));
    }
    release_own_console();
    let ui = AppWindow::new()?;
    ui.set_config_path(config_path.display().to_string().into());
    ui.set_host_name(host_name().into());
    let mut state = State::new(config.clone());
    state.bind(&ui);

    // Commands: their own threads and connections.
    let weak = Mutex::new(ui.as_weak());
    let command_config = config.clone();
    state.pool = Some(CommandPool::start(
        COMMAND_THREADS,
        Arc::new(move || {
            Box::new(ControlCommands {
                config: command_config.clone(),
                client: None,
            }) as Box<dyn CommandTransport>
        }),
        Arc::new(move |outcome| {
            let weak = tkfs::util::lock::lock(&weak).clone();
            let _ = weak.upgrade_in_event_loop(move |ui| {
                with_state(|st| on_outcome(&ui, st, outcome));
            });
        }),
    ));

    // The subscription: one thread, its own connection.
    let weak = ui.as_weak();
    state.feed = Some(Feed::start(
        Box::new(ControlStatus {
            config: config.clone(),
            client: None,
        }),
        Some(Box::new(Spawner {
            config_path: config_path.clone(),
            log: daemon_log(&config),
        })),
        FeedOptions::default(),
        Box::new(move |update| {
            weak.upgrade_in_event_loop(move |ui| {
                with_state(|st| on_feed(&ui, st, update));
            })
            .is_ok()
        }),
    ));
    STATE.with(|cell| *cell.borrow_mut() = Some(state));
    wire(&ui);
    with_state(|st| render(&ui, st));

    // Relative times ("3 min ago", "retry in 8 s") move on without a new
    // snapshot.
    let weak = ui.as_weak();
    let timer = slint::Timer::default();
    timer.start(
        slint::TimerMode::Repeated,
        Duration::from_secs(1),
        move || {
            if let Some(ui) = weak.upgrade() {
                with_state(|st| render(&ui, st));
            }
        },
    );
    ui.run()?;
    // Closing the window leaves the daemon and its mounts running.
    STATE.with(|cell| {
        if let Some(state) = cell.borrow_mut().take()
            && let Some(feed) = &state.feed
        {
            feed.stop();
        }
    });
    Ok(())
}

/// `tkfs` is a console program, so a desktop started without a terminal
/// (from Explorer or a shortcut) gets a console window of its own: when this
/// process is the only one attached to its console, the console is released
/// (and closes). A desktop started from a terminal keeps it.
fn release_own_console() {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetConsoleProcessList(list: *mut u32, count: u32) -> u32;
        fn FreeConsole() -> i32;
    }
    let mut ids = [0u32; 4];
    // SAFETY: `ids` is a writable buffer of the length passed.
    let attached = unsafe { GetConsoleProcessList(ids.as_mut_ptr(), ids.len() as u32) };
    if attached == 1 {
        // SAFETY: no handle to the console is held across this call.
        unsafe { FreeConsole() };
    }
}

fn host_name() -> String {
    std::env::var("COMPUTERNAME").unwrap_or_else(|_| "This computer".into())
}

/// The local time zone's current offset from UTC, in minutes.
fn utc_offset_min() -> i32 {
    #[repr(C)]
    struct SystemTime([u16; 8]);
    #[repr(C)]
    struct TimeZoneInformation {
        bias: i32,
        standard_name: [u16; 32],
        standard_date: SystemTime,
        standard_bias: i32,
        daylight_name: [u16; 32],
        daylight_date: SystemTime,
        daylight_bias: i32,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetTimeZoneInformation(info: *mut TimeZoneInformation) -> u32;
    }
    let mut info = TimeZoneInformation {
        bias: 0,
        standard_name: [0; 32],
        standard_date: SystemTime([0; 8]),
        standard_bias: 0,
        daylight_name: [0; 32],
        daylight_date: SystemTime([0; 8]),
        daylight_bias: 0,
    };
    // SAFETY: `info` is a valid, writable TIME_ZONE_INFORMATION.
    let kind = unsafe { GetTimeZoneInformation(&mut info) };
    const STANDARD: u32 = 1;
    const DAYLIGHT: u32 = 2;
    let bias = match kind {
        STANDARD => info.bias + info.standard_bias,
        DAYLIGHT => info.bias + info.daylight_bias,
        _ => info.bias,
    };
    -bias
}

// ---------------------------------------------------------------------------
// Render to a file (no window)

struct RenderPlatform {
    window: Rc<slint::platform::software_renderer::MinimalSoftwareWindow>,
}

impl slint::platform::Platform for RenderPlatform {
    fn create_window_adapter(
        &self,
    ) -> Result<Rc<dyn slint::platform::WindowAdapter>, slint::PlatformError> {
        Ok(self.window.clone())
    }
}

fn render_to_file(config_path: &Path, config: Config, out: &Path) -> Result<()> {
    use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
    let snapshot: StatusSnapshot = match std::env::var_os("TKFS_GUI_RENDER_SNAPSHOT") {
        Some(file) => serde_json::from_slice(&std::fs::read(&file)?)
            .context("TKFS_GUI_RENDER_SNAPSHOT is not a StatusSnapshot")?,
        None => serde_json::from_value(
            ControlClient::connect(&config)?
                .call(&Action::Status, default_deadline(&Action::Status))?,
        )?,
    };
    let (width, height) = std::env::var("TKFS_GUI_RENDER_SIZE")
        .ok()
        .and_then(|size| {
            let (w, h) = size.split_once('x')?;
            Some((w.parse().ok()?, h.parse().ok()?))
        })
        .unwrap_or((1380u32, 880u32));
    let window = MinimalSoftwareWindow::new(RepaintBufferType::NewBuffer);
    slint::platform::set_platform(Box::new(RenderPlatform {
        window: window.clone(),
    }))
    .map_err(|e| anyhow::anyhow!("cannot set the render platform: {e}"))?;
    let ui = AppWindow::new()?;
    window.set_size(slint::PhysicalSize::new(width, height));
    ui.set_config_path(config_path.display().to_string().into());
    ui.set_host_name(host_name().into());
    let mut state = State::new(config);
    state.bind(&ui);
    state.rates.observe(&snapshot);
    // Render at the snapshot's own time, in UTC, so the picture depends on
    // the snapshot alone.
    state.fixed_now = Some(snapshot.at_ms);
    state.snapshot = Some(Arc::new(snapshot));
    state.utc_offset_min = 0;
    let view = std::env::var("TKFS_GUI_RENDER_VIEW").unwrap_or_else(|_| "folders".into());
    let mut parts = view.splitn(3, ':');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("activity"), _, _) => ui.set_view("activity".into()),
        (Some("folder"), Some(selected), tab) => {
            ui.set_selected_state(selected.into());
            show_dialog(&ui, "folder");
            ui.set_detail_tab(tab.unwrap_or("overview").into());
        }
        (Some("peer"), Some(installation), _) => {
            ui.set_selected_peer(installation.into());
            show_dialog(&ui, "peer");
        }
        _ => {}
    }
    STATE.with(|cell| *cell.borrow_mut() = Some(state));
    with_state(|st| render(&ui, st));
    ui.show()?;
    let mut pixels = vec![slint::Rgb8Pixel::default(); (width * height) as usize];
    window.request_redraw();
    window.draw_if_needed(|renderer| {
        renderer.render(&mut pixels, width as usize);
    });
    write_bmp(out, width, height, &pixels)?;
    println!("{}", out.display());
    Ok(())
}

/// A 24-bit BMP (bottom-up rows, BGR, rows padded to 4 bytes).
fn write_bmp(path: &Path, width: u32, height: u32, pixels: &[slint::Rgb8Pixel]) -> Result<()> {
    let row = (width * 3).div_ceil(4) * 4;
    let size = 54 + row * height;
    let mut out = Vec::with_capacity(size as usize);
    out.extend_from_slice(b"BM");
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&54u32.to_le_bytes());
    out.extend_from_slice(&40u32.to_le_bytes());
    out.extend_from_slice(&(width as i32).to_le_bytes());
    out.extend_from_slice(&(height as i32).to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&24u16.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&(row * height).to_le_bytes());
    out.extend_from_slice(&2835u32.to_le_bytes());
    out.extend_from_slice(&2835u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    for y in (0..height).rev() {
        let start = out.len();
        for x in 0..width {
            let p = pixels[(y * width + x) as usize];
            out.extend_from_slice(&[p.b, p.g, p.r]);
        }
        out.resize(start + row as usize, 0);
    }
    std::fs::write(path, out)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tkfs::ui_model::Command;

    /// The window without a screen (the render-to-file platform), on this
    /// test's own thread.
    fn window() -> AppWindow {
        use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
        slint::platform::set_platform(Box::new(RenderPlatform {
            window: MinimalSoftwareWindow::new(RepaintBufferType::NewBuffer),
        }))
        .expect("a platform for this thread");
        AppWindow::new().expect("the window")
    }

    fn state(ui: &AppWindow) -> State {
        let config: Config = toml::from_str(concat!(
            "format_version = 2\n",
            "data_directory = 'data'\n",
            "default_mount_directory = 'mounts'\n",
        ))
        .expect("a config");
        let st = State::new(config);
        st.bind(ui);
        st
    }

    fn reply(ui: &AppWindow, st: &mut State, id: u64, action: Action, value: Value) {
        on_outcome(
            ui,
            st,
            CommandOutcome {
                command: Command {
                    id,
                    key: GLOBAL.into(),
                    action,
                },
                result: Ok(value),
                elapsed: Duration::ZERO,
            },
        );
    }

    /// Invariant 21 (§14 WP3.4): the code a Reset returns is shown in the
    /// host dialog only until the window leaves it, whichever way it leaves.
    #[test]
    fn the_pairing_code_is_cleared_whenever_the_window_leaves_the_host_dialog() {
        let ui = window();
        let mut st = state(&ui);
        let code = || json!({"code": "one-time-code-42"});
        slint::platform::update_timers_and_animations();

        // Settings, then "Generate a new pairing code".
        ui.set_dialog("host".into());
        reply(&ui, &mut st, 1, Action::ResetPairingCode, code());
        assert_eq!(ui.get_pairing_code(), "one-time-code-42");
        // "Shut down the daemon", then Shut down: the reply closes the
        // dialog, and Settings opened later shows no code.
        reply(&ui, &mut st, 2, Action::Shutdown, json!({}));
        assert_eq!(ui.get_dialog(), "");
        assert_eq!(ui.get_pairing_code(), "");

        // Any other way to another dialog: the window clears it before the
        // next frame.
        ui.set_dialog("host".into());
        slint::platform::update_timers_and_animations();
        reply(&ui, &mut st, 3, Action::ResetPairingCode, code());
        assert_eq!(ui.get_pairing_code(), "one-time-code-42");
        ui.set_dialog("shutdown".into());
        slint::platform::update_timers_and_animations();
        assert_eq!(ui.get_pairing_code(), "");

        // A code that arrives once the host dialog is gone is never shown.
        show_dialog(&ui, "");
        reply(&ui, &mut st, 4, Action::ResetPairingCode, code());
        assert_eq!(ui.get_pairing_code(), "");
    }
}
