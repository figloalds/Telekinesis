//! The Windows desktop: a client of the orchestrator of its configuration
//! (the same `orchestrator.toml` as the CLI). All I/O stays off the UI
//! thread: a status thread polls the orchestrator, an action thread runs one
//! request at a time. Neither starts the orchestrator by itself: when it is
//! lost, the banner offers to start it again.
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use slint::{ComponentHandle, ModelRc, VecModel, Weak};
use std::{
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, Instant},
};
use tkfs::{
    core::Conflict,
    desktop::{Client, explain, mount_choice, pairing_code},
    orchestrator::{config::Config, launcher::Started, process::LOG_FILE},
    util::log::daemon_log_path,
};
slint::include_modules!();

/// How often the status thread polls.
const POLL: Duration = Duration::from_secs(1);
/// Activity rows kept for the selected folder.
const RECENT_EVENTS: usize = 128;
/// How long `clip.exe` may take, and how long a killed one gets to go.
const CLIPBOARD_BOUND: Duration = Duration::from_secs(5);
/// How long the status thread waits for a reply: a hung orchestrator shows
/// as lost within a poll or two, not after the actions' much longer wait
/// (a status read is retried once).
const STATUS_REPLY_TIMEOUT: Duration = Duration::from_secs(10);
/// `CREATE_NO_WINDOW`: helper processes never flash a console.
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
/// The error codes that mean the orchestrator is gone.
const LOST: &[&str] = &["ORCHESTRATOR_NOT_RUNNING", "CONNECTION_LOST"];

/// A request for the action thread.
enum Message {
    /// `Client::open`: attach to the orchestrator, or start it.
    StartOrchestrator,
    Action {
        operation: String,
        state: String,
        args: [String; 4],
        headless: bool,
        pairing_generation: i32,
    },
}

/// What the status thread is told between polls.
enum Poll {
    /// The selected folder (its id).
    Select(String),
    /// A new connection, after the orchestrator was (re)opened.
    Client(Box<Client>),
    /// Poll now.
    Refresh,
}

/// What a finished action changes in the window.
#[derive(Default)]
struct Done {
    message: String,
    select: Option<String>,
    close: bool,
    code: Option<String>,
    remote: Option<Vec<Value>>,
    preview: Option<String>,
}

fn done(message: impl Into<String>) -> Done {
    Done {
        message: message.into(),
        ..Done::default()
    }
}

fn model<T: Clone + 'static>(rows: Vec<T>) -> ModelRc<T> {
    ModelRc::new(VecModel::from(rows))
}

fn text(value: &Value, key: &str) -> String {
    value[key].as_str().unwrap_or_default().into()
}

/// Whether `error` means the orchestrator stopped or the connection to it
/// is gone.
fn lost(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let cause = cause.to_string();
        LOST.iter().any(|code| cause.starts_with(code))
    })
}

/// The banner (and its action) for a failed status poll. Only a banner: the
/// orchestrator starts again only when that action is clicked.
fn on_status_error(error: &anyhow::Error) -> (String, &'static str) {
    if lost(error) {
        ("Orchestrator stopped".into(), "start-orchestrator")
    } else {
        (
            format!("The orchestrator's status failed: {}", explain(error)),
            "",
        )
    }
}

/// `name` under `%SystemRoot%\System32` (`C:\Windows` when `SystemRoot` is
/// unset), or directly under `%SystemRoot%` where it lives there (`explorer.exe`
/// is not in `System32`): a bare name would be searched for in the
/// application's own directory first.
fn system_tool(name: &str) -> PathBuf {
    let root = std::env::var_os("SystemRoot")
        .filter(|root| !root.is_empty())
        .map_or_else(|| PathBuf::from(r"C:\Windows"), PathBuf::from);
    let system32 = root.join("System32").join(name);
    let beside = root.join(name);
    if !system32.exists() && beside.exists() {
        beside
    } else {
        system32
    }
}

fn spawn_hidden(program: &str, path: &Path) -> Result<()> {
    use std::os::windows::process::CommandExt;
    std::process::Command::new(system_tool(program))
        .arg(path)
        .creation_flags(CREATE_NO_WINDOW)
        .spawn()
        .with_context(|| format!("starting {program}"))?;
    Ok(())
}

/// Puts `text` on the clipboard through `clip.exe`, within
/// [`CLIPBOARD_BOUND`]; a `clip.exe` that does not go after its kill within
/// [`CLIPBOARD_BOUND`] is reported as `STRANDED_PROCESS`.
fn copy_to_clipboard(text: &str) -> Result<()> {
    use std::{io::Write, os::windows::process::CommandExt, process::Stdio};
    let mut child = std::process::Command::new(system_tool("clip.exe"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(CREATE_NO_WINDOW)
        .spawn()
        .context("CLIPBOARD_UNAVAILABLE: clip.exe did not start")?;
    let written = child
        .stdin
        .take()
        .context("CLIPBOARD_UNAVAILABLE: no stdin")
        .and_then(|mut stdin| Ok(stdin.write_all(text.as_bytes())?));
    let end = Instant::now() + CLIPBOARD_BOUND;
    loop {
        // An error from `try_wait` is not an exit: it ends in the kill below.
        if let Ok(Some(status)) = child.try_wait() {
            written?;
            ensure!(status.success(), "CLIPBOARD_FAILED: clip.exe {status}");
            return Ok(());
        }
        if Instant::now() >= end {
            let pid = child.id();
            let _ = child.kill();
            let gone_by = Instant::now() + CLIPBOARD_BOUND;
            while !matches!(child.try_wait(), Ok(Some(_))) {
                if Instant::now() >= gone_by {
                    bail!(
                        "CLIPBOARD_TIMEOUT: clip.exe did not finish; STRANDED_PROCESS: pid {pid}"
                    );
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            bail!("CLIPBOARD_TIMEOUT: clip.exe did not finish");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A folder's record from the orchestrator (`inspect`).
fn record(client: &mut Client, state: &str) -> Result<Value> {
    Ok(client.read("inspect", json!({"state": state}))?["record"].clone())
}

/// Runs one GUI operation as an orchestrator request (the action table).
fn perform(
    client: &mut Client,
    operation: &str,
    state: &str,
    args: &[String; 4],
    headless: bool,
) -> Result<Done> {
    let [a, b, c, d] = args;
    let folder = json!({"state": state});
    let mount_directory = client.config.default_mount_directory.clone();
    Ok(match operation {
        "create" => {
            let mount = mount_choice(&mount_directory, a, b, headless)?;
            let created = client.act("create", json!({"label": a.trim(), "mount": mount}))?;
            Done {
                select: Some(text(&created, "id")),
                close: true,
                ..done("Folder created and started")
            }
        }
        "remote-list" => {
            let listed = client.read("remote-list", json!({"endpoint": a}))?;
            Done {
                remote: Some(listed.as_array().cloned().unwrap_or_default()),
                ..done("Folders listed")
            }
        }
        "pair" => {
            client.act("pair", json!({"endpoint": a, "code": pairing_code(b)}))?;
            match client.read("remote-list", json!({"endpoint": a})) {
                Ok(listed) => Done {
                    remote: Some(listed.as_array().cloned().unwrap_or_default()),
                    ..done("Paired")
                },
                Err(error) => done(format!(
                    "Paired; listing its folders failed: {}",
                    explain(&error)
                )),
            }
        }
        "receive" => {
            let mount = mount_choice(&mount_directory, c, d, headless)?;
            let received = client.act(
                "receive",
                json!({"endpoint": a, "repo": b, "label": c, "mount": mount}),
            )?;
            Done {
                select: Some(text(&received, "id")),
                close: true,
                ..done("Folder received; syncing from the host")
            }
        }
        "start" | "stop" | "pause" | "resume" => {
            client.act(operation, folder)?;
            done(match operation {
                "start" => "Folder started",
                "stop" => "Folder stopped",
                "pause" => "Sync paused",
                _ => "Sync resumed",
            })
        }
        "restart" => {
            client.act("stop", folder.clone())?;
            client.act("start", folder)?;
            done("Folder restarted")
        }
        "configure-mount" => {
            let mount = a.trim();
            let mount = if mount.is_empty() {
                Value::Null
            } else {
                json!(mount)
            };
            client.act("configure-mount", json!({"state": state, "mount": mount}))?;
            Done {
                close: true,
                ..done("Mount point saved")
            }
        }
        "resolve" => {
            client.folder_op(state, "resolve", json!({"entity": a, "version": b}))?;
            done("Conflict resolved; alternatives retained in history")
        }
        "preview" => {
            let reply = client.folder_op(state, "preview-version", json!({"version": a}))?;
            Done {
                preview: Some(text(&reply, "preview")),
                ..Done::default()
            }
        }
        "fork" => {
            client.folder_op(state, "fork", json!({"name": a}))?;
            done("Branch created")
        }
        "checkout" => {
            client.folder_op(state, "checkout", json!({"branch": a}))?;
            done("Branch checked out")
        }
        "checkpoint" => {
            let snapshot = client.folder_op(state, "checkpoint", json!({}))?;
            done(format!("Checkpoint {}", text(&snapshot, "id")))
        }
        "gc" => {
            let collected = client.folder_op(state, "gc", json!({}))?;
            done(format!(
                "Collected {} unreferenced objects",
                collected["removed"]
            ))
        }
        "reset-code" => {
            let reply = client.act("reset-pairing-code", json!({}))?;
            Done {
                code: Some(text(&reply, "code")),
                ..done("New pairing code generated")
            }
        }
        "unpair" => {
            client.act("unpair", json!({"installation": a}))?;
            done("Peer unpaired")
        }
        "shutdown" => {
            client.act("shutdown", json!({}))?;
            Done {
                close: true,
                ..done("Orchestrator stopped")
            }
        }
        "open" => {
            let record = record(client, state)?;
            let mount = record["mount"].as_str().context("FOLDER_NOT_MOUNTED")?;
            spawn_hidden("explorer.exe", Path::new(mount))?;
            Done::default()
        }
        "log" => {
            let record = record(client, state)?;
            let state = record["state"]
                .as_str()
                .context("INVALID_REPLY: record.state")?;
            spawn_hidden("notepad.exe", &Path::new(state).join(LOG_FILE))?;
            Done::default()
        }
        _ => bail!("UNKNOWN_DESKTOP_ACTION: {operation}"),
    })
}

/// The host's Log: the orchestrator's log, also when it does not run.
fn open_host_log(config_path: &Path) -> Result<Done> {
    let config = Config::load(config_path)?;
    spawn_hidden("notepad.exe", &daemon_log_path(&config.data_directory))?;
    Ok(Done::default())
}

/// Attaches to (or starts) the orchestrator: the status thread's client
/// from `open`, and a second one for the action thread.
fn open_clients(config_path: &Path) -> Result<(Client, Client, Started)> {
    let exe = std::env::current_exe()?;
    let (mut status, started) = Client::open(config_path, &exe)?;
    let actions = status.connect_again()?;
    status.set_reply_timeout(STATUS_REPLY_TIMEOUT);
    Ok((status, actions, started))
}

/// The selected folder's recent events: seeded with the newest
/// [`RECENT_EVENTS`] (`events {last}`), then read forward from `after`.
#[derive(Default)]
struct Feed {
    state: String,
    seeded: bool,
    after: u64,
    events: Vec<Value>,
}

impl Feed {
    /// The `events` payload for folder `state` (a new folder starts over).
    fn request(&mut self, state: &str) -> Value {
        if self.state != state {
            *self = Self {
                state: state.into(),
                ..Self::default()
            };
        }
        if self.seeded {
            json!({"after": self.after})
        } else {
            json!({"last": RECENT_EVENTS})
        }
    }

    /// Takes in a reply to [`Self::request`].
    fn absorb(&mut self, reply: &Value) {
        self.seeded = true;
        for row in reply.as_array().into_iter().flatten() {
            if let Some(seq) = row[0].as_u64()
                && seq > self.after
            {
                self.after = seq;
                self.events.push(row.clone());
            }
        }
        let excess = self.events.len().saturating_sub(RECENT_EVENTS);
        self.events.drain(..excess);
    }

    fn poll(&mut self, client: &mut Client, state: &str) {
        let request = self.request(state);
        if let Ok(reply) = client.folder_op(state, "events", request) {
            self.absorb(&reply);
        }
    }
}

/// One poll: `status`, plus for the selected folder `inspect` and, while it
/// runs, its conflicts and events. Only `status` failing fails the poll.
fn poll(client: &mut Client, selected: &str, feed: &mut Feed) -> Result<Value> {
    let status = client.status()?;
    let entry = status["folders"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["record"]["id"] == selected)
        .cloned();
    let mut detail = Value::Null;
    if let Some(entry) = entry {
        detail = json!({
            "inspect": client.read("inspect", json!({"state": selected})).unwrap_or(Value::Null),
        });
        if entry["phase"]["phase"] == "Running" {
            if let Ok(conflicts) = client.folder_op(selected, "conflicts", json!({"history": true}))
            {
                detail["conflicts"] = conflicts;
            }
            feed.poll(client, selected);
            detail["events"] = json!(feed.events);
        }
    }
    Ok(json!({
        "status": status,
        "selected": selected,
        "detail": detail,
        "config_path": client.config_path,
        "default_mount_directory": client.config.default_mount_directory,
    }))
}

/// The status thread. On an error its only reaction is the banner
/// ([`on_status_error`]); it never opens a client or starts anything.
fn status_loop(weak: Weak<AppWindow>, polls: mpsc::Receiver<Poll>) {
    let mut client: Option<Box<Client>> = None;
    let mut selected = String::new();
    let mut feed = Feed::default();
    loop {
        match polls.recv_timeout(POLL) {
            Ok(Poll::Select(id)) => selected = id,
            Ok(Poll::Client(new)) => client = Some(new),
            Ok(Poll::Refresh) | Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        let Some(current) = client.as_deref_mut() else {
            continue;
        };
        let posted = match poll(current, &selected, &mut feed) {
            Ok(value) => weak.upgrade_in_event_loop(move |ui| render(&ui, value)),
            Err(error) => {
                let (banner, action) = on_status_error(&error);
                weak.upgrade_in_event_loop(move |ui| show_banner(&ui, banner, action))
            }
        };
        if posted.is_err() {
            break;
        }
    }
}

/// The action thread: opens the orchestrator once at start and again only
/// for `StartOrchestrator`, then runs each message in turn.
fn action_loop(
    weak: Weak<AppWindow>,
    messages: mpsc::Receiver<Message>,
    polls: mpsc::Sender<Poll>,
    config_path: PathBuf,
) {
    let mut client: Option<Client> = None;
    let mut next = Some(Message::StartOrchestrator);
    loop {
        let message = match next.take() {
            Some(message) => message,
            None => match messages.recv() {
                Ok(message) => message,
                Err(_) => break,
            },
        };
        let posted = match message {
            Message::StartOrchestrator => match open_clients(&config_path) {
                Ok((status, actions, started)) => {
                    client = Some(actions);
                    let _ = polls.send(Poll::Client(Box::new(status)));
                    let message = match started {
                        Started::Attached => "Attached to the running orchestrator".into(),
                        Started::Spawned { pid } => format!("Started the orchestrator (pid {pid})"),
                    };
                    weak.upgrade_in_event_loop(move |ui| {
                        ui.set_global_busy("".into());
                        ui.set_banner_text("".into());
                        ui.set_banner_action("".into());
                        ui.set_status(message.into());
                        ui.set_status_error(false);
                    })
                }
                Err(error) => {
                    let banner = format!("The orchestrator did not start: {}", explain(&error));
                    weak.upgrade_in_event_loop(move |ui| {
                        ui.set_global_busy("".into());
                        show_banner(&ui, banner, "start-orchestrator");
                    })
                }
            },
            Message::Action {
                operation,
                state,
                args,
                headless,
                pairing_generation,
            } => {
                let result = if operation == "host-log" {
                    open_host_log(&config_path)
                } else {
                    client
                        .as_mut()
                        .context("ORCHESTRATOR_NOT_RUNNING: start the orchestrator first")
                        .and_then(|client| perform(client, &operation, &state, &args, headless))
                };
                if let Ok(Done {
                    select: Some(id), ..
                }) = &result
                {
                    let _ = polls.send(Poll::Select(id.clone()));
                }
                let _ = polls.send(Poll::Refresh);
                weak.upgrade_in_event_loop(move |ui| {
                    finish(&ui, &operation, &args, pairing_generation, result)
                })
            }
        };
        if posted.is_err() {
            break;
        }
    }
}

/// Whether a folder list for `requested` belongs in the window: the pair
/// dialog is open on that endpoint. Not the pairing generation: Browse in
/// the peer dialog opens this dialog and lists in one handler, and the
/// `changed dialog` bump runs after the request captured the generation.
/// An edited endpoint changes `endpoint`, so a stale list never shows.
fn remote_list_applies(dialog: &str, endpoint: &str, requested: &str) -> bool {
    dialog == "connect" && endpoint == requested
}

/// Shows an action's outcome.
fn finish(
    ui: &AppWindow,
    operation: &str,
    args: &[String; 4],
    pairing_generation: i32,
    result: Result<Done>,
) {
    ui.set_global_busy("".into());
    let current = ui.get_pairing_generation() == pairing_generation;
    match result {
        Ok(done) => {
            if let Some(id) = done.select {
                ui.set_selected_state(id.into());
            }
            if let Some(code) = done.code
                && ui.get_dialog() == "host"
                && current
            {
                ui.set_pairing_code(code.into());
            }
            if let Some(preview) = done.preview
                && ui.get_selected_revision() == args[0].as_str()
            {
                ui.set_conflict_preview(preview.into());
            }
            if let Some(remote) = done.remote
                && remote_list_applies(&ui.get_dialog(), &ui.get_endpoint(), &args[0])
            {
                ui.set_remote_message(
                    if remote.is_empty() {
                        "That host publishes no folders yet."
                    } else {
                        "Select a folder to receive a new local copy."
                    }
                    .into(),
                );
                ui.set_remote_folders(model(
                    remote
                        .iter()
                        .map(|row| RemoteRow {
                            repo: text(row, "repo").into(),
                            label: row["label"].as_str().unwrap_or("Remote folder").into(),
                        })
                        .collect(),
                ));
            }
            ui.set_status(done.message.into());
            ui.set_status_error(false);
            if done.close {
                ui.set_dialog("".into());
            } else if operation == "stop" && ui.get_dialog() == "stop-first" {
                ui.set_dialog("mount".into());
            } else if operation == "unpair" && ui.get_dialog() == "peer" {
                ui.set_dialog("".into());
            }
        }
        Err(error) => {
            let message = explain(&error);
            if matches!(operation, "pair" | "remote-list") && ui.get_dialog() == "connect" {
                ui.set_remote_message(message.clone().into());
            }
            ui.set_status(message.into());
            ui.set_status_error(true);
            if lost(&error) {
                let (banner, action) = on_status_error(&error);
                show_banner(ui, banner, action);
            }
        }
    }
}

include!("gui_adapter.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helper_tools_are_absolute_paths_under_the_windows_directory() {
        for name in ["clip.exe", "notepad.exe", "explorer.exe"] {
            let path = system_tool(name);
            assert!(path.is_absolute(), "{path:?}");
            assert!(path.exists(), "{path:?}");
            assert_eq!(path.file_name().and_then(|n| n.to_str()), Some(name));
        }
    }

    /// A folder entry shaped like `hub::Daemon::snapshot`'s `folders[i]`.
    fn entry(id: &str, phase: Value, paused: bool, error: Value) -> Value {
        json!({
            "record": {
                "id": id, "label": format!("Folder {id}"), "state": format!(r"C:\TK\data\{id}"),
                "repo": format!("repo-{id}"), "mount": null, "desired_running": true,
                "paused": paused, "created_ms": 1,
            },
            "phase": phase,
            "heartbeat": {"at_ms": 1, "entries": 4, "conflicts": 0, "writable_handles": 0, "error": null},
            "lanes": [],
            "conflicts": 0,
            "error": error,
            "sync": {},
        })
    }

    fn running() -> Value {
        json!({"phase": "Running", "since_ms": 1, "instance": "i", "pid": 7, "address": "127.0.0.1:1", "adopted": false})
    }

    #[test]
    fn folder_rows_come_from_the_status_snapshot() {
        let row = folder_row(&entry("a", running(), false, Value::Null));
        assert_eq!(row.state, "a");
        assert_eq!(row.label, "Folder a");
        assert_eq!(row.mount_chip.text, "Headless");
        assert!(row.running);
        assert_eq!(row.life.text, "Running");
        assert!(row.error_title.is_empty());

        let paused = folder_row(&entry("b", running(), true, Value::Null));
        assert_eq!(paused.sync.text, "Paused");

        let failed = folder_row(&entry(
            "c",
            json!({"phase": "Failed", "since_ms": 1, "code": "WORKER_EXITED", "message": "exit 3"}),
            false,
            json!("WORKER_EXITED: exit 3"),
        ));
        assert!(!failed.running);
        assert_eq!(failed.life.text, "Failed");
        assert_eq!(failed.error_title, "WORKER_EXITED: exit 3");
    }

    #[test]
    fn peer_rows_come_from_the_status_snapshot() {
        let lane = |index: u64| {
            json!({"state": "Idle", "lane": index, "role": if index == 0 { "control" } else { "bulk" },
                   "spec": {}, "last_error": null})
        };
        let peer = json!({
            "endpoint": "ws://office-pc:43180", "installation": "0123456789abcdef",
            "direction": "outbound", "paired": true, "connections": null,
            "lanes": [lane(0), lane(1)], "inbound": 0, "clock": null,
        });
        let row = peer_row(&peer);
        assert_eq!(row.endpoint, "ws://office-pc:43180");
        assert_eq!(row.name, "ws://office-pc:43180");
        assert_eq!(row.installation, "0123456789abcdef");
        assert!(
            row.lanes_summary.contains("2 lanes"),
            "{}",
            row.lanes_summary
        );
        assert!(!row.inbound_only);
    }

    #[test]
    fn a_lost_orchestrator_is_not_restarted_automatically() {
        for error in [
            anyhow::anyhow!("ORCHESTRATOR_NOT_RUNNING: no orchestrator serves C:\\TK\\data"),
            anyhow::anyhow!("WS_CONNECT_FAILED: refused")
                .context("CONNECTION_LOST: no reply to status"),
        ] {
            assert_eq!(
                on_status_error(&error),
                ("Orchestrator stopped".into(), "start-orchestrator")
            );
        }
        let (banner, action) = on_status_error(&anyhow::anyhow!("INVALID_REPLY: x"));
        assert!(banner.contains("INVALID_REPLY"), "{banner}");
        assert_eq!(action, "");
    }

    #[test]
    fn the_activity_feed_seeds_with_the_newest_events_then_follows_the_cursor() {
        let event = |seq: u64| json!([seq, {"id": format!("e{seq}"), "operation": "Create"}]);
        let mut feed = Feed::default();
        // A newly selected folder asks for its newest events, not for seq 0.
        assert_eq!(feed.request("a"), json!({"last": RECENT_EVENTS}));
        feed.absorb(&json!(
            (9_000..9_000 + RECENT_EVENTS as u64)
                .map(event)
                .collect::<Vec<_>>()
        ));
        let after = 9_000 + RECENT_EVENTS as u64 - 1;
        assert_eq!(feed.request("a"), json!({"after": after}));
        feed.absorb(&json!([event(after + 1)]));
        assert_eq!(feed.events.len(), RECENT_EVENTS);
        assert_eq!(feed.events.last().unwrap()[0], after + 1);
        assert_eq!(feed.request("a"), json!({"after": after + 1}));
        // A folder that has no events yet is seeded once; then it follows.
        assert_eq!(feed.request("b"), json!({"last": RECENT_EVENTS}));
        assert!(feed.events.is_empty());
        feed.absorb(&json!([]));
        assert_eq!(feed.request("b"), json!({"after": 0}));
        // Switching back starts over from the newest again.
        assert_eq!(feed.request("a"), json!({"last": RECENT_EVENTS}));
    }

    #[test]
    fn a_folder_list_shows_while_the_pair_dialog_is_on_its_endpoint() {
        // Browse from the peer dialog: the dialog change bumps the pairing
        // generation after the request; the list still applies.
        assert!(remote_list_applies(
            "connect",
            "ws://office:43180",
            "ws://office:43180"
        ));
        assert!(!remote_list_applies(
            "connect",
            "ws://laptop:43180",
            "ws://office:43180"
        ));
        assert!(!remote_list_applies(
            "",
            "ws://office:43180",
            "ws://office:43180"
        ));
        assert!(!remote_list_applies(
            "peer",
            "ws://office:43180",
            "ws://office:43180"
        ));
    }
}
