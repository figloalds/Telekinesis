//! The management API's actions, its client ([`client`], [`ControlClient`])
//! and the text rendering of status (§3.5, §7.2).
//!
//! **Control token (WP4.1, §14).** The daemon's control endpoint requires a
//! local token as the `X-Tkfs-Token` of its WebSocket upgrade: a fresh one
//! per daemon start, in `DATA/orchestrator.token` (owner-only, written
//! before `orchestrator.json` names the endpoint; [`install_control_token`]).
//! An upgrade without it is refused (HTTP 403) before any action is read.
//! [`client`] and [`ControlClient`] read the file next to the
//! `orchestrator.json` they follow, again on every connect (a restarted
//! daemon has a new token); a client of a known endpoint presents the token
//! its own process knows for it, if any (`ws::tokens`).
use super::config::Config;
use super::hub_rules::{format_bytes, format_duration, format_elapsed};
use crate::{
    model::{
        errors::{ErrorRecord, Scope},
        health::{MountIntent, MountState, StoreHealth},
        orchestrator::CallError,
        status::{
            Direction, FolderPeerState, FolderPeerSync, FolderStatus, FolderSyncState, GateKind,
            LaneState, LifecyclePhase, PeerConnState, PeerStatus, StatusSnapshot, TransferProgress,
        },
    },
    ws,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fmt::Write as _,
    fs, io,
    net::TcpStream,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tungstenite::stream::MaybeTlsStream;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Action {
    List,
    Inspect {
        state: String,
    },
    Folder {
        state: String,
        request: String,
        payload: Value,
    },
    Create {
        label: String,
        mount: Option<PathBuf>,
        #[serde(default)]
        headless: bool,
    },
    ConfigureMount {
        state: String,
        mount: Option<PathBuf>,
    },
    Start {
        state: String,
    },
    Stop {
        state: String,
        /// `false` returns at once instead of waiting for `Stopped|StopBlocked`
        /// (§3.5). The legacy supervisor always stops synchronously. Omitted
        /// when `None`, so a stop request still parses on a 6298d0b daemon,
        /// whose `Action` denies unknown fields.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wait: Option<bool>,
    },
    Shutdown,
    ResetPairingCode,
    Pair {
        endpoint: String,
        code: String,
        connections: Option<usize>,
    },
    Unpair {
        endpoint: String,
    },
    Peers,
    RemoteList {
        endpoint: String,
    },
    Receive {
        endpoint: String,
        repo: String,
        label: String,
        mount: Option<PathBuf>,
        #[serde(default)]
        headless: bool,
    },
    // Management API v2 (§3.5, §14 WP2.2). Frozen by WP0.INT; served by
    // `manage_v2::handle` (through `manage::manage`).
    Status,
    Subscribe {
        since: u64,
        #[serde(default)]
        wait_ms: Option<u32>,
    },
    Attach {
        state: String,
    },
    Detach {
        state: String,
        #[serde(default)]
        force: bool,
    },
    PauseSync {
        state: String,
    },
    ResumeSync {
        state: String,
    },
    OverrideGate {
        state: String,
    },
    ForceStop {
        state: String,
    },
    Retry {
        state: String,
    },
    Revalidate {
        state: String,
        #[serde(default)]
        ids: Option<Vec<String>>,
    },
    Diagnostics,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub state: String,
    pub label: String,
    pub repo: String,
    pub device: String,
    pub directory: PathBuf,
    pub mount: Option<PathBuf>,
    pub desired_running: bool,
}

/// `DATA/orchestrator.token`: the control endpoint's local token (WP4.1).
pub const TOKEN_FILE: &str = "orchestrator.token";

/// `DATA/orchestrator.token`.
pub fn control_token_path(data_directory: &Path) -> PathBuf {
    data_directory.join(TOKEN_FILE)
}

/// The control token in `data_directory`, if there is one.
pub fn read_control_token(data_directory: &Path) -> Option<String> {
    crate::worker::record::read_token(&control_token_path(data_directory))
}

/// The daemon's control token (WP4.1): the value its control server
/// requires on every upgrade, and the registration that lets clients of this
/// process (in-process daemons of the tests) find it.
pub struct ControlToken {
    token: Arc<str>,
    /// Released when the control server stops.
    hold: Option<Arc<ws::tokens::Hold>>,
}

impl ControlToken {
    /// The token the control server requires.
    pub fn token(&self) -> Arc<str> {
        self.token.clone()
    }

    /// Ends the in-process registration (the control server stopped).
    pub fn release(&mut self) {
        self.hold = None;
    }
}

impl std::fmt::Debug for ControlToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlToken")
            .field("token", &"<redacted>")
            .field("registered", &self.hold.is_some())
            .finish()
    }
}

/// Protects the daemon's control endpoint `control` (`ws://127.0.0.1:PORT`)
/// with a fresh local token (WP4.1; CCR-WP4.1-3, applied by WP4.INT: called
/// by `ctx::build_with` right after the control listener is bound, and the
/// control server requires the returned token on every upgrade, whatever the
/// process-wide registry says). Writes `DATA/orchestrator.token`
/// (owner-only; the catalog holds `orchestrator.lock`, so no other daemon
/// writes it) and registers the token for the endpoint's loopback address so
/// clients of this process present it. A token file that cannot be written
/// is logged and the endpoint is still protected (fail closed: clients then
/// cannot connect).
pub fn install_control_token(catalog: &super::catalog::Catalog, control: &str) -> ControlToken {
    let token = ws::tokens::generate();
    let path = control_token_path(catalog.data_directory());
    if let Err(error) = crate::worker::record::write_private(&path, token.as_bytes()) {
        crate::util::log::event!(error, PERMISSION_DENIED, {
            "detail": "cannot write the control token; management clients cannot connect",
            "path": path.display().to_string(),
            "error": format!("{error:#}"),
        });
    }
    let hold = ws::tokens::addresses(control)
        .first()
        .and_then(|address| ws::tokens::protect(*address, &token, None));
    ControlToken {
        token: token.into(),
        hold,
    }
}

/// One request to the running daemon: [`ControlClient::connect`], then
/// [`ControlClient::call`] with the action's [`default_deadline`]. A
/// read-only action is retried once on a fresh connection after a transport
/// failure; a mutating one never is (`CONNECTION_LOST`, invariant 16).
pub fn client(config: &Config, action: &Action) -> Result<Value> {
    ControlClient::connect(config)?.call(action, default_deadline(action))
}

/// Makes the mount paths of `action` absolute: the daemon resolves paths
/// against its own working directory, not the caller's.
fn absolute_mounts(action: &Action) -> Result<Action> {
    let mut action = action.clone();
    match &mut action {
        Action::Create { mount, .. }
        | Action::Receive { mount, .. }
        | Action::ConfigureMount { mount, .. } => {
            if let Some(path) = mount {
                *path = std::path::absolute(&*path)?;
            }
        }
        _ => {}
    }
    Ok(action)
}

// ---------------------------------------------------------------------------
// ControlClient (§3.5, §14 WP2.2)

/// Whether `action` only reads, so sending it again after a transport failure
/// cannot repeat an effect: List, Inspect, Peers, Status, Subscribe and
/// RemoteList. Everything else mutates (or may: `Folder` passes worker ops
/// such as `resolve` through, `Diagnostics` writes a bundle).
pub fn is_read_only(action: &Action) -> bool {
    matches!(
        action,
        Action::List
            | Action::Inspect { .. }
            | Action::Peers
            | Action::Status
            | Action::Subscribe { .. }
            | Action::RemoteList { .. }
    )
}

/// A deadline for one call of `action` that covers the daemon's own waits
/// (§3.5, `model::orchestrator::limits`) plus slack for the transport.
pub fn default_deadline(action: &Action) -> Duration {
    use crate::model::{orchestrator::limits, status::policy};
    const SLACK: Duration = Duration::from_secs(15);
    match action {
        Action::Subscribe { wait_ms, .. } => {
            let wait = wait_ms
                .unwrap_or(policy::SUBSCRIBE_MAX_WAIT_MS)
                .min(policy::SUBSCRIBE_MAX_WAIT_MS);
            Duration::from_millis(u64::from(wait)) + SLACK
        }
        Action::Stop { .. } | Action::ForceStop { .. } => limits::API_STOP_WAIT * 2 + SLACK,
        Action::Attach { .. } | Action::Detach { .. } => limits::ATTACH_DEADLINE + SLACK,
        Action::Shutdown => {
            limits::SHUTDOWN_NETWORK
                + limits::SHUTDOWN_FOLDERS
                + limits::SHUTDOWN_CONTROL_JOIN
                + SLACK
        }
        Action::Create { .. }
        | Action::Receive { .. }
        | Action::Pair { .. }
        | Action::RemoteList { .. }
        | Action::Diagnostics => Duration::from_secs(120),
        Action::Folder { .. } | Action::Revalidate { .. } => Duration::from_secs(60),
        _ => Duration::from_secs(30),
    }
}

/// Where a [`ControlClient`] finds the daemon.
#[derive(Clone, Debug)]
enum Target {
    /// `DATA/orchestrator.json`, read again on every connect, so a client
    /// follows a restarted daemon to its new port.
    Discovery(PathBuf),
    /// A fixed control endpoint (`ws://127.0.0.1:PORT`).
    Endpoint(String),
}

/// A persistent connection to the daemon's control endpoint (§3.5).
///
/// * One socket carries every call, one at a time; it is opened on demand
///   and replaced after a transport failure.
/// * Before a request is sent, a socket the daemon has closed (an idle
///   timeout, a restart) is noticed and replaced: nothing was sent on it, so
///   this is never a replay.
/// * After the request was sent, a transport failure or an expired deadline
///   leaves the outcome unknown (invariant 16). A read-only action
///   ([`is_read_only`]) is then sent once more on a fresh connection, within
///   the same deadline. A mutating action is **never** sent again: the call
///   fails with `CONNECTION_LOST` (or `REQUEST_TIMEOUT`) and the caller
///   re-reads the status to learn what happened.
/// * An error reply from the daemon (`{"ok":false}`) keeps the connection
///   (invariant 17) and is returned as [`ws::RemoteError`], as
///   [`client`] always did.
pub struct ControlClient {
    target: Target,
    socket: Option<ws::Socket>,
    endpoint: Option<String>,
    connects: u64,
}

impl std::fmt::Debug for ControlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlClient")
            .field("target", &self.target)
            .field("connected", &self.socket.is_some())
            .field("endpoint", &self.endpoint)
            .field("connects", &self.connects)
            .finish()
    }
}

/// Why one attempt of a call failed.
enum Failure {
    /// Nothing reached the daemon (no connection, or the deadline was spent
    /// before sending): never retried here, safe for the caller to retry.
    NotSent(anyhow::Error),
    /// The daemon answered with an error; the connection stays usable.
    Remote(anyhow::Error),
    /// The request may have reached the daemon; the connection is dropped.
    Uncertain(anyhow::Error),
}

impl ControlClient {
    /// Connects to the daemon of `config` (its `orchestrator.json`).
    /// Fails with `ORCHESTRATOR_NOT_RUNNING` when there is none.
    pub fn connect(config: &Config) -> Result<Self> {
        Self::open_with(Target::Discovery(
            config.data_directory.join("orchestrator.json"),
        ))
    }

    /// Connects to a known control endpoint (`ws://127.0.0.1:PORT`).
    pub fn connect_endpoint(endpoint: &str) -> Result<Self> {
        Self::open_with(Target::Endpoint(endpoint.to_owned()))
    }

    fn open_with(target: Target) -> Result<Self> {
        let mut client = Self {
            target,
            socket: None,
            endpoint: None,
            connects: 0,
        };
        client.open()?;
        Ok(client)
    }

    /// The endpoint of the current connection, if any.
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// How many connections this client has opened (the first one included).
    pub fn connections(&self) -> u64 {
        self.connects
    }

    /// Closes the connection; the next call opens a new one.
    pub fn disconnect(&mut self) {
        if let Some(mut socket) = self.socket.take() {
            let _ = socket.close(None);
            let _ = socket.flush();
        }
    }

    /// Sends `action` and waits at most `deadline` for its reply (see the
    /// type's documentation for what happens on failure).
    pub fn call(&mut self, action: &Action, deadline: Duration) -> Result<Value> {
        let action = absolute_mounts(action)?;
        let started = Instant::now();
        let until = started.checked_add(deadline);
        let mut sent_once = false;
        loop {
            match self.attempt(&action, started, until) {
                Ok(value) => return Ok(value),
                Err(Failure::NotSent(error) | Failure::Remote(error)) => return Err(error),
                Err(Failure::Uncertain(error)) => {
                    self.socket = None;
                    self.endpoint = None;
                    let time_left = until.is_none_or(|until| Instant::now() < until);
                    if is_read_only(&action) && !sent_once && time_left {
                        sent_once = true;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }

    fn open(&mut self) -> Result<()> {
        let (endpoint, token) = match &self.target {
            Target::Endpoint(endpoint) => (endpoint.clone(), None),
            Target::Discovery(path) => {
                let discovery: Value =
                    serde_json::from_slice(&fs::read(path).context("ORCHESTRATOR_NOT_RUNNING")?)
                        .context("INVALID_ORCHESTRATOR_DISCOVERY")?;
                let endpoint = discovery["control"]
                    .as_str()
                    .context("INVALID_ORCHESTRATOR_DISCOVERY")?
                    .to_owned();
                // WP4.1: the daemon's token, next to its discovery.
                let token = path.parent().and_then(read_control_token);
                (endpoint, token)
            }
        };
        let socket = ws::connect_authorized(&endpoint, token.as_deref())?;
        self.socket = Some(socket);
        self.endpoint = Some(endpoint);
        self.connects += 1;
        Ok(())
    }

    fn attempt(
        &mut self,
        action: &Action,
        started: Instant,
        until: Option<Instant>,
    ) -> std::result::Result<Value, Failure> {
        if self.socket.as_ref().is_some_and(closed_by_peer) {
            self.disconnect();
        }
        if self.socket.is_none() {
            self.open().map_err(Failure::NotSent)?;
        }
        let remaining = match until {
            Some(until) => until.saturating_duration_since(Instant::now()),
            None => Duration::from_secs(3600),
        };
        let timeout = || {
            let after_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            anyhow::Error::new(CallError::Timeout { after_ms })
        };
        if remaining.is_zero() {
            return Err(Failure::NotSent(timeout()));
        }
        let socket = self.socket.as_mut().expect("connected above");
        if set_timeouts(socket, remaining).is_err() {
            // Nothing was sent on a socket we cannot configure.
            self.disconnect();
            return Err(Failure::NotSent(anyhow::Error::new(
                CallError::ConnectionLost("the control socket is unusable".into()),
            )));
        }
        let op = action_op(action);
        let lost = |error: anyhow::Error| {
            if is_timeout(&error) {
                return timeout();
            }
            anyhow::Error::new(CallError::ConnectionLost(if is_read_only(action) {
                format!("{op}: {error:#}")
            } else {
                format!(
                    "{op} was sent but no reply arrived ({error:#}); it may or may not have \
                     run, and it is not sent again: check the status"
                )
            }))
        };
        ws::send(socket, action).map_err(|error| Failure::Uncertain(lost(error)))?;
        let reply: Value = ws::receive(socket).map_err(|error| Failure::Uncertain(lost(error)))?;
        match reply.get("ok").and_then(Value::as_bool) {
            Some(true) => reply.get("result").cloned().ok_or_else(|| {
                Failure::Uncertain(anyhow::anyhow!("INVALID_WS_REPLY: missing result"))
            }),
            Some(false) => match reply["error"].as_str() {
                Some(error) => Err(Failure::Remote(ws::RemoteError(error.to_owned()).into())),
                None => Err(Failure::Uncertain(anyhow::anyhow!(
                    "INVALID_WS_REPLY: missing error"
                ))),
            },
            None => Err(Failure::Uncertain(anyhow::anyhow!(
                "INVALID_WS_REPLY: missing boolean ok"
            ))),
        }
    }
}

/// The wire name of an action (`"start"`, `"pause-sync"`, …).
fn action_op(action: &Action) -> String {
    serde_json::to_value(action)
        .ok()
        .and_then(|value| value["op"].as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn tcp_of(socket: &ws::Socket) -> Option<&TcpStream> {
    match socket.get_ref() {
        MaybeTlsStream::Plain(tcp) => Some(tcp),
        MaybeTlsStream::NativeTls(tls) => Some(tls.get_ref()),
        _ => None,
    }
}

/// Whether the daemon closed (or wrote unrequested bytes on) an idle
/// connection: a non-blocking peek sees end of stream or data.
fn closed_by_peer(socket: &ws::Socket) -> bool {
    let Some(tcp) = tcp_of(socket) else {
        return false;
    };
    if tcp.set_nonblocking(true).is_err() {
        return true;
    }
    let mut byte = [0u8; 1];
    let peeked = tcp.peek(&mut byte);
    let restored = tcp.set_nonblocking(false).is_ok();
    !restored || !matches!(peeked, Err(ref error) if error.kind() == io::ErrorKind::WouldBlock)
}

fn set_timeouts(socket: &ws::Socket, remaining: Duration) -> io::Result<()> {
    let Some(tcp) = tcp_of(socket) else {
        return Ok(());
    };
    let timeout = Some(remaining.max(Duration::from_millis(1)));
    tcp.set_read_timeout(timeout)?;
    tcp.set_write_timeout(timeout)
}

fn is_timeout(error: &anyhow::Error) -> bool {
    let timed_out = |io: &io::Error| {
        matches!(
            io.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        )
    };
    error.chain().any(|cause| {
        cause
            .downcast_ref::<tungstenite::Error>()
            .is_some_and(|error| matches!(error, tungstenite::Error::Io(io) if timed_out(io)))
            || cause.downcast_ref::<io::Error>().is_some_and(timed_out)
    })
}

// ---------------------------------------------------------------------------
// Status rendering (§7.2: `tkfs manage status` and `tkfs manage watch`)
//
// Every relative time ("12 s ago", "retry in 8 s") is measured against the
// snapshot's own `at_ms`, and clock times are UTC, so the text depends on the
// snapshot alone. Nothing here can show a pairing code: the snapshot never
// carries one (invariant 21).

/// Open errors listed per folder (and for the daemon) before "+N more".
const ERRORS_SHOWN: usize = 5;
/// Activity entries listed, newest first.
const ACTIVITY_SHOWN: usize = 6;
/// Messages are cut to this many characters.
const MESSAGE_CHARS: usize = 160;
/// Key width of the folder rows (`  lifecycle  running …`).
const FOLDER_KEY: usize = 10;
/// Indent and key width of a folder's per-peer rows.
const PEER_ROW_INDENT: usize = 2 + FOLDER_KEY + 3;
const PEER_ROW_KEY: usize = 8;

/// The text of `tkfs manage status`: the daemon, a one-line overview, one
/// block per folder (sync summary, lifecycle, mount, gate, store, inbox,
/// content, journal, each sharing peer with its metadata and transfer
/// progress, rates and ETA, open errors with class and time, and hints for
/// the actions that would help), the peers and the recent activity.
pub fn format_status(s: &StatusSnapshot) -> String {
    let names = Names {
        folders: &s.folders,
        peers: &s.peers,
    };
    let mut out = String::new();
    daemon_section(&mut out, s, &names);
    out.push('\n');
    line(&mut out, overview(s));
    if s.folders.is_empty() {
        line(
            &mut out,
            "No folders yet: tkfs manage create <label> --mount <path>",
        );
    }
    for folder in &s.folders {
        out.push('\n');
        folder_section(&mut out, folder, &names, s.at_ms);
    }
    if !s.peers.is_empty() {
        out.push('\n');
        peers_section(&mut out, s, &names);
    }
    if !s.activity.is_empty() {
        out.push('\n');
        activity_section(&mut out, s, &names);
    }
    out
}

/// One folder's block of [`format_status`], for the reply of a folder action
/// (`{state, status}`); `peers` names the peers, `at_ms` is "now".
pub fn format_folder(folder: &FolderStatus, peers: &[PeerStatus], at_ms: u64) -> String {
    let folders = std::slice::from_ref(folder);
    let names = Names { folders, peers };
    let mut out = String::new();
    folder_section(&mut out, folder, &names, at_ms);
    out
}

/// Display names of folders and peers.
struct Names<'a> {
    folders: &'a [FolderStatus],
    peers: &'a [PeerStatus],
}

impl Names<'_> {
    fn peer(&self, installation: &str) -> String {
        match self.peers.iter().find(|p| p.installation == installation) {
            Some(peer) => peer_name(
                peer.label.as_deref(),
                peer.endpoint.as_deref(),
                installation,
            ),
            None => peer_name(None, None, installation),
        }
    }

    fn folder(&self, state: &str) -> String {
        match self.folders.iter().find(|f| f.state == state) {
            Some(folder) => folder_name(folder),
            None => short(state),
        }
    }

    fn repo(&self, repo: &str) -> String {
        match self.folders.iter().find(|f| f.repo == repo) {
            Some(folder) => folder_name(folder),
            None => format!("repo {}", short(repo)),
        }
    }
}

fn folder_name(folder: &FolderStatus) -> String {
    if folder.label.is_empty() {
        short(&folder.state)
    } else {
        folder.label.clone()
    }
}

fn peer_name(label: Option<&str>, endpoint: Option<&str>, installation: &str) -> String {
    match (label, endpoint) {
        (Some(label), Some(endpoint)) => format!("{label} ({endpoint})"),
        (Some(label), None) => label.to_owned(),
        (None, Some(endpoint)) => endpoint.to_owned(),
        (None, None) => format!("peer {}", short(installation)),
    }
}

// -- sections ----------------------------------------------------------------

fn daemon_section(out: &mut String, s: &StatusSnapshot, names: &Names) {
    let d = &s.daemon;
    let mut head = vec![if d.version.is_empty() {
        "Telekinesis daemon".to_owned()
    } else {
        format!("Telekinesis daemon {}", d.version)
    }];
    if d.pid != 0 {
        head.push(format!("pid {}", d.pid));
    }
    if d.started_at_ms != 0 && s.at_ms >= d.started_at_ms {
        head.push(format!("up {}", format_elapsed(s.at_ms - d.started_at_ms)));
    }
    if d.stopping {
        head.push("stopping: shutdown in progress".into());
    }
    line(out, head.join(" · "));
    const KEY: usize = 12;
    if !d.installation.is_empty() {
        kv(out, 2, KEY, "installation", &d.installation);
    }
    if !d.control.is_empty() {
        kv(out, 2, KEY, "control", &d.control);
    }
    kv(
        out,
        2,
        KEY,
        "network",
        d.network.as_deref().unwrap_or("not listening"),
    );
    if let Some(log) = &d.log_path {
        kv(out, 2, KEY, "log", log);
    }
    // Peer and lane errors are listed under their peer; the daemon lists its
    // own, and those of peers the snapshot does not list.
    let own: Vec<&ErrorRecord> = d
        .errors
        .iter()
        .filter(|record| match scope_peer(&record.scope) {
            Some(installation) => !s.peers.iter().any(|p| p.installation == installation),
            None => true,
        })
        .collect();
    errors_rows(out, 2, KEY, &own, names, s.at_ms);
}

fn overview(s: &StatusSnapshot) -> String {
    use FolderSyncState as F;
    const ORDER: [FolderSyncState; 14] = [
        F::NeedsAttention,
        F::Stalled,
        F::Blocked,
        F::Backoff,
        F::Offline,
        F::InitialSync,
        F::Downloading,
        F::Uploading,
        F::Syncing,
        F::Metadata,
        F::Paused,
        F::NotShared,
        F::Stopped,
        F::UpToDate,
    ];
    let folders: Vec<String> = ORDER
        .iter()
        .filter_map(|state| {
            let n = s.folders.iter().filter(|f| f.sync.state == *state).count();
            (n > 0).then(|| format!("{n} {}", sync_state_name(*state)))
        })
        .collect();
    use PeerConnState as P;
    const PEERS: [PeerConnState; 7] = [
        P::Online,
        P::Connecting,
        P::Backoff,
        P::Offline,
        P::Incompatible,
        P::Unauthorized,
        P::Revoked,
    ];
    let peers: Vec<String> = PEERS
        .iter()
        .filter_map(|state| {
            let n = s.peers.iter().filter(|p| p.state == *state).count();
            (n > 0).then(|| format!("{n} {}", conn_state_name(*state)))
        })
        .collect();
    let mut text = count(s.folders.len() as u64, "folder", "folders");
    if !folders.is_empty() {
        let _ = write!(text, " ({})", folders.join(", "));
    }
    let _ = write!(text, " · {}", count(s.peers.len() as u64, "peer", "peers"));
    if !peers.is_empty() {
        let _ = write!(text, " ({})", peers.join(", "));
    }
    text
}

fn folder_section(out: &mut String, f: &FolderStatus, names: &Names, at: u64) {
    line(out, format!("{} [{}]", folder_name(f), short(&f.state)));
    let row = |out: &mut String, key: &str, value: &str| kv(out, 2, FOLDER_KEY, key, value);
    let summary = if f.sync.summary.is_empty() {
        capitalized(sync_state_name(f.sync.state))
    } else {
        f.sync.summary.clone()
    };
    let attention = matches!(
        f.sync.state,
        FolderSyncState::NeedsAttention | FolderSyncState::Stalled | FolderSyncState::Blocked
    );
    let summary = if attention {
        format!("! {summary}")
    } else {
        summary
    };
    row(out, "sync", &summary);
    row(out, "lifecycle", &lifecycle_text(f, at));
    if let Some(exit) = last_exit_text(f, at) {
        row(out, "last exit", &exit);
    }
    row(out, "mount", &mount_text(f, at));
    if f.gate.state != GateKind::None {
        row(out, "gate", &gate_text(f, names, at));
    }
    if !f.intents.sync_enabled {
        row(out, "intents", "sync paused");
    }
    let live = f.lifecycle.phase == LifecyclePhase::Running;
    if live || !f.store.health.is_healthy() {
        row(out, "store", &store_text(f));
    }
    if live || f.activation.inbox.total() > 0 {
        row(out, "inbox", &inbox_text(f));
    }
    if let Some(text) = activation_text(f, at) {
        row(out, "activation", &text);
    }
    if live || f.content.current_objects > 0 {
        row(out, "content", &content_text(f));
    }
    if live || f.journal.events > 0 {
        row(out, "journal", &journal_text(f));
    }
    for peer in f
        .sync
        .peers
        .iter()
        .filter(|peer| peer.state != FolderPeerState::NotShared)
    {
        peer_rows(out, peer, names, at);
    }
    let errors: Vec<&ErrorRecord> = f.errors.iter().collect();
    errors_rows(out, 2, FOLDER_KEY, &errors, names, at);
    for hint in hints(f) {
        row(out, "hint", &hint);
    }
}

fn peer_rows(out: &mut String, p: &FolderPeerSync, names: &Names, at: u64) {
    let name = peer_name(p.label.as_deref(), p.endpoint.as_deref(), &p.installation);
    let direction = direction_name(p.direction);
    kv(
        out,
        2,
        FOLDER_KEY,
        "peer",
        &format!("{name} · {direction} · {}", peer_state_text(p, at)),
    );
    let sub = |out: &mut String, key: &str, value: &str| {
        kv(out, PEER_ROW_INDENT, PEER_ROW_KEY, key, value);
    };
    sub(out, "metadata", &meta_text(p, at));
    if transfer_active(&p.objects_down) {
        sub(out, "download", &transfer_text(&p.objects_down));
    }
    if transfer_active(&p.objects_up) {
        sub(out, "upload", &transfer_text(&p.objects_up));
    }
    if let Some(last) = p.last_progress_ms {
        sub(out, "progress", &format!("last {}", ago(at, last)));
    }
    if let Some(error) = &p.error {
        sub(out, "error", &error_text(error, names, at, false));
    }
}

fn peers_section(out: &mut String, s: &StatusSnapshot, names: &Names) {
    let at = s.at_ms;
    line(out, format!("Peers ({})", s.peers.len()));
    const KEY: usize = 7;
    for p in &s.peers {
        let mut head = vec![
            peer_name(p.label.as_deref(), p.endpoint.as_deref(), &p.installation),
            conn_text(p, at),
            direction_name(p.direction).to_owned(),
        ];
        if let Some(protocol) = &p.protocol {
            head.push(protocol.clone());
        }
        if let Some(rtt) = p.rtt_ms {
            head.push(format!("rtt {rtt} ms"));
        }
        if p.rate_up_bps > 0 || p.rate_down_bps > 0 {
            head.push(format!(
                "up {}/s · down {}/s",
                format_bytes(p.rate_up_bps),
                format_bytes(p.rate_down_bps)
            ));
        }
        line(out, format!("  {}", head.join(" · ")));
        if let Some(lanes) = lanes_text(p, at) {
            kv(out, 4, KEY, "lanes", &lanes);
        }
        if !p.shared_folders.is_empty() {
            let folders: Vec<String> = p.shared_folders.iter().map(|r| names.repo(r)).collect();
            kv(out, 4, KEY, "folders", &folders.join(", "));
        }
        if let Some(error) = &p.error {
            kv(out, 4, KEY, "error", &error_text(error, names, at, false));
        }
    }
}

fn activity_section(out: &mut String, s: &StatusSnapshot, names: &Names) {
    line(out, "Recent activity");
    for entry in s.activity.iter().rev().take(ACTIVITY_SHOWN) {
        let subject = match (&entry.folder, &entry.peer) {
            (Some(folder), Some(peer)) => {
                format!("{} / {}: ", names.folder(folder), names.peer(peer))
            }
            (Some(folder), None) => format!("{}: ", names.folder(folder)),
            (None, Some(peer)) => format!("{}: ", names.peer(peer)),
            (None, None) => String::new(),
        };
        line(
            out,
            format!(
                "  {:<11} {:<10} {subject}{}",
                ago(s.at_ms, entry.at_ms),
                entry.kind,
                clip(&entry.message)
            ),
        );
    }
}

// -- folder rows ---------------------------------------------------------------

fn lifecycle_text(f: &FolderStatus, at: u64) -> String {
    let l = &f.lifecycle;
    let since = (l.since_ms != 0 && at >= l.since_ms).then(|| format_elapsed(at - l.since_ms));
    let for_since = |verb: &str| match &since {
        Some(since) => format!("{verb} for {since}"),
        None => verb.to_owned(),
    };
    let mut parts = Vec::new();
    match l.phase {
        LifecyclePhase::Stopped => parts.push(if f.intents.desired_running {
            "stopped (start pending)".to_owned()
        } else {
            "stopped".to_owned()
        }),
        LifecyclePhase::Starting => {
            parts.push(for_since("starting"));
            match (&l.startup, &l.detail) {
                (Some(progress), _) if !l.adopted && !progress.phase.is_empty() => {
                    parts.push(format!("{} (step {})", progress.phase, progress.seq));
                }
                (_, Some(detail)) => parts.push(detail.clone()),
                _ => {}
            }
        }
        LifecyclePhase::Running => parts.push(for_since("running")),
        LifecyclePhase::StopBlocked | LifecyclePhase::Stopping => {
            parts.push(for_since(if l.phase == LifecyclePhase::StopBlocked {
                "stop blocked"
            } else {
                "stopping"
            }));
            if let Some(detail) = &l.detail {
                parts.push(clip(detail));
            }
        }
        LifecyclePhase::Backoff => {
            parts.push(match l.next_retry_at_ms {
                Some(next) if next > at => {
                    format!("restarting in {}", format_duration(next - at))
                }
                _ => "restarting".to_owned(),
            });
            if let Some(detail) = &l.detail {
                parts.push(clip(detail));
            }
        }
        LifecyclePhase::Failed => {
            parts.push("failed".to_owned());
            if let Some(detail) = &l.detail {
                parts.push(clip(detail));
            }
        }
    }
    if let Some(pid) = l.pid {
        parts.push(format!("pid {pid}"));
    }
    if l.adopted && l.phase == LifecyclePhase::Running {
        parts.push("adopted".to_owned());
    }
    if l.degraded {
        parts.push("degraded: the engine lock is stalled".to_owned());
    }
    if l.restarts > 0 {
        parts.push(count(u64::from(l.restarts), "restart", "restarts"));
    }
    parts.join(" · ")
}

fn last_exit_text(f: &FolderStatus, at: u64) -> Option<String> {
    let exit = f.lifecycle.last_exit.as_ref()?;
    if f.lifecycle.phase == LifecyclePhase::Running {
        return None;
    }
    let mut parts = vec![match exit.code {
        Some(code) => format!("exit code {code}"),
        None => "no exit code".to_owned(),
    }];
    match &exit.fatal {
        Some(fatal) => {
            let message = if fatal.message.starts_with(&fatal.code) {
                fatal.message.clone()
            } else {
                format!("{}: {}", fatal.code, fatal.message)
            };
            parts.push(format!("[{}] {}", fatal.class, clip(&message)));
        }
        None => {
            if let Some(tail) = exit
                .stderr_tail
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
            {
                parts.push(format!("log: {}", clip(tail)));
            }
        }
    }
    if exit.at_ms != 0 {
        parts.push(ago(at, exit.at_ms));
    }
    Some(parts.join(" · "))
}

fn mount_text(f: &FolderStatus, at: u64) -> String {
    let m = &f.mount;
    let path = m.path.as_ref().map(|path| path.display().to_string());
    let at_path = |verb: &str| match &path {
        Some(path) => format!("{verb} at {path}"),
        None => verb.to_owned(),
    };
    match &m.state {
        MountState::Attached { since_ms } => {
            let mut text = at_path("attached");
            if *since_ms != 0 && at >= *since_ms {
                let _ = write!(text, " for {}", format_elapsed(at - since_ms));
            }
            text
        }
        MountState::Preflight | MountState::Attaching => at_path("attaching"),
        MountState::Detaching => at_path("detaching"),
        MountState::AttachFailed {
            error,
            attempt,
            next_retry_at_ms,
        } => {
            let mut text = format!(
                "{} (attempt {attempt}): [{}] {}",
                at_path("attach failed"),
                error.class,
                clip(&error.to_string())
            );
            match next_retry_at_ms {
                Some(next) if *next > at => {
                    let _ = write!(text, " · retry in {}", format_duration(next - at));
                }
                Some(_) => text.push_str(" · retrying"),
                None => text.push_str(" · waiting for you to fix it"),
            }
            text
        }
        MountState::Lost { error } => format!(
            "{}: [{}] {}",
            at_path("lost"),
            error.class,
            clip(&error.to_string())
        ),
        MountState::Detached | MountState::Disabled => match &path {
            None => "headless (no mount path)".to_owned(),
            Some(path) if !f.intents.mount_enabled => {
                format!("not mounted ({path}) · mounting disabled")
            }
            Some(path) if m.intent == MountIntent::Detach => {
                format!("detached on request ({path})")
            }
            Some(path) if matches!(f.gate.state, GateKind::Awaiting | GateKind::Blocked) => {
                format!("waiting for the initial sync before mounting at {path}")
            }
            Some(path) if f.lifecycle.phase != LifecyclePhase::Running => {
                format!("not mounted (worker not running) · {path}")
            }
            Some(path) => format!("not mounted ({path})"),
        },
    }
}

fn gate_text(f: &FolderStatus, names: &Names, at: u64) -> String {
    let source = f
        .gate
        .source
        .as_deref()
        .map(|source| names.peer(source))
        .unwrap_or_else(|| "the source peer".to_owned());
    match f.gate.state {
        GateKind::None => String::new(),
        GateKind::Awaiting => {
            let mut text = format!("waiting for the initial sync from {source}");
            if let Some(since) = f.gate.since_ms.filter(|since| *since != 0 && at >= *since) {
                let _ = write!(text, " for {}", format_elapsed(at - since));
            }
            if !f.gate.waiting_on.is_empty() {
                let _ = write!(text, ": {}", f.gate.waiting_on.join(", "));
            }
            text
        }
        GateKind::Blocked => format!("blocked: events from {source} are quarantined or rejected"),
        GateKind::Satisfied => "initial sync complete".to_owned(),
        GateKind::Overridden => "overridden: mounting before the initial sync finished".to_owned(),
    }
}

fn store_text(f: &FolderStatus) -> String {
    let s = &f.store;
    let mut parts = vec![match &s.health {
        StoreHealth::Healthy => "healthy".to_owned(),
        StoreHealth::Degraded { error } => {
            format!("degraded: [{}] {}", error.class, clip(&error.to_string()))
        }
        StoreHealth::Failed { error } => {
            format!("failed: [{}] {}", error.class, clip(&error.to_string()))
        }
    }];
    if s.open_handles > 0 {
        parts.push(count(u64::from(s.open_handles), "open file", "open files"));
    }
    if s.pending_saves > 0 {
        parts.push(count(
            u64::from(s.pending_saves),
            "pending save",
            "pending saves",
        ));
    }
    if s.unflushed_files > 0 {
        parts.push(count(
            u64::from(s.unflushed_files),
            "unflushed file",
            "unflushed files",
        ));
    }
    parts.join(" · ")
}

fn inbox_text(f: &FolderStatus) -> String {
    let i = &f.activation.inbox;
    let pending = i.pending_active();
    let mut parts = Vec::new();
    if pending > 0 {
        let detail: Vec<String> = [
            (i.ready, "ready"),
            (i.waiting, "waiting for parents"),
            (i.retry, "retrying"),
            (i.unclassified, "unclassified"),
        ]
        .into_iter()
        .filter(|(n, _)| *n > 0)
        .map(|(n, what)| format!("{} {what}", group(n)))
        .collect();
        parts.push(format!(
            "{} to activate ({})",
            count(pending, "event", "events"),
            detail.join(", ")
        ));
    }
    if i.quarantined > 0 {
        parts.push(format!("{} quarantined", group(i.quarantined)));
    }
    if i.blocked > 0 {
        parts.push(format!("{} blocked", group(i.blocked)));
    }
    if parts.is_empty() {
        "nothing to activate".to_owned()
    } else {
        parts.join(" · ")
    }
}

fn activation_text(f: &FolderStatus, at: u64) -> Option<String> {
    let c = &f.activation.counters;
    if !c.running && c.activated == 0 {
        return None;
    }
    let mut parts = Vec::new();
    if c.running {
        parts.push("running".to_owned());
    }
    if c.activated > 0 {
        parts.push(format!(
            "{} activated ({} fast, {} slow)",
            group(c.activated),
            group(c.fast),
            group(c.slow)
        ));
    }
    if let Some(last) = c.last_activation_at_ms {
        parts.push(format!("last {}", ago(at, last)));
    }
    if let Some(since) = c.starved_since_ms.filter(|since| at >= *since) {
        parts.push(format!("starved for {}", format_elapsed(at - since)));
    }
    Some(parts.join(" · "))
}

fn content_text(f: &FolderStatus) -> String {
    let c = &f.content;
    let mut parts = Vec::new();
    if c.current_missing == 0 {
        parts.push(format!(
            "all {} available",
            count(c.current_objects, "file", "files")
        ));
    } else {
        parts.push(format!(
            "{} of {} available",
            group(c.current_available),
            count(c.current_objects, "file", "files")
        ));
        parts.push(format!(
            "{} missing ({})",
            group(c.current_missing),
            format_bytes(c.current_missing_bytes)
        ));
    }
    if c.downloading > 0 {
        parts.push(format!(
            "{} downloading ({} so far)",
            group(c.downloading),
            format_bytes(c.partial_bytes)
        ));
    }
    if c.requested > 0 {
        parts.push(format!("{} requested", group(c.requested)));
    }
    if !c.complete && c.shared_objects > c.shared_available {
        parts.push(format!(
            "history {} of {} available",
            group(c.shared_available),
            group(c.shared_objects)
        ));
    }
    parts.join(" · ")
}

fn journal_text(f: &FolderStatus) -> String {
    let j = &f.journal;
    let mut parts = vec![count(j.events, "event", "events")];
    if j.outgoing_unacknowledged > 0 {
        parts.push(format!(
            "{} not yet acknowledged by a peer",
            group(j.outgoing_unacknowledged)
        ));
    } else if j.shared_events > 0 {
        parts.push("all acknowledged by peers".to_owned());
    }
    if j.unresolved_conflicts > 0 {
        parts.push(count(
            j.unresolved_conflicts,
            "unresolved conflict",
            "unresolved conflicts",
        ));
    }
    parts.join(" · ")
}

/// What the user can do about the folder's state, as CLI commands (the
/// commands take the short state shown in the folder's heading).
fn hints(f: &FolderStatus) -> Vec<String> {
    let state = short(&f.state);
    let mut hints = Vec::new();
    let mut hint = |why: &str, verb: &str| hints.push(format!("{why}: tkfs manage {verb} {state}"));
    match f.lifecycle.phase {
        LifecyclePhase::Failed => hint("restart the failed worker", "retry"),
        LifecyclePhase::StopBlocked => hint("stop although files are open", "force-stop"),
        _ => {}
    }
    if !f.intents.sync_enabled {
        hint("sync is paused", "resume");
    }
    if f.gate.state == GateKind::Awaiting && f.mount.path.is_some() && f.intents.mount_enabled {
        hint("mount before the initial sync finishes", "mount-now");
    }
    if f.gate.state == GateKind::Blocked || f.activation.inbox.quarantined > 0 {
        hint("retry the quarantined events", "revalidate");
    }
    let running = f.lifecycle.phase == LifecyclePhase::Running;
    match &f.mount.state {
        MountState::AttachFailed {
            next_retry_at_ms: None,
            ..
        } if running => hint("after fixing the mount problem", "attach"),
        MountState::Lost { .. } if running => hint("mount again", "attach"),
        MountState::Detached if running && f.mount.intent == MountIntent::Detach => {
            hint("mount again", "attach");
        }
        _ => {}
    }
    hints
}

// -- peer rows ---------------------------------------------------------------

fn peer_state_text(p: &FolderPeerSync, at: u64) -> String {
    match p.state {
        FolderPeerState::Stalled => match p.last_progress_ms.filter(|last| at >= *last) {
            Some(last) => format!("stalled: no progress for {}", format_elapsed(at - last)),
            None => "stalled".to_owned(),
        },
        FolderPeerState::Backoff => match p.backoff_until_ms.filter(|until| *until > at) {
            Some(until) => format!("backing off (retry in {})", format_duration(until - at)),
            None => "backing off".to_owned(),
        },
        state => peer_state_name(state).to_owned(),
    }
}

fn meta_text(p: &FolderPeerSync, at: u64) -> String {
    let m = &p.metadata;
    let mut parts: Vec<String> = [
        (m.to_receive, "to receive"),
        (m.to_send, "to send"),
        (m.local_pending, "received, not yet activated"),
        (m.remote_pending, "pending at the peer"),
        (m.blocked, "blocked"),
        (m.quarantined, "quarantined"),
        (m.rejected, "rejected"),
    ]
    .into_iter()
    .filter(|(n, _)| *n > 0)
    .map(|(n, what)| format!("{} {what}", group(n)))
    .collect();
    parts.push(
        if m.caught_up {
            "caught up"
        } else {
            "not caught up"
        }
        .to_owned(),
    );
    if let Some(last) = m.last_exchange_ms {
        parts.push(format!("last exchange {}", ago(at, last)));
    }
    parts.join(" · ")
}

fn transfer_active(t: &TransferProgress) -> bool {
    t.objects_remaining > 0 || t.bytes_remaining > 0 || t.in_flight > 0 || t.rate_bps > 0
}

fn transfer_text(t: &TransferProgress) -> String {
    let mut parts = Vec::new();
    if t.objects_remaining > 0 {
        parts.push(count(t.objects_remaining, "file", "files"));
    }
    if t.bytes_remaining > 0 {
        parts.push(if t.total_bytes > t.bytes_remaining {
            format!(
                "{} of {} left",
                format_bytes(t.bytes_remaining),
                format_bytes(t.total_bytes)
            )
        } else {
            format!("{} left", format_bytes(t.bytes_remaining))
        });
    }
    if t.rate_bps > 0 {
        parts.push(format!("{}/s", format_bytes(t.rate_bps)));
    }
    if let Some(eta) = t.eta_s {
        parts.push(format!("ETA {}", format_duration(eta.saturating_mul(1000))));
    }
    if t.in_flight > 0 {
        parts.push(format!("{} in flight", t.in_flight));
    }
    if parts.is_empty() {
        "idle".to_owned()
    } else {
        parts.join(" · ")
    }
}

fn conn_text(p: &PeerStatus, at: u64) -> String {
    let since = p
        .since_ms
        .filter(|since| *since != 0 && at >= *since)
        .map(|since| format_elapsed(at - since));
    match p.state {
        PeerConnState::Online | PeerConnState::Offline => {
            let name = conn_state_name(p.state);
            match since {
                Some(since) => format!("{name} for {since}"),
                None => name.to_owned(),
            }
        }
        PeerConnState::Backoff => {
            let retry = p
                .lanes
                .iter()
                .filter_map(|lane| lane.backoff_until_ms)
                .filter(|until| *until > at)
                .min();
            match retry {
                Some(until) => format!("backing off (retry in {})", format_duration(until - at)),
                None => "backing off".to_owned(),
            }
        }
        PeerConnState::Incompatible => "needs upgrade (incompatible protocol)".to_owned(),
        PeerConnState::Unauthorized => "unauthorized: pair again with the current code".to_owned(),
        state => conn_state_name(state).to_owned(),
    }
}

fn lanes_text(p: &PeerStatus, at: u64) -> Option<String> {
    let outbound: Vec<_> = p
        .lanes
        .iter()
        .filter(|lane| lane.direction != Direction::Inbound)
        .collect();
    let mut parts = Vec::new();
    if !outbound.is_empty() {
        let ready = outbound
            .iter()
            .filter(|lane| lane.state == LaneState::Ready)
            .count();
        parts.push(format!("{ready} of {} ready", outbound.len()));
        for lane in outbound
            .iter()
            .filter(|lane| lane.state != LaneState::Ready)
        {
            let mut text = format!("lane {} {}", lane.lane, lane_state_name(lane.state));
            if let Some(until) = lane.backoff_until_ms.filter(|until| *until > at) {
                let _ = write!(text, " (retry in {})", format_duration(until - at));
            }
            parts.push(text);
        }
    }
    let inflight: usize = p.lanes.iter().map(|lane| lane.inflight.len()).sum();
    if inflight > 0 {
        parts.push(count(
            inflight as u64,
            "request in flight",
            "requests in flight",
        ));
    }
    if p.inbound_sessions > 0 {
        parts.push(count(
            u64::from(p.inbound_sessions),
            "inbound session",
            "inbound sessions",
        ));
    }
    (!parts.is_empty()).then(|| parts.join(" · "))
}

// -- errors ------------------------------------------------------------------

/// Open errors (`! …`), at most [`ERRORS_SHOWN`]; when none is open, the
/// most recent resolved one (`last: …`).
fn errors_rows(
    out: &mut String,
    indent: usize,
    key: usize,
    records: &[&ErrorRecord],
    names: &Names,
    at: u64,
) {
    let open: Vec<&ErrorRecord> = records
        .iter()
        .copied()
        .filter(|record| !record.is_resolved())
        .collect();
    let mut rows = Vec::new();
    for record in open.iter().take(ERRORS_SHOWN) {
        rows.push(format!("! {}", error_text(record, names, at, true)));
    }
    if open.len() > ERRORS_SHOWN {
        rows.push(format!(
            "+{} more (tkfs manage status --json lists them)",
            open.len() - ERRORS_SHOWN
        ));
    }
    if open.is_empty()
        && let Some(last) = records.iter().find(|record| record.is_resolved())
    {
        rows.push(format!("last: {}", error_text(last, names, at, true)));
    }
    if !rows.is_empty() {
        kv(out, indent, key, "errors", &rows.join("\n"));
    }
}

/// `[class] CODE: message · scope · Nx since T · last at T (age) · retry in X`.
fn error_text(record: &ErrorRecord, names: &Names, at: u64, with_scope: bool) -> String {
    let mut parts = vec![format!(
        "[{}] {}",
        record.error.class,
        clip(&record.error.to_string())
    )];
    if with_scope && let Some(scope) = scope_text(&record.scope, names) {
        parts.push(scope);
    }
    let when = format!(
        "at {} ({})",
        utc(record.last_at_ms, at),
        ago(at, record.last_at_ms)
    );
    if record.count > 1 {
        parts.push(format!(
            "{}x since {}",
            group(record.count),
            utc(record.first_at_ms, at)
        ));
        parts.push(format!("last {when}"));
    } else {
        parts.push(when);
    }
    if let Some(next) = record.next_retry_at_ms.filter(|next| *next > at) {
        parts.push(format!("retry in {}", format_duration(next - at)));
    }
    if let Some(resolved) = record.resolved_at_ms {
        parts.push(format!("resolved {}", ago(at, resolved)));
    }
    parts.join(" · ")
}

fn scope_text(scope: &Scope, names: &Names) -> Option<String> {
    match scope {
        Scope::Daemon | Scope::Folder { .. } => None,
        Scope::Mount { .. } => Some("mount".to_owned()),
        Scope::Object { hash, .. } => Some(format!("object {}", short_hash(hash))),
        Scope::Event { id, .. } => Some(format!("event {}", short(id))),
        Scope::FolderPeer { installation, .. } | Scope::Peer { installation } => {
            Some(format!("peer {}", names.peer(installation)))
        }
        Scope::Lane { installation, lane } => {
            Some(format!("peer {} lane {lane}", names.peer(installation)))
        }
    }
}

fn scope_peer(scope: &Scope) -> Option<&str> {
    match scope {
        Scope::Peer { installation }
        | Scope::Lane { installation, .. }
        | Scope::FolderPeer { installation, .. } => Some(installation),
        _ => None,
    }
}

// -- names of states ---------------------------------------------------------

fn sync_state_name(state: FolderSyncState) -> &'static str {
    match state {
        FolderSyncState::Stopped => "stopped",
        FolderSyncState::NotShared => "not shared",
        FolderSyncState::Paused => "paused",
        FolderSyncState::Offline => "offline",
        FolderSyncState::InitialSync => "initial sync",
        FolderSyncState::Metadata => "syncing metadata",
        FolderSyncState::Downloading => "downloading",
        FolderSyncState::Uploading => "uploading",
        FolderSyncState::Syncing => "syncing",
        FolderSyncState::Blocked => "blocked",
        FolderSyncState::Backoff => "backing off",
        FolderSyncState::Stalled => "stalled",
        FolderSyncState::NeedsAttention => "needs attention",
        FolderSyncState::UpToDate => "up to date",
    }
}

fn peer_state_name(state: FolderPeerState) -> &'static str {
    match state {
        FolderPeerState::NotShared => "not shared",
        FolderPeerState::Offline => "offline",
        FolderPeerState::Connecting => "connecting",
        FolderPeerState::Incompatible => "needs upgrade",
        FolderPeerState::Unauthorized => "unauthorized",
        FolderPeerState::WaitingForWorker => "waiting for the local worker",
        FolderPeerState::WaitingForRemote => "waiting for the peer's folder",
        FolderPeerState::Paused => "paused",
        FolderPeerState::Metadata => "syncing metadata",
        FolderPeerState::Downloading => "downloading",
        FolderPeerState::Uploading => "uploading",
        FolderPeerState::Syncing => "syncing",
        FolderPeerState::Blocked => "blocked",
        FolderPeerState::Backoff => "backing off",
        FolderPeerState::Stalled => "stalled",
        FolderPeerState::UpToDate => "up to date",
    }
}

fn conn_state_name(state: PeerConnState) -> &'static str {
    match state {
        PeerConnState::Online => "online",
        PeerConnState::Connecting => "connecting",
        PeerConnState::Offline => "offline",
        PeerConnState::Backoff => "backing off",
        PeerConnState::Incompatible => "incompatible",
        PeerConnState::Unauthorized => "unauthorized",
        PeerConnState::Revoked => "revoked",
    }
}

fn lane_state_name(state: LaneState) -> &'static str {
    match state {
        LaneState::Idle => "idle",
        LaneState::Dialing => "dialing",
        LaneState::Handshaking => "handshaking",
        LaneState::Ready => "ready",
        LaneState::Backoff => "backing off",
        LaneState::Incompatible => "incompatible",
        LaneState::Unauthorized => "unauthorized",
        LaneState::Closed => "closed",
    }
}

fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Outbound => "outbound",
        Direction::Inbound => "inbound",
        Direction::Both => "both ways",
    }
}

// -- text helpers ------------------------------------------------------------

fn line(out: &mut String, text: impl AsRef<str>) {
    out.push_str(text.as_ref());
    out.push('\n');
}

/// `key value`, the key padded to `width` after `indent` spaces; the lines
/// of a multi-line value line up under the first.
fn kv(out: &mut String, indent: usize, width: usize, key: &str, value: &str) {
    for (index, part) in value.split('\n').enumerate() {
        let key = if index == 0 { key } else { "" };
        let _ = writeln!(out, "{:indent$}{key:<width$} {part}", "");
    }
}

/// `1,234,567`.
fn group(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// `1 file`, `1,234 files`.
fn count(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", group(n), if n == 1 { one } else { many })
}

fn capitalized(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// The first 8 characters of an id (folder states, installations).
fn short(id: &str) -> String {
    id.chars().take(8).collect()
}

fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
}

/// One line of at most [`MESSAGE_CHARS`] characters.
fn clip(text: &str) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= MESSAGE_CHARS {
        return flat;
    }
    let mut cut: String = flat.chars().take(MESSAGE_CHARS).collect();
    cut.push_str("...");
    cut
}

/// `12 s ago`; `just now` under a second (and for a time after `at`, a
/// clock skew between the daemon and its peers).
fn ago(at: u64, then: u64) -> String {
    if then >= at || at - then < 1000 {
        "just now".to_owned()
    } else {
        format!("{} ago", format_elapsed(at - then))
    }
}

/// `12:04:33 UTC` on the UTC day of `at`, else `2026-10-13 23:59:01 UTC`.
fn utc(ms: u64, at: u64) -> String {
    let (date, time) = civil_utc(ms);
    if civil_utc(at).0 == date {
        format!("{time} UTC")
    } else {
        format!("{date} {time} UTC")
    }
}

/// (`YYYY-MM-DD`, `HH:MM:SS`) of a Unix time in milliseconds, in UTC.
fn civil_utc(ms: u64) -> (String, String) {
    let seconds = ms / 1000;
    let days = i64::try_from(seconds / 86_400).unwrap_or(i64::MAX / 2);
    let rest = seconds % 86_400;
    // Days since the epoch to a proleptic Gregorian date (H. Hinnant's
    // `civil_from_days`).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (
        format!("{year:04}-{month:02}-{day:02}"),
        format!(
            "{:02}:{:02}:{:02}",
            rest / 3600,
            (rest / 60) % 60,
            rest % 60
        ),
    )
}
