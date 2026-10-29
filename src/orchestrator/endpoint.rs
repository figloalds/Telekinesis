//! The orchestrator's loopback control endpoint, for `manage` (and the GUI,
//! later): a JSON RPC over a plain WebSocket whose upgrade must carry
//! `X-Tkfs-Token: <DATA/orchestrator.token>`.
//!
//! Request `{"action", "payload"}`; reply `{"ok":true,"result"}` or
//! `{"ok":false,"error":{"code","message"}}` (`code` is the message's
//! leading `SCREAMING_CASE` token, else `ORCHESTRATOR_ERROR`). A connection
//! carries any number of requests; a frame that is not a request is answered
//! `INVALID_REQUEST` and the connection stays usable.
//!
//! Discovery: `DATA/orchestrator.token` (owner-only) is written first, then
//! `DATA/orchestrator.json` (atomically): `{control, installation, pid,
//! version, listen, advertise, started_ms, data_directory}`, `control` and
//! `listen` as `host:port`.
use super::{config::Config, hub::Daemon, worker::open_private};
use crate::{
    util::log::{event, register_secret},
    ws::{self, Socket},
};
use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use std::{
    fs,
    io::{ErrorKind, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use tungstenite::stream::MaybeTlsStream;

/// Control connections served at once; more are closed on accept.
pub const IDLE_CONNECTIONS: usize = 64;
pub const DISCOVERY_FILE: &str = "orchestrator.json";
pub const TOKEN_FILE: &str = "orchestrator.token";
/// A connection with no request for this long is closed.
pub const CONTROL_IDLE: Duration = Duration::from_secs(300);
/// How long a client waits for one reply: the longest action (`stop` of a
/// still-starting folder, `shutdown`) fits.
pub const REPLY_TIMEOUT: Duration = Duration::from_secs(180);
/// After a failed accept, the accept loop waits this long (as the peer
/// listener's does) instead of spinning on a persistent error.
const ACCEPT_RETRY: Duration = Duration::from_millis(50);

/// Binds `127.0.0.1:0`, writes the token and `orchestrator.json`, and serves
/// control connections on background threads. Returns the endpoint's
/// `host:port`.
pub fn serve(daemon: Arc<Daemon>) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").context("CONTROL_BIND_FAILED")?;
    let control = listener.local_addr()?.to_string();
    let token = ws::tokens::generate();
    register_secret(&token);
    let data = &daemon.config.data_directory;
    // A file left by a daemon that died names a dead endpoint: gone first,
    // so no client pairs it with the new token.
    match fs::remove_file(data.join(DISCOVERY_FILE)) {
        Err(error) if error.kind() != ErrorKind::NotFound => {
            return Err(error).context("DISCOVERY_WRITE_FAILED");
        }
        _ => {}
    }
    let mut file = open_private(
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true),
        &data.join(TOKEN_FILE),
    )
    .context("DISCOVERY_WRITE_FAILED: orchestrator.token")?;
    file.write_all(token.as_bytes())?;
    file.sync_all()?;
    drop(file);
    write_atomically(
        &data.join(DISCOVERY_FILE),
        &serde_json::to_vec_pretty(&daemon.discovery(&control))?,
    )
    .context("DISCOVERY_WRITE_FAILED: orchestrator.json")?;
    daemon.set_control(control.clone());
    let active = Arc::new(AtomicUsize::new(0));
    thread::Builder::new()
        .name("tkfs-control-accept".into())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else {
                    // A persistent accept error (descriptors exhausted,
                    // …) must not spin.
                    thread::sleep(ACCEPT_RETRY);
                    continue;
                };
                if active.load(Ordering::Acquire) >= IDLE_CONNECTIONS {
                    event!(warn, CONTROL_CONNECTIONS_EXHAUSTED, { "limit": IDLE_CONNECTIONS });
                    continue;
                }
                active.fetch_add(1, Ordering::AcqRel);
                let (daemon, token, done) = (daemon.clone(), token.clone(), active.clone());
                let spawned = thread::Builder::new()
                    .name("tkfs-control".into())
                    .spawn(move || {
                        serve_connection(&daemon, stream, &token);
                        done.fetch_sub(1, Ordering::AcqRel);
                    });
                if spawned.is_err() {
                    active.fetch_sub(1, Ordering::AcqRel);
                }
            }
        })
        .context("CONTROL_THREAD_FAILED")?;
    Ok(control)
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    let mut file = fs::File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    Ok(())
}

fn serve_connection(daemon: &Daemon, stream: TcpStream, token: &str) {
    let Ok(mut socket) = ws::accept_authorized(stream, Some(token)) else {
        return;
    };
    if set_read_timeout(&socket, CONTROL_IDLE).is_err() {
        return;
    }
    loop {
        let Ok(request) = ws::receive::<Value>(&mut socket) else {
            return;
        };
        let _request = daemon.begin_request();
        let result = match (request["action"].as_str(), &request["payload"]) {
            (Some(action), payload) if payload.is_object() || payload.is_null() => {
                daemon.execute(action, payload)
            }
            _ => Err(anyhow!(
                "INVALID_REQUEST: a request is {{\"action\": string, \"payload\": object}}"
            )),
        };
        let reply = match result {
            Ok(result) => json!({"ok": true, "result": result}),
            Err(error) => {
                let message = format!("{error:#}");
                json!({"ok": false, "error": {"code": leading_code(&message), "message": message}})
            }
        };
        if ws::send(&mut socket, &reply).is_err() {
            return;
        }
    }
}

/// The message's leading `SCREAMING_CASE` code, else `ORCHESTRATOR_ERROR`.
fn leading_code(message: &str) -> String {
    let head = message.split(':').next().unwrap_or_default().trim();
    let mut bytes = head.bytes();
    let coded = bytes.next().is_some_and(|b| b.is_ascii_uppercase())
        && bytes.all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
    if coded {
        head.to_owned()
    } else {
        "ORCHESTRATOR_ERROR".to_owned()
    }
}

fn set_read_timeout(socket: &Socket, timeout: Duration) -> Result<()> {
    match socket.get_ref() {
        MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout))?,
        MaybeTlsStream::NativeTls(stream) => stream.get_ref().set_read_timeout(Some(timeout))?,
        _ => bail!("UNSUPPORTED_OPERATION: unsupported stream"),
    }
    Ok(())
}

/// A `manage` connection to the orchestrator of a configuration's data
/// directory.
pub struct ControlClient {
    endpoint: String,
    token: String,
    socket: Option<Socket>,
    /// How long a reply (and, when shorter than the dial's own bounds, each
    /// step of a dial) may take.
    reply_timeout: Duration,
}

impl ControlClient {
    /// `ORCHESTRATOR_NOT_RUNNING` without `orchestrator.json`; the dial's
    /// `WS_CONNECT_FAILED` or `ACCESS_DENIED` otherwise.
    pub fn connect(config: &Config) -> Result<Self> {
        Self::connect_within(config, REPLY_TIMEOUT)
    }

    /// [`Self::connect`] whose TCP connect, upgrade and each reply wait at
    /// most `timeout` (non-zero) instead of [`REPLY_TIMEOUT`]: for a probe
    /// that must fit a deadline.
    pub fn connect_within(config: &Config, timeout: Duration) -> Result<Self> {
        let data = &config.data_directory;
        let not_running = || {
            format!(
                "ORCHESTRATOR_NOT_RUNNING: no orchestrator serves {}",
                data.display()
            )
        };
        let discovery: Value = match fs::read(data.join(DISCOVERY_FILE)) {
            Ok(bytes) => serde_json::from_slice(&bytes).context("INVALID_DISCOVERY")?,
            Err(error) if error.kind() == ErrorKind::NotFound => bail!(not_running()),
            Err(error) => return Err(error).context("INVALID_DISCOVERY"),
        };
        let control = discovery["control"]
            .as_str()
            .context("INVALID_DISCOVERY: control missing")?;
        let token = fs::read_to_string(data.join(TOKEN_FILE)).with_context(not_running)?;
        let token = token.trim().to_owned();
        register_secret(&token);
        let mut client = Self {
            endpoint: format!("ws://{control}"),
            token,
            socket: None,
            reply_timeout: timeout,
        };
        client.socket = Some(client.dial()?);
        Ok(client)
    }

    /// Replies (and later dials) wait at most `timeout` from now on. Drops
    /// the current connection: the next request dials again.
    pub fn set_reply_timeout(&mut self, timeout: Duration) {
        self.reply_timeout = timeout;
        self.socket = None;
    }

    fn dial(&self) -> Result<Socket> {
        let socket =
            ws::connect_authorized_within(&self.endpoint, Some(&self.token), self.reply_timeout)?;
        set_read_timeout(&socket, self.reply_timeout)?;
        Ok(socket)
    }

    /// One action. A mutation is never resent: a connection lost before its
    /// reply is `CONNECTION_LOST` (it may or may not have run).
    pub fn request(&mut self, action: &str, payload: Value) -> Result<Value> {
        let request = json!({"action": action, "payload": payload});
        match self.exchange(&request) {
            Ok(reply) => parse_reply(reply),
            Err(error) => Err(error.context(format!(
                "CONNECTION_LOST: no reply to {action}; it may or may not have run"
            ))),
        }
    }

    /// A read-only action: retried once on a new connection.
    pub fn request_read(&mut self, action: &str, payload: Value) -> Result<Value> {
        let request = json!({"action": action, "payload": payload});
        match self.exchange(&request) {
            Ok(reply) => parse_reply(reply),
            Err(_) => match self.exchange(&request) {
                Ok(reply) => parse_reply(reply),
                Err(error) => Err(error.context(format!("CONNECTION_LOST: no reply to {action}"))),
            },
        }
    }

    /// Sends `request` and reads its reply; a transport failure drops the
    /// socket (the next exchange dials again).
    fn exchange(&mut self, request: &Value) -> Result<Value> {
        let mut socket = match self.socket.take() {
            Some(socket) => socket,
            None => self.dial()?,
        };
        ws::send(&mut socket, request)?;
        let reply: Value = ws::receive(&mut socket)?;
        self.socket = Some(socket);
        Ok(reply)
    }
}

fn parse_reply(reply: Value) -> Result<Value> {
    match reply["ok"].as_bool() {
        Some(true) => Ok(reply["result"].clone()),
        Some(false) => {
            let code = reply["error"]["code"]
                .as_str()
                .unwrap_or("ORCHESTRATOR_ERROR");
            let message = reply["error"]["message"].as_str().unwrap_or_default();
            if message == code || message.starts_with(&format!("{code}:")) {
                bail!("{message}")
            }
            bail!("{code}: {message}")
        }
        None => bail!("INVALID_REPLY: {reply}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_come_from_the_message() {
        assert_eq!(leading_code("FOLDER_NOT_FOUND: x"), "FOLDER_NOT_FOUND");
        assert_eq!(leading_code("WORKER_LIMIT"), "WORKER_LIMIT");
        assert_eq!(leading_code("No such file: x"), "ORCHESTRATOR_ERROR");
    }

    #[test]
    fn replies_become_values_or_coded_errors() {
        assert_eq!(
            parse_reply(json!({"ok": true, "result": {"a": 1}})).unwrap(),
            json!({"a": 1})
        );
        let error = parse_reply(json!({"ok": false, "error": {"code": "BUSY_VIEW", "message": "BUSY_VIEW: 1 files open"}}))
            .unwrap_err();
        assert_eq!(error.to_string(), "BUSY_VIEW: 1 files open");
        let error = parse_reply(
            json!({"ok": false, "error": {"code": "ORCHESTRATOR_ERROR", "message": "disk full"}}),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "ORCHESTRATOR_ERROR: disk full");
        assert!(parse_reply(json!({})).is_err());
    }
}
