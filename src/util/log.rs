//! Structured JSON-line events with per-code rate limiting, redaction and
//! rotating log files.
//!
//! ```ignore
//! use tkfs::util::log::event;
//! event!(warn, "OBJECT_IO_ERROR", { "object": hash, attempts: 3 });
//! event!(fatal, ENGINE_POISONED);           // a bare identifier is the code itself
//! event!(info, (code.as_str()), { k: v });  // any other expression: parenthesize it
//! ```
//!
//! Each event is one line `{"at_ms":…,"level":"…","code":"…",<fields…>}` written
//! to the process sink (stderr unless [`set_sink`] or [`install_file`] replaced
//! it; worker stdout is reserved for startup lines). At most [`RATE_LIMIT`]
//! events per code are written per [`RATE_WINDOW`]; the rest are counted and
//! reported as one `{"code":…,"suppressed":N}` line when the next window opens
//! (or on [`flush_suppressed`]). `fatal` events are never suppressed. Field
//! values are serialized with serde; a value that fails to serialize becomes
//! `null`.
//!
//! At most [`MAX_TRACKED_CODES`] codes have their own rate window. When a new
//! code would exceed that, windows whose period has ended are pruned first
//! (their pending `suppressed` summaries are written); if every window is
//! still current, the new code shares the [`OTHER_CODES`] window, so dynamic
//! codes (for example peer-supplied `ErrorWire` codes) cannot grow the map
//! (CCR-WP0.INT-4 item 5). Each line still carries its own code.
//!
//! Lines are formatted under the logger's lock and written after it is
//! released, under the sink's own lock (CCR-WP0.INT-4 item 4): a slow sink
//! never holds up the rate-limit bookkeeping, and every line is written
//! whole. Lines of concurrent events may reach the sink in either order.
//!
//! # Redaction (WP4.2)
//!
//! A log line never carries a pairing code, a per-peer secret or a token
//! (invariant 21):
//! * a field (at any depth) whose name contains `token`, `secret`,
//!   `password`, `passphrase`, `credential`, `verifier`, `pairing_code`,
//!   `authorization`, `cookie` or `private_key` is written as [`REDACTED`]
//!   unless its value is a number, a boolean or null (counters such as
//!   `verifier_generation` stay readable);
//! * a field named `code` whose value is a string that is not shaped like an
//!   event code (`[A-Z0-9_]+`, `*`, or a captured stream's `stdout` and
//!   `stderr`) is redacted too: a pairing code is 64 hex digits, an error
//!   code is SCREAMING_CASE;
//! * every string passed to [`register_secret`] is replaced by [`REDACTED`]
//!   wherever it appears in a line ([`scrub`]).
//!
//! [`redact_line`] applies the same rules to a line read from elsewhere (a
//! worker's stdout, a captured stream, a log tail in a diagnostics bundle).
//!
//! # Files (WP4.2)
//!
//! [`install_file`] makes a [`RotatingFile`] the process sink: one JSON line
//! per event, rotated when the next write would take the file past
//! [`Rotation::max_bytes`] (`log` → `log.1` → … → `log.<files-1>`, the oldest
//! dropped; [`ORCHESTRATOR_LOG_BYTES`] × [`ORCHESTRATOR_LOG_FILES`] by default,
//! `TKFS_LOG_ROTATE_BYTES` and `TKFS_LOG_ROTATE_FILES` override). The
//! process's standard streams are classified first, against the file as it
//! is before any rotation (an inherited log already past the limit is still
//! the log; it rotates once the streams no longer write to it):
//! * a stream that **is** the log file (the desktop starts its daemon with
//!   stdout and stderr appended to `DATA/logs/orchestrator.log`), or that is
//!   missing or the null device, is **captured**: it is redirected into a pipe
//!   whose lines a `log-stdout`/`log-stderr` thread writes into the rotating
//!   file as events with code `stdout`/`stderr` (a JSON object line is kept
//!   whole under `json`, other text under `text`). Raw output then follows the
//!   rotation instead of landing in a renamed (and later deleted) file, and
//!   nothing is discarded. A panic message is written straight to the file by
//!   a panic hook, so it is not lost if the process ends before the capture
//!   thread runs. On Windows the C runtime's descriptors 1 and 2 (`printf`;
//!   they keep the handles of startup) follow into the pipe 2 s later, and
//!   only then is a replaced log-file handle closed, so no descriptor ever
//!   writes through a closed (and possibly reused) handle value;
//! * events are also written to stderr when it is a terminal, a pipe or
//!   another file (a console run, the test harness's `daemon.log`).
//!
//! [`install_daemon_log`] installs `DATA/logs/orchestrator.log`.
//! [`adopt_desktop_log`] is the desktop (GUI) mode adapter: when the
//! process's stderr is a file named `orchestrator.log` in a directory named
//! `logs`, that file becomes the rotating log. [`rotate_if_larger`] is the
//! spawn-time rotation of `worker.log` (`orchestrator::process`).
use serde::Serialize;
use serde_json::Value;
use std::{
    borrow::Cow,
    cell::Cell,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

pub use crate::__tkfs_log_event as event;

pub const RATE_LIMIT: u32 = 20;
pub const RATE_WINDOW: Duration = Duration::from_secs(60);
/// Codes with a rate window of their own (see the module docs).
pub const MAX_TRACKED_CODES: usize = 1024;
/// The shared rate window of codes beyond [`MAX_TRACKED_CODES`]; its
/// `suppressed` summary line carries this as its code.
pub const OTHER_CODES: &str = "*";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Trace => "trace",
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
            Level::Fatal => "fatal",
        }
    }
}

struct Window {
    started: Instant,
    written: u32,
    suppressed: u64,
    level: Level,
}

type Sink = Arc<Mutex<Box<dyn Write + Send>>>;

struct Logger {
    sink: Option<Sink>,
    min_level: Level,
    windows: HashMap<String, Window>,
}

fn logger() -> &'static Mutex<Logger> {
    static LOGGER: OnceLock<Mutex<Logger>> = OnceLock::new();
    LOGGER.get_or_init(|| {
        Mutex::new(Logger {
            sink: None,
            min_level: Level::Info,
            windows: HashMap::new(),
        })
    })
}

/// Replaces the process sink (`None` restores stderr).
pub fn set_sink(sink: Option<Box<dyn Write + Send>>) {
    crate::util::lock::lock(logger()).sink = sink.map(|sink| Arc::new(Mutex::new(sink)));
}

/// Events below this level are dropped (default `Info`).
pub fn set_min_level(level: Level) {
    crate::util::lock::lock(logger()).min_level = level;
}

/// Builds one field for [`emit`]; used by the `event!` macro.
pub fn field<T: Serialize + ?Sized>(key: &'static str, value: &T) -> (&'static str, Value) {
    (key, serde_json::to_value(value).unwrap_or(Value::Null))
}

/// Writes one event unless filtered by level or rate limit; returns whether a
/// line for this event was written.
pub fn emit(level: Level, code: &str, fields: &[(&'static str, Value)]) -> bool {
    let (sink, out, allowed) = {
        let mut guard = crate::util::lock::lock(logger());
        let logger = &mut *guard;
        if level < logger.min_level {
            return false;
        }
        let now = Instant::now();
        let mut out = String::new();
        let key = if logger.windows.contains_key(code) || logger.windows.len() < MAX_TRACKED_CODES {
            code
        } else {
            prune_expired(&mut logger.windows, now, &mut out);
            if logger.windows.len() < MAX_TRACKED_CODES {
                code
            } else {
                OTHER_CODES
            }
        };
        let mut pending = None;
        let window = logger
            .windows
            .entry(key.to_owned())
            .or_insert_with(|| Window {
                started: now,
                written: 0,
                suppressed: 0,
                level,
            });
        if now.saturating_duration_since(window.started) >= RATE_WINDOW {
            if window.suppressed > 0 {
                pending = Some((window.level, window.suppressed));
            }
            *window = Window {
                started: now,
                written: 0,
                suppressed: 0,
                level,
            };
        }
        let allowed = level == Level::Fatal || window.written < RATE_LIMIT;
        if allowed {
            window.written = window.written.saturating_add(1);
        } else {
            window.suppressed += 1;
            window.level = window.level.max(level);
        }
        if let Some((level, suppressed)) = pending {
            out.push_str(&line(
                level,
                key,
                &[("suppressed", Value::from(suppressed))],
            ));
        }
        if allowed {
            out.push_str(&line(level, code, fields));
        }
        (logger.sink.clone(), out, allowed)
    };
    if !out.is_empty() {
        write(sink.as_ref(), &scrub(&out));
    }
    allowed
}

/// Drops every window whose period has ended, appending the `suppressed`
/// summary of those that still had one to `out`.
fn prune_expired(windows: &mut HashMap<String, Window>, now: Instant, out: &mut String) {
    let mut summaries = vec![];
    windows.retain(|code, window| {
        let current = now.saturating_duration_since(window.started) < RATE_WINDOW;
        if !current && window.suppressed > 0 {
            summaries.push((code.clone(), window.level, window.suppressed));
        }
        current
    });
    summaries.sort();
    for (code, level, suppressed) in summaries {
        out.push_str(&line(
            level,
            &code,
            &[("suppressed", Value::from(suppressed))],
        ));
    }
}

/// How many codes currently have a rate window (at most
/// [`MAX_TRACKED_CODES`] plus the shared [`OTHER_CODES`] window).
pub fn tracked_codes() -> usize {
    crate::util::lock::lock(logger()).windows.len()
}

/// Writes a `suppressed` summary line for every code with suppressed events in
/// its current window, and resets those counters.
pub fn flush_suppressed() {
    let (sink, out) = {
        let mut guard = crate::util::lock::lock(logger());
        let logger = &mut *guard;
        let mut out = String::new();
        let mut codes: Vec<_> = logger
            .windows
            .iter_mut()
            .filter(|(_, window)| window.suppressed > 0)
            .map(|(code, window)| {
                let summary = (code.clone(), window.level, window.suppressed);
                window.suppressed = 0;
                summary
            })
            .collect();
        codes.sort();
        for (code, level, suppressed) in codes {
            out.push_str(&line(
                level,
                &code,
                &[("suppressed", Value::from(suppressed))],
            ));
        }
        (logger.sink.clone(), out)
    };
    if !out.is_empty() {
        write(sink.as_ref(), &scrub(&out));
    }
}

/// One event line (with its newline); field values are redacted
/// ([`redact_value`]).
fn line(level: Level, code: &str, fields: &[(&'static str, Value)]) -> String {
    let mut out = format!(
        "{{\"at_ms\":{},\"level\":\"{}\",\"code\":{}",
        crate::util::time::now_ms(),
        level.as_str(),
        Value::from(code)
    );
    for (key, value) in fields {
        out.push(',');
        out.push_str(&Value::from(*key).to_string());
        out.push(':');
        if hides(key, value) {
            out.push_str(&Value::from(REDACTED).to_string());
        } else if needs_redaction(value, 0) {
            let mut value = value.clone();
            redact_value(&mut value);
            out.push_str(&value.to_string());
        } else {
            out.push_str(&value.to_string());
        }
    }
    out.push_str("}\n");
    out
}

/// Writes `text` to `sink` (stderr when `None`) under the sink's own lock,
/// never under the logger's.
fn write(sink: Option<&Sink>, text: &str) {
    match sink {
        Some(sink) => {
            let mut sink = crate::util::lock::lock(sink);
            let _ = sink.write_all(text.as_bytes());
            let _ = sink.flush();
        }
        None => {
            let mut stderr = std::io::stderr().lock();
            let _ = stderr.write_all(text.as_bytes());
            let _ = stderr.flush();
        }
    }
}

// ---------------------------------------------------------------------------
// Redaction

/// What a redacted value is replaced with.
pub const REDACTED: &str = "<redacted>";

/// Field-name fragments (lower case) whose string, array or object values are
/// never written (see the module docs).
pub const SENSITIVE_KEY_PARTS: &[&str] = &[
    "token",
    "secret",
    "password",
    "passphrase",
    "credential",
    "verifier",
    "pairing_code",
    "authorization",
    "cookie",
    "private_key",
];

/// Strings shorter than this are never registered as secrets (scrubbing them
/// would mangle unrelated text).
pub const MIN_SECRET_LEN: usize = 8;
/// At most this many registered secrets are kept (the oldest is dropped).
pub const MAX_SECRETS: usize = 1024;
/// Redaction descends at most this deep; anything deeper is redacted whole.
const MAX_REDACT_DEPTH: usize = 64;

/// Whether a field named `key` is sensitive (see the module docs).
pub fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    SENSITIVE_KEY_PARTS.iter().any(|part| key.contains(part))
}

/// Whether `text` is shaped like an event code: an error code (`[A-Z0-9_]+`),
/// [`OTHER_CODES`], or a captured stream's name (`stdout`, `stderr`).
fn code_shaped(text: &str) -> bool {
    matches!(text, OTHER_CODES | "stdout" | "stderr")
        || (!text.is_empty()
            && text
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'))
}

/// Whether the value of a field named `key` is replaced as a whole.
fn hides(key: &str, value: &Value) -> bool {
    if matches!(value, Value::Null | Value::Bool(_) | Value::Number(_)) {
        return false;
    }
    if key.eq_ignore_ascii_case("code") {
        return value.as_str().is_some_and(|code| !code_shaped(code));
    }
    is_sensitive_key(key)
}

fn needs_redaction(value: &Value, depth: usize) -> bool {
    match value {
        Value::Object(map) => {
            depth >= MAX_REDACT_DEPTH
                || map
                    .iter()
                    .any(|(key, value)| hides(key, value) || needs_redaction(value, depth + 1))
        }
        Value::Array(items) => {
            depth >= MAX_REDACT_DEPTH || items.iter().any(|item| needs_redaction(item, depth + 1))
        }
        _ => false,
    }
}

/// Redacts sensitive fields of `value` in place, at any depth; returns whether
/// anything changed.
pub fn redact_value(value: &mut Value) -> bool {
    redact_at(value, 0)
}

fn redact_at(value: &mut Value, depth: usize) -> bool {
    if depth >= MAX_REDACT_DEPTH && matches!(value, Value::Object(_) | Value::Array(_)) {
        *value = Value::from(REDACTED);
        return true;
    }
    let mut changed = false;
    match value {
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                if hides(key, value) {
                    *value = Value::from(REDACTED);
                    changed = true;
                } else {
                    changed |= redact_at(value, depth + 1);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                changed |= redact_at(item, depth + 1);
            }
        }
        _ => {}
    }
    changed
}

fn secrets() -> &'static Mutex<Vec<String>> {
    static SECRETS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    SECRETS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Registers a secret (a pairing code, a token, a per-peer secret): from now
/// on it is replaced by [`REDACTED`] in every line this process logs. Strings
/// shorter than [`MIN_SECRET_LEN`] are ignored.
pub fn register_secret(secret: &str) {
    let secret = secret.trim();
    if secret.len() < MIN_SECRET_LEN {
        return;
    }
    let mut secrets = crate::util::lock::lock(secrets());
    if secrets.iter().any(|known| known == secret) {
        return;
    }
    if secrets.len() >= MAX_SECRETS {
        secrets.remove(0);
    }
    secrets.push(secret.to_owned());
}

/// Forgets a secret registered with [`register_secret`].
pub fn forget_secret(secret: &str) {
    crate::util::lock::lock(secrets()).retain(|known| known != secret.trim());
}

/// `text` with every registered secret replaced by [`REDACTED`].
pub fn scrub(text: &str) -> Cow<'_, str> {
    let secrets = crate::util::lock::lock(secrets());
    scrub_with(text, &secrets)
}

/// `text` with every string of `secrets` (at least [`MIN_SECRET_LEN`] long)
/// replaced by [`REDACTED`].
pub fn scrub_with<'a, S: AsRef<str>>(text: &'a str, secrets: &[S]) -> Cow<'a, str> {
    let mut out = Cow::Borrowed(text);
    for secret in secrets {
        let secret = secret.as_ref();
        if secret.len() >= MIN_SECRET_LEN && out.contains(secret) {
            out = Cow::Owned(out.replace(secret, REDACTED));
        }
    }
    out
}

/// One line read from elsewhere, redacted: a JSON object line has its
/// sensitive fields replaced ([`redact_value`]; it is re-serialized only when
/// something changed), then registered secrets are scrubbed.
pub fn redact_line(line: &str) -> Cow<'_, str> {
    let trimmed = line.trim_start();
    if trimmed.starts_with('{')
        && let Ok(mut value) = serde_json::from_str::<Value>(trimmed)
        && value.is_object()
        && redact_value(&mut value)
    {
        return Cow::Owned(scrub(&value.to_string()).into_owned());
    }
    scrub(line)
}

// ---------------------------------------------------------------------------
// Rotating files

/// The daemon log's directory under the data directory.
pub const LOGS_DIRECTORY: &str = "logs";
/// The daemon log's file name (`DATA/logs/orchestrator.log`).
pub const ORCHESTRATOR_LOG: &str = "orchestrator.log";
/// The daemon log rotates at 10 MiB…
pub const ORCHESTRATOR_LOG_BYTES: u64 = 10 * 1024 * 1024;
/// …and keeps 5 files (`orchestrator.log` and `.1` to `.4`).
pub const ORCHESTRATOR_LOG_FILES: usize = 5;
/// Environment overrides of [`Rotation::from_env`].
pub const ROTATE_BYTES_ENV: &str = "TKFS_LOG_ROTATE_BYTES";
pub const ROTATE_FILES_ENV: &str = "TKFS_LOG_ROTATE_FILES";
/// The smallest rotation size an override may set.
pub const MIN_ROTATE_BYTES: u64 = 256;
/// The most files an override may keep.
pub const MAX_ROTATE_FILES: usize = 100;
/// The longest line a capture thread writes as one event (longer output is
/// split).
pub const MAX_CAPTURED_LINE: usize = 64 * 1024;

/// When a log file rotates and how many files it keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Rotation {
    /// A write that would take the file past this many bytes rotates it
    /// first (a single larger write still goes to a fresh file whole).
    pub max_bytes: u64,
    /// Files kept, the live one included (`log`, `log.1` … `log.<files-1>`);
    /// 1 keeps no history (the file is removed at rotation).
    pub files: usize,
}

impl Default for Rotation {
    fn default() -> Self {
        Self {
            max_bytes: ORCHESTRATOR_LOG_BYTES,
            files: ORCHESTRATOR_LOG_FILES,
        }
    }
}

impl Rotation {
    /// The default, with `TKFS_LOG_ROTATE_BYTES` (at least
    /// [`MIN_ROTATE_BYTES`]) and `TKFS_LOG_ROTATE_FILES` (1 to
    /// [`MAX_ROTATE_FILES`]) applied when they parse.
    pub fn from_env() -> Self {
        let mut rotation = Self::default();
        if let Some(bytes) = std::env::var(ROTATE_BYTES_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
        {
            rotation.max_bytes = bytes.max(MIN_ROTATE_BYTES);
        }
        if let Some(files) = std::env::var(ROTATE_FILES_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
        {
            rotation.files = files.clamp(1, MAX_ROTATE_FILES);
        }
        rotation
    }
}

/// `path.n` (`orchestrator.log.1`); `n == 0` is `path` itself.
pub fn rotated_path(path: &Path, n: usize) -> PathBuf {
    if n == 0 {
        return path.to_path_buf();
    }
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{n}"));
    PathBuf::from(name)
}

/// The files of a rotated log, newest first: `path`, `path.1`, … (only those
/// that exist, up to `files`).
pub fn rotated_files(path: &Path, files: usize) -> Vec<PathBuf> {
    (0..files.max(1))
        .map(|n| rotated_path(path, n))
        .filter(|path| path.is_file())
        .collect()
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

fn rename_if_exists(from: &Path, to: &Path) -> io::Result<()> {
    match fs::rename(from, to) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Where [`rotate_files`] moves the live file while it makes room for it in
/// the history (`path.rotating`).
fn aside_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".rotating");
    PathBuf::from(name)
}

/// Rotates `path`: it becomes `path.1`, and the history moves up by one
/// (`path.k` → `path.k+1`) as far as the first missing generation; when none
/// is missing, the oldest kept file (`path.<files-1>`) is removed first. With
/// `files == 1`, `path` is removed. A missing `path` is not rotated.
///
/// The live file is moved aside (`path.rotating`) **before** any older file
/// moves: when it cannot be renamed (another process holds it without delete
/// sharing), nothing has changed, so a writer that retries later never loses
/// history to failed attempts. When the history cannot move (an older file is
/// held), the live file is put back (its writer keeps appending) and the error
/// is returned; the next attempt fills the gap the partial move left instead
/// of removing another old file. A live file left aside by an interrupted
/// rotation is placed into the history first.
pub fn rotate_files(path: &Path, files: usize) -> io::Result<()> {
    let files = files.max(1);
    if files == 1 {
        return remove_if_exists(path);
    }
    let aside = aside_path(path);
    if aside.is_file() {
        make_room(path, files)?;
        fs::rename(&aside, rotated_path(path, 1))?;
    }
    match fs::rename(path, &aside) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    let placed = make_room(path, files).and_then(|()| fs::rename(&aside, rotated_path(path, 1)));
    if let Err(error) = placed {
        // A failed put-back leaves it aside, for the next rotation to place.
        let _ = fs::rename(&aside, path);
        return Err(error);
    }
    Ok(())
}

/// Frees `path.1` for [`rotate_files`] (`files` ≥ 2): the generations below
/// the first missing one move up by one; when none is missing, the oldest
/// kept one is removed first.
fn make_room(path: &Path, files: usize) -> io::Result<()> {
    let last = files.max(2) - 1;
    let top = match (1..=last).find(|&n| !rotated_path(path, n).exists()) {
        Some(free) => free,
        None => {
            remove_if_exists(&rotated_path(path, last))?;
            last
        }
    };
    for n in (1..top).rev() {
        rename_if_exists(&rotated_path(path, n), &rotated_path(path, n + 1))?;
    }
    Ok(())
}

/// Rotates `path` ([`rotate_files`]) if it is larger than `max_bytes`;
/// returns whether it rotated. A missing file is not rotated.
pub fn rotate_if_larger(path: &Path, max_bytes: u64, files: usize) -> io::Result<bool> {
    match fs::metadata(path) {
        Ok(meta) if meta.len() > max_bytes => rotate_files(path, files).map(|()| true),
        Ok(_) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Whether the file at `path` ends with a newline (an unreadable file counts
/// as one: nothing is added).
fn ends_with_newline(path: &Path) -> bool {
    use std::io::{Seek, SeekFrom};
    let Ok(mut file) = File::open(path) else {
        return true;
    };
    let mut last = [0u8; 1];
    file.seek(SeekFrom::End(-1)).is_err() || file.read_exact(&mut last).is_err() || last[0] == b'\n'
}

/// An append-only log file that rotates by size ([`Rotation`]).
///
/// Writes are best effort: a failed rotation (another process holds the file
/// without delete sharing) leaves the live file in place, notes the failure
/// in it, and is retried after another `max_bytes / 8` (at least 64 KiB) were
/// written (no history is lost to failed attempts, see [`rotate_files`]); a
/// failed write drops that write and reopens the file at the next one. Each `write` call lands whole in one file, so callers that write whole
/// lines get whole lines. A file that does not end with a newline when it is
/// opened (a partial line another writer left) gets one first.
#[derive(Debug)]
pub struct RotatingFile {
    path: PathBuf,
    rotation: Rotation,
    file: Option<File>,
    size: u64,
    /// After a failed rotation: the size at which it is tried again.
    retry_at: u64,
    rotations: u64,
}

impl RotatingFile {
    /// Opens (appends to) `path`, creating its directory; a file already at
    /// or past the limit is rotated first.
    pub fn open(path: &Path, rotation: Rotation) -> io::Result<Self> {
        let mut this = Self::open_unrotated(path, rotation)?;
        this.rotate_if_full();
        Ok(this)
    }

    /// [`open`](Self::open) without the rotation: the live file is the one
    /// at `path` now, whatever its size ([`install_file`] compares the
    /// standard streams with this very file before it may rotate).
    fn open_unrotated(path: &Path, rotation: Rotation) -> io::Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let rotation = Rotation {
            max_bytes: rotation.max_bytes.max(1),
            files: rotation.files.max(1),
        };
        let mut this = Self {
            path: path.to_path_buf(),
            rotation,
            file: None,
            size: 0,
            retry_at: 0,
            rotations: 0,
        };
        this.reopen()?;
        Ok(this)
    }

    /// Rotates a live file already at or past the limit (unless a failed
    /// rotation is not due for its retry yet).
    fn rotate_if_full(&mut self) {
        if self.size >= self.rotation.max_bytes && self.size >= self.retry_at {
            self.rotate();
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn rotation(&self) -> Rotation {
        self.rotation
    }

    /// Bytes in the live file (as written through this handle).
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Rotations done by this writer.
    pub fn rotations(&self) -> u64 {
        self.rotations
    }

    /// The open live file (`None` after a failed reopen, until the next write).
    pub fn file(&self) -> Option<&File> {
        self.file.as_ref()
    }

    fn reopen(&mut self) -> io::Result<()> {
        let open = |path: &Path| OpenOptions::new().create(true).append(true).open(path);
        let mut file = match open(&self.path) {
            Ok(file) => file,
            // The directory went away (a user cleared `DATA/logs` while the
            // daemon runs): made again, once, so the log does not stop for
            // the rest of the process (WP4.INT, the WP4.2 review's minor).
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
                    fs::create_dir_all(parent)?;
                }
                open(&self.path)?
            }
            Err(error) => return Err(error),
        };
        self.size = file.metadata().map(|m| m.len()).unwrap_or(0);
        // Output written before (a partial line of a process that wrote the
        // file directly) must not swallow the first line written now.
        if self.size > 0 && !ends_with_newline(&self.path) && file.write_all(b"\n").is_ok() {
            self.size += 1;
        }
        self.file = Some(file);
        Ok(())
    }

    fn rotate(&mut self) {
        // Our own handle is closed first: the rename never waits on us.
        self.file = None;
        let failure = rotate_files(&self.path, self.rotation.files).err();
        match &failure {
            None => {
                self.retry_at = 0;
                self.rotations += 1;
            }
            Some(_) => {
                let step = (self.rotation.max_bytes / 8).max(64 * 1024);
                self.retry_at = self.size.saturating_add(step);
            }
        }
        if self.reopen().is_err() {
            return;
        }
        if let Some(error) = failure {
            let note = line(
                Level::Warn,
                crate::model::errors::UNKNOWN_CODE,
                &[
                    (
                        "detail",
                        Value::from("log rotation failed; still appending"),
                    ),
                    ("error", Value::from(error.to_string())),
                ],
            );
            let _ = self.append(note.as_bytes());
        }
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let Some(file) = self.file.as_mut() else {
            return Err(io::Error::other("the log file is not open"));
        };
        match file.write_all(bytes) {
            Ok(()) => {
                self.size = self.size.saturating_add(bytes.len() as u64);
                Ok(())
            }
            Err(error) => {
                self.file = None;
                Err(error)
            }
        }
    }
}

impl Write for RotatingFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.file.is_none() {
            self.reopen()?;
        }
        if self.size > 0
            && self.size.saturating_add(bytes.len() as u64) > self.rotation.max_bytes
            && self.size >= self.retry_at
        {
            self.rotate();
            if self.file.is_none() {
                self.reopen()?;
            }
        }
        self.append(bytes)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

/// The installed process log ([`install_file`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FileLog {
    pub path: PathBuf,
    pub rotation: Rotation,
    /// Standard output is redirected into the log (it was the log file
    /// itself, missing, or the null device).
    pub captured_stdout: bool,
    /// The same for standard error.
    pub captured_stderr: bool,
    /// Events are also written to standard error (a terminal, a pipe or
    /// another file).
    pub tee_stderr: bool,
}

/// The installed process log: published once, read without a lock.
static INSTALLED: OnceLock<FileLog> = OnceLock::new();

/// Serializes [`install_file`] calls only (the install redirects the
/// standard streams, once per process). It guards no data: [`file_log`] and
/// every other reader use [`INSTALLED`] and never wait for an install.
static INSTALLING: Mutex<()> = Mutex::new(());

/// The process log installed by [`install_file`], if any.
pub fn file_log() -> Option<FileLog> {
    INSTALLED.get().cloned()
}

/// `DATA/logs/orchestrator.log`.
pub fn daemon_log_path(data_directory: &Path) -> PathBuf {
    data_directory.join(LOGS_DIRECTORY).join(ORCHESTRATOR_LOG)
}

/// Installs `DATA/logs/orchestrator.log` ([`daemon_log_path`]) as the
/// process log with [`Rotation::from_env`] (see [`install_file`]).
pub fn install_daemon_log(data_directory: &Path) -> io::Result<FileLog> {
    install_file(&daemon_log_path(data_directory), Rotation::from_env())
}

/// Desktop (GUI) mode: when this process's stderr is a file named
/// `orchestrator.log` in a directory named `logs` (the desktop starts its
/// daemon with stdout and stderr appended to `DATA/logs/orchestrator.log`),
/// that file becomes the rotating process log ([`install_file`], which then
/// captures both streams). Returns the installed log (also one installed
/// earlier), or `None` when stderr is anything else.
pub fn adopt_desktop_log() -> Option<FileLog> {
    if let Some(log) = file_log() {
        return Some(log);
    }
    let path = stdio::file_path(Stream::Stderr)?;
    let named = |path: &Path, name: &str| {
        path.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.eq_ignore_ascii_case(name))
    };
    let in_logs = path
        .parent()
        .is_some_and(|parent| named(parent, LOGS_DIRECTORY));
    if !named(&path, ORCHESTRATOR_LOG) || !in_logs {
        return None;
    }
    install_file(&path, Rotation::from_env()).ok()
}

/// Makes a [`RotatingFile`] at `path` the process sink (see the module docs:
/// standard streams that are the log file, missing or the null device are
/// captured into it; events are teed to any other stderr). The first install
/// wins: a later call returns the installed log unchanged.
///
/// The streams are classified against the file as it is **before** any
/// rotation: an inherited log already at or past the limit (the desktop's
/// `orchestrator.log` after an upgrade from unbounded raw output) is still
/// recognized as the log, captured, and only then rotated, so nothing keeps
/// writing to the renamed file.
pub fn install_file(path: &Path, rotation: Rotation) -> io::Result<FileLog> {
    if let Some(log) = file_log() {
        return Ok(log);
    }
    let _installing = crate::util::lock::lock(&INSTALLING);
    if let Some(log) = file_log() {
        return Ok(log);
    }
    let file = RotatingFile::open_unrotated(path, rotation)?;
    let (out_kind, err_kind) = match file.file() {
        Some(handle) => (
            stdio::classify(Stream::Stdout, handle),
            stdio::classify(Stream::Stderr, handle),
        ),
        None => (StreamKind::Other, StreamKind::Other),
    };
    let rotation = file.rotation();
    let file = Arc::new(Mutex::new(file));
    let captured_stdout = out_kind.captured() && capture(Stream::Stdout, out_kind, &file).is_ok();
    let captured_stderr = err_kind.captured() && capture(Stream::Stderr, err_kind, &file).is_ok();
    // The streams that were the file now write to their pipes: an oversized
    // file can rotate without anything still appending to it.
    crate::util::lock::lock(&file).rotate_if_full();
    if captured_stderr {
        install_panic_hook(&file);
    }
    let tee_stderr = matches!(err_kind, StreamKind::Terminal | StreamKind::Other);
    set_sink(Some(Box::new(FileSink {
        file: file.clone(),
        tee: tee_stderr,
    })));
    let log = FileLog {
        path: path.to_path_buf(),
        rotation,
        captured_stdout,
        captured_stderr,
        tee_stderr,
    };
    Ok(INSTALLED.get_or_init(|| log).clone())
}

/// The process sink of an installed file log.
struct FileSink {
    file: Arc<Mutex<RotatingFile>>,
    tee: bool,
}

impl Write for FileSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let _ = write_log(&self.file, bytes);
        if self.tee {
            let mut stderr = std::io::stderr().lock();
            let _ = stderr.write_all(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.tee {
            let _ = std::io::stderr().flush();
        }
        Ok(())
    }
}

/// One captured line as an event (`code` = the stream's name): a JSON object
/// under `json`, anything else under `text`; redacted and scrubbed.
pub fn captured_line(stream: &str, text: &str) -> String {
    let level = if stream == "stderr" {
        Level::Warn
    } else {
        Level::Info
    };
    let trimmed = text.trim();
    let field = match serde_json::from_str::<Value>(trimmed) {
        Ok(value) if value.is_object() => ("json", value),
        _ => ("text", Value::from(text)),
    };
    scrub(&line(level, stream, &[field])).into_owned()
}

/// Redirects `stream` into a pipe whose lines a thread writes into `file`.
fn capture(stream: Stream, kind: StreamKind, file: &Arc<Mutex<RotatingFile>>) -> io::Result<()> {
    let (reader, previous) = stdio::redirect(stream)?;
    let file = file.clone();
    let spawned = std::thread::Builder::new()
        .name(format!("log-{}", stream.name()))
        .spawn(move || pump(reader, stream.name(), &file));
    if let Err(error) = spawned {
        // Nothing reads the pipe: writers would block once it fills. Put the
        // previous stream back.
        stdio::restore(stream, previous);
        return Err(error);
    }
    // The C runtime's descriptor follows the stream into the pipe, and a
    // replaced handle to the log file is released (Windows; see `stdio`).
    stdio::settle_later(stream, previous, kind == StreamKind::LogFile);
    Ok(())
}

/// Copies lines from a captured stream into the log until the pipe closes.
/// It never writes to stdout or stderr (they are the pipe it reads).
fn pump(reader: File, stream: &'static str, file: &Mutex<RotatingFile>) {
    let mut reader = BufReader::with_capacity(64 * 1024, reader);
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        match reader
            .by_ref()
            .take(MAX_CAPTURED_LINE as u64)
            .read_until(b'\n', &mut buffer)
        {
            Ok(0) => return,
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return,
        }
        let mut end = buffer.len();
        while end > 0 && matches!(buffer[end - 1], b'\n' | b'\r') {
            end -= 1;
        }
        let text = String::from_utf8_lossy(&buffer[..end]);
        if text.trim().is_empty() {
            continue;
        }
        let line = std::panic::catch_unwind(|| captured_line(stream, &text));
        if let Ok(line) = line {
            let _ = write_log(file, line.as_bytes());
        }
    }
}

thread_local! {
    /// Set while this thread holds an installed log's mutex ([`write_log`]).
    static IN_LOG_WRITE: Cell<bool> = const { Cell::new(false) };
}

/// Writes `bytes` to the installed log under its mutex, marking this thread
/// as inside a log write meanwhile (the panic hook checks it).
fn write_log(file: &Mutex<RotatingFile>, bytes: &[u8]) -> io::Result<()> {
    /// Clears the mark when the write ends, also by unwinding.
    struct Writing;
    impl Drop for Writing {
        fn drop(&mut self) {
            IN_LOG_WRITE.with(|flag| flag.set(false));
        }
    }
    let mut file = crate::util::lock::lock(file);
    IN_LOG_WRITE.with(|flag| flag.set(true));
    let _writing = Writing;
    file.write_all(bytes)
}

/// Writes panic messages straight into the log (the captured stderr would
/// carry them only if the capture thread ran before the process ended).
fn install_panic_hook(file: &Arc<Mutex<RotatingFile>>) {
    let file = Arc::downgrade(file);
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let message = payload
            .downcast_ref::<&str>()
            .map(|s| (*s).to_owned())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "Box<dyn Any>".to_owned());
        let thread = std::thread::current();
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()));
        let mut fields = vec![
            ("panic", Value::from(message)),
            ("thread", Value::from(thread.name().unwrap_or("<unnamed>"))),
            ("location", Value::from(location)),
        ];
        // As the default hook: a backtrace when RUST_BACKTRACE asks for one.
        let backtrace = std::backtrace::Backtrace::capture();
        if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            fields.push(("backtrace", Value::from(backtrace.to_string())));
        }
        let text = scrub(&line(Level::Error, "stderr", &fields)).into_owned();
        // A panic inside a log write (this thread holds the log's mutex) must
        // not deadlock on it: then the (captured) stderr gets the line.
        let written = !IN_LOG_WRITE.with(Cell::get)
            && file
                .upgrade()
                .is_some_and(|file| write_log(&file, text.as_bytes()).is_ok());
        if !written {
            let _ = std::io::stderr().write_all(text.as_bytes());
        }
    }));
}

/// A standard output stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    fn name(self) -> &'static str {
        match self {
            Stream::Stdout => "stdout",
            Stream::Stderr => "stderr",
        }
    }
}

/// What a standard stream is connected to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamKind {
    /// A console or terminal.
    Terminal,
    /// The log file itself.
    LogFile,
    /// No stream (closed or never set).
    Missing,
    /// The null device.
    Null,
    /// A pipe or another file.
    Other,
}

impl StreamKind {
    fn captured(self) -> bool {
        matches!(
            self,
            StreamKind::LogFile | StreamKind::Missing | StreamKind::Null
        )
    }
}

#[cfg(windows)]
mod stdio {
    use super::{Stream, StreamKind};
    use std::{
        ffi::{OsString, c_void},
        fs::File,
        io::{self, Write},
        os::windows::{
            ffi::OsStringExt,
            io::{AsRawHandle, FromRawHandle},
        },
        path::PathBuf,
        time::Duration,
    };

    type Handle = *mut c_void;
    const STD_OUTPUT_HANDLE: u32 = -11i32 as u32;
    const STD_ERROR_HANDLE: u32 = -12i32 as u32;
    const FILE_TYPE_UNKNOWN: u32 = 0;
    const FILE_TYPE_DISK: u32 = 1;
    const FILE_TYPE_CHAR: u32 = 2;
    const PIPE_BYTES: u32 = 64 * 1024;
    /// A switch is settled ([`settle_later`]) this long after it, so a caller
    /// that read the previous handle just before it (a child spawn inheriting
    /// the stream) is done with it.
    const SETTLE_AFTER: Duration = Duration::from_secs(2);
    const DUPLICATE_SAME_ACCESS: u32 = 2;
    const HANDLE_FLAG_INHERIT: u32 = 1;
    /// `_O_TEXT`: the mode the C runtime gives its standard descriptors.
    const O_TEXT: i32 = 0x4000;

    /// The C runtime's invalid-parameter handler (`_invalid_parameter_handler`).
    type InvalidParameterHandler =
        Option<unsafe extern "C" fn(*const u16, *const u16, *const u16, u32, usize)>;

    #[repr(C)]
    #[derive(Default)]
    struct FileInfo {
        attributes: u32,
        times: [u32; 6],
        volume: u32,
        size_high: u32,
        size_low: u32,
        links: u32,
        index_high: u32,
        index_low: u32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetStdHandle(id: u32) -> Handle;
        fn SetStdHandle(id: u32, handle: Handle) -> i32;
        fn GetFileType(handle: Handle) -> u32;
        fn GetConsoleMode(handle: Handle, mode: *mut u32) -> i32;
        fn GetFileInformationByHandle(handle: Handle, info: *mut FileInfo) -> i32;
        fn GetFinalPathNameByHandleW(handle: Handle, path: *mut u16, size: u32, flags: u32) -> u32;
        fn CreatePipe(
            read: *mut Handle,
            write: *mut Handle,
            attributes: *const c_void,
            size: u32,
        ) -> i32;
        fn CloseHandle(handle: Handle) -> i32;
        fn GetCurrentProcess() -> Handle;
        fn DuplicateHandle(
            source_process: Handle,
            source: Handle,
            target_process: Handle,
            target: *mut Handle,
            access: u32,
            inherit: i32,
            options: u32,
        ) -> i32;
        fn SetHandleInformation(handle: Handle, mask: u32, flags: u32) -> i32;
    }

    // The C runtime's low-level I/O (the UCRT every C object and Rust's
    // standard library link): `printf` and every C write end at its
    // descriptors 1 and 2, which hold the standard handles of startup and
    // never see `SetStdHandle`.
    unsafe extern "C" {
        fn _get_osfhandle(fd: i32) -> isize;
        fn _open_osfhandle(handle: isize, flags: i32) -> i32;
        fn _dup2(from: i32, to: i32) -> i32;
        fn _close(fd: i32) -> i32;
        fn _set_thread_local_invalid_parameter_handler(
            handler: InvalidParameterHandler,
        ) -> InvalidParameterHandler;
    }

    fn id(stream: Stream) -> u32 {
        match stream {
            Stream::Stdout => STD_OUTPUT_HANDLE,
            Stream::Stderr => STD_ERROR_HANDLE,
        }
    }

    /// The C runtime's descriptor of `stream`.
    fn descriptor(stream: Stream) -> i32 {
        match stream {
            Stream::Stdout => 1,
            Stream::Stderr => 2,
        }
    }

    /// Holds Rust's lock on `stream`: `print!`/`eprint!` writers wait
    /// meanwhile, so none writes through a handle it read before a switch,
    /// or meets the moment `_dup2` leaves the standard handle unset.
    fn writers(
        stream: Stream,
    ) -> (
        Option<io::StdoutLock<'static>>,
        Option<io::StderrLock<'static>>,
    ) {
        match stream {
            Stream::Stdout => (Some(io::stdout().lock()), None),
            Stream::Stderr => (None, Some(io::stderr().lock())),
        }
    }

    fn std_handle(stream: Stream) -> Option<Handle> {
        // SAFETY: reads this process's standard handle table.
        let handle = unsafe { GetStdHandle(id(stream)) };
        (!handle.is_null() && handle as isize != -1).then_some(handle)
    }

    fn identity(handle: Handle) -> Option<(u32, u64)> {
        let mut info = FileInfo::default();
        // SAFETY: `info` is a writable BY_HANDLE_FILE_INFORMATION.
        if unsafe { GetFileInformationByHandle(handle, &mut info) } == 0 {
            return None;
        }
        Some((
            info.volume,
            (u64::from(info.index_high) << 32) | u64::from(info.index_low),
        ))
    }

    pub(super) fn classify(stream: Stream, log: &File) -> StreamKind {
        let Some(handle) = std_handle(stream) else {
            return StreamKind::Missing;
        };
        // SAFETY: plain queries on a handle of this process.
        match unsafe { GetFileType(handle) } {
            FILE_TYPE_DISK => {
                let ours = identity(log.as_raw_handle() as Handle);
                match (identity(handle), ours) {
                    (Some(a), Some(b)) if a == b => StreamKind::LogFile,
                    _ => StreamKind::Other,
                }
            }
            FILE_TYPE_CHAR => {
                let mut mode = 0u32;
                // SAFETY: `mode` is writable; the call only reads the handle.
                if unsafe { GetConsoleMode(handle, &mut mode) } != 0 {
                    StreamKind::Terminal
                } else {
                    StreamKind::Null
                }
            }
            FILE_TYPE_UNKNOWN if io::Error::last_os_error().raw_os_error() != Some(0) => {
                StreamKind::Missing
            }
            _ => StreamKind::Other,
        }
    }

    /// The path of the regular file `stream` writes to, if it is one.
    pub(super) fn file_path(stream: Stream) -> Option<PathBuf> {
        let handle = std_handle(stream)?;
        // SAFETY: a query on a handle of this process.
        if unsafe { GetFileType(handle) } != FILE_TYPE_DISK {
            return None;
        }
        let mut buffer = vec![0u16; 32 * 1024];
        // SAFETY: `buffer` holds `len` writable UTF-16 units.
        let len = unsafe {
            GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), buffer.len() as u32, 0)
        } as usize;
        if len == 0 || len >= buffer.len() {
            return None;
        }
        let path = OsString::from_wide(&buffer[..len]);
        let text = path.to_string_lossy();
        // `\\?\C:\…` → `C:\…` (a UNC path keeps its prefix).
        match text.strip_prefix(r"\\?\") {
            Some(rest) if rest.as_bytes().get(1) == Some(&b':') => Some(PathBuf::from(rest)),
            _ => Some(PathBuf::from(path)),
        }
    }

    /// Points `stream` at a new pipe (never inherited by children) and
    /// returns its read end and the previous handle.
    pub(super) fn redirect(stream: Stream) -> io::Result<(File, usize)> {
        let mut read: Handle = std::ptr::null_mut();
        let mut write: Handle = std::ptr::null_mut();
        // SAFETY: both out-pointers are valid; no security attributes, so
        // neither end is inheritable.
        if unsafe { CreatePipe(&mut read, &mut write, std::ptr::null(), PIPE_BYTES) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let _ = io::stdout().flush();
        let _ = io::stderr().flush();
        let _writers = writers(stream);
        if stream == Stream::Stdout {
            // Anything buffered since, under the lock (stderr is unbuffered).
            let _ = io::stdout().flush();
        }
        let previous = std_handle(stream).map_or(0, |handle| handle as usize);
        // SAFETY: `write` is a valid handle this process owns from now on
        // (as the standard handle); `read` is owned by the returned File.
        unsafe {
            if SetStdHandle(id(stream), write) == 0 {
                let error = io::Error::last_os_error();
                CloseHandle(read);
                CloseHandle(write);
                return Err(error);
            }
            // The replaced handle (an inherited log file) stays open until
            // the switch settles: a child spawned meanwhile must not inherit
            // it as a leaked handle (Rust's `Command` passes every
            // inheritable handle; the streams it gives a child are
            // duplicated for it anyway). WP4.INT, the WP4.2 review's minor.
            if previous != 0 && previous != usize::MAX {
                SetHandleInformation(previous as Handle, HANDLE_FLAG_INHERIT, 0);
            }
            Ok((File::from_raw_handle(read as _), previous))
        }
    }

    /// Puts a previous handle back (the capture could not start).
    pub(super) fn restore(stream: Stream, previous: usize) {
        // SAFETY: `previous` was this stream's handle a moment ago.
        unsafe {
            SetStdHandle(id(stream), previous as Handle);
        }
    }

    /// Settles the switch of `stream` to its pipe after [`SETTLE_AFTER`]
    /// ([`settle`]): the C runtime's descriptor follows the stream, and the
    /// replaced handle `previous` is released when `release` (it was the log
    /// file, which must not stay open once a rotation renames it). Without
    /// the thread nothing changes: the descriptor keeps `previous`, which
    /// then stays open (to the old file) until exit.
    pub(super) fn settle_later(stream: Stream, previous: usize, release: bool) {
        let _ = std::thread::Builder::new()
            .name("log-settle".into())
            .spawn(move || {
                std::thread::sleep(SETTLE_AFTER);
                settle(stream, previous, release);
            });
    }

    /// Points the C runtime's descriptor of `stream` (1 or 2) at a duplicate
    /// of the pipe that is now the stream's standard handle, so C code's
    /// output is captured like Rust's, then closes `previous` (when
    /// `release`) unless something still holds it. `_dup2` closes the handle
    /// the descriptor held (at startup the runtime took the inherited
    /// standard handle itself, so usually `previous`), or
    /// leaves it to the other descriptor while that one still holds the same
    /// handle; such a handle is never closed here, so it is closed exactly
    /// once, and never while a descriptor or a standard handle refers to it
    /// (a value Windows could reuse for an unrelated file). Streams settle
    /// one at a time.
    fn settle(stream: Stream, previous: usize, release: bool) {
        static SETTLING: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _settling = crate::util::lock::lock(&SETTLING);
        // SAFETY: `ignore` returns; the runtime then reports a bad descriptor
        // to this thread with -1 instead of ending the process.
        unsafe {
            _set_thread_local_invalid_parameter_handler(Some(ignore));
        }
        let _writers = writers(stream);
        let Some(pipe) = std_handle(stream) else {
            return;
        };
        let held = crt_handle(descriptor(stream));
        crt_point(descriptor(stream), pipe);
        // In a console process `_dup2` also makes its duplicate the standard
        // handle: the pipe handle stays the standard handle.
        if std_handle(stream) != Some(pipe) {
            // SAFETY: `pipe` is this process's open pipe handle.
            unsafe {
                SetStdHandle(id(stream), pipe);
            }
        }
        if !release || previous == 0 || held == Some(previous) || in_use(previous) {
            return;
        }
        // SAFETY: neither standard handle nor either C descriptor refers to
        // `previous` any more, and the runtime did not own it: it was
        // inherited for this process's standard streams only.
        unsafe {
            CloseHandle(previous as Handle);
        }
    }

    /// An invalid-parameter handler that returns.
    unsafe extern "C" fn ignore(_: *const u16, _: *const u16, _: *const u16, _: u32, _: usize) {}

    /// The handle C descriptor `fd` holds, if it is open on one.
    fn crt_handle(fd: i32) -> Option<usize> {
        // SAFETY: a lookup; a closed descriptor reports -1 (the caller's
        // invalid-parameter handler returns).
        let handle = unsafe { _get_osfhandle(fd) };
        // -2: the runtime's "no console" placeholder.
        (handle != -1 && handle != -2 && handle != 0).then_some(handle as usize)
    }

    /// Whether a standard handle or a C descriptor still refers to `handle`.
    fn in_use(handle: usize) -> bool {
        [Stream::Stdout, Stream::Stderr].into_iter().any(|stream| {
            std_handle(stream).map(|h| h as usize) == Some(handle)
                || crt_handle(descriptor(stream)) == Some(handle)
        })
    }

    /// Points C descriptor `fd` at a non-inheritable duplicate of `handle`
    /// (`_dup2` duplicates as inheritable).
    fn crt_point(fd: i32, handle: Handle) -> bool {
        let mut copy: Handle = std::ptr::null_mut();
        // SAFETY: duplicates a handle of this process within it.
        let duplicated = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                handle,
                GetCurrentProcess(),
                &mut copy,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        } != 0;
        if !duplicated {
            return false;
        }
        // SAFETY: the runtime owns `copy` from here on (as `temporary`).
        let temporary = unsafe { _open_osfhandle(copy as isize, O_TEXT) };
        if temporary < 0 {
            // SAFETY: `copy` is still ours.
            unsafe {
                CloseHandle(copy);
            }
            return false;
        }
        // SAFETY: both descriptors are the runtime's; `temporary` is closed
        // once `fd` holds its own duplicate.
        let pointed = unsafe { _dup2(temporary, fd) } == 0;
        // SAFETY: `temporary` is open and nothing else uses it.
        unsafe {
            _close(temporary);
        }
        if pointed && let Some(now) = crt_handle(fd) {
            // SAFETY: `now` is the handle descriptor `fd` holds.
            unsafe {
                SetHandleInformation(now as Handle, HANDLE_FLAG_INHERIT, 0);
            }
        }
        pointed
    }
}

#[cfg(unix)]
mod stdio {
    use super::{Stream, StreamKind};
    use std::{
        fs::File,
        io::{self, Write},
        os::fd::{AsRawFd, FromRawFd},
        path::PathBuf,
    };

    fn fd(stream: Stream) -> i32 {
        match stream {
            Stream::Stdout => 1,
            Stream::Stderr => 2,
        }
    }

    fn stat(fd: i32) -> Option<libc::stat> {
        // SAFETY: `st` is a writable stat buffer.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        (unsafe { libc::fstat(fd, &mut st) } == 0).then_some(st)
    }

    pub(super) fn classify(stream: Stream, log: &File) -> StreamKind {
        let fd = fd(stream);
        let Some(st) = stat(fd) else {
            return StreamKind::Missing;
        };
        match st.st_mode & libc::S_IFMT {
            libc::S_IFREG => match stat(log.as_raw_fd()) {
                Some(ours) if ours.st_dev == st.st_dev && ours.st_ino == st.st_ino => {
                    StreamKind::LogFile
                }
                _ => StreamKind::Other,
            },
            // SAFETY: a query on this process's descriptor.
            libc::S_IFCHR if unsafe { libc::isatty(fd) } == 1 => StreamKind::Terminal,
            libc::S_IFCHR => StreamKind::Null,
            _ => StreamKind::Other,
        }
    }

    pub(super) fn file_path(stream: Stream) -> Option<PathBuf> {
        let fd = fd(stream);
        let st = stat(fd)?;
        if st.st_mode & libc::S_IFMT != libc::S_IFREG {
            return None;
        }
        std::fs::read_link(format!("/proc/self/fd/{fd}")).ok()
    }

    pub(super) fn redirect(stream: Stream) -> io::Result<(File, usize)> {
        let mut fds = [0i32; 2];
        // SAFETY: `fds` holds two writable descriptors.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        for fd in fds {
            // SAFETY: fresh descriptors of this process.
            unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
        }
        let _ = io::stdout().flush();
        let _ = io::stderr().flush();
        // SAFETY: `fds[1]` is open; dup2 replaces the standard descriptor
        // atomically (the old file description closes with its last fd).
        unsafe {
            if libc::dup2(fds[1], fd(stream)) < 0 {
                let error = io::Error::last_os_error();
                libc::close(fds[0]);
                libc::close(fds[1]);
                return Err(error);
            }
            libc::close(fds[1]);
            Ok((File::from_raw_fd(fds[0]), 0))
        }
    }

    pub(super) fn restore(_stream: Stream, _previous: usize) {
        // The previous descriptor is gone (dup2 replaced it); output written
        // to the pipe without a reader fails instead of blocking.
    }

    /// Nothing to settle: `dup2` replaced descriptor 1 or 2 itself, which C
    /// code writes to as well, and closed the previous file with it.
    pub(super) fn settle_later(_stream: Stream, _previous: usize, _release: bool) {}
}

#[cfg(not(any(windows, unix)))]
mod stdio {
    use super::{Stream, StreamKind};
    use std::{fs::File, io, path::PathBuf};

    pub(super) fn classify(_stream: Stream, _log: &File) -> StreamKind {
        StreamKind::Other
    }
    pub(super) fn file_path(_stream: Stream) -> Option<PathBuf> {
        None
    }
    pub(super) fn redirect(_stream: Stream) -> io::Result<(File, usize)> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
    pub(super) fn restore(_stream: Stream, _previous: usize) {}
    pub(super) fn settle_later(_stream: Stream, _previous: usize, _release: bool) {}
}

// ---------------------------------------------------------------------------
// Tails

/// The last `max` bytes of the rotated log at `path` (older files first, so a
/// small live file is completed by the end of `path.1`), starting at a line
/// boundary, each line passed through [`redact_line`].
pub fn tail_lines(path: &Path, files: usize, max: usize) -> String {
    let mut chunks: Vec<Vec<u8>> = vec![];
    let mut budget = max;
    for file in rotated_files(path, files) {
        if budget == 0 {
            break;
        }
        let bytes = tail_bytes(&file, budget);
        budget = budget.saturating_sub(bytes.len());
        chunks.push(bytes);
    }
    chunks.reverse();
    let bytes = chunks.concat();
    // Drop a partial first line when the budget cut into it.
    let start = if bytes.len() >= max {
        bytes
            .iter()
            .position(|b| *b == b'\n')
            .map_or(bytes.len(), |at| at + 1)
    } else {
        0
    };
    let text = String::from_utf8_lossy(&bytes[start..]);
    let mut out = String::with_capacity(text.len());
    for line in text.lines() {
        out.push_str(&redact_line(line));
        out.push('\n');
    }
    out
}

fn tail_bytes(path: &Path, max: usize) -> Vec<u8> {
    use std::io::{Seek, SeekFrom};
    let Ok(mut file) = File::open(path) else {
        return vec![];
    };
    let Ok(length) = file.metadata().map(|m| m.len()) else {
        return vec![];
    };
    let start = length.saturating_sub(max as u64);
    if file.seek(SeekFrom::Start(start)).is_err() {
        return vec![];
    }
    let mut bytes = Vec::with_capacity(max.min(length as usize));
    if file.take(max as u64).read_to_end(&mut bytes).is_err() {
        return vec![];
    }
    bytes
}

/// An in-memory sink for tests and diagnostics.
#[derive(Clone, Debug, Default)]
pub struct MemorySink(Arc<Mutex<Vec<u8>>>);

impl MemorySink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contents(&self) -> String {
        String::from_utf8_lossy(&crate::util::lock::lock(&self.0)).into_owned()
    }

    /// Every complete line parsed as JSON (unparseable lines are skipped).
    pub fn events(&self) -> Vec<Value> {
        self.contents()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }

    pub fn clear(&self) {
        crate::util::lock::lock(&self.0).clear();
    }
}

impl Write for MemorySink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        crate::util::lock::lock(&self.0).extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __tkfs_log_level {
    (trace) => {
        $crate::util::log::Level::Trace
    };
    (debug) => {
        $crate::util::log::Level::Debug
    };
    (info) => {
        $crate::util::log::Level::Info
    };
    (warn) => {
        $crate::util::log::Level::Warn
    };
    (error) => {
        $crate::util::log::Level::Error
    };
    (fatal) => {
        $crate::util::log::Level::Fatal
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __tkfs_log_key {
    ($key:ident) => {
        stringify!($key)
    };
    ($key:literal) => {
        $key
    };
}

/// `event!(level, CODE)` or `event!(level, CODE, { key: value, "key": value })`.
/// See [`crate::util::log`].
#[doc(hidden)]
#[macro_export]
macro_rules! __tkfs_log_event {
    ($level:ident, $code:ident $(,)?) => {
        $crate::util::log::emit($crate::__tkfs_log_level!($level), stringify!($code), &[])
    };
    ($level:ident, $code:ident, { $($key:tt : $value:expr),* $(,)? } $(,)?) => {
        $crate::util::log::emit(
            $crate::__tkfs_log_level!($level),
            stringify!($code),
            &[$($crate::util::log::field($crate::__tkfs_log_key!($key), &$value)),*],
        )
    };
    ($level:ident, $code:expr $(,)?) => {
        $crate::util::log::emit($crate::__tkfs_log_level!($level), &$code, &[])
    };
    ($level:ident, $code:expr, { $($key:tt : $value:expr),* $(,)? } $(,)?) => {
        $crate::util::log::emit(
            $crate::__tkfs_log_level!($level),
            &$code,
            &[$($crate::util::log::field($crate::__tkfs_log_key!($key), &$value)),*],
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A live file left aside by an interrupted rotation is placed into the
    /// history (as the newest older file) before the live file rotates.
    #[test]
    fn an_interrupted_rotation_is_completed_by_the_next_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.log");
        let read = |n: usize| fs::read_to_string(rotated_path(&path, n)).ok();
        fs::write(rotated_path(&path, 1), "older").unwrap();
        fs::write(aside_path(&path), "interrupted").unwrap();
        fs::write(&path, "live").unwrap();
        rotate_files(&path, 4).unwrap();
        assert_eq!(
            (read(0), read(1), read(2), read(3)),
            (
                None,
                Some("live".into()),
                Some("interrupted".into()),
                Some("older".into())
            )
        );
        assert!(!aside_path(&path).exists());
    }
}
