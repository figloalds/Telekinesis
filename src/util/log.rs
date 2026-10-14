//! Structured JSON-line events with per-code rate limiting.
//!
//! ```ignore
//! use tkfs::util::log::event;
//! event!(warn, "OBJECT_IO_ERROR", { "object": hash, attempts: 3 });
//! event!(fatal, ENGINE_POISONED);           // a bare identifier is the code itself
//! event!(info, (code.as_str()), { k: v });  // any other expression: parenthesize it
//! ```
//!
//! Each event is one line `{"at_ms":…,"level":"…","code":"…",<fields…>}` written
//! to the process sink (stderr unless [`set_sink`] replaced it; worker stdout is
//! reserved for startup lines). At most [`RATE_LIMIT`] events per code are
//! written per [`RATE_WINDOW`]; the rest are counted and reported as one
//! `{"code":…,"suppressed":N}` line when the next window opens (or on
//! [`flush_suppressed`]). `fatal` events are never suppressed. Field values
//! are serialized with serde; a value that fails to serialize becomes `null`.
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
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::HashMap,
    io::Write,
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
        write(sink.as_ref(), &out);
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
        write(sink.as_ref(), &out);
    }
}

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
        out.push_str(&value.to_string());
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
