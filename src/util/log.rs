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

struct Logger {
    sink: Option<Box<dyn Write + Send>>,
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
    crate::util::lock::lock(logger()).sink = sink;
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
    let mut logger = crate::util::lock::lock(logger());
    if level < logger.min_level {
        return false;
    }
    let now = Instant::now();
    let mut pending = None;
    let window = logger
        .windows
        .entry(code.to_owned())
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
    let mut out = String::new();
    if let Some((level, suppressed)) = pending {
        out.push_str(&line(
            level,
            code,
            &[("suppressed", Value::from(suppressed))],
        ));
    }
    if allowed {
        out.push_str(&line(level, code, fields));
    }
    if !out.is_empty() {
        write(&mut logger, &out);
    }
    allowed
}

/// Writes a `suppressed` summary line for every code with suppressed events in
/// its current window, and resets those counters.
pub fn flush_suppressed() {
    let mut logger = crate::util::lock::lock(logger());
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
    if !out.is_empty() {
        write(&mut logger, &out);
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

fn write(logger: &mut Logger, text: &str) {
    match logger.sink.as_mut() {
        Some(sink) => {
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
