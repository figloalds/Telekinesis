//! Named fault points for qualification scenarios.
//!
//! A fault is armed by environment variables, read once per process (or set
//! in-process with [`configure`]):
//!
//! | variable | meaning |
//! |---|---|
//! | `TKFS_FAULT_POINT` | exact point name, e.g. `worker.rpc.published-summary` |
//! | `TKFS_FAULT_SCOPE` | optional; fires only if it is a substring of the [`set_scope`] value |
//! | `TKFS_FAULT_ACTION` | `hang` (default), `fail`, `exit` or `panic` (used by [`trigger`]) |
//! | `TKFS_FAULT_MS` | hang duration in ms (absent: hang forever) |
//! | `TKFS_FAULT_COUNT` | how many hits fire (absent: every hit) |
//!
//! The legacy `core::fault` (`TKFS_FAULT`, exit 86) is separate and unchanged.
use anyhow::{Result, bail};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

/// Exit code of [`exit`] and of the `exit` action (same as `core::fault`).
pub const EXIT_CODE: i32 = crate::model::ipc::EXIT_FAULT;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultAction {
    Hang,
    Fail,
    Exit,
    Panic,
}

impl FaultAction {
    pub fn as_str(self) -> &'static str {
        match self {
            FaultAction::Hang => "hang",
            FaultAction::Fail => "fail",
            FaultAction::Exit => "exit",
            FaultAction::Panic => "panic",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "hang" => Some(FaultAction::Hang),
            "fail" => Some(FaultAction::Fail),
            "exit" => Some(FaultAction::Exit),
            "panic" => Some(FaultAction::Panic),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultConfig {
    pub point: String,
    pub scope: Option<String>,
    pub action: FaultAction,
    pub ms: Option<u64>,
    pub count: Option<u64>,
}

impl FaultConfig {
    pub fn new(point: &str, action: FaultAction) -> Self {
        Self {
            point: point.to_owned(),
            scope: None,
            action,
            ms: None,
            count: None,
        }
    }

    /// The configuration described by the `TKFS_FAULT_*` variables, if any.
    /// An unknown action falls back to `hang`.
    pub fn from_env() -> Option<Self> {
        let point = std::env::var("TKFS_FAULT_POINT")
            .ok()
            .filter(|point| !point.is_empty())?;
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        Some(Self {
            point,
            scope: var("TKFS_FAULT_SCOPE"),
            action: var("TKFS_FAULT_ACTION")
                .and_then(|action| FaultAction::parse(&action))
                .unwrap_or(FaultAction::Hang),
            ms: var("TKFS_FAULT_MS").and_then(|ms| ms.trim().parse().ok()),
            count: var("TKFS_FAULT_COUNT").and_then(|count| count.trim().parse().ok()),
        })
    }
}

struct State {
    loaded: bool,
    config: Option<FaultConfig>,
    fired: u64,
    scope: String,
}

static STATE: Mutex<State> = Mutex::new(State {
    loaded: false,
    config: None,
    fired: 0,
    scope: String::new(),
});
// 0 = not loaded yet, 1 = disarmed, 2 = armed. Lets unarmed points skip the lock.
static ARMED: AtomicU8 = AtomicU8::new(0);

fn state() -> std::sync::MutexGuard<'static, State> {
    let mut state = crate::util::lock::lock(&STATE);
    if !state.loaded {
        state.loaded = true;
        state.config = FaultConfig::from_env();
        state.fired = 0;
        ARMED.store(if state.config.is_some() { 2 } else { 1 }, Ordering::SeqCst);
    }
    state
}

/// Replaces the process-wide fault configuration (tests); `None` disarms.
/// Resets the hit counter.
pub fn configure(config: Option<FaultConfig>) {
    let mut state = state();
    ARMED.store(if config.is_some() { 2 } else { 1 }, Ordering::SeqCst);
    state.config = config;
    state.fired = 0;
}

/// Re-reads the `TKFS_FAULT_*` variables.
pub fn reload_from_env() {
    configure(FaultConfig::from_env());
}

/// The active configuration, if armed.
pub fn current() -> Option<FaultConfig> {
    state().config.clone()
}

/// Diagnostic count of injections actually fired, without triggering one.
pub fn fired() -> u64 {
    state().fired
}

/// Sets this process's scope string (workers use their state path).
pub fn set_scope(scope: &str) {
    state().scope = scope.to_owned();
}

pub fn scope() -> String {
    state().scope.clone()
}

/// Returns the configured action name when `point` is armed, in scope and not
/// exhausted; counts the hit. Never acts by itself.
pub fn hit(point: &str) -> Option<String> {
    fire(point).map(|config| config.action.as_str().to_owned())
}

fn fire(point: &str) -> Option<FaultConfig> {
    if ARMED.load(Ordering::SeqCst) == 1 {
        return None;
    }
    let mut state = state();
    let config = state.config.as_ref()?;
    if config.point != point {
        return None;
    }
    if let Some(scope) = &config.scope
        && !state.scope.contains(scope.as_str())
    {
        return None;
    }
    if config.count.is_some_and(|count| state.fired >= count) {
        return None;
    }
    let config = config.clone();
    state.fired += 1;
    Some(config)
}

/// Hangs when `point` fires: `TKFS_FAULT_MS` milliseconds, or forever.
pub fn hang(point: &str) {
    if let Some(config) = fire(point) {
        hang_for(config.ms);
    }
}

/// Fails with `FAULT_INJECTED: <point>` when `point` fires.
pub fn fail(point: &str) -> Result<()> {
    if fire(point).is_some() {
        bail!("FAULT_INJECTED: {point}");
    }
    Ok(())
}

/// Exits the process with [`EXIT_CODE`] when `point` fires.
pub fn exit(point: &str) {
    if fire(point).is_some() {
        std::process::exit(EXIT_CODE);
    }
}

/// Performs the configured action when `point` fires: `hang` (then `Ok`),
/// `fail` (`FAULT_INJECTED`), `exit` (code 86) or `panic`.
pub fn trigger(point: &str) -> Result<()> {
    let Some(config) = fire(point) else {
        return Ok(());
    };
    match config.action {
        FaultAction::Hang => {
            hang_for(config.ms);
            Ok(())
        }
        FaultAction::Fail => bail!("FAULT_INJECTED: {point}"),
        FaultAction::Exit => std::process::exit(EXIT_CODE),
        FaultAction::Panic => panic!("FAULT_INJECTED: {point}"),
    }
}

fn hang_for(ms: Option<u64>) {
    match ms {
        Some(ms) => std::thread::sleep(Duration::from_millis(ms)),
        None => loop {
            std::thread::sleep(Duration::from_secs(3600));
        },
    }
}
