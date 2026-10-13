//! Measurements for qualification scenarios.
//!
//! - `Stopwatch`, `summarize` (percentiles), `process_cpu_ms` (Windows
//!   `GetProcessTimes` FFI, Linux `/proc/<pid>/stat`), `find_pids`.
//! - `qual(..)` prints one `QUAL {json}` line per metric on stdout and, when
//!   `TKFS_QUAL_OUT` names a file, appends the same JSON line there
//!   (`scripts/qualify.ps1` sets it; captured test output is not needed).
//! - `Scenario` wraps a qualification test: budget, metrics, status line and
//!   floor assertions from `tests/qualification-floors.json`.
//!
//! Environment: `TKFS_QUAL_BUDGET_S` (default 120) bounds the measured phase of a
//! scenario; `TKFS_QUAL_SCALE` (default 1.0) scales sizes for quick dry runs;
//! `TKFS_QUAL_ENFORCE=0` reports floors without failing the test.
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

pub struct Stopwatch(Instant);
impl Default for Stopwatch {
    fn default() -> Self {
        Self::start()
    }
}
impl Stopwatch {
    pub fn start() -> Self {
        Self(Instant::now())
    }
    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
    pub fn ms(&self) -> f64 {
        self.0.elapsed().as_secs_f64() * 1000.0
    }
    pub fn restart(&mut self) -> f64 {
        let ms = self.ms();
        self.0 = Instant::now();
        ms
    }
}

/// Time `f` in milliseconds.
pub fn time_ms<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let watch = Stopwatch::start();
    let value = f();
    (value, watch.ms())
}

/// Nearest-rank percentile (`p` in 0..=100) of unsorted samples; NaN if empty.
pub fn percentile(samples: &[f64], p: f64) -> f64 {
    if samples.is_empty() {
        return f64::NAN;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Summary {
    pub count: usize,
    pub min: f64,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}
impl Summary {
    pub fn json(&self) -> Value {
        json!({"count":self.count,"min":self.min,"mean":self.mean,"p50":self.p50,
            "p90":self.p90,"p99":self.p99,"max":self.max})
    }
}

pub fn summarize(samples: &[f64]) -> Summary {
    if samples.is_empty() {
        return Summary::default();
    }
    Summary {
        count: samples.len(),
        min: samples.iter().copied().fold(f64::INFINITY, f64::min),
        mean: samples.iter().sum::<f64>() / samples.len() as f64,
        p50: percentile(samples, 50.0),
        p90: percentile(samples, 90.0),
        p99: percentile(samples, 99.0),
        max: samples.iter().copied().fold(f64::NEG_INFINITY, f64::max),
    }
}

/// User + kernel CPU time of `pid` in milliseconds, if the process is visible.
pub fn process_cpu_ms(pid: u32) -> Option<f64> {
    os::cpu_ms(pid)
}

#[cfg(windows)]
mod os {
    use std::ffi::c_void;
    #[repr(C)]
    #[derive(Default, Clone, Copy)]
    struct FileTime {
        low: u32,
        high: u32,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
        fn GetProcessTimes(
            process: *mut c_void,
            creation: *mut FileTime,
            exit: *mut FileTime,
            kernel: *mut FileTime,
            user: *mut FileTime,
        ) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }
    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    pub fn cpu_ms(pid: u32) -> Option<f64> {
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return None;
        }
        let (mut c, mut e, mut k, mut u) = Default::default();
        let ok = unsafe { GetProcessTimes(handle, &mut c, &mut e, &mut k, &mut u) };
        unsafe { CloseHandle(handle) };
        if ok == 0 {
            return None;
        }
        let ticks = |t: FileTime| (((t.high as u64) << 32) | t.low as u64) as f64;
        // FILETIME units are 100 ns.
        Some((ticks(k) + ticks(u)) / 10_000.0)
    }
}

#[cfg(unix)]
mod os {
    pub fn cpu_ms(pid: u32) -> Option<f64> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        // Fields after the parenthesized command; utime and stime are fields 14 and 15.
        let rest = &stat[stat.rfind(')')? + 2..];
        let fields: Vec<&str> = rest.split_whitespace().collect();
        let utime: f64 = fields.get(11)?.parse().ok()?;
        let stime: f64 = fields.get(12)?.parse().ok()?;
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        let ticks = if ticks > 0 { ticks as f64 } else { 100.0 };
        Some((utime + stime) * 1000.0 / ticks)
    }
}

#[cfg(not(any(windows, unix)))]
mod os {
    pub fn cpu_ms(_pid: u32) -> Option<f64> {
        None
    }
}

/// Process ids whose command line contains `needle` (for example a worker's
/// state directory). Windows queries CIM through PowerShell; Linux scans `/proc`.
/// Neither the caller nor the querying PowerShell (whose own command line
/// contains `needle`) is listed.
pub fn find_pids(needle: &str) -> Vec<u32> {
    #[cfg(windows)]
    {
        let filter = needle.replace('\'', "''");
        let script = format!(
            "Get-CimInstance Win32_Process | Where-Object {{ $_.ProcessId -ne $PID -and $_.CommandLine -and $_.CommandLine.Contains('{filter}') }} | ForEach-Object {{ $_.ProcessId }}"
        );
        let Ok(out) = std::process::Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output()
        else {
            return vec![];
        };
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.trim().parse().ok())
            .filter(|pid| *pid != std::process::id())
            .collect()
    }
    #[cfg(unix)]
    {
        let mut pids = vec![];
        if let Ok(entries) = fs::read_dir("/proc") {
            for entry in entries.flatten() {
                let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                    continue;
                };
                if pid == std::process::id() {
                    continue;
                }
                if let Ok(cmd) = fs::read(entry.path().join("cmdline"))
                    && String::from_utf8_lossy(&cmd)
                        .replace('\0', " ")
                        .contains(needle)
                {
                    pids.push(pid);
                }
            }
        }
        pids
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = needle;
        vec![]
    }
}

/// Emit `QUAL {json}`: `{"scenario","metric","value","unit", ...extra}`.
pub fn qual(scenario: &str, metric: &str, value: f64, unit: &str, extra: Value) {
    let mut line = json!({"scenario":scenario,"metric":metric,"value":finite(value),"unit":unit,
        "at_ms":tkfs::core::time()});
    if let Value::Object(map) = extra {
        for (k, v) in map {
            line[k] = v;
        }
    }
    emit(&line);
}

fn finite(v: f64) -> Value {
    if v.is_finite() { json!(v) } else { Value::Null }
}

/// Emit an arbitrary QUAL record (status lines, notes).
pub fn emit(record: &Value) {
    let text = format!("QUAL {record}");
    println!("{text}");
    if let Ok(path) = std::env::var("TKFS_QUAL_OUT")
        && !path.is_empty()
        && let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(path)
    {
        let _ = writeln!(file, "{record}");
    }
}

/// The measured-phase budget of one scenario (`TKFS_QUAL_BUDGET_S`, default 120 s).
pub fn budget() -> Duration {
    budget_or(120)
}

pub fn budget_or(default_s: u64) -> Duration {
    Duration::from_secs(
        std::env::var("TKFS_QUAL_BUDGET_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default_s),
    )
}

/// `n` scaled by `TKFS_QUAL_SCALE` (default 1.0, minimum 1).
pub fn scaled(n: usize) -> usize {
    let scale: f64 = std::env::var("TKFS_QUAL_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1.0);
    ((n as f64 * scale).round() as usize).max(1)
}

pub fn floors_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/qualification-floors.json")
}

/// Floors (from `tests/qualification-floors.json`) that apply to `scenario`.
pub fn floors_for(scenario: &str) -> Vec<Value> {
    let Ok(bytes) = fs::read(floors_path()) else {
        return vec![];
    };
    let Ok(doc) = serde_json::from_slice::<Value>(&bytes) else {
        return vec![];
    };
    doc["floors"]
        .as_array()
        .map(|floors| {
            floors
                .iter()
                .filter(|f| f["scenario"] == scenario)
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Whether `value` satisfies a floor record `{op, value}`.
pub fn meets(floor: &Value, value: f64) -> bool {
    let Some(limit) = floor["value"].as_f64() else {
        return false;
    };
    value.is_finite()
        && match floor["op"].as_str().unwrap_or("<=") {
            "<" => value < limit,
            "<=" => value <= limit,
            ">" => value > limit,
            ">=" => value >= limit,
            "==" => value == limit,
            _ => false,
        }
}

/// One qualification scenario: `file::name`, a budget, metrics and a final status.
///
/// ```ignore
/// let mut s = Scenario::new("qual_wp1_6_activation", "remote_linear_create_2k");
/// s.metric("per_event_ms", 3.1, "ms");
/// s.finish();   // emits the status line, asserts floors and checks
/// ```
pub struct Scenario {
    pub id: String,
    pub started: Instant,
    pub budget: Duration,
    metrics: Vec<(String, f64)>,
    failures: Vec<String>,
    notes: Vec<String>,
    timed_out: bool,
    finished: bool,
}
impl Scenario {
    pub fn new(file: &str, name: &str) -> Self {
        Self::with_budget(file, name, budget())
    }
    pub fn with_budget(file: &str, name: &str, budget: Duration) -> Self {
        let id = format!("{file}::{name}");
        emit(&json!({"scenario":id,"status":"started","budget_s":budget.as_secs()}));
        Self {
            id,
            started: Instant::now(),
            budget,
            metrics: vec![],
            failures: vec![],
            notes: vec![],
            timed_out: false,
            finished: false,
        }
    }
    /// Restart the budget clock (call after setup, before the measured phase).
    pub fn measure_from_now(&mut self) {
        self.started = Instant::now();
    }
    pub fn deadline(&self) -> Instant {
        self.started + self.budget
    }
    pub fn over_budget(&self) -> bool {
        Instant::now() >= self.deadline()
    }
    pub fn remaining(&self) -> Duration {
        self.deadline().saturating_duration_since(Instant::now())
    }
    pub fn metric(&mut self, metric: &str, value: f64, unit: &str) {
        self.metric_with(metric, value, unit, json!({}));
    }
    pub fn metric_with(&mut self, metric: &str, value: f64, unit: &str, extra: Value) {
        qual(&self.id, metric, value, unit, extra);
        self.metrics.push((metric.into(), value));
    }
    pub fn note(&mut self, text: impl Into<String>) {
        let text = text.into();
        emit(&json!({"scenario":self.id,"note":text}));
        self.notes.push(text);
    }
    /// Record a functional check; failures fail the test at `finish`.
    pub fn check(&mut self, ok: bool, what: impl Into<String>) -> bool {
        if !ok {
            let what = what.into();
            emit(&json!({"scenario":self.id,"check_failed":what}));
            self.failures.push(what);
        }
        ok
    }
    /// Mark the measured phase as having exceeded its budget.
    pub fn timeout(&mut self, what: impl Into<String>) {
        let what = what.into();
        self.timed_out = true;
        self.failures.push(format!("QUAL_TIMEOUT: {what}"));
    }
    /// The capability under test does not exist in this build (for example a
    /// baseline worker answering `UNKNOWN_OPERATION`). Fails the scenario.
    pub fn unsupported(&mut self, what: impl Into<String>) {
        let what = what.into();
        emit(&json!({"scenario":self.id,"unsupported":what}));
        self.failures.push(format!("UNSUPPORTED: {what}"));
    }
    /// Skip on this machine (for example no mount support). Passes.
    pub fn skip(mut self, why: &str) {
        emit(&json!({"scenario":self.id,"status":"skipped","reason":why,
            "elapsed_ms":self.started.elapsed().as_millis() as u64}));
        self.finished = true;
    }
    /// Emit the final status, then fail on any failed check, timeout or floor.
    #[track_caller]
    pub fn finish(mut self) {
        self.finished = true;
        let enforce = std::env::var("TKFS_QUAL_ENFORCE").as_deref() != Ok("0");
        let mut floor_results = vec![];
        for floor in floors_for(&self.id) {
            let metric = floor["metric"].as_str().unwrap_or("");
            let measured = self
                .metrics
                .iter()
                .rev()
                .find(|(m, _)| m == metric)
                .map(|(_, v)| *v);
            // A metric from a run cut short by its budget is not comparable.
            let ok = !self.timed_out && measured.is_some_and(|v| meets(&floor, v));
            floor_results.push(json!({"id":floor["id"],"metric":metric,"op":floor["op"],
                "floor":floor["value"],"measured":measured.map(finite),"ok":ok}));
            if !ok {
                self.failures.push(format!(
                    "FLOOR {}: {metric} {} {} (measured {:?})",
                    floor["id"].as_str().unwrap_or("?"),
                    floor["op"].as_str().unwrap_or("<="),
                    floor["value"],
                    measured
                ));
            }
        }
        let status = if self.timed_out {
            "timeout"
        } else if self.failures.is_empty() {
            "pass"
        } else {
            "fail"
        };
        emit(&json!({"scenario":self.id,"status":status,
            "elapsed_ms":self.started.elapsed().as_millis() as u64,
            "failures":self.failures,"notes":self.notes,"floors":floor_results}));
        if enforce {
            assert!(
                self.failures.is_empty(),
                "{} {status}: {:#?}",
                self.id,
                self.failures
            );
        }
    }
}
impl Drop for Scenario {
    fn drop(&mut self) {
        if !self.finished {
            emit(&json!({"scenario":self.id,"status":"fail",
                "elapsed_ms":self.started.elapsed().as_millis() as u64,
                "failures":["PANICKED"],"notes":self.notes}));
        }
    }
}
