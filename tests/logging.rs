//! WP4.2: rotating JSON-line logs, redaction, the desktop (GUI) mode capture,
//! `worker.log` rotation at spawn and the diagnostics bundle
//! (REVAMP_PLAN §14 WP4.2 tasks 1 and 2, acceptance: "log rotation test" and
//! "a diagnostics bundle with no secrets (grep)").
//!
//! Tests that install a process log or redirect the standard streams run in a
//! child process (this binary re-run with `TKFS_LOGGING_CHILD=<mode>`): both
//! are process-wide and cannot be undone.
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};
use support::{Control, Node, bin, has_file, state_dir, wait_until, write_file};
use tkfs::{
    model::{ipc::StartupLine, orchestrator::ProcessEvent, status::StatusSnapshot},
    orchestrator::process::{LogPolicy, RealLauncher},
    util::log::{self, MemorySink, REDACTED, RotatingFile, Rotation},
};

const CHILD_ENV: &str = "TKFS_LOGGING_CHILD";
/// The log path a child installs (modes `tee` and `null`).
const CHILD_LOG_ENV: &str = "TKFS_LOGGING_CHILD_LOG";

fn child_mode() -> Option<String> {
    std::env::var(CHILD_ENV).ok()
}

/// This test binary re-running one test in `mode`; the caller sets stdio.
fn child(test: &str, mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode)
        .stdin(Stdio::null());
    command
}

/// Serializes the tests of this binary that replace the process sink.
fn sink_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// The files of a rotated log, oldest first (`log.n` … `log.1`, `log`).
fn rotated_oldest_first(path: &Path, files: usize) -> Vec<PathBuf> {
    let mut found = log::rotated_files(path, files);
    found.reverse();
    found
}

fn json_lines(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line)
                .unwrap_or_else(|e| panic!("{}: not a JSON line ({e}): {line}", path.display()))
        })
        .collect()
}

/// Every byte under `dir` (recursively) and in `files`, as text.
fn texts_under(dir: &Path, out: &mut Vec<(PathBuf, String)>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            texts_under(&path, out);
        } else if let Ok(bytes) = fs::read(&path) {
            out.push((path, String::from_utf8_lossy(&bytes).into_owned()));
        }
    }
}

fn assert_no_secret(texts: &[(PathBuf, String)], secrets: &[String]) {
    assert!(!secrets.is_empty(), "the grep needs secrets to look for");
    for (path, text) in texts {
        for secret in secrets {
            assert!(
                !text.contains(secret.as_str()),
                "{} contains a secret ({}…)",
                path.display(),
                &secret[..secret.len().min(6)]
            );
        }
    }
}

// ===========================================================================
// Rotation

#[test]
fn rotating_file_rotates_by_size_and_keeps_its_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("logs").join("test.log");
    let rotation = Rotation {
        max_bytes: 1024,
        files: 3,
    };
    let mut file = RotatingFile::open(&path, rotation).unwrap();
    let lines: Vec<String> = (0..100)
        .map(|i| format!("{{\"n\":{i},\"pad\":\"{}\"}}\n", "p".repeat(60)))
        .collect();
    for line in &lines {
        file.write_all(line.as_bytes()).unwrap();
    }
    assert!(file.rotations() >= 2, "rotations: {}", file.rotations());
    let files = rotated_oldest_first(&path, 10);
    assert_eq!(
        files.len(),
        3,
        "the live file and two older ones: {files:?}"
    );
    assert!(
        !log::rotated_path(&path, 3).exists(),
        "the oldest is dropped"
    );
    let mut kept = vec![];
    for file in &files {
        let len = fs::metadata(file).unwrap().len();
        assert!(
            len <= rotation.max_bytes,
            "{} has {len} bytes",
            file.display()
        );
        kept.extend(json_lines(file));
    }
    // The newest lines survive, whole and in order, ending with the last one.
    let numbers: Vec<u64> = kept.iter().map(|v| v["n"].as_u64().unwrap()).collect();
    assert_eq!(*numbers.last().unwrap(), 99);
    assert!(numbers.windows(2).all(|w| w[1] == w[0] + 1), "{numbers:?}");
    assert!(numbers.len() >= 2 * (1024 / lines[0].len()));

    // A reopened writer keeps appending to the live file and rotates it.
    drop(file);
    let before = fs::read_to_string(&path).unwrap();
    let mut file = RotatingFile::open(&path, rotation).unwrap();
    assert_eq!(file.size(), before.len() as u64);
    for line in &lines[..20] {
        file.write_all(line.as_bytes()).unwrap();
    }
    assert!(file.rotations() >= 1);
}

/// A log directory removed while the daemon runs (a user cleared
/// `DATA/logs`) is made again at the next rotation: the log does not stop for
/// the rest of the process (WP4.INT, the WP4.2 review's minor).
#[test]
fn a_removed_log_directory_is_made_again_at_the_next_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let logs = dir.path().join("logs");
    let path = logs.join("cleared.log");
    let rotation = Rotation {
        max_bytes: 1024,
        files: 3,
    };
    let mut file = RotatingFile::open(&path, rotation).unwrap();
    let line = |n: usize| {
        format!(
            "{{\"n\":{n},\"pad\":\"{}\"}}
",
            "c".repeat(60)
        )
    };
    file.write_all(line(0).as_bytes()).unwrap();
    if let Err(error) = fs::remove_dir_all(&logs) {
        // A platform that keeps an open file's directory: nothing to test.
        eprintln!("the log directory could not be removed while open: {error}");
        return;
    }
    assert!(!logs.exists());
    // Past `max_bytes`: the rotation closes the handle and reopens the path.
    for n in 1..40 {
        file.write_all(line(n).as_bytes()).unwrap();
    }
    assert!(path.is_file(), "the live file is back in a new directory");
    let numbers: Vec<u64> = json_lines(&path)
        .iter()
        .map(|v| v["n"].as_u64().unwrap())
        .collect();
    assert_eq!(numbers.last(), Some(&39), "{numbers:?}");
    file.write_all(line(40).as_bytes()).unwrap();
    assert!(fs::read_to_string(&path).unwrap().contains("\"n\":40,"));
}

#[test]
fn a_one_file_rotation_keeps_no_history_and_an_oversized_line_is_written_whole() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("one.log");
    let mut file = RotatingFile::open(
        &path,
        Rotation {
            max_bytes: 300,
            files: 1,
        },
    )
    .unwrap();
    for i in 0..20 {
        file.write_all(format!("line {i:03} {}\n", "x".repeat(40)).as_bytes())
            .unwrap();
    }
    assert!(!log::rotated_path(&path, 1).exists());
    assert!(fs::metadata(&path).unwrap().len() <= 300);
    let big = format!("{}\n", "y".repeat(1000));
    file.write_all(big.as_bytes()).unwrap();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        big,
        "alone in a fresh file"
    );
}

#[test]
fn rotate_if_larger_moves_only_a_file_past_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker.log");
    assert!(
        !log::rotate_if_larger(&path, 10, 3).unwrap(),
        "missing file"
    );
    fs::write(&path, "0123456789").unwrap();
    assert!(
        !log::rotate_if_larger(&path, 10, 3).unwrap(),
        "at the limit"
    );
    fs::write(&path, "first generation, past the limit\n").unwrap();
    assert!(log::rotate_if_larger(&path, 10, 3).unwrap());
    assert!(!path.exists());
    fs::write(&path, "second generation, past the limit\n").unwrap();
    assert!(log::rotate_if_larger(&path, 10, 3).unwrap());
    fs::write(&path, "third generation, past the limit\n").unwrap();
    assert!(log::rotate_if_larger(&path, 10, 3).unwrap());
    assert_eq!(
        fs::read_to_string(log::rotated_path(&path, 1)).unwrap(),
        "third generation, past the limit\n"
    );
    assert_eq!(
        fs::read_to_string(log::rotated_path(&path, 2)).unwrap(),
        "second generation, past the limit\n"
    );
    assert!(!log::rotated_path(&path, 3).exists(), "three files at most");
}

/// A rotation fills the first missing generation instead of dropping the
/// oldest; with none missing, the oldest goes. A missing live file is not
/// rotated (the history stays as it is).
#[test]
fn a_rotation_fills_a_gap_before_dropping_the_oldest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gap.log");
    let read = |n: usize| fs::read_to_string(log::rotated_path(&path, n)).ok();
    fs::write(log::rotated_path(&path, 1), "A").unwrap();
    fs::write(log::rotated_path(&path, 3), "C").unwrap();
    log::rotate_files(&path, 4).unwrap();
    assert_eq!(
        (read(0), read(1), read(2), read(3)),
        (None, Some("A".into()), None, Some("C".into())),
        "no live file: nothing moves"
    );
    fs::write(&path, "L").unwrap();
    log::rotate_files(&path, 4).unwrap();
    assert_eq!(
        (read(0), read(1), read(2), read(3)),
        (None, Some("L".into()), Some("A".into()), Some("C".into())),
        "the gap at .2 is filled; nothing is dropped"
    );
    fs::write(&path, "M").unwrap();
    log::rotate_files(&path, 4).unwrap();
    assert_eq!(
        (read(0), read(1), read(2), read(3)),
        (None, Some("M".into()), Some("L".into()), Some("A".into())),
        "no gap: the oldest is dropped"
    );
    let names: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names.len(), 3, "no stray file: {names:?}");
}

/// The files in `dir`, by name.
#[cfg(windows)]
fn file_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// Windows: opens `path` the way a log viewer without delete sharing does
/// (its rename then fails).
#[cfg(windows)]
fn open_without_delete_sharing(path: &Path) -> fs::File {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_SHARE_READ: u32 = 1;
    const FILE_SHARE_WRITE: u32 = 2;
    fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(path)
        .unwrap()
}

/// Windows: a reader without delete sharing blocks the rename; the writer
/// keeps appending (with a note), retries after another max(max/8, 64 KiB),
/// never loses history to the failed attempts, and rotates once the reader
/// is gone.
#[cfg(windows)]
#[test]
fn a_blocked_rotation_keeps_appending_and_rotates_later() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blocked.log");
    let rotation = Rotation {
        max_bytes: 4096,
        files: 3,
    };
    fs::write(log::rotated_path(&path, 1), "history one\n").unwrap();
    fs::write(log::rotated_path(&path, 2), "history two\n").unwrap();
    let mut file = RotatingFile::open(&path, rotation).unwrap();
    let line = format!("{}\n", "z".repeat(100));
    file.write_all(line.as_bytes()).unwrap();
    let blocker = open_without_delete_sharing(&path);
    // About 200 KB: the first attempt and at least two retries fail.
    for _ in 0..2000 {
        file.write_all(line.as_bytes()).unwrap();
    }
    assert_eq!(file.rotations(), 0);
    let text = fs::read_to_string(&path).unwrap();
    let notes = text.matches("log rotation failed").count();
    assert!(notes >= 3, "every failed attempt is noted ({notes})");
    assert!(text.len() > 200_000, "still appending");
    assert_eq!(
        (
            fs::read_to_string(log::rotated_path(&path, 1)).unwrap(),
            fs::read_to_string(log::rotated_path(&path, 2)).unwrap()
        ),
        ("history one\n".to_owned(), "history two\n".to_owned()),
        "failed attempts never touch the history"
    );
    assert_eq!(
        file_names(dir.path()),
        ["blocked.log", "blocked.log.1", "blocked.log.2"]
    );
    drop(blocker);
    // The next retry rotates.
    for _ in 0..1000 {
        if file.rotations() > 0 {
            break;
        }
        file.write_all(line.as_bytes()).unwrap();
    }
    assert_eq!(file.rotations(), 1);
    let rotated = fs::read_to_string(log::rotated_path(&path, 1)).unwrap();
    assert!(rotated.contains("log rotation failed") && rotated.len() > 200_000);
    assert_eq!(
        fs::read_to_string(log::rotated_path(&path, 2)).unwrap(),
        "history one\n",
        "one rotation drops one generation"
    );
}

/// Windows: an older file held without delete sharing stops the history
/// from moving. The live file is put back (appending continues), the first
/// attempt costs the oldest generation, and the retries fill the gap it left
/// instead of dropping another one.
#[cfg(windows)]
#[test]
fn a_blocked_older_file_costs_at_most_one_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("held.log");
    let rotation = Rotation {
        max_bytes: 4096,
        files: 4,
    };
    for (n, text) in [(1, "one\n"), (2, "two\n"), (3, "three\n")] {
        fs::write(log::rotated_path(&path, n), text).unwrap();
    }
    let blocker = open_without_delete_sharing(&log::rotated_path(&path, 1));
    let mut file = RotatingFile::open(&path, rotation).unwrap();
    let line = format!("{}\n", "h".repeat(100));
    for _ in 0..2000 {
        file.write_all(line.as_bytes()).unwrap();
    }
    assert_eq!(file.rotations(), 0);
    let text = fs::read_to_string(&path).unwrap();
    let notes = text.matches("log rotation failed").count();
    assert!(notes >= 3, "every failed attempt is noted ({notes})");
    assert!(text.len() > 200_000, "the live file was put back each time");
    let read = |n: usize| fs::read_to_string(log::rotated_path(&path, n)).ok();
    assert_eq!(
        (read(1), read(2), read(3)),
        (Some("one\n".into()), None, Some("two\n".into())),
        "only the oldest generation went, once"
    );
    assert_eq!(
        file_names(dir.path()),
        ["held.log", "held.log.1", "held.log.3"]
    );
    drop(blocker);
    for _ in 0..1000 {
        if file.rotations() > 0 {
            break;
        }
        file.write_all(line.as_bytes()).unwrap();
    }
    assert_eq!(file.rotations(), 1);
    assert!(read(1).is_some_and(|t| t.contains("log rotation failed")));
    assert_eq!(
        (read(2), read(3)),
        (Some("one\n".into()), Some("two\n".into())),
        "the gap was filled"
    );
}

// ===========================================================================
// Redaction

#[test]
fn events_never_carry_codes_tokens_or_registered_secrets() {
    let _serial = sink_lock();
    let sink = MemorySink::new();
    log::set_sink(Some(Box::new(sink.clone())));
    let code = "0123456789abcdef".repeat(4);
    let registered = "registered-secret-in-a-message-0001";
    log::register_secret(registered);
    log::event!(warn, WS_CLOSED, {
        "token": "tok-0123456789",
        "Authorization": "Bearer abcdefghijkl",
        "nested": json!({"pairing_code": code, "list": [{"secret": [1, 2, 3]}], "keep": "visible"}),
        "action": json!({"op": "pair", "endpoint": "ws://h:1", "code": code}),
        "error": json!({"code": "WS_CLOSED", "message": "closed"}),
        "verifier_generation": 7,
        "message": format!("peer said {registered}"),
    });
    log::flush_suppressed();
    log::set_sink(None);
    log::forget_secret(registered);
    let text = sink.contents();
    for secret in [code.as_str(), "tok-0123456789", "abcdefghijkl", registered] {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }
    let event = sink
        .events()
        .into_iter()
        .find(|e| e["code"] == "WS_CLOSED")
        .unwrap();
    assert_eq!(event["token"], REDACTED);
    assert_eq!(event["Authorization"], REDACTED);
    assert_eq!(event["nested"]["pairing_code"], REDACTED);
    assert_eq!(event["nested"]["list"][0]["secret"], REDACTED);
    assert_eq!(event["nested"]["keep"], "visible");
    assert_eq!(event["action"]["code"], REDACTED, "not an error code");
    assert_eq!(event["action"]["endpoint"], "ws://h:1");
    assert_eq!(event["error"]["code"], "WS_CLOSED", "error codes stay");
    assert_eq!(event["verifier_generation"], 7, "numbers stay");
    assert_eq!(event["message"], format!("peer said {REDACTED}"));
}

#[test]
fn foreign_lines_are_redacted() {
    let ready = r#"{"ready":true,"repo":"r","pid":4,"token":"worker-token-0123456789"}"#;
    let redacted = log::redact_line(ready);
    assert!(!redacted.contains("worker-token-0123456789"));
    let value: Value = serde_json::from_str(&redacted).unwrap();
    assert_eq!(value["token"], REDACTED);
    assert_eq!(value["pid"], 4);
    let clean = r#"{"startup_progress":true,"phase":"open"}"#;
    assert_eq!(
        log::redact_line(clean),
        clean,
        "unchanged lines stay as written"
    );
    assert_eq!(log::redact_line("plain text"), "plain text");
    let secrets = ["abcdefgh-secret".to_owned()];
    assert_eq!(
        log::scrub_with("x abcdefgh-secret y", &secrets),
        format!("x {REDACTED} y")
    );
    assert_eq!(
        log::scrub_with("short", &["short".to_owned()]),
        "short",
        "strings under MIN_SECRET_LEN are not secrets"
    );
    let line = log::captured_line("stdout", r#"{"format":3,"token":"t-0123456789"}"#);
    let value: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(value["code"], "stdout");
    assert_eq!(value["json"]["format"], 3);
    assert_eq!(value["json"]["token"], REDACTED);
    // A captured line read back (a diagnostics tail) keeps its stream code.
    let again: Value = serde_json::from_str(&log::redact_line(&line)).unwrap();
    assert_eq!(again["code"], "stdout");
    assert_eq!(again["json"]["token"], REDACTED);
    let line = log::captured_line("stderr", "thread 'main' panicked");
    let value: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(
        (value["level"].as_str(), value["text"].as_str()),
        (Some("warn"), Some("thread 'main' panicked"))
    );
}

// ===========================================================================
// Process logs (child processes)

/// The desktop starts its daemon with stdout and stderr appended to
/// `DATA/logs/orchestrator.log` (`gui.rs`'s `Spawner`). Building the launcher
/// adopts that file as the rotating log: both streams are captured, every
/// line is a JSON event, raw output follows the rotation (nothing lands in a
/// renamed file), panics are kept and secrets are redacted.
#[test]
fn desktop_mode_daemon_output_goes_to_the_rotating_log() {
    const TEST: &str = "desktop_mode_daemon_output_goes_to_the_rotating_log";
    if child_mode().as_deref() == Some("desktop") {
        desktop_child();
        return;
    }
    if child_mode().is_some() {
        return;
    }
    run_desktop_child(TEST, None);
}

/// The first start after an upgrade: the inherited `orchestrator.log` holds
/// the earlier daemon's unbounded raw output and is already past the rotation
/// size. The standard streams are still recognized as the log (they are
/// classified before the file rotates), so both are captured, nothing is teed
/// into the renamed legacy file, and that file stops growing.
#[test]
fn desktop_mode_adopts_an_oversized_legacy_log() {
    const TEST: &str = "desktop_mode_adopts_an_oversized_legacy_log";
    if child_mode().as_deref() == Some("desktop") {
        desktop_child();
        return;
    }
    if child_mode().is_some() {
        return;
    }
    run_desktop_child(TEST, Some(&legacy_log(6500)));
}

/// Raw output of a daemon from before WP4.2 (no JSON events), at least `len`
/// bytes.
fn legacy_log(len: usize) -> String {
    let mut text = String::new();
    let mut n = 0;
    while text.len() < len {
        text.push_str(&format!(
            "legacy raw output line {n:04} {}\n",
            "l".repeat(60)
        ));
        n += 1;
    }
    text
}

/// The child side of the desktop-mode tests (stdout and stderr are the log).
fn desktop_child() {
    let _launcher = RealLauncher::new(LogPolicy::default());
    let installed = log::file_log().expect("the desktop log is adopted");
    assert!(installed.captured_stdout && installed.captured_stderr);
    assert!(!installed.tee_stderr);
    assert_eq!(installed.rotation.max_bytes, 4096);
    println!(
        "{}",
        json!({"format":3,"pid":1,"control":"ws://127.0.0.1:1","token":"tok-leak-0123456789"})
    );
    eprintln!("plain stderr line");
    log::event!(warn, WS_CLOSED, {"detail": "an event through the sink", "token": "tok-event-0123456789"});
    log::register_secret("registered-secret-value");
    eprintln!("a raw line that mentions registered-secret-value");
    for i in 0..150 {
        println!("stdout filler {i:04} {}", "x".repeat(80));
        eprintln!("stderr filler {i:04} {}", "y".repeat(80));
    }
    let _ = thread::spawn(|| panic!("child panic kept by the log")).join();
    println!("stdout end marker");
    eprintln!("stderr end marker");
    let live = installed.path.clone();
    let drained = wait_until(Duration::from_secs(20), || {
        let text: String = log::rotated_files(&live, 50)
            .iter()
            .map(|p| fs::read_to_string(p).unwrap_or_default())
            .collect();
        text.contains("stdout end marker") && text.contains("stderr end marker")
    });
    assert!(drained, "the capture threads drained");
}

/// Runs `test`'s desktop child with stdout and stderr appended to a
/// `logs/orchestrator.log` that starts with `legacy` (if any), and checks the
/// rotated log it leaves.
fn run_desktop_child(test: &str, legacy: Option<&str>) {
    let dir = tempfile::tempdir().unwrap();
    let log_path = log::daemon_log_path(&dir.path().join("data"));
    fs::create_dir_all(log_path.parent().unwrap()).unwrap();
    if let Some(legacy) = legacy {
        fs::write(&log_path, legacy).unwrap();
    }
    // Exactly as the desktop's Spawner: one file, appended, for both streams.
    let out = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let err = out.try_clone().unwrap();
    let status = child(test, "desktop")
        .env(log::ROTATE_BYTES_ENV, "4096")
        .env(log::ROTATE_FILES_ENV, "50")
        .stdout(out)
        .stderr(err)
        .status()
        .unwrap();
    let mut files = rotated_oldest_first(&log_path, 50);
    let all: String = files
        .iter()
        .map(|p| fs::read_to_string(p).unwrap())
        .collect();
    assert!(status.success(), "child failed: {status}\n{all}");
    if let Some(legacy) = legacy {
        // The renamed legacy file holds what it held, plus only what libtest
        // printed before the child adopted the log (`running 1 test`,
        // `test NAME ... `): no event, no captured line, no teed copy.
        let oldest = files.remove(0);
        let text = fs::read_to_string(&oldest).unwrap();
        assert!(
            text.starts_with(legacy),
            "{} is not the legacy file",
            oldest.display()
        );
        let after = &text[legacy.len()..];
        assert!(
            after.len() < 512,
            "the legacy file kept growing ({} bytes more):\n{after}",
            after.len()
        );
        for line in after.lines() {
            assert!(
                serde_json::from_str::<Value>(line).is_err() && !line.contains("filler"),
                "an event or a captured line reached the legacy file: {line}"
            );
        }
        let rest: String = files
            .iter()
            .map(|p| fs::read_to_string(p).unwrap())
            .collect();
        assert!(
            !rest.contains("legacy raw output"),
            "the legacy output stays in its own file"
        );
    }
    assert!(files.len() >= 5, "rotated: {files:?}");
    let mut events = vec![];
    for (n, file) in files.iter().enumerate() {
        let len = fs::metadata(file).unwrap().len();
        assert!(len <= 4096, "{} has {len} bytes", file.display());
        let lines = if n == 0 && legacy.is_none() {
            // libtest's own output reached the file before the child adopted
            // it (`running 1 test`, `test NAME ... `); everything after is ours.
            let text = fs::read_to_string(file).unwrap();
            let preamble = text
                .lines()
                .take_while(|line| serde_json::from_str::<Value>(line).is_err())
                .count();
            text.lines()
                .skip(preamble)
                .map(|line| {
                    serde_json::from_str::<Value>(line)
                        .unwrap_or_else(|e| panic!("not a JSON line ({e}): {line}"))
                })
                .collect()
        } else {
            json_lines(file)
        };
        for event in lines {
            assert!(
                event["at_ms"].is_u64() && event["level"].is_string() && event["code"].is_string(),
                "every line is an event: {event}"
            );
            events.push(event);
        }
    }
    let texts: Vec<&str> = events.iter().filter_map(|e| e["text"].as_str()).collect();
    for stream in ["stdout", "stderr"] {
        let fillers: Vec<&str> = texts
            .iter()
            .copied()
            .filter(|t| t.starts_with(&format!("{stream} filler ")))
            .collect();
        let expected: Vec<String> = (0..150)
            .map(|i| {
                let pad = if stream == "stdout" { "x" } else { "y" };
                format!("{stream} filler {i:04} {}", pad.repeat(80))
            })
            .collect();
        assert_eq!(fillers, expected, "{stream}: every line, once, in order");
    }
    assert!(
        events.iter().any(|e| e["code"] == "stdout"
            && e["json"]["format"] == 3
            && e["json"]["token"] == REDACTED),
        "a JSON stdout line is kept whole, redacted"
    );
    assert!(events.iter().any(|e| e["code"] == "stderr"
        && e["level"] == "warn"
        && e["text"] == "plain stderr line"));
    let sink_events: Vec<&Value> = events
        .iter()
        .filter(|e| e["code"] == "WS_CLOSED" && e["detail"] == "an event through the sink")
        .collect();
    assert_eq!(
        sink_events.len(),
        1,
        "the event is written once (no teed copy): {sink_events:?}"
    );
    assert_eq!(sink_events[0]["token"], REDACTED);
    assert!(
        events
            .iter()
            .any(|e| e["code"] == "stderr" && e["panic"] == "child panic kept by the log"),
        "the panic is in the log"
    );
    for secret in [
        "tok-leak-0123456789",
        "tok-event-0123456789",
        "registered-secret-value",
    ] {
        assert!(!all.contains(secret), "{secret} leaked");
    }
}

/// A console run (stderr a terminal, a pipe or another file): events go to
/// the log file and to stderr; stdout and stderr are left alone.
#[test]
fn a_daemon_log_tees_events_to_another_stderr() {
    const TEST: &str = "a_daemon_log_tees_events_to_another_stderr";
    if child_mode().as_deref() == Some("tee") {
        let data = PathBuf::from(std::env::var(CHILD_LOG_ENV).unwrap());
        let installed = log::install_daemon_log(&data).unwrap();
        assert_eq!(installed.path, log::daemon_log_path(&data));
        assert!(!installed.captured_stdout && !installed.captured_stderr);
        assert!(installed.tee_stderr);
        assert_eq!(installed.rotation, Rotation::default());
        log::event!(warn, WS_CLOSED, {"detail": "teed event"});
        println!("plain stdout stays on stdout");
        // A second install keeps the first.
        let again = log::install_file(&data.join("other.log"), Rotation::default()).unwrap();
        assert_eq!(again, installed);
        return;
    }
    if child_mode().is_some() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let output = child(TEST, "tee")
        .env(CHILD_LOG_ENV, &data)
        .env_remove(log::ROTATE_BYTES_ENV)
        .env_remove(log::ROTATE_FILES_ENV)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stderr.contains("teed event"), "{stderr}");
    assert!(stdout.contains("plain stdout stays on stdout"));
    let file = fs::read_to_string(log::daemon_log_path(&data)).unwrap();
    assert!(file.contains("teed event"));
    assert!(!file.contains("plain stdout stays on stdout"));
    assert!(!data.join("other.log").exists());
}

/// A process started without standard streams (the null device) keeps its
/// raw output in the log instead of discarding it.
#[test]
fn null_standard_streams_are_captured_into_the_log() {
    const TEST: &str = "null_standard_streams_are_captured_into_the_log";
    if child_mode().as_deref() == Some("null") {
        let path = PathBuf::from(std::env::var(CHILD_LOG_ENV).unwrap());
        let installed = log::install_file(
            &path,
            Rotation {
                max_bytes: 1 << 20,
                files: 2,
            },
        )
        .unwrap();
        assert!(installed.captured_stdout && installed.captured_stderr);
        assert!(!installed.tee_stderr);
        println!("stdout kept although it was discarded");
        eprintln!("stderr kept although it was discarded");
        log::event!(info, WS_CLOSED, {"detail": "null mode event"});
        assert!(wait_until(Duration::from_secs(20), || {
            let text = fs::read_to_string(&path).unwrap_or_default();
            text.contains("stdout kept") && text.contains("stderr kept")
        }));
        return;
    }
    if child_mode().is_some() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("null.log");
    let status = child(TEST, "null")
        .env(CHILD_LOG_ENV, &path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    let text = fs::read_to_string(&path).unwrap_or_default();
    assert!(status.success(), "{status}\n{text}");
    let events = json_lines(&path);
    assert!(
        events
            .iter()
            .any(|e| e["code"] == "stdout" && e["text"] == "stdout kept although it was discarded")
    );
    assert!(
        events
            .iter()
            .any(|e| e["code"] == "stderr" && e["text"] == "stderr kept although it was discarded")
    );
    assert!(events.iter().any(|e| e["detail"] == "null mode event"));
}

/// The C runtime of the test process (descriptors, not Rust's streams).
#[cfg(windows)]
mod crt {
    use std::ffi::c_void;

    unsafe extern "C" {
        fn _write(fd: i32, buffer: *const c_void, count: u32) -> i32;
        fn _get_osfhandle(fd: i32) -> isize;
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetHandleInformation(handle: isize, flags: *mut u32) -> i32;
    }

    /// `_write(fd, text)`, as C code's `printf` ends; whether all of it was
    /// written.
    pub fn write(fd: i32, text: &str) -> bool {
        // SAFETY: `text` is readable for its length; descriptors 1 and 2 are
        // always open in the runtime.
        unsafe { _write(fd, text.as_ptr().cast(), text.len() as u32) == text.len() as i32 }
    }

    /// Whether the handle descriptor `fd` holds is inheritable.
    pub fn inheritable(fd: i32) -> Option<bool> {
        // SAFETY: lookups on an open descriptor and its handle.
        unsafe {
            let handle = _get_osfhandle(fd);
            let mut flags = 0u32;
            (handle != -1 && handle != -2 && GetHandleInformation(handle, &mut flags) != 0)
                .then_some(flags & 1 != 0)
        }
    }
}

/// The events of a log that may also hold raw lines.
#[cfg(windows)]
fn events_in(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(Value::is_object)
        .collect()
}

/// C code's writes (the C runtime's descriptors 1 and 2, which keep the
/// handles of startup and never see `SetStdHandle`) follow the captured
/// streams into the log once the capture settles. Before WP4.2 fix round 2
/// they kept the replaced log-file handle, which the capture closed 2 s
/// later: C output then failed, or went into whichever file reused the handle
/// value. The descriptors' new handles are not inheritable.
#[cfg(windows)]
#[test]
fn c_runtime_writes_follow_the_captured_streams() {
    const TEST: &str = "c_runtime_writes_follow_the_captured_streams";
    if child_mode().as_deref() == Some("crt") {
        crt_child();
        return;
    }
    if child_mode().is_some() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("logs").join("crt.log");
    fs::create_dir_all(log_path.parent().unwrap()).unwrap();
    // As the desktop's Spawner: the log file, appended, for both streams.
    let out = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let err = out.try_clone().unwrap();
    let status = child(TEST, "crt")
        .env(CHILD_LOG_ENV, &log_path)
        .stdout(out)
        .stderr(err)
        .status()
        .unwrap();
    let text = fs::read_to_string(&log_path).unwrap_or_default();
    assert!(status.success(), "{status}\n{text}");
    let events = events_in(&log_path);
    for (code, line) in [
        ("stdout", "C stdout after the switch"),
        ("stderr", "C stderr after the switch"),
        ("stdout", "Rust stdout after the switch"),
        ("stderr", "Rust stderr after the switch"),
    ] {
        assert!(
            events
                .iter()
                .any(|e| e["code"] == code && e["text"] == line),
            "{code}: {line:?} is not a captured event\n{text}"
        );
    }
    let victims = dir.path().join("victims");
    let mut opened = 0;
    for victim in fs::read_dir(&victims).unwrap() {
        let victim = victim.unwrap().path();
        assert_eq!(
            fs::read_to_string(&victim).unwrap(),
            "",
            "{} received C output",
            victim.display()
        );
        opened += 1;
    }
    assert_eq!(opened, 64);
}

#[cfg(windows)]
fn crt_child() {
    let path = PathBuf::from(std::env::var(CHILD_LOG_ENV).unwrap());
    let installed = log::install_file(
        &path,
        Rotation {
            max_bytes: 1 << 20,
            files: 2,
        },
    )
    .unwrap();
    assert!(installed.captured_stdout && installed.captured_stderr);
    let captured = |code: &str, prefix: &str| {
        events_in(&path)
            .iter()
            .any(|e| e["code"] == code && e["text"].as_str().is_some_and(|t| t.starts_with(prefix)))
    };
    // Until a stream settles, C writes still reach the log file raw (through
    // the inherited handle); then they arrive as captured events.
    let mut probe = 0;
    let settled = wait_until(Duration::from_secs(60), || {
        probe += 1;
        crt::write(1, &format!("C stdout probe {probe}\n"));
        crt::write(2, &format!("C stderr probe {probe}\n"));
        captured("stdout", "C stdout probe ") && captured("stderr", "C stderr probe ")
    });
    assert!(settled, "C writes never reached the capture");
    // Files opened from now on may get the value of a released handle.
    let victims = path.parent().unwrap().parent().unwrap().join("victims");
    fs::create_dir_all(&victims).unwrap();
    let _open: Vec<fs::File> = (0..64)
        .map(|i| fs::File::create(victims.join(format!("victim{i:02}"))).unwrap())
        .collect();
    assert!(crt::write(1, "C stdout after the switch\n"));
    assert!(crt::write(2, "C stderr after the switch\n"));
    println!("Rust stdout after the switch");
    eprintln!("Rust stderr after the switch");
    assert_eq!(crt::inheritable(1), Some(false));
    assert_eq!(crt::inheritable(2), Some(false));
    let drained = wait_until(Duration::from_secs(20), || {
        let events = events_in(&path);
        [
            ("stdout", "C stdout after the switch"),
            ("stderr", "C stderr after the switch"),
            ("stdout", "Rust stdout after the switch"),
            ("stderr", "Rust stderr after the switch"),
        ]
        .iter()
        .all(|(code, line)| {
            events
                .iter()
                .any(|e| e["code"] == *code && e["text"] == *line)
        })
    });
    assert!(drained, "the capture threads drained");
}

// ===========================================================================
// worker.log at spawn

/// A `worker.log` past the limit is rotated when the next worker spawns, and
/// the Ready line's token reaches the controller but never the file.
#[test]
fn worker_log_rotates_at_spawn_and_never_keeps_the_ready_token() {
    const TEST: &str = "worker_log_rotates_at_spawn_and_never_keeps_the_ready_token";
    const TOKEN: &str = "worker-token-0123456789abcdef";
    if child_mode().as_deref() == Some("worker") {
        // libtest printed `test NAME ... ` without a newline on this stdout.
        println!();
        println!(
            "{}",
            json!({"ready":true,"repo":"r","device":"d","runtime":"rt","mounted":false,
                "instance":"i-1","pid":std::process::id(),"address":"127.0.0.1:1",
                "version":"v","ipc":3,"token":TOKEN})
        );
        eprintln!("worker stderr line");
        return;
    }
    if child_mode().is_some() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("worker.log");
    let old = "an earlier worker's output\n".repeat(100);
    fs::write(&log_path, &old).unwrap();
    let launcher = RealLauncher::new(LogPolicy {
        rotate_bytes: 1024,
        rotate_files: 3,
        ..LogPolicy::default()
    });
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    let sink: tkfs::model::orchestrator::ProcessSink = Arc::new(move |event| {
        let _ = tkfs::util::lock::lock(&tx).send(event);
    });
    let _process = launcher
        .launch_command(child(TEST, "worker"), &log_path, "test", sink)
        .unwrap();
    let mut token = None;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let event = rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("the worker exits");
        match event {
            ProcessEvent::Line(StartupLine::Ready(ready)) => token = ready.token,
            ProcessEvent::Line(_) => {}
            ProcessEvent::Exited { code, .. } => {
                assert_eq!(code, Some(0));
                break;
            }
        }
    }
    assert_eq!(
        token.as_deref(),
        Some(TOKEN),
        "the controller gets the token"
    );
    assert_eq!(
        fs::read_to_string(log::rotated_path(&log_path, 1)).unwrap(),
        old,
        "the old log was rotated at spawn"
    );
    let text = fs::read_to_string(&log_path).unwrap();
    assert!(!text.contains(TOKEN), "{text}");
    assert!(
        text.contains(&format!("\"token\":\"{REDACTED}\"")),
        "{text}"
    );
    assert!(text.contains("worker stderr line"));
    assert!(!text.contains("an earlier worker's output"));
    // CCR-WP4.1-4 (applied by WP4.INT): `worker.log` is owner-only, also
    // when an earlier file was not.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&log_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "worker.log is {mode:o}");
        fs::set_permissions(&log_path, fs::Permissions::from_mode(0o644)).unwrap();
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let sink: tkfs::model::orchestrator::ProcessSink = Arc::new(move |event| {
            let _ = tkfs::util::lock::lock(&tx).send(event);
        });
        let _again = launcher
            .launch_command(child(TEST, "worker"), &log_path, "test", sink)
            .unwrap();
        while !matches!(
            rx.recv_timeout(Duration::from_secs(60))
                .expect("the worker exits"),
            ProcessEvent::Exited { .. }
        ) {}
        let mode = fs::metadata(&log_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "an older worker.log is made owner-only: {mode:o}"
        );
    }
}

// ===========================================================================
// Daemons

/// The secrets a daemon's data directory holds: pairing codes and verifier
/// hashes in `folders.sqlite`, any setting named like a secret, token or
/// credential, the sensitive columns (named like a secret, [`log::is_sensitive_key`])
/// of any table named like one (WP4.1's `peer_credentials.secret_hash`, not
/// its public `installation` and `side`), `*.token` files and sensitive
/// fields of `worker.json` / `runtime.json`.
fn secrets_of(data: &Path) -> Vec<String> {
    let mut secrets = vec![];
    let catalog = data.join("folders.sqlite");
    if catalog.is_file() {
        let db = rusqlite::Connection::open_with_flags(
            &catalog,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let mut codes = db.prepare("SELECT code FROM peers").unwrap();
        for code in codes.query_map([], |r| r.get::<_, String>(0)).unwrap() {
            secrets.push(code.unwrap());
        }
        let mut settings = db.prepare("SELECT key, value FROM settings").unwrap();
        for row in settings
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .unwrap()
        {
            let (key, value) = row.unwrap();
            let key = key.to_ascii_lowercase();
            if ["pairing", "secret", "token", "credential"]
                .iter()
                .any(|k| key.contains(k))
            {
                match serde_json::from_str::<Value>(&value) {
                    Ok(Value::Object(map)) => {
                        // The verifier's hash is HMAC-key-equivalent to the code.
                        for (field, value) in map {
                            if field != "salt"
                                && let Some(text) = value.as_str()
                            {
                                secrets.push(text.to_owned());
                            }
                        }
                    }
                    _ => secrets.push(value),
                }
            }
        }
        let tables: Vec<String> = db
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        for table in tables {
            let name = table.to_ascii_lowercase();
            if !["secret", "token", "credential"]
                .iter()
                .any(|k| name.contains(k))
            {
                continue;
            }
            let mut rows = db.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
            // Only the columns named like a secret hold one (WP4.1's
            // `peer_credentials.secret_hash`); the others are public and
            // appear legitimately in the status, the bundle and the logs
            // (`installation`, `side` = "outbound", times).
            let sensitive: Vec<usize> = (0..rows.column_count())
                .filter(|&i| log::is_sensitive_key(rows.column_name(i).unwrap()))
                .collect();
            assert!(
                !sensitive.is_empty(),
                "table {table} is named like a secret but none of its columns is: \
                 teach secrets_of which column holds the secret"
            );
            let mut query = rows.query([]).unwrap();
            while let Some(row) = query.next().unwrap() {
                for &i in &sensitive {
                    match row.get_ref(i).unwrap() {
                        rusqlite::types::ValueRef::Text(t) => {
                            secrets.push(String::from_utf8_lossy(t).into_owned())
                        }
                        rusqlite::types::ValueRef::Blob(b) => secrets.push(hex::encode(b)),
                        _ => {}
                    }
                }
            }
        }
    }
    let mut directories = vec![data.to_path_buf()];
    if let Ok(states) = fs::read_dir(data.join("states")) {
        directories.extend(states.flatten().map(|e| e.path()));
    }
    for dir in directories {
        for entry in fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "token") {
                secrets.push(fs::read_to_string(&path).unwrap().trim().to_owned());
            }
        }
        for name in ["worker.json", "runtime.json"] {
            if let Ok(bytes) = fs::read(dir.join(name))
                && let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&bytes)
            {
                for (key, value) in map {
                    if log::is_sensitive_key(&key)
                        && let Some(text) = value.as_str()
                    {
                        secrets.push(text.to_owned());
                    }
                }
            }
        }
    }
    secrets.retain(|s| s.len() >= log::MIN_SECRET_LEN);
    secrets.sort();
    secrets.dedup();
    secrets
}

/// The grep's secret list on a catalog shaped like WP4.1's (its
/// `peer_credentials` schema, verbatim): every pairing code, the verifier's
/// hash and every `secret_hash` are secrets; the installation ids, the
/// `side` values and the salt are not (they are public: the status, the
/// redacted catalog and `versions.json` carry the installation ids, and
/// `"outbound"` is a status direction).
#[test]
fn secrets_of_takes_only_the_secret_columns_of_a_credential_table() {
    let dir = tempfile::tempdir().unwrap();
    let ours = "11111111-2222-4333-8444-555555555555";
    let peer = "66666666-7777-4888-9999-aaaaaaaaaaaa";
    let code = "c0de".repeat(16);
    let hash = "4a54".repeat(16);
    let salt = "5a17".repeat(16);
    let outbound = "0b0b".repeat(16);
    let inbound = "1b1b".repeat(16);
    let db = rusqlite::Connection::open(dir.path().join("folders.sqlite")).unwrap();
    db.execute_batch(
        "CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
         CREATE TABLE peers(endpoint TEXT PRIMARY KEY,code TEXT NOT NULL,installation TEXT NOT NULL,connections INTEGER NOT NULL,revision TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS peer_credentials(installation TEXT NOT NULL,side TEXT NOT NULL,secret_hash TEXT NOT NULL,created_ms INTEGER NOT NULL,revoked_ms INTEGER,PRIMARY KEY(installation,side));",
    )
    .unwrap();
    db.execute("INSERT INTO settings VALUES('installation',?1)", [ours])
        .unwrap();
    db.execute(
        "INSERT INTO settings VALUES('pairing',?1)",
        [json!({"salt": salt, "hash": hash}).to_string()],
    )
    .unwrap();
    db.execute(
        "INSERT INTO peers VALUES('127.0.0.1:1',?1,?2,2,'r1')",
        rusqlite::params![code, peer],
    )
    .unwrap();
    db.execute(
        "INSERT INTO peer_credentials VALUES(?1,'outbound',?2,1,NULL)",
        rusqlite::params![peer, outbound],
    )
    .unwrap();
    db.execute(
        "INSERT INTO peer_credentials VALUES(?1,'inbound',?2,2,3)",
        rusqlite::params![peer, inbound],
    )
    .unwrap();
    drop(db);
    let secrets = secrets_of(dir.path());
    for secret in [&code, &hash, &outbound, &inbound] {
        assert!(secrets.contains(secret), "{secret} missing: {secrets:?}");
    }
    for public in [ours, peer, "outbound", "inbound", &salt, "r1"] {
        assert!(
            !secrets.iter().any(|s| s == public),
            "{public} is not a secret: {secrets:?}"
        );
    }
    assert_eq!(secrets.len(), 4, "{secrets:?}");
}

/// Every log a node wrote: its daemon output (`daemon.log`), `DATA/logs`,
/// and every `worker.log*`.
fn logs_of(node: &Node) -> Vec<(PathBuf, String)> {
    let mut texts = vec![];
    texts.push((
        node.log_path(),
        fs::read_to_string(node.log_path()).unwrap_or_default(),
    ));
    texts_under(&node.data_dir().join("logs"), &mut texts);
    if let Ok(states) = fs::read_dir(node.data_dir().join("states")) {
        for state in states.flatten() {
            for file in fs::read_dir(state.path()).into_iter().flatten().flatten() {
                let name = file.file_name().to_string_lossy().into_owned();
                if name.starts_with("worker.log") {
                    texts.push((
                        file.path(),
                        fs::read_to_string(file.path()).unwrap_or_default(),
                    ));
                }
            }
        }
    }
    texts
}

/// Two paired daemons with a synchronized folder: their logs and status hold
/// no secret, and each diagnostics bundle has every part and no secret even
/// after a pairing code and a token were planted in a worker log. Until
/// CCR-WP4.2-1 these daemons logged to the harness's `daemon.log` only (no
/// installed log): the reply's `log_path` was null, and a
/// `DATA/logs/orchestrator.log` left by an earlier daemon was bundled only as
/// `orchestrator.log.not-installed.tail`. With the CCR applied (WP4.INT),
/// every daemon installs its log: `log_path` is `DATA/logs/orchestrator.log`
/// and its tail is `orchestrator.log.tail` (asserted).
#[test]
fn diagnostics_bundles_have_every_part_and_no_secret() {
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Diagnostics", true);
    write_file(&pa, "hello.txt", b"hello");
    let code = a.code();
    b.pair(&a, &code);
    let pb = b.receive(&a, &pa);
    b.wait_for("hello on B", Duration::from_secs(90), || {
        has_file(&pb, "hello.txt", b"hello")
    });
    let mut secrets = secrets_of(&a.data_dir());
    secrets.extend(secrets_of(&b.data_dir()));
    secrets.push(code.clone());
    secrets.sort();
    secrets.dedup();
    assert!(
        secrets_of(&b.data_dir()).contains(&code),
        "B's catalog holds A's code (the grep has something to find)"
    );
    // Logs and status, as written by the daemons.
    let mut texts = logs_of(&a);
    texts.extend(logs_of(&b));
    for node in [&a, &b] {
        let status = node.call(json!({"op":"status"}));
        texts.push((node.data_dir(), status.to_string()));
    }
    assert_no_secret(&texts, &secrets);

    // A leak planted in B's worker log is scrubbed from B's bundle.
    let planted_token = "planted-token-0123456789".to_owned();
    {
        let mut worker_log = fs::OpenOptions::new()
            .append(true)
            .open(state_dir(&pb).join("worker.log"))
            .unwrap();
        writeln!(worker_log, "a leaked line with {code} in it").unwrap();
        writeln!(worker_log, "{}", json!({"token": planted_token, "x": 1})).unwrap();
    }
    // An earlier daemon's log in A's DATA/logs, when A writes none itself
    // (until CCR-WP4.2-1 every harness daemon logs to `daemon.log` only).
    let stale_token = "stale-token-0123456789".to_owned();
    let a_log = log::daemon_log_path(&a.data_dir());
    let stale_planted = !a_log.exists();
    if stale_planted {
        fs::create_dir_all(a_log.parent().unwrap()).unwrap();
        let event = json!({"at_ms": 1, "level": "warn", "code": "WS_CLOSED",
            "detail": "stale event marker", "token": stale_token});
        fs::write(&a_log, format!("{event}\nstale raw output marker\n")).unwrap();
    }
    let mut planted = secrets.clone();
    planted.push(planted_token);
    planted.push(stale_token);
    for (node, folder) in [(&a, &pa), (&b, &pb)] {
        let reply = node.call(json!({"op":"diagnostics"}));
        assert_eq!(reply["schema"], 1, "{reply}");
        assert!(
            !reply["log_path"].is_null(),
            "CCR-WP4.2-1: every daemon installs DATA/logs/orchestrator.log: {reply}"
        );
        let dir = PathBuf::from(reply["path"].as_str().unwrap());
        assert!(
            fs::canonicalize(&dir)
                .unwrap()
                .starts_with(fs::canonicalize(node.data_dir().join("diagnostics")).unwrap()),
            "{}",
            dir.display()
        );
        let state = folder["state"].as_str().unwrap();
        for file in [
            "status.json".to_owned(),
            "activity.json".to_owned(),
            "catalog.redacted.json".to_owned(),
            "versions.json".to_owned(),
            format!("folders/{state}.heartbeat.json"),
            format!("logs/{state}.worker.log.tail"),
        ] {
            assert!(dir.join(&file).is_file(), "missing {file}");
            assert!(
                reply["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|f| f["path"] == file.as_str()),
                "{file} not listed: {reply}"
            );
        }
        let listed = |file: &str| {
            reply["files"]
                .as_array()
                .unwrap()
                .iter()
                .any(|f| f["path"] == file)
        };
        let own_tail = "logs/orchestrator.log.tail";
        let not_installed = "logs/orchestrator.log.not-installed.tail";
        if reply["log_path"].is_null() {
            // No installed log: no tail claims to be this daemon's log, and a
            // DATA/logs file is bundled under a name that says it is not.
            assert!(!dir.join(own_tail).exists() && !listed(own_tail));
            if std::ptr::eq(node, &a) && stale_planted {
                let tail = fs::read_to_string(dir.join(not_installed)).unwrap();
                assert!(listed(not_installed), "{reply}");
                assert!(
                    tail.contains("stale event marker") && tail.contains("stale raw output marker"),
                    "{tail}"
                );
                assert!(
                    tail.contains(&format!("\"token\":\"{REDACTED}\"")),
                    "{tail}"
                );
            } else {
                assert!(!dir.join(not_installed).exists() && !listed(not_installed));
            }
        } else {
            // An installed log (CCR-WP4.2-1): DATA/logs/orchestrator.log.
            assert_eq!(
                fs::canonicalize(reply["log_path"].as_str().unwrap()).unwrap(),
                fs::canonicalize(log::daemon_log_path(&node.data_dir())).unwrap()
            );
            assert!(dir.join(own_tail).is_file() && listed(own_tail), "{reply}");
            assert!(!dir.join(not_installed).exists() && !listed(not_installed));
        }
        let status: StatusSnapshot =
            serde_json::from_slice(&fs::read(dir.join("status.json")).unwrap()).unwrap();
        assert_eq!(status.schema, 2);
        assert!(status.folders.iter().any(|f| f.state == state));
        let activity: Value =
            serde_json::from_slice(&fs::read(dir.join("activity.json")).unwrap()).unwrap();
        assert!(activity.is_array());
        let heartbeat: Value = serde_json::from_slice(
            &fs::read(dir.join(format!("folders/{state}.heartbeat.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(heartbeat["state"], state);
        assert!(heartbeat["phase"].is_object() || heartbeat["phase"].is_string());
        assert!(
            heartbeat["heartbeat"]["instance"].is_string(),
            "a running worker's heartbeat: {heartbeat}"
        );
        let versions: Value =
            serde_json::from_slice(&fs::read(dir.join("versions.json")).unwrap()).unwrap();
        assert_eq!(versions["protocol"], 3);
        assert_eq!(
            versions["log"].is_null(),
            reply["log_path"].is_null(),
            "{}",
            versions["log"]
        );
        assert!(versions["tkfs"].as_str().is_some_and(|v| !v.is_empty()));
        assert_eq!(versions["workers"][0]["state"], state);
        let worker_tail =
            fs::read_to_string(dir.join(format!("logs/{state}.worker.log.tail"))).unwrap();
        assert!(!worker_tail.is_empty());
        let mut bundle = vec![];
        texts_under(&dir, &mut bundle);
        assert_no_secret(&bundle, &planted);
    }
    // The catalogs as the bundles show them.
    let latest = |node: &Node| -> PathBuf {
        let mut dirs: Vec<PathBuf> = fs::read_dir(node.data_dir().join("diagnostics"))
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .collect();
        dirs.sort();
        dirs.pop().unwrap()
    };
    let catalog_b: Value =
        serde_json::from_slice(&fs::read(latest(&b).join("catalog.redacted.json")).unwrap())
            .unwrap();
    assert_eq!(catalog_b["peers"][0]["has_code"], true);
    assert_eq!(catalog_b["peers"][0]["endpoint"], a.endpoint());
    assert!(catalog_b["peers"][0].get("code").is_none());
    let catalog_a: Value =
        serde_json::from_slice(&fs::read(latest(&a).join("catalog.redacted.json")).unwrap())
            .unwrap();
    assert_eq!(catalog_a["pairing"]["verifier_set"], true);
    assert!(catalog_a["pairing"].get("hash").is_none());
    let tail_b = fs::read_to_string(latest(&b).join(format!(
        "logs/{}.worker.log.tail",
        pb["state"].as_str().unwrap()
    )))
    .unwrap();
    assert!(tail_b.contains(&format!("a leaked line with {REDACTED} in it")));
    assert!(tail_b.contains(&format!("\"token\":\"{REDACTED}\"")));
}

/// A daemon started the way the desktop starts it (stdout and stderr appended
/// to `DATA/logs/orchestrator.log`) writes a rotating JSON-line log: its
/// discovery line and its events are there, the log rotates, the
/// diagnostics bundle carries its tail, and the pairing code is nowhere.
#[test]
fn a_desktop_started_daemon_writes_a_rotating_log() {
    run_desktop_daemon(None);
}

/// The same daemon started on an `orchestrator.log` already past the rotation
/// size (the first start after an upgrade): both streams are still captured,
/// nothing is teed, and the renamed legacy file never grows.
#[test]
fn a_desktop_started_daemon_adopts_an_oversized_legacy_log() {
    run_desktop_daemon(Some(&legacy_log(6500)));
}

fn run_desktop_daemon(legacy: Option<&str>) {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("orchestrator.toml");
    fs::write(
        &config,
        "format_version=2\ndata_directory='data'\ndefault_mount_directory='mounts'\nmaximum_running=8\n[network]\nlisten='127.0.0.1:0'\nconnections=2\nsync_interval_ms=500\n",
    )
    .unwrap();
    let data = root.path().join("data");
    let log_path = log::daemon_log_path(&data);
    fs::create_dir_all(log_path.parent().unwrap()).unwrap();
    if let Some(legacy) = legacy {
        fs::write(&log_path, legacy).unwrap();
    }
    let out = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let err = out.try_clone().unwrap();
    let mut command = Command::new(bin());
    command
        .arg("-f")
        .arg(&config)
        .arg("orchestrator")
        .env(log::ROTATE_BYTES_ENV, "2048")
        .env(log::ROTATE_FILES_ENV, "20")
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err);
    let mut daemon = KillOnDrop(command.spawn().unwrap());
    let control = Control {
        data_dir: data.clone(),
    };
    let all_logs = || -> String {
        log::rotated_files(&log_path, 20)
            .iter()
            .rev()
            .map(|p| fs::read_to_string(p).unwrap_or_default())
            .collect()
    };
    // The legacy file (renamed by the first rotation) and its size once the
    // daemon answers.
    let legacy_file = |legacy: &str| -> Option<(PathBuf, String)> {
        log::rotated_files(&log_path, 20).into_iter().find_map(|p| {
            let text = fs::read_to_string(&p).unwrap_or_default();
            text.starts_with(legacy).then_some((p, text))
        })
    };
    assert!(
        wait_until(Duration::from_secs(30), || control
            .call(&json!({"op":"list"}))
            .is_ok()),
        "the daemon answers\n{}",
        all_logs()
    );
    let legacy_at_start = legacy.map(|legacy| {
        let (path, text) = legacy_file(legacy).expect("the legacy file is kept");
        assert_ne!(path, log_path, "the oversized legacy log was rotated");
        text.len()
    });
    let code = control.call(&json!({"op":"reset-pairing-code"})).unwrap()["code"]
        .as_str()
        .unwrap()
        .to_owned();
    let discovery: Value =
        serde_json::from_slice(&fs::read(data.join("orchestrator.json")).unwrap()).unwrap();
    let network = discovery["network"]
        .as_str()
        .unwrap()
        .trim_start_matches("ws://")
        .to_owned();
    // Failed inbound handshakes are logged (at most 20 a minute per code).
    for _ in 0..30 {
        if let Ok(mut stream) = TcpStream::connect(&network) {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
            let _ = stream.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
            let mut sink = Vec::new();
            let _ = stream.read_to_end(&mut sink);
        }
    }
    assert!(
        wait_until(Duration::from_secs(20), || all_logs()
            .contains("inbound handshake failed")),
        "handshake failures are logged\n{}",
        all_logs()
    );
    let reply = control.call(&json!({"op":"diagnostics"})).unwrap();
    assert_eq!(reply["log_path"], log_path.display().to_string());
    let bundle = PathBuf::from(reply["path"].as_str().unwrap());
    let tail = fs::read_to_string(bundle.join("logs/orchestrator.log.tail")).unwrap();
    assert!(tail.contains("inbound handshake failed"), "{tail}");
    let versions: Value =
        serde_json::from_slice(&fs::read(bundle.join("versions.json")).unwrap()).unwrap();
    assert_eq!(
        (
            &versions["log"]["captured_stdout"],
            &versions["log"]["captured_stderr"],
            &versions["log"]["tee_stderr"],
        ),
        (&json!(true), &json!(true), &json!(false)),
        "both streams are captured, nothing is teed: {}",
        versions["log"]
    );
    let _ = control.call(&json!({"op":"shutdown"}));
    let exited = wait_until(Duration::from_secs(30), || {
        daemon.0.try_wait().ok().flatten().is_some()
    });
    assert!(exited, "the daemon exits\n{}", all_logs());
    let mut files = log::rotated_files(&log_path, 20);
    if let (Some(legacy), Some(at_start)) = (legacy, legacy_at_start) {
        let (path, text) = legacy_file(legacy).expect("the legacy file is kept");
        assert_eq!(
            text.len(),
            at_start,
            "the renamed legacy file stopped growing:\n{}",
            &text[legacy.len()..]
        );
        assert!(
            !text[legacy.len()..].contains("handshake"),
            "no event reached the legacy file"
        );
        files.retain(|file| *file != path);
    }
    assert!(files.len() >= 2, "the log rotated: {files:?}");
    let mut events = vec![];
    for file in &files {
        assert!(fs::metadata(file).unwrap().len() <= 2048);
        events.extend(json_lines(file));
    }
    assert!(
        events
            .iter()
            .any(|e| e["code"] == "stdout" && e["json"]["format"] == 3),
        "the discovery line is captured"
    );
    assert!(
        events
            .iter()
            .any(|e| e["detail"] == "inbound handshake failed")
    );
    assert!(
        events.iter().any(|e| e["code"] == "ORCHESTRATOR_STOPPING"),
        "shutdown events reach the live log"
    );
    let mut texts = vec![];
    texts_under(&data.join("logs"), &mut texts);
    texts_under(&bundle, &mut texts);
    let mut secrets = secrets_of(&data);
    secrets.push(code);
    assert_no_secret(&texts, &secrets);
}

/// Kills a process this test started if the test fails before it exits.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// CCR-WP4.2-1 (applied by WP4.INT): a daemon started from a console (here
/// the harness, whose stderr is its `daemon.log`) installs
/// `DATA/logs/orchestrator.log` and reports it in `Status` (`daemon.log_path`,
/// the `log` row of `manage status`); its events land in that file as JSON
/// lines and are still teed to its stderr.
#[test]
fn every_daemon_installs_its_log_and_reports_it() {
    let node = Node::new();
    let status: StatusSnapshot = serde_json::from_value(node.call(json!({"op":"status"}))).unwrap();
    let reported = status
        .daemon
        .log_path
        .clone()
        .expect("the daemon reports its log");
    assert!(
        !reported.starts_with(r"\?\"),
        "a path as the user knows it: {reported}"
    );
    let path = log::daemon_log_path(&node.data_dir());
    assert_eq!(
        fs::canonicalize(&reported).unwrap(),
        fs::canonicalize(&path).unwrap()
    );
    // A failed inbound handshake is an event: in the file and on stderr.
    let network = node.endpoint();
    let network = network.trim_start_matches("ws://");
    if let Ok(mut stream) = TcpStream::connect(network) {
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let _ = stream.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink);
    }
    assert!(
        wait_until(Duration::from_secs(20), || {
            fs::read_to_string(&path)
                .unwrap_or_default()
                .contains("inbound handshake failed")
                && fs::read_to_string(node.log_path())
                    .unwrap_or_default()
                    .contains("inbound handshake failed")
        }),
        "the event reaches the installed log and the teed stderr"
    );
    for line in fs::read_to_string(&path).unwrap().lines() {
        serde_json::from_str::<Value>(line)
            .unwrap_or_else(|e| panic!("not a JSON line ({e}): {line}"));
    }
    let text = Command::new(bin())
        .arg("-f")
        .arg(node.config_path())
        .args(["manage", "status"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&text.stdout);
    assert!(
        text.lines()
            .any(|line| line.contains("log") && line.contains("orchestrator.log")),
        "manage status shows the log row:\n{text}"
    );
}
