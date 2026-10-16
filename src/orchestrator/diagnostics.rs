//! Owned by WP4.2 (stub landed by WP0.INT).
//!
//! The diagnostics bundle (REVAMP_PLAN §14 WP4.2 task 2, §15.2 P4). [`collect`]
//! writes `DATA/diagnostics/<ts>/` and returns where:
//!
//! | file | content |
//! |---|---|
//! | `status.json` | the hub's `StatusSnapshot` (schema 2) |
//! | `activity.json` | its activity log, oldest first |
//! | `folders/<state>.heartbeat.json` | each folder controller's view: phase, gate, mount, the worker endpoint (without its token), the last `WorkerHeartbeat`, the last errors, restarts |
//! | `catalog.redacted.json` | the catalog snapshot: folders, peers (endpoint, installation, connections; never the code), whether a pairing verifier is set and its generation (never its salt or hash), gates, peer metadata |
//! | `logs/*.tail` | the end of the daemon log (`orchestrator.log.tail`, [`DAEMON_TAIL_BYTES`], across its rotated files) and of every folder's `worker.log` (`<state>.worker.log.tail`, [`WORKER_TAIL_BYTES`]) |
//! | `versions.json` | daemon and worker versions, protocol, IPC, feed and status versions, platform, process |
//!
//! The reply is `{schema: 1, path, at_ms, files: [{path, bytes}], log_path,
//! secrets_redacted}`.
//!
//! **No secret leaves in a bundle** (invariant 21). Every file goes through
//! two passes before it is written:
//! 1. structured redaction (`util::log::redact_value` on JSON,
//!    `util::log::redact_line` on every log line): fields named like tokens,
//!    secrets, verifiers, credentials or pairing codes become `<redacted>`;
//! 2. a scrub of every secret this daemon knows, wherever it appears: the
//!    pairing codes of paired peers (`peers.code`), the verifier hash, the
//!    contents of `*.token` files in the data directory and in every folder
//!    state directory, sensitive fields of each folder's `worker.json` and
//!    `runtime.json` (for example the worker token), the worker endpoints'
//!    tokens, and the secrets registered with `util::log::register_secret`.
//!    `secrets_redacted` counts the files in which this pass replaced
//!    something.
//!
//! Collecting reads only snapshots and files: no worker RPC, no catalog
//! command, no lock beyond the snapshot reads, so a hung worker or a busy
//! catalog never stalls it. A file that cannot be read is skipped (its tail
//! is empty). The newest [`KEEP_BUNDLES`] bundles are kept; older ones (only
//! directories named like a bundle) are removed.
use crate::{
    model::{
        ipc::IPC_VERSION,
        orchestrator::{CatalogSnapshot, Ctx, FolderView},
        protocol::{PROTOCOL_VERSION, feed::FEED_FORMAT},
        status::STATUS_SCHEMA,
    },
    util::{log, time::now_ms},
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

/// The bundle format of the reply (`schema`).
pub const SCHEMA: u32 = 1;
/// Bundles live in `DATA/diagnostics/`.
pub const DIRECTORY: &str = "diagnostics";
/// The daemon log's tail (across its rotated files).
pub const DAEMON_TAIL_BYTES: usize = 512 * 1024;
/// Each worker log's tail.
pub const WORKER_TAIL_BYTES: usize = 256 * 1024;
/// Bundles kept in `DATA/diagnostics/` (the new one included).
pub const KEEP_BUNDLES: usize = 10;

/// Writes a bundle under `DATA/diagnostics/<ts>/` (see the module docs).
pub fn collect(ctx: &Arc<Ctx>) -> Result<Value> {
    collect_in(ctx, &ctx.config.data_directory.join(DIRECTORY))
}

/// [`collect`] into `root/<ts>/` (tests).
pub fn collect_in(ctx: &Arc<Ctx>, root: &Path) -> Result<Value> {
    let at_ms = now_ms();
    let dir = bundle_directory(root, at_ms)?;
    let catalog = ctx.catalog.snapshot();
    let views = ctx.folders.all();
    let secrets = known_secrets(ctx, &catalog, &views);
    let mut bundle = Bundle {
        dir: dir.clone(),
        secrets,
        files: vec![],
        scrubbed: 0,
    };

    let status = ctx.status_source.snapshot();
    bundle.json("status.json", serde_json::to_value(&*status)?)?;
    bundle.json("activity.json", serde_json::to_value(&status.activity)?)?;
    for view in &views {
        let name = format!("folders/{}.heartbeat.json", file_safe(&view.record.state));
        bundle.json(&name, folder_view(view))?;
    }
    bundle.json("catalog.redacted.json", catalog_redacted(&catalog))?;

    let data = &ctx.config.data_directory;
    let daemon_log = log::file_log()
        .map(|installed| installed.path)
        .unwrap_or_else(|| log::daemon_log_path(data));
    let daemon_files = log::file_log()
        .map_or(log::ORCHESTRATOR_LOG_FILES, |installed| {
            installed.rotation.files
        })
        .max(log::ORCHESTRATOR_LOG_FILES);
    bundle.text(
        "logs/orchestrator.log.tail",
        &log::tail_lines(&daemon_log, daemon_files, DAEMON_TAIL_BYTES),
    )?;
    for record in &catalog.folders {
        let worker_log = record.directory.join("worker.log");
        if !worker_log.is_file() {
            continue;
        }
        let name = format!("logs/{}.worker.log.tail", file_safe(&record.state));
        let files = super::process::WORKER_LOG_FILES;
        bundle.text(
            &name,
            &log::tail_lines(&worker_log, files, WORKER_TAIL_BYTES),
        )?;
    }
    bundle.json("versions.json", versions(ctx, &views))?;

    prune(root, &dir);
    let files: Vec<Value> = bundle
        .files
        .iter()
        .map(|(path, bytes)| json!({"path": path, "bytes": bytes}))
        .collect();
    Ok(json!({
        "schema": SCHEMA,
        "path": dir.display().to_string(),
        "at_ms": at_ms,
        "files": files,
        "log_path": daemon_log.display().to_string(),
        "secrets_redacted": bundle.scrubbed,
    }))
}

/// The files of one bundle being written.
struct Bundle {
    dir: PathBuf,
    secrets: Vec<String>,
    /// `(relative path, bytes)`.
    files: Vec<(String, u64)>,
    /// Files in which a known secret was replaced.
    scrubbed: usize,
}

impl Bundle {
    fn json(&mut self, name: &str, mut value: Value) -> Result<()> {
        log::redact_value(&mut value);
        let text = serde_json::to_string_pretty(&value)? + "\n";
        self.write(name, &text)
    }

    /// Text that was already redacted line by line (log tails).
    fn text(&mut self, name: &str, text: &str) -> Result<()> {
        self.write(name, text)
    }

    fn write(&mut self, name: &str, text: &str) -> Result<()> {
        let scrubbed = log::scrub_with(text, &self.secrets);
        let changed = matches!(scrubbed, std::borrow::Cow::Owned(_));
        let text = log::scrub(&scrubbed).into_owned();
        if changed {
            self.scrubbed += 1;
        }
        let path = self.dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        fs::write(&path, text.as_bytes()).with_context(|| format!("writing {}", path.display()))?;
        self.files.push((name.to_owned(), text.len() as u64));
        Ok(())
    }
}

/// `root/<UTC timestamp>` (with `-n` appended if it exists), created.
fn bundle_directory(root: &Path, at_ms: u64) -> Result<PathBuf> {
    fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
    let stamp = timestamp(at_ms);
    for n in 0..1000u32 {
        let name = if n == 0 {
            stamp.clone()
        } else {
            format!("{stamp}-{n}")
        };
        let dir = root.join(name);
        match fs::create_dir(&dir) {
            Ok(()) => return Ok(dir),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("creating {}", dir.display()));
            }
        }
    }
    anyhow::bail!("BUSY: too many diagnostics bundles at {stamp}")
}

/// `YYYYMMDDTHHMMSS.mmmZ` (UTC): sortable, and valid in Windows file names.
pub fn timestamp(at_ms: u64) -> String {
    let secs = (at_ms / 1000) as i64;
    let (days, rest) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}.{:03}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        at_ms % 1000
    )
}

/// Proleptic Gregorian date of a day count since 1970-01-01.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Whether a directory name looks like a bundle ([`timestamp`], maybe `-n`).
fn is_bundle_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() >= 20
        && bytes[..8].iter().all(u8::is_ascii_digit)
        && bytes[8] == b'T'
        && bytes[9..15].iter().all(u8::is_ascii_digit)
        && bytes[15] == b'.'
        && bytes[19] == b'Z'
}

/// Keeps the newest [`KEEP_BUNDLES`] bundles (`keep` always survives).
fn prune(root: &Path, keep: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    let mut bundles: Vec<PathBuf> = entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|t| t.is_dir()))
        .filter(|entry| entry.file_name().to_str().is_some_and(is_bundle_name))
        .map(|entry| entry.path())
        .collect();
    bundles.sort();
    let excess = bundles.len().saturating_sub(KEEP_BUNDLES);
    for old in bundles.into_iter().take(excess) {
        if old != keep {
            let _ = fs::remove_dir_all(&old);
        }
    }
}

/// A state id as a file name (anything but `[A-Za-z0-9._-]` becomes `_`).
fn file_safe(name: &str) -> String {
    let name: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() || name.chars().all(|c| c == '.') {
        "_".to_owned()
    } else {
        name
    }
}

/// A folder controller's view without the worker token.
fn folder_view(view: &FolderView) -> Value {
    let worker = view.worker.as_ref().map(|worker| {
        json!({
            "instance": worker.instance,
            "pid": worker.pid,
            "address": worker.address,
            "version": worker.version,
            "token": worker.token.as_ref().map(|_| log::REDACTED),
        })
    });
    json!({
        "state": view.record.state,
        "label": view.record.label,
        "repo": view.record.repo,
        "generation": view.generation,
        "phase": view.phase,
        "gate": view.gate,
        "mount": view.mount,
        "worker": worker,
        "heartbeat": view.heartbeat,
        "last_error": view.last_error,
        "published_error": view.published_error,
        "restarts": view.restarts,
        "degraded": view.degraded,
    })
}

/// The catalog without codes, the verifier or any credential.
fn catalog_redacted(catalog: &CatalogSnapshot) -> Value {
    let peers: Vec<Value> = catalog
        .peers
        .iter()
        .map(|peer| {
            json!({
                "endpoint": peer.endpoint,
                "installation": peer.installation,
                "connections": peer.connections,
                "revision": peer.revision,
                "has_code": !peer.code.is_empty(),
            })
        })
        .collect();
    json!({
        "generation": catalog.generation,
        "installation": catalog.installation,
        "folders": catalog.folders,
        "peers": peers,
        "pairing": {
            "verifier_set": catalog.verifier.is_some(),
            "verifier_generation": catalog.verifier_generation,
        },
        "gates": catalog.gates,
        "peer_meta": catalog.peer_meta,
    })
}

fn versions(ctx: &Arc<Ctx>, views: &[Arc<FolderView>]) -> Value {
    let workers: Vec<Value> = views
        .iter()
        .filter_map(|view| {
            view.worker.as_ref().map(|worker| {
                json!({
                    "state": view.record.state,
                    "version": worker.version,
                    "pid": worker.pid,
                    "instance": worker.instance,
                })
            })
        })
        .collect();
    json!({
        "tkfs": crate::worker::version(),
        "package": env!(stringify!(CARGO_PKG_VERSION)),
        "protocol": PROTOCOL_VERSION,
        "ipc": IPC_VERSION,
        "feed_format": FEED_FORMAT,
        "status_schema": STATUS_SCHEMA,
        "diagnostics_schema": SCHEMA,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "family": std::env::consts::FAMILY,
        "pid": std::process::id(),
        "started_at_ms": ctx.started_at_ms,
        "installation": ctx.installation,
        "executable": std::env::current_exe().ok().map(|p| p.display().to_string()),
        "log": log::file_log(),
        "workers": workers,
    })
}

/// Every secret this daemon can name (see the module docs), at least
/// `log::MIN_SECRET_LEN` long, longest first (a secret that contains another
/// is replaced whole).
fn known_secrets(
    ctx: &Arc<Ctx>,
    catalog: &CatalogSnapshot,
    views: &[Arc<FolderView>],
) -> Vec<String> {
    let mut secrets: Vec<String> = vec![];
    for peer in &catalog.peers {
        secrets.push(peer.code.clone());
    }
    if let Some(verifier) = &catalog.verifier {
        secrets.push(verifier.hash.clone());
    }
    for view in views {
        if let Some(token) = view.worker.as_ref().and_then(|w| w.token.clone()) {
            secrets.push(token);
        }
    }
    let mut directories = vec![ctx.config.data_directory.clone()];
    directories.extend(catalog.folders.iter().map(|f| f.directory.clone()));
    for directory in directories {
        token_files(&directory, &mut secrets);
        for name in ["worker.json", "runtime.json", "orchestrator.json"] {
            if let Ok(bytes) = fs::read(directory.join(name))
                && let Ok(value) = serde_json::from_slice::<Value>(&bytes)
            {
                sensitive_strings(&value, &mut secrets, 0);
            }
        }
    }
    secrets.retain(|secret| secret.trim().len() >= log::MIN_SECRET_LEN);
    for secret in &mut secrets {
        *secret = secret.trim().to_owned();
    }
    secrets.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    secrets.dedup();
    secrets
}

/// The contents of every `*.token` file directly in `directory`.
fn token_files(directory: &Path, out: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "token")
            && entry.file_type().is_ok_and(|t| t.is_file())
            && let Ok(text) = fs::read_to_string(&path)
        {
            out.push(text.trim().to_owned());
        }
    }
}

/// String values of sensitive fields, at any depth.
fn sensitive_strings(value: &Value, out: &mut Vec<String>, depth: usize) {
    if depth > 32 {
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if log::is_sensitive_key(key)
                    && let Some(text) = value.as_str()
                {
                    out.push(text.to_owned());
                }
                sensitive_strings(value, out, depth + 1);
            }
        }
        Value::Array(items) => {
            for item in items {
                sensitive_strings(item, out, depth + 1);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_are_utc_and_sortable() {
        assert_eq!(timestamp(0), "19700101T000000.000Z");
        // 2024-02-29T23:59:59.999Z (a leap day).
        assert_eq!(timestamp(1_709_251_199_999), "20240229T235959.999Z");
        // 2026-10-16T12:34:56.789Z.
        assert_eq!(timestamp(1_792_154_096_789), "20261016T123456.789Z");
        assert!(is_bundle_name(&timestamp(1_792_154_096_789)));
        assert!(is_bundle_name("20261016T123456.789Z-3"));
        assert!(!is_bundle_name("notes"));
        assert!(!is_bundle_name("20261016-123456"));
    }

    #[test]
    fn file_names_are_safe() {
        assert_eq!(file_safe("a1b2-c3_d.e"), "a1b2-c3_d.e");
        assert_eq!(file_safe("../x:y"), ".._x_y");
        assert_eq!(file_safe(".."), "_");
        assert_eq!(file_safe(""), "_");
    }

    #[test]
    fn only_the_newest_bundles_are_kept() {
        let root = tempfile::tempdir().unwrap();
        let mut made = vec![];
        for n in 0..(KEEP_BUNDLES + 3) {
            let dir = bundle_directory(root.path(), 1_792_154_096_000 + n as u64).unwrap();
            fs::write(dir.join("status.json"), "{}").unwrap();
            made.push(dir);
        }
        // Not bundles: never removed.
        fs::create_dir(root.path().join("notes")).unwrap();
        fs::write(root.path().join("19700101T000000.000Z"), "a file").unwrap();
        let newest = made.last().unwrap().clone();
        prune(root.path(), &newest);
        let kept: Vec<_> = made.iter().filter(|dir| dir.exists()).collect();
        assert_eq!(kept.len(), KEEP_BUNDLES);
        assert!(
            kept.iter().all(|dir| made[3..].contains(dir)),
            "the oldest go"
        );
        assert!(root.path().join("notes").is_dir());
        assert!(root.path().join("19700101T000000.000Z").is_file());
        // Two bundles in the same millisecond get distinct directories.
        let a = bundle_directory(root.path(), 5).unwrap();
        let b = bundle_directory(root.path(), 5).unwrap();
        assert_ne!(a, b);
        assert!(is_bundle_name(b.file_name().unwrap().to_str().unwrap()));
    }

    #[test]
    fn sensitive_strings_are_found_at_any_depth() {
        let value = json!({"pid": 1, "token": "tok-0123456789", "nested": {"secret": "s3cr3t-value"},
            "list": [{"credential": "cred-abcdefgh"}], "other": "visible"});
        let mut out = vec![];
        sensitive_strings(&value, &mut out, 0);
        out.sort();
        assert_eq!(out, ["cred-abcdefgh", "s3cr3t-value", "tok-0123456789"]);
    }
}
