//! Legacy fixtures captured with the baseline binary (6298d0b) by WP0.5
//! (`tests/fixtures/legacy`, see its README). Tests never touch the committed
//! files: `copy_legacy_worker()` and `copy_legacy_datadir()` return temp copies
//! of the committed files only (SQLite `-wal`/`-shm`/`-journal` side files,
//! which `.gitignore` hides, are never copied).
use super::node::NodeConfig;
use anyhow::{Context, Result};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
};

pub fn legacy_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy")
}

fn manifest(kind: &str) -> Value {
    let path = legacy_dir().join(kind).join("manifest.json");
    serde_json::from_slice(
        &fs::read(&path).unwrap_or_else(|e| panic!("missing {}: {e}", path.display())),
    )
    .unwrap()
}

/// A temp copy of the legacy worker state (`STATE` = `state`).
pub struct LegacyWorker {
    pub temp: tempfile::TempDir,
    pub state: PathBuf,
    /// `tests/fixtures/legacy/worker/manifest.json`: what the state contains.
    pub manifest: Value,
}

/// Copy `tests/fixtures/legacy/worker/state` to a new temp directory.
///
/// Contents (6298d0b format): resolved and unresolved conflicts, staged inbox
/// rows waiting for a missing parent, quarantined inbox rows (an invalid event
/// and its quarantined descendant), a partial spool in `incoming-objects`, and
/// one `pending_saves` row with its `staging/*.stage` file.
pub fn copy_legacy_worker() -> LegacyWorker {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    copy_fixture(&legacy_dir().join("worker/state"), &state).unwrap();
    LegacyWorker {
        temp,
        state,
        manifest: manifest("worker"),
    }
}

/// A temp copy of the legacy orchestrator root: `ROOT/orchestrator.toml` (written
/// here with `NodeConfig::default()`) and `ROOT/data` (the captured data dir).
pub struct LegacyDataDir {
    pub root: tempfile::TempDir,
    pub data: PathBuf,
    pub config: PathBuf,
    /// `tests/fixtures/legacy/datadir/manifest.json`.
    pub manifest: Value,
}

/// Copy `tests/fixtures/legacy/datadir/data` to `ROOT/data` and relocate the
/// absolute paths stored in `folders.sqlite` (`projects.directory`,
/// `projects.mount`) from the capture root to `ROOT`, the way a user moving a
/// data directory would. The captured peer endpoint (an ephemeral port of the
/// capture run) becomes `DEAD_PEER_ENDPOINT`, so a daemon on the copy never
/// dials a listener of a parallel test. Everything else is byte-for-byte the
/// baseline output.
///
/// Contents: `projects` (a received running folder, a received folder gated by
/// `initial_sync`, a stopped local folder), one outbound `peers` row, the
/// `initial_sync` row and a pairing verifier (`settings.pairing`).
pub fn copy_legacy_datadir() -> LegacyDataDir {
    copy_legacy_datadir_with(&NodeConfig::default()).unwrap()
}

pub fn copy_legacy_datadir_with(cfg: &NodeConfig) -> Result<LegacyDataDir> {
    let root = tempfile::tempdir()?;
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o777))?;
    }
    let data = root.path().join("data");
    copy_fixture(&legacy_dir().join("datadir/data"), &data)?;
    let config = root.path().join("orchestrator.toml");
    fs::write(&config, cfg.toml())?;
    let manifest = manifest("datadir");
    relocate(&data, &manifest, root.path())?;
    Ok(LegacyDataDir {
        root,
        data,
        config,
        manifest,
    })
}

/// Whether `name` is a SQLite side file (`-wal`, `-shm`, `-journal`). The
/// committed fixtures have none (their WALs were checkpointed); one next to a
/// fixture database was left by a tool that opened the fixture in place and
/// is ignored by git, so it must not reach a test's copy.
pub fn is_sqlite_side_file(name: &str) -> bool {
    ["-wal", "-shm", "-journal"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
}

/// Copy a fixture tree without SQLite side files.
pub fn copy_fixture(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_fixture(&entry.path(), &target)?;
        } else if !is_sqlite_side_file(&entry.file_name().to_string_lossy()) {
            fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// SQLite side files anywhere under `root` (none in a fresh fixture copy).
pub fn sqlite_side_files(root: &Path) -> Vec<PathBuf> {
    let mut found = vec![];
    for entry in fs::read_dir(root).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(sqlite_side_files(&path));
        } else if is_sqlite_side_file(&entry.file_name().to_string_lossy()) {
            found.push(path);
        }
    }
    found
}

/// Where the legacy data dir's outbound peer points in a temp copy: a closed,
/// privileged port (TCP discard), never handed out as an ephemeral port.
pub const DEAD_PEER_ENDPOINT: &str = "ws://127.0.0.1:9";

/// Rewrite the capture-time absolute prefixes in `projects` to `new_root`, and
/// every captured peer endpoint (`peers`, `initial_sync`) to `DEAD_PEER_ENDPOINT`.
pub fn relocate(data: &Path, manifest: &Value, new_root: &Path) -> Result<()> {
    let old_root = manifest["root"].as_str().context("manifest root")?;
    // The daemon stores paths derived from the canonical configuration path.
    let new_root = fs::canonicalize(new_root)?;
    let db = rusqlite::Connection::open(data.join("folders.sqlite"))?;
    let rows: Vec<(String, String, Option<String>)> = db
        .prepare("SELECT state,directory,mount FROM projects")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for (state, directory, mount) in rows {
        let directory = relocate_path(&directory, old_root, &new_root).unwrap_or(directory);
        let mount = mount.map(|m| relocate_path(&m, old_root, &new_root).unwrap_or(m));
        db.execute(
            "UPDATE projects SET directory=?,mount=? WHERE state=?",
            rusqlite::params![directory, mount, state],
        )?;
    }
    for peer in manifest["peers"].as_array().context("manifest peers")? {
        let endpoint = peer["endpoint"].as_str().context("peer endpoint")?;
        db.execute(
            "UPDATE peers SET endpoint=? WHERE endpoint=?",
            [DEAD_PEER_ENDPOINT, endpoint],
        )?;
        db.execute(
            "UPDATE initial_sync SET endpoint=? WHERE endpoint=?",
            [DEAD_PEER_ENDPOINT, endpoint],
        )?;
    }
    Ok(())
}

/// `old_prefix/rest` → `new_prefix/rest`, accepting `/` and `\` in `rest`.
pub fn relocate_path(old: &str, old_prefix: &str, new_prefix: &Path) -> Option<String> {
    let rest = old.strip_prefix(old_prefix)?;
    let mut path = new_prefix.to_path_buf();
    for component in rest.split(['/', '\\']).filter(|c| !c.is_empty()) {
        path.push(component);
    }
    Some(path.to_string_lossy().into_owned())
}

/// Fold every SQLite WAL under `root` into its database file and remove the
/// `-wal`/`-shm` side files (content unchanged). Used when capturing fixtures.
pub fn checkpoint_tree(root: &Path) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            checkpoint_tree(&path)?;
        } else if path.extension().is_some_and(|e| e == "sqlite") {
            let db = rusqlite::Connection::open(&path)?;
            db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
            drop(db);
            for suffix in ["-wal", "-shm"] {
                let side = PathBuf::from(format!("{}{suffix}", path.display()));
                if side.exists() {
                    fs::remove_file(side)?;
                }
            }
        }
    }
    Ok(())
}

/// Total size in bytes of the files under `root`.
pub fn tree_size(root: &Path) -> u64 {
    fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| {
            let path = e.path();
            if path.is_dir() {
                tree_size(&path)
            } else {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            }
        })
        .sum()
}
