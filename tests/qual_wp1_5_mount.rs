//! Qualification scenarios for WP1.5 (MountController and adapters).
//! REVAMP_PLAN §14.6, §4.3. Needs WinFsp (Windows) or `/dev/fuse` (Linux);
//! scenarios skip otherwise.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP1.5"]`.
//! Written against the worker JSON ops `mount-attach`/`mount-detach`/`mount-status`.
//! `cargo test --release --test qual_wp1_5_mount -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{fs, io::Write, path::Path, time::Duration};
use support::*;
use tkfs::core::{Store, id};

const FILE: &str = "qual_wp1_5_mount";

fn new_state(dir: &Path) -> std::path::PathBuf {
    let state = dir.join("state");
    drop(Store::initialize(&state, &id(), &id()).unwrap());
    state
}

fn mount_state(reply: &Value) -> String {
    reply["state"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            reply["state"]
                .as_object()
                .and_then(|o| o.keys().next().cloned())
        })
        .unwrap_or_else(|| reply["state"].to_string())
}

/// attach → write → detach → attach on the same worker process.
#[test]
#[ignore = "WP1.5"]
fn attach_detach_attach_same_pid() {
    let s = Scenario::new(FILE, "attach_detach_attach_same_pid");
    if !mount_supported() {
        return s.skip("no mount support");
    }
    let mut s = s;
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("mnt");
    let path = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    let pid = worker.pid();
    let attach = worker.rpc(json!({"op":"mount-attach","path":path}));
    let Ok(attach) = attach else {
        s.unsupported(format!("mount-attach: {attach:?}"));
        return s.finish();
    };
    s.check(
        mount_state(&attach).contains("attached"),
        format!("attached: {attach}"),
    );
    fs::write(mount.join("a.txt"), b"first attach").unwrap();
    let detach = worker.rpc(json!({"op":"mount-detach","force":false}));
    s.check(
        detach
            .as_ref()
            .is_ok_and(|d| mount_state(d).contains("detached")),
        format!("detached: {detach:?}"),
    );
    let (again, ms) = time_ms(|| worker.rpc(json!({"op":"mount-attach","path":path})));
    s.metric("reattach_ms", ms, "ms");
    s.check(
        again
            .as_ref()
            .is_ok_and(|a| mount_state(a).contains("attached")),
        format!("re-attached: {again:?}"),
    );
    s.check(
        fs::read(mount.join("a.txt")).ok().as_deref() == Some(b"first attach".as_slice()),
        "file visible after re-attach",
    );
    s.check(
        worker.pid() == pid && worker.try_exit().is_none(),
        "same pid",
    );
    let _ = worker.rpc(json!({"op":"mount-detach","force":true}));
    s.finish();
}

/// Force detach with a dirty open file persists it (`pending_saves`) and the
/// bytes are visible after re-attach.
#[test]
#[ignore = "WP1.5"]
fn force_detach_persists_dirty() {
    let s = Scenario::new(FILE, "force_detach_persists_dirty");
    if !mount_supported() {
        return s.skip("no mount support");
    }
    let mut s = s;
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("mnt");
    let path = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    if let Err(e) = worker.rpc(json!({"op":"mount-attach","path":path})) {
        s.unsupported(format!("mount-attach: {e:#}"));
        return s.finish();
    }
    let bytes = b"dirty bytes kept by force detach";
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(mount.join("dirty.txt"))
        .unwrap();
    file.write_all(bytes).unwrap();
    let graceful = worker.rpc(json!({"op":"mount-detach","force":false}));
    s.check(
        graceful
            .as_ref()
            .is_err_and(|e| format!("{e:#}").contains("BUSY_VIEW")),
        format!("graceful detach refused while open: {graceful:?}"),
    );
    let forced = worker.rpc(json!({"op":"mount-detach","force":true}));
    drop(file);
    s.check(
        forced
            .as_ref()
            .is_ok_and(|d| mount_state(d).contains("detached")),
        format!("force detach: {forced:?}"),
    );
    let db = rusqlite::Connection::open(state.join("metadata.sqlite")).unwrap();
    let pending: i64 = db
        .query_row("SELECT count(*) FROM pending_saves", [], |r| r.get(0))
        .unwrap_or(0);
    s.check(pending >= 1, format!("pending_saves rows: {pending}"));
    let again = worker.rpc(json!({"op":"mount-attach","path":path}));
    s.check(again.is_ok(), format!("re-attach: {again:?}"));
    let recovered = wait_until(Duration::from_secs(20), || {
        fs::read(mount.join("dirty.txt")).ok().as_deref() == Some(bytes.as_slice())
    });
    s.check(recovered, "dirty bytes visible after recovery");
    let _ = worker.rpc(json!({"op":"mount-detach","force":true}));
    s.finish();
}

/// A pre-existing user directory at the mount path is never touched.
#[test]
#[ignore = "WP1.5"]
fn foreign_mount_untouched() {
    let s = Scenario::new(FILE, "foreign_mount_untouched");
    if !mount_supported() {
        return s.skip("no mount support");
    }
    let mut s = s;
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("foreign");
    fs::create_dir_all(mount.join("nested")).unwrap();
    fs::write(mount.join("keep.txt"), b"foreign data").unwrap();
    fs::write(mount.join("nested/inner.txt"), b"inner").unwrap();
    let path = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&state, &["--mount", &path], &[]).unwrap();
    let ready = worker.wait_ready(Duration::from_secs(60));
    let code = match &ready {
        Ok(r) => r["mount_error"]["code"].as_str().unwrap_or("").to_owned(),
        // A worker that exits reports the code in its log tail.
        Err(e) => {
            let text = format!("{e:#}");
            if text.contains("MOUNT_PATH_MUST_NOT_EXIST") {
                "MOUNT_PATH_MUST_NOT_EXIST".to_owned()
            } else {
                error_code(&text)
            }
        }
    };
    s.note(format!("outcome: {ready:?}"));
    s.check(
        code == "MOUNT_PATH_MUST_NOT_EXIST",
        format!("classified as MOUNT_PATH_MUST_NOT_EXIST, got {code}"),
    );
    s.check(
        fs::read(mount.join("keep.txt")).ok().as_deref() == Some(b"foreign data".as_slice())
            && fs::read(mount.join("nested/inner.txt")).ok().as_deref()
                == Some(b"inner".as_slice()),
        "foreign directory untouched",
    );
    s.check(
        fs::read_dir(&mount).unwrap().count() == 2,
        "no entries added or removed",
    );
    s.finish();
}
