//! Real executable/daemon checks. These create only native stores, never mounts.
use anyhow::Result;
use serde_json::json;
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tkfs::desktop::Desktop;

struct Managed(Option<Desktop>);
impl std::ops::Deref for Managed {
    type Target = Desktop;
    fn deref(&self) -> &Desktop {
        self.0.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for Managed {
    fn deref_mut(&mut self) -> &mut Desktop {
        self.0.as_mut().unwrap()
    }
}
impl Drop for Managed {
    fn drop(&mut self) {
        if let Some(desktop) = &mut self.0 {
            let ids: Vec<_> = desktop
                .config
                .folders
                .iter()
                .filter(|f| f.autostart)
                .map(|f| f.id.clone())
                .collect();
            for id in ids {
                if let Err(error) = desktop.stop(&id) {
                    eprintln!("desktop test cleanup: {error:#}");
                }
            }
        }
    }
}
fn open(home: &Path, single: Option<&Path>) -> Result<Managed> {
    let binary = std::env::var_os("TKFS_DESKTOP_TEST_BINARY")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_tkfs").into());
    Ok(Managed(Some(Desktop::open(
        &home.join("orchestrator.toml"),
        single,
        &binary,
    )?)))
}

#[test]
fn fresh_desktop_autoinits_reuses_a_daemon_and_preserves_an_explicit_stop() -> Result<()> {
    let home = tempfile::tempdir()?;
    let state = home.path().join("my-folder");
    let mut first = open(home.path(), Some(&state))?;
    first.autostart();
    let id = first.config.folders[0].id.clone();
    let status = first.request(&id, "status", json!({}))?;
    let pid = status["pid"].clone();
    assert!(state.join("metadata.sqlite").is_file());
    assert!(home.path().join("orchestrator.toml").is_file());
    let snapshot = first.snapshot(&id);
    assert_eq!(snapshot["folders"][0]["online"], true);
    assert!(snapshot["detail"]["branches"].as_array().unwrap().len() == 1);
    // Closing the window leaves sync running. Reopen must reconnect, not spawn
    // another owner or reinitialize the folder's identity.
    drop(first.0.take());
    let mut second = open(home.path(), None)?;
    second.autostart();
    assert_eq!(second.request(&id, "status", json!({}))?["pid"], pid);
    second.stop(&id)?;
    drop(second);
    let mut third = open(home.path(), None)?;
    third.autostart();
    assert!(!third.config.folders[0].autostart);
    assert_eq!(third.snapshot(&id)["folders"][0]["online"], false);
    assert!(!state.join("control.json").exists());
    Ok(())
}

#[test]
fn desktop_recovers_stale_control_records_and_reports_real_startup_failures() -> Result<()> {
    let home = tempfile::tempdir()?;
    let state = home.path().join("folder");
    std::fs::create_dir_all(&state)?;
    std::fs::write(state.join("control.json"), "invalid stale control record")?;
    let mut desktop = open(home.path(), Some(&state))?;
    desktop.autostart();
    let id = desktop.config.folders[0].id.clone();
    desktop.request(&id, "status", json!({}))?;
    desktop.stop(&id)?;
    // A real worker startup failure after a successful `init`: a directory
    // where the worker writes `runtime.token` (fatal RUNTIME_FILES_FAILED, 73).
    std::fs::create_dir(state.join("runtime.token"))?;
    let error = desktop.start(&id).unwrap_err();
    assert!(
        error.to_string().contains("DAEMON_START_FAILED"),
        "{error:#}"
    );
    assert!(
        error.to_string().contains("RUNTIME_FILES_FAILED"),
        "{error:#}"
    );
    let value = desktop.snapshot(&id);
    assert_eq!(value["folders"][0]["online"], false);
    assert!(
        value["detail"].is_null(),
        "failed startup must not show empty loaded conflict data"
    );
    assert!(!state.join("control.json").exists());
    Ok(())
}

/// Pairing-code discovery (`catalog.list` on a Probe lane). The worker-level
/// catalog that provided it was removed; the orchestrator brings it back.
fn probe(endpoint: &str, code: &str) -> Result<serde_json::Value> {
    use tkfs::session;
    let hello = session::client_hello(
        "desktop-test-probe",
        tkfs::model::protocol::LaneRole::Probe,
        None,
        1,
    );
    let (conn, _) = session::dial(
        endpoint,
        hello,
        &session::CodeAuth { code: code.into() },
        None,
        session::ConnOptions::default(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let reply = conn
        .call(
            "catalog.list",
            json!({}),
            Vec::new(),
            session::Priority::Control,
            Duration::from_secs(10),
        )
        .map_err(|e| anyhow::anyhow!("{e}"));
    conn.close();
    Ok(serde_json::from_slice(&reply?.body)?)
}

#[test]
#[ignore = "worker peer listener removed by design; pairing/sync return at the orchestrator level (Task 11) and the desktop becomes an orchestrator client in the GUI plan"]
fn desktop_can_receive_pair_and_manage_versions_over_the_current_protocol() -> Result<()> {
    let home = tempfile::tempdir()?;
    let mut desktop = open(home.path(), None)?;
    let source = desktop.create("Source", None, None)?;
    let status = desktop.request(&source, "status", json!({}))?;
    let endpoint = status["endpoint"].as_str().unwrap();
    let code = desktop.request(&source, "pairing-code", json!({}))?["code"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(probe(endpoint, "wrong-code").is_err());
    let info = probe(endpoint, &code)?;
    assert_eq!(info["label"], "Source");
    let target = desktop.create("Received", Some(info["repo"].as_str().unwrap()), None)?;
    desktop.request(&target, "pair", json!({"endpoint":endpoint,"code":code}))?;
    let payload = home.path().join("import.bin");
    std::fs::write(&payload, b"received using desktop startup")?;
    desktop.request(&source, "import", json!({"path":"file","source":payload}))?;
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(reply) = desktop.request(&target, "cat", json!({"path":"file"})) {
            let bytes: Vec<u8> = serde_json::from_value(reply["bytes"].clone())?;
            if bytes == b"received using desktop startup" {
                break;
            }
        }
        anyhow::ensure!(Instant::now() < end, "received file did not arrive");
        std::thread::sleep(Duration::from_millis(50));
    }
    desktop.request(&target, "checkpoint", json!({}))?;
    let branch = desktop.request(&target, "fork", json!({"name":"draft"}))?["branch"].clone();
    desktop.request(&target, "checkout", json!({"branch":branch}))?;
    let snapshot = desktop.request(&target, "desktop-snapshot", json!({}))?;
    assert_eq!(snapshot["status"]["branch"], branch);
    assert_eq!(snapshot["peers"].as_array().unwrap().len(), 1);
    assert!(!snapshot.to_string().contains("credential"));
    assert_eq!(
        desktop.request(&target, "peer-info", json!({"endpoint":endpoint}))?["repo"],
        info["repo"]
    );
    let version = snapshot["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row[1]["operation"] == "content")
        .unwrap()[1]["id"]
        .clone();
    assert!(
        desktop.request(&target, "preview-version", json!({"version":version}))?["preview"]
            .as_str()
            .unwrap()
            .contains("received using desktop startup")
    );
    let installation = snapshot["peers"][0]["installation"].clone();
    desktop.request(&target, "unpair", json!({"installation":installation}))?;
    assert!(
        desktop.request(&target, "desktop-snapshot", json!({}))?["peers"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    Ok(())
}

#[test]
fn cleanup_quarantine_survives_reopening_the_desktop() -> Result<()> {
    let home = tempfile::tempdir()?;
    let state = home.path().join("folder");
    let first = open(home.path(), Some(&state))?;
    let path = first.config_path.with_extension("blocked.json");
    drop(first);
    std::fs::write(
        path,
        r#"{"stranded":true,"reason":"retained cleanup failure"}"#,
    )?;
    let mut reopened = open(home.path(), None)?;
    reopened.autostart();
    let id = reopened.config.folders[0].id.clone();
    assert!(reopened.start(&id).is_err());
    assert!(!state.join("metadata.sqlite").exists());
    assert_eq!(reopened.snapshot(&id)["blocked"], true);
    reopened.config.folders[0].autostart = false; // no daemon exists for fixture cleanup
    Ok(())
}

#[cfg(windows)]
#[test]
#[ignore = "isolated Windows VM only; validates real desktop-owned mount stop/start"]
fn desktop_mounted_start_stop() -> Result<()> {
    anyhow::ensure!(
        std::env::var("TKFS_DESKTOP_ISOLATED_VM").as_deref() == Ok("1"),
        "requires isolated Windows VM"
    );
    let root = std::path::PathBuf::from(
        std::env::var_os("TKFS_DESKTOP_MOUNT_HOME")
            .expect("persistent native evidence directory required"),
    );
    for kind in ["letter", "directory"] {
        let home = root.join(kind);
        std::fs::create_dir_all(&home)?;
        let state = home.join("store");
        let mut desktop = open(&home, Some(&state))?;
        let id = desktop.config.folders[0].id.clone();
        let mount = if kind == "letter" {
            std::path::PathBuf::from("P:")
        } else {
            home.join("mount")
        };
        desktop.config.folders[0].mount = Some(mount.clone());
        desktop.config.folders[0].mount_enabled = true;
        desktop.start(&id)?;
        assert_eq!(
            desktop.request(&id, "status", json!({}))?["mount"],
            json!(mount)
        );
        desktop.set_mount_enabled(&id, false)?;
        assert!(desktop.request(&id, "status", json!({}))?["mount"].is_null());
        desktop.set_mount_enabled(&id, true)?;
        assert_eq!(
            desktop.request(&id, "status", json!({}))?["mount"],
            json!(mount)
        );
        desktop.stop(&id)?;
        assert!(!state.join("control.json").exists());
        println!("{kind}: mounted -> sync only -> mounted -> stopped; all bounded exits passed");
    }
    Ok(())
}

#[test]
fn a_failed_desktop_shutdown_reports_error_and_keeps_recovery_bytes() -> Result<()> {
    let home = tempfile::tempdir()?;
    let mut desktop = open(home.path(), None)?;
    let id = desktop.create("Fault", None, None)?;
    let state = desktop.folder(&id)?.state;
    desktop.request(&id, "inject-fault", json!({"name":"flush-persistent"}))?;
    let payload = home.path().join("recover.bin");
    std::fs::write(&payload, b"recover this local save")?;
    assert!(
        desktop
            .request(&id, "import", json!({"path":"recover","source":payload}))
            .is_err()
    );
    assert!(desktop.stop(&id).is_err());
    assert!(!state.join("control.json").exists());
    assert!(!desktop.folder(&id)?.autostart);
    // A failed flush cannot strand an owned daemon or prevent a later clean start.
    desktop.start(&id)?;
    let bytes: Vec<u8> = serde_json::from_value(
        desktop.request(&id, "cat", json!({"path":"recover"}))?["bytes"].clone(),
    )?;
    assert_eq!(bytes, b"recover this local save");
    Ok(())
}
