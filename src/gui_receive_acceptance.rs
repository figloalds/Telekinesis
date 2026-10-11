//! Explicit disposable fixture only. All remote-owner grants below are local
//! test setup; production receive callbacks never administer the source.
use super::{App, snapshot};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use slint::Model;
use std::path::{Path, PathBuf};
use tkfs::{pairing::SyncConfiguration, pairing_service::Config, runtime::Discovery};

#[derive(Deserialize)]
struct Fixture {
    orchestrator: PathBuf,
    server: PathBuf,
    client: PathBuf,
    source_runtime: PathBuf,
    repo: String,
    mount: PathBuf,
}
pub(super) struct Acceptance {
    fixture: Fixture,
    step: usize,
    raced: bool,
}
impl Acceptance {
    pub fn from_stdin() -> Result<Self> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::io::stdin().take(131073).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 131072, "RECEIVE_FIXTURE_TOO_LARGE");
        Ok(Self {
            fixture: serde_json::from_slice(&bytes)?,
            step: 0,
            raced: false,
        })
    }
    fn pause(&self, enabled: bool) -> Result<()> {
        let config = Config::load(&self.fixture.client)?;
        let mut registry = config.registry()?;
        let mut grant = registry
            .configurations()?
            .into_iter()
            .next()
            .context("GRANT_MISSING")?;
        grant.enabled = enabled;
        registry.configure_sync(&grant)
    }
    pub fn tick(&mut self, ui: &App, directory: &Path, checks: &mut Vec<String>) -> Result<bool> {
        let f = &self.fixture;
        match self.step {
            0 => {
                ui.set_config_input(f.orchestrator.to_string_lossy().into_owned().into());
                ui.invoke_select_config();
            }
            1 => {
                ensure!(
                    ui.get_connected(),
                    "RECEIVE_CONNECT: {} | {}",
                    ui.get_message(),
                    ui.get_operation_detail()
                );
                ui.set_main_page(4);
                ui.set_pair_config(f.client.to_string_lossy().into_owned().into());
                ui.invoke_pair_action(0);
            }
            2 => {
                ensure!(ui.get_pair_ready(), "RECEIVE_PAIR_CONFIG");
                ensure!(ui.get_pair_peers().row_count() == 1, "FIXTURE_SOURCE_PEER");
                ui.set_pair_selected_peer(0);
                ui.set_receive_label("Received fixture".into());
                ui.set_receive_repository(f.repo.clone().into());
                ui.invoke_receive_action(0);
                ui.invoke_receive_action(0);
            }
            3 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_receive_running()
                        && ui.get_receive_headless()
                        && ui.get_projects().row_count() == 1,
                    "PREPARE: {}",
                    ui.get_message()
                );
                ensure!(
                    !f.mount.exists() && !ui.get_receive_can_attach(),
                    "PREMATURE_MOUNT"
                );
                snapshot(ui, &directory.join("receive-prepared.bmp"))?;
                ui.set_receive_mount(f.mount.to_string_lossy().into_owned().into());
                ui.invoke_receive_action(5);
            }
            4 => {
                ensure!(
                    ui.get_error() && !f.mount.exists(),
                    "UNVERIFIED_ATTACH_ACCEPTED"
                );
                let server = Config::load(&f.server)?;
                let client = Config::load(&f.client)?;
                let source: Discovery = serde_json::from_slice(&std::fs::read(&f.source_runtime)?)?;
                server.registry()?.configure_sync(&SyncConfiguration {
                    peer: client.installation,
                    repo: f.repo.clone(),
                    local_runtime: f.source_runtime.clone(),
                    local_replica: source.device,
                    remote_replica: ui.get_receive_device().to_string(),
                    enabled: true,
                })?;
                ui.invoke_pair_action(4);
            }
            5 => {
                ensure!(
                    !ui.get_error() && ui.get_pair_repos().row_count() == 1,
                    "DISCOVER: {}",
                    ui.get_message()
                );
                ui.set_receive_confirmed(true);
                ui.invoke_receive_action(1);
            }
            6 => {
                ensure!(!ui.get_error(), "CONFIGURE: {}", ui.get_message());
                ui.invoke_receive_action(2);
                ui.invoke_receive_action(2);
            }
            7 => {
                ensure!(!ui.get_error(), "SYNC: {}", ui.get_message());
                if !ui.get_receive_caught_up() {
                    ui.invoke_receive_action(2);
                    return Ok(false);
                }
                self.pause(false)?;
                ui.invoke_refresh();
            }
            8 => {
                ensure!(
                    !ui.get_receive_caught_up() && !ui.get_receive_can_attach(),
                    "PAUSED_GRANT_PROOF_REUSED"
                );
                self.pause(true)?;
                ui.invoke_receive_action(2);
            }
            9 => {
                ensure!(!ui.get_error(), "RESYNC: {}", ui.get_message());
                if !ui.get_receive_caught_up() {
                    ui.invoke_receive_action(2);
                    return Ok(false);
                }
                let branch = ui
                    .get_branches()
                    .iter()
                    .position(|b| b.shared && b.label.contains("main"))
                    .context("SHARED_MAIN_MISSING")?;
                ui.set_selected_branch(branch as i32);
                ui.invoke_receive_action(3);
            }
            10 => {
                ensure!(
                    !ui.get_error() && ui.get_receive_materialized(),
                    "CHECKOUT: {}",
                    ui.get_message()
                );
                snapshot(ui, &directory.join("receive-verified.bmp"))?;
                ui.invoke_receive_action(4);
            }
            11 => {
                ensure!(
                    !ui.get_error() && ui.get_receive_can_attach() && !ui.get_receive_running(),
                    "STOP_PROOF: {}",
                    ui.get_message()
                );
                ui.invoke_refresh();
            }
            12 => {
                ensure!(
                    ui.get_receive_can_attach(),
                    "INTENTIONAL_STOP_INVALIDATED_PROOF"
                );
                if !self.raced {
                    // A competing owner changes the stopped store after the UI
                    // proof. Attachment must check it under its own held lock.
                    let native = PathBuf::from(ui.get_receive_runtime().as_str())
                        .parent()
                        .context("NATIVE_PARENT")?
                        .to_owned();
                    let _lock = tkfs::orchestrator::lock_state(&native)?;
                    let mut store = tkfs::core::Store::open_existing(&native)?;
                    store.fork("competing-owner", false)?;
                    store.checkout("competing-owner")?;
                    self.raced = true;
                    self.step = 99;
                }
                ui.invoke_receive_action(5);
                ui.invoke_receive_action(5);
            }
            13 => {
                ensure!(
                    !ui.get_error()
                        && !ui.get_receive_headless()
                        && !ui.get_receive_running()
                        && !f.mount.exists(),
                    "ATTACH: {}",
                    ui.get_message()
                );
                ui.invoke_receive_action(6);
            }
            14 => {
                ensure!(
                    !ui.get_error() && ui.get_receive_running(),
                    "MOUNT_START: {}",
                    ui.get_message()
                );
                if std::fs::read(f.mount.join("published.txt")).ok().as_deref()
                    != Some(b"receive wizard verified bytes")
                {
                    ui.invoke_refresh();
                    return Ok(false);
                }
                ensure!(
                    !f.mount.join("private-canary.txt").exists(),
                    "PRIVATE_DATA_MOUNTED"
                );
                checks.push("Real callbacks prepare one headless replica, refuse premature attach, discover an owner grant, sync, invalidate paused-grant proof, verify shared checkout, preserve proof through planned stop, attach once and mount verified bytes".into());
                snapshot(ui, &directory.join("receive-mounted.bmp"))?;
                self.pause(false)?;
                ui.invoke_background_sync_action(true);
            }
            15 => {
                ensure!(!ui.get_error(), "BACKGROUND_ENABLE: {}", ui.get_message());
                if !ui.get_background_sync_status().contains("running") {
                    ui.invoke_refresh();
                    return Ok(false);
                }
                ensure!(
                    !Config::load(&f.client)?.registry()?.configurations()?[0].enabled,
                    "BACKGROUND_RESUMED_PAUSED_GRANT"
                );
                ui.invoke_background_sync_action(false);
            }
            16 => {
                ensure!(!ui.get_error(), "BACKGROUND_DISABLE: {}", ui.get_message());
                if !ui
                    .get_background_sync_status()
                    .starts_with("Service: disabled")
                {
                    ui.invoke_refresh();
                    return Ok(false);
                }
                checks.push("Supervised service reaches running then disabled; enabling never resumes a paused grant".into());
                self.pause(true)?;
                ui.invoke_background_sync_action(true);
            }
            100 => {
                ensure!(
                    ui.get_error() && ui.get_receive_headless() && !f.mount.exists(),
                    "COMPETING_BRANCH_ATTACH_ACCEPTED"
                );
                checks.push("Concurrent owner checkout after planned stop is rejected by the locked attachment fence; headless restart and fresh sync recover".into());
                ui.invoke_receive_action(6);
            }
            101 => {
                ensure!(
                    !ui.get_error() && ui.get_receive_running(),
                    "RACE_RECOVERY_START"
                );
                ui.invoke_receive_action(2);
                self.step = 8;
            }
            _ => {
                ensure!(!ui.get_error(), "BACKGROUND_REENABLE: {}", ui.get_message());
                if !ui
                    .get_background_sync_status()
                    .starts_with("Service: running")
                {
                    ui.invoke_refresh();
                    return Ok(false);
                }
                checks.push("GUI exits while supervisor retains enabled background sync; parent fixture verifies subsequent mounted edits reach source".into());
                return Ok(true);
            }
        }
        self.step += 1;
        Ok(false)
    }
}
