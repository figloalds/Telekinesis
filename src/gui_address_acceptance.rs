//! Disposable native three-field pairing and receiver setup acceptance.
use super::{App, snapshot};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use slint::{ComponentHandle, Model};
use std::path::{Path, PathBuf};
use tkfs::pairing_service::Config;
use zeroize::Zeroizing;

#[derive(Deserialize)]
struct Fixture {
    root: PathBuf,
    orchestrator: PathBuf,
    port: u16,
}
pub(super) struct Acceptance {
    fixture: Fixture,
    step: usize,
    code: Zeroizing<String>,
    fingerprint: String,
}
impl Acceptance {
    pub fn from_stdin() -> Result<Self> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::io::stdin().take(65537).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 65536, "ADDRESS_FIXTURE_TOO_LARGE");
        Ok(Self {
            fixture: serde_json::from_slice(&bytes)?,
            step: 0,
            code: Zeroizing::new(String::new()),
            fingerprint: String::new(),
        })
    }
    fn client(&self) -> PathBuf {
        self.fixture.root.join("o1/desktop-pairing/client.toml")
    }
    fn submit(&self, ui: &App, port: u16, code: &str) {
        ui.set_pair_host("127.0.0.1".into());
        ui.set_pair_port(port.to_string().into());
        ui.set_pair_token(code.into());
        ui.invoke_pair_action(14);
    }
    pub fn tick(&mut self, ui: &App, directory: &Path, checks: &mut Vec<String>) -> Result<bool> {
        match self.step {
            0 => {
                ui.set_config_input(
                    self.fixture
                        .orchestrator
                        .to_string_lossy()
                        .into_owned()
                        .into(),
                );
                ui.invoke_select_config();
            }
            1 => {
                ensure!(
                    ui.get_connected(),
                    "ADDRESS_OWNER_CONNECT: {}",
                    ui.get_message()
                );
                ui.set_main_page(1);
                ui.set_pairing_page(2);
                ui.set_receiving_address("127.0.0.1".into());
                ui.set_receiving_port(self.fixture.port.to_string().into());
                ui.invoke_receiving_action(true);
                ui.invoke_receiving_action(true);
            }
            2 => {
                ensure!(!ui.get_error(), "RECEIVER_SETUP: {}", ui.get_message());
                if ui.get_receiving_state() != "running" {
                    ui.invoke_refresh();
                    return Ok(false);
                }
                ensure!(!self.client().exists(), "STARTUP_PROVISIONED_CLIENT");
                snapshot(ui, &directory.join("receiver-ready.bmp"))?;
                ui.set_server_reset_confirmed(true);
                ui.invoke_reset_pairing_code();
                ui.invoke_reset_pairing_code();
            }
            3 => {
                ensure!(
                    ui.get_server_code_visible(),
                    "ADDRESS_KEY_MISSING: {}",
                    ui.get_message()
                );
                self.code = Zeroizing::new(ui.get_server_code().to_string());
                tkfs::pairing_code::validate(&self.code)?;
                ensure!(
                    snapshot(ui, &directory.join("forbidden-key.bmp")).is_err(),
                    "KEY_SCREENSHOT_ALLOWED"
                );
                ui.invoke_dismiss_server_code();
                ui.set_pairing_page(0);
                ui.set_pair_host("hostname.invalid".into());
                ui.set_pair_token(self.code.as_str().into());
                ui.invoke_pair_action(14);
            }
            4 => {
                ensure!(
                    ui.get_error() && !self.client().exists() && ui.get_pair_token().is_empty(),
                    "INVALID_IP_PROVISIONED_OR_RETAINED_KEY"
                );
                snapshot(ui, &directory.join("simple-validation.bmp"))?;
                self.submit(ui, self.fixture.port, &"0".repeat(200));
            }
            5 => {
                ensure!(
                    ui.get_error() && self.client().exists(),
                    "WRONG_KEY_ACCEPTED_OR_SETUP_FAILED: {}",
                    ui.get_message()
                );
                let client = Config::load(&self.client())?;
                ensure!(client.registry()?.peers()?.is_empty(), "WRONG_KEY_TRUSTED");
                self.fingerprint = client.identity()?.fingerprint();
                self.submit(ui, 1, &self.code);
                ui.invoke_pair_cancel();
            }
            6 => {
                ensure!(
                    ui.get_error() && ui.get_pair_token().is_empty(),
                    "CANCEL_RETAINED_KEY"
                );
                self.submit(ui, self.fixture.port, &self.code);
                ui.invoke_pair_action(14);
            }
            7 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_pairing_page() == 1
                        && ui.get_pair_peers().row_count() == 1
                        && ui.get_pair_token().is_empty(),
                    "SIMPLE_ENROLLMENT_FAILED: {}",
                    ui.get_message()
                );
                ensure!(
                    Config::load(&self.client())?.identity()?.fingerprint() == self.fingerprint,
                    "CLIENT_IDENTITY_REPLACED"
                );
                snapshot(ui, &directory.join("awaiting-approval.bmp"))?;
                ui.set_pairing_page(2);
                ui.invoke_refresh();
            }
            8 => {
                ensure!(
                    ui.get_pending_devices().row_count() == 1,
                    "REPEAT_ENROLLMENT_NOT_SUPPRESSED"
                );
                snapshot(ui, &directory.join("pending-device.bmp"))?;
                let candidate = ui
                    .get_pending_devices()
                    .row_data(0)
                    .context("NO_CANDIDATE")?;
                ui.invoke_approve_device(candidate.invitation.clone());
                ui.invoke_approve_device(candidate.invitation);
            }
            9 => {
                ensure!(
                    !ui.get_error() && ui.get_pending_devices().row_count() == 0,
                    "APPROVAL_FAILED: {}",
                    ui.get_message()
                );
                ui.set_pairing_page(1);
                ui.set_config_input(
                    self.fixture
                        .orchestrator
                        .to_string_lossy()
                        .into_owned()
                        .into(),
                );
                ui.invoke_select_config();
            }
            10 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_pair_ready()
                        && ui.get_pair_peers().row_count() == 1
                        && ui.get_pair_token().is_empty(),
                    "EXISTING_PAIRING_NOT_REOPENED"
                );
                ensure!(
                    Config::load(&self.client())?.identity()?.fingerprint() == self.fingerprint,
                    "REOPEN_IDENTITY_CHANGED"
                );
                ui.invoke_pair_action(4);
            }
            11 => {
                ensure!(
                    !ui.get_error() && ui.get_pair_repos().row_count() == 0,
                    "APPROVED_DISCOVERY_FAILED: {}",
                    ui.get_message()
                );
                ui.set_pairing_page(2);
                ui.invoke_receiving_action(false);
            }
            12 => {
                ensure!(!ui.get_error(), "RECEIVER_DISABLE_FAILED");
                if ui.get_receiving_state() != "disabled" {
                    ui.invoke_refresh();
                    return Ok(false);
                }
                ensure!(
                    Config::load(&self.client())?.identity()?.fingerprint() == self.fingerprint,
                    "DISABLE_REPLACED_IDENTITY"
                );
                // Startup must not turn a missing registry into a new trust DB.
                let client = Config::load(&self.client())?;
                std::fs::remove_file(client.data_directory.join("pairs.sqlite"))?;
                ui.set_config_input(
                    self.fixture
                        .orchestrator
                        .to_string_lossy()
                        .into_owned()
                        .into(),
                );
                ui.invoke_select_config();
            }
            13 => {
                ensure!(
                    ui.get_connected() && ui.get_error() && !ui.get_pair_ready(),
                    "MISSING_REGISTRY_AUTO_REPAIRED"
                );
                ensure!(
                    !Config::load(&self.client())?
                        .data_directory
                        .join("pairs.sqlite")
                        .exists(),
                    "MISSING_TRUST_DB_RECREATED"
                );
                // A damaged existing config is never silently recreated either.
                std::fs::write(self.client(), b"deliberately invalid fixture config")?;
                self.submit(ui, self.fixture.port, &self.code);
            }
            14 => {
                ensure!(
                    ui.get_error()
                        && std::fs::read(self.client())? == b"deliberately invalid fixture config",
                    "DAMAGED_CONFIG_REPLACED"
                );
                ui.set_pairing_page(0);
                ui.window().set_size(slint::LogicalSize::new(980.0, 720.0));
            }
            15 => {
                snapshot(ui, &directory.join("simple-minimum.bmp"))?;
                ui.invoke_shutdown();
            }
            16 => {
                ensure!(
                    !ui.get_connected() && !ui.get_error(),
                    "ADDRESS_SHUTDOWN_FAILED"
                );
                if std::fs::File::open(&self.fixture.root.join("o1/orchestrator.lock")).is_err() {
                    return Ok(false);
                }
                super::code_acceptance::scan(&self.fixture.root, std::slice::from_ref(&self.code))?;
                checks.push("Actual receiver setup/status/disable and owner approval callbacks; numeric IP + port + key auto-provisions encrypted client, authenticates server, waits for approval and loads empty authorized discovery".into());
                checks.push("Invalid IP creates no identity; wrong key creates no trust; cancel clears input; repeated connect/reset/approve gated; reconnect preserves identity; damaged config preserved; fixture files contain no plaintext key; screenshots exclude secrets".into());
                return Ok(true);
            }
            _ => unreachable!(),
        }
        self.step += 1;
        Ok(false)
    }
}
