//! Opt-in disposable native code-enrollment acceptance. Public fixture paths
//! arrive on stdin; codes are created by the real reset callback and stay in RAM.
use super::{App, UiLifetime, View, apply, close_ui, snapshot};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use slint::Model;
use std::path::{Path, PathBuf};
use tkfs::{pairing::Peer, pairing_code, pairing_service::Config};
use zeroize::Zeroizing;

#[derive(Deserialize)]
struct Fixture {
    root: PathBuf,
    orchestrator: PathBuf,
    server: PathBuf,
    client: PathBuf,
    wrong_bootstrap: Peer,
}
pub(super) struct Acceptance {
    fixture: Fixture,
    step: usize,
    secrets: Vec<Zeroizing<String>>,
    public: String,
    enrollment: String,
}
impl Acceptance {
    pub fn from_stdin() -> Result<Self> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::io::stdin().take(131073).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 131072, "CODE_FIXTURE_TOO_LARGE");
        let fixture: Fixture = serde_json::from_slice(&bytes)?;
        let server = Config::load(&fixture.server)?;
        Ok(Self {
            public: serde_json::to_string(&tkfs::pairing_service::public_bootstrap(&server)?)?,
            enrollment: server.enrollment_advertise,
            fixture,
            step: 0,
            secrets: Vec::new(),
        })
    }
    fn pending(&self) -> Result<usize> {
        Ok(Config::load(&self.fixture.server)?
            .registry()?
            .pending()?
            .as_array()
            .context("PENDING_ARRAY")?
            .len())
    }
    fn review(&self, ui: &App) {
        ui.set_pair_bootstrap(self.public.clone().into());
        ui.set_pair_enrollment(self.enrollment.clone().into());
        ui.invoke_pair_action(12);
    }
    fn submit(&self, ui: &App, confirmed: bool) {
        ui.set_pair_confirmed(confirmed);
        ui.set_pair_token(self.secrets[0].as_str().into());
        ui.invoke_pair_action(13);
    }
    pub fn tick(&mut self, ui: &App, directory: &Path, checks: &mut Vec<String>) -> Result<bool> {
        match self.step {
            0 => {
                ui.set_main_page(1);
                ui.set_pairing_page(2);
                ui.set_server_config(
                    self.fixture
                        .orchestrator
                        .to_string_lossy()
                        .into_owned()
                        .into(),
                );
                ui.set_server_reset_confirmed(true);
                ui.invoke_reset_pairing_code();
                ui.invoke_reset_pairing_code();
            }
            1 => {
                ensure!(ui.get_server_code_visible(), "RESET_RESULT_MISSING");
                let code = Zeroizing::new(ui.get_server_code().to_string());
                pairing_code::validate(&code)?;
                let db = rusqlite::Connection::open(
                    pairing_code::Store::directory_for_config(&self.fixture.orchestrator)?
                        .join("verifier.sqlite"),
                )?;
                let generation: i64 =
                    db.query_row("SELECT generation FROM code", [], |r| r.get(0))?;
                ensure!(generation == 1, "DUPLICATE_RESET_NOT_BLOCKED");
                ensure!(
                    snapshot(ui, &directory.join("forbidden-code.bmp")).is_err(),
                    "CODE_SCREENSHOT_NOT_BLOCKED"
                );
                self.secrets.push(code);
                ui.invoke_dismiss_server_code();
                ui.set_pairing_page(0);
                ui.set_pair_config(self.fixture.client.to_string_lossy().into_owned().into());
                ui.invoke_pair_action(0);
            }
            2 => {
                ensure!(
                    ui.get_server_code().is_empty()
                        && !ui.get_server_code_visible()
                        && ui.get_pair_ready(),
                    "DISMISSED_CODE_REDISPLAYED"
                );
                self.review(ui);
            }
            3 => {
                ui.set_pair_enrollment("wss://127.0.0.1:1/tkfs/enroll".into());
                self.submit(ui, true);
            }
            4 => {
                ensure!(
                    ui.get_error() && self.pending()? == 0,
                    "CHANGED_ENDPOINT_ACCEPTED"
                );
                self.review(ui);
            }
            5 => {
                let mut public: Peer = serde_json::from_str(&self.public)?;
                public.installation = tkfs::core::id();
                ui.set_pair_bootstrap(serde_json::to_string(&public)?.into());
                self.submit(ui, true);
            }
            6 => {
                ensure!(
                    ui.get_error() && self.pending()? == 0,
                    "CHANGED_BOOTSTRAP_ACCEPTED"
                );
                self.review(ui);
            }
            7 => {
                ui.set_pair_config(
                    self.fixture
                        .root
                        .join("missing-client.toml")
                        .to_string_lossy()
                        .into_owned()
                        .into(),
                );
                self.submit(ui, true);
            }
            8 => {
                ensure!(
                    ui.get_error() && self.pending()? == 0,
                    "CHANGED_CONFIG_ACCEPTED"
                );
                ui.set_pair_config(self.fixture.client.to_string_lossy().into_owned().into());
                ui.set_pair_bootstrap(serde_json::to_string(&self.fixture.wrong_bootstrap)?.into());
                ui.set_pair_enrollment(self.enrollment.clone().into());
                ui.invoke_pair_action(12);
            }
            9 => {
                ensure!(!ui.get_pair_review().is_empty(), "WRONG_PIN_REVIEW_MISSING");
                self.submit(ui, true);
            }
            10 => {
                ensure!(
                    ui.get_error() && self.pending()? == 0,
                    "WRONG_TLS_PIN_ACCEPTED"
                );
                self.review(ui);
            }
            11 => {
                self.submit(ui, false);
            }
            12 => {
                ensure!(
                    ui.get_error() && self.pending()? == 0,
                    "UNCONFIRMED_CODE_SENT"
                );
                self.submit(ui, true);
                ui.invoke_pair_action(13);
            }
            13 => {
                ensure!(
                    !ui.get_error() && self.pending()? == 1 && ui.get_pair_peers().row_count() == 1,
                    "CODE_ENROLLMENT_CALLBACK_FAILED"
                );
                ensure!(ui.get_pair_token().is_empty(), "SUBMITTED_CODE_RETAINED");
                ui.invoke_pair_action(0);
            }
            14 => {
                ensure!(
                    ui.get_server_code().is_empty() && !ui.get_server_code_visible(),
                    "REFRESH_REDISPLAYED_CODE"
                );
                let server = Config::load(&self.fixture.server)?;
                let mut registry = server.registry()?;
                let pending = registry.pending()?;
                registry.approve(pending[0]["invitation"].as_str().context("CANDIDATE_ID")?)?;
                ui.invoke_pair_action(4);
            }
            15 => {
                ensure!(
                    !ui.get_error() && ui.get_pair_repos().row_count() == 0,
                    "APPROVED_EMPTY_DISCOVERY_FAILED"
                );
                snapshot(ui, &directory.join("code-enrolled-no-secret.bmp"))?;
                ui.set_pairing_page(2);
                ui.set_server_reset_confirmed(true);
                ui.invoke_reset_pairing_code();
            }
            16 => {
                ensure!(
                    ui.get_server_code_visible()
                        && ui.get_server_code().as_str() != self.secrets[0].as_str(),
                    "EXPLICIT_ROTATION_FAILED"
                );
                self.secrets
                    .push(Zeroizing::new(ui.get_server_code().to_string()));
                ui.invoke_dismiss_server_code();
                let server = Config::load(&self.fixture.server)?;
                ensure!(
                    server.registry()?.peers()?.len() == 1,
                    "ROTATION_REMOVED_PAIR"
                );
                // A real committed reset result is queued before close, then delivered
                // after close on the UI thread. The same production gate must drop it.
                let late = pairing_code::Store::for_config(&self.fixture.orchestrator)?.reset()?;
                self.secrets.push(Zeroizing::new(late.expose().to_owned()));
                let view = View {
                    show_once_code: Some(late),
                    ..View::default()
                };
                let lifecycle = UiLifetime::default();
                close_ui(ui, &lifecycle);
                lifecycle.deliver(|| apply(ui, view));
                ensure!(
                    ui.get_server_code().is_empty() && !ui.get_server_code_visible(),
                    "LATE_RESET_REPOPULATED_CLOSED_UI"
                );
                // Restore only test working state; no production window is reopened.
                ui.set_working(false);
            }
            17 => {
                std::fs::write(directory.join("stop-fixture-service"), b"ready")?;
                if !directory.join("fixture-service-stopped").exists() {
                    return Ok(false);
                }
                snapshot(ui, &directory.join("after-dismiss-and-late-result.bmp"))?;
                scan(&self.fixture.root, &self.secrets)?;
                ensure!(
                    !directory.join("forbidden-code.bmp").exists(),
                    "SECRET_SCREENSHOT_CREATED"
                );
                checks.push("Actual reset and WSS enrollment callbacks passed; repeated reset/enrollment suppressed; changed bootstrap, endpoint, config and wrong pin rejected before trust".into());
                checks.push("Code dismissal and client reload do not redisplay; late committed reset result discarded after close; existing pair survives rotation".into());
                checks.push("All disposable fixture files scanned for code bytes (UTF-8 and UTF-16); no secret in config, verifier store, journals, logs or evidence; secret screenshot blocked".into());
                return Ok(true);
            }
            _ => unreachable!(),
        }
        self.step += 1;
        Ok(false)
    }
}
fn scan(path: &Path, secrets: &[Zeroizing<String>]) -> Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            scan(&entry.path(), secrets)?;
        } else {
            let bytes = std::fs::read(entry.path())
                .with_context(|| format!("SCAN_FIXTURE_FILE: {}", entry.path().display()))?;
            for secret in secrets {
                ensure!(
                    !bytes.windows(secret.len()).any(|b| b == secret.as_bytes()),
                    "CODE_LEAK_IN_FIXTURE_FILE"
                );
                let wide = Zeroizing::new(
                    secret
                        .encode_utf16()
                        .flat_map(u16::to_le_bytes)
                        .collect::<Vec<_>>(),
                );
                ensure!(
                    !bytes.windows(wide.len()).any(|b| b == wide.as_slice()),
                    "WIDE_CODE_LEAK_IN_FIXTURE_FILE"
                );
            }
        }
    }
    Ok(())
}
