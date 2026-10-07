//! Explicit disposable gui-test mode. The server-owner actions below belong to
//! the fixture only; production desktop code never approves a remote device.
use super::{App, snapshot};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use slint::Model;
use std::path::{Path, PathBuf};
use tkfs::{pairing::SyncConfiguration, pairing_service::Config, runtime::Discovery};
use zeroize::Zeroize;

#[derive(Deserialize)]
struct Fixture {
    token: String,
    endpoint: String,
    client_config: PathBuf,
    server_config: PathBuf,
    client_runtime: PathBuf,
    server_runtime: PathBuf,
    wrong_runtime: PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.token.zeroize();
    }
}
pub(super) struct Acceptance {
    step: usize,
    fixture: Fixture,
}
impl Acceptance {
    pub(super) fn from_stdin() -> Result<Self> {
        use std::io::Read;
        let mut bytes = zeroize::Zeroizing::new(Vec::new());
        std::io::stdin().take(131073).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 131072, "GUI_PAIRING_FIXTURE_TOO_LARGE");
        Ok(Self {
            step: 0,
            fixture: serde_json::from_slice(&bytes)?,
        })
    }
    pub(super) fn tick(
        &mut self,
        ui: &App,
        directory: &Path,
        checks: &mut Vec<String>,
    ) -> Result<bool> {
        let f = &self.fixture;
        match self.step {
            0 => {
                ui.set_main_page(1);
                ui.set_pair_config(f.client_config.to_string_lossy().into_owned().into());
                ui.set_pair_endpoint(f.endpoint.clone().into());
                ui.invoke_pair_action(0);
            }
            1 => {
                ensure!(ui.get_pair_ready(), "GUI_PROTECTED_CLIENT_UNAVAILABLE");
                snapshot(ui, &directory.join("pairing-identity.bmp"))?;
                ui.set_pair_endpoint("wss://127.0.0.1:1/tkfs/sync".into());
                ui.set_pair_token(f.token.clone().into());
                ui.invoke_pair_action(2);
                ensure!(
                    ui.get_pair_token().is_empty(),
                    "GUI_INVITATION_INPUT_NOT_CLEARED"
                );
            }
            2 => {
                ensure!(
                    ui.get_error() && ui.get_pair_review().is_empty(),
                    "GUI_ENDPOINT_MISMATCH_NOT_REFUSED"
                );
                ensure!(
                    !ui.get_message().contains(&f.token),
                    "GUI_SECRET_ERROR_LEAK"
                );
                checks.push("protected credential reference displayed; mismatched endpoint refused and secret input cleared".into());
                ui.set_pair_endpoint(f.endpoint.clone().into());
                ui.set_pair_token(f.token.clone().into());
                ui.invoke_pair_action(2);
            }
            3 => {
                ensure!(
                    !ui.get_error() && !ui.get_pair_review().is_empty(),
                    "GUI_INVITATION_REVIEW_FAILED"
                );
                snapshot(ui, &directory.join("pairing-review.bmp"))?;
                ui.set_pair_confirmed(false);
                ui.invoke_pair_action(3);
            }
            4 => {
                ensure!(
                    ui.get_error()
                        && Config::load(&f.server_config)?
                            .registry()?
                            .pending()?
                            .as_array()
                            .unwrap()
                            .is_empty(),
                    "GUI_UNCONFIRMED_ENROLLMENT_SENT"
                );
                checks.push("exact server key review is mandatory; unconfirmed enrollment causes no remote candidate".into());
                ui.set_pair_confirmed(true);
                ui.invoke_pair_action(3);
                ui.invoke_pair_action(3);
            }
            5 => {
                ensure!(
                    !ui.get_error() && ui.get_pair_peers().row_count() == 1,
                    "GUI_ENROLLMENT_FAILED"
                );
                checks.push("enrollment over pinned WSS succeeds once; server-owner approval remains separate".into());
                ui.invoke_pair_action(4);
            }
            6 => {
                ensure!(
                    ui.get_error() && ui.get_pair_repos().row_count() == 0,
                    "GUI_REMOTE_APPROVAL_BYPASSED"
                );
                let server = Config::load(&f.server_config)?;
                let client = Config::load(&f.client_config)?;
                let mut registry = server.registry()?;
                let pending = registry.pending()?;
                let candidate = pending
                    .as_array()
                    .unwrap()
                    .first()
                    .context("GUI_FIXTURE_CANDIDATE_MISSING")?;
                ensure!(
                    candidate["installation"] == client.installation
                        && candidate["fingerprint"] == client.identity()?.fingerprint(),
                    "GUI_FIXTURE_APPROVAL_KEY_MISMATCH"
                );
                registry.approve(candidate["invitation"].as_str().unwrap())?;
                let local: Discovery = serde_json::from_slice(&std::fs::read(&f.server_runtime)?)?;
                let remote: Discovery = serde_json::from_slice(&std::fs::read(&f.client_runtime)?)?;
                registry.configure_sync(&SyncConfiguration {
                    peer: client.installation,
                    repo: local.repo,
                    local_runtime: f.server_runtime.clone(),
                    local_replica: local.device,
                    remote_replica: remote.device,
                    enabled: true,
                })?;
                drop(registry);
                ui.invoke_pair_action(4);
            }
            7 => {
                ensure!(
                    !ui.get_error() && ui.get_pair_repos().row_count() == 1,
                    "GUI_PUBLISHED_LIST_FAILED"
                );
                ensure!(
                    !ui.get_pair_repos()
                        .row_data(0)
                        .unwrap()
                        .branches
                        .contains("fixture-private"),
                    "GUI_PRIVATE_BRANCH_EXPOSED"
                );
                checks.push("approval check initially refused; exact local owner approval/grant exposes only published branches".into());
                ui.set_pair_runtime(f.wrong_runtime.to_string_lossy().into_owned().into());
                ui.set_pair_grant_confirmed(true);
                ui.invoke_pair_action(5);
            }
            8 => {
                ensure!(
                    ui.get_error() && ui.get_pair_grants().row_count() == 0,
                    "GUI_WRONG_REPLICA_GRANTED"
                );
                ui.set_pair_runtime(f.client_runtime.to_string_lossy().into_owned().into());
                ui.set_pair_grant_confirmed(true);
                ui.invoke_pair_action(5);
            }
            9 => {
                ensure!(
                    !ui.get_error() && ui.get_pair_grants().row_count() == 1,
                    "GUI_MATCHING_REPLICA_NOT_GRANTED"
                );
                checks.push("wrong repository replica refused; matching running replica and mount verified before explicit bidirectional grant".into());
                ui.invoke_pair_grant_action(0, 0);
            }
            10 => {
                ensure!(!ui.get_error(), "GUI_SYNC_FAILED");
                if !ui.get_message().contains("No more work remains") {
                    ui.invoke_pair_grant_action(0, 0);
                    return Ok(false);
                }
                checks.push(
                    "published data synchronized via owner-authenticated worker bridge".into(),
                );
                ui.invoke_pair_grant_action(0, 1);
            }
            11 => {
                ensure!(
                    !ui.get_error() && !ui.get_pair_grants().row_data(0).unwrap().enabled,
                    "GUI_PAUSE_FAILED"
                );
                ui.invoke_pair_action(0);
            }
            12 => {
                ensure!(
                    !ui.get_pair_grants().row_data(0).unwrap().enabled
                        && ui.get_pair_repos().row_count() == 0,
                    "GUI_PAUSE_RELOAD_FAILED"
                );
                ui.invoke_pair_grant_action(0, 2);
            }
            13 => {
                ensure!(
                    !ui.get_error() && ui.get_pair_grants().row_data(0).unwrap().enabled,
                    "GUI_RESUME_FAILED"
                );
                checks.push("paused grant survives config reload; configured sync resumes without an online repository listing".into());
                ui.invoke_pair_grant_action(0, 0);
                ui.invoke_pair_cancel();
            }
            14 => {
                ensure!(
                    ui.get_error() && ui.get_message().contains("Stopped local"),
                    "GUI_CANCEL_NOT_OBSERVED"
                );
                checks.push("network cancellation signals the background operation; committed data is retained for resume".into());
                ui.invoke_pair_grant_action(0, 0);
            }
            15 => {
                ensure!(!ui.get_error(), "GUI_SYNC_RESUME_FAILED");
                if !ui.get_message().contains("No more work remains") {
                    ui.invoke_pair_grant_action(0, 0);
                    return Ok(false);
                }
                ui.set_pair_revoke_confirmed(true);
                ui.invoke_pair_action(9);
            }
            16 => {
                ensure!(
                    !ui.get_error() && ui.get_pair_peers().row_data(0).unwrap().revoked,
                    "GUI_REVOCATION_FAILED"
                );
                ui.invoke_pair_action(0);
            }
            17 => {
                ensure!(
                    ui.get_pair_grants().row_data(0).unwrap().revoked,
                    "GUI_REVOCATION_NOT_DURABLE"
                );
                ui.invoke_pair_grant_action(0, 2);
            }
            _ => {
                ensure!(
                    ui.get_error() && ui.get_pair_grants().row_data(0).unwrap().revoked,
                    "GUI_REVOKED_PAIR_RESUMED"
                );
                ensure!(
                    ui.get_pair_token().is_empty() && !ui.get_message().contains(&f.token),
                    "GUI_SECRET_PERSISTENCE_LEAK"
                );
                snapshot(ui, &directory.join("pairing-revoked.bmp"))?;
                checks.push("revocation survives reload and refuses resume; no invitation secret remains in input or errors".into());
                return Ok(true);
            }
        }
        self.step += 1;
        Ok(false)
    }
}
