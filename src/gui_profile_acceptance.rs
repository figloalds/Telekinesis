use super::{App, snapshot};
use anyhow::{Result, ensure};
use std::path::Path;
pub(super) fn tick(
    ui: &App,
    directory: &Path,
    step: &mut i32,
    reopen: bool,
    checks: &mut Vec<String>,
) -> Result<bool> {
    match *step {
        0 => {
            ensure!(ui.get_first_run(), "PROFILE_SILENTLY_SELECTED");
            ensure!(ui.get_profile_exists() == reopen, "PROFILE_DISCOVERY_WRONG");
            ensure!(!ui.get_profile_setup(), "PROFILE_DEFAULT_RELOCATES");
            snapshot(ui, &directory.join("profile-choice.bmp"))?;
            if reopen {
                ui.invoke_open_profile();
            } else {
                ui.set_profile_setup(true);
                ui.invoke_setup();
            }
        }
        1 => {
            ensure!(
                ui.get_connected() && !ui.get_error(),
                "PROFILE_CONNECT_FAILED: {}",
                ui.get_message()
            );
            ensure!(
                std::fs::canonicalize(ui.get_config_path().as_str())?
                    == std::fs::canonicalize(ui.get_profile_config().as_str())?,
                "PROFILE_WRONG_CONFIG"
            );
            snapshot(ui, &directory.join("profile-connected.bmp"))?;
            ui.invoke_shutdown();
        }
        2 => {
            ensure!(
                !ui.get_connected() && !ui.get_error(),
                "PROFILE_SHUTDOWN_FAILED"
            );
            checks.push(if reopen { "Normal executable relaunch offers explicit existing-profile action and reconnects selected profile without relocating portable default" } else { "Explicit profile first-run action saves selected paths; portable default remains unchanged" }.into());
            return Ok(true);
        }
        _ => unreachable!(),
    }
    *step += 1;
    Ok(false)
}
