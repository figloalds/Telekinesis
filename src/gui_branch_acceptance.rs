//! Explicit disposable gui-test mode only. Production RPC stays on Controller's worker.
use super::{App, Paths, Session, snapshot};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use slint::{ComponentHandle, Model};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Default)]
pub(super) struct BranchAcceptance {
    step: usize,
    busy: Option<fs::File>,
    original: String,
    pending: Option<Value>,
    resume: bool,
    close_pending: bool,
}
fn select_branch(ui: &App, label: &str) -> Result<()> {
    let index = ui
        .get_branches()
        .iter()
        .position(|row| row.label == label)
        .context("TEST_BRANCH_NOT_FOUND")?;
    ui.set_selected_branch(index as i32);
    Ok(())
}
fn mount(ui: &App) -> Result<PathBuf> {
    Ok(PathBuf::from(
        ui.get_projects()
            .row_data(ui.get_selected() as usize)
            .context("TEST_PROJECT_NOT_SELECTED")?
            .mount
            .as_str(),
    ))
}
fn client() -> Result<Session> {
    Session::connect(Paths::from_executable(std::env::current_exe()?)?)
}
fn journal() -> Result<Value> {
    let paths = Paths::from_executable(std::env::current_exe()?)?;
    Ok(serde_json::from_slice(&fs::read(
        paths.folder.join(".tkfs-ui-branch-operation.json"),
    )?)?)
}
fn submit(ui: &App, kind: i32, input: &str) {
    ui.invoke_begin_branch_action(kind);
    ui.set_branch_input(input.into());
    ui.invoke_submit_branch();
}
impl BranchAcceptance {
    pub(super) fn set_phase(&mut self, phase: &str) {
        self.resume = phase == "branches-resume";
        self.close_pending = phase == "branches-pending";
    }
    pub(super) fn tick(
        &mut self,
        ui: &App,
        directory: &Path,
        checks: &mut Vec<String>,
    ) -> Result<bool> {
        match self.step {
            0 => {
                ensure!(
                    ui.get_connected() && ui.get_projects().row_count() == 2,
                    "BRANCH_TEST_PROJECTS_NOT_READY"
                );
                let index = ui
                    .get_projects()
                    .iter()
                    .position(|row| {
                        row.label
                            == if self.resume {
                                "Research lab"
                            } else {
                                "Design studio"
                            }
                    })
                    .context("TEST_DESIGN_PROJECT_MISSING")?;
                ui.set_details_page(1);
                ui.invoke_select_project(index as i32);
            }
            1 => {
                if self.resume {
                    let saved = journal()?;
                    ensure!(
                        ui.get_can_retry_branch()
                            && ui.get_branches().row_count() == 1
                            && ui.get_current_branch_name() == "main",
                        "PENDING_BRANCH_NOT_RESTORED_ON_OTHER_PROJECT"
                    );
                    self.original = saved["context"]["active"]
                        .as_str()
                        .context("SAVED_ACTIVE_MISSING")?
                        .into();
                    self.pending = Some(saved);
                    ui.invoke_retry_branch();
                    self.step = 4;
                    checks.push("GUI reopens with pending branch request on another project; retry remains bound to its original project/UUID/payload".into());
                    return Ok(false);
                }
                ensure!(
                    ui.get_runtime_ready()
                        && ui.get_branches().row_count() == 1
                        && ui.get_current_branch_shared(),
                    "INITIAL_SHARED_MAIN_NOT_VISIBLE"
                );
                self.original = ui.get_current_branch_id().to_string();
                ui.invoke_begin_branch_action(1);
                ui.set_branch_input("Cancelled branch".into());
                snapshot(ui, &directory.join("private-branch-dialog.bmp"))?;
                ui.invoke_cancel_branch_dialog();
                ensure!(ui.get_branch_dialog() == 0, "CANCEL_DID_NOT_DISMISS");
                let paths = Paths::from_executable(std::env::current_exe()?)?;
                ensure!(
                    !paths.folder.join(".tkfs-ui-branch-operation.json").exists(),
                    "CANCEL_DELIVERED_A_REQUEST"
                );
                checks.push(
                    "cancel-before-submit leaves branch journal and mounted state unchanged".into(),
                );
                submit(ui, 1, "Private work");
                ui.invoke_submit_branch();
            }
            2 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_branches().row_count() == 2
                        && ui.get_current_branch_id() == self.original,
                    "PRIVATE_BRANCH_CREATE_OR_CLICK_GUARD_FAILED: {}",
                    ui.get_message()
                );
                checks.push("private branch created once despite repeated submit; current main remains selected".into());
                select_branch(ui, "Private work")?;
                ensure!(ui.get_can_checkout(), "SWITCH_CONTROL_NOT_ENABLED");
                ui.invoke_begin_branch_action(2);
                self.busy = Some(fs::File::open(mount(ui)?.join("hello.txt"))?);
                ui.invoke_submit_branch();
            }
            3 => {
                ensure!(
                    ui.get_error()
                        && ui.get_can_retry_branch()
                        && ui.get_current_branch_id() == self.original,
                    "BUSY_CHECKOUT_NOT_REFUSED: {}",
                    ui.get_message()
                );
                ensure!(
                    !ui.get_can_branch_actions(),
                    "PENDING_BRANCH_REQUEST_MUST_BLOCK_NEW_ACTIONS"
                );
                self.pending = Some(journal()?);
                snapshot(ui, &directory.join("busy-checkout.bmp"))?;
                self.busy = None;
                if self.close_pending {
                    checks.push("busy checkout is durably pending; GUI closes without stopping either worker or issuing another branch action".into());
                    return Ok(true);
                }
                ui.invoke_retry_branch();
            }
            4 => {
                ensure!(
                    !ui.get_error()
                        && !ui.get_current_branch_shared()
                        && ui.get_current_branch_name() == "Private work",
                    "BUSY_CHECKOUT_RETRY_FAILED: {}",
                    ui.get_message()
                );
                let after = journal()?;
                let before = self.pending.as_ref().unwrap();
                ensure!(
                    after["request"] == before["request"]
                        && after["payload"] == before["payload"]
                        && after["context"] == before["context"],
                    "BRANCH_RETRY_CHANGED_REQUEST"
                );
                checks.push("open native file handle refuses checkout; exact UUID/payload/context retry switches only after release".into());
                let root = mount(ui)?;
                ensure!(
                    fs::read(root.join("hello.txt"))? == b"UI acceptance durable data",
                    "CHECKOUT_CHANGED_LITERAL_DATA"
                );
                fs::write(root.join("private-only.txt"), b"PRIVATE-VISIBLE")?;
                fs::write(
                    root.join("history-canary.txt"),
                    b"never-published historical bytes",
                )?;
                fs::remove_file(root.join("history-canary.txt"))?;
                ui.invoke_refresh();
            }
            5 => {
                ui.invoke_begin_branch_action(4);
                ui.set_branch_input("Private checkpoint".into());
                snapshot(ui, &directory.join("checkpoint-dialog.bmp"))?;
                ui.invoke_submit_branch();
            }
            6 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_checkpoints().row_count() == 1
                        && ui.get_selected_checkpoint() == 0,
                    "CHECKPOINT_NOT_DISPLAYED: {}",
                    ui.get_message()
                );
                ui.invoke_begin_branch_action(5);
                ui.set_branch_input("Restored private".into());
                snapshot(ui, &directory.join("restore-dialog.bmp"))?;
                ui.invoke_submit_branch();
            }
            7 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_branches().row_count() == 3
                        && ui.get_current_branch_name() == "Private work",
                    "RESTORE_CHANGED_ACTIVE_BRANCH: {}",
                    ui.get_message()
                );
                checks.push("checkpoint lists its private source; restore creates a new private branch without switching or replacing current files".into());
                ui.invoke_begin_branch_action(1);
                ui.set_branch_input("Stale target".into());
                // Another client switches while the dialog holds its original context.
                let session = client()?;
                session.runtime(ui.get_branch_source_project().as_str(),json!({"op":"checkout","name":self.original,"generation":ui.get_branch_source_generation().parse::<u64>()?}))?;
                ui.invoke_submit_branch();
            }
            8 => {
                ensure!(
                    ui.get_error()
                        && !ui.get_can_retry_branch()
                        && ui.get_message().contains("changed"),
                    "STALE_VIEW_ACTION_WAS_NOT_REJECTED: {}",
                    ui.get_message()
                );
                snapshot(ui, &directory.join("stale-view.bmp"))?;
                ui.invoke_refresh();
            }
            9 => {
                ensure!(
                    ui.get_current_branch_id() == self.original
                        && ui.get_branches().row_count() == 3,
                    "STALE_ACTION_CHANGED_PROJECT"
                );
                let index = ui
                    .get_projects()
                    .iter()
                    .position(|row| row.label == "Research lab")
                    .context("TEST_RESEARCH_PROJECT_MISSING")?;
                ui.invoke_select_project(index as i32);
            }
            10 => {
                ensure!(
                    ui.get_branches().row_count() == 1
                        && ui.get_checkpoints().row_count() == 0
                        && ui.get_current_branch_name() == "main",
                    "PROJECT_CHANGE_RETAINED_WRONG_BRANCH_ROWS"
                );
                let index = ui
                    .get_projects()
                    .iter()
                    .position(|row| row.label == "Design studio")
                    .unwrap();
                ui.invoke_select_project(index as i32);
            }
            11 => {
                ensure!(
                    ui.get_branches().row_count() == 3 && ui.get_selected_checkpoint() == -1,
                    "PROJECT_CHANGE_RETAINED_SNAPSHOT_SELECTION"
                );
                checks.push("another-client checkout rejects stale dialog; changing projects clears branch/checkpoint selection and keeps stores independent".into());
                select_branch(ui, "Private work")?;
                submit(ui, 2, "");
            }
            12 => {
                ensure!(
                    !ui.get_error() && ui.get_current_branch_name() == "Private work",
                    "RETURN_TO_PRIVATE_FAILED"
                );
                ui.invoke_begin_branch_action(3);
                ui.set_branch_input("Published visible".into());
                ensure!(
                    !ui.get_can_submit_branch(),
                    "PUBLICATION_ENABLED_WITHOUT_CONFIRMATION"
                );
                let before = journal()?;
                ui.invoke_submit_branch();
                ensure!(
                    !ui.get_working() && ui.get_branch_dialog() == 3 && journal()? == before,
                    "UNCONFIRMED_PUBLICATION_DELIVERED"
                );
                snapshot(ui, &directory.join("publication-confirmation.bmp"))?;
                ui.invoke_cancel_branch_dialog();
                ui.invoke_begin_branch_action(3);
                ui.set_branch_input("Published visible".into());
                ui.set_publication_confirmed(true);
                ui.invoke_submit_branch();
            }
            13 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_branches().row_count() == 4
                        && ui.get_current_branch_name() == "Private work",
                    "PUBLICATION_CHANGED_SOURCE_BRANCH: {}",
                    ui.get_message()
                );
                let session = client()?;
                let state = ui
                    .get_projects()
                    .row_data(ui.get_selected() as usize)
                    .unwrap()
                    .id;
                let events = session.runtime(state.as_str(), json!({"op":"events"}))?;
                let events = events.as_array().context("TEST_EVENTS_INVALID")?;
                let private = serde_json::to_string(
                    &events
                        .iter()
                        .filter(|event| event["branch"]["shared"] != true)
                        .collect::<Vec<_>>(),
                )?;
                let shared = serde_json::to_string(
                    &events
                        .iter()
                        .filter(|event| event["branch"]["shared"] == true)
                        .collect::<Vec<_>>(),
                )?;
                let canary_hash = tkfs::core::hash(b"never-published historical bytes");
                ensure!(
                    private.contains("history-canary.txt") && private.contains(&canary_hash),
                    "PRIVATE_HISTORY_CANARY_WAS_NOT_RECORDED"
                );
                ensure!(
                    !shared.contains("history-canary.txt") && !shared.contains(&canary_hash),
                    "PUBLICATION_LEAKED_PRIVATE_HISTORY"
                );
                snapshot(ui, &directory.join("branches-private-and-shared.bmp"))?;
                checks.push("publication requires checkbox confirmation; visible state creates a new shared branch while deleted private-history metadata/object references remain private".into());
                select_branch(ui, "Restored private")?;
                submit(ui, 2, "");
            }
            14 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_current_branch_name() == "Restored private"
                        && !ui.get_current_branch_shared(),
                    "RESTORED_BRANCH_SWITCH_FAILED"
                );
                ensure!(
                    fs::read(mount(ui)?.join("private-only.txt"))? == b"PRIVATE-VISIBLE",
                    "RESTORED_SNAPSHOT_LITERAL_DATA_CHANGED"
                );
                ensure!(
                    !mount(ui)?.join("history-canary.txt").exists(),
                    "RESTORE_INCLUDED_DELETED_PRIVATE_FILE"
                );
                select_branch(ui, "Published visible")?;
                submit(ui, 2, "");
            }
            15 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_current_branch_name() == "Published visible"
                        && ui.get_current_branch_shared(),
                    "PUBLISHED_BRANCH_SWITCH_FAILED"
                );
                ensure!(
                    fs::read(mount(ui)?.join("private-only.txt"))? == b"PRIVATE-VISIBLE",
                    "PUBLISHED_LITERAL_DATA_CHANGED"
                );
                snapshot(ui, &directory.join("published-current-branch.bmp"))?;
                select_branch(ui, "main")?;
                submit(ui, 2, "");
            }
            16 => {
                ensure!(
                    !ui.get_error()
                        && ui.get_current_branch_id() == self.original
                        && ui.get_current_branch_shared(),
                    "HASH_IDENTIFIED_MAIN_REOPEN_FAILED"
                );
                ensure!(
                    !mount(ui)?.join("private-only.txt").exists(),
                    "PRIVATE_FILES_CONTAMINATED_ORIGINAL_MAIN"
                );
                checks.push("restored and published mounted cuts retain exact visible bytes; switching back to hash-identified main restores its isolated view".into());
                ui.window().set_size(slint::LogicalSize::new(980.0, 720.0));
            }
            _ => {
                snapshot(ui, &directory.join("branches-minimum-size.bmp"))?;
                return Ok(true);
            }
        }
        self.step += 1;
        Ok(false)
    }
}
