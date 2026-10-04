//! Slint view/controller. All management, startup and runtime I/O is serialized off the UI thread.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use slint::{ComponentHandle, ModelRc, VecModel};
use std::{
    path::{Path, PathBuf},
    sync::mpsc,
    time::Duration,
};
use tkfs::{
    desktop::{self, Paths, Session},
    desktop_branch::{BranchAction, BranchClient, BranchContext},
    orchestrator::Action,
};
slint::include_modules!();
#[path = "gui_branch_acceptance.rs"]
mod branch_acceptance;

enum Job {
    Setup(PathBuf, PathBuf, usize),
    Connect,
    Refresh,
    Select(String),
    Create(String, Option<PathBuf>),
    Start,
    Stop,
    Shutdown,
    Retry,
    Operation,
    Browse(i32),
    Open,
    Branch(BranchContext, BranchAction),
    RetryBranch,
}
#[derive(Default)]
struct View {
    connected: bool,
    first_run: bool,
    connection: String,
    rows: Vec<(String, String, String, String, bool)>,
    selected: i32,
    status: String,
    branches: String,
    branch_rows: Vec<BranchItem>,
    checkpoint_rows: Vec<CheckpointItem>,
    runtime: Option<BranchContext>,
    current_branch_name: String,
    current_branch_shared: bool,
    branch_operation: String,
    retry_branch: bool,
    preferred_branch: Option<String>,
    preferred_checkpoint: Option<String>,
    message: Option<(String, bool)>,
    operation: String,
    retry: bool,
    folder: Option<(i32, String)>,
}
struct Controller {
    paths: Paths,
    session: Option<Session>,
    selected: Option<String>,
    catalog: Value,
    branch_client: Option<BranchClient>,
}
fn text(value: &Value, key: &str) -> String {
    value[key].as_str().unwrap_or_default().to_owned()
}
fn display_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    if let Some(unc) = text.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{unc}")
    } else {
        text.strip_prefix(r"\\?\").unwrap_or(&text).to_owned()
    }
}
fn project_status(state: &Value) -> &'static str {
    match state["observation"]["status"].as_str() {
        Some("running") => "Running",
        Some("stopped") => "Stopped",
        Some("unavailable") => "Unavailable",
        Some("degraded") => "Needs attention",
        Some("stop-pending") => "Stopping · files in use",
        _ => "Recovering",
    }
}
fn describe_error(message: &str) -> String {
    if message.contains("PENDING_SAVE") {
        "Unsaved data is retained. Saving is retried automatically; switching or stopping stays blocked until it succeeds. Check the storage error if this continues.".into()
    } else if message.contains("BUSY_VIEW") {
        "This project is in use. Close files, editors, and Explorer windows using it, then retry the same request. Your data is preserved.".into()
    } else if message.contains("STALE_MANAGEMENT_GENERATION")
        || message.contains("STALE_VIEW")
        || message.contains("STALE_PROJECT_SELECTION")
    {
        "The project changed in another client. Refresh, review its state, and try the action again.".into()
    } else if message.contains("OPERATION_PENDING") || message.contains("STATE_OPERATION_PENDING") {
        "An earlier operation is still pending. Inspect it and retry its exact request before starting another action.".into()
    } else if message.contains("WORKER_START") {
        "The project worker could not become ready. Check the project's stderr log in the data folder; verify WinFsp and the mount path, then retry.".into()
    } else {
        message.into()
    }
}
fn read_catalog(session: &Session) -> Result<Value> {
    session.call(Action::List).or_else(|error| {
        if !format!("{error:#}").contains("LOCAL_IPC") {
            return Err(error);
        }
        std::thread::sleep(Duration::from_millis(100));
        session.call(Action::List)
    })
}
fn connection_error(view: &mut View, has_session: bool, refresh: bool, error: &str) {
    if (refresh || error.contains("LOCAL_IPC")) && !has_session {
        view.connected = false;
        view.connection = "Disconnected · existing data preserved".into();
        for row in &mut view.rows {
            row.2 = "Unknown · disconnected".into();
        }
        view.status = "The supervisor is unavailable. Reconnect to refresh project status; existing data is preserved.".into();
    } else if error.contains("LOCAL_IPC") {
        view.connection = "Connection interrupted · status may be stale · refresh to retry".into();
    }
}
impl Controller {
    fn execute(&mut self, job: Job) -> Result<View> {
        match job {
            Job::Setup(data, mounts, limit) => {
                desktop::bootstrap(&self.paths, &data, &mounts, limit)?;
                self.session = Some(Session::connect(self.paths.clone())?);
                self.branch_client = Some(BranchClient::open(self.session.as_ref().unwrap())?);
                self.refresh(Some((
                    "Configuration saved. You're ready to create a project.".into(),
                    false,
                )))
            }
            Job::Connect => {
                self.session = Some(Session::connect(self.paths.clone())?);
                self.branch_client = Some(BranchClient::open(self.session.as_ref().unwrap())?);
                self.refresh(None)
            }
            Job::Refresh => self.refresh(None),
            Job::Select(state) => {
                self.selected = Some(state);
                self.refresh(None)
            }
            Job::Browse(kind) => {
                let folder = desktop::pick_folder()?;
                Ok(View {
                    folder: folder.map(|p| (kind, p.to_string_lossy().into_owned())),
                    ..self.view()
                })
            }
            Job::Open => {
                let state = self.selected_state()?;
                desktop::open_folder(Path::new(
                    state["mount"].as_str().context("PROJECT_HAS_NO_MOUNT")?,
                ))?;
                Ok(self.view())
            }
            Job::Operation => {
                let result = self
                    .session
                    .as_mut()
                    .context("NOT_CONNECTED")?
                    .operation()?;
                let mut view = self.view();
                view.operation = serde_json::to_string_pretty(&result)?;
                if let Some(response) = result.get("response").filter(|r| !r.is_null()) {
                    view.retry = response["retryable"] == true;
                    view.message = Some((
                        if response["ok"] == true {
                            "Operation completed.".into()
                        } else {
                            describe_error(&text(response, "error"))
                        },
                        response["ok"] != true,
                    ));
                }
                Ok(view)
            }
            Job::Create(label, mount) => {
                let label = label.trim().to_owned();
                ensure!(!label.is_empty(), "PROJECT_NAME_REQUIRED");
                let session = self.session.as_ref().context("NOT_CONNECTED")?;
                let mount = match mount {
                    Some(p) => p,
                    None => {
                        let parent = session.config.default_mount_directory.as_ref().context(
                            "MOUNT_PATH_REQUIRED: this configuration has no default project parent",
                        )?;
                        ensure!(
                            !label.contains(['\\', '/', ':', '*', '?', '"', '<', '>', '|'])
                                && !label.ends_with(['.', ' '])
                                && label != "."
                                && label != "..",
                            "CHOOSE_A_VALID_WINDOWS_FOLDER_NAME_OR_EXPLICIT_MOUNT_PATH"
                        );
                        parent.join(&label)
                    }
                };
                let generation = session.call(Action::List)?["catalog_generation"]
                    .as_u64()
                    .context("CATALOG_GENERATION_MISSING")?;
                self.mutation(
                    Action::Create {
                        label,
                        mount: Some(mount),
                    },
                    generation,
                )
            }
            Job::Start | Job::Stop => {
                let state = self.selected_state()?;
                let id = text(state, "state_id");
                let observed = self
                    .session
                    .as_ref()
                    .context("NOT_CONNECTED")?
                    .call(Action::Inspect { state: id.clone() })?;
                let generation = observed["management_generation"]
                    .as_u64()
                    .context("STATE_GENERATION_MISSING")?;
                self.mutation(
                    if matches!(job, Job::Start) {
                        Action::Start { state: id }
                    } else {
                        Action::Stop { state: id }
                    },
                    generation,
                )
            }
            Job::Shutdown => {
                let generation = self
                    .session
                    .as_ref()
                    .context("NOT_CONNECTED")?
                    .call(Action::List)?["catalog_generation"]
                    .as_u64()
                    .context("CATALOG_GENERATION_MISSING")?;
                self.mutation(Action::Shutdown, generation)
            }
            Job::Retry => {
                let reply = self.session.as_mut().context("NOT_CONNECTED")?.retry()?;
                self.after_mutation(reply)
            }
            Job::Branch(context, action) => {
                ensure!(
                    self.selected.as_deref() == Some(context.state.as_str()),
                    "STALE_PROJECT_SELECTION: review the selected project again"
                );
                let reply = self
                    .branch_client
                    .as_mut()
                    .context("BRANCH_CLIENT_NOT_READY")?
                    .mutate(
                        self.session.as_ref().context("NOT_CONNECTED")?,
                        context,
                        action,
                    )?;
                self.after_branch(reply)
            }
            Job::RetryBranch => {
                let reply = self
                    .branch_client
                    .as_mut()
                    .context("BRANCH_CLIENT_NOT_READY")?
                    .retry(self.session.as_ref().context("NOT_CONNECTED")?)?;
                self.after_branch(reply)
            }
        }
    }
    fn after_branch(&mut self, reply: Value) -> Result<View> {
        if let Some(saved) = self
            .branch_client
            .as_ref()
            .and_then(|client| client.saved.as_ref())
        {
            self.selected = Some(saved.context.state.clone());
        }
        let mut view = self.refresh(Some((
            if reply["ok"] == true {
                "Branch action completed. The displayed state is confirmed by the runtime.".into()
            } else {
                describe_error(&text(&reply, "error"))
            },
            reply["ok"] != true,
        )))?;
        if reply["ok"] == true {
            view.preferred_branch = reply["result"]["id"].as_str().map(str::to_owned);
            view.preferred_checkpoint = reply["result"]["checkpoint"].as_str().map(str::to_owned);
        }
        Ok(view)
    }
    fn selected_state(&self) -> Result<&Value> {
        self.catalog["states"]
            .as_array()
            .context("NO_CATALOG")?
            .iter()
            .find(|s| s["state_id"].as_str() == self.selected.as_deref())
            .context("SELECT_A_PROJECT")
    }
    fn mutation(&mut self, action: Action, generation: u64) -> Result<View> {
        let reply = self
            .session
            .as_mut()
            .context("NOT_CONNECTED")?
            .mutate(action, generation)?;
        if reply["ok"] == true && reply["result"]["state_id"].is_string() {
            self.selected = Some(text(&reply["result"], "state_id"));
        }
        self.after_mutation(reply)
    }
    fn after_mutation(&mut self, reply: Value) -> Result<View> {
        if reply["ok"] == true
            && self
                .session
                .as_ref()
                .and_then(|s| s.saved.as_ref())
                .is_some_and(|s| matches!(s.request.action, Action::Shutdown))
        {
            let mut view = self.view();
            view.connected = false;
            view.connection = "Supervisor stopped · projects will return on reconnect".into();
            for row in &mut view.rows {
                row.2 = "Stopped".into();
            }
            view.status =
                "Background workers stopped. Native data and history are preserved.".into();
            view.message = Some((
                "Background workers stopped. You can close this window or reconnect.".into(),
                false,
            ));
            return Ok(view);
        }
        self.refresh(Some((
            if reply["ok"] == true {
                "Operation completed.".into()
            } else {
                describe_error(&text(&reply, "error"))
            },
            reply["ok"] != true,
        )))
    }
    fn refresh(&mut self, message: Option<(String, bool)>) -> Result<View> {
        if let Some(client) = &mut self.branch_client {
            client.reload()?;
        }
        let session = self.session.as_ref().context("NOT_CONNECTED")?;
        self.catalog = read_catalog(session)?;
        let states = self.catalog["states"]
            .as_array()
            .context("CATALOG_STATES_MISSING")?;
        if !states
            .iter()
            .any(|s| s["state_id"].as_str() == self.selected.as_deref())
        {
            self.selected = states.first().map(|s| text(s, "state_id"));
        }
        let mut view = self.view();
        view.message = message;
        if let Some(state) = states
            .iter()
            .find(|s| s["state_id"].as_str() == self.selected.as_deref())
        {
            view.status = format!(
                "{}\n\nMount folder\n{}\n\nDesired state: {}",
                project_status(state),
                display_path(Path::new(
                    state["mount"].as_str().unwrap_or("Headless worker")
                )),
                if state["desired_running"] == true {
                    "Running"
                } else {
                    "Stopped"
                }
            );
            if matches!(
                project_status(state),
                "Running" | "Needs attention" | "Stopping · files in use"
            ) {
                match session.runtime(state["state_id"].as_str().unwrap(), json!({"op":"status"})) {
                    Ok(status) => {
                        if status["busy"] != true
                            && status["stale"] != true
                            && let Some(generation) = status["generation"].as_u64()
                            && let Some(active) = status["branch"]["id"].as_str()
                        {
                            view.runtime = Some(BranchContext {
                                state: text(state, "state_id"),
                                active: active.into(),
                                generation,
                            });
                            view.current_branch_name = text(&status["branch"], "name");
                            view.current_branch_shared = status["branch"]["shared"] == true;
                        }
                        if status["busy"] == true {
                            view.status.push_str("\n\nFilesystem busy · showing the last observed counts; saving continues.");
                        }
                        view.status.push_str(&format!("\n\nCurrent branch: {}\nStorage: {}\nOpen files and folders: {}\nFiles awaiting flush: {}\nUnresolved conflicts: {}",text(&status["branch"],"name"),if status["health"].is_null(){"Healthy"}else{"Needs attention"},status["open_handles"],status["unflushed_files"],status["unresolved_conflicts"]));
                        if !status["health"].is_null() {
                            view.status
                                .push_str(&format!("\nStorage health: {}", status["health"]));
                        }
                    }
                    Err(e) => view
                        .status
                        .push_str(&format!("\n\nRuntime unavailable: {e:#}")),
                }
                if let Ok(branches) = session.runtime(
                    state["state_id"].as_str().unwrap(),
                    json!({"op":"branches"}),
                ) {
                    view.branches = "Branches\n".into();
                    for branch in branches.as_array().context("INVALID_BRANCHES")? {
                        view.branch_rows.push(BranchItem {
                            id: text(branch, "id").into(),
                            label: text(branch, "name").into(),
                            shared: branch["shared"] == true,
                            current: view
                                .runtime
                                .as_ref()
                                .is_some_and(|context| context.active == text(branch, "id")),
                        });
                        view.branches.push_str(&format!(
                            "\n{} · {}",
                            text(branch, "name"),
                            if branch["shared"] == true {
                                "Shared branch, no peer configured"
                            } else {
                                "Private, local only"
                            }
                        ));
                    }
                }
                if let Ok(history) =
                    session.runtime(state["state_id"].as_str().unwrap(), json!({"op":"history"}))
                {
                    for checkpoint in history.as_array().context("INVALID_HISTORY")?.iter().rev() {
                        let branch = view.branch_rows.iter().find(|row| {
                            row.id.as_str() == checkpoint["branch"].as_str().unwrap_or_default()
                        });
                        let label = format!(
                            "{} · {} ({})",
                            text(checkpoint, "message"),
                            branch
                                .map(|row| row.label.as_str())
                                .unwrap_or("Unknown branch"),
                            if branch.is_some_and(|row| row.shared) {
                                "shared"
                            } else {
                                "private"
                            }
                        );
                        view.checkpoint_rows.push(CheckpointItem {
                            id: text(checkpoint, "id").into(),
                            label: label.into(),
                        });
                    }
                }
            } else if project_status(state) == "Unavailable" {
                view.status.push_str(&format!(
                    "\n\nDetails\n{}",
                    text(&state["observation"], "error")
                ));
            }
        }
        Ok(view)
    }
    fn view(&self) -> View {
        let mut view = View {
            first_run: !self.paths.config.exists(),
            connection: "Disconnected · reconnect to load projects".into(),
            selected: -1,
            ..View::default()
        };
        if let Some(session) = &self.session {
            view.connected = true;
            view.connection = if session.hello["shutdown_pending"] == true {
                "Connected · unfinished shutdown requires its exact retry".into()
            } else {
                "Connected · local supervisor".into()
            };
            if let Some(states) = self.catalog["states"].as_array() {
                for (i, state) in states.iter().enumerate() {
                    if state["state_id"].as_str() == self.selected.as_deref() {
                        view.selected = i as i32;
                    }
                    view.rows.push((
                        text(state, "label"),
                        text(state, "state_id"),
                        project_status(state).into(),
                        text(state, "mount"),
                        state["desired_running"] == true,
                    ));
                }
            }
            if let Some(saved) = &session.saved {
                view.operation = format!(
                    "Operation {}\n{}",
                    saved.request.operation_id.as_deref().unwrap_or(""),
                    if saved.uncertain {
                        "Delivery uncertain · inspect before retrying"
                    } else if saved
                        .response
                        .as_ref()
                        .is_some_and(|r| r["retryable"] == true)
                    {
                        "Pending · exact retry available"
                    } else {
                        "Completed receipt recorded"
                    }
                );
                view.retry = saved.uncertain
                    || saved
                        .response
                        .as_ref()
                        .is_some_and(|r| r["retryable"] == true);
                if view.retry {
                    view.message=Some(("An earlier operation needs attention. Inspect its result or retry the exact request.".into(),true));
                }
            }
        }
        if let Some(saved) = self
            .branch_client
            .as_ref()
            .and_then(|client| client.saved.as_ref())
        {
            let label = self.catalog["states"]
                .as_array()
                .and_then(|rows| {
                    rows.iter()
                        .find(|row| row["state_id"] == saved.context.state)
                })
                .map(|row| text(row, "label"))
                .unwrap_or_else(|| saved.context.state.clone());
            view.retry_branch = saved.pending();
            view.branch_operation = format!(
                "Branch action: {} · Project: {}\nRequest {} · {}",
                text(&saved.payload, "op"),
                label,
                saved.request,
                if saved.uncertain {
                    "Delivery uncertain · exact retry preserves the original project and payload"
                } else if saved.pending() {
                    "Busy or unavailable · exact retry available"
                } else {
                    "Runtime response recorded"
                }
            );
            if view.retry_branch {
                view.message = Some(("An earlier branch request needs attention. Retry its original request before starting another branch action.".into(),true));
            }
        }
        view
    }
}

fn apply(ui: &App, view: View) {
    use slint::Model;
    let old_project = ui
        .get_projects()
        .row_data(ui.get_selected() as usize)
        .map(|row| row.id);
    let new_project = view
        .rows
        .get(view.selected as usize)
        .map(|row| row.1.as_str());
    let same_project = old_project.as_ref().map(|id| id.as_str()) == new_project;
    let old_branch = if same_project {
        ui.get_branches()
            .row_data(ui.get_selected_branch() as usize)
            .map(|row| row.id)
    } else {
        None
    };
    let old_checkpoint = if same_project {
        ui.get_checkpoints()
            .row_data(ui.get_selected_checkpoint() as usize)
            .map(|row| row.id)
    } else {
        None
    };
    let selected_branch = view
        .branch_rows
        .iter()
        .position(|row| {
            Some(row.id.as_str())
                == view
                    .preferred_branch
                    .as_deref()
                    .or(old_branch.as_ref().map(|id| id.as_str()))
        })
        .or_else(|| view.branch_rows.iter().position(|row| row.current))
        .map(|i| i as i32)
        .unwrap_or(-1);
    let selected_checkpoint = view
        .checkpoint_rows
        .iter()
        .position(|row| {
            Some(row.id.as_str())
                == view
                    .preferred_checkpoint
                    .as_deref()
                    .or(old_checkpoint.as_ref().map(|id| id.as_str()))
        })
        .map(|i| i as i32)
        .unwrap_or(-1);
    ui.set_working(false);
    ui.set_first_run(view.first_run);
    ui.set_connected(view.connected);
    ui.set_connection(view.connection.into());
    ui.set_projects(ModelRc::new(VecModel::from(
        view.rows
            .into_iter()
            .map(|(label, id, status, mount, desired)| Project {
                label: label.into(),
                id: id.into(),
                status: status.into(),
                mount: mount.into(),
                desired,
            })
            .collect::<Vec<_>>(),
    )));
    ui.set_selected(view.selected);
    if !view.status.is_empty() {
        ui.set_status_detail(view.status.into());
    }
    ui.set_branches_detail(view.branches.into());
    ui.set_branches(ModelRc::new(VecModel::from(view.branch_rows)));
    ui.set_checkpoints(ModelRc::new(VecModel::from(view.checkpoint_rows)));
    ui.set_selected_branch(selected_branch);
    ui.set_selected_checkpoint(selected_checkpoint);
    ui.set_runtime_ready(view.runtime.is_some());
    ui.set_current_branch_name(view.current_branch_name.into());
    ui.set_current_branch_shared(view.current_branch_shared);
    ui.set_current_branch_id(
        view.runtime
            .as_ref()
            .map(|context| context.active.as_str())
            .unwrap_or_default()
            .into(),
    );
    ui.set_runtime_generation(
        view.runtime
            .as_ref()
            .map(|context| context.generation.to_string())
            .unwrap_or_default()
            .into(),
    );
    ui.set_branch_operation_detail(view.branch_operation.into());
    ui.set_can_retry_branch(view.retry_branch);
    if let Some((message, error)) = view.message {
        ui.set_message(message.into());
        ui.set_error(error);
    }
    ui.set_operation_detail(view.operation.into());
    ui.set_can_retry(view.retry);
    if let Some((kind, path)) = view.folder {
        match kind {
            0 => ui.set_data_folder(path.into()),
            1 => ui.set_mount_folder(path.into()),
            _ => ui.set_project_mount(
                Path::new(&path)
                    .join(ui.get_project_name().as_str())
                    .to_string_lossy()
                    .into_owned()
                    .into(),
            ),
        }
    }
}
fn enqueue(ui: &slint::Weak<App>, sender: &mpsc::Sender<Job>, job: Job) {
    if let Some(ui) = ui.upgrade() {
        if ui.get_working() {
            return;
        }
        ui.set_working(true);
        let _ = sender.send(job);
    }
}
pub fn run(report: Option<&Path>) -> Result<()> {
    slint::BackendSelector::new()
        .backend_name("winit".into())
        .renderer_name("software".into())
        .select()?;
    let paths = Paths::from_executable(std::env::current_exe()?)?;
    let ui = App::new()?;
    ui.set_config_path(display_path(&paths.config).into());
    ui.set_data_folder(display_path(&paths.folder.join("data")).into());
    ui.set_mount_folder(display_path(&paths.folder.join("Projects")).into());
    let first_run = !paths.config.try_exists()?;
    ui.set_first_run(first_run);
    ui.set_connection(
        if first_run {
            "Welcome · configuration required"
        } else {
            "Connecting to your installation…"
        }
        .into(),
    );
    let (sender, receiver) = mpsc::channel();
    let weak = ui.as_weak();
    let worker_weak = weak.clone();
    std::thread::spawn(move || {
        let mut controller = Controller {
            paths,
            session: None,
            selected: None,
            catalog: Value::Null,
            branch_client: None,
        };
        for job in receiver {
            let refresh = matches!(job, Job::Refresh | Job::Connect | Job::Setup(..));
            let result = controller.execute(job);
            let view = match result {
                Ok(view) => view,
                Err(e) => {
                    let mut view = controller.view();
                    connection_error(
                        &mut view,
                        controller.session.is_some(),
                        refresh,
                        &format!("{e:#}"),
                    );
                    view.message = Some((describe_error(&format!("{e:#}")), true));
                    view.operation.push_str(&format!("\nDetails: {e:#}"));
                    view
                }
            };
            if worker_weak
                .upgrade_in_event_loop(move |ui| apply(&ui, view))
                .is_err()
            {
                break;
            }
        }
    });
    macro_rules! callback {
        ($method:ident,$job:expr) => {{
            let weak = weak.clone();
            let sender = sender.clone();
            ui.$method(move || enqueue(&weak, &sender, $job));
        }};
    }
    callback!(on_connect, Job::Connect);
    callback!(on_refresh, Job::Refresh);
    callback!(on_start_project, Job::Start);
    callback!(on_stop_project, Job::Stop);
    callback!(on_shutdown, Job::Shutdown);
    callback!(on_retry, Job::Retry);
    callback!(on_inspect_operation, Job::Operation);
    callback!(on_open_project, Job::Open);
    callback!(on_retry_branch, Job::RetryBranch);
    {
        let weak = weak.clone();
        let sender = sender.clone();
        ui.on_submit_branch(move || {
            if let Some(ui) = weak.upgrade() {
                if !ui.get_can_submit_branch() {
                    return;
                }
                let action = match ui.get_branch_dialog() {
                    1 => BranchAction::Create {
                        name: ui.get_branch_input().to_string(),
                    },
                    2 => BranchAction::Checkout {
                        branch: ui.get_branch_target_id().to_string(),
                    },
                    3 => BranchAction::Publish {
                        name: ui.get_branch_input().to_string(),
                        confirmed: ui.get_publication_confirmed(),
                    },
                    4 => BranchAction::Checkpoint {
                        message: ui.get_branch_input().to_string(),
                    },
                    5 => BranchAction::Restore {
                        checkpoint: ui.get_branch_target_id().to_string(),
                        name: ui.get_branch_input().to_string(),
                    },
                    _ => return,
                };
                let Ok(generation) = ui.get_branch_source_generation().parse() else {
                    return;
                };
                let context = BranchContext {
                    state: ui.get_branch_source_project().to_string(),
                    active: ui.get_branch_source_active().to_string(),
                    generation,
                };
                ui.set_branch_dialog(0);
                enqueue(&weak, &sender, Job::Branch(context, action));
            }
        });
    }
    {
        let weak = weak.clone();
        let sender = sender.clone();
        ui.on_setup(move || {
            if let Some(ui) = weak.upgrade() {
                enqueue(
                    &weak,
                    &sender,
                    Job::Setup(
                        PathBuf::from(ui.get_data_folder().as_str()),
                        PathBuf::from(ui.get_mount_folder().as_str()),
                        ui.get_worker_limit() as usize,
                    ),
                );
            }
        });
    }
    {
        let weak = weak.clone();
        let sender = sender.clone();
        ui.on_create_project(move || {
            if let Some(ui) = weak.upgrade() {
                let mount = ui.get_project_mount();
                enqueue(
                    &weak,
                    &sender,
                    Job::Create(
                        ui.get_project_name().to_string(),
                        (!mount.is_empty()).then(|| PathBuf::from(mount.as_str())),
                    ),
                );
            }
        });
    }
    {
        let weak = weak.clone();
        let sender = sender.clone();
        ui.on_select_project(move |index| {
            use slint::Model;
            if let Some(ui) = weak.upgrade()
                && let Some(row) = ui.get_projects().row_data(index as usize)
            {
                enqueue(&weak, &sender, Job::Select(row.id.to_string()));
            }
        });
    }
    {
        let weak = weak.clone();
        let sender = sender.clone();
        ui.on_browse(move |kind| enqueue(&weak, &sender, Job::Browse(kind)));
    }
    let timer = slint::Timer::default();
    if report.is_none() {
        let weak = weak.clone();
        let sender = sender.clone();
        timer.start(
            slint::TimerMode::Repeated,
            Duration::from_secs(3),
            move || {
                if let Some(ui) = weak.upgrade()
                    && ui.get_connected()
                    && !ui.get_working()
                    && !ui.get_create_visible()
                    && !ui.get_shutdown_visible()
                    && !ui.get_about_visible()
                    && ui.get_branch_dialog() == 0
                {
                    enqueue(&weak, &sender, Job::Refresh);
                }
            },
        );
    }
    if !first_run {
        enqueue(&weak, &sender, Job::Connect);
    }
    let test_timer = if let Some(report) = report {
        Some(test_ui(&ui, report.to_owned())?)
    } else {
        None
    };
    ui.run()?;
    drop(test_timer);
    drop(timer);
    drop(sender);
    Ok(())
}

// This mode is explicitly selected by the isolated acceptance runner. It exercises
// real component callbacks and the native Windows window, never a headless mock.
fn snapshot(ui: &App, path: &Path) -> Result<()> {
    use std::io::Write;
    let pixels = ui.window().take_snapshot()?;
    let (w, h) = (pixels.width(), pixels.height());
    let size = w * h * 4;
    let mut file = std::fs::File::create(path)?;
    file.write_all(b"BM")?;
    file.write_all(&(54 + size).to_le_bytes())?;
    file.write_all(&[0; 4])?;
    file.write_all(&54u32.to_le_bytes())?;
    file.write_all(&40u32.to_le_bytes())?;
    file.write_all(&(w as i32).to_le_bytes())?;
    file.write_all(&(-(h as i32)).to_le_bytes())?;
    file.write_all(&1u16.to_le_bytes())?;
    file.write_all(&32u16.to_le_bytes())?;
    file.write_all(&[0; 24])?;
    let bytes = pixels
        .as_slice()
        .iter()
        .flat_map(|pixel| [pixel.b, pixel.g, pixel.r, 255])
        .collect::<Vec<_>>();
    file.write_all(&bytes)?;
    Ok(())
}
fn test_ui(ui: &App, report: PathBuf) -> Result<slint::Timer> {
    use slint::Model;
    let theme = std::env::var("TKFS_UI_TEST_THEME").unwrap_or_else(|_| "system".into());
    match theme.as_str() {
        "light" => ui.invoke_set_theme(1),
        "dark" => ui.invoke_set_theme(2),
        _ => {}
    }
    let directory = report
        .parent()
        .context("REPORT_PARENT_REQUIRED")?
        .to_owned();
    std::fs::create_dir_all(&directory)?;
    let weak = ui.as_weak();
    let timer = slint::Timer::default();
    let mut step = 0;
    let mut checks = Vec::<String>::new();
    let mut busy = None;
    let mut branch_acceptance = branch_acceptance::BranchAcceptance::default();
    let started = std::time::Instant::now();
    let mut ticks = 0;
    let phase = std::env::var("TKFS_UI_TEST_PHASE").unwrap_or_else(|_| "first".into());
    branch_acceptance.set_phase(&phase);
    timer.start(slint::TimerMode::Repeated,Duration::from_millis(300),move || {
        let Some(ui)=weak.upgrade()else{return;};ticks+=1;
        let result=(||->Result<bool>{
            ensure!(started.elapsed()<Duration::from_secs(100),"UI_TEST_TIMEOUT at step {step}");
            if ticks<3 || ui.get_working(){return Ok(false);}
            if phase.starts_with("branches") {
                return branch_acceptance.tick(&ui,&directory,&mut checks);
            } else if phase=="invalid-catalog" {
                ensure!(!ui.get_connected() && ui.get_error(),"INVALID_CATALOG_NOT_REFUSED: {}",ui.get_message());
                ensure!(ui.get_projects().row_count()==0,"INVALID_CATALOG_PROJECTS_EXPOSED");
                snapshot(&ui,&directory.join("startup-refused.bmp"))?;
                checks.push(format!("GUI refuses invalid established catalog and shows startup error: {}",ui.get_message()));
                return Ok(true);
            } else if phase=="reopen" {
                match step {
                    0=>{ensure!(ui.get_connected(),"REOPEN_NOT_CONNECTED: {}",ui.get_message());ensure!(ui.get_projects().row_count()==2,"REOPEN_PROJECT_COUNT");snapshot(&ui,&directory.join("reopened.bmp"))?;checks.push("existing config reconnects to both running projects".into());ui.set_about_visible(true);step=1;},
                    1=>{snapshot(&ui,&directory.join("about.bmp"))?;ui.set_about_visible(false);ui.invoke_shutdown();step=2;},
                    2=>{ensure!(!ui.get_connected()&&!ui.get_error(),"SHUTDOWN_FAILED: {}",ui.get_message());ensure!(ui.get_can_start_supervisor()&&!ui.get_can_stop_supervisor(),"SUPERVISOR_CONTROL_STATE");checks.push("explicit GUI shutdown cooperatively stops test workers; start/reconnect remains available".into());snapshot(&ui,&directory.join("shutdown.bmp"))?;ui.invoke_connect();step=3;},
                    3=>{ensure!(ui.get_connected(),"SUPERVISOR_RESTART_FAILED: {}",ui.get_message());if ui.get_projects().iter().any(|row|row.status!="Running"){ui.invoke_refresh();return Ok(false);}snapshot(&ui,&directory.join("supervisor-restarted.bmp"))?;checks.push("GUI reconnect starts stopped supervisor and restores desired project mounts".into());ui.invoke_shutdown();step=4;},
                    _=>{ensure!(!ui.get_connected()&&!ui.get_error(),"FINAL_SHUTDOWN_FAILED: {}",ui.get_message());return Ok(true);}
                }
            } else {
                match step {
                    0=>{ensure!(ui.get_first_run(),"FIRST_RUN_WIZARD_MISSING");snapshot(&ui,&directory.join("first-run.bmp"))?;checks.push("native window shows first-run wizard".into());ui.invoke_setup();step=1;},
                    1=>{ensure!(ui.get_connected(),"SETUP_FAILED: {}",ui.get_message());snapshot(&ui,&directory.join("empty.bmp"))?;checks.push("configuration saved and supervisor connected".into());ui.set_create_visible(true);ui.set_project_name("".into());step=2;},
                    2=>{ui.invoke_focus_project_name();ensure!(ui.get_project_name_focused(),"PROJECT_NAME_FOCUS_FAILED");ui.window().try_dispatch_event(slint::platform::WindowEvent::KeyPressed{text:slint::platform::Key::Tab.into()})?;ui.window().try_dispatch_event(slint::platform::WindowEvent::KeyReleased{text:slint::platform::Key::Tab.into()})?;ensure!(!ui.get_project_name_focused(),"TAB_NAVIGATION_FAILED");ui.invoke_focus_project_name();for letter in "Design studio".chars(){ui.window().try_dispatch_event(slint::platform::WindowEvent::KeyPressed{text:letter.to_string().into()})?;ui.window().try_dispatch_event(slint::platform::WindowEvent::KeyReleased{text:letter.to_string().into()})?;}ensure!(ui.get_project_name()=="Design studio","KEYBOARD_TEXT_ENTRY_FAILED");checks.push("toolkit keyboard focus, Tab navigation, and text entry work in create dialog".into());snapshot(&ui,&directory.join("create.bmp"))?;ui.set_create_visible(false);ui.invoke_create_project();step=3;},
                    3=>{ensure!(ui.get_projects().row_count()==1&&!ui.get_error(),"FIRST_CREATE_FAILED: {}",ui.get_message());snapshot(&ui,&directory.join("project.bmp"))?;let row=ui.get_projects().row_data(ui.get_selected() as usize).context("NO_SELECTED_PROJECT")?;let file=PathBuf::from(row.mount.as_str()).join("hello.txt");std::fs::write(&file,b"UI acceptance durable data")?;busy=Some(std::fs::File::open(file)?);ui.invoke_stop_project();step=4;},
                    4=>{ensure!(ui.get_error()&&ui.get_can_retry(),"BUSY_STOP_NOT_RETRYABLE: {}",ui.get_message());snapshot(&ui,&directory.join("busy.bmp"))?;checks.push("busy real file handle produces pending stop and exact retry UI".into());busy=None;ui.invoke_retry();step=5;},
                    5=>{ensure!(!ui.get_error(),"EXACT_RETRY_FAILED: {}",ui.get_message());ensure!(ui.get_can_start_project()&&!ui.get_can_stop_project(),"STOPPED_PROJECT_CONTROLS");snapshot(&ui,&directory.join("stopped.bmp"))?;ui.invoke_start_project();step=6;},
                    6=>{ensure!(!ui.get_error(),"RESTART_FAILED: {}",ui.get_message());let row=ui.get_projects().row_data(ui.get_selected() as usize).context("NO_SELECTED_PROJECT")?;ensure!(std::fs::read(PathBuf::from(row.mount.as_str()).join("hello.txt"))?==b"UI acceptance durable data","DATA_CHANGED");checks.push("exact retry stops worker; start restores durable mounted data".into());ui.set_project_name("Research lab".into());ui.set_project_mount("".into());ui.invoke_create_project();step=7;},
                    7=>{ensure!(ui.get_projects().row_count()==2&&!ui.get_error(),"SECOND_CREATE_FAILED: {}",ui.get_message());snapshot(&ui,&directory.join("two-projects.bmp"))?;ui.window().set_size(slint::LogicalSize::new(980.0,720.0));step=8;}, _=>{snapshot(&ui,&directory.join("minimum-size.bmp"))?;checks.push("two independent stores mounted; minimum-size layout captured; GUI closes without shutdown".into());return Ok(true);}
                }
            }
            Ok(false)
        })();
        match result {Ok(false)=>{},result=>{
            let error=result.as_ref().err().map(|e|format!("{e:#}"));let passed=error.is_none();let _=std::fs::write(&report,serde_json::to_vec_pretty(&json!({"passed":passed,"phase":phase,"theme":theme,"checks":checks,"error":error,"elapsed_seconds":started.elapsed().as_secs_f64()})).unwrap());let _=ui.hide();let _=slint::quit_event_loop();
        }}
    });
    Ok(timer)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_branch_action_never_follows_a_changed_project_selection() {
        let directory = tempfile::tempdir().unwrap();
        let mut controller = Controller {
            paths: Paths {
                executable: directory.path().join("fixture.exe"),
                folder: directory.path().into(),
                config: directory.path().join("orchestrator.toml"),
            },
            session: None,
            selected: Some(tkfs::core::id()),
            catalog: Value::Null,
            branch_client: None,
        };
        let context = BranchContext {
            state: tkfs::core::id(),
            active: tkfs::core::id(),
            generation: 0,
        };
        let error = controller
            .execute(Job::Branch(
                context,
                BranchAction::Create {
                    name: "must not be created".into(),
                },
            ))
            .err()
            .unwrap();
        assert!(error.to_string().contains("STALE_PROJECT_SELECTION"));
        assert!(
            !directory
                .path()
                .join(".tkfs-ui-branch-operation.json")
                .exists()
        );
    }
    #[test]
    fn transient_transport_failure_preserves_controls_and_marks_stale_status() {
        let mut view = View {
            connected: true,
            rows: vec![(
                "id".into(),
                "project".into(),
                "Running".into(),
                "mount".into(),
                true,
            )],
            ..Default::default()
        };
        connection_error(&mut view, true, true, "LOCAL_IPC: broken pipe");
        assert!(view.connected);
        assert_eq!(view.rows[0].2, "Running");
        assert!(view.connection.contains("stale"));
    }
    #[test]
    fn catalog_read_retries_a_transient_disconnect_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let name = format!("gui-retry-{}", tkfs::core::id());
        let config_path = dir.path().join("config.toml");
        std::fs::write(&config_path,format!("format_version=1\ndata_directory='data'\n[control]\ntransport='named-pipe'\nname='{name}'")).unwrap();
        let listener = tkfs::local_ipc::Listener::bind(&name).unwrap();
        let thread = std::thread::spawn(move || {
            let mut listener = listener;
            loop {
                if listener.receive::<Value>().unwrap().is_some() {
                    break;
                }
            }
            drop(listener);
            let mut listener = loop {
                match tkfs::local_ipc::Listener::bind(&name) {
                    Ok(l) => break l,
                    Err(_) => std::thread::sleep(Duration::from_millis(10)),
                }
            };
            loop {
                if let Some(request) = listener.receive::<Value>().unwrap() {
                    assert_eq!(request["action"]["op"], "list");
                    break;
                }
            }
            listener
                .reply(&json!({"ok":true,"result":{"states":[]}}))
                .unwrap();
        });
        let paths = Paths {
            executable: dir.path().join("test.exe"),
            folder: dir.path().into(),
            config: config_path.clone(),
        };
        let session = Session {
            paths,
            config: tkfs::orchestrator::Config::load(&config_path).unwrap(),
            identity: "test".into(),
            hello: Value::Null,
            saved: None,
        };
        assert_eq!(read_catalog(&session).unwrap(), json!({"states":[]}));
        thread.join().unwrap();
    }
}
