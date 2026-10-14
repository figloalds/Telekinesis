use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::{
    io::{IsTerminal, Write},
    path::PathBuf,
    time::Duration,
};
use tkfs::{
    core::{Store, id},
    model::{
        errors::code_of,
        health::MountState,
        status::{FolderStatus, GateKind, LifecyclePhase, PeerStatus, StatusSnapshot, policy},
    },
    orchestrator::{
        self, Action, Config,
        manage_v2::{ControlClient, default_deadline, format_folder, format_status},
    },
    runtime::{self, Discovery},
    util::time::now_ms,
    ws,
};
#[cfg(windows)]
mod gui;

#[derive(Parser)]
#[command(
    version,
    about = "Telekinesis: folders, workers and WebSocket orchestrators"
)]
struct Args {
    #[arg(short = 'f', long, global = true, default_value = "orchestrator.toml")]
    defaults_file: PathBuf,
    #[arg(short = 'C', global = true)]
    directory: Option<PathBuf>,
    #[arg(long, global = true)]
    runtime: Option<PathBuf>,
    #[arg(long, global = true)]
    request_id: Option<String>,
    #[arg(long, global = true)]
    generation: Option<u64>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Initialize a daemon configuration, or a standalone folder state.
    Init {
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long, requires = "state")]
        repo: Option<String>,
    },
    /// Run the folder supervisor and its built-in WS listener.
    #[command(alias = "start")]
    Orchestrator,
    /// Open the folder management desktop.
    Gui,
    /// Manage folders and network connections through the running daemon.
    #[command(after_help = MANAGE_HELP)]
    Manage {
        /// Print the daemon's JSON reply (`watch`: one snapshot per line).
        #[arg(long, global = true)]
        json: bool,
        #[command(subcommand)]
        action: ManagementCommand,
    },
    /// Create and mount a folder using the daemon.
    Create {
        label: String,
        #[arg(long)]
        mount: Option<PathBuf>,
        #[arg(long)]
        headless: bool,
    },
    /// Run one mount worker. Workers expose loopback control only.
    Daemon {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        mount: Option<PathBuf>,
        /// Launched by the orchestrator: report Ready once the store is
        /// recovered and RPC is up, and attach the mount only on `mount-attach`.
        #[arg(long)]
        supervised: bool,
    },
    Stop {
        /// Persist unsaved files and detach even while files are open.
        #[arg(long)]
        force: bool,
    },
    Status,
    Branches,
    Conflicts,
    State,
    Events,
    History,
    /// Export authorized shared objects to an explicit LOCAL test bucket directory.
    BucketExport {
        #[arg(long = "directory")]
        destination: PathBuf,
    },
    /// Move a reviewed displaced entry to a free path, preserving its stable ID.
    Recover {
        entity: String,
        target: String,
        #[arg(long,num_args=1..,required=true)]
        conflict: Vec<String>,
    },
    /// Restore a checkpoint's visible snapshot into a NEW private branch.
    Restore {
        checkpoint: String,
        #[arg(long)]
        branch: String,
    },
    Branch {
        name: String,
    },
    Checkout {
        name: String,
    },
    /// Explicitly publish only the visible cut as a NEW shared branch. Private
    /// history and private conflict alternatives remain on this computer.
    Publish {
        name: String,
        #[arg(long, required = true)]
        current_state_only: bool,
    },
    Checkpoint {
        #[arg(short = 'm', long)]
        message: String,
    },
    Resolve {
        #[arg(long,num_args=1..)]
        conflict: Vec<String>,
        #[arg(long)]
        revision: String,
    },
    CatObject {
        object: String,
    },
    Import {
        path: String,
        source: PathBuf,
    },
    Mkdir {
        path: String,
    },
    Rename {
        path: String,
        target: String,
        #[arg(long)]
        replace: bool,
    },
    Delete {
        path: String,
    },
}
/// Shown after `tkfs manage --help`.
const MANAGE_HELP: &str = "Folder arguments take the full state id, a unique prefix of it (the \
[id] that `status` shows) or a unique folder label.";

#[derive(Subcommand)]
enum ManagementCommand {
    /// Show the daemon, each folder (lifecycle, mount, sync with each peer,
    /// pending work, rates and ETA, errors) and the peers.
    Status,
    /// Show the status and keep it updated until Ctrl-C.
    Watch,
    /// List the folders (JSON).
    List,
    /// One folder's details (JSON).
    Inspect { state: String },
    /// Create a folder (and start it).
    Create {
        label: String,
        #[arg(long)]
        mount: Option<PathBuf>,
        #[arg(long)]
        headless: bool,
    },
    /// Set or clear a stopped folder's mount path.
    ConfigureMount {
        state: String,
        #[arg(long)]
        mount: Option<PathBuf>,
    },
    /// Start a folder's worker.
    Start { state: String },
    /// Stop a folder's worker (refused while files are open).
    Stop {
        state: String,
        /// Return at once instead of waiting for the worker to stop.
        #[arg(long)]
        no_wait: bool,
    },
    /// Stop a folder even while files are open (unsaved changes are kept).
    ForceStop { state: String },
    /// Restart a failed or backing-off folder now, and retry a held mount.
    Retry { state: String },
    /// Mount a folder's filesystem now.
    Attach { state: String },
    /// Unmount a folder's filesystem; the folder keeps syncing.
    Detach {
        state: String,
        /// Detach even while files are open (unsaved changes are kept).
        #[arg(long)]
        force: bool,
    },
    /// Mount a received folder before its initial sync has finished.
    MountNow { state: String },
    /// Pause syncing a folder with its peers.
    Pause { state: String },
    /// Resume syncing a folder with its peers.
    Resume { state: String },
    /// Check quarantined and blocked incoming events of a folder again.
    Revalidate {
        state: String,
        /// Only these events (and the ones blocked on them); repeatable.
        #[arg(long = "id")]
        ids: Vec<String>,
    },
    /// Collect a diagnostics bundle.
    Diagnostics,
    /// Stop the daemon (busy workers keep running and are adopted later).
    Shutdown,
    /// Create a new pairing code for peers to pair with.
    ResetPairingCode,
    /// Pair to ws://host:port or wss://host. Reads the remote code from stdin.
    Pair {
        endpoint: String,
        #[arg(long)]
        connections: Option<usize>,
    },
    /// Forget a paired peer.
    Unpair { endpoint: String },
    /// The paired peers and their lanes (JSON).
    Peers,
    /// The folders a paired peer publishes.
    RemoteList { endpoint: String },
    /// Receive a peer's folder.
    Receive {
        endpoint: String,
        repo: String,
        label: String,
        #[arg(long)]
        mount: Option<PathBuf>,
        #[arg(long)]
        headless: bool,
    },
}

/// `tkfs manage …`: one persistent [`ControlClient`] for the command.
///
/// `status` renders the snapshot ([`format_status`]); `watch` follows it.
/// Folder actions of the management API v2 print what happened and the
/// folder's block; every other command prints the daemon's JSON reply, as
/// before. `--json` prints the reply as it is.
fn manage(config: &Config, command: ManagementCommand, json: bool) -> Result<()> {
    if let ManagementCommand::Watch = command {
        return watch(config, json);
    }
    let mut client = ControlClient::connect(config)?;
    if let ManagementCommand::Status = command {
        let reply = client.call(&Action::Status, default_deadline(&Action::Status))?;
        if json {
            return print(reply);
        }
        let snapshot = as_of_now(serde_json::from_value(reply).context("INVALID_REPLY: status")?);
        return Ok(emit(&format_status(&snapshot))?);
    }
    let mut folders = Folders::default();
    let (action, verb) = management(&mut client, &mut folders, command)?;
    let reply = client.call(&action, default_deadline(&action))?;
    match verb {
        Some(verb) if !json => describe(verb, &reply, &folders),
        _ => print(reply),
    }
}

/// The action for a command, and for a folder action of the management API
/// v2 the verb [`describe`] reports.
fn management(
    client: &mut ControlClient,
    folders: &mut Folders,
    action: ManagementCommand,
) -> Result<(Action, Option<&'static str>)> {
    let mut state = |given: String| folders.resolve(client, given);
    Ok(match action {
        ManagementCommand::Status | ManagementCommand::Watch => {
            bail!("UNSUPPORTED_OPERATION: status and watch are not actions")
        }
        ManagementCommand::Attach { state: given } => (
            Action::Attach {
                state: state(given)?,
            },
            Some("attach"),
        ),
        ManagementCommand::Detach {
            state: given,
            force,
        } => (
            Action::Detach {
                state: state(given)?,
                force,
            },
            Some("detach"),
        ),
        ManagementCommand::MountNow { state: given } => (
            Action::OverrideGate {
                state: state(given)?,
            },
            Some("mount-now"),
        ),
        ManagementCommand::Pause { state: given } => (
            Action::PauseSync {
                state: state(given)?,
            },
            Some("pause"),
        ),
        ManagementCommand::Resume { state: given } => (
            Action::ResumeSync {
                state: state(given)?,
            },
            Some("resume"),
        ),
        ManagementCommand::ForceStop { state: given } => (
            Action::ForceStop {
                state: state(given)?,
            },
            Some("force-stop"),
        ),
        ManagementCommand::Retry { state: given } => (
            Action::Retry {
                state: state(given)?,
            },
            Some("retry"),
        ),
        ManagementCommand::Revalidate { state: given, ids } => (
            Action::Revalidate {
                state: state(given)?,
                ids: (!ids.is_empty()).then_some(ids),
            },
            Some("revalidate"),
        ),
        ManagementCommand::Diagnostics => (Action::Diagnostics, None),
        ManagementCommand::List => (Action::List, None),
        ManagementCommand::Inspect { state: given } => (
            Action::Inspect {
                state: state(given)?,
            },
            None,
        ),
        ManagementCommand::Create {
            label,
            mount,
            headless,
        } => (
            Action::Create {
                label,
                mount,
                headless,
            },
            None,
        ),
        ManagementCommand::ConfigureMount {
            state: given,
            mount,
        } => (
            Action::ConfigureMount {
                state: state(given)?,
                mount,
            },
            None,
        ),
        ManagementCommand::Start { state: given } => (
            Action::Start {
                state: state(given)?,
            },
            None,
        ),
        ManagementCommand::Stop {
            state: given,
            no_wait,
        } => (
            Action::Stop {
                state: state(given)?,
                wait: no_wait.then_some(false),
            },
            None,
        ),
        other => (legacy_management(other)?, None),
    })
}

/// The commands that name no folder.
fn legacy_management(action: ManagementCommand) -> Result<Action> {
    Ok(match action {
        ManagementCommand::Shutdown => Action::Shutdown,
        ManagementCommand::ResetPairingCode => Action::ResetPairingCode,
        ManagementCommand::Pair {
            endpoint,
            connections,
        } => {
            eprintln!("Pairing code:");
            let mut code = String::new();
            std::io::stdin().read_line(&mut code)?;
            Action::Pair {
                endpoint,
                connections,
                code: code.trim().into(),
            }
        }
        ManagementCommand::Unpair { endpoint } => Action::Unpair { endpoint },
        ManagementCommand::Peers => Action::Peers,
        ManagementCommand::RemoteList { endpoint } => Action::RemoteList { endpoint },
        ManagementCommand::Receive {
            endpoint,
            repo,
            label,
            mount,
            headless,
        } => Action::Receive {
            endpoint,
            repo,
            label,
            mount,
            headless,
        },
        _ => bail!("UNSUPPORTED_OPERATION: not a management command"),
    })
}

/// The daemon's folders, read once (`status`) to resolve folder arguments
/// and to name peers in [`describe`].
#[derive(Default)]
struct Folders {
    snapshot: Option<StatusSnapshot>,
    read: bool,
}

impl Folders {
    /// The full state of a folder argument: the state itself, a unique
    /// folder label, or a unique prefix (at least 4 characters) of a state.
    /// An argument that matches nothing is passed on unchanged, so the
    /// daemon answers `FOLDER_NOT_FOUND`; so is every argument when the
    /// daemon has no `status` action (an older daemon).
    fn resolve(&mut self, client: &mut ControlClient, given: String) -> Result<String> {
        if !self.read {
            self.read = true;
            match client.call(&Action::Status, default_deadline(&Action::Status)) {
                Ok(reply) => self.snapshot = serde_json::from_value(reply).ok(),
                // An older daemon may close the connection after an error
                // reply: start the real action on a fresh one.
                Err(_) => client.disconnect(),
            }
        }
        let Some(snapshot) = &self.snapshot else {
            return Ok(given);
        };
        if snapshot.folders.iter().any(|folder| folder.state == given) {
            return Ok(given);
        }
        let pick = |matches: Vec<&FolderStatus>, how: &str| -> Result<Option<String>> {
            match matches.as_slice() {
                [] => Ok(None),
                [folder] => Ok(Some(folder.state.clone())),
                many => bail!(
                    "INVALID_REQUEST: {given:?} is the {how} of {} folders ({}); use a longer id",
                    many.len(),
                    many.iter()
                        .map(|folder| folder.state.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        };
        let by_label = snapshot
            .folders
            .iter()
            .filter(|folder| folder.label == given)
            .collect();
        if let Some(state) = pick(by_label, "label")? {
            return Ok(state);
        }
        let by_prefix = snapshot
            .folders
            .iter()
            .filter(|folder| given.len() >= 4 && folder.state.starts_with(&given))
            .collect();
        Ok(pick(by_prefix, "id prefix")?.unwrap_or(given))
    }

    fn peers(&self) -> &[PeerStatus] {
        self.snapshot
            .as_ref()
            .map_or(&[][..], |snapshot| snapshot.peers.as_slice())
    }
}

/// What a folder action did (one line), then the folder's block.
fn describe(verb: &str, reply: &serde_json::Value, folders: &Folders) -> Result<()> {
    let status: Option<FolderStatus> =
        serde_json::from_value(reply["status"].clone()).context("INVALID_REPLY: status")?;
    let Some(status) = status else {
        return print(reply.clone());
    };
    let text = format!(
        "{}
{}",
        outcome(verb, &status, reply),
        format_folder(&status, folders.peers(), now_ms())
    );
    Ok(emit(&text)?)
}

/// One sentence about the result of `verb` on the folder.
fn outcome(verb: &str, f: &FolderStatus, reply: &serde_json::Value) -> String {
    let name = if f.label.is_empty() {
        f.state.clone()
    } else {
        f.label.clone()
    };
    let path = f.mount.path.as_ref().map(|path| path.display().to_string());
    let phase = phase_name(f.lifecycle.phase);
    let not_mounted = || -> String {
        if path.is_none() {
            "it has no mount path (tkfs manage configure-mount sets one)".into()
        } else if !f.intents.mount_enabled {
            "mounting is disabled for it".into()
        } else if f.lifecycle.phase != LifecyclePhase::Running {
            format!("its worker is {phase}; it mounts once the worker runs")
        } else if matches!(f.gate.state, GateKind::Awaiting | GateKind::Blocked) {
            "it waits for the initial sync (tkfs manage mount-now mounts it anyway)".into()
        } else {
            format!("the mount is {}", f.mount.state.name().replace('_', " "))
        }
    };
    match verb {
        "attach" | "mount-now" => {
            let prefix = if verb == "mount-now" {
                "Initial-sync gate overridden. "
            } else {
                ""
            };
            match &f.mount.state {
                MountState::Attached { .. } => format!(
                    "{prefix}{name} is mounted at {}.",
                    path.as_deref().unwrap_or("its mount path")
                ),
                MountState::AttachFailed { error, .. } => {
                    format!("{prefix}{name} could not be mounted: {error}.")
                }
                MountState::Preflight | MountState::Attaching => {
                    format!("{prefix}{name} is being mounted.")
                }
                _ => format!("{prefix}{name} is not mounted: {}.", not_mounted()),
            }
        }
        "detach" => match &f.mount.state {
            MountState::Attached { .. } => format!("{name} is still mounted."),
            MountState::Detaching => format!("{name} is being unmounted."),
            _ => format!("{name} is unmounted; it keeps syncing."),
        },
        "pause" => format!("Sync of {name} is paused."),
        "resume" => format!("Sync of {name} is resumed."),
        "force-stop" => match f.lifecycle.phase {
            LifecyclePhase::Stopped => format!("{name} is stopped."),
            _ => format!("{name} is {phase}."),
        },
        "retry" => format!("Retry requested: {name} is {phase}."),
        "revalidate" => {
            let requeued = reply["requeued"].as_u64().unwrap_or(0);
            format!(
                "{requeued} event{} of {name} re-queued for activation.",
                if requeued == 1 { "" } else { "s" }
            )
        }
        _ => format!("{verb}: {name} is {phase}."),
    }
}

fn phase_name(phase: LifecyclePhase) -> &'static str {
    match phase {
        LifecyclePhase::Stopped => "stopped",
        LifecyclePhase::Starting => "starting",
        LifecyclePhase::Running => "running",
        LifecyclePhase::StopBlocked => "blocked from stopping",
        LifecyclePhase::Stopping => "stopping",
        LifecyclePhase::Backoff => "waiting to restart",
        LifecyclePhase::Failed => "failed",
    }
}

/// The daemon runs on this machine: show ages as of now, not as of the
/// snapshot's last rebuild (an idle hub does not rebuild).
fn as_of_now(mut snapshot: StatusSnapshot) -> StatusSnapshot {
    snapshot.at_ms = snapshot.at_ms.max(now_ms());
    snapshot
}

/// How often `watch` redraws while nothing changes (ages and countdowns).
const WATCH_REFRESH_MS: u32 = 2_000;
/// How long `watch` waits before reconnecting to an unreachable daemon.
const WATCH_RECONNECT: Duration = Duration::from_secs(2);

/// `tkfs manage watch`: long-polls `subscribe` and redraws the status on
/// every change (and every 2 s, for the ages). On a terminal the screen is
/// redrawn in place; otherwise each changed snapshot is printed. `--json`
/// prints each new snapshot as one line. A daemon that goes away (or is
/// shutting down) is waited for, and followed to its new port after a
/// restart; any other error reply (an older daemon without `subscribe`)
/// ends the command. A closed output pipe ends it quietly.
fn watch(config: &Config, json: bool) -> Result<()> {
    match follow(config, json) {
        Err(error)
            if error.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
            }) =>
        {
            Ok(())
        }
        other => other,
    }
}

fn follow(config: &Config, json: bool) -> Result<()> {
    let ansi = !json && std::io::stdout().is_terminal() && enable_ansi();
    let mut client: Option<ControlClient> = None;
    let mut since = 0;
    let mut shown = None;
    let mut down = false;
    let mut screen = Screen::default();
    loop {
        let wait_ms = if json {
            policy::SUBSCRIBE_MAX_WAIT_MS
        } else {
            WATCH_REFRESH_MS
        };
        let next = (|| -> Result<StatusSnapshot> {
            let client = match &mut client {
                Some(client) => client,
                None => client.insert(ControlClient::connect(config)?),
            };
            let action = Action::Subscribe {
                since,
                wait_ms: Some(wait_ms),
            };
            let reply = client.call(&action, default_deadline(&action))?;
            serde_json::from_value(reply).context("INVALID_REPLY: status snapshot")
        })();
        match next {
            Ok(snapshot) => {
                down = false;
                since = snapshot.generation;
                let changed = shown != Some(snapshot.generation);
                shown = Some(snapshot.generation);
                if json {
                    if changed {
                        emit(&format!("{}\n", serde_json::to_string(&snapshot)?))?;
                    }
                } else if ansi {
                    let text = format_status(&as_of_now(snapshot));
                    screen.draw(&format!(
                        "tkfs manage watch · updates live · Ctrl-C to quit\n\n{text}"
                    ))?;
                } else if changed {
                    emit(&format!("{}\n", format_status(&as_of_now(snapshot))))?;
                }
            }
            Err(error)
                if error.downcast_ref::<ws::RemoteError>().is_some()
                    && code_of(&error) != "ORCHESTRATOR_STOPPING" =>
            {
                return Err(error);
            }
            Err(error) => {
                client = None;
                since = 0;
                shown = None;
                let text = format!(
                    "tkfs manage watch: the daemon is not reachable ({error:#}); retrying every {} s",
                    WATCH_RECONNECT.as_secs()
                );
                if ansi {
                    screen.draw(&text)?;
                } else if !down {
                    eprintln!("{text}");
                }
                down = true;
                std::thread::sleep(WATCH_RECONNECT);
            }
        }
    }
}

/// Writes and flushes `text` to stdout (an error, such as a closed pipe, is
/// returned instead of panicking as `print!` would).
fn emit(text: &str) -> std::io::Result<()> {
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(text.as_bytes())?;
    stdout.flush()
}

/// In-place redraws on an ANSI terminal.
#[derive(Default)]
struct Screen {
    cleared: bool,
}

impl Screen {
    fn draw(&mut self, text: &str) -> std::io::Result<()> {
        let mut frame = String::with_capacity(text.len() + 256);
        if !self.cleared {
            frame.push_str("\x1b[2J");
            self.cleared = true;
        }
        frame.push_str("\x1b[H");
        for line in text.lines() {
            frame.push_str(line);
            frame.push_str("\x1b[K\n");
        }
        frame.push_str("\x1b[J");
        emit(&frame)
    }
}

/// Turns on ANSI escape processing for the console (Windows 10+ needs it
/// for in-place redraws); false when stdout is not a console that can.
#[cfg(windows)]
fn enable_ansi() -> bool {
    use std::ffi::c_void;
    unsafe extern "system" {
        fn GetStdHandle(handle: u32) -> *mut c_void;
        fn GetConsoleMode(console: *mut c_void, mode: *mut u32) -> i32;
        fn SetConsoleMode(console: *mut c_void, mode: u32) -> i32;
    }
    const STD_OUTPUT_HANDLE: u32 = -11i32 as u32;
    const ENABLE_VIRTUAL_TERMINAL_PROCESSING: u32 = 0x0004;
    // SAFETY: plain Win32 calls on this process's standard output handle;
    // `mode` outlives the call that writes it.
    unsafe {
        let console = GetStdHandle(STD_OUTPUT_HANDLE);
        if console.is_null() || console as isize == -1 {
            return false;
        }
        let mut mode = 0u32;
        if GetConsoleMode(console, &mut mode) == 0 {
            return false;
        }
        mode & ENABLE_VIRTUAL_TERMINAL_PROCESSING != 0
            || SetConsoleMode(console, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING) != 0
    }
}

#[cfg(not(windows))]
fn enable_ansi() -> bool {
    // `stringify!`: the contracts test reads quoted SCREAMING_CASE words as
    // error codes.
    std::env::var_os(stringify!(TERM)).is_none_or(|term| term != "dumb")
}
fn discover(args: &Args) -> Result<Discovery> {
    if let Some(path) = &args.runtime {
        return Ok(serde_json::from_slice(&std::fs::read(path)?)?);
    }
    let cwd = args.directory.clone().unwrap_or(std::env::current_dir()?);
    for path in cwd.ancestors() {
        let file = path.join(".tkfs-runtime.json");
        if file.is_file() {
            return Ok(serde_json::from_slice(&std::fs::read(file)?)?);
        }
    }
    bail!("FOLDER_NOT_FOUND: use -C MOUNT or --runtime STATE/runtime.json")
}
fn print(value: serde_json::Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    #[cfg(windows)]
    if std::env::args_os().len() == 1 {
        return gui::run(std::path::Path::new("orchestrator.toml"));
    }
    let args = Args::parse();
    match args.command {
        Command::Init {
            ref state,
            ref repo,
        } => {
            if let Some(state) = state {
                std::fs::create_dir_all(state)?;
                let _lock = orchestrator::lock_state(state)?;
                return print(Store::open(state, repo.as_deref())?.status()?);
            }
            return print(json!(Config::initialize(&args.defaults_file)?));
        }
        Command::Orchestrator => return orchestrator::run(Config::load(&args.defaults_file)?),
        Command::Gui => {
            #[cfg(windows)]
            return gui::run(&args.defaults_file);
            #[cfg(not(windows))]
            bail!("DESKTOP_WINDOWS_ONLY");
        }
        Command::Daemon {
            ref state,
            ref mount,
            supervised,
        } => {
            return tkfs::worker::run(tkfs::worker::WorkerArgs {
                state: state.clone(),
                mount: mount.clone(),
                supervised,
            });
        }
        Command::Create {
            label,
            mount,
            headless,
        } => {
            return print(orchestrator::client(
                &Config::load(&args.defaults_file)?,
                &Action::Create {
                    label,
                    mount,
                    headless,
                },
            )?);
        }
        Command::Manage { json, action } => {
            return manage(&Config::load(&args.defaults_file)?, action, json);
        }
        _ => {}
    }
    let info = discover(&args)?;
    let mut payload = match &args.command {
        Command::Stop { force: false } => json!({"op":"stop"}),
        Command::Stop { force: true } => json!({"op":"stop","mode":"force"}),
        Command::Status => json!({"op":"status"}),
        Command::Branches => json!({"op":"branches"}),
        Command::Conflicts => json!({"op":"conflicts"}),
        Command::State => json!({"op":"state"}),
        Command::Events => json!({"op":"events"}),
        Command::History => json!({"op":"history"}),
        Command::BucketExport { destination } => {
            json!({"op":"bucket-export","directory":std::path::absolute(destination)?})
        }
        Command::Recover {
            entity,
            target,
            conflict,
        } => json!({"op":"recover","entity":entity,"target":target,"conflicts":conflict}),
        Command::Restore { checkpoint, branch } => {
            json!({"op":"restore","checkpoint":checkpoint,"name":branch})
        }
        Command::Branch { name } => json!({"op":"branch","name":name}),
        Command::Checkout { name } => json!({"op":"checkout","name":name}),
        Command::Publish {
            name,
            current_state_only,
        } => json!({"op":"publish","name":name,"current_state_only":current_state_only}),
        Command::Checkpoint { message } => json!({"op":"checkpoint","message":message}),
        Command::Resolve { conflict, revision } => {
            json!({"op":"resolve","conflicts":conflict,"revision":revision})
        }
        Command::CatObject { object } => json!({"op":"cat-object","object":object}),
        Command::Import { path, source } => {
            json!({"op":"import","path":path,"hex":hex::encode(std::fs::read(source)?)})
        }
        Command::Mkdir { path } => json!({"op":"mkdir","path":path}),
        Command::Rename {
            path,
            target,
            replace,
        } => json!({"op":"rename","path":path,"target":target,"replace":replace}),
        Command::Delete { path } => json!({"op":"delete","path":path}),
        _ => unreachable!(),
    };
    if let Some(g) = args.generation {
        payload["generation"] = json!(g);
    }
    let result = runtime::rpc(&info, &args.request_id.unwrap_or_else(id), payload)?;
    if let Command::CatObject { .. } = args.command {
        use std::io::Write;
        std::io::stdout().write_all(&hex::decode(
            result["hex"].as_str().context("INVALID_REPLY")?,
        )?)?;
    } else {
        println!("{}", serde_json::to_string_pretty(&result)?);
    }
    Ok(())
}
