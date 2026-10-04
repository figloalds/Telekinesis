#![cfg_attr(windows, windows_subsystem = "windows")]
use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tkfs::{
    core::{Store, id},
    runtime::{self, Discovery, Engine, PeerConfig},
};
#[cfg(windows)]
mod gui;

#[derive(Parser)]
#[command(
    version,
    about = "TKFS causal filesystem: Windows WinFsp and Linux headless FUSE"
)]
struct Args {
    /// Resolve a mounted project from this directory (otherwise use cwd).
    #[arg(short = 'C', global = true)]
    directory: Option<PathBuf>,
    /// Runtime discovery file, for control from outside the mount.
    #[arg(long, global = true)]
    runtime: Option<PathBuf>,
    /// Retry the same semantic request using the same UUID.
    #[arg(long, global = true)]
    request_id: Option<String>,
    /// Reject an obsolete checkout or management generation.
    #[arg(long, global = true)]
    generation: Option<u64>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Open the portable desktop application (also the default with no arguments).
    Gui,
    #[command(hide = true)]
    GuiTest {
        #[arg(long)]
        report: PathBuf,
    },
    /// Foreground O1 management supervisor; networking is disabled.
    Orchestrator {
        /// Configuration in the current directory; -f selects another file.
        #[arg(short = 'f', long, default_value = "orchestrator.toml")]
        defaults_file: PathBuf,
    },
    /// Client of the same-user management API. Mutations require --generation.
    Manage {
        /// Configuration in the current directory; -f selects another file.
        #[arg(short = 'f', long, default_value = "orchestrator.toml")]
        defaults_file: PathBuf,
        #[command(subcommand)]
        action: ManagementCommand,
    },
    #[command(hide = true)]
    ManagedWorker,
    Init {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        repo: Option<String>,
    },
    Daemon {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        mount: Option<PathBuf>,
        #[arg(long,requires_all=["peer","peer_device"])]
        listen: Option<String>,
        #[arg(long, requires = "listen")]
        peer: Option<String>,
        #[arg(long, requires = "listen")]
        peer_device: Option<String>,
    },
    #[cfg(target_os = "linux")]
    Stop,
    Status,
    Branches,
    Conflicts,
    State,
    Events,
    Sync,
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
#[derive(Subcommand)]
enum ManagementCommand {
    Hello,
    Create {
        label: String,
        #[arg(long)]
        mount: Option<PathBuf>,
    },
    List,
    Inspect {
        state: String,
    },
    Start {
        state: String,
    },
    Stop {
        state: String,
    },
    Shutdown,
    Operation {
        operation: String,
    },
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
    bail!("PROJECT_NOT_FOUND: use -C <mounted path> or --runtime <state>/runtime.json")
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{}", json!({"ok":false,"error":format!("{e:#}")}));
        #[cfg(windows)]
        if std::env::args_os().nth(1).is_none_or(|arg| arg == "gui") {
            use windows::{
                Win32::UI::WindowsAndMessaging::{MB_ICONERROR, MB_OK, MessageBoxW},
                core::PCWSTR,
            };
            let message: Vec<u16> = format!("Telekinesis could not open.\n\n{e:#}")
                .encode_utf16()
                .chain(Some(0))
                .collect();
            let title: Vec<u16> = "Telekinesis".encode_utf16().chain(Some(0)).collect();
            unsafe {
                MessageBoxW(
                    None,
                    PCWSTR(message.as_ptr()),
                    PCWSTR(title.as_ptr()),
                    MB_OK | MB_ICONERROR,
                );
            }
        }
        std::process::exit(1);
    }
}
#[cfg(target_os = "linux")]
static LINUX_STOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(target_os = "linux")]
extern "C" fn request_linux_stop(_: libc::c_int) {
    LINUX_STOP.store(true, std::sync::atomic::Ordering::Relaxed);
}

fn run() -> Result<()> {
    #[cfg(windows)]
    if std::env::args_os().len() == 1 {
        return gui::run(None);
    }
    #[cfg(windows)]
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg != "gui" && arg != "gui-test")
    {
        unsafe extern "system" {
            fn AttachConsole(pid: u32) -> i32;
        }
        // Redirected handles are preserved; interactive CLI attaches its parent's console.
        unsafe {
            AttachConsole(u32::MAX);
        }
    }
    let args = Args::parse();
    match &args.command {
        Command::Gui => {
            #[cfg(windows)]
            return gui::run(None);
            #[cfg(not(windows))]
            bail!("DESKTOP_WINDOWS_ONLY");
        }
        Command::GuiTest { report } => {
            #[cfg(windows)]
            return gui::run(Some(report));
            #[cfg(not(windows))]
            bail!("DESKTOP_WINDOWS_ONLY: {}", report.display());
        }
        Command::Orchestrator { defaults_file } => {
            #[cfg(windows)]
            return tkfs::orchestrator::Supervisor::open(tkfs::orchestrator::Config::load(
                defaults_file,
            )?)?
            .run();
            #[cfg(not(windows))]
            bail!("O1_WINDOWS_ONLY: {}", defaults_file.display());
        }
        Command::ManagedWorker => {
            #[cfg(windows)]
            return tkfs::orchestrator::worker();
            #[cfg(not(windows))]
            bail!("O1_WINDOWS_ONLY");
        }
        Command::Manage {
            defaults_file,
            action,
        } => {
            #[cfg(windows)]
            {
                use tkfs::orchestrator::{self, Action, Request};
                let config = orchestrator::Config::load(defaults_file)?;
                let action = match action {
                    ManagementCommand::Hello => Action::Hello,
                    ManagementCommand::Create { label, mount } => Action::Create {
                        label: label.clone(),
                        mount: mount.clone(),
                    },
                    ManagementCommand::List => Action::List,
                    ManagementCommand::Inspect { state } => Action::Inspect {
                        state: state.clone(),
                    },
                    ManagementCommand::Start { state } => Action::Start {
                        state: state.clone(),
                    },
                    ManagementCommand::Stop { state } => Action::Stop {
                        state: state.clone(),
                    },
                    ManagementCommand::Shutdown => Action::Shutdown,
                    ManagementCommand::Operation { operation } => Action::Operation {
                        operation: operation.clone(),
                    },
                };
                if action.mutates() {
                    ensure!(
                        args.generation.is_some(),
                        "MANAGEMENT_GENERATION_REQUIRED: inspect catalog/state generation; retry with the same --request-id and --generation"
                    );
                }
                let request = Request {
                    version: 1,
                    target_installation: config.installation_id.clone(),
                    operation_id: if action.mutates() {
                        Some(args.request_id.clone().unwrap_or_else(id))
                    } else {
                        None
                    },
                    expected_generation: args.generation,
                    action,
                };
                if request.action.mutates() && args.request_id.is_none() {
                    eprintln!(
                        "{}",
                        json!({"operation_id":request.operation_id,"expected_generation":request.expected_generation,"retry":"repeat the same action with these --request-id and --generation values"})
                    );
                }
                let response = orchestrator::client(&config, &request)?;
                if response["ok"] != true {
                    bail!("{}", response);
                }
                println!("{}", serde_json::to_string_pretty(&response["result"])?);
                return Ok(());
            }
            #[cfg(not(windows))]
            {
                let _ = action;
                bail!("O1_WINDOWS_ONLY: {}", defaults_file.display());
            }
        }
        Command::Init { state, repo } => {
            std::fs::create_dir_all(state)?;
            let _lock = lock_state(state)?;
            let store = Store::open(state, repo.as_deref())?;
            println!("{}", serde_json::to_string_pretty(&store.status()?)?);
            return Ok(());
        }
        Command::Daemon {
            state,
            mount,
            listen,
            peer,
            peer_device,
        } => {
            ensure!(state.is_dir(), "STATE_NOT_INITIALIZED");
            let state = std::fs::canonicalize(state)?;
            // Native exclusive sharing prevents two owners of the same SQLite,
            // even after crash: the OS releases the lock, no stale lockfile policy.
            let _lock = lock_state(&state)?;
            let store = Store::open_existing(&state)?;
            let engine = Arc::new(Mutex::new(Engine::new(store)));
            let mount = mount.as_ref().map(std::path::absolute).transpose()?;
            if let Some(m) = &mount {
                // Compare canonical parent paths, including Windows extended
                // prefixes/junctions, rather than mixing C:\ and \\?\C:\ forms.
                let resolved_mount =
                    std::fs::canonicalize(m.parent().context("MOUNT_REQUIRES_PARENT")?)?
                        .join(m.file_name().context("FOLDER_MOUNT_REQUIRED")?);
                ensure!(
                    !resolved_mount.starts_with(&state) && !state.starts_with(&resolved_mount),
                    "STATE_MUST_BE_OUTSIDE_MOUNT"
                );
            }
            if let Some(listen) = listen {
                let bytes=hex::decode(std::env::var("TKFS_PEER_KEY").context("Set TKFS_PEER_KEY to an ephemeral 64-hex-character pairing secret; no key is persisted")?)?;
                let key: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("INVALID_PEER_KEY"))?;
                ensure!(
                    peer_device.as_ref() != Some(&engine.lock().unwrap().store.device),
                    "PEER_MUST_BE_DISTINCT_DEVICE"
                );
                runtime::start_peer(
                    engine.clone(),
                    PeerConfig {
                        listen: listen.clone(),
                        address: peer.clone().unwrap(),
                        peer: peer_device.clone().unwrap(),
                        key,
                    },
                )?;
            }
            let info = runtime::start_rpc(
                engine.clone(),
                &state,
                mount.as_ref().map(|m| m.to_string_lossy().into_owned()),
            )?;
            if let Some(m) = &mount {
                #[cfg(any(windows, target_os = "linux"))]
                tkfs::mount::start(engine.clone(), m)?;
                #[cfg(not(any(windows, target_os = "linux")))]
                bail!("Windows WinFsp mount only in this PoC");
            }
            println!(
                "{}",
                json!({"ready":true,"repo":info.repo,"device":info.device,"runtime":state.join("runtime.json"),"mount":mount})
            );
            #[cfg(target_os = "linux")]
            unsafe {
                libc::signal(libc::SIGINT, request_linux_stop as libc::sighandler_t);
                libc::signal(libc::SIGTERM, request_linux_stop as libc::sighandler_t);
            }
            loop {
                #[cfg(target_os = "linux")]
                {
                    if LINUX_STOP.swap(false, std::sync::atomic::Ordering::Relaxed) {
                        let stopped = if engine.lock().unwrap().mounted {
                            tkfs::mount::stop(&engine)
                        } else {
                            engine.lock().unwrap().quiet()
                        };
                        match stopped {
                            Ok(()) => {
                                let _ = std::fs::remove_file(state.join("control.sock"));
                                return Ok(());
                            }
                            Err(error) => eprintln!(
                                "SHUTDOWN_REFUSED: {error:#}; close clients and retry stop"
                            ),
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                #[cfg(not(target_os = "linux"))]
                std::thread::park();
            }
        }
        _ => {}
    }
    let info = discover(&args)?;
    let mut payload = match &args.command {
        #[cfg(target_os = "linux")]
        Command::Stop => json!({"op":"stop"}),
        Command::Status => json!({"op":"status"}),
        Command::Branches => json!({"op":"branches"}),
        Command::Conflicts => json!({"op":"conflicts"}),
        Command::State => json!({"op":"state"}),
        Command::Events => json!({"op":"events"}),
        Command::Sync => json!({"op":"sync"}),
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
fn lock_state(state: &Path) -> Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true).truncate(false).write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        opts.share_mode(0);
    }
    let file = opts
        .open(state.join("owner.lock"))
        .context("STATE_ALREADY_OWNED")?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = std::fs::metadata(state)?;
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() },
            "STATE_OWNER_MISMATCH"
        );
        std::fs::set_permissions(state, std::fs::Permissions::from_mode(0o700))?;
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "STATE_ALREADY_OWNED: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(file)
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn orchestrator_configuration_defaults_to_current_directory_only() {
        let args = Args::try_parse_from(["tkfs", "orchestrator"]).unwrap();
        let Command::Orchestrator { defaults_file } = args.command else {
            panic!("wrong command")
        };
        assert_eq!(defaults_file, PathBuf::from("orchestrator.toml"));
        let args = Args::try_parse_from(["tkfs", "manage", "list"]).unwrap();
        let Command::Manage { defaults_file, .. } = args.command else {
            panic!("wrong command")
        };
        assert_eq!(defaults_file, PathBuf::from("orchestrator.toml"));
    }

    #[test]
    fn explicit_configuration_and_short_alias_preserve_paths_with_spaces() {
        for option in ["--defaults-file", "-f"] {
            let args = Args::try_parse_from([
                "tkfs",
                "manage",
                option,
                "some directory/custom.toml",
                "list",
            ])
            .unwrap();
            let Command::Manage { defaults_file, .. } = args.command else {
                panic!("wrong command")
            };
            assert_eq!(defaults_file, PathBuf::from("some directory/custom.toml"));
            let args = Args::try_parse_from([
                "tkfs",
                "orchestrator",
                option,
                "some directory/custom.toml",
            ])
            .unwrap();
            let Command::Orchestrator { defaults_file } = args.command else {
                panic!("wrong command")
            };
            assert_eq!(defaults_file, PathBuf::from("some directory/custom.toml"));
        }
    }

    #[cfg(windows)]
    #[test]
    fn selected_configuration_rejects_missing_invalid_and_resolves_relative_paths() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("orchestrator.toml");
        let error = tkfs::orchestrator::Config::load(&missing).unwrap_err();
        assert!(format!("{error:#}").contains("ORCHESTRATOR_CONFIG_NOT_FOUND"));
        std::fs::write(&missing, "not valid toml [").unwrap();
        let error = tkfs::orchestrator::Config::load(&missing).unwrap_err();
        assert!(format!("{error:#}").contains("INVALID_ORCHESTRATOR_CONFIG"));
        let explicit = directory.path().join("config with spaces.toml");
        std::fs::write(&explicit, "format_version=1\ndata_directory='data'\n[control]\ntransport='named-pipe'\nname='cli-fixture-only'\n").unwrap();
        let config = tkfs::orchestrator::Config::load(&explicit).unwrap();
        assert_eq!(
            config.data_directory,
            std::fs::canonicalize(directory.path())
                .unwrap()
                .join("data")
        );
    }
}
