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

#[derive(Parser)]
#[command(
    version,
    about = "TKFS Windows mounted, causal filesystem proof of concept"
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
    /// Foreground O1 management supervisor; networking is disabled.
    Orchestrator {
        #[arg(long)]
        defaults_file: PathBuf,
    },
    /// Client of the same-user management API. Mutations require --generation.
    Manage {
        #[arg(long)]
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
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let args = Args::parse();
    match &args.command {
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
            bail!("O1_WINDOWS_ONLY: {}", defaults_file.display());
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
                #[cfg(windows)]
                tkfs::mount::start(engine.clone(), m)?;
                #[cfg(not(windows))]
                bail!("Windows WinFsp mount only in this PoC");
            }
            println!(
                "{}",
                json!({"ready":true,"repo":info.repo,"device":info.device,"runtime":state.join("runtime.json"),"mount":mount})
            );
            loop {
                std::thread::park();
            }
        }
        _ => {}
    }
    let info = discover(&args)?;
    let mut payload = match &args.command {
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
    opts.open(state.join("owner.lock"))
        .context("STATE_ALREADY_OWNED")
}
