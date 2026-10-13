use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;
use tkfs::{
    core::{Store, id},
    orchestrator::{self, Action, Config},
    runtime::{self, Discovery},
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
    Manage {
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
#[derive(Subcommand)]
enum ManagementCommand {
    List,
    Inspect {
        state: String,
    },
    Create {
        label: String,
        #[arg(long)]
        mount: Option<PathBuf>,
        #[arg(long)]
        headless: bool,
    },
    ConfigureMount {
        state: String,
        #[arg(long)]
        mount: Option<PathBuf>,
    },
    Start {
        state: String,
    },
    Stop {
        state: String,
    },
    Shutdown,
    ResetPairingCode,
    /// Pair to ws://host:port or wss://host. Reads the remote code from stdin.
    Pair {
        endpoint: String,
        #[arg(long)]
        connections: Option<usize>,
    },
    Unpair {
        endpoint: String,
    },
    Peers,
    RemoteList {
        endpoint: String,
    },
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
fn management(action: ManagementCommand) -> Result<Action> {
    Ok(match action {
        ManagementCommand::List => Action::List,
        ManagementCommand::Inspect { state } => Action::Inspect { state },
        ManagementCommand::Create {
            label,
            mount,
            headless,
        } => Action::Create {
            label,
            mount,
            headless,
        },
        ManagementCommand::ConfigureMount { state, mount } => {
            Action::ConfigureMount { state, mount }
        }
        ManagementCommand::Start { state } => Action::Start { state },
        ManagementCommand::Stop { state } => Action::Stop { state, wait: None },
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
    })
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
        Command::Manage { action } => {
            return print(orchestrator::client(
                &Config::load(&args.defaults_file)?,
                &management(action)?,
            )?);
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
