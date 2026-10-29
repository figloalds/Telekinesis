use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::path::PathBuf;
use tkfs::{
    orchestrator::{
        config::{CONFIG_ENV, Config, resolve_path},
        control, hub,
        manage::{self, ManagementCommand},
        settings::Settings,
        worker::{self, EXIT_CONFIG, Fatal},
    },
    runtime::Engine,
};
#[cfg(windows)]
mod gui;

#[derive(Parser)]
#[command(
    version,
    about = "Telekinesis: versioned folders with host filesystem I/O"
)]
struct Cli {
    /// A folder's state directory (folder commands, `daemon`, `init`).
    #[arg(long, global = true)]
    state: Option<PathBuf>,
    /// Orchestrator configuration (`orchestrator.toml`; created by `start`
    /// when missing). Default: `$TKFS_CONFIG`, else next to `tkfs.exe` on
    /// Windows and in the current directory elsewhere.
    #[arg(short = 'f', long = "defaults-file", alias = "config", global = true)]
    config: Option<PathBuf>,
    /// A worker's `runtime.json`: its directory is the folder's state.
    #[arg(long, global = true)]
    runtime: Option<PathBuf>,
    /// Passed with filesystem mutations.
    #[arg(long, global = true)]
    request_id: Option<String>,
    /// Passed with filesystem mutations.
    #[arg(long, global = true)]
    generation: Option<u64>,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    /// Create the configuration (`-f`), or with `--state` a folder store.
    Init {
        #[arg(long)]
        repo: Option<String>,
    },
    /// Run the orchestrator: the catalog, one worker per folder and the
    /// control endpoint.
    #[command(alias = "start")]
    Orchestrator,
    /// Manage folders through the running orchestrator.
    Manage {
        /// Print the raw JSON reply.
        #[arg(long)]
        json: bool,
        #[command(subcommand)]
        command: ManagementCommand,
    },
    /// Create a folder (`manage create`).
    Create {
        label: String,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long, conflicts_with = "headless")]
        mount: Option<PathBuf>,
        #[arg(long)]
        headless: bool,
    },
    /// Run the folder's worker: a loopback session for local clients.
    Daemon {
        #[arg(long)]
        mount: Option<PathBuf>,
        /// Started by a supervisor: exit quietly after a period without
        /// contact (`TKFS_UNSUPERVISED_EXIT_S`, default 600 s).
        #[arg(long)]
        supervised: bool,
    },
    Status,
    Tree,
    Events,
    Conflicts {
        #[arg(long)]
        history: bool,
    },
    Resolve {
        entity: String,
        version: String,
    },
    Checkpoint,
    Branch {
        name: String,
        #[arg(long)]
        snapshot: Option<String>,
    },
    Checkout {
        branch: String,
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
    Import {
        path: String,
        source: PathBuf,
    },
    Cat {
        path: String,
    },
    Flush,
    Gc,
    Stop,
    Gui,
    /// Call a local daemon operation with a JSON payload.
    Rpc {
        operation: String,
        #[arg(default_value = "{}")]
        payload: String,
    },
}
/// Filesystem ops that `--request-id` and `--generation` accompany.
const MUTATIONS: &[&str] = &[
    "resolve",
    "checkpoint",
    "fork",
    "checkout",
    "mkdir",
    "rename",
    "delete",
    "import",
    "gc",
];
fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Gui);
    let config_path = resolve_path(
        cli.config.as_deref(),
        std::env::var_os(CONFIG_ENV).as_deref(),
        &std::env::current_exe()?,
        &std::env::current_dir()?,
    );
    if matches!(command, Command::Gui) {
        #[cfg(windows)]
        return gui::run(config_path, None);
        #[cfg(not(windows))]
        anyhow::bail!("DESKTOP_REQUIRES_WINDOWS: use --help for headless commands");
    }
    match command {
        Command::Orchestrator => return hub::run(&config_path),
        Command::Manage { json, command } => return manage::run(&config_path, command, json),
        Command::Create {
            label,
            repo,
            mount,
            headless,
        } => {
            return manage::run(
                &config_path,
                ManagementCommand::Create {
                    label,
                    repo,
                    mount,
                    headless,
                },
                false,
            );
        }
        Command::Init { repo } if cli.state.is_none() => {
            anyhow::ensure!(
                repo.is_none(),
                "FOLDER_REQUIRED: --repo initializes a folder store: use --state DIR"
            );
            let config = Config::initialize(&config_path)?;
            println!(
                "{}",
                json!({"config": config_path, "data_directory": config.data_directory})
            );
            return Ok(());
        }
        _ => {}
    }
    let root = std::path::absolute(match (cli.state, cli.runtime) {
        (Some(state), _) => state,
        (None, Some(runtime)) => std::path::absolute(&runtime)?
            .parent()
            .context("FOLDER_REQUIRED: --runtime has no parent directory")?
            .to_owned(),
        (None, None) => {
            anyhow::bail!("FOLDER_REQUIRED: use --state DIR or --runtime STATE/runtime.json")
        }
    })?;
    match command {
        Command::Init { repo } => {
            let engine = Engine::open(&root, repo.as_deref())?;
            let result = {
                let store = engine.store.lock().unwrap();
                json!({"device":store.device,"repo":store.repo,"state":root})
            };
            engine.shutdown()?;
            println!("{result}");
            Ok(())
        }
        Command::Daemon { mount, supervised } => {
            if let Err(error) = tkfs::clock::check() {
                worker::exit_fatal(&Fatal::new(
                    "CLOCK_INVALID",
                    format!("{error:#}"),
                    EXIT_CONFIG,
                ));
            }
            let engine = match Engine::open(&root, None) {
                Ok(engine) => engine,
                Err(error) => worker::exit_fatal(&Fatal::store_open(&error)),
            };
            match worker::run(engine, worker::Options { supervised, mount }) {
                Err(error) => match error.downcast::<Fatal>() {
                    Ok(fatal) => worker::exit_fatal(&fatal),
                    Err(error) => Err(error),
                },
                Ok(()) => Ok(()),
            }
        }
        Command::Gui | Command::Orchestrator | Command::Manage { .. } | Command::Create { .. } => {
            unreachable!()
        }
        command => {
            let (operation, payload) = match command {
                Command::Status => ("status", json!({})),
                Command::Tree => ("tree", json!({})),
                Command::Events => ("events", json!({})),
                Command::Conflicts { history } => ("conflicts", json!({"history":history})),
                Command::Resolve { entity, version } => {
                    ("resolve", json!({"entity":entity,"version":version}))
                }
                Command::Checkpoint => ("checkpoint", json!({})),
                Command::Branch { name, snapshot } => {
                    ("fork", json!({"name":name,"snapshot":snapshot}))
                }
                Command::Checkout { branch } => ("checkout", json!({"branch":branch})),
                Command::Mkdir { path } => ("mkdir", json!({"path":path})),
                Command::Rename {
                    path,
                    target,
                    replace,
                } => (
                    "rename",
                    json!({"path":path,"target":target,"replace":replace}),
                ),
                Command::Delete { path } => ("delete", json!({"path":path})),
                Command::Import { path, source } => (
                    "import",
                    json!({"path":path,"source":std::path::absolute(source)?}),
                ),
                Command::Cat { path } => ("cat", json!({"path":path})),
                Command::Flush => ("flush", json!({})),
                Command::Gc => ("gc", json!({})),
                Command::Stop => ("shutdown", json!({})),
                Command::Rpc { operation, payload } => {
                    let payload: Value = serde_json::from_str(&payload)?;
                    let result = control::request(&root, &operation, payload)?;
                    println!("{}", serde_json::to_string_pretty(&result)?);
                    return Ok(());
                }
                _ => unreachable!(),
            };
            let mut payload = payload;
            if MUTATIONS.contains(&operation) {
                if let Some(id) = &cli.request_id {
                    payload["request_id"] = json!(id);
                }
                if let Some(generation) = cli.generation {
                    payload["generation"] = json!(generation);
                }
            }
            let result = if root.join(worker::RUNTIME_FILE).exists() {
                control::request(&root, operation, payload)?
            } else {
                let engine = Engine::open(&root, None).context("OPEN_FOLDER_FAILED")?;
                let settings = Settings {
                    engine: engine.clone(),
                };
                let result = control::execute(&engine, &settings, operation, &payload);
                engine.shutdown()?;
                result?
            };
            if operation == "cat" {
                use std::io::Write;
                let bytes: Vec<u8> = serde_json::from_value(result["bytes"].clone())?;
                std::io::stdout().write_all(&bytes)?;
            } else {
                println!("{}", serde_json::to_string_pretty(&result)?);
            }
            Ok(())
        }
    }
}
