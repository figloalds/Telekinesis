use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::path::PathBuf;
use tkfs::{
    orchestrator::{
        catalog::Catalog,
        control,
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
    #[arg(long, global = true)]
    state: Option<PathBuf>,
    /// Desktop configuration (created automatically on first launch).
    #[arg(short = 'f', long = "defaults-file", alias = "config", global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long)]
        repo: Option<String>,
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
    PairingCode,
    Pair {
        endpoint: String,
        code: String,
    },
    Peers,
    Unpair {
        installation: String,
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
fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Gui);
    if matches!(command, Command::Gui) {
        #[cfg(windows)]
        {
            let config = if let Some(config) = cli.config {
                config
            } else if let Some(state) = &cli.state {
                state.join("desktop.toml")
            } else {
                std::env::current_exe()?.with_file_name("orchestrator.toml")
            };
            return gui::run(std::path::absolute(config)?, cli.state);
        }
        #[cfg(not(windows))]
        anyhow::bail!("DESKTOP_REQUIRES_WINDOWS: use --help for headless commands");
    }
    let root = std::path::absolute(cli.state.unwrap_or_else(|| PathBuf::from(".tkfs")))?;
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
        Command::Gui => unreachable!(),
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
                Command::PairingCode => ("pairing-code", json!({})),
                Command::Pair { endpoint, code } => {
                    ("pair", json!({"endpoint":endpoint,"code":code}))
                }
                Command::Peers => ("peers", json!({})),
                Command::Unpair { installation } => {
                    ("unpair", json!({"installation":installation}))
                }
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
            let result = if root.join(worker::RUNTIME_FILE).exists() {
                control::request(&root, operation, payload)?
            } else {
                let engine = Engine::open(&root, None).context("OPEN_FOLDER_FAILED")?;
                let settings = Settings {
                    engine: engine.clone(),
                };
                let result = Catalog::open(engine.clone()).and_then(|catalog| {
                    control::pairing(&catalog, &engine, &settings, operation, &payload)
                        .unwrap_or_else(|| {
                            control::execute(&engine, &settings, operation, &payload)
                        })
                });
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
