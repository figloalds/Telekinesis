use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::path::PathBuf;
use tkfs::{
    orchestrator::{self, catalog::Catalog, control},
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
    #[arg(long, global = true, default_value = ".tkfs")]
    state: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long)]
        repo: Option<String>,
    },
    Daemon {
        #[arg(long)]
        mount: Option<PathBuf>,
        #[arg(long, default_value = "127.0.0.1:43180")]
        listen: String,
        #[arg(long)]
        advertise: Option<String>,
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
    let root = std::path::absolute(&cli.state)?;
    match cli.command {
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
        Command::Daemon {
            mount,
            listen,
            advertise,
        } => {
            let engine = Engine::open(&root, None)?;
            if let Some(path) = mount {
                #[cfg(any(windows, target_os = "linux"))]
                tkfs::mount::attach(engine.clone(), &path)?;
            }
            orchestrator::run(engine, &listen, advertise.as_deref())
        }
        Command::Gui => {
            #[cfg(windows)]
            return gui::run(root);
            #[cfg(not(windows))]
            anyhow::bail!("DESKTOP_REQUIRES_WINDOWS");
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
            let result = if root.join("control.json").exists() {
                control::request(&root, operation, payload)?
            } else {
                let engine = Engine::open(&root, None).context("OPEN_FOLDER_FAILED")?;
                let catalog = Catalog::open(engine.clone())?;
                let result = control::execute(&engine, &catalog, operation, &payload);
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
