//! `tkfs manage …`: the orchestrator's command-line client.
use super::{config::Config, endpoint::ControlClient};
use anyhow::Result;
use clap::Subcommand;
use serde_json::{Value, json};
use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
    time::Duration,
};

/// How often `watch` refreshes.
pub const WATCH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Subcommand, Debug, Clone)]
pub enum ManagementCommand {
    /// The daemon and every folder.
    Status,
    /// `status` every second until Ctrl-C.
    Watch,
    /// The catalog's folders.
    List,
    /// One folder, with its worker's status while it runs.
    Inspect {
        #[arg(value_name = "STATE")]
        folder: String,
    },
    /// Create a folder and start its worker.
    Create {
        label: String,
        #[arg(long)]
        repo: Option<String>,
        /// Mount the folder here: a directory that does not exist yet
        /// (Windows: or a drive letter such as `X:`).
        #[arg(long, conflicts_with = "headless")]
        mount: Option<PathBuf>,
        #[arg(long)]
        headless: bool,
    },
    /// Set a stopped folder's mount (`--mount`), or clear it without one.
    ConfigureMount {
        #[arg(value_name = "STATE")]
        folder: String,
        /// The new mount point; omitted, the folder runs headless.
        #[arg(long)]
        mount: Option<PathBuf>,
    },
    /// Start a folder's worker.
    Start {
        #[arg(value_name = "STATE")]
        folder: String,
    },
    /// Stop a folder's worker; refused (`BUSY_VIEW`) while files are open.
    Stop {
        #[arg(value_name = "STATE")]
        folder: String,
        /// Return at once; the orchestrator stops it in the background.
        #[arg(long)]
        no_wait: bool,
    },
    /// Stop a folder's worker even with files open; kill it if it hangs.
    ForceStop {
        #[arg(value_name = "STATE")]
        folder: String,
    },
    /// Clear the crash-loop breaker and start.
    Retry {
        #[arg(value_name = "STATE")]
        folder: String,
    },
    /// Pause a folder's sync.
    Pause {
        #[arg(value_name = "STATE")]
        folder: String,
    },
    /// Resume a folder's sync.
    Resume {
        #[arg(value_name = "STATE")]
        folder: String,
    },
    /// Rotate this orchestrator's pairing code: every pairing made with the
    /// old one is revoked. Prints the new code.
    ResetPairingCode,
    /// Pair with the orchestrator at ENDPOINT (`ws://host:port`); the code is
    /// read from stdin unless `--code` is given.
    Pair {
        endpoint: String,
        /// Lanes to this peer (1–16; at least 2 are opened).
        #[arg(long)]
        connections: Option<usize>,
        #[arg(long)]
        code: Option<String>,
    },
    /// Forget a paired installation (and ask it to forget this one).
    Unpair { installation: String },
    /// Paired peers and their lanes.
    Peers,
    /// The folders a paired orchestrator publishes.
    RemoteList { endpoint: String },
    /// Create a folder of REPO, which the paired orchestrator at ENDPOINT
    /// publishes, and start syncing it.
    Receive {
        endpoint: String,
        repo: String,
        label: String,
        /// Mount the folder here: a directory that does not exist yet
        /// (Windows: or a drive letter such as `X:`).
        #[arg(long, conflicts_with = "headless")]
        mount: Option<PathBuf>,
        #[arg(long)]
        headless: bool,
    },
    /// Stop every folder and the orchestrator.
    Shutdown,
}

/// Runs one management command against the orchestrator of `config_path`.
pub fn run(config_path: &Path, command: ManagementCommand, json: bool) -> Result<()> {
    let config = Config::load(config_path)?;
    let mut client = ControlClient::connect(&config)?;
    if matches!(command, ManagementCommand::Watch) {
        loop {
            let snapshot = client.request_read("status", json!({}))?;
            if json {
                println!("{snapshot}");
            } else {
                // Clear the screen, then the status.
                print!("\x1b[2J\x1b[H{}", format_status(&snapshot));
            }
            std::thread::sleep(WATCH_INTERVAL);
        }
    }
    let (action, payload, read) = request(command)?;
    let reply = if read {
        client.request_read(action, payload)?
    } else {
        client.request(action, payload)?
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&reply)?);
    } else if action == "status" {
        print!("{}", format_status(&reply));
    } else if action == "list" {
        for folder in reply.as_array().into_iter().flatten() {
            println!(
                "[{}] {}  {}",
                short(folder["id"].as_str().unwrap_or_default()),
                folder["label"].as_str().unwrap_or_default(),
                folder["state"].as_str().unwrap_or_default()
            );
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&reply)?);
    }
    Ok(())
}

/// The action, payload and whether it is read-only (retried once).
fn request(command: ManagementCommand) -> Result<(&'static str, Value, bool)> {
    use ManagementCommand as C;
    // A bare drive letter (`X:`) is a drive mount on Windows, not the
    // current directory on that drive.
    let absolute = |path: Option<PathBuf>| {
        path.map(|path| match path.to_str().map(str::as_bytes) {
            Some([letter, b':']) if cfg!(windows) && letter.is_ascii_alphabetic() => Ok(path),
            _ => std::path::absolute(path),
        })
        .transpose()
    };
    Ok(match command {
        C::Status | C::Watch => ("status", json!({}), true),
        C::List => ("list", json!({}), true),
        C::Inspect { folder } => ("inspect", json!({"state": folder}), true),
        C::Create {
            label, repo, mount, ..
        } => (
            "create",
            json!({"label": label, "repo": repo, "mount": absolute(mount)?}),
            false,
        ),
        C::ConfigureMount { folder, mount } => (
            "configure-mount",
            json!({"state": folder, "mount": absolute(mount)?}),
            false,
        ),
        C::Start { folder } => ("start", json!({"state": folder}), false),
        C::Stop { folder, no_wait } => ("stop", json!({"state": folder, "wait": !no_wait}), false),
        C::ForceStop { folder } => ("force-stop", json!({"state": folder}), false),
        C::Retry { folder } => ("retry", json!({"state": folder}), false),
        C::Pause { folder } => ("pause", json!({"state": folder}), false),
        C::Resume { folder } => ("resume", json!({"state": folder}), false),
        C::ResetPairingCode => ("reset-pairing-code", json!({}), false),
        C::Pair {
            endpoint,
            connections,
            code,
        } => {
            let code = match code {
                Some(code) => code,
                None => read_code(std::io::stdin().lock())?,
            };
            (
                "pair",
                json!({"endpoint": endpoint, "code": code.trim(), "connections": connections}),
                false,
            )
        }
        C::Unpair { installation } => ("unpair", json!({"installation": installation}), false),
        C::Peers => ("peers", json!({}), true),
        C::RemoteList { endpoint } => ("remote-list", json!({"endpoint": endpoint}), true),
        C::Receive {
            endpoint,
            repo,
            label,
            mount,
            ..
        } => (
            "receive",
            json!({
                "endpoint": endpoint,
                "repo": repo,
                "label": label,
                "mount": absolute(mount)?,
            }),
            false,
        ),
        C::Shutdown => ("shutdown", json!({}), false),
    })
}

/// The pairing code: the first line of `input`.
fn read_code(mut input: impl std::io::BufRead) -> Result<String> {
    let mut line = String::new();
    input.read_line(&mut line)?;
    let code = line.trim();
    anyhow::ensure!(
        !code.is_empty(),
        "PAIRING_CODE_REQUIRED: give the peer's pairing code on stdin or with --code"
    );
    Ok(code.to_owned())
}

fn short(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

/// The daemon line, one line per folder (`[id8] label  phase
/// mount|headless  conflicts N  error`) and the peers section.
pub fn format_status(snapshot: &Value) -> String {
    let daemon = &snapshot["daemon"];
    let mut out = String::new();
    let text = |value: &Value| match value {
        Value::String(text) => text.clone(),
        Value::Null => "-".into(),
        value => value.to_string(),
    };
    let _ = writeln!(
        out,
        "orchestrator pid {}  version {}  control {}  listen {}{}",
        text(&daemon["pid"]),
        text(&daemon["version"]),
        text(&daemon["control"]),
        text(&daemon["listen"]),
        if daemon["stopping"] == true {
            "  STOPPING"
        } else {
            ""
        }
    );
    let folders = snapshot["folders"].as_array().cloned().unwrap_or_default();
    if folders.is_empty() {
        let _ = writeln!(out, "no folders");
    }
    for folder in &folders {
        let record = &folder["record"];
        let mount = match record["mount"].as_str() {
            Some(mount) => mount.to_owned(),
            None => "headless".into(),
        };
        let mut line = format!(
            "[{}] {}  {}  {}  conflicts {}",
            short(record["id"].as_str().unwrap_or_default()),
            record["label"].as_str().unwrap_or_default(),
            folder["phase"]["phase"].as_str().unwrap_or("?"),
            mount,
            folder["conflicts"].as_u64().unwrap_or(0),
        );
        if record["paused"] == true {
            line.push_str("  paused");
        }
        if let Some(error) = folder["error"].as_str() {
            line.push_str("  ");
            line.push_str(error);
        }
        let _ = writeln!(out, "{line}");
    }
    let peers = snapshot["peers"].as_array().cloned().unwrap_or_default();
    let _ = writeln!(out, "peers: {}", peers.len());
    for peer in &peers {
        let _ = writeln!(
            out,
            "  {}  {}",
            text(&peer["endpoint"]),
            text(&peer["installation"])
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_text_has_a_daemon_line_a_line_per_folder_and_peers() {
        let snapshot = json!({
            "daemon": {"pid": 7, "version": "0.1.0", "control": "127.0.0.1:5", "listen": null, "stopping": false},
            "folders": [
                {
                    "record": {"id": "0123456789abcdef", "label": "Project", "mount": null, "paused": false},
                    "phase": {"phase": "Running"},
                    "conflicts": 2,
                    "error": null,
                },
                {
                    "record": {"id": "fedcba9876543210", "label": "Media", "mount": "/m", "paused": true},
                    "phase": {"phase": "Failed", "code": "CRASH_LOOP"},
                    "conflicts": 0,
                    "error": "CRASH_LOOP: 5 failures",
                },
            ],
            "peers": [],
        });
        let text = format_status(&snapshot);
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines[0].starts_with("orchestrator pid 7"), "{text}");
        assert_eq!(
            lines[1],
            "[01234567] Project  Running  headless  conflicts 2"
        );
        assert_eq!(
            lines[2],
            "[fedcba98] Media  Failed  /m  conflicts 0  paused  CRASH_LOOP: 5 failures"
        );
        assert_eq!(lines[3], "peers: 0");
    }

    #[test]
    fn stop_waits_unless_told_not_to_and_reads_are_retried() {
        let (action, payload, read) = request(ManagementCommand::Stop {
            folder: "P".into(),
            no_wait: true,
        })
        .unwrap();
        assert_eq!((action, read), ("stop", false));
        assert_eq!(payload, json!({"state": "P", "wait": false}));
        assert!(request(ManagementCommand::Status).unwrap().2);
        assert!(!request(ManagementCommand::Shutdown).unwrap().2);
    }

    #[test]
    fn pairing_commands_map_to_actions_and_the_code_comes_from_stdin() {
        let (action, payload, read) = request(ManagementCommand::Pair {
            endpoint: "ws://h:1".into(),
            connections: Some(2),
            code: Some(
                " abc 
"
                .into(),
            ),
        })
        .unwrap();
        assert_eq!((action, read), ("pair", false));
        assert_eq!(
            payload,
            json!({"endpoint": "ws://h:1", "code": "abc", "connections": 2})
        );
        assert_eq!(
            read_code(
                &b"c0de
rest"[..]
            )
            .unwrap(),
            "c0de"
        );
        let missing = read_code(&b""[..]).unwrap_err().to_string();
        assert!(missing.starts_with("PAIRING_CODE_REQUIRED"), "{missing}");
        assert_eq!(
            request(ManagementCommand::RemoteList {
                endpoint: "ws://h:1".into()
            })
            .unwrap(),
            ("remote-list", json!({"endpoint": "ws://h:1"}), true)
        );
        assert_eq!(
            request(ManagementCommand::ResetPairingCode).unwrap().0,
            "reset-pairing-code"
        );
        assert_eq!(
            request(ManagementCommand::Receive {
                endpoint: "ws://h:1".into(),
                repo: "r1".into(),
                label: "Shared".into(),
                mount: None,
                headless: true,
            })
            .unwrap(),
            (
                "receive",
                json!({"endpoint": "ws://h:1", "repo": "r1", "label": "Shared", "mount": null}),
                false
            )
        );
    }
}
