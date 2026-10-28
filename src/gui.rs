//! Desktop management over current folder daemons. All I/O stays off the UI thread.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use slint::{ComponentHandle, ModelRc, VecModel};
use std::{path::PathBuf, sync::mpsc, time::Duration};
use tkfs::{core::Conflict, desktop::Desktop, orchestrator::catalog};
slint::include_modules!();

enum Message {
    Select(String),
    Action {
        folder: String,
        operation: String,
        args: [String; 3],
        mount: String,
        pairing_generation: i32,
    },
}
fn model<T: Clone + 'static>(rows: Vec<T>) -> ModelRc<T> {
    ModelRc::new(VecModel::from(rows))
}
fn text(value: &Value, key: &str) -> String {
    value[key].as_str().unwrap_or_default().into()
}
fn optional_path(value: &str) -> Option<PathBuf> {
    (!value.trim().is_empty()).then(|| PathBuf::from(value.trim()))
}
fn shell_open(program: &str, path: &std::path::Path) -> Result<()> {
    use std::os::windows::process::CommandExt;
    std::process::Command::new(program)
        .arg(path)
        .creation_flags(0x08000000)
        .spawn()?;
    Ok(())
}
fn perform(
    desktop: &mut Desktop,
    folder: &str,
    operation: &str,
    args: &[String; 3],
    mount: &str,
) -> Result<(Option<String>, String)> {
    let [a, b, c] = args;
    match operation {
        "create" => {
            let id = desktop.create(a, None, optional_path(b))?;
            Ok((Some(id), "Folder created and started".into()))
        }
        "receive" => {
            let info = catalog::probe(b, c)?;
            let repo = info["repo"]
                .as_str()
                .context("REMOTE_REPOSITORY_REQUIRED")?;
            let id = desktop.create(a, Some(repo), optional_path(mount))?;
            desktop.request(&id, "pair", json!({"endpoint":b,"code":c}))?;
            Ok((
                Some(id),
                "Folder paired; receiving events and content".into(),
            ))
        }
        "start" => {
            desktop.start(folder)?;
            Ok((None, "Folder started".into()))
        }
        "stop" => {
            desktop.stop(folder)?;
            Ok((None, "Folder stopped".into()))
        }
        "restart" => {
            desktop.stop(folder)?;
            desktop.start(folder)?;
            Ok((None, "Folder restarted".into()))
        }
        "attach" | "detach" => {
            desktop.set_mount_enabled(folder, operation == "attach")?;
            Ok((
                None,
                if operation == "attach" {
                    "Folder mounted"
                } else {
                    "Folder unmounted; sync continues"
                }
                .into(),
            ))
        }
        "shutdown-all" => {
            let ids: Vec<_> = desktop
                .config
                .folders
                .iter()
                .filter(|f| f.autostart)
                .map(|f| f.id.clone())
                .collect();
            for id in ids {
                desktop.stop(&id)?;
            }
            Ok((None, "All folders stopped".into()))
        }
        "gc" => {
            let result = desktop.request(folder, "gc", json!({}))?;
            Ok((
                None,
                format!("Collected {} unreferenced objects", result["removed"]),
            ))
        }
        "configure" => {
            ensure!(!b.trim().is_empty(), "LISTEN_ADDRESS_REQUIRED");
            let _: std::net::SocketAddr = b.parse().context("INVALID_LISTEN_ADDRESS")?;
            if !c.trim().is_empty() {
                tkfs::ws::endpoint_uri(c)?;
            }
            desktop.configure(
                folder,
                optional_path(a),
                b.clone(),
                (!c.trim().is_empty()).then(|| c.clone()),
            )?;
            Ok((None, "Folder configuration saved".into()))
        }
        "pair" => {
            let info = catalog::probe(a, b)?;
            let local = desktop.request(folder, "status", json!({}))?;
            ensure!(
                info["repo"] == local["repo"],
                "REPOSITORY_MISMATCH: use Receive folder for a different repository"
            );
            desktop.request(folder, "pair", json!({"endpoint":a,"code":b}))?;
            Ok((None, "Peer paired".into()))
        }
        "open" => {
            let status = desktop.request(folder, "status", json!({}))?;
            shell_open(
                "explorer.exe",
                &PathBuf::from(status["mount"].as_str().context("FOLDER_NOT_MOUNTED")?),
            )?;
            Ok((None, String::new()))
        }
        "log" => {
            shell_open(
                "notepad.exe",
                &desktop.folder(folder)?.state.join("daemon.log"),
            )?;
            Ok((None, String::new()))
        }
        "resolve" => {
            desktop.request(folder, "resolve", json!({"entity":a,"version":b}))?;
            Ok((
                None,
                "Conflict resolved; alternatives retained in history".into(),
            ))
        }
        "unpair" => {
            desktop.request(folder, "unpair", json!({"installation":a}))?;
            Ok((None, "Peer unpaired".into()))
        }
        "fork" => {
            desktop.request(folder, "fork", json!({"name":a}))?;
            Ok((None, "Branch created".into()))
        }
        "checkout" => {
            desktop.request(folder, "checkout", json!({"branch":a}))?;
            Ok((None, "Branch checked out".into()))
        }
        "checkpoint" => {
            let snapshot = desktop.request(folder, "checkpoint", json!({}))?;
            Ok((None, format!("Checkpoint {}", text(&snapshot, "id"))))
        }
        "flush" => {
            desktop.request(folder, "flush", json!({}))?;
            Ok((None, "Local changes flushed".into()))
        }
        _ => anyhow::bail!("UNKNOWN_DESKTOP_ACTION"),
    }
}

include!("gui_adapter.rs");
