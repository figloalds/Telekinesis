use super::{
    config::Config,
    lanes::{lane_role, link_key},
    peering::{dial, remote_list},
    supervisor::Shared,
};
use crate::{pairing_code::Verifier, runtime, ws};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{fs, path::PathBuf, sync::atomic::Ordering};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Action {
    List,
    Inspect {
        state: String,
    },
    Folder {
        state: String,
        request: String,
        payload: Value,
    },
    Create {
        label: String,
        mount: Option<PathBuf>,
        #[serde(default)]
        headless: bool,
    },
    ConfigureMount {
        state: String,
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
    Pair {
        endpoint: String,
        code: String,
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
        mount: Option<PathBuf>,
        #[serde(default)]
        headless: bool,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub state: String,
    pub label: String,
    pub repo: String,
    pub device: String,
    pub directory: PathBuf,
    pub mount: Option<PathBuf>,
    pub desired_running: bool,
}

pub fn client(config: &Config, action: &Action) -> Result<Value> {
    let mut action = action.clone();
    match &mut action {
        Action::Create { mount, .. }
        | Action::Receive { mount, .. }
        | Action::ConfigureMount { mount, .. } => {
            if let Some(path) = mount {
                *path = std::path::absolute(&*path)?;
            }
        }
        _ => {}
    }

    let discovery: Value = serde_json::from_slice(
        &fs::read(config.data_directory.join("orchestrator.json"))
            .context("ORCHESTRATOR_NOT_RUNNING")?,
    )?;
    let mut socket = ws::connect(
        discovery["control"]
            .as_str()
            .context("INVALID_ORCHESTRATOR_DISCOVERY")?,
    )?;
    ws::call(&mut socket, &action)
}

pub fn manage(shared: &Shared, action: Action) -> Result<Value> {
    // Remote I/O runs without holding the lifecycle/catalog lock.
    match action {
        Action::Folder {
            state,
            request,
            payload,
        } => {
            ensure!(
                matches!(
                    payload["op"].as_str(),
                    Some(
                        "status"
                            | "folder-details"
                            | "conflicts"
                            | "preview-object"
                            | "request-object"
                            | "resolve"
                            | "recover"
                            | "branches"
                            | "history"
                            | "checkpoint"
                            | "branch"
                            | "restore"
                    )
                ),
                "UNSUPPORTED_FOLDER_ACTION"
            );
            let info = shared.lock().unwrap().discovery(&state)?;
            return runtime::rpc(&info, &request, payload);
        }
        Action::Pair {
            endpoint,
            code,
            connections,
        } => {
            let (installation, count) = {
                let s = shared.lock().unwrap();
                (
                    s.installation.clone(),
                    connections.unwrap_or(s.config.network.connections),
                )
            };
            ensure!((1..=16).contains(&count), "INVALID_CONNECTION_COUNT");
            let (_, remote) = dial(&endpoint, &code, &installation)?;
            ensure!(remote != installation, "CANNOT_PAIR_WITH_SELF");
            let s = shared.lock().unwrap();
            s.upsert_peer(&endpoint, &code, &remote, count)?;
            return Ok(
                json!({"endpoint":endpoint,"installation":remote,"connections":count.max(2),"configured_connections":count,"reserved_connections":1}),
            );
        }
        Action::RemoteList { endpoint } => return Ok(json!(remote_list(shared, &endpoint)?)),
        Action::Receive {
            endpoint,
            repo,
            label,
            mount,
            headless,
        } => {
            ensure!(
                remote_list(shared, &endpoint)?
                    .iter()
                    .any(|p| p.repo == repo),
                "REMOTE_FOLDER_NOT_FOUND"
            );
            return shared.lock().unwrap().create(
                label,
                Some(repo),
                mount,
                headless,
                Some(&endpoint),
            );
        }
        _ => {}
    }
    let mut s = shared.lock().unwrap();
    ensure!(!s.stopping.load(Ordering::Relaxed), "ORCHESTRATOR_STOPPING");
    match action {
        Action::List => Ok(json!({"installation":s.installation,"folders":s.projects()?.iter().map(|p|s.inspect(&p.state)).collect::<Result<Vec<_>>>()?})),
        Action::Inspect { state } => s.inspect(&state),
        Action::Create { label, mount, headless } => s.create(label, None, mount, headless, None),
        Action::ConfigureMount { state, mount } => {
            ensure!(!s.workers.contains_key(&state), "STOP_FOLDER_BEFORE_CONFIGURING");
            let p = s.project(&state)?;
            let mount = if mount == p.mount { mount } else { mount.map(|p2|s.mount_path(&p.directory,&p2)).transpose()? };
            s.set_mount(&state, mount)?;
            s.inspect(&state)
        }
        Action::Start { state } => {
            s.project(&state)?;
            s.set_desired(&state, true)?;
            s.start(&state)?;
            s.inspect(&state)
        }
        Action::Stop { state } => {
            s.project(&state)?;
            s.stop(&state)?;
            s.set_desired(&state, false)?;
            s.inspect(&state)
        }
        Action::Shutdown => {
            let states: Vec<_> = s.workers.keys().cloned().collect();
            for state in states { s.stop(&state)?; }
            s.stopping.store(true, Ordering::Relaxed);
            Ok(json!({"stopped":true}))
        }
        Action::ResetPairingCode => {
            let (code, verifier) = Verifier::generate();
            s.set_pairing_verifier(&verifier)?;
            Ok(json!({"code":code}))
        }
        Action::Unpair { endpoint } => {
            s.delete_peer(&endpoint)?;
            Ok(json!({"removed":endpoint}))
        }
        Action::Peers => Ok(json!(s.peers()?.iter().map(|p| json!({"endpoint":p.endpoint,"installation":p.installation,"connections":p.pool_size(),"configured_connections":p.connections,"reserved_connections":1,"lanes":(0..p.pool_size()).map(|lane| {
            let mut status = json!(s.links.get(&link_key(p,lane)).cloned().unwrap_or_default());
            status["role"] = json!(lane_role(lane));
            status
        }).collect::<Vec<_>>()})).collect::<Vec<_>>())),
        _ => unreachable!(),
    }
}
