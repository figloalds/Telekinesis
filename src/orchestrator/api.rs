use super::{
    config::Config,
    lanes::{lane_role, link_key},
    peering::{dial, remote_list},
    supervisor::Shared,
};
use crate::{pairing_code::Verifier, runtime, ws};
use anyhow::{Context, Result, bail, ensure};
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
        /// `false` returns at once instead of waiting for `Stopped|StopBlocked`
        /// (§3.5). The legacy supervisor always stops synchronously. Omitted
        /// when `None`, so a stop request still parses on a 6298d0b daemon,
        /// whose `Action` denies unknown fields.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wait: Option<bool>,
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
    // Management API v2 (§3.5, §14 WP2.2). Frozen by WP0.INT; the legacy
    // `manage` answers each of them with NOT_IMPLEMENTED.
    Status,
    Subscribe {
        since: u64,
        #[serde(default)]
        wait_ms: Option<u32>,
    },
    Attach {
        state: String,
    },
    Detach {
        state: String,
        #[serde(default)]
        force: bool,
    },
    PauseSync {
        state: String,
    },
    ResumeSync {
        state: String,
    },
    OverrideGate {
        state: String,
    },
    ForceStop {
        state: String,
    },
    Retry {
        state: String,
    },
    Revalidate {
        state: String,
        #[serde(default)]
        ids: Option<Vec<String>>,
    },
    Diagnostics,
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
            let info = crate::util::lock::lock(shared).discovery(&state)?;
            return runtime::rpc(&info, &request, payload);
        }
        Action::Pair {
            endpoint,
            code,
            connections,
        } => {
            let (installation, count) = {
                let s = crate::util::lock::lock(shared);
                (
                    s.installation.clone(),
                    connections.unwrap_or(s.config.network.connections),
                )
            };
            ensure!((1..=16).contains(&count), "INVALID_CONNECTION_COUNT");
            let (_, remote) = dial(&endpoint, &code, &installation)?;
            ensure!(remote != installation, "CANNOT_PAIR_WITH_SELF");
            let s = crate::util::lock::lock(shared);
            s.upsert_peer(&endpoint, &code, &remote, count)?;
            return Ok(
                json!({"endpoint":endpoint,"installation":remote,"connections":count.max(2),"configured_connections":count,"reserved_connections":1}),
            );
        }
        Action::RemoteList { endpoint } => return Ok(json!(remote_list(shared, &endpoint)?)),
        // Management API v2 (WP2.1/WP2.2) is not served by the legacy
        // supervisor; answer without taking the lifecycle lock.
        v2 @ (Action::Status
        | Action::Subscribe { .. }
        | Action::Attach { .. }
        | Action::Detach { .. }
        | Action::PauseSync { .. }
        | Action::ResumeSync { .. }
        | Action::OverrideGate { .. }
        | Action::ForceStop { .. }
        | Action::Retry { .. }
        | Action::Revalidate { .. }
        | Action::Diagnostics) => {
            let op = serde_json::to_value(&v2)?["op"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            bail!("NOT_IMPLEMENTED: {op} is not served by the legacy orchestrator");
        }
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
            return crate::util::lock::lock(shared).create(
                label,
                Some(repo),
                mount,
                headless,
                Some(&endpoint),
            );
        }
        _ => {}
    }
    let mut s = crate::util::lock::lock(shared);
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
        Action::Stop { state, wait: _ } => {
            s.project(&state)?;
            s.stop(&state)?;
            s.set_desired(&state, false)?;
            s.inspect(&state)
        }
        Action::Shutdown => {
            // `stopping` first: lanes, inbound requests and reconcile wind down
            // while folders stop. A folder that fails to stop never aborts the
            // others; it is left running and reported (WP1.12).
            s.stopping.store(true, Ordering::Relaxed);
            let states: Vec<_> = s.workers.keys().cloned().collect();
            let folders: Vec<_> = states.iter().map(|state| s.shutdown_folder(state)).collect();
            Ok(json!({"stopped":true,"folders":folders}))
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
        // Answered above, before the lifecycle lock is taken. Explicit arms
        // (no `unreachable!`): a request can never panic the control thread.
        Action::Folder { .. }
        | Action::Pair { .. }
        | Action::RemoteList { .. }
        | Action::Receive { .. }
        | Action::Status
        | Action::Subscribe { .. }
        | Action::Attach { .. }
        | Action::Detach { .. }
        | Action::PauseSync { .. }
        | Action::ResumeSync { .. }
        | Action::OverrideGate { .. }
        | Action::ForceStop { .. }
        | Action::Retry { .. }
        | Action::Revalidate { .. }
        | Action::Diagnostics => bail!("UNSUPPORTED_OPERATION: action already dispatched"),
    }
}
