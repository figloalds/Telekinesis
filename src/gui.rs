//! The desktop is a client of the same folder/peer management API as the CLI.
use anyhow::Result;
use serde_json::{Value, json};
use slint::{ComponentHandle, ModelRc, VecModel};
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    rc::Rc,
    sync::mpsc,
    time::Duration,
};
use tkfs::orchestrator::{self, Action, Config};
slint::include_modules!();

fn path(text: slint::SharedString) -> Option<PathBuf> {
    let text = text.trim();
    (!text.is_empty()).then(|| PathBuf::from(text))
}

fn folder_rows(value: &Value) -> Vec<FolderRow> {
    value["folders"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|p| FolderRow {
            state: p["state"].as_str().unwrap_or_default().into(),
            label: p["label"].as_str().unwrap_or_default().into(),
            mount: p["mount"].as_str().unwrap_or_default().into(),
            running: p["running"] == true,
            starting: p["starting"] == true,
            error: p["error"].as_str().unwrap_or_default().into(),
            sync: sync_label(p).into(),
        })
        .collect()
}

fn filtered_folders(rows: &[FolderRow], query: &str) -> Vec<FolderRow> {
    let query = query.trim().to_lowercase();
    rows.iter()
        .filter(|p| {
            p.label.to_lowercase().contains(&query) || p.mount.to_lowercase().contains(&query)
        })
        .cloned()
        .collect()
}

fn update_folders(ui: &AppWindow, rows: &[FolderRow]) {
    ui.set_folder_count(rows.len() as i32);
    ui.set_mounted_count(
        rows.iter()
            .filter(|p| p.running && !p.mount.is_empty())
            .count() as i32,
    );
    ui.set_folders(ModelRc::new(VecModel::from(filtered_folders(
        rows,
        &ui.get_search(),
    ))));
}

fn peer_rows(value: &Value) -> Vec<PeerRow> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .map(|p| PeerRow {
            endpoint: p["endpoint"].as_str().unwrap_or_default().into(),
            online: p["lanes"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|lane| lane["connected"] == true),
        })
        .collect()
}

fn folder_action(state: &str, payload: Value) -> Action {
    Action::Folder {
        state: state.into(),
        request: tkfs::core::id(),
        payload,
    }
}
fn folder_summary(value: &Value) -> String {
    let s = &value["status"];
    if s["stale"] == true && !s["content"].is_object() {
        return format!(
            "Worker busy; content counts are unavailable in this observation ({}). Peer and lane status continues below.",
            age(&s["sampled_at_ms"])
        );
    }
    let c = &s["content"];
    let peers = s["peers"]
        .as_object()
        .map(|p| {
            p.values()
                .map(|v| {
                    let age =
                        tkfs::core::time().saturating_sub(v["at"].as_u64().unwrap_or(0)) / 1000;
                    format!(
                        "{}: {} ({}s ago)",
                        v["replica"].as_str().unwrap_or("peer"),
                        if age >= 15 {
                            "observation stale"
                        } else if v["metadata_caught_up"] == true {
                            "metadata up to date"
                        } else {
                            "syncing metadata"
                        },
                        age
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    let pending = c["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|v| {
            format!(
                "{}: {} ({} / {} bytes){}",
                v["path"].as_str().unwrap_or("file"),
                v["state"].as_str().unwrap_or("missing"),
                v["downloaded"],
                v["size"],
                v["error"]
                    .as_str()
                    .map(|e| format!(" — {e}"))
                    .unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "{}Metadata: {} | Current objects: {} / {} available | {} downloading\nShared history: {} / {} objects available | Partial data: {} | Requested: {}\nMetadata queued locally: {} | Quarantined: {} | Awaiting acknowledgement: {}\nUnflushed files: {} | Health: {}\n{}\n{}",
        if s["stale"] == true {
            "STALE: worker busy; showing the last observation.\n"
        } else {
            ""
        },
        if s["metadata_caught_up"] == true {
            "up to date"
        } else {
            "pending / awaiting peer observation"
        },
        c["current_available"],
        c["current_objects"],
        c["downloading"],
        c["shared_available"],
        c["shared_objects"],
        bytes(c["partial_bytes"].as_u64().unwrap_or(0)),
        c["requested"],
        s["incoming_pending"],
        s["incoming_quarantined"],
        s["outgoing_unacknowledged"],
        s["unflushed_files"],
        s["health"].as_str().unwrap_or("healthy"),
        peers,
        pending
    )
}

fn bytes(n: u64) -> String {
    if n >= 1024 * 1024 {
        format!("{:.1} MiB", n as f64 / 1048576.0)
    } else if n >= 1024 {
        format!("{:.1} KiB", n as f64 / 1024.0)
    } else {
        format!("{n} B")
    }
}

fn age(at: &Value) -> String {
    at.as_u64()
        .map(|at| format!("{}s ago", tkfs::core::time().saturating_sub(at) / 1000))
        .unwrap_or_else(|| "not observed".into())
}

fn sync_label(project: &Value) -> &'static str {
    if project["error"].is_string() || project["publication_error"].is_string() {
        return "Sync blocked";
    }
    if project["starting"] == true {
        return "Waiting for worker";
    }
    if project["running"] != true {
        return "Sync stopped";
    }
    let peers: Vec<_> = project["sync"].as_array().into_iter().flatten().collect();
    if peers.is_empty() {
        return "No outbound peers";
    }
    if peers.iter().any(|p| p["connected"] != true) {
        return "Peer disconnected";
    }
    if peers.iter().any(|p| p["remote_available"] == false) {
        return "Remote unavailable";
    }
    if peers.iter().any(|p| {
        p["error"].is_string()
            || p["lanes"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|l| l["error"].is_string() || l["repository"]["error"].is_string())
    }) {
        return "Sync retrying";
    }
    if peers.iter().any(|p| p["metadata"].is_null()) {
        return "Awaiting sync status";
    }
    if peers.iter().any(|p| {
        p["metadata"]["last_success_ms"]
            .as_u64()
            .is_none_or(|at| tkfs::core::time().saturating_sub(at) >= 15000)
    }) {
        return "Sync status stale";
    }
    if peers
        .iter()
        .any(|p| p["metadata"]["metadata_caught_up"] != true)
    {
        return "Syncing metadata";
    }
    if peers.iter().any(|p| {
        p["lanes"].as_array().into_iter().flatten().any(|l| {
            let i = &l["repository"]["object_inventory"];
            i["local_only"].as_u64().unwrap_or(0) > 0 || i["remote_only"].as_u64().unwrap_or(0) > 0
        })
    }) {
        return "Syncing objects";
    }
    // Equal inventories cannot prove availability of bytes missing on both peers.
    "Metadata up to date"
}

fn network_summary(project: &Value) -> String {
    let mut lines = vec![sync_label(project).to_owned()];
    for key in ["error", "publication_error", "startup_phase"] {
        if let Some(message) = project[key].as_str() {
            lines.push(message.into());
        }
    }
    for peer in project["sync"].as_array().into_iter().flatten() {
        lines.push(format!(
            "\n{}",
            peer["endpoint"].as_str().unwrap_or("Remote")
        ));
        if peer["connected"] != true {
            lines.push("Control connection offline; retrying automatically.".into());
        } else if peer["remote_available"] == false {
            lines.push("Connected, but this folder is not advertised by the remote. Its worker may be stopped, starting, or unable to publish.".into());
        }
        if let Some(error) = peer["error"].as_str() {
            lines.push(error.into());
        }
        let metadata = &peer["metadata"];
        let progress = &metadata["metadata_progress"];
        if progress.is_object() {
            lines.push(format!("Metadata events: local {} / remote {} · Waiting to commit: local {} / remote {}\nEvents absent from remote: {} · Absent locally: {} · Observation {}",
                progress["local_events"], progress["remote_events"], progress["local_pending"], progress["remote_pending"],
                progress["local_only_events"], progress["remote_only_events"], age(&metadata["last_success_ms"])));
        }
        for lane in peer["lanes"].as_array().into_iter().flatten() {
            let s = &lane["repository"];
            let codec = match lane["transport"].as_str() {
                Some("tkfs-binary-gzip-v1") => "binary + adaptive gzip",
                Some(_) => "legacy JSON",
                None => "negotiating transport",
            };
            lines.push(format!(
                "Lane {} · {} · {} · {}",
                lane["lane"],
                if lane["role"] == "control" {
                    "reserved control"
                } else {
                    "objects"
                },
                if lane["connected"] == true {
                    "connected"
                } else {
                    "offline"
                },
                codec
            ));
            if s.is_object() {
                lines.push(format!(
                    "  Objects sent {} / received {} · Last progress {} · Last success {}",
                    bytes(s["total_sent_object_bytes"].as_u64().unwrap_or(0)),
                    bytes(s["total_received_object_bytes"].as_u64().unwrap_or(0)),
                    age(&s["last_progress_ms"]),
                    age(&s["last_success_ms"])
                ));
                if s["object_inventory"].is_object() {
                    let i = &s["object_inventory"];
                    lines.push(format!("  Available objects: local {} / remote {} · Only local {} / only remote {} · Partial local {} / remote {}",
                        i["local_available"], i["remote_available"], i["local_only"], i["remote_only"], i["local_partial"], i["remote_partial"]));
                }
                if let Some(ms) = s["transfer"]["total_ms"].as_u64() {
                    lines.push(format!("  Last exchange {} ms (prepare {} / remote {} / apply {}) · Wire sent {} / received {}",
                        ms, s["transfer"]["offer_ms"], s["transfer"]["remote_exchange_ms"], s["transfer"]["apply_ms"],
                        bytes(s["total_sent_wire_bytes"].as_u64().unwrap_or(0)), bytes(s["total_received_wire_bytes"].as_u64().unwrap_or(0))));
                }
            }
            if let Some(error) = s["error"].as_str().or_else(|| lane["error"].as_str()) {
                lines.push(format!("  Retry: {error}"));
            }
        }
    }
    lines.push("\nCounters cover successful exchanges since the daemon started or this folder was rediscovered. Object inventories are sampled per lane and can lag receipt by one exchange. Shared history includes older revisions; raw directory file counts can differ from current content.".into());
    lines.join("\n")
}

fn clear_remote(ui: &AppWindow) {
    ui.set_selected_repo("".into());
    ui.set_remote_folders(ModelRc::default());
    ui.set_remote_message("Browse the selected orchestrator to see its shared folders.".into());
}

fn submit(weak: &slint::Weak<AppWindow>, tx: &mpsc::Sender<Action>, action: Action) {
    let Some(ui) = weak.upgrade() else {
        return;
    };
    if ui.get_busy() {
        return;
    }
    let invalid = match &action {
        Action::Create { label, .. } => label.trim().is_empty().then_some("Enter a folder name."),
        Action::Pair { endpoint, code, .. } => {
            if tkfs::ws::endpoint_uri(endpoint).is_err() {
                Some("Enter a WebSocket address, such as ws://host:43180 or wss://host.")
            } else {
                code.is_empty()
                    .then_some("Enter the remote host's pairing code.")
            }
        }
        Action::RemoteList { endpoint }
        | Action::Receive { endpoint, .. }
        | Action::Unpair { endpoint } => tkfs::ws::endpoint_uri(endpoint)
            .is_err()
            .then_some("Enter a WebSocket address, such as ws://host:43180 or wss://host."),
        _ => None,
    };
    if let Some(message) = invalid {
        ui.set_status_error(true);
        ui.set_status(message.into());
        return;
    }
    if matches!(
        action,
        Action::RemoteList { .. } | Action::Pair { .. } | Action::Unpair { .. }
    ) {
        clear_remote(&ui);
    }
    ui.set_status_error(false);
    ui.set_busy(true);
    if tx.send(action).is_err() {
        ui.set_busy(false);
        ui.set_status_error(true);
        ui.set_status(
            "The desktop connection worker has stopped. Reopen the window to reconnect.".into(),
        );
    }
}

pub fn run(config_path: &Path) -> Result<()> {
    let config_path = std::path::absolute(config_path)?;
    if !config_path.exists() {
        Config::initialize(&config_path)?;
    }
    let config = Config::load(&config_path)?;
    let ui = AppWindow::new()?;
    ui.set_config_path(config_path.display().to_string().into());
    ui.set_listen_address(
        config
            .network
            .listen
            .as_deref()
            .unwrap_or("Disabled")
            .into(),
    );
    ui.set_host_name(
        std::env::var("COMPUTERNAME")
            .unwrap_or_else(|_| "This computer".into())
            .into(),
    );
    let watched = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
    let watch_worker = watched.clone();
    let (tx, rx) = mpsc::channel::<Action>();
    // The boolean distinguishes user commands from quiet background snapshots.
    let (results, out) = mpsc::channel::<(Action, bool, Result<Value, String>)>();
    std::thread::spawn(move || {
        if orchestrator::client(&config, &Action::List).is_err() {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .arg("-f")
                .arg(&config_path)
                .arg("orchestrator")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
            if let Err(error) = command.spawn() {
                let _ = results.send((Action::List, false, Err(error.to_string())));
            }
            for _ in 0..50 {
                if orchestrator::client(&config, &Action::List).is_ok() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        let snapshot = || {
            let mut actions = vec![Action::List, Action::Peers];
            if let Some(state) = watch_worker.lock().unwrap().as_ref() {
                actions.push(folder_action(state, json!({"op":"status"})));
            }
            for action in actions {
                let response = orchestrator::client(&config, &action).map_err(|e| format!("{e:#}"));
                if results.send((action, false, response)).is_err() {
                    return false;
                }
            }
            true
        };
        if !snapshot() {
            return;
        }
        let mut stopped = false;
        loop {
            let action = match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(action) => action,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if !stopped && !snapshot() {
                        break;
                    }
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            let response = orchestrator::client(&config, &action).map_err(|e| format!("{e:#}"));
            if matches!(action, Action::Shutdown) && response.is_ok() {
                stopped = true;
            }
            if matches!(action, Action::List) && response.is_ok() {
                stopped = false;
            }
            let refresh_folder = match &action {
                Action::Folder { state, payload, .. }
                    if response.is_ok()
                        && matches!(payload["op"].as_str(), Some("resolve" | "recover")) =>
                {
                    Some(state.clone())
                }
                _ => None,
            };
            let pair_endpoint = match &action {
                Action::Pair { endpoint, .. } if response.is_ok() => Some(endpoint.clone()),
                _ => None,
            };
            if results.send((action, true, response)).is_err() {
                break;
            }
            if let Some(state) = refresh_folder {
                let action = folder_action(&state, json!({"op":"folder-details"}));
                let response = orchestrator::client(&config, &action).map_err(|e| format!("{e:#}"));
                if results.send((action, true, response)).is_err() {
                    break;
                }
            }
            if let Some(endpoint) = pair_endpoint {
                let action = Action::RemoteList { endpoint };
                let response = orchestrator::client(&config, &action).map_err(|e| format!("{e:#}"));
                if results.send((action, true, response)).is_err() {
                    break;
                }
            }
            if !stopped && !snapshot() {
                break;
            }
        }
    });

    let rows = Rc::new(RefCell::new(Vec::<FolderRow>::new()));
    let projects = Rc::new(RefCell::new(Value::Null));
    let cached = rows.clone();
    let weak = ui.as_weak();
    ui.on_search_folders(move |query| {
        if let Some(ui) = weak.upgrade() {
            ui.set_folders(ModelRc::new(VecModel::from(filtered_folders(
                &cached.borrow(),
                &query,
            ))));
        }
    });
    let weak = ui.as_weak();
    ui.on_remote_changed(move || {
        if let Some(ui) = weak.upgrade() {
            clear_remote(&ui);
        }
    });

    macro_rules! handler {
        ($method:ident, || $action:expr) => {{
            let t = tx.clone();
            let weak = ui.as_weak();
            ui.$method(move || submit(&weak, &t, $action));
        }};
        ($method:ident, |$($arg:ident),*| $action:expr) => {{
            let t = tx.clone();
            let weak = ui.as_weak();
            ui.$method(move |$($arg: slint::SharedString),*| submit(&weak, &t, $action));
        }};
    }
    handler!(on_refresh, || Action::List);
    handler!(on_create_folder, |label, mount| Action::Create {
        label: label.trim().into(),
        mount: path(mount),
        headless: false
    });
    handler!(on_start_folder, |state| Action::Start {
        state: state.into()
    });
    handler!(on_stop_folder, |state| Action::Stop {
        state: state.into(),
        wait: None
    });
    handler!(on_configure_mount, |state, mount| Action::ConfigureMount {
        state: state.into(),
        mount: path(mount)
    });
    handler!(on_pair, |endpoint, code| Action::Pair {
        endpoint: endpoint.trim().into(),
        code: code.trim().into(),
        connections: None
    });
    handler!(on_remote_list, |endpoint| Action::RemoteList {
        endpoint: endpoint.trim().into()
    });
    handler!(on_receive_folder, |endpoint, repo, label, mount| {
        Action::Receive {
            endpoint: endpoint.trim().into(),
            repo: repo.into(),
            label: label.into(),
            mount: path(mount),
            headless: false,
        }
    });
    handler!(on_reset_code, || Action::ResetPairingCode);
    handler!(on_unpair, |endpoint| Action::Unpair {
        endpoint: endpoint.trim().into()
    });
    handler!(on_peers, || Action::Peers);
    handler!(on_shutdown, || Action::Shutdown);

    let detail = Rc::new(RefCell::new(Value::Null));
    {
        let weak = ui.as_weak();
        let tx = tx.clone();
        let projects = projects.clone();
        ui.on_folder_details(move |state| {
            if let Some(ui) = weak.upgrade() {
                ui.set_folder_summary("Loading folder state…".into());
                let snapshot = projects.borrow();
                if let Some(project) = snapshot["folders"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|p| p["state"] == state.as_str())
                {
                    ui.set_network_summary(network_summary(project).into());
                    if project["running"] != true {
                        ui.set_folder_summary(
                            "Worker unavailable. Connection and startup status are shown below."
                                .into(),
                        );
                        ui.set_conflict_count(0);
                        ui.set_conflicts(ModelRc::default());
                        return;
                    }
                }
            }
            submit(
                &weak,
                &tx,
                folder_action(&state, json!({"op":"folder-details"})),
            );
        });
    }
    {
        let weak = ui.as_weak();
        let tx = tx.clone();
        ui.on_conflict_page(move |offset| {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            submit(
                &weak,
                &tx,
                folder_action(
                    &ui.get_selected_state(),
                    json!({"op":"folder-details","offset":offset}),
                ),
            );
        });
    }
    {
        let weak = ui.as_weak();
        let detail = detail.clone();
        ui.on_select_conflict(move |id| {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let data = detail.borrow();
            let conflict = data["conflicts"]
                .as_array()
                .and_then(|cs| cs.iter().find(|c| c["id"] == id.as_str()));
            let revisions = conflict
                .into_iter()
                .flat_map(|c| c["alternatives"].as_array().into_iter().flatten())
                .filter(|v| {
                    ui.get_conflict_field() != "namespace"
                        || data["revision_entities"][v["revision"].as_str().unwrap_or_default()]
                            .as_array()
                            .is_some_and(|entities| {
                                entities
                                    .iter()
                                    .any(|e| e == ui.get_selected_entity().as_str())
                            })
                })
                .map(|v| RevisionRow {
                    id: v["revision"].as_str().unwrap_or_default().into(),
                    description: format!(
                        "{} · {} · {}",
                        v["device"].as_str().unwrap_or("unknown"),
                        v["created_ms"],
                        v["revision"].as_str().unwrap_or_default()
                    )
                    .into(),
                })
                .collect::<Vec<_>>();
            ui.set_revisions(ModelRc::new(VecModel::from(revisions)));
            ui.set_conflict_preview(
                conflict
                    .map(|c| {
                        format!(
                            "{}\nCommon bases: {}\nSelect an alternative to inspect it.",
                            c["reason"].as_str().unwrap_or_default(),
                            c["bases"]
                        )
                    })
                    .unwrap_or_default()
                    .into(),
            );
        });
    }
    {
        let weak = ui.as_weak();
        let detail = detail.clone();
        let tx = tx.clone();
        ui.on_preview_revision(move |revision| {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let data = detail.borrow();
            let conflict = data["conflicts"].as_array().and_then(|cs| {
                cs.iter()
                    .find(|c| c["id"] == ui.get_selected_conflict().as_str())
            });
            let Some(c) = conflict else {
                return;
            };
            let Some(v) = c["alternatives"]
                .as_array()
                .and_then(|vs| vs.iter().find(|v| v["revision"] == revision.as_str()))
            else {
                return;
            };
            if c["field"] == "content" {
                ui.set_conflict_preview("Loading and verifying content…".into());
                submit(
                    &weak,
                    &tx,
                    folder_action(
                        &ui.get_selected_state(),
                        json!({"op":"preview-object","object":v["value"]}),
                    ),
                );
            } else {
                ui.set_conflict_preview(serde_json::to_string_pretty(v).unwrap_or_default().into());
            }
        });
    }
    {
        let weak = ui.as_weak();
        let detail = detail.clone();
        let tx = tx.clone();
        ui.on_resolve_conflict(move || {
            let Some(ui) = weak.upgrade() else { return; };
            submit(&weak, &tx, folder_action(&ui.get_selected_state(), json!({"op":"resolve","conflicts":[ui.get_selected_conflict().as_str()],
                "revision":ui.get_selected_revision().as_str(),"review_token":detail.borrow()["review_token"]})));
        });
    }
    {
        let weak = ui.as_weak();
        let detail = detail.clone();
        let tx = tx.clone();
        ui.on_recover_conflict(move |target| {
            let Some(ui) = weak.upgrade() else { return; };
            let data = detail.borrow();
            if !data["conflicts"].as_array().is_some_and(|cs| cs.iter().any(|c| c["id"] == ui.get_selected_conflict().as_str())) { return; }
            // Namespace alternatives carry entry locations; use the selected
            // revision's event to identify the affected entity in the backend.
            submit(&weak, &tx, folder_action(&ui.get_selected_state(), json!({"op":"recover","conflicts":[ui.get_selected_conflict().as_str()],
                "entity":ui.get_selected_entity().as_str(),"revision":ui.get_selected_revision().as_str(),"target":target.as_str(),"review_token":data["review_token"]})));
        });
    }
    let weak = ui.as_weak();
    let timer = slint::Timer::default();
    let mut previous_peers = Vec::new();
    timer.start(slint::TimerMode::Repeated, Duration::from_millis(100), move || {
        let Some(ui) = weak.upgrade() else { return; };
        *watched.lock().unwrap() = (ui.get_dialog() == "folder" && rows.borrow().iter()
            .any(|p| p.state == ui.get_selected_state() && p.running))
            .then(|| ui.get_selected_state().to_string());
        while let Ok((action, interactive, result)) = out.try_recv() {
            if interactive { ui.set_busy(false); }
            match result {
                Err(error) => {
                    if matches!(action, Action::List) { ui.set_connected(false); }
                    if matches!(action, Action::RemoteList { .. }) {
                        clear_remote(&ui);
                        ui.set_remote_message("Unable to load remote folders. Check the address and pairing code.".into());
                    }
                    if interactive || !ui.get_status_error() {
                        ui.set_status_error(true);
                        ui.set_status(error.into());
                    }
                }
                Ok(value) => {
                    let message = match action {
                        Action::Folder { ref state, ref payload, .. } => {
                            if state != ui.get_selected_state().as_str() { continue; }
                            match payload["op"].as_str() {
                                Some("status") => ui.set_folder_summary(folder_summary(&json!({"status":value})).into()),
                                Some("folder-details") => {
                                    ui.set_folder_summary(folder_summary(&value).into());
                                    ui.set_conflict_count(value["conflict_count"].as_i64().unwrap_or(0) as i32);
                                    ui.set_conflict_offset(value["offset"].as_i64().unwrap_or(0) as i32);
                                    let conflicts = value["conflicts"].as_array().into_iter().flatten().filter(|c| c["resolved_by"].is_null()).flat_map(|c| {
                                        c["entity"].as_str().unwrap_or_default().split(',').map(|entity| ConflictRow {
                                            id:c["id"].as_str().unwrap_or_default().into(), entity:entity.into(),
                                            field:c["field"].as_str().unwrap_or_default().into(),
                                            description:format!("{} · {}",value["paths"][entity].as_str().unwrap_or(entity),c["field"].as_str().unwrap_or_default()).into(),
                                        }).collect::<Vec<_>>()
                                    }).collect::<Vec<_>>();
                                    ui.set_conflicts(ModelRc::new(VecModel::from(conflicts)));
                                    ui.set_selected_conflict("".into()); ui.set_selected_revision("".into());
                                    ui.set_revisions(ModelRc::default()); ui.set_conflict_preview("Select a conflict to review its alternatives.".into());
                                    *detail.borrow_mut() = value;
                                }
                                Some("preview-object") => ui.set_conflict_preview(format!("{} bytes{}\n\n{}",value["size"],if value["truncated"] == true { " (preview truncated)" } else { "" },value["text"].as_str().unwrap_or_default()).into()),
                                _ => {}
                            }
                            "Folder details updated."
                        }
                        Action::List => {
                            ui.set_connected(true);
                            if let Some(project) = value["folders"].as_array().into_iter().flatten()
                                .find(|p| p["state"] == ui.get_selected_state().as_str()) {
                                ui.set_network_summary(network_summary(project).into());
                            }
                            *projects.borrow_mut() = value.clone();
                            let next_rows = folder_rows(&value);
                            if *rows.borrow() != next_rows {
                                *rows.borrow_mut() = next_rows;
                                update_folders(&ui, &rows.borrow());
                            }
                            let failures: Vec<_> = rows.borrow().iter().filter(|p| !p.error.is_empty())
                                .map(|p| format!("{}: {}", p.label, p.error)).collect();
                            if !failures.is_empty() && !ui.get_busy() {
                                ui.set_status_error(true);
                                ui.set_status(failures.join("\n").into());
                                continue;
                            }
                            "Folder list refreshed."
                        }
                        Action::Peers => {
                            let peers = peer_rows(&value);
                            ui.set_online_count(peers.iter().filter(|p| p.online).count() as i32);
                            if peers != previous_peers {
                                ui.set_peer_rows(ModelRc::new(VecModel::from(peers.clone())));
                                previous_peers = peers;
                            }
                            "Connection status updated."
                        }
                        Action::RemoteList { ref endpoint } => {
                            if endpoint.trim() != ui.get_endpoint().trim() { continue; }
                            ui.set_selected_repo("".into());
                            let remotes = value.as_array().into_iter().flatten().map(|p| RemoteRow {
                                repo: p["repo"].as_str().unwrap_or_default().into(),
                                label: p["label"].as_str().unwrap_or_default().into(),
                            }).collect::<Vec<_>>();
                            ui.set_remote_message(if remotes.is_empty() { "This host has no shared folders available." } else { "Select a folder to receive on this host." }.into());
                            ui.set_remote_folders(ModelRc::new(VecModel::from(remotes)));
                            "Remote folders loaded."
                        }
                        Action::Create { .. } => { ui.set_dialog("".into()); "Folder created. Starting filesystem…" }
                        Action::Receive { .. } => { ui.set_dialog("".into()); "Remote folder added. Starting filesystem…" }
                        Action::ConfigureMount { .. } => { ui.set_dialog("".into()); "Mount point saved. Start the folder when you're ready." }
                        Action::Start { .. } => "Folder startup requested.",
                        Action::Stop { .. } => {
                            if ui.get_dialog() == "stop-first" { ui.set_dialog("mount".into()); }
                            "Folder stopped."
                        }
                        Action::Pair { .. } => { ui.set_code("".into()); ui.set_busy(true); "Connected. Loading remote folders…" }
                        Action::Unpair { .. } => { clear_remote(&ui); "Orchestrator disconnected. Local folders remain available." }
                        Action::ResetPairingCode => {
                            ui.set_pairing_code(value["code"].as_str().unwrap_or_default().into());
                            "New pairing code generated. Existing peers must pair again."
                        }
                        Action::Shutdown => {
                            ui.set_connected(false);
                            for row in rows.borrow_mut().iter_mut() { row.running = false; }
                            update_folders(&ui, &rows.borrow());
                            ui.set_online_count(0);
                            previous_peers.clear();
                            ui.set_peer_rows(ModelRc::default());
                            ui.set_dialog("".into());
                            "Daemon shut down. Reopen this window to start it again."
                        }
                        _ => "Done.",
                    };
                    if interactive || ui.get_status().starts_with("Connecting") {
                        ui.set_status_error(false);
                        ui.set_status(message.into());
                    }
                }
            }
        }
    });
    ui.run()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn connected_peer_does_not_hide_an_unavailable_folder_or_stale_sync() {
        let mut project =
            json!({"running":true,"sync":[{"connected":true,"remote_available":false}]});
        assert_eq!(sync_label(&project), "Remote unavailable");
        assert!(network_summary(&project).contains("not advertised"));
        project["sync"][0]["remote_available"] = json!(true);
        project["sync"][0]["metadata"] = json!({"metadata_caught_up":true,"last_success_ms":0});
        assert_eq!(sync_label(&project), "Sync status stale");
        project["sync"][0]["metadata"]["last_success_ms"] = json!(tkfs::core::time());
        assert_eq!(sync_label(&project), "Metadata up to date");
        project["sync"][0]["lanes"] = json!([{"repository":{"object_inventory":{"local_only":3}}}]);
        assert_eq!(sync_label(&project), "Syncing objects");
    }

    #[test]
    fn summary_distinguishes_current_content_from_history_and_pending_metadata() {
        let summary = folder_summary(&json!({"status":{"content":{
            "current_available":100,"current_objects":108,"shared_available":108,"shared_objects":291,
            "partial_bytes":2048,"requested":1,"downloading":2},
            "incoming_pending":673,"incoming_quarantined":0,"outgoing_unacknowledged":4}}));
        assert!(summary.contains("Current objects: 100 / 108"));
        assert!(summary.contains("Shared history: 108 / 291"));
        assert!(summary.contains("Metadata queued locally: 673"));
    }
    #[test]
    fn folder_snapshot_preserves_headless_and_failure_states() {
        let rows = folder_rows(&json!({"folders": [
            {"state":"a", "label":"Design Assets", "mount":"C:\\Work\\design", "running":true},
            {"state":"b", "label":"Archive", "mount":null, "running":true},
            {"state":"c", "label":"Failed", "mount":"X:", "running":false, "error":"Mount unavailable"}
        ]}));
        assert_eq!(
            rows.iter()
                .filter(|p| p.running && !p.mount.is_empty())
                .count(),
            1
        );
        assert_eq!(filtered_folders(&rows, " DESIGN ").len(), 1);
        assert_eq!(filtered_folders(&rows, "c:\\work")[0].state, "a");
        assert!(filtered_folders(&rows, "missing").is_empty());
        assert_eq!(rows[2].error, "Mount unavailable");
    }
    #[test]
    fn peer_is_online_when_any_lane_is_connected() {
        let rows = peer_rows(&json!([
            {"endpoint":"ws://one:123", "lanes":[{"connected":false},{"connected":true}]},
            {"endpoint":"ws://two:123", "lanes":[]},
            {"endpoint":"ws://three:123", "lanes":[{"connected":false}]}
        ]));
        assert!(rows[0].online);
        assert!(!rows[1].online);
        assert!(!rows[2].online);
    }
}
