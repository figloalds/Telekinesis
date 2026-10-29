// Renders the orchestrator's `status` (hub::Daemon::snapshot) into the
// WP3.4 layout, and wires the window's callbacks to the two threads.
use std::cell::RefCell;
thread_local! { static LAST:RefCell<Value>=const {RefCell::new(Value::Null)}; }
fn chip(label: &str, tone: &str, action: &str, action_label: &str) -> Chip {
    Chip {
        text: label.into(),
        tone: tone.into(),
        action: action.into(),
        action_label: action_label.into(),
    }
}
fn kv(key: &str, value: impl Into<String>) -> KV {
    KV {
        key: key.into(),
        value: value.into().into(),
        tone: "neutral".into(),
    }
}
fn bytes(value: u64) -> String {
    if value >= 1024 * 1024 {
        format!("{:.1} MiB", value as f64 / (1024.0 * 1024.0))
    } else if value >= 1024 {
        format!("{:.1} KiB", value as f64 / 1024.0)
    } else {
        format!("{value} B")
    }
}
/// The first 8 characters of an installation or folder id.
fn short(id: &str) -> String {
    id.chars().take(8).collect()
}
fn plural(count: usize, one: &str, many: &str) -> String {
    format!("{count} {}", if count == 1 { one } else { many })
}
/// A folder entry's phase name (`Running`, `Stopped`, …).
fn phase(entry: &Value) -> &str {
    entry["phase"]["phase"].as_str().unwrap_or_default()
}
/// Whether a folder has (or is getting) a worker: what Stop applies to.
fn active(entry: &Value) -> bool {
    matches!(phase(entry), "Running" | "Starting" | "Backoff")
}
/// Whether the folder's worker serves its mount right now (its heartbeat
/// says so), not merely has one configured.
fn mounted(entry: &Value) -> bool {
    phase(entry) == "Running"
        && entry["heartbeat"]["mounts"]
            .as_array()
            .is_some_and(|mounts| !mounts.is_empty())
}
/// One row of the folder table, from a `status.folders[i]` entry.
fn folder_row(entry: &Value) -> FolderRow {
    let record = &entry["record"];
    let phase = phase(entry);
    let running = phase == "Running";
    let paused = record["paused"] == true;
    let error = text(entry, "error");
    let mount = text(record, "mount");
    let peers = entry["sync"].as_object().map_or(0, |sync| sync.len());
    FolderRow {
        state: text(record, "id").into(),
        label: text(record, "label").into(),
        mount: mount.clone().into(),
        sync: if paused {
            chip("Paused", "warn", "resume", "Resume")
        } else if !error.is_empty() {
            chip("Needs attention", "error", "", "")
        } else if running && peers == 0 {
            chip("Local only", "muted", "", "")
        } else if running {
            chip(
                &format!("Syncing with {}", plural(peers, "peer", "peers")),
                "ok",
                "",
                "",
            )
        } else if phase == "Starting" {
            chip("Starting", "progress", "", "")
        } else {
            chip("Not syncing", "muted", "", "")
        },
        life: match phase {
            "Running" => chip("Running", "ok", "stop", "Stop"),
            "Starting" => chip("Starting", "progress", "", ""),
            "Stopping" => chip("Stopping", "progress", "", ""),
            "Backoff" => chip("Retrying", "warn", "stop", "Stop"),
            "Failed" => chip("Failed", "error", "start", "Start"),
            _ => chip("Stopped", "muted", "start", "Start"),
        },
        mount_chip: if mount.is_empty() {
            chip("Headless", "muted", "", "")
        } else if mounted(entry) {
            chip("Mounted", "ok", "open", "Open")
        } else if phase == "Starting" {
            chip("Mounting", "progress", "", "")
        } else {
            chip("Not mounted", "muted", "configure-mount", "Change")
        },
        detail: if running {
            format!(
                "{} entries",
                entry["heartbeat"]["entries"]
                    .as_u64()
                    .unwrap_or(1)
                    .saturating_sub(1)
            )
        } else if error.is_empty() && phase == "Stopped" {
            "Start to sync.".into()
        } else {
            String::new()
        }
        .into(),
        conflicts: entry["conflicts"].as_i64().unwrap_or(0) as i32,
        error_title: error.clone().into(),
        error_hint: if error.is_empty() {
            ""
        } else {
            "Open the folder's log for details; start it again after correcting the problem."
        }
        .into(),
        error_tone: "error".into(),
        attention: !error.is_empty(),
        running,
        primary: if active(entry) {
            "stop"
        } else if phase == "Stopping" {
            ""
        } else {
            "start"
        }
        .into(),
        primary_label: if active(entry) { "Stop" } else { "Start" }.into(),
        ..Default::default()
    }
}
/// The state of a lane as a chip (`network.peers()[i].lanes[j]`).
fn lane_chip(lane: &Value) -> Chip {
    match lane["state"].as_str().unwrap_or_default() {
        "Ready" => chip("Ready", "ok", "", ""),
        "Dialing" => chip("Connecting", "progress", "", ""),
        "Backoff" => chip("Unreachable", "warn", "", ""),
        "Unauthorized" => chip("Not authorized", "error", "", ""),
        "Incompatible" => chip("Incompatible", "error", "", ""),
        _ => chip("Idle", "muted", "", ""),
    }
}
fn lane_bytes(lanes: &[Value]) -> (u64, u64) {
    lanes.iter().fold((0, 0), |(up, down), lane| {
        let health = &lane["health"];
        (
            up + health["bytes_up"].as_u64().unwrap_or(0),
            down + health["bytes_down"].as_u64().unwrap_or(0),
        )
    })
}
/// One peer of the sidebar, from a `status.peers[i]` entry.
fn peer_row(peer: &Value) -> PeerRow {
    let lanes = peer["lanes"].as_array().cloned().unwrap_or_default();
    let ready = lanes.iter().filter(|lane| lane["state"] == "Ready").count();
    let inbound = peer["inbound"].as_u64().unwrap_or(0) as usize;
    let inbound_only = peer["direction"] == "inbound";
    let endpoint = text(peer, "endpoint");
    let installation = text(peer, "installation");
    let online = ready > 0 || inbound > 0;
    let state = |name: &str| lanes.iter().any(|lane| lane["state"] == name);
    let badge = if online {
        chip("Connected", "ok", "", "")
    } else if state("Unauthorized") {
        chip("Not authorized", "error", "", "")
    } else if state("Incompatible") {
        chip("Incompatible", "error", "", "")
    } else if state("Backoff") {
        chip("Unreachable", "warn", "", "")
    } else if state("Dialing") {
        chip("Connecting", "progress", "", "")
    } else if inbound_only {
        chip("Paired", "muted", "", "")
    } else {
        chip("Offline", "muted", "", "")
    };
    let error = lanes
        .iter()
        .find_map(|lane| lane["error"].as_str().or(lane["last_error"].as_str()))
        .unwrap_or_default();
    let rtt = peer["clock"]["rtt_ms"].as_u64().or_else(|| {
        lanes
            .iter()
            .find_map(|lane| lane["health"]["rtt_ms"].as_u64())
    });
    let (up, down) = lane_bytes(&lanes);
    let mut summary = format!("{} · {} ready", plural(lanes.len(), "lane", "lanes"), ready);
    if inbound > 0 {
        summary += &format!(
            " · {}",
            plural(inbound, "inbound session", "inbound sessions")
        );
    }
    PeerRow {
        installation: installation.clone().into(),
        name: if endpoint.is_empty() {
            format!("Inbound · {}", short(&installation))
        } else {
            endpoint.clone()
        }
        .into(),
        endpoint: endpoint.into(),
        badge,
        direction: if inbound_only {
            "Inbound only"
        } else {
            "Outbound"
        }
        .into(),
        inbound_only,
        online,
        rtt: rtt.map(|ms| format!("{ms} ms")).unwrap_or_default().into(),
        rates: if ready > 0 {
            format!("↑ {} · ↓ {} total", bytes(up), bytes(down))
        } else {
            String::new()
        }
        .into(),
        error_title: error.into(),
        error_hint: if error.is_empty() {
            ""
        } else {
            "Check the peer's endpoint, listener and network connection."
        }
        .into(),
        lanes_summary: summary.into(),
        ..Default::default()
    }
}
fn lane_row(lane: &Value) -> LaneRow {
    let health = &lane["health"];
    LaneRow {
        lane: lane["lane"].to_string().into(),
        role: text(lane, "role").into(),
        state: lane_chip(lane),
        inflight: health["inflight"]
            .as_u64()
            .map(|n| n.to_string())
            .unwrap_or_default()
            .into(),
        bytes: if health.is_object() {
            format!(
                "↑ {} · ↓ {}",
                bytes(health["bytes_up"].as_u64().unwrap_or(0)),
                bytes(health["bytes_down"].as_u64().unwrap_or(0))
            )
        } else {
            String::new()
        }
        .into(),
        error: lane["error"]
            .as_str()
            .or(lane["last_error"].as_str())
            .unwrap_or_default()
            .into(),
        ..Default::default()
    }
}
fn activity(detail: &Value) -> Vec<ActivityRow> {
    detail["events"]
        .as_array()
        .into_iter()
        .flatten()
        .rev()
        .map(|row| {
            let event = &row[1];
            ActivityRow {
                time: format!("Event {}", row[0]).into(),
                kind: text(event, "operation").into(),
                subject: text(&event["state"], "name").into(),
                message: format!("Peer {} · {}", text(event, "author"), text(event, "id")).into(),
                tone: "neutral".into(),
                ..Default::default()
            }
        })
        .collect()
}
fn conflicts(detail: &Value) -> Vec<Conflict> {
    serde_json::from_value(detail["conflicts"].clone()).unwrap_or_default()
}
fn revisions(ui: &AppWindow, detail: &Value) {
    let selected = ui.get_selected_entity();
    let conflict = conflicts(detail)
        .into_iter()
        .find(|c| c.entity == selected.as_str());
    let rows = conflict
        .as_ref()
        .map(|c| {
            c.heads
                .iter()
                .map(|event| RevisionRow {
                    id: event.id.clone().into(),
                    description: format!(
                        "{} · {:?} · {} bytes · {}",
                        event.state.name,
                        event.operation,
                        event.state.content.as_ref().map_or(0, |v| v.total_len),
                        if c.current_heads.contains(&event.id) {
                            "current head"
                        } else {
                            "retained history"
                        }
                    )
                    .into(),
                })
                .collect()
        })
        .unwrap_or_default();
    ui.set_revisions(model(rows));
    ui.set_resolution_enabled(
        ui.get_detail_loaded()
            && conflict.is_some_and(|c| {
                !c.resolved
                    && c.current_heads
                        .contains(ui.get_selected_revision().as_str())
            }),
    );
}
/// The orchestrator is gone (or its status failed): the banner, and with
/// `start-orchestrator` its button. Nothing is started from here.
fn show_banner(ui: &AppWindow, banner: String, action: &str) {
    if action == "start-orchestrator" {
        LAST.with(|last| *last.borrow_mut() = Value::Null);
        ui.set_connected(false);
        ui.set_detail_loaded(false);
        ui.set_daemon_status(chip("Stopped", "error", "", ""));
        ui.set_folders(model(Vec::new()));
        ui.set_folder_count(0);
        ui.set_peers(model(Vec::new()));
        ui.set_daemon_network("—".into());
        for set in [
            AppWindow::set_stat_folders,
            AppWindow::set_stat_mounted,
            AppWindow::set_stat_peers,
            AppWindow::set_stat_transfer,
        ] {
            set(ui, "—".into());
        }
        for set in [
            AppWindow::set_stat_folders_detail,
            AppWindow::set_stat_mounted_detail,
            AppWindow::set_stat_peers_detail,
            AppWindow::set_stat_transfer_detail,
        ] {
            set(ui, "".into());
        }
        ui.set_stat_folders_tone("neutral".into());
    }
    ui.set_banner_text(banner.into());
    ui.set_banner_tone("error".into());
    ui.set_banner_action(action.into());
    ui.set_banner_action_label("Start orchestrator".into());
}
/// The sync tab: one row per peer of the folder's repo (`entry.sync`).
fn sync_rows(entry: &Value, peers: &[Value]) -> Vec<PeerSyncRow> {
    let now = tkfs::util::time::now_ms();
    entry["sync"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(installation, job)| {
            let peer = peers
                .iter()
                .find(|p| p["installation"] == installation.as_str());
            let error = text(job, "error");
            let last = job["last_exchange_ms"].as_u64().unwrap_or(0);
            PeerSyncRow {
                name: peer
                    .map(|p| peer_row(p).name.to_string())
                    .unwrap_or_else(|| short(installation))
                    .into(),
                direction: peer.map(|p| peer_row(p).direction).unwrap_or_default(),
                state: if !error.is_empty() {
                    chip("Error", "error", "", "")
                } else if last > 0 {
                    chip("Exchanging", "ok", "", "")
                } else {
                    chip("Waiting", "muted", "", "")
                },
                meta: "Events apply independently of content availability.".into(),
                counts: format!(
                    "{} sent · {} received",
                    job["sent"].as_u64().unwrap_or(0),
                    job["received"].as_u64().unwrap_or(0)
                )
                .into(),
                progress: if last > 0 {
                    format!("Last exchange {} s ago", now.saturating_sub(last) / 1000)
                } else {
                    "No exchange yet".into()
                }
                .into(),
                error_title: error.into(),
                error_tone: "error".into(),
                ..Default::default()
            }
        })
        .collect()
}
fn action(id: &str, label: &str, enabled: bool) -> ActionData {
    ActionData {
        id: id.into(),
        label: label.into(),
        enabled,
        ..Default::default()
    }
}
/// Renders one poll of the status thread (`{status, selected, detail, …}`).
fn render(ui: &AppWindow, value: Value) {
    LAST.with(|last| *last.borrow_mut() = value.clone());
    let status = &value["status"];
    let daemon = &status["daemon"];
    let selected = ui.get_selected_state();
    let detail = if value["selected"] == selected.as_str() {
        &value["detail"]
    } else {
        &Value::Null
    };
    let rows = status["folders"].as_array().cloned().unwrap_or_default();
    let peers = status["peers"].as_array().cloned().unwrap_or_default();
    let current = rows
        .iter()
        .find(|entry| entry["record"]["id"] == selected.as_str())
        .cloned()
        .unwrap_or(Value::Null);
    let record = &current["record"];
    let running = phase(&current) == "Running";
    let worker = &detail["inspect"]["worker"];
    let loaded = running && worker.is_object();
    let stopping = daemon["stopping"] == true;
    // Host.
    ui.set_connected(true);
    ui.set_detail_loaded(loaded);
    ui.set_config_path(
        daemon["config"]
            .as_str()
            .or(value["config_path"].as_str())
            .unwrap_or_default()
            .into(),
    );
    ui.set_default_mount_directory(text(&value, "default_mount_directory").into());
    ui.set_host_name(
        std::env::var("COMPUTERNAME")
            .unwrap_or_else(|_| "This computer".into())
            .into(),
    );
    ui.set_daemon_version(text(daemon, "version").into());
    ui.set_daemon_status(if stopping {
        chip("Stopping", "warn", "", "")
    } else {
        chip("Running", "ok", "", "")
    });
    ui.set_daemon_network(
        daemon["listen"]
            .as_str()
            .filter(|listen| !listen.is_empty())
            .unwrap_or("not listening")
            .into(),
    );
    ui.set_daemon_installation(short(&text(daemon, "installation")).into());
    if stopping {
        ui.set_banner_text("The orchestrator is stopping…".into());
        ui.set_banner_tone("progress".into());
    } else {
        ui.set_banner_text("".into());
    }
    ui.set_banner_action("".into());
    // Folders.
    let running_count = rows.iter().filter(|e| phase(e) == "Running").count();
    let mounted_count = rows.iter().filter(|e| mounted(e)).count();
    ui.set_folder_count(rows.len() as i32);
    ui.set_stat_folders(running_count.to_string().into());
    ui.set_stat_folders_detail(format!("of {}", plural(rows.len(), "folder", "folders")).into());
    ui.set_stat_folders_tone(if running_count > 0 { "ok" } else { "neutral" }.into());
    ui.set_stat_mounted(mounted_count.to_string().into());
    ui.set_stat_mounted_detail("Others sync headless".into());
    let search = ui.get_search().to_lowercase();
    ui.set_folders(model(
        rows.iter()
            .filter(|entry| {
                search.is_empty()
                    || format!(
                        "{} {}",
                        text(&entry["record"], "label"),
                        text(&entry["record"], "mount")
                    )
                    .to_lowercase()
                    .contains(&search)
            })
            .map(folder_row)
            .collect(),
    ));
    if ui.get_dialog() != "connect" {
        ui.set_selected_label(text(record, "label").into());
    }
    ui.set_detail_row(folder_row(&current));
    // Peers.
    let peer_rows: Vec<_> = peers.iter().map(peer_row).collect();
    let online = peer_rows.iter().filter(|p| p.online).count();
    ui.set_peers(model(peer_rows.clone()));
    ui.set_stat_peers(format!("{online} / {}", peer_rows.len()).into());
    ui.set_stat_peers_detail("Paired hosts".into());
    let (up, down) = peers.iter().fold((0, 0), |(up, down), peer| {
        let lanes = peer["lanes"].as_array().cloned().unwrap_or_default();
        let (u, d) = lane_bytes(&lanes);
        (up + u, down + d)
    });
    ui.set_stat_transfer(format!("↑ {} · ↓ {}", bytes(up), bytes(down)).into());
    ui.set_stat_transfer_detail("Peer sessions · totals".into());
    let selected_peer = ui.get_selected_peer();
    match peers
        .iter()
        .find(|p| p["installation"] == selected_peer.as_str())
    {
        Some(peer) => {
            ui.set_peer_detail(peer_row(peer));
            ui.set_peer_lanes(model(
                peer["lanes"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(lane_row)
                    .collect(),
            ));
        }
        None => {
            ui.set_peer_detail(PeerRow::default());
            ui.set_peer_lanes(model(Vec::new()));
        }
    }
    // The selected folder.
    ui.set_detail_peers(model(sync_rows(&current, &peers)));
    let mut overview = vec![
        kv("Folder", text(record, "id")),
        kv("Store", text(record, "state")),
        kv("Repository", text(record, "repo")),
        kv(
            "Mount",
            if record["mount"].is_string() {
                text(record, "mount")
            } else {
                "Headless".into()
            },
        ),
        kv("Worker", phase(&current)),
        kv(
            "Sync",
            if record["paused"] == true {
                "Paused"
            } else {
                "Active"
            },
        ),
    ];
    if loaded {
        overview.extend([
            kv("Current branch", text(worker, "branch")),
            kv("Device", text(worker, "device")),
            kv(
                "Entries",
                worker["entries"]
                    .as_u64()
                    .unwrap_or(1)
                    .saturating_sub(1)
                    .to_string(),
            ),
        ]);
    }
    ui.set_detail_overview(model(overview));
    ui.set_current_branch(text(worker, "branch").into());
    ui.set_detail_activation(model(vec![kv(
        "Current branch",
        if loaded {
            text(worker, "branch")
        } else {
            "Start the folder to see its branch.".into()
        },
    )]));
    ui.set_detail_content(model(vec![
        kv("Chunks", "64 KiB"),
        kv("Durability", "Flush/fsync commits locally before returning"),
        kv(
            "Availability",
            "Missing immutable content is fetched from paired peers",
        ),
    ]));
    ui.set_detail_missing_note(
        if running {
            "Open a file or preview a revision to fetch its missing content."
        } else {
            "Start this folder to load its current content state."
        }
        .into(),
    );
    let error = text(&current, "error");
    ui.set_detail_errors(model(if error.is_empty() {
        Vec::new()
    } else {
        vec![ErrorRow {
            title: error.into(),
            tone: "error".into(),
            open: true,
            ..Default::default()
        }]
    }));
    ui.set_activity(model(activity(detail)));
    ui.set_detail_activity(model(activity(detail)));
    let history = conflicts(detail);
    let unresolved = history.iter().filter(|c| !c.resolved).count();
    ui.set_conflict_count(history.len() as i32);
    ui.set_conflict_summary(
        if !loaded {
            "Start the folder to load its conflict state.".into()
        } else {
            format!("{unresolved} unresolved entities · reviewed alternatives stay in history")
        }
        .into(),
    );
    let offset = ui.get_conflict_offset().max(0) as usize;
    ui.set_conflicts(model(
        history
            .iter()
            .skip(offset)
            .take(100)
            .map(|c| ConflictRow {
                id: c.entity.clone().into(),
                entity: c.entity.clone().into(),
                field: "entity".into(),
                description: format!(
                    "{} · {}",
                    c.heads
                        .first()
                        .map(|e| e.state.name.as_str())
                        .unwrap_or(&c.entity),
                    if c.resolved { "history" } else { "unresolved" }
                )
                .into(),
            })
            .collect(),
    ));
    revisions(ui, detail);
    let paused = record["paused"] == true;
    let mut actions = vec![
        if active(&current) {
            action("stop", "Stop", true)
        } else {
            action("start", "Start", phase(&current) != "Stopping")
        },
        if paused {
            action("resume", "Resume sync", true)
        } else {
            action("pause", "Pause sync", true)
        },
        action("configure-mount", "Change mount point", true),
        action("open", "Open folder", record["mount"].is_string()),
        action("log", "Open log", true),
    ];
    if running {
        actions.extend([
            action("checkpoint", "Checkpoint", true),
            action("gc", "Clean up storage", true),
            action("restart", "Restart", true),
        ]);
    }
    ui.set_detail_actions(model(actions));
}
fn refresh_cached(ui: &AppWindow) {
    let cached = LAST.with(|last| last.borrow().clone());
    if !cached.is_null() {
        render(ui, cached);
    }
}
/// The cached `status.folders` entry of folder `id`.
fn cached_entry(id: &str) -> Option<Value> {
    LAST.with(|last| {
        last.borrow()["status"]["folders"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|entry| entry["record"]["id"] == id)
            .cloned()
    })
}
fn send(
    tx: &mpsc::SyncSender<Message>,
    ui: &AppWindow,
    operation: &str,
    state: String,
    args: [String; 4],
) {
    let message = Message::Action {
        operation: operation.into(),
        state,
        args,
        headless: ui.get_headless(),
        pairing_generation: ui.get_pairing_generation(),
    };
    if tx.try_send(message).is_ok() {
        ui.set_global_busy(operation.into());
        ui.set_status("".into());
        ui.set_status_error(false);
    } else {
        ui.set_status("The desktop is busy; please retry.".into());
        ui.set_status_error(true);
    }
}
fn args(first: impl Into<String>) -> [String; 4] {
    [first.into(), String::new(), String::new(), String::new()]
}
fn wire(ui: &AppWindow, tx: mpsc::SyncSender<Message>, polls: mpsc::Sender<Poll>) {
    let weak = ui.as_weak();
    let sender = tx.clone();
    let poller = polls.clone();
    ui.on_folder_action(move |id, operation| {
        let Some(ui) = weak.upgrade() else { return };
        ui.set_selected_state(id.clone());
        let _ = poller.send(Poll::Select(id.to_string()));
        if operation == "configure-mount" {
            if let Some(entry) = cached_entry(&id) {
                let label = text(&entry["record"], "label");
                let mount = text(&entry["record"], "mount");
                ui.set_selected_label(label.clone().into());
                // Headless when it has no mount; the default mount shows as
                // the blank field's placeholder rather than as typed text.
                ui.set_headless(mount.is_empty());
                let default = ui.get_default_mount_directory().to_string();
                let is_default = tkfs::desktop::default_mount(Path::new(&default), &label)
                    .is_ok_and(|path| path.as_os_str().eq_ignore_ascii_case(&mount));
                ui.set_mount_path(if is_default { String::new() } else { mount }.into());
                let stopped = matches!(phase(&entry), "Stopped" | "Failed");
                ui.set_dialog(if stopped { "mount" } else { "stop-first" }.into());
            }
            return;
        }
        send(
            &sender,
            &ui,
            operation.as_str(),
            id.into(),
            Default::default(),
        );
    });
    let poller = polls.clone();
    ui.on_open_folder(move |id| {
        let _ = poller.send(Poll::Select(id.into()));
    });
    let weak = ui.as_weak();
    ui.on_open_peer(move |_| {
        if let Some(ui) = weak.upgrade() {
            refresh_cached(&ui);
        }
    });
    let weak = ui.as_weak();
    ui.on_search_folders(move |_| {
        if let Some(ui) = weak.upgrade() {
            refresh_cached(&ui);
        }
    });
    let poller = polls.clone();
    ui.on_detail_tab_changed(move |_| {
        let _ = poller.send(Poll::Refresh);
    });
    let poller = polls;
    ui.on_refresh_content(move || {
        let _ = poller.send(Poll::Refresh);
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_create_folder(move |label, mount, headless| {
        if let Some(ui) = weak.upgrade() {
            ui.set_headless(headless);
            send(
                &sender,
                &ui,
                "create",
                String::new(),
                [label.into(), mount.into(), String::new(), String::new()],
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_configure_mount(move |id, mount, headless| {
        if let Some(ui) = weak.upgrade() {
            ui.set_headless(headless);
            let label = ui.get_selected_label().to_string();
            send(
                &sender,
                &ui,
                "configure-mount",
                id.into(),
                [mount.into(), label, String::new(), String::new()],
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_pair(move |endpoint, code| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                "pair",
                String::new(),
                [endpoint.into(), code.into(), String::new(), String::new()],
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_remote_list(move |endpoint| {
        if let Some(ui) = weak.upgrade() {
            send(&sender, &ui, "remote-list", String::new(), args(endpoint));
        }
    });
    let weak = ui.as_weak();
    ui.on_remote_changed(move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_remote_folders(model(Vec::new()));
            ui.set_selected_repo("".into());
            ui.set_pairing_generation(ui.get_pairing_generation() + 1);
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_receive_folder(move |endpoint, repo, label, mount, headless| {
        if let Some(ui) = weak.upgrade() {
            ui.set_headless(headless);
            send(
                &sender,
                &ui,
                "receive",
                String::new(),
                [endpoint.into(), repo.into(), label.into(), mount.into()],
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_reset_code(move || {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                "reset-code",
                String::new(),
                Default::default(),
            );
        }
    });
    let weak = ui.as_weak();
    ui.on_copy_code(move || {
        let Some(ui) = weak.upgrade() else { return };
        let code = ui.get_pairing_code().to_string();
        let weak = ui.as_weak();
        let copied = std::thread::Builder::new()
            .name("tkfs-clipboard".into())
            .spawn(move || {
                let result = copy_to_clipboard(&code);
                let _ = weak.upgrade_in_event_loop(move |ui| match result {
                    Ok(()) => {
                        ui.set_status("Pairing code copied".into());
                        ui.set_status_error(false);
                    }
                    Err(error) => {
                        ui.set_status(explain(&error).into());
                        ui.set_status_error(true);
                    }
                });
            });
        if let Err(error) = copied {
            ui.set_status(format!("CLIPBOARD_UNAVAILABLE: {error}").into());
            ui.set_status_error(true);
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_unpair(move |installation| {
        if let Some(ui) = weak.upgrade() {
            send(&sender, &ui, "unpair", String::new(), args(installation));
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_shutdown(move || {
        if let Some(ui) = weak.upgrade() {
            send(&sender, &ui, "shutdown", String::new(), Default::default());
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_open_log(move || {
        if let Some(ui) = weak.upgrade() {
            send(&sender, &ui, "host-log", String::new(), Default::default());
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_start_orchestrator(move || {
        let Some(ui) = weak.upgrade() else { return };
        if sender.try_send(Message::StartOrchestrator).is_ok() {
            ui.set_global_busy("Starting the orchestrator".into());
            ui.set_banner_text("Connecting to the orchestrator…".into());
            ui.set_banner_tone("progress".into());
            ui.set_banner_action("".into());
        } else {
            ui.set_status("The desktop is busy; please retry.".into());
            ui.set_status_error(true);
        }
    });
    let weak = ui.as_weak();
    ui.on_dialog_closed(move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_code("".into());
            ui.set_pairing_code("".into());
        }
    });
    let weak = ui.as_weak();
    ui.on_conflict_page(move |offset| {
        if let Some(ui) = weak.upgrade() {
            ui.set_conflict_offset(offset);
            refresh_cached(&ui);
        }
    });
    let weak = ui.as_weak();
    ui.on_select_conflict(move |_| {
        if let Some(ui) = weak.upgrade() {
            ui.set_conflict_preview("Select a revision to preview it.".into());
            let cached = LAST.with(|last| last.borrow().clone());
            revisions(&ui, &cached["detail"]);
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_preview_revision(move |version| {
        if let Some(ui) = weak.upgrade() {
            let cached = LAST.with(|last| last.borrow().clone());
            revisions(&ui, &cached["detail"]);
            send(
                &sender,
                &ui,
                "preview",
                ui.get_selected_state().into(),
                args(version),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_resolve_conflict(move || {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                "resolve",
                ui.get_selected_state().into(),
                [
                    ui.get_selected_entity().into(),
                    ui.get_selected_revision().into(),
                    String::new(),
                    String::new(),
                ],
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_fork_branch(move |name| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                "fork",
                ui.get_selected_state().into(),
                args(name),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx;
    ui.on_checkout_branch(move |branch| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                "checkout",
                ui.get_selected_state().into(),
                args(branch),
            );
        }
    });
}
pub fn run(config: PathBuf) -> Result<()> {
    release_own_console();
    let app = AppWindow::new()?;
    let (tx, messages) = mpsc::sync_channel(32);
    let (polls, poll_rx) = mpsc::channel();
    wire(&app, tx, polls.clone());
    app.set_global_busy("Connecting to the orchestrator".into());
    let weak = app.as_weak();
    std::thread::Builder::new()
        .name("tkfs-status".into())
        .spawn(move || status_loop(weak, poll_rx))?;
    let weak = app.as_weak();
    std::thread::Builder::new()
        .name("tkfs-actions".into())
        .spawn(move || action_loop(weak, messages, polls, config))?;
    app.run()?;
    Ok(())
}
fn release_own_console() {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetConsoleProcessList(list: *mut u32, count: u32) -> u32;
        fn FreeConsole() -> i32;
    }
    let mut ids = [0; 4];
    if unsafe { GetConsoleProcessList(ids.as_mut_ptr(), ids.len() as u32) } == 1 {
        unsafe { FreeConsole() };
    }
}
