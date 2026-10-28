// Adapts the restored WP3.4 layout to the current per-folder daemon API.
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
fn folder_row(row: &Value) -> FolderRow {
    let folder = &row["folder"];
    let status = &row["status"];
    let running = row["online"] == true;
    let error = if running {
        text(status, "error")
    } else {
        text(row, "error")
    };
    let mounted = status["mount"].is_string();
    FolderRow {
        state: text(folder, "id").into(),
        label: text(folder, "label").into(),
        mount: if mounted {
            text(status, "mount")
        } else {
            text(folder, "mount")
        }
        .into(),
        sync: if !running {
            chip("Stopped", "muted", "start", "Start")
        } else if !error.is_empty() {
            chip("Needs attention", "error", "", "")
        } else {
            chip("Sync active", "ok", "", "")
        },
        life: if running {
            chip("Running", "ok", "stop", "Stop")
        } else {
            chip("Stopped", "muted", "start", "Start")
        },
        mount_chip: if mounted {
            chip("Mounted", "ok", "detach", "Unmount")
        } else if folder["mount"].is_string() && running {
            chip("Unmounted", "muted", "attach", "Mount")
        } else {
            chip("Sync only", "muted", "configure-mount", "Configure")
        },
        detail: if running {
            format!(
                "{} entries · branch {}",
                status["entries"].as_u64().unwrap_or(1).saturating_sub(1),
                text(status, "branch")
            )
        } else {
            "Start to sync; configure a mount while stopped.".into()
        }
        .into(),
        conflicts: status["conflicts"].as_i64().unwrap_or(0) as i32,
        error_title: error.clone().into(),
        error_hint: if error.is_empty() {
            ""
        } else {
            "Open the daemon log for details; retry after correcting the problem."
        }
        .into(),
        error_tone: "error".into(),
        attention: !error.is_empty(),
        running,
        primary: if running { "stop" } else { "start" }.into(),
        primary_label: if running { "Stop" } else { "Start" }.into(),
        ..Default::default()
    }
}
fn peer_row(peer: &Value, label: &str) -> PeerRow {
    let runtime = &peer["runtime"];
    let health = &runtime["health"];
    let state = if runtime["state"].is_string() {
        text(runtime, "state")
    } else {
        "Paired".into()
    };
    let online = state == "Connected";
    PeerRow {
        installation: text(peer, "installation").into(),
        name: text(peer, "endpoint").into(),
        endpoint: text(peer, "endpoint").into(),
        badge: chip(
            &state,
            if online {
                "ok"
            } else if runtime["error"].is_string() {
                "warn"
            } else {
                "muted"
            },
            "",
            "",
        ),
        direction: "Two-way".into(),
        protocol: "Current protocol".into(),
        folders: label.into(),
        online,
        rtt: health["rtt_ms"]
            .as_u64()
            .map(|v| format!("{v} ms"))
            .unwrap_or_default()
            .into(),
        rates: if health.is_object() {
            format!(
                "↑ {} · ↓ {} total",
                bytes(health["bytes_up"].as_u64().unwrap_or(0)),
                bytes(health["bytes_down"].as_u64().unwrap_or(0))
            )
        } else {
            String::new()
        }
        .into(),
        error_title: text(runtime, "error").into(),
        error_hint: "Check the peer's endpoint, listener and network connection.".into(),
        lanes_summary: "Metadata and immutable objects share the authenticated session.".into(),
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
fn render(ui: &AppWindow, value: Value, selected: &str) {
    if (!ui.get_selected_state().is_empty() && ui.get_selected_state() != selected)
        || value["selected"] != selected
    {
        return;
    }
    LAST.with(|last| *last.borrow_mut() = value.clone());
    let rows = value["folders"].as_array().cloned().unwrap_or_default();
    let current = rows
        .iter()
        .find(|row| row["folder"]["id"] == selected)
        .cloned()
        .unwrap_or(Value::Null);
    let detail = &value["detail"];
    let online = current["online"] == true;
    let active = rows.iter().filter(|row| row["online"] == true).count();
    let mounted = rows
        .iter()
        .filter(|row| row["status"]["mount"].is_string())
        .count();
    let loaded = detail.is_object() && online;
    ui.set_connected(value["blocked"] != true);
    ui.set_detail_loaded(loaded);
    ui.set_config_path(text(&value, "config_path").into());
    ui.set_host_name(
        std::env::var("COMPUTERNAME")
            .unwrap_or_else(|_| "This computer".into())
            .into(),
    );
    ui.set_daemon_version(env!("CARGO_PKG_VERSION").into());
    ui.set_daemon_status(chip(
        if value["blocked"] == true {
            "Cleanup blocked"
        } else {
            "Ready"
        },
        if value["blocked"] == true {
            "error"
        } else {
            "ok"
        },
        "",
        "",
    ));
    ui.set_daemon_network(
        if online {
            text(&current["status"], "endpoint")
        } else {
            "—".into()
        }
        .into(),
    );
    ui.set_daemon_installation(
        text(&current["status"], "device")
            .chars()
            .take(8)
            .collect::<String>()
            .into(),
    );
    ui.set_daemon_control("Local folder workers".into());
    if value["blocked"] == true {
        ui.set_banner_text("A process did not finish cleanup. Further starts are blocked; inspect desktop-exit-state.json and the saved configuration's blocked.json report.".into());
        ui.set_banner_tone("error".into());
    } else {
        ui.set_banner_text("".into());
        ui.set_banner_action("".into());
    }
    ui.set_folder_count(rows.len() as i32);
    ui.set_stat_folders(active.to_string().into());
    ui.set_stat_folders_detail(format!("of {} saved folders", rows.len()).into());
    ui.set_stat_folders_tone(if active > 0 { "ok" } else { "neutral" }.into());
    ui.set_stat_mounted(mounted.to_string().into());
    ui.set_stat_mounted_detail("Others run without a filesystem mount".into());
    let search = ui.get_search().to_lowercase();
    ui.set_folders(model(
        rows.iter()
            .filter(|row| {
                search.is_empty()
                    || format!(
                        "{} {}",
                        text(&row["folder"], "label"),
                        text(&row["folder"], "mount")
                    )
                    .to_lowercase()
                    .contains(&search)
            })
            .map(folder_row)
            .collect(),
    ));
    ui.set_selected_state(selected.into());
    if ui.get_dialog() != "connect" {
        ui.set_selected_label(text(&current["folder"], "label").into());
    }
    ui.set_detail_row(folder_row(&current));
    if ui.get_dialog() != "host" {
        ui.set_listener(text(&current["folder"], "listen").into());
        ui.set_advertised(text(&current["folder"], "advertise").into());
    }
    let peers = detail["peers"].as_array().cloned().unwrap_or_default();
    let peer_rows: Vec<_> = peers
        .iter()
        .map(|p| peer_row(p, &text(&current["folder"], "label")))
        .collect();
    ui.set_peers(model(peer_rows.clone()));
    ui.set_stat_peers(
        peer_rows
            .iter()
            .filter(|p| p.online)
            .count()
            .to_string()
            .into(),
    );
    ui.set_stat_peers_detail(
        if selected.is_empty() {
            "Select a folder to view its peers".into()
        } else {
            text(&current["folder"], "label")
        }
        .into(),
    );
    let uploaded = peers
        .iter()
        .filter_map(|p| p["runtime"]["health"]["bytes_up"].as_u64())
        .sum();
    let downloaded = peers
        .iter()
        .filter_map(|p| p["runtime"]["health"]["bytes_down"].as_u64())
        .sum();
    ui.set_stat_transfer(format!("↑ {} · ↓ {}", bytes(uploaded), bytes(downloaded)).into());
    ui.set_stat_transfer_detail("Selected folder · session totals".into());
    if let Some(peer) = peer_rows
        .iter()
        .find(|p| p.installation == ui.get_selected_peer() || p.endpoint == ui.get_selected_peer())
    {
        ui.set_peer_detail(peer.clone());
        let data = peers
            .iter()
            .find(|p| p["installation"] == peer.installation.as_str())
            .unwrap();
        let health = &data["runtime"]["health"];
        ui.set_peer_lanes(model(if health.is_object() {
            vec![LaneRow {
                lane: "1".into(),
                role: "Session".into(),
                state: peer.badge.clone(),
                inflight: health["inflight"].to_string().into(),
                bytes: peer.rates.clone(),
                ..Default::default()
            }]
        } else {
            Vec::new()
        }));
    } else {
        ui.set_peer_detail(PeerRow::default());
        ui.set_peer_lanes(model(Vec::new()));
    }
    ui.set_detail_peers(model(
        peers
            .iter()
            .map(|p| {
                let row = peer_row(p, &text(&current["folder"], "label"));
                PeerSyncRow {
                    name: row.name,
                    direction: row.direction,
                    state: row.badge,
                    meta: "Events apply independently of content availability.".into(),
                    down: bytes(p["runtime"]["health"]["bytes_down"].as_u64().unwrap_or(0)).into(),
                    up: bytes(p["runtime"]["health"]["bytes_up"].as_u64().unwrap_or(0)).into(),
                    progress: "Missing content downloads on demand and in the background.".into(),
                    error_title: row.error_title,
                    error_tone: "error".into(),
                    ..Default::default()
                }
            })
            .collect(),
    ));
    let mut overview = vec![
        kv("Store", text(&current["folder"], "state")),
        kv(
            "Configured mount",
            if current["folder"]["mount"].is_string() {
                text(&current["folder"], "mount")
            } else {
                "Sync only".into()
            },
        ),
    ];
    if online {
        overview.extend([
            kv("Repository", text(&current["status"], "repo")),
            kv("Current branch", text(&current["status"], "branch")),
            kv("Peer endpoint", text(&current["status"], "endpoint")),
            kv("Device", text(&current["status"], "device")),
        ]);
    }
    ui.set_detail_overview(model(overview));
    ui.set_detail_activation(model(vec![
        kv("Current branch", text(&current["status"], "branch")),
        kv(
            "Checkpoints",
            detail["snapshots"]
                .as_array()
                .map_or(0, Vec::len)
                .to_string(),
        ),
    ]));
    ui.set_detail_content(model(vec![
        kv("Chunks", "64 KiB"),
        kv("Durability", "Flush/fsync commits locally before returning"),
        kv(
            "Availability",
            "Missing immutable content is fetched from paired peers",
        ),
    ]));
    ui.set_detail_missing_note(
        if online {
            "Open a file or preview a revision to fetch its missing content."
        } else {
            "Start this folder to load its current content state."
        }
        .into(),
    );
    let error = if online {
        text(&current["status"], "error")
    } else {
        text(&current, "error")
    };
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
    ui.set_branches(model(
        detail["branches"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|b| BranchRow {
                id: text(b, "id").into(),
                name: text(b, "name").into(),
            })
            .collect(),
    ));
    ui.set_snapshots(model(
        detail["snapshots"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| s.as_str().map(Into::into))
            .collect(),
    ));
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
    let mut actions = vec![
        ActionData {
            id: if online { "stop" } else { "start" }.into(),
            label: if online { "Stop" } else { "Start" }.into(),
            enabled: true,
            ..Default::default()
        },
        ActionData {
            id: "configure-mount".into(),
            label: "Change mount point".into(),
            enabled: true,
            ..Default::default()
        },
    ];
    if online {
        actions.extend([
            ActionData {
                id: "open".into(),
                label: "Open folder".into(),
                enabled: current["status"]["mount"].is_string(),
                ..Default::default()
            },
            ActionData {
                id: "flush".into(),
                label: "Flush".into(),
                enabled: true,
                ..Default::default()
            },
            ActionData {
                id: "checkpoint".into(),
                label: "Checkpoint".into(),
                enabled: true,
                ..Default::default()
            },
            ActionData {
                id: "restart".into(),
                label: "Restart".into(),
                enabled: true,
                ..Default::default()
            },
        ]);
    }
    ui.set_detail_actions(model(actions));
}
fn refresh_cached(ui: &AppWindow) {
    let cached = LAST.with(|last| last.borrow().clone());
    if !cached.is_null() {
        render(ui, cached, ui.get_selected_state().as_str());
    }
}
fn send(
    tx: &mpsc::SyncSender<Message>,
    ui: &AppWindow,
    folder: String,
    operation: &str,
    args: [String; 3],
    mount: String,
) {
    if tx
        .try_send(Message::Action {
            folder,
            operation: operation.into(),
            args,
            mount,
            pairing_generation: ui.get_pairing_generation(),
        })
        .is_ok()
    {
        ui.set_global_busy(operation.into());
        ui.set_status("".into());
        ui.set_status_error(false);
    } else {
        ui.set_status("Desktop is busy; please retry.".into());
        ui.set_status_error(true);
    }
}
fn wire(ui: &AppWindow, tx: mpsc::SyncSender<Message>) {
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_folder_action(move |id, action| {
        if let Some(ui) = weak.upgrade() {
            ui.set_selected_state(id.clone());
            if action == "configure-mount" {
                let cached = LAST.with(|last| last.borrow().clone());
                if let Some(row) = cached["folders"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|row| row["folder"]["id"] == id.as_str())
                {
                    ui.set_selected_label(text(&row["folder"], "label").into());
                    ui.set_mount_path(text(&row["folder"], "mount").into());
                    ui.set_dialog(
                        if row["online"] == true {
                            "stop-first"
                        } else {
                            "mount"
                        }
                        .into(),
                    );
                }
                let _ = sender.try_send(Message::Select(id.into()));
                return;
            }
            send(
                &sender,
                &ui,
                id.into(),
                action.as_str(),
                Default::default(),
                String::new(),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_open_folder(move |id| {
        if let Some(ui) = weak.upgrade() {
            ui.set_selected_state(id.clone());
            let _ = sender.try_send(Message::Select(id.into()));
        }
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
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_detail_tab_changed(move |_| {
        if let Some(ui) = weak.upgrade() {
            let _ = sender.try_send(Message::Select(ui.get_selected_state().into()));
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_refresh_content(move || {
        if let Some(ui) = weak.upgrade() {
            let _ = sender.try_send(Message::Select(ui.get_selected_state().into()));
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_create_folder(move |label, mount| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                ui.get_selected_state().into(),
                "create",
                [label.into(), mount.into(), String::new()],
                String::new(),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_configure_mount(move |id, mount| {
        if let Some(ui) = weak.upgrade() {
            let cached = LAST.with(|last| last.borrow().clone());
            if let Some(row) = cached["folders"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|row| row["folder"]["id"] == id.as_str())
            {
                send(
                    &sender,
                    &ui,
                    id.into(),
                    "configure",
                    [
                        mount.into(),
                        text(&row["folder"], "listen"),
                        text(&row["folder"], "advertise"),
                    ],
                    String::new(),
                );
            }
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_pair(move |endpoint, code| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                ui.get_selected_state().into(),
                "probe",
                [endpoint.into(), code.into(), String::new()],
                String::new(),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_remote_list(move |endpoint| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                ui.get_selected_state().into(),
                "probe",
                [endpoint.into(), ui.get_code().into(), String::new()],
                String::new(),
            );
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
    ui.on_receive_folder(move |endpoint, _, label, mount| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                ui.get_selected_state().into(),
                "receive",
                [label.into(), endpoint.into(), ui.get_code().into()],
                mount.into(),
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
                ui.get_selected_state().into(),
                "pairing-code",
                Default::default(),
                String::new(),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_unpair(move |endpoint| {
        if let Some(ui) = weak.upgrade() {
            let cached = LAST.with(|last| last.borrow().clone());
            if let Some(peer) = cached["detail"]["peers"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|p| p["endpoint"] == endpoint.as_str())
            {
                send(
                    &sender,
                    &ui,
                    ui.get_selected_state().into(),
                    "unpair",
                    [text(peer, "installation"), String::new(), String::new()],
                    String::new(),
                );
            }
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_shutdown(move || {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                ui.get_selected_state().into(),
                "shutdown-all",
                Default::default(),
                String::new(),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_open_log(move || {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                ui.get_selected_state().into(),
                "log",
                Default::default(),
                String::new(),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_banner_clicked(move |_| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                ui.get_selected_state().into(),
                "retry",
                Default::default(),
                String::new(),
            );
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
                ui.get_selected_state().into(),
                "preview",
                [version.into(), String::new(), String::new()],
                String::new(),
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
                ui.get_selected_state().into(),
                "resolve",
                [
                    ui.get_selected_entity().into(),
                    ui.get_selected_revision().into(),
                    String::new(),
                ],
                String::new(),
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
                ui.get_selected_state().into(),
                "fork",
                [name.into(), String::new(), String::new()],
                String::new(),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx.clone();
    ui.on_checkout_branch(move |branch| {
        if let Some(ui) = weak.upgrade() {
            send(
                &sender,
                &ui,
                ui.get_selected_state().into(),
                "checkout",
                [branch.into(), String::new(), String::new()],
                String::new(),
            );
        }
    });
    let weak = ui.as_weak();
    let sender = tx;
    ui.on_save_listener(move |listener, advertised| {
        if let Some(ui) = weak.upgrade() {
            let cached = LAST.with(|last| last.borrow().clone());
            if let Some(row) = cached["folders"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|row| row["folder"]["id"] == ui.get_selected_state().as_str())
            {
                send(
                    &sender,
                    &ui,
                    ui.get_selected_state().into(),
                    "configure",
                    [
                        text(&row["folder"], "mount"),
                        listener.into(),
                        advertised.into(),
                    ],
                    String::new(),
                );
            }
        }
    });
}
pub fn run(config: PathBuf, single: Option<PathBuf>) -> Result<()> {
    release_own_console();
    let app = AppWindow::new()?;
    let (tx, rx) = mpsc::sync_channel(32);
    wire(&app, tx);
    let weak = app.as_weak();
    std::thread::Builder::new().name("tkfs-desktop".into()).spawn(move || {
        let mut desktop:Option<Desktop>=None;
        let mut selected=String::new();
        loop {
            if desktop.is_none() {
                let opened=std::env::current_exe().map_err(Into::into).and_then(|exe|Desktop::open(&config,single.as_deref(),&exe));
                match opened {
                    Ok(mut value)=>{selected=value.config.folders.first().map(|f|f.id.clone()).unwrap_or_default();value.autostart();desktop=Some(value);let _=weak.upgrade_in_event_loop(|ui|{ui.set_global_busy("".into());ui.set_status("".into());ui.set_status_error(false);});}
                    Err(error)=>{
                        let message=format!("Desktop startup failed: {error:#}");
                        if weak.upgrade_in_event_loop(move |ui|{ui.set_connected(false);ui.set_global_busy("".into());ui.set_banner_text(message.into());ui.set_banner_tone("error".into());ui.set_banner_action("retry".into());ui.set_banner_action_label("Retry startup".into());ui.set_daemon_status(chip("Startup failed","error","",""));}).is_err(){break;}
                        match rx.recv(){Ok(Message::Action{operation,..}) if operation=="retry"=>continue,_=>break}
                    }
                }
            }
            let Some(value)=desktop.as_mut() else {continue};
            let snapshot=value.snapshot(&selected);let selection=selected.clone();
            if weak.upgrade_in_event_loop(move |ui|render(&ui,snapshot,&selection)).is_err(){break;}
            match rx.recv_timeout(Duration::from_millis(750)) {
                Ok(Message::Select(id))=>selected=id,
                Ok(Message::Action{folder,operation,args,mount,pairing_generation})=>{
                    selected=folder.clone();
                    if operation=="probe" {
                        let mut result=if args[1].is_empty(){value.request(&folder,"peer-info",json!({"endpoint":args[0]}))}else{probe(&args[0],&args[1])};
                        if !folder.is_empty() && !args[1].is_empty() && let Ok(info)=&result {
                            let remote_repo=info["repo"].clone();
                            if let Ok(local)=value.request(&folder,"status",json!({})) && local["repo"]==remote_repo
                                && let Err(error)=value.request(&folder,"pair",json!({"endpoint":args[0],"code":args[1]})) {
                                result=Err(error);
                            }
                        }
                        let endpoint=args[0].clone();
                        let _=weak.upgrade_in_event_loop(move |ui|{
                            ui.set_global_busy("".into());
                            if ui.get_endpoint()!=endpoint || ui.get_dialog()!="connect" || ui.get_pairing_generation()!=pairing_generation{return;}
                            match result {
                                Ok(info)=>{ui.set_remote_folders(model(vec![RemoteRow{repo:text(&info,"repo").into(),label:info["label"].as_str().unwrap_or("Remote folder").into()}]));ui.set_remote_message("Authenticated. Select the folder to receive a new local copy. Enter its pairing code when receiving.".into());ui.set_status("Folder discovered".into());}
                                Err(error)=>{ui.set_remote_message(format!("{error:#}").into());ui.set_status_error(true);}
                            }
                        });continue;
                    }
                    if operation=="pairing-code" || operation=="preview" {
                        let preview=operation=="preview";
                        let result=value.request(&folder,if preview{"preview-version"}else{"pairing-code"},if preview{json!({"version":args[0]})}else{json!({})});
                        let _=weak.upgrade_in_event_loop(move |ui|{
                            ui.set_global_busy("".into());
                            match result {
                                Ok(reply)=>{
                                    if preview && ui.get_selected_revision()==args[0] {ui.set_conflict_preview(text(&reply,"preview").into());}
                                    else if !preview && ui.get_dialog()=="host" && ui.get_selected_state()==folder && ui.get_pairing_generation()==pairing_generation {ui.set_pairing_code(text(&reply,"code").into());}
                                }
                                Err(error)=>{ui.set_status(format!("{error:#}").into());ui.set_status_error(true);}
                            }
                        });continue;
                    }
                    let result=perform(value,&folder,&operation,&args,&mount);
                    if let Ok((Some(id),_))=&result {selected=id.clone();}
                    let _=weak.upgrade_in_event_loop(move |ui|{
                        ui.set_global_busy("".into());
                        match result {
                            Ok((new_selection,message))=>{
                                if let Some(id)=new_selection {ui.set_selected_state(id.into());}
                                ui.set_status(message.into());ui.set_status_error(false);
                                if matches!(operation.as_str(),"create"|"receive"|"configure"|"shutdown-all"){ui.set_dialog("".into());}
                                else if operation=="stop" && ui.get_dialog()=="stop-first"{ui.set_dialog("mount".into());}
                                else if operation=="unpair" && ui.get_dialog()=="peer"{ui.set_dialog("".into());}
                            }
                            Err(error)=>{ui.set_status(format!("{error:#}").into());ui.set_status_error(true);}
                        }
                    });
                }
                Err(mpsc::RecvTimeoutError::Disconnected)=>break,
                Err(mpsc::RecvTimeoutError::Timeout)=>{}
            }
        }
    })?;
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
