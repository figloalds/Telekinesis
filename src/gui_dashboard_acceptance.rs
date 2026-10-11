//! Native rendered acceptance with synthetic view data only. No credentials,
//! workers, mounts, listeners, or filesystem mutations other than evidence.
use super::{App, View, apply, filter_projects, snapshot};
use anyhow::{Result, ensure};
use slint::{ComponentHandle, Model};
use std::path::Path;

pub fn tick(ui: &App, directory: &Path, step: &mut i32, checks: &mut Vec<String>) -> Result<bool> {
    match *step {
        0 => {
            ui.set_first_run(true);
            ui.set_connection("Synthetic fixture · no supervisor started".into());
        }
        1 => {
            snapshot(ui, &directory.join("first-run.bmp"))?;
            apply(
                ui,
                View {
                    connected: true,
                    connection: "Synthetic fixture · local supervisor observation".into(),
                    selected: 0,
                    status:
                        "Running\n\nMount folder\nC:\\Synthetic\\Design\n\nDesired state: Running"
                            .into(),
                    rows: vec![
                        (
                            "Design assets".into(),
                            "synthetic-design".into(),
                            "Running".into(),
                            "C:\\Synthetic\\Design".into(),
                            true,
                        ),
                        (
                            "Team documents".into(),
                            "synthetic-docs".into(),
                            "Stopped".into(),
                            "C:\\Synthetic\\Documents".into(),
                            false,
                        ),
                        (
                            "Photo archive".into(),
                            "synthetic-photos".into(),
                            "Unavailable".into(),
                            "C:\\Synthetic\\Photos".into(),
                            true,
                        ),
                    ],
                    ..View::default()
                },
            );
            ensure!(ui.get_visible_projects().row_count() == 3, "DASHBOARD_ROWS");
            ensure!(
                ui.get_running_count() == 1 && ui.get_attention_count() == 1,
                "OBSERVED_COUNTERS"
            );
        }
        2 => {
            snapshot(ui, &directory.join("dashboard.bmp"))?;
            ui.set_project_filter("DOCUMENTS".into());
            filter_projects(ui, "DOCUMENTS");
            ensure!(
                ui.get_visible_projects().row_data(0) == Some(1),
                "SEARCH_PRESERVES_SOURCE_INDEX"
            );
            ensure!(
                ui.get_visible_projects().row_count() == 1,
                "SEARCH_CASE_INSENSITIVE"
            );
        }
        3 => {
            snapshot(ui, &directory.join("search.bmp"))?;
            filter_projects(ui, "unmatched fixture");
            ensure!(ui.get_visible_projects().row_count() == 0, "EMPTY_SEARCH");
        }
        4 => {
            snapshot(ui, &directory.join("empty-search.bmp"))?;
            ui.set_project_filter("".into());
            filter_projects(ui, "");
            ui.set_main_page(3);
        }
        5 => {
            snapshot(ui, &directory.join("details.bmp"))?;
            ui.set_working(true);
            // Exercise the actual callback guards while a request is pending.
            // enqueue must reject these before the controller sees any job.
            ui.invoke_start_project();
            ui.invoke_start_project();
            ui.invoke_pair_action(12);
            ensure!(
                !ui.get_can_start_project() && !ui.get_can_stop_project(),
                "PENDING_GATES_REPEAT_ACTIONS"
            );
            ui.set_connection("Refreshing… synthetic pending operation".into());
        }
        6 => {
            snapshot(ui, &directory.join("pending.bmp"))?;
            ui.set_working(false);
            ui.set_connected(false);
            ui.set_connection(
                "Connection interrupted · status may be stale · reconnect to refresh".into(),
            );
            ui.set_main_page(0);
            ensure!(
                !ui.get_can_stop_project() && !ui.get_can_branch_actions(),
                "STALE_ACTIONS_DISABLED"
            );
        }
        7 => {
            snapshot(ui, &directory.join("stale.bmp"))?;
            ui.set_config_input("C:\\Synthetic\\orchestrator.toml".into());
            ui.set_config_visible(true);
        }
        8 => {
            snapshot(ui, &directory.join("configuration.bmp"))?;
            ui.set_config_visible(false);
            ui.set_create_visible(true);
            ui.invoke_focus_project_name();
            ensure!(ui.get_project_name_focused(), "KEYBOARD_FOCUS");
            ui.window()
                .try_dispatch_event(slint::platform::WindowEvent::KeyPressed {
                    text: slint::platform::Key::Tab.into(),
                })?;
            ui.window()
                .try_dispatch_event(slint::platform::WindowEvent::KeyReleased {
                    text: slint::platform::Key::Tab.into(),
                })?;
            ensure!(!ui.get_project_name_focused(), "TAB_TRAVERSAL");
            ui.set_create_visible(false);
            ui.set_main_page(1);
        }
        9 => {
            snapshot(ui, &directory.join("pairing-empty.bmp"))?;
            ui.set_pairing_page(2);
        }
        10 => {
            snapshot(ui, &directory.join("server-reset.bmp"))?;
            // A fixed noncredential exercises erasure and the evidence barrier.
            // It is never generated, enrolled, persisted, or captured.
            ui.set_server_code("0".repeat(200).into());
            ui.set_server_code_visible(true);
            ensure!(
                snapshot(ui, &directory.join("must-not-exist.bmp")).is_err(),
                "SECRET_EVIDENCE_BARRIER"
            );
            ui.invoke_dismiss_server_code();
            ensure!(
                ui.get_server_code().is_empty() && !ui.get_server_code_visible(),
                "SHOW_ONCE_ERASURE"
            );
            ui.set_main_page(0);
            ui.window().set_size(slint::LogicalSize::new(980.0, 720.0));
        }
        11 => {
            snapshot(ui, &directory.join("minimum-size.bmp"))?;
            checks.push("Synthetic native dashboard, first run, configuration, search, details, pending/stale and pairing rendered without production I/O".into());
            checks.push("Search preserves source indices; observed counters, repeat-action gates, native focus and Tab traversal verified".into());
            checks.push("Show-once dismissal erases transient field; screenshot refuses nonempty code; no reset or enrollment executed".into());
            return Ok(true);
        }
        _ => unreachable!(),
    }
    *step += 1;
    Ok(false)
}
