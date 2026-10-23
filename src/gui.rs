use anyhow::Result;
use slint::{ComponentHandle, ModelRc, VecModel};
use std::{path::PathBuf, time::Duration};
use tkfs::{core::Conflict, orchestrator::control};
slint::include_modules!();

pub fn run(root: PathBuf) -> Result<()> {
    let app = App::new()?;
    let weak = app.as_weak();
    let state = root.clone();
    app.on_resolve(move |entity, version| {
        let state = state.clone();
        let weak = weak.clone();
        std::thread::spawn(move || {
            if let Err(error) = control::request(
                &state,
                "resolve",
                serde_json::json!({"entity":entity.as_str(),"version":version.as_str()}),
            ) {
                let message = format!("{error:#}");
                let _ = weak.upgrade_in_event_loop(move |ui| ui.set_error(message.into()));
            }
        });
    });
    let weak = app.as_weak();
    std::thread::spawn(move || {
        loop {
            let result = (|| -> Result<_> {
                let status = control::request(&root, "status", serde_json::json!({}))?;
                let conflicts: Vec<Conflict> = serde_json::from_value(control::request(
                    &root,
                    "conflicts",
                    serde_json::json!({"history":true}),
                )?)?;
                Ok((status, conflicts))
            })();
            let alive = weak.upgrade_in_event_loop(move |ui| match result {
                Ok((status, conflicts)) => {
                    ui.set_status(
                        format!(
                            "Branch {} · {} entries · {} conflicts",
                            status["branch"].as_str().unwrap_or(""),
                            status["entries"],
                            status["conflicts"]
                        )
                        .into(),
                    );
                    let rows = conflicts
                        .into_iter()
                        .flat_map(|conflict| {
                            let resolved = conflict.resolved;
                            conflict.heads.into_iter().map(move |event| VersionRow {
                                entity: conflict.entity.clone().into(),
                                version: event.id.into(),
                                author: format!("Peer {}", event.author).into(),
                                description: format!(
                                    "{} · {:?} · {} bytes",
                                    event.state.name,
                                    event.operation,
                                    event
                                        .state
                                        .content
                                        .as_ref()
                                        .map(|c| c.total_len)
                                        .unwrap_or(0)
                                )
                                .into(),
                                resolved,
                            })
                        })
                        .collect::<Vec<_>>();
                    ui.set_versions(ModelRc::new(VecModel::from(rows)));
                    ui.set_error("".into());
                }
                Err(error) => ui.set_error(format!("{error:#}").into()),
            });
            if alive.is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    });
    app.run()?;
    Ok(())
}
