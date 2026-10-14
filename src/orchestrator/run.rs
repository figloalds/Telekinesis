use super::{
    api::manage,
    config::Config,
    lanes::{link_key, run_lane},
    peering::serve_peer,
    supervisor::Supervisor,
};
use crate::{
    util::{lock::lock, log::event},
    ws,
};
use anyhow::Result;
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};

/// Supervision cadence: worker exits and Ready lines are noticed within this,
/// and backed-off restarts (1 s → 60 s) run when due (WP1.12).
const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);

pub fn run(config: Config) -> Result<()> {
    let supervisor = Supervisor::open(config)?;
    let stop = supervisor.stopping.clone();
    let control = TcpListener::bind("127.0.0.1:0")?;
    control.set_nonblocking(true)?;
    let network = supervisor
        .config
        .network
        .listen
        .as_ref()
        .map(TcpListener::bind)
        .transpose()?;
    if let Some(listener) = &network {
        listener.set_nonblocking(true)?;
    }
    let discovery = json!({"format":2,"installation":supervisor.installation,"control":format!("ws://{}",control.local_addr()?),"network":network.as_ref().map(|l|l.local_addr().map(|a|format!("ws://{a}"))).transpose()?});
    let discovery_path = supervisor.config.data_directory.join("orchestrator.json");
    fs::write(&discovery_path, serde_json::to_vec_pretty(&discovery)?)?;
    let shared = Arc::new(Mutex::new(supervisor));
    println!("{discovery}");
    let mut lanes = BTreeMap::<String, std::thread::JoinHandle<()>>::new();
    let mut controls = Vec::new();
    let mut reconcile = Instant::now();
    // The last peer list read from the catalog. A failed read keeps it: one
    // SQLite error must not end the daemon and orphan its workers.
    let mut peers = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        if let Ok((stream, _)) = control.accept() {
            let shared = shared.clone();
            controls.push(std::thread::spawn(move || {
                let _ = (|| -> Result<()> {
                    let mut socket = ws::accept(stream)?;
                    let action = ws::receive(&mut socket)?;
                    ws::reply(&mut socket, manage(&shared, action))
                })();
            }));
        }
        if let Some(listener) = &network
            && let Ok((stream, _)) = listener.accept()
        {
            let shared = shared.clone();
            std::thread::spawn(move || {
                let _ = serve_peer(shared, stream);
            });
        }
        if Instant::now() >= reconcile {
            let mut s = lock(&shared);
            if let Err(error) = s.reconcile() {
                eprintln!("worker reconcile: {error:#}");
            }
            reconcile = s.next_reconcile(Instant::now() + RECONCILE_INTERVAL);
        }
        let current = lock(&shared).peers();
        match current {
            Ok(current) => peers = current,
            Err(error) => {
                event!(warn, "CATALOG_UNAVAILABLE", {
                    "error": format!("{error:#}"),
                    "kept_peers": peers.len(),
                });
            }
        }
        for peer in &peers {
            for lane in 0..peer.pool_size() {
                let key = link_key(peer, lane);
                lanes.entry(key).or_insert_with(|| {
                    let shared = shared.clone();
                    let peer = peer.clone();
                    std::thread::spawn(move || run_lane(shared, peer, lane))
                });
            }
        }
        lanes.retain(|_, handle| !handle.is_finished());
        controls.retain(|handle| !handle.is_finished());
        std::thread::sleep(Duration::from_millis(10));
    }
    // Finish the shutdown response before the executable exits.
    for handle in controls {
        let _ = handle.join();
    }
    let _ = fs::remove_file(discovery_path);
    Ok(())
}
