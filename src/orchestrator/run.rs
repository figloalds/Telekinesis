use super::{
    api::manage,
    config::Config,
    lanes::{link_key, run_lane},
    peering::serve_peer,
    supervisor::Supervisor,
};
use crate::ws;
use anyhow::Result;
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};

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
            if let Err(error) = shared.lock().unwrap().reconcile() {
                eprintln!("worker reconcile: {error:#}");
            }
            reconcile = Instant::now() + Duration::from_secs(2);
        }
        let peers = shared.lock().unwrap().peers()?;
        for peer in peers {
            for lane in 0..peer.pool_size() {
                let key = link_key(&peer, lane);
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
