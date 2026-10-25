//! A folder daemon owns its metadata, mount and paired session connections.
pub mod catalog;
pub mod control;
pub mod sync;
use crate::{
    content::working::lock,
    model::{
        content::{ContentKind, FileContentRef},
        protocol::LaneRole,
    },
    orchestrator::sync::{DigestFetch, FeedCursor, SessionSource},
    runtime::Engine,
    session::{self, Conn, ConnOptions},
};
use anyhow::{Result, ensure};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

fn prefetch_pass(engine: &Engine, fetch: &DigestFetch, references: &[FileContentRef]) {
    for batch in references.chunks(sync::DIRECT_PREFETCH_BATCH) {
        if engine.stopping.load(Ordering::Acquire) {
            break;
        }
        fetch.prefetch_direct_roots(batch, &engine.stopping);
        for reference in batch.iter().filter(|r| r.kind == ContentKind::Manifest) {
            if engine.stopping.load(Ordering::Acquire) {
                break;
            }
            if fetch
                .prefetch(&[(reference.digest, crate::content::Kind::Manifest)])
                .is_ok()
                && let Ok(chunks) = engine.working.content().chunks(reference)
            {
                let wanted = chunks
                    .into_iter()
                    .map(|d| (d, crate::content::Kind::Chunk))
                    .collect::<Vec<_>>();
                let _ = fetch.prefetch(&wanted);
            }
        }
    }
}

pub fn run(engine: Arc<Engine>, listen: &str, advertise: Option<&str>) -> Result<()> {
    // Startup and loop failures take the same cleanup path as a control stop.
    // In particular, bind/catalog errors after attach must not skip detach.
    let served = serve(engine.clone(), listen, advertise);
    let shutdown = engine.shutdown();
    #[cfg(any(windows, target_os = "linux"))]
    let detach = crate::mount::detach();
    #[cfg(not(any(windows, target_os = "linux")))]
    let detach: Result<()> = Ok(());
    let root = lock(&engine.store).root.clone();
    let _ = std::fs::remove_file(root.join("control.json"));
    let errors: Vec<String> = [served.err(), shutdown.err(), detach.err()]
        .into_iter()
        .flatten()
        .map(|error| format!("{error:#}"))
        .collect();
    ensure!(
        errors.is_empty(),
        "SHUTDOWN_INCOMPLETE: {}",
        errors.join("; ")
    );
    Ok(())
}

fn serve(engine: Arc<Engine>, listen: &str, advertise: Option<&str>) -> Result<()> {
    let listener = TcpListener::bind(listen)?;
    listener.set_nonblocking(true)?;
    let endpoint = advertise
        .map(str::to_owned)
        .unwrap_or_else(|| format!("ws://{}", listener.local_addr().unwrap()));
    let catalog = catalog::Catalog::open(engine.clone())?;
    catalog.set("advertise", &endpoint)?;
    let control = control::start(engine.clone(), catalog.clone())?;
    let fetch = DigestFetch::new(engine.working.content().packs.clone());
    engine.working.content().set_fetcher(fetch.clone());
    let (device, repo) = {
        let s = lock(&engine.store);
        (s.device.clone(), s.repo.clone())
    };
    println!(
        "{}",
        serde_json::json!({"ready":true,"peer":endpoint,"control":control,"device":device,"repo":repo})
    );
    let mut started = BTreeMap::<String, std::thread::JoinHandle<()>>::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut sessions: Vec<Conn> = Vec::new();
    let handshakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    while !engine.stopping.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                if sessions.len() < 64 && handshakes.load(Ordering::Acquire) < 16 {
                    let catalog = catalog.clone();
                    let engine = engine.clone();
                    let tx = tx.clone();
                    let count = handshakes.clone();
                    count.fetch_add(1, Ordering::AcqRel);
                    std::thread::spawn(move || {
                        if let Ok((conn, peer)) =
                            session::accept(stream, catalog.clone(), ConnOptions::default())
                        {
                            conn.serve(Arc::new(catalog::PeerService {
                                catalog,
                                engine,
                                peer: peer.installation,
                            }));
                            let _ = tx.send(conn);
                        }
                        count.fetch_sub(1, Ordering::AcqRel);
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e.into()),
        }
        sessions.extend(rx.try_iter());
        sessions.retain(|conn| conn.closed().is_none());
        started.retain(|_, thread| !thread.is_finished());
        for peer in catalog.peers()? {
            if !started.contains_key(&peer.endpoint) {
                let endpoint = peer.endpoint.clone();
                let weak = Arc::downgrade(&engine);
                let catalog = catalog.clone();
                let fetch = fetch.clone();
                let thread =
                    std::thread::Builder::new()
                        .name("tkfs-peer".into())
                        .spawn(move || {
                            let mut delay = 1;
                            let mut cursor = FeedCursor::default();
                            loop {
                                let Some(engine) = weak.upgrade() else {
                                    break;
                                };
                                if engine.stopping.load(Ordering::Acquire)
                                    || !catalog.contains(&peer.endpoint)
                                {
                                    break;
                                }
                                let device = lock(&engine.store).device.clone();
                                let hello =
                                    session::client_hello(&device, LaneRole::Control, None, 1);
                                let key = match peer.key() {
                                    Ok(key) => key,
                                    Err(_) => break,
                                };
                                match session::dial_expecting(
                                    &peer.endpoint,
                                    hello,
                                    &session::auth::PeerAuth { key },
                                    Some(&peer.installation),
                                    None,
                                    ConnOptions::default(),
                                ) {
                                    Ok((conn, _)) => {
                                        fetch.add(
                                            peer.endpoint.clone(),
                                            Arc::new(SessionSource {
                                                conn: conn.clone(),
                                                repo: lock(&engine.store).repo.clone(),
                                            }),
                                        );
                                        delay = 1;
                                        while !engine.stopping.load(Ordering::Acquire)
                                            && catalog.contains(&peer.endpoint)
                                            && conn.closed().is_none()
                                        {
                                            match sync::exchange(&engine, &conn, &mut cursor) {
                                                Ok(more) => {
                                                    if more {
                                                        continue;
                                                    }
                                                }
                                                Err(error) => {
                                                    eprintln!("peer {}: {error:#}", peer.endpoint);
                                                    break;
                                                }
                                            }
                                            let references = lock(&engine.store)
                                                .projection()
                                                .entries
                                                .values()
                                                .filter(|e| !e.deleted)
                                                .filter_map(|e| e.content.clone())
                                                .collect::<Vec<_>>();
                                            prefetch_pass(&engine, &fetch, &references);
                                            std::thread::sleep(Duration::from_millis(100));
                                        }
                                        conn.close();
                                        fetch.remove(&peer.endpoint);
                                    }
                                    Err(error) => eprintln!("peer {}: {error}", peer.endpoint),
                                }
                                drop(engine);
                                for _ in 0..delay * 10 {
                                    let Some(engine) = weak.upgrade() else {
                                        return;
                                    };
                                    if engine.stopping.load(Ordering::Acquire) {
                                        return;
                                    }
                                    drop(engine);
                                    std::thread::sleep(Duration::from_millis(100));
                                }
                                delay = (delay * 2).min(60);
                            }
                        })?;
                started.insert(endpoint, thread);
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    for session in sessions {
        session.close();
    }
    Ok(())
}
