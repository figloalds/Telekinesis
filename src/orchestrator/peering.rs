use super::{api::Project, supervisor::Shared};
use crate::{
    core::id,
    model::ipc::deadlines,
    paired_sync::{PublishedState, SyncPage, SyncRoute},
    pairing_code::Verifier,
    runtime::{self, Discovery},
    util::lock::lock,
    ws,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

pub(crate) fn dial(endpoint: &str, code: &str, installation: &str) -> Result<(ws::Socket, String)> {
    let (socket, installation, _) = dial_role(endpoint, code, installation, Some("control"), None)?;
    Ok((socket, installation))
}
pub(crate) fn dial_role(
    endpoint: &str,
    code: &str,
    installation: &str,
    role: Option<&str>,
    bulk_lane: Option<crate::bulk_sync::Lane>,
) -> Result<(ws::Socket, String, bool)> {
    let mut socket = ws::connect(endpoint)?;
    let hello = ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":code,"installation":installation,"lane":role,"bulk_protocol":crate::bulk_sync::PROTOCOL,"bulk_lane":bulk_lane}),
    )?;
    if let Some(role) = role {
        ensure!(
            hello["lane"] == role,
            "RESERVED_LANES_UNSUPPORTED: upgrade the remote orchestrator"
        );
    }
    let parallel = hello["bulk_protocol"] == crate::bulk_sync::PROTOCOL;
    if parallel {
        ensure!(
            hello["bulk_lane"] == json!(bulk_lane),
            "BULK_LANE_NEGOTIATION_FAILED"
        );
    }
    Ok((
        socket,
        hello["installation"]
            .as_str()
            .context("INVALID_PAIR_REPLY")?
            .into(),
        parallel,
    ))
}
pub(crate) fn remote_list(shared: &Shared, endpoint: &str) -> Result<Vec<PublishedState>> {
    let (p, installation) = {
        let s = lock(shared);
        (
            s.peers()?
                .into_iter()
                .find(|p| p.endpoint == endpoint)
                .context("PEER_NOT_PAIRED")?,
            s.installation.clone(),
        )
    };
    let (mut socket, remote) = dial(&p.endpoint, &p.code, &installation)?;
    ensure!(remote == p.installation, "PEER_INSTALLATION_CHANGED");
    Ok(serde_json::from_value(ws::call(
        &mut socket,
        &json!({"op":"list"}),
    )?)?)
}
/// A loopback worker connection for `published-summary` with a hard
/// per-call deadline. It mirrors `runtime::RpcLane` (one reused socket,
/// reconnect after 10 s idle or an address change, dropped after any
/// transport error, never replayed) and adds the deadline: the request and
/// its reply must complete within what is left of it after connecting.
/// Adapter for CCR-WP1.12-1 (`RpcLane` with a timeout).
#[derive(Default)]
pub(crate) struct DeadlineLane {
    connection: Option<(String, ws::Socket)>,
    last_used: Option<Instant>,
}
impl DeadlineLane {
    pub(crate) fn call(
        &mut self,
        info: &Discovery,
        payload: Value,
        deadline: Duration,
    ) -> Result<Value> {
        let started = Instant::now();
        if self
            .connection
            .as_ref()
            .is_none_or(|(address, _)| address != &info.address)
            || self
                .last_used
                .is_some_and(|at| at.elapsed() >= Duration::from_secs(10))
        {
            self.connection = None;
            self.connection = Some((info.address.clone(), ws::connect(&info.address)?));
        }
        let socket = &mut self.connection.as_mut().unwrap().1;
        // A zero timeout means "block forever" to the OS: keep at least 1 ms.
        let remaining = deadline
            .saturating_sub(started.elapsed())
            .max(Duration::from_millis(1));
        let result = socket_deadline(socket, remaining)
            .and_then(|()| ws::call(socket, &json!({"request":id(),"payload":payload})));
        self.last_used = Some(Instant::now());
        match result {
            Ok(value) => Ok(value),
            Err(error) if error.downcast_ref::<ws::RemoteError>().is_some() => Err(error),
            Err(error) => {
                // Uncertain: the socket may still deliver a late reply.
                self.connection = None;
                if timed_out(&error) {
                    Err(error.context(format!(
                        "REQUEST_TIMEOUT: no reply within {} ms",
                        deadline.as_millis()
                    )))
                } else {
                    Err(error)
                }
            }
        }
    }
}

/// Bounds every read and write on a plain (loopback) socket.
fn socket_deadline(socket: &mut ws::Socket, timeout: Duration) -> Result<()> {
    if let tungstenite::stream::MaybeTlsStream::Plain(tcp) = socket.get_mut() {
        tcp.set_read_timeout(Some(timeout))?;
        tcp.set_write_timeout(Some(timeout))?;
    }
    Ok(())
}

/// Whether an error chain ends in a socket timeout (`WouldBlock` on Unix,
/// `TimedOut` on Windows).
fn timed_out(error: &anyhow::Error) -> bool {
    let timeout = |io: &std::io::Error| {
        matches!(
            io.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )
    };
    error.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(timeout)
            || matches!(cause.downcast_ref::<tungstenite::Error>(),
                Some(tungstenite::Error::Io(io)) if timeout(io))
    })
}

fn summary(
    client: &mut DeadlineLane,
    project: &Project,
    info: Result<Discovery>,
) -> Result<PublishedState> {
    let info = info?;
    let state: PublishedState = serde_json::from_value(client.call(
        &info,
        json!({"op":"published-summary"}),
        deadlines::PUBLISHED_SUMMARY,
    )?)?;
    ensure!(
        state.repo == project.repo && state.replica == project.device,
        "WORKER_STATE_MISMATCH"
    );
    Ok(state)
}

/// The catalog this daemon publishes to an authenticated peer (`list`).
/// Every running worker is asked at once, each under the 3 s
/// `PUBLISHED_SUMMARY` deadline, so a hung worker costs at most that
/// deadline and never the dialer's 15 s `list` budget. A worker that fails
/// or times out is left out and reported as the folder's publication error.
fn published(
    shared: &Shared,
    clients: &mut BTreeMap<String, DeadlineLane>,
) -> Result<Vec<PublishedState>> {
    let workers = {
        let s = lock(shared);
        s.projects()?
            .into_iter()
            .filter(|p| s.workers.get(&p.state).is_some_and(|w| w.startup.is_none()))
            .map(|p| {
                let info = s.discovery(&p.state);
                (p, info)
            })
            .collect::<Vec<_>>()
    };
    let states: Vec<String> = workers.iter().map(|(p, _)| p.state.clone()).collect();
    let mut ask = |(p, info): (Project, Result<Discovery>)| {
        let mut client = clients.remove(&p.state).unwrap_or_default();
        move || {
            let result = summary(&mut client, &p, info);
            (p, result, client)
        }
    };
    let answers: Vec<_> = if workers.len() <= 1 {
        workers.into_iter().map(|w| ask(w)()).collect()
    } else {
        std::thread::scope(|scope| {
            let pending: Vec<_> = workers.into_iter().map(|w| scope.spawn(ask(w))).collect();
            pending
                .into_iter()
                .filter_map(|answer| answer.join().ok())
                .collect()
        })
    };
    let mut published = Vec::new();
    let mut errors = BTreeMap::new();
    for state in states {
        errors.insert(
            state,
            "PUBLISHED_SUMMARY_FAILED: no answer from the summary thread".to_owned(),
        );
    }
    for (p, result, client) in answers {
        clients.insert(p.state.clone(), client);
        match result {
            Ok(mut state) => {
                errors.remove(&p.state);
                state.label = p.label;
                published.push(state);
            }
            Err(error) => {
                errors.insert(p.state, format!("PUBLISHED_SUMMARY_FAILED: {error:#}"));
            }
        }
    }
    lock(shared).publication_errors = errors;
    Ok(published)
}
pub(crate) fn route(info: &Discovery, peer: &str, replica: &str) -> SyncRoute {
    SyncRoute {
        peer: peer.into(),
        repo: info.repo.clone(),
        local_replica: info.device.clone(),
        remote_replica: replica.into(),
    }
}
pub(crate) fn serve_peer(shared: Shared, stream: std::net::TcpStream) -> Result<()> {
    let mut socket = ws::accept(stream)?;
    let hello: Value = ws::receive(&mut socket)?;
    let role = hello["lane"].as_str();
    let parallel = hello["bulk_protocol"] == crate::bulk_sync::PROTOCOL;
    let bulk_lane: Option<crate::bulk_sync::Lane> =
        serde_json::from_value(hello["bulk_lane"].clone())?;
    let verified = (|| -> Result<(Verifier, String, String)> {
        ensure!(
            hello["op"] == "pair" && hello["version"] == 2,
            "INCOMPATIBLE_ORCHESTRATOR_PROTOCOL"
        );
        ensure!(
            hello["lane"].is_null() || matches!(role, Some("control" | "bulk")),
            "INVALID_LANE_ROLE"
        );
        if parallel && role == Some("bulk") {
            bulk_lane.context("BULK_LANE_REQUIRED")?.validate()?;
        }
        let peer = hello["installation"]
            .as_str()
            .context("INSTALLATION_REQUIRED")?
            .to_owned();
        let s = lock(&shared);
        let verifier = s.verifier()?;
        ensure!(
            verifier.verify(hello["code"].as_str().context("PAIRING_CODE_REQUIRED")?),
            "INVALID_PAIRING_CODE"
        );
        ensure!(peer != s.installation, "CANNOT_PAIR_WITH_SELF");
        Ok((verifier, peer, s.installation.clone()))
    })();
    let (verifier, peer, installation) = match verified {
        Ok(v) => v,
        Err(error) => {
            ws::reply(&mut socket, Err(error))?;
            return Ok(());
        }
    };
    ws::reply(
        &mut socket,
        Ok(
            json!({"installation":installation,"version":2,"lane":role,"bulk_protocol":if parallel { crate::bulk_sync::PROTOCOL } else { 0 },"bulk_lane":bulk_lane}),
        ),
    )?;
    let mut workers = BTreeMap::<String, runtime::RpcLane>::new();
    let mut summaries = BTreeMap::<String, DeadlineLane>::new();
    loop {
        let request: Value = ws::receive(&mut socket)?;
        let result = (|| -> Result<Value> {
            {
                let s = lock(&shared);
                ensure!(!s.stopping.load(Ordering::Relaxed), "ORCHESTRATOR_STOPPING");
                ensure!(s.verifier()?.hash == verifier.hash, "PAIRING_CODE_CHANGED");
            }
            match request["op"].as_str() {
                Some("ping") => Ok(json!({"alive":true})),
                Some("list") => Ok(json!(published(&shared, &mut summaries)?)),
                Some("exchange") => {
                    ensure!(!parallel || role != Some("bulk"), "METADATA_ON_BULK_LANE");
                    let page: SyncPage = serde_json::from_value(request["page"].clone())?;
                    let info = {
                        let s = lock(&shared);
                        let p = s
                            .projects()?
                            .into_iter()
                            .find(|p| p.repo == page.repo)
                            .context("REPOSITORY_NOT_MANAGED")?;
                        s.discovery(&p.state)?
                    };
                    let binding = route(&info, &peer, &page.sender);
                    workers.entry(info.state.clone()).or_default().call(
                        &info,
                        &id(),
                        json!({"op":"published-exchange","route":binding,"page":page,"max_frame":4*1024*1024,"metadata_only":role == Some("control")}),
                    )
                }
                Some("objects") => {
                    ensure!(parallel && role == Some("bulk"), "BULK_ON_CONTROL_LANE");
                    let page: crate::bulk_sync::Page =
                        serde_json::from_value(request["page"].clone())?;
                    ensure!(Some(page.lane) == bulk_lane, "BULK_LANE_MISMATCH");
                    let info = {
                        let s = lock(&shared);
                        let project = s
                            .projects()?
                            .into_iter()
                            .find(|p| p.repo == page.repo)
                            .context("REPOSITORY_NOT_MANAGED")?;
                        s.discovery(&project.state)?
                    };
                    let binding = route(&info, &peer, &page.sender);
                    workers.entry(info.state.clone()).or_default().call(&info, &id(),
                        json!({"op":"published-bulk-exchange","route":binding,"lane":page.lane,"page":page}))
                }
                _ => bail!("UNKNOWN_PEER_OPERATION"),
            }
        })();
        ws::reply(&mut socket, result)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    fn discovery(address: String) -> Discovery {
        Discovery {
            format: 2,
            address,
            state: "state".into(),
            mount: None,
            repo: "repo".into(),
            device: "device".into(),
        }
    }

    /// A worker stand-in: answers `published-summary` with `reply`, or never
    /// answers when `reply` is `None`. Counts accepted connections.
    fn worker(reply: Option<Result<Value, String>>) -> (Discovery, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                accepted.fetch_add(1, Ordering::Relaxed);
                let reply = reply.clone();
                std::thread::spawn(move || {
                    let Ok(mut socket) = ws::accept(stream) else {
                        return;
                    };
                    while let Ok(request) = ws::receive::<Value>(&mut socket) {
                        assert_eq!(request["payload"]["op"], "published-summary");
                        match &reply {
                            Some(reply) => {
                                let reply = reply.clone().map_err(|e| anyhow::anyhow!(e));
                                if ws::reply(&mut socket, reply).is_err() {
                                    return;
                                }
                            }
                            None => std::thread::sleep(Duration::from_secs(5)),
                        }
                    }
                });
            }
        });
        (discovery(address), connections)
    }

    #[test]
    fn a_hung_worker_costs_one_deadline_and_its_connection() {
        let (info, connections) = worker(None);
        let mut lane = DeadlineLane::default();
        for attempt in 1..=2 {
            let started = Instant::now();
            let error = lane
                .call(
                    &info,
                    json!({"op":"published-summary"}),
                    Duration::from_millis(300),
                )
                .unwrap_err();
            let elapsed = started.elapsed();
            assert!(
                elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(2),
                "{elapsed:?}"
            );
            assert!(
                format!("{error:#}").contains("REQUEST_TIMEOUT"),
                "{error:#}"
            );
            // Never replayed on the uncertain socket: each call reconnects.
            assert!(lane.connection.is_none());
            assert_eq!(connections.load(Ordering::Relaxed), attempt);
        }
    }

    #[test]
    fn healthy_and_remote_error_replies_reuse_the_connection() {
        let (info, connections) = worker(Some(Ok(json!({"repo":"repo"}))));
        let mut lane = DeadlineLane::default();
        for _ in 0..3 {
            let reply = lane
                .call(
                    &info,
                    json!({"op":"published-summary"}),
                    Duration::from_secs(3),
                )
                .unwrap();
            assert_eq!(reply["repo"], "repo");
        }
        assert_eq!(connections.load(Ordering::Relaxed), 1);
        let (info, connections) = worker(Some(Err("WORKER_BUSY".into())));
        let mut lane = DeadlineLane::default();
        for _ in 0..2 {
            let error = lane
                .call(
                    &info,
                    json!({"op":"published-summary"}),
                    Duration::from_secs(3),
                )
                .unwrap_err();
            assert!(error.downcast_ref::<ws::RemoteError>().is_some());
        }
        assert_eq!(connections.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn an_unreachable_worker_fails_within_the_deadline() {
        let info = discovery("ws://127.0.0.1:0".into());
        let started = Instant::now();
        assert!(
            DeadlineLane::default()
                .call(
                    &info,
                    json!({"op":"published-summary"}),
                    deadlines::PUBLISHED_SUMMARY
                )
                .is_err()
        );
        assert!(started.elapsed() < Duration::from_secs(4));
    }
}
