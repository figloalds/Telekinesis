//! Owned by WP2.1 in Wave 2 (WP1.12 in Wave 1; deleted by WP3.1).
//!
//! Legacy v2 peering: the pairing dial, `remote_list`, the deadline-bounded
//! loopback worker call, and the inbound server (`serve_inbound`, adapted
//! from the WP1.12 `serve_peer` to the actor orchestrator, REVAMP_PLAN §14
//! WP2.1 task 4):
//!
//! * the verifier and the folders come from the catalog snapshot (O(1), no
//!   lock), checked again on every request (`PAIRING_CODE_CHANGED`);
//! * `list` is the published-catalog cache (`PublishedCatalogApi::list()`,
//!   running entries only), never a fan-out to the workers;
//! * the local worker of a folder is its controller's endpoint
//!   (`LegacyBridge::discovery`); a paused folder answers
//!   `FOLDER_SYNC_PAUSED`, a folder without a running worker
//!   `LOCAL_WORKER_UNAVAILABLE`;
//! * each worker call is bounded by [`INBOUND_WORKER_DEADLINE`].
use super::{lanes, legacy_bridge::LegacyBridge, supervisor::Shared};
use crate::{
    core::id,
    model::{
        orchestrator::{Ctx, EntryState, FolderPhase},
        protocol::LaneRole,
        status::Direction,
    },
    paired_sync::{PublishedState, SyncPage, SyncRoute},
    pairing_code::Verifier,
    runtime::Discovery,
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

/// The bound of one inbound worker call (`published-exchange`,
/// `published-bulk-exchange`): the legacy lanes' socket timeout, so a worker
/// call never outlives what the dialer waits for its reply.
pub(crate) const INBOUND_WORKER_DEADLINE: Duration = Duration::from_secs(15);
/// `list` waits at most this long for the first `published-summary` of a
/// folder that is already Running (its controller asks right after Ready).
pub(crate) const FIRST_SUMMARY_WAIT: Duration = Duration::from_secs(1);

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

/// Dials `endpoint` as installation `installation` and lists its published
/// folders; the remote must still be `expected` (`PEER_INSTALLATION_CHANGED`).
pub(crate) fn remote_catalog(
    endpoint: &str,
    code: &str,
    installation: &str,
    expected: &str,
) -> Result<Vec<PublishedState>> {
    let (mut socket, remote) = dial(endpoint, code, installation)?;
    ensure!(remote == expected, "PEER_INSTALLATION_CHANGED");
    Ok(serde_json::from_value(ws::call(
        &mut socket,
        &json!({"op":"list"}),
    )?)?)
}

/// [`remote_catalog`] through the legacy `Supervisor` (the legacy
/// `api::manage`, which stays compiled until WP2.INT).
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
    remote_catalog(&p.endpoint, &p.code, &installation, &p.installation)
}

/// A loopback worker connection with a hard per-call deadline. It mirrors
/// `runtime::RpcLane` (one reused socket, reconnect after 10 s idle or an
/// address change, dropped after any transport error, never replayed) and
/// adds the deadline: the request and its reply must complete within what is
/// left of it after connecting. Adapter for CCR-WP1.12-1 (`RpcLane` with a
/// timeout); the inbound server and the `Folder` passthrough use it.
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
        self.call_as(info, &id(), payload, deadline)
    }

    /// [`call`](Self::call) with the caller's request id (the worker's
    /// receipts make a retried mutation with the same id idempotent).
    pub(crate) fn call_as(
        &mut self,
        info: &Discovery,
        request: &str,
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
            .and_then(|()| ws::call(socket, &json!({"request":request,"payload":payload})));
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

pub(crate) fn route(info: &Discovery, peer: &str, replica: &str) -> SyncRoute {
    SyncRoute {
        peer: peer.into(),
        repo: info.repo.clone(),
        local_replica: info.device.clone(),
        remote_replica: replica.into(),
    }
}

/// The folders this daemon publishes to an authenticated peer (`list`): the
/// published-catalog cache, Running entries only, labelled with the catalog
/// label (the legacy `list` did the same). A folder that is Running but whose
/// first `published-summary` has not come back yet (it is asked right after
/// Ready) is waited for, at most [`FIRST_SUMMARY_WAIT`]; a worker that does
/// not answer it is left out, and its controller records the publication
/// error (`PUBLISHED_SUMMARY_FAILED`).
pub(crate) fn published_list(ctx: &Ctx) -> Vec<PublishedState> {
    let deadline = Instant::now() + FIRST_SUMMARY_WAIT;
    loop {
        let (generation, entries) = ctx.published.list();
        let snapshot = ctx.catalog.snapshot();
        let awaiting = snapshot.folders.iter().any(|folder| {
            folder.sync_enabled
                && !entries.iter().any(|entry| entry.repo == folder.repo)
                && ctx
                    .folders
                    .view(&folder.state)
                    .is_some_and(|view| matches!(view.phase, FolderPhase::Running { .. }))
        });
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !awaiting || remaining.is_zero() {
            return entries
                .iter()
                .filter(|entry| entry.state == EntryState::Running)
                .map(|entry| PublishedState {
                    repo: entry.repo.clone(),
                    replica: entry.replica.clone(),
                    label: snapshot
                        .folder_by_repo(&entry.repo)
                        .map_or_else(|| entry.label.clone(), |folder| folder.label.clone()),
                    branches: entry.branches.clone(),
                })
                .collect();
        }
        ctx.published.wait_newer(generation, remaining);
    }
}

/// The role and lane of an inbound connection, from its pairing hello.
struct Hello {
    peer: String,
    role: Option<String>,
    parallel: bool,
    bulk_lane: Option<crate::bulk_sync::Lane>,
}

impl Hello {
    fn lane(&self) -> (u32, LaneRole) {
        match (self.role.as_deref(), self.bulk_lane) {
            (Some("bulk"), Some(lane)) => (
                u32::try_from(lane.index + 1).unwrap_or(u32::MAX),
                LaneRole::Bulk,
            ),
            (Some("bulk"), None) => (1, LaneRole::Bulk),
            (Some("control"), _) => (0, LaneRole::Control),
            _ => (0, LaneRole::Probe),
        }
    }
}

/// One inbound legacy connection: the pairing hello, then a request loop
/// until the peer goes away (or its socket is shut down by the bridge).
pub(crate) fn serve_inbound(
    bridge: &LegacyBridge,
    ctx: &Ctx,
    stream: std::net::TcpStream,
) -> Result<()> {
    let mut socket = ws::accept(stream)?;
    let hello: Value = ws::receive(&mut socket)?;
    let role = hello["lane"].as_str();
    let parallel = hello["bulk_protocol"] == crate::bulk_sync::PROTOCOL;
    let bulk_lane: Option<crate::bulk_sync::Lane> =
        serde_json::from_value(hello["bulk_lane"].clone())?;
    let verified = (|| -> Result<(Verifier, String)> {
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
        let snapshot = ctx.catalog.snapshot();
        let verifier = snapshot
            .verifier
            .clone()
            .context("PAIRING_CODE_NOT_SET: reset-pairing-code first")?;
        ensure!(
            verifier.verify(hello["code"].as_str().context("PAIRING_CODE_REQUIRED")?),
            "INVALID_PAIRING_CODE"
        );
        ensure!(peer != ctx.installation, "CANNOT_PAIR_WITH_SELF");
        Ok((verifier, peer))
    })();
    let (verifier, peer) = match verified {
        Ok(v) => v,
        Err(error) => {
            ws::reply(&mut socket, Err(error))?;
            return Ok(());
        }
    };
    ws::reply(
        &mut socket,
        Ok(
            json!({"installation":ctx.installation,"version":2,"lane":role,"bulk_protocol":if parallel { crate::bulk_sync::PROTOCOL } else { 0 },"bulk_lane":bulk_lane}),
        ),
    )?;
    let session = Hello {
        peer,
        role: role.map(str::to_owned),
        parallel,
        bulk_lane,
    };
    let (lane, lane_role) = session.lane();
    bridge.inbound_session(ctx, &session.peer, lane, lane_role, true);
    let served = serve_requests(bridge, ctx, &mut socket, &session, &verifier);
    bridge.inbound_session(ctx, &session.peer, lane, lane_role, false);
    served
}

fn serve_requests(
    bridge: &LegacyBridge,
    ctx: &Ctx,
    socket: &mut ws::Socket,
    session: &Hello,
    verifier: &Verifier,
) -> Result<()> {
    let (lane, _) = session.lane();
    let role = session.role.as_deref();
    let parallel = session.parallel;
    let mut workers = BTreeMap::<String, DeadlineLane>::new();
    loop {
        let request: Value = ws::receive(socket)?;
        let started = Instant::now();
        let mut exchanged: Option<(String, &'static str)> = None;
        let result = (|| -> Result<Value> {
            ensure!(
                !ctx.stopping.load(Ordering::Acquire),
                "ORCHESTRATOR_STOPPING"
            );
            let snapshot = ctx.catalog.snapshot();
            let current = snapshot
                .verifier
                .as_ref()
                .context("PAIRING_CODE_NOT_SET: reset-pairing-code first")?;
            ensure!(current.hash == verifier.hash, "PAIRING_CODE_CHANGED");
            let (op, worker_op, page_repo, sender) = match request["op"].as_str() {
                Some("ping") => return Ok(json!({"alive":true})),
                Some("list") => return Ok(json!(published_list(ctx))),
                Some("exchange") => {
                    ensure!(!parallel || role != Some("bulk"), "METADATA_ON_BULK_LANE");
                    let page: SyncPage = serde_json::from_value(request["page"].clone())?;
                    ("exchange", "published-exchange", page.repo, page.sender)
                }
                Some("objects") => {
                    ensure!(parallel && role == Some("bulk"), "BULK_ON_CONTROL_LANE");
                    let page: crate::bulk_sync::Page =
                        serde_json::from_value(request["page"].clone())?;
                    ensure!(Some(page.lane) == session.bulk_lane, "BULK_LANE_MISMATCH");
                    ("objects", "published-bulk-exchange", page.repo, page.sender)
                }
                _ => bail!("UNKNOWN_PEER_OPERATION"),
            };
            let folder = snapshot
                .folder_by_repo(&page_repo)
                .context("REPOSITORY_NOT_MANAGED")?;
            exchanged = Some((folder.repo.clone(), op));
            ensure!(
                folder.sync_enabled,
                "FOLDER_SYNC_PAUSED: sync is paused for {}",
                folder.label
            );
            let info = bridge
                .discovery(ctx, folder)
                .context("LOCAL_WORKER_UNAVAILABLE")?;
            let binding = route(&info, &session.peer, &sender);
            let mut payload = json!({"op":worker_op,"route":binding,"page":request["page"]});
            if op == "exchange" {
                payload["max_frame"] = json!(4 * 1024 * 1024);
                payload["metadata_only"] = json!(role == Some("control"));
            } else {
                payload["lane"] = json!(session.bulk_lane);
            }
            workers.entry(folder.state.clone()).or_default().call(
                &info,
                payload,
                INBOUND_WORKER_DEADLINE,
            )
        })();
        if let Some((repo, op)) = &exchanged {
            // Seen from here: the peer's parts come down, the reply's go up.
            let (bytes_down, objects_down) = lanes::part_stats(&request["page"]["object_parts"]);
            let (bytes_up, objects_up) = result
                .as_ref()
                .map_or((0, 0), |reply| lanes::part_stats(&reply["object_parts"]));
            ctx.status.record(lanes::exchange_event(
                &session.peer,
                repo,
                lane,
                op,
                Direction::Inbound,
                result.as_ref().err(),
                None,
                (bytes_up, bytes_down),
                (objects_up, objects_down),
                started.elapsed(),
            ));
        }
        ws::reply(socket, result)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ipc::deadlines;
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
    /// answers when `reply` is `None`. Counts accepted connections and
    /// remembers the request ids it saw.
    fn worker(
        reply: Option<Result<Value, String>>,
    ) -> (
        Discovery,
        Arc<AtomicUsize>,
        Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let accepted = connections.clone();
        let seen = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                accepted.fetch_add(1, Ordering::Relaxed);
                let reply = reply.clone();
                let seen = seen.clone();
                std::thread::spawn(move || {
                    let Ok(mut socket) = ws::accept(stream) else {
                        return;
                    };
                    while let Ok(request) = ws::receive::<Value>(&mut socket) {
                        assert_eq!(request["payload"]["op"], "published-summary");
                        seen.lock()
                            .unwrap()
                            .push(request["request"].as_str().unwrap_or_default().into());
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
        (discovery(address), connections, requests)
    }

    #[test]
    fn a_hung_worker_costs_one_deadline_and_its_connection() {
        let (info, connections, _) = worker(None);
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
        let (info, connections, _) = worker(Some(Ok(json!({"repo":"repo"}))));
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
        let (info, connections, _) = worker(Some(Err("WORKER_BUSY".into())));
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

    #[test]
    fn call_as_keeps_the_callers_request_id() {
        let (info, _, requests) = worker(Some(Ok(json!({"repo":"repo"}))));
        let mut lane = DeadlineLane::default();
        lane.call_as(
            &info,
            "my-request",
            json!({"op":"published-summary"}),
            Duration::from_secs(3),
        )
        .unwrap();
        lane.call(
            &info,
            json!({"op":"published-summary"}),
            Duration::from_secs(3),
        )
        .unwrap();
        let seen = requests.lock().unwrap().clone();
        assert_eq!(seen[0], "my-request");
        assert_ne!(seen[1], "my-request");
    }
}
