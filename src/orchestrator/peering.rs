use super::supervisor::Shared;
use crate::{
    core::id,
    paired_sync::{PublishedState, SyncPage, SyncRoute},
    pairing_code::Verifier,
    runtime::{self, Discovery},
    ws,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::atomic::Ordering};

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
        let s = shared.lock().unwrap();
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
fn published(
    shared: &Shared,
    clients: &mut BTreeMap<String, runtime::RpcLane>,
) -> Result<Vec<PublishedState>> {
    let workers = {
        let s = shared.lock().unwrap();
        s.projects()?
            .into_iter()
            .filter(|p| s.workers.get(&p.state).is_some_and(|w| w.startup.is_none()))
            .map(|p| {
                let info = s.discovery(&p.state);
                (p, info)
            })
            .collect::<Vec<_>>()
    };
    let mut published = Vec::new();
    let mut errors = BTreeMap::new();
    for (p, info) in workers {
        let result = (|| -> Result<PublishedState> {
            let info = info?;
            let state: PublishedState =
                serde_json::from_value(clients.entry(info.state.clone()).or_default().call(
                    &info,
                    &id(),
                    json!({"op":"published-summary"}),
                )?)?;
            ensure!(
                state.repo == p.repo && state.replica == p.device,
                "WORKER_STATE_MISMATCH"
            );
            Ok(state)
        })();
        match result {
            Ok(mut state) => {
                state.label = p.label;
                published.push(state);
            }
            Err(error) => {
                errors.insert(p.state, format!("PUBLISHED_SUMMARY_FAILED: {error:#}"));
            }
        }
    }
    shared.lock().unwrap().publication_errors = errors;
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
        let s = shared.lock().unwrap();
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
    loop {
        let request: Value = ws::receive(&mut socket)?;
        let result = (|| -> Result<Value> {
            {
                let s = shared.lock().unwrap();
                ensure!(!s.stopping.load(Ordering::Relaxed), "ORCHESTRATOR_STOPPING");
                ensure!(s.verifier()?.hash == verifier.hash, "PAIRING_CODE_CHANGED");
            }
            match request["op"].as_str() {
                Some("ping") => Ok(json!({"alive":true})),
                Some("list") => Ok(json!(published(&shared, &mut workers)?)),
                Some("exchange") => {
                    ensure!(!parallel || role != Some("bulk"), "METADATA_ON_BULK_LANE");
                    let page: SyncPage = serde_json::from_value(request["page"].clone())?;
                    let info = {
                        let s = shared.lock().unwrap();
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
                        let s = shared.lock().unwrap();
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
