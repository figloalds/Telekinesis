//! Foreground compatibility adapter for legacy whole-file identities. Parts
//! remain private spools until the existing whole-file verifier installs them.
use super::{
    content_transport::{self, ContentDaemon, Outgoing},
    objects,
    worker_client::SessionWorkerClient,
};
use crate::{
    content::scheduler::{TransferKey, TransferLease},
    model::{
        content::{Digest, ObjectKey, ObjectKind},
        content_wire as wire,
        errors::ErrorWire,
        lease::Generation,
        orchestrator::{CallError, Ctx, WorkerEndpoint},
        protocol::{
            frame::ChunkHeader,
            objects::{
                ObjectOutcome, ObjectResult, ObjectsPull, ObjectsResult, ReleaseHeader, WantItem,
                WantPriority, WantReply, WriteHeader, WriteItem, WriteReply,
            },
        },
        workspace::DemandPriority,
    },
    paired_sync::SyncRoute,
    session::{self, Conn, Incoming, Priority},
    util::Cancel,
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;
const CALL: Duration = Duration::from_secs(5);
const COST: usize = 1024 * 1024;

pub(crate) struct Target {
    pub ctx: Arc<Ctx>,
    pub state: Arc<ContentDaemon>,
    pub endpoint: WorkerEndpoint,
    pub conn: Conn,
    pub route: SyncRoute,
    pub scope: wire::PeerScope,
    pub generation: Generation,
    pub cancel: Cancel,
}
pub(crate) struct Task {
    target: Target,
    client: Arc<SessionWorkerClient>,
    item: Option<WantItem>,
    hash: String,
    session: String,
    budget: Option<TransferLease>,
    turn: Option<Outgoing>,
    hold_ms: Option<u64>,
}
impl Drop for Task {
    fn drop(&mut self) {
        // A unique, ordered c-frame identity makes even an uncertain delayed
        // release harmless to the next frame, while a/b keep their run IDs.
        if let Ok(header) = serde_json::to_value(ReleaseHeader {
            route: self.target.route.clone(),
            session: self.session.clone(),
            hashes: vec![self.hash.clone()],
            hold_ms: self.hold_ms,
        }) {
            let _ = self.client.request(
                "objects.release",
                header,
                vec![],
                Priority::Control,
                Duration::from_millis(250),
            );
        }
        drop(self.budget.take());
        drop(self.turn.take());
    }
}
fn coded(code: &str, detail: impl std::fmt::Display) -> CallError {
    CallError::Remote(ErrorWire::coded(code, detail))
}
fn encode(value: &impl serde::Serialize) -> std::result::Result<Value, CallError> {
    serde_json::to_value(value).map_err(|e| coded("INVALID_REQUEST", e))
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> std::result::Result<T, CallError> {
    serde_json::from_value(value).map_err(|e| coded("INVALID_REPLY", e))
}

pub(crate) fn prepare(target: Target, hash: &str) -> std::result::Result<Option<Task>, CallError> {
    if target.cancel.is_cancelled() || target.state.cancel.is_cancelled() {
        return Err(CallError::Cancelled);
    }
    content_transport::validate_route(&target.conn, &target.route, &target.scope)?;
    let key = objects::object_key(
        &target.ctx.installation,
        &target.route.peer,
        &target.route.repo,
        false,
        hash,
    );
    if objects::object_blocked(&key) {
        return Ok(None);
    }
    let Some(turn) = target
        .state
        .outgoing(&target.route.peer, DemandPriority::Foreground)
    else {
        return Ok(None);
    };
    let provider =
        Uuid::parse_str(&target.endpoint.instance).map_err(|e| coded("INVALID_REPLY", e))?;
    let digest = Digest::parse(hash).map_err(|e| coded("INVALID_CONTENT_DIGEST", e))?;
    let Some(budget) = target
        .state
        .scheduler
        .try_acquire(
            TransferKey {
                provider,
                object: ObjectKey {
                    kind: ObjectKind::Bytes,
                    digest,
                },
            },
            &target.route.repo,
            &target.route.peer,
            DemandPriority::Foreground,
            COST,
        )
        .map_err(|e| CallError::Remote(ErrorWire::from_error(&e)))?
    else {
        return Ok(None);
    };
    let frame = super::lease_clock::next(&target.ctx)?.run;
    let session = crate::model::lease::qualify(
        &format!(
            "{}:c{frame:016x}",
            objects::pull_holder(&target.route.peer, target.scope.lane)
        ),
        target.generation,
    )?;
    let client = target.state.planner(&target.endpoint)?;
    let mut task = Task {
        hash: hash.to_owned(),
        session,
        client,
        item: None,
        target,
        budget: Some(budget),
        turn: Some(turn),
        hold_ms: None,
    };
    let reply = task.client.request_cancellable(
        wire::LEGACY_PREPARE,
        encode(&wire::LegacyPrepare {
            route: task.target.route.clone(),
            lane: task.target.scope.lane,
            session: task.session.clone(),
            hash: hash.to_owned(),
        })?,
        vec![],
        Duration::from_millis(50),
        &|| task.target.cancel.is_cancelled() || task.target.state.cancel.is_cancelled(),
    )?;
    let wanted: WantReply = decode(reply.header)?;
    if wanted.items.is_empty() {
        return Ok(None);
    }
    if wanted.items.len() != 1
        || wanted.items[0].hash != hash
        || wanted.items[0].priority != WantPriority::Demand
    {
        return Err(coded("INVALID_REPLY", "legacy foreground selection"));
    }
    task.item = wanted.items.into_iter().next();
    Ok(Some(task))
}
impl Task {
    pub(crate) fn perform(mut self) -> std::result::Result<bool, CallError> {
        let item = self
            .item
            .take()
            .ok_or_else(|| coded("INVALID_REPLY", "legacy offer absent"))?;
        let start = item.offset;
        let unknown = objects::size_unknown(&item);
        let end = item.offset
            + item
                .size
                .saturating_sub(item.offset)
                .min(wire::LEGACY_BYTES as u64);
        let client = self.target.state.worker(&self.target.endpoint)?;
        let request = ObjectsPull {
            repo: self.target.scope.repo.clone(),
            sender: self.target.scope.sender.clone(),
            receiver: self.target.scope.receiver.clone(),
            lane: self.target.scope.lane,
            items: vec![item.clone()],
            budget_bytes: wire::LEGACY_BYTES as u64,
        };
        let begin = self.target.conn.call_cancellable(
            wire::LEGACY_BEGIN,
            encode(&request)?,
            vec![],
            Priority::Control,
            CALL,
            &|| self.target.cancel.is_cancelled() || self.target.state.cancel.is_cancelled(),
        );
        let (fallback, peer_credit) = match begin {
            Ok(reply) => (
                false,
                Some(PeerCredit {
                    conn: self.target.conn.clone(),
                    scope: self.target.scope.clone(),
                    token: decode(reply.header)?,
                }),
            ),
            Err(error) if error.code() == "UNSUPPORTED_OPERATION" => (true, None),
            Err(error) => return Err(error),
        };
        let mut next = start;
        let mut chunks = 0;
        let mut consume = |chunk: ChunkHeader,
                           bytes: &[u8]|
         -> std::result::Result<(), ErrorWire> {
            let chunk_end = chunk
                .offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| ErrorWire::coded("INVALID_REPLY", "legacy range overflow"))?;
            if chunk.hash.as_deref() != Some(&self.hash)
                || chunk.offset != next
                || bytes.len() > wire::FRAME_BYTES
                || (!unknown && (chunk_end > end || chunk.total != item.size))
                || (unknown && (chunks != 0 || !bytes.is_empty()))
                || (bytes.is_empty() && !unknown && item.size != 0)
            {
                return Err(ErrorWire::coded(
                    "INVALID_REPLY",
                    "legacy foreground frame range",
                ));
            }
            let reply = client
                .request_cancellable(
                    "objects.write",
                    encode(&WriteHeader {
                        route: self.target.route.clone(),
                        session: self.session.clone(),
                        items: vec![WriteItem {
                            hash: self.hash.clone(),
                            offset: chunk.offset,
                            total: chunk.total,
                            len: bytes.len() as u64,
                            last: chunk_end == chunk.total,
                        }],
                    })
                    .map_err(|e| e.to_wire())?,
                    bytes.to_vec(),
                    CALL,
                    &|| {
                        self.target.cancel.is_cancelled() || self.target.state.cancel.is_cancelled()
                    },
                )
                .map_err(|e| e.to_wire())?;
            let reply: WriteReply = decode(reply.header).map_err(|e| e.to_wire())?;
            if reply.results.len() != 1 || reply.results[0].hash != self.hash {
                return Err(ErrorWire::coded("INVALID_REPLY", "legacy write receipt"));
            }
            match &reply.results[0].outcome {
                ObjectOutcome::Progressed { .. }
                | ObjectOutcome::Completed
                | ObjectOutcome::Skipped => {}
                ObjectOutcome::Failed(e) => return Err(e.clone()),
                other => {
                    return Err(ErrorWire::coded(
                        other.code().unwrap_or("IN_FLIGHT_ELSEWHERE"),
                        "legacy write ownership/outcome",
                    ));
                }
            }
            next = chunk_end;
            chunks += 1;
            Ok(())
        };
        let header = if let Some(credit) = peer_credit.as_ref() {
            encode(&wire::LegacyPull {
                request: request.clone(),
                token: credit.token,
            })?
        } else {
            encode(&request)?
        };
        let read = self.target.conn.call_streaming_cancellable(
            if fallback {
                "objects.pull"
            } else {
                wire::LEGACY_PULL
            },
            header,
            vec![],
            Priority::Control,
            Some(CALL),
            CALL,
            &mut consume,
            &|| self.target.cancel.is_cancelled() || self.target.state.cancel.is_cancelled(),
        );
        let reply = read?;
        let result: ObjectsResult = if fallback {
            decode(reply.header)?
        } else {
            let reply: wire::LegacyReply = decode(reply.header)?;
            if peer_credit
                .as_ref()
                .is_none_or(|credit| credit.token != reply.token)
            {
                return Err(coded("INVALID_REPLY", "legacy credit identity"));
            }
            reply.result
        };
        drop(peer_credit);
        if result.results.len() != 1 || result.results[0].hash != self.hash {
            return Err(coded("INVALID_REPLY", "legacy outcome identity"));
        }
        if result.results[0].bytes != next - start {
            return Err(coded("INVALID_REPLY", "legacy byte receipt"));
        }
        match &result.results[0].outcome {
            ObjectOutcome::Completed
            | ObjectOutcome::Progressed { .. }
            | ObjectOutcome::Skipped => {}
            outcome => {
                let code = outcome.code().unwrap_or("NOT_AVAILABLE");
                let key = objects::object_key(
                    &self.target.ctx.installation,
                    &self.target.route.peer,
                    &self.target.route.repo,
                    false,
                    &self.hash,
                );
                let delay = if matches!(
                    outcome,
                    ObjectOutcome::NotAvailable | ObjectOutcome::NotPermitted
                ) {
                    objects::object_unserved(&key)
                } else {
                    objects::object_failed(&key, code)
                };
                self.hold_ms = Some(delay.as_millis().min(u64::MAX as u128) as u64);
                return Err(coded(code, "legacy source outcome"));
            }
        }
        if chunks == 0 || (!unknown && next != end) {
            return Err(coded("INVALID_REPLY", "legacy foreground short stream"));
        }
        objects::object_installed(&objects::object_key(
            &self.target.ctx.installation,
            &self.target.route.peer,
            &self.target.route.repo,
            false,
            &self.hash,
        ));
        Ok(true)
    }
}

struct PeerCredit {
    conn: Conn,
    scope: wire::PeerScope,
    token: wire::Token,
}
impl Drop for PeerCredit {
    fn drop(&mut self) {
        if let Ok(header) = encode(&wire::PeerAck {
            scope: self.scope.clone(),
            token: self.token,
        }) {
            let _ = self.conn.call(
                wire::ACK,
                header,
                vec![],
                Priority::Control,
                Duration::from_millis(250),
            );
        }
    }
}

pub(crate) fn serve(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: Incoming) {
    let Incoming {
        op,
        header,
        body,
        responder,
        ..
    } = req;
    let mut active = None;
    let result = (|| -> Result<Value> {
        ensure!(body.is_empty(), "INVALID_REQUEST");
        let (request, token) = if op == wire::LEGACY_BEGIN {
            (serde_json::from_value::<ObjectsPull>(header)?, None)
        } else {
            let pull: wire::LegacyPull = serde_json::from_value(header)?;
            (pull.request, Some(pull.token))
        };
        ensure!(
            request.items.len() == 1 && request.budget_bytes <= wire::LEGACY_BYTES as u64,
            "CONTENT_BUFFER_LIMIT"
        );
        let item = &request.items[0];
        ensure!(item.priority == WantPriority::Demand, "INVALID_REQUEST");
        let scope = wire::PeerScope {
            repo: request.repo,
            sender: request.sender,
            receiver: request.receiver,
            lane: request.lane,
        };
        let (route, endpoint) =
            content_transport::bind(ctx, peer, &scope).map_err(anyhow::Error::new)?;
        ensure!(scope.lane.owns(&item.hash), "OBJECT_ON_WRONG_LANE");
        let digest = Digest::parse(&item.hash)?;
        let object = ObjectKey {
            kind: ObjectKind::Bytes,
            digest,
        };
        let state = content_transport::daemon(ctx)?;
        if token.is_none() {
            let provider = Uuid::parse_str(&endpoint.instance)?;
            let lease = state
                .scheduler
                .try_acquire(
                    TransferKey { provider, object },
                    &scope.repo,
                    &peer.installation,
                    DemandPriority::Foreground,
                    COST,
                )?
                .ok_or_else(|| anyhow::anyhow!("CONTENT_RESOURCE_LIMIT"))?;
            let token = state.retain_credit(peer, scope, responder.connection(), lease);
            return Ok(serde_json::to_value(token)?);
        }
        let token = token.unwrap();
        active = Some(state.start_legacy(peer, &scope, token, object)?);
        let client = state.worker(&endpoint).map_err(anyhow::Error::new)?;
        let span = objects::allot(std::slice::from_ref(item), request.budget_bytes)[0];
        let unknown = objects::size_unknown(item);
        let mut at = span.0;
        let mut outcome = ObjectOutcome::Completed;
        loop {
            content_transport::bind(ctx, peer, &scope).map_err(anyhow::Error::new)?;
            let len = (span.1 - at).min(wire::FRAME_BYTES as u64);
            let read = client.request_cancellable(
                wire::LEGACY_READ,
                json!({"route":route,"hash":item.hash,"offset":at,"len":len}),
                vec![],
                CALL,
                &|| responder.cancelled() || state.cancel.is_cancelled(),
            );
            match read {
                Ok(reply) => {
                    let total = reply
                        .header
                        .get("total")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| anyhow::anyhow!("INVALID_REPLY"))?;
                    if reply.body.len() as u64 != len || (!unknown && total != item.size) {
                        outcome = ObjectOutcome::SizeMismatch;
                        break;
                    }
                    content_transport::bind(ctx, peer, &scope).map_err(anyhow::Error::new)?;
                    responder
                        .chunk(
                            ChunkHeader {
                                id: 0,
                                hash: Some(item.hash.clone()),
                                offset: at,
                                total,
                                last: at + len == total,
                            },
                            &reply.body,
                        )
                        .map_err(anyhow::Error::new)?;
                    at += len;
                }
                Err(error) => {
                    outcome = objects::read_outcome(&error);
                    break;
                }
            }
            if at == span.1 {
                break;
            }
        }
        ensure!(!responder.cancelled(), "CONTENT_CANCELLED");
        Ok(serde_json::to_value(wire::LegacyReply {
            result: ObjectsResult {
                results: vec![ObjectResult {
                    hash: item.hash.clone(),
                    outcome,
                    bytes: at - span.0,
                }],
            },
            token,
        })?)
    })();
    match result {
        Ok(header) => responder.reply(header, vec![]),
        Err(error) => responder.error(ErrorWire::from_error(&error)),
    }
    drop(active);
}
