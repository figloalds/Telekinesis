//! The orchestrator's link to one folder worker: one control lane for RPC,
//! `feed.*`, `meta.exchange` and the `watch` long poll, and N bulk lanes for
//! `objects.fetch` / `objects.write`. Every lane is a token-authenticated
//! loopback session whose `X-Tkfs-Lane` header makes the worker enforce the
//! lane's op policy; the link also refuses object ops on the control lane
//! itself, before anything is sent.
use super::{
    sync::{OBJECT_BATCH, Object},
    worker::Ready,
};
use crate::{
    content::Kind,
    model::{content::Digest, errors::ErrorWire, protocol::LaneRole},
    session::{self, CallError, Conn, ConnHealth, ConnOptions, Priority, Reply},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

/// Deadline of one control or object call.
pub const CALL_DEADLINE: Duration = Duration::from_secs(10);
/// How long one `watch` parks on the worker.
pub const WATCH_WAIT_MS: u64 = 2000;
/// Deadline of one `watch`: its park plus slack for the reply.
pub const WATCH_DEADLINE: Duration = Duration::from_secs(4);

/// A `watch` reply: the worker's wake-up counter, its health summary and the
/// objects its readers are waiting for.
#[derive(Debug, Deserialize)]
pub struct Watch {
    pub seq: u64,
    pub heartbeat: Value,
    pub demand: Vec<(Digest, Kind)>,
}

pub struct WorkerLink {
    pub ready: Ready,
    control: Conn,
    bulk: Vec<Conn>,
}

impl WorkerLink {
    /// Dials the worker named by `ready`: one control lane, then `bulk_lanes`
    /// (at least one) bulk lanes. If any dial fails, the lanes already open
    /// are closed.
    pub fn connect(ready: &Ready, bulk_lanes: usize) -> Result<Arc<Self>, CallError> {
        let dial = |role: LaneRole| {
            session::dial_loopback_as(
                &ready.address,
                Some(&ready.token),
                role,
                ConnOptions::for_role(role),
            )
            .map_err(|error| CallError::Unavailable(error.to_string()))
        };
        let control = dial(LaneRole::Control)?;
        let mut bulk = Vec::with_capacity(bulk_lanes.max(1));
        for _ in 0..bulk_lanes.max(1) {
            match dial(LaneRole::Bulk) {
                Ok(conn) => bulk.push(conn),
                Err(error) => {
                    control.close();
                    bulk.iter().for_each(Conn::close);
                    return Err(error);
                }
            }
        }
        Ok(Arc::new(Self {
            ready: ready.clone(),
            control,
            bulk,
        }))
    }

    /// One call on the control lane. Object ops are refused here with
    /// `OP_NOT_ALLOWED`, without sending anything.
    pub fn call(
        &self,
        op: &str,
        payload: Value,
        body: Vec<u8>,
        deadline: Duration,
    ) -> Result<Reply, CallError> {
        if op.starts_with("objects.") {
            return Err(CallError::Remote(ErrorWire::coded(
                "OP_NOT_ALLOWED",
                format!("{op} is not allowed on a control lane"),
            )));
        }
        self.control
            .call(op, payload, body, Priority::Control, deadline)
    }

    /// [`Self::call`] with no body, its reply body parsed as JSON (`null`
    /// when empty).
    pub fn call_json(&self, op: &str, payload: Value) -> Result<Value, CallError> {
        let reply = self.call(op, payload, Vec::new(), CALL_DEADLINE)?;
        parse(op, &reply.body)
    }

    /// Long-polls the worker for a wake-up past `after_seq` (or pending
    /// demand), parked at most [`WATCH_WAIT_MS`].
    pub fn watch(&self, after_seq: u64) -> Result<Watch, CallError> {
        let reply = self.call(
            "watch",
            json!({"after_seq": after_seq, "wait_ms": WATCH_WAIT_MS}),
            Vec::new(),
            WATCH_DEADLINE,
        )?;
        parse("watch", &reply.body)
    }

    /// Reads up to [`OBJECT_BATCH`] objects from the worker on the bulk lane
    /// of the first digest; absent ones are left out of the reply.
    pub fn fetch(&self, wanted: &[(Digest, Kind)]) -> Result<Vec<Object>, CallError> {
        let Some((first, _)) = wanted.first() else {
            return Ok(Vec::new());
        };
        check_batch(wanted.len())?;
        let body = encode("objects.fetch", &wanted)?;
        let reply = self.bulk[self.lane_for(first)].call(
            "objects.fetch",
            json!({"repo": self.ready.repo}),
            body,
            Priority::Data,
            CALL_DEADLINE,
        )?;
        parse("objects.fetch", &reply.body)
    }

    /// Stores up to [`OBJECT_BATCH`] objects in the worker on the bulk lane of
    /// the first digest; the number stored.
    pub fn write(&self, objects: Vec<Object>) -> Result<usize, CallError> {
        let Some(first) = objects.first() else {
            return Ok(0);
        };
        check_batch(objects.len())?;
        let lane = self.lane_for(&first.digest);
        let body = encode("objects.write", &objects)?;
        drop(objects);
        let reply = self.bulk[lane].call(
            "objects.write",
            json!({"repo": self.ready.repo}),
            body,
            Priority::Data,
            CALL_DEADLINE,
        )?;
        let stored: Value = parse("objects.write", &reply.body)?;
        stored["stored"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| bad_reply("objects.write", "no stored count"))
    }

    /// The bulk lane that carries objects batched behind `digest`.
    pub fn lane_for(&self, digest: &Digest) -> usize {
        digest.0[0] as usize % self.bulk.len()
    }

    /// Every lane's role and health, control first.
    pub fn health(&self) -> Vec<(LaneRole, ConnHealth)> {
        std::iter::once((LaneRole::Control, self.control.health()))
            .chain(self.bulk.iter().map(|conn| (LaneRole::Bulk, conn.health())))
            .collect()
    }

    /// Whether every lane is still open.
    pub fn alive(&self) -> bool {
        self.lanes().all(|conn| conn.closed().is_none())
    }

    /// Closes every lane; calls in flight fail with `ConnectionLost`.
    pub fn close(&self) {
        self.lanes().for_each(Conn::close);
    }

    fn lanes(&self) -> impl Iterator<Item = &Conn> {
        std::iter::once(&self.control).chain(&self.bulk)
    }
}

fn check_batch(len: usize) -> Result<(), CallError> {
    if len > OBJECT_BATCH {
        return Err(CallError::Remote(ErrorWire::coded(
            "OBJECT_BATCH_TOO_LARGE",
            format!("{len} objects exceed the batch of {OBJECT_BATCH}"),
        )));
    }
    Ok(())
}

/// The JSON request body (nothing is sent when it does not encode).
fn encode(op: &str, value: &impl serde::Serialize) -> Result<Vec<u8>, CallError> {
    serde_json::to_vec(value).map_err(|error| {
        CallError::Remote(ErrorWire::coded(
            "PROTOCOL_VIOLATION",
            format!("{op} request does not encode: {error}"),
        ))
    })
}

fn parse<T: serde::de::DeserializeOwned>(op: &str, body: &[u8]) -> Result<T, CallError> {
    let body = if body.is_empty() {
        b"null".as_slice()
    } else {
        body
    };
    serde_json::from_slice(body).map_err(|error| bad_reply(op, &error.to_string()))
}

fn bad_reply(op: &str, detail: &str) -> CallError {
    CallError::Remote(ErrorWire::coded(
        "PROTOCOL_VIOLATION",
        format!("malformed {op} reply: {detail}"),
    ))
}
