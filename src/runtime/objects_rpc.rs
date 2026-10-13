//! Owned by WP3.2 — stub landed by WP0.INT
//!
//! Worker `objects.want`, `objects.read`, `objects.write` and `objects.report`
//! (REVAMP_PLAN §6.4), with binary bodies and leases in
//! `WorkerCtx::inflight_objects`. Until WP3.2 lands every op is
//! `UNSUPPORTED_OPERATION`.
use super::ctx::WorkerCtx;
use crate::model::errors::ErrorWire;
use serde_json::Value;

pub fn dispatch(
    ctx: &WorkerCtx,
    op: &str,
    header: &Value,
    body: &[u8],
) -> Result<(Value, Vec<u8>), ErrorWire> {
    let _ = (ctx, header, body);
    Err(ErrorWire::coded("UNSUPPORTED_OPERATION", op))
}
