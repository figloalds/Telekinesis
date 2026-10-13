//! Owned by WP2.5 — stub landed by WP0.INT
//!
//! Worker `feed-offer`, `feed-exchange` and `feed-apply` (REVAMP_PLAN §6.1,
//! §6.3), served on pooled secondary stores without the engine lock. Until
//! WP2.5 lands every op is `UNSUPPORTED_OPERATION`.
use super::ctx::WorkerCtx;
use anyhow::{Result, bail};
use serde_json::Value;

pub fn dispatch(ctx: &WorkerCtx, op: &str, payload: &Value) -> Result<Value> {
    let _ = (ctx, payload);
    bail!("UNSUPPORTED_OPERATION: {op}")
}
