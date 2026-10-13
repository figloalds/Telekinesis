//! Owned by WP2.1 — stub landed by WP0.INT
//!
//! Management dispatch over the actor orchestrator (REVAMP_PLAN §3.5): the
//! legacy actions with their compatibility shapes (§7.3), and the new
//! variants forwarded to `manage_v2::handle`. Until WP2.1 the daemon keeps
//! using the legacy `orchestrator::manage(&Shared, Action)`.
use crate::model::orchestrator::Ctx;
use anyhow::bail;
use serde_json::Value;
use std::sync::Arc;

pub fn manage(ctx: &Arc<Ctx>, action: super::api::Action) -> anyhow::Result<Value> {
    let _ = (ctx, action);
    bail!("NOT_IMPLEMENTED")
}
