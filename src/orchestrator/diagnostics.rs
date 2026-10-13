//! Owned by WP4.2 — stub landed by WP0.INT
//!
//! The `diagnostics` bundle (REVAMP_PLAN §14 WP4.2): status, activity, folder
//! heartbeats, a redacted catalog, log tails and versions under
//! `data/diagnostics/<ts>/`. The stub returns only the schema number.
use crate::model::orchestrator::Ctx;
use serde_json::{Value, json};
use std::sync::Arc;

pub fn collect(ctx: &Arc<Ctx>) -> anyhow::Result<Value> {
    let _ = ctx;
    Ok(json!({"schema":1}))
}
