//! Owned by WP2.2 — stub landed by WP0.INT
//!
//! Management API v2 actions (REVAMP_PLAN §3.5, §14 WP2.2): `status`,
//! `subscribe`, `attach`, `detach`, `pause-sync`, `resume-sync`,
//! `override-gate`, `force-stop`, `retry`, `revalidate` and `diagnostics`.
use crate::model::orchestrator::Ctx;
use anyhow::bail;
use serde_json::Value;
use std::sync::Arc;

pub fn handle(ctx: &Arc<Ctx>, action: super::api::Action) -> anyhow::Result<Value> {
    let _ = (ctx, action);
    bail!("NOT_IMPLEMENTED")
}
