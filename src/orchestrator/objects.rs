//! Owned by WP2.7 (interim) and WP3.3 — stub landed by WP0.INT
//!
//! One bulk lane of a peer (lanes 1..N-1): the interim `objects.exchange` path
//! in Wave 2 (REVAMP_PLAN §6.5), objects v3 in Wave 3 (§6.4). The stub holds
//! the lane open and does nothing until it is cancelled.
use crate::{
    model::{
        orchestrator::{Ctx, PeerContext},
        protocol::LaneSpec,
    },
    session,
    util::cancel::Cancel,
};
use std::sync::Arc;

pub fn run_bulk_lane(
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: session::Conn,
    cancel: Cancel,
) {
    let _held = (ctx, peer, lane, conn);
    cancel.wait();
}
