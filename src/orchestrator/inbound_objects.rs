//! Owned by WP2.8 and WP3.3 — stub landed by WP0.INT
//!
//! Inbound `objects.*` requests on an authenticated bulk session (REVAMP_PLAN
//! §3.8): the interim `objects.exchange` in Wave 2, objects v3 in Wave 3. The
//! stub answers every request with a Remote `UNSUPPORTED_OPERATION` error,
//! which leaves the session usable (invariant 17).
use crate::{model::errors::ErrorWire, model::orchestrator::Ctx, session};
use std::sync::Arc;

pub fn handle(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming) {
    let _ = (ctx, peer);
    let session::Incoming { op, responder, .. } = req;
    responder.error(ErrorWire::coded("UNSUPPORTED_OPERATION", op));
}
