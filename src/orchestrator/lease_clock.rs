//! A persisted daemon incarnation plus an in-process monotonic run number.
use crate::{
    model::{
        errors::ErrorWire,
        lease::{Generation, PullRun},
        orchestrator::{Ctx, PeerContext},
    },
    util::{Cancel, lock::lock},
};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU64, Ordering},
};

pub struct Clock {
    incarnation: u64,
    next: AtomicU64,
    pools: Mutex<BTreeMap<usize, (Weak<PeerContext>, Generation)>>,
}
impl Clock {
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            next: AtomicU64::new(0),
            pools: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn next(&self) -> Result<Generation, ErrorWire> {
        // Contexts manually constructed by tests have no catalog. Their zero
        // incarnation has process-wide ordering, without touching host files.
        static EPHEMERAL: AtomicU64 = AtomicU64::new(0);
        let counter = if self.incarnation == 0 {
            &EPHEMERAL
        } else {
            &self.next
        };
        let old = counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| ErrorWire::coded("CATALOG_UNAVAILABLE", "lease generation exhausted"))?;
        Ok(Generation {
            incarnation: self.incarnation,
            run: old + 1,
        })
    }

    pub fn register_peer(&self, peer: &Arc<PeerContext>) -> Result<Generation, ErrorWire> {
        let mut pools = lock(&self.pools);
        let key = Arc::as_ptr(peer) as usize;
        if let Some((_, generation)) = pools.get(&key) {
            return Ok(*generation);
        }
        if pools.len() >= crate::model::lease::MAX_RUN_FENCES {
            pools.retain(|_, (peer, _)| peer.strong_count() > 0);
            if pools.len() >= crate::model::lease::MAX_RUN_FENCES {
                return Err(ErrorWire::coded(
                    "CONTENT_RESOURCE_LIMIT",
                    "peer pool generation capacity",
                ));
            }
        }
        let generation = self.next()?;
        pools.insert(key, (Arc::downgrade(peer), generation));
        Ok(generation)
    }

    pub fn pull(&self, peer: &Arc<PeerContext>, cancel: &Cancel) -> Result<PullRun, ErrorWire> {
        if cancel.is_cancelled() {
            return Err(ErrorWire::coded("CONTENT_CANCELLED", "pull run cancelled"));
        }
        let layout = lock(&self.pools)
            .get(&(Arc::as_ptr(peer) as usize))
            .map(|(_, g)| *g);
        let layout = match layout {
            Some(layout) => layout,
            // Hand-built fixture contexts have no supervisor constructor.
            None if self.incarnation == 0 => self.register_peer(peer)?,
            None => {
                return Err(ErrorWire::coded(
                    "CONTENT_RESOURCE_LIMIT",
                    "unregistered peer pool",
                ));
            }
        };
        Ok(PullRun {
            generation: self.next()?,
            layout: layout.run,
        })
    }
}
pub fn next(ctx: &Arc<Ctx>) -> Result<Generation, ErrorWire> {
    super::ctx::extras(ctx)
        .lease_clock
        .get_or_init(|| Clock::new(0))
        .next()
}

pub(crate) fn register_peer(ctx: &Arc<Ctx>, peer: &Arc<PeerContext>) -> Result<(), ErrorWire> {
    super::ctx::extras(ctx)
        .lease_clock
        .get_or_init(|| Clock::new(0))
        .register_peer(peer)
        .map(|_| ())
}

pub(crate) fn pull(
    ctx: &Arc<Ctx>,
    peer: &Arc<PeerContext>,
    cancel: &Cancel,
) -> Result<PullRun, ErrorWire> {
    super::ctx::extras(ctx)
        .lease_clock
        .get_or_init(|| Clock::new(0))
        .pull(peer, cancel)
}
