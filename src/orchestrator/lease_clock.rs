//! A persisted daemon incarnation plus an in-process monotonic run number.
use crate::{
    model::{
        errors::ErrorWire,
        lease::{Generation, PullRun, PushRun},
        orchestrator::{Ctx, PeerContext},
    },
    util::{Cancel, lock::lock},
};
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU64, Ordering},
};

type InboundPool = (u32, Arc<Mutex<u64>>);
pub struct Clock {
    incarnation: u64,
    next: AtomicU64,
    pools: Mutex<BTreeMap<usize, (Weak<PeerContext>, Generation)>>,
    inbound: Mutex<BTreeMap<String, InboundPool>>,
}

pub struct PushSession {
    pub run: PushRun,
    current_pool: Arc<Mutex<u64>>,
}
impl PushSession {
    pub fn current(&self) -> bool {
        *lock(&self.current_pool) == self.run.layout
    }
    pub(crate) fn while_current<T>(&self, apply: impl FnOnce() -> T) -> Option<T> {
        let current = lock(&self.current_pool);
        (*current == self.run.layout).then(apply)
    }
}
impl Clock {
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            next: AtomicU64::new(0),
            pools: Mutex::new(BTreeMap::new()),
            inbound: Mutex::new(BTreeMap::new()),
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

    /// Allocated only after an authenticated bulk handshake, before requests.
    pub fn accept_push(&self, peer: &str, count: u32) -> Result<Arc<PushSession>, ErrorWire> {
        if peer.is_empty() || peer.len() > 128 || !(1..=15).contains(&count) {
            return Err(ErrorWire::coded("INVALID_REQUEST", "inbound pool identity"));
        }
        let mut inbound = lock(&self.inbound);
        if !inbound.contains_key(peer) && inbound.len() >= crate::model::lease::MAX_RUN_FENCES {
            return Err(ErrorWire::coded(
                "CONTENT_RESOURCE_LIMIT",
                "inbound pool identity capacity",
            ));
        }
        let pool = match inbound.get_mut(peer) {
            Some((prior, pool)) => {
                if *prior != count {
                    *lock(pool) = self.next()?.run;
                    *prior = count;
                }
                pool.clone()
            }
            None => {
                let pool = Arc::new(Mutex::new(self.next()?.run));
                inbound.insert(peer.to_owned(), (count, pool.clone()));
                pool
            }
        };
        let generation = self.next()?;
        let layout = *lock(&pool);
        Ok(Arc::new(PushSession {
            run: PushRun { generation, layout },
            current_pool: pool,
        }))
    }
}

pub(crate) fn accept_push(
    ctx: &Arc<Ctx>,
    peer: &str,
    count: u32,
) -> Result<Arc<PushSession>, ErrorWire> {
    super::ctx::extras(ctx)
        .lease_clock
        .get_or_init(|| Clock::new(0))
        .accept_push(peer, count)
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
