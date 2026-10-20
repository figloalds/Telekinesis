//! Bounded global transfer admission and independent range waiters. No I/O is
//! performed while state is locked. Leases release only their own generation.
use super::{ReadContext, local::ObjectKey};
use crate::{
    model::workspace::{Demand, DemandPriority, limits},
    util::{
        Cancel,
        lock::lock,
        phases::{Phase, PhaseCounters},
    },
};
use anyhow::{Result, ensure};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Instant,
};
use uuid::Uuid;

pub const DEMAND_BYTES: usize = 2 * 1024 * 1024;
pub const GLOBAL_DEMAND_BYTES: usize = 64 * 1024 * 1024;
pub const GLOBAL_DEMAND_WAITERS: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransferKey {
    pub provider: Uuid,
    pub object: ObjectKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseToken {
    pub owner: Uuid,
    pub generation: u64,
}

struct Active {
    token: LeaseToken,
    repository: String,
    peer: String,
    priority: DemandPriority,
    bytes: usize,
}
struct Waiter {
    token: Uuid,
    demand: Demand,
    deadline: Instant,
    cancel: Cancel,
    bytes: usize,
    provider: Uuid,
    queued: Instant,
    admitted: bool,
    satisfied: bool,
}
#[derive(Default)]
struct State {
    generation: u64,
    active: BTreeMap<TransferKey, Active>,
    waiters: BTreeMap<Uuid, Waiter>,
    waiter_bytes: usize,
    waiter_turn: BTreeMap<Uuid, usize>,
}

#[derive(Default)]
pub struct ContentScheduler {
    state: Mutex<State>,
    pub counters: PhaseCounters,
}

pub struct TransferLease {
    scheduler: Weak<ContentScheduler>,
    key: TransferKey,
    token: LeaseToken,
}
impl TransferLease {
    pub fn token(&self) -> LeaseToken {
        self.token
    }
    pub fn key(&self) -> TransferKey {
        self.key
    }
}
impl Drop for TransferLease {
    fn drop(&mut self) {
        if let Some(owner) = self.scheduler.upgrade() {
            owner.release(self.key, self.token);
        }
    }
}

pub struct DemandLease {
    scheduler: Weak<ContentScheduler>,
    waiter: Uuid,
    token: Uuid,
}
impl Drop for DemandLease {
    fn drop(&mut self) {
        if let Some(owner) = self.scheduler.upgrade() {
            let mut state = lock(&owner.state);
            if state
                .waiters
                .get(&self.waiter)
                .is_some_and(|waiter| waiter.token == self.token)
            {
                let removed = state.waiters.remove(&self.waiter).unwrap();
                state.waiter_bytes -= removed.bytes;
            }
        }
    }
}

#[derive(Clone)]
pub struct DemandSnapshot {
    pub demand: Demand,
    pub deadline: Instant,
}

impl ContentScheduler {
    fn prune(state: &mut State, now: Instant) {
        state.waiters.retain(|_, waiter| {
            let keep = waiter.deadline > now && !waiter.cancel.is_cancelled();
            if !keep {
                state.waiter_bytes -= waiter.bytes;
            }
            keep
        });
    }

    pub fn demand(self: &Arc<Self>, demand: Demand, context: &ReadContext) -> Result<DemandLease> {
        self.demand_on(Uuid::nil(), demand, context)
    }

    pub fn demand_on(
        self: &Arc<Self>,
        provider: Uuid,
        demand: Demand,
        context: &ReadContext,
    ) -> Result<DemandLease> {
        context.check()?;
        demand.validate()?;
        let bytes = serde_json::to_vec(&demand)?.len();
        let mut state = lock(&self.state);
        Self::prune(&mut state, Instant::now());
        ensure!(
            !state.waiters.contains_key(&demand.waiter),
            "DEMAND_ID_REUSED"
        );
        let provider_waiters = state
            .waiters
            .values()
            .filter(|waiter| waiter.provider == provider);
        let provider_count = provider_waiters.clone().count();
        let provider_bytes: usize = provider_waiters.map(|waiter| waiter.bytes).sum();
        ensure!(
            provider_count < limits::DEMAND_WAITERS
                && state.waiters.len() < GLOBAL_DEMAND_WAITERS
                && bytes <= DEMAND_BYTES.saturating_sub(provider_bytes)
                && bytes <= GLOBAL_DEMAND_BYTES.saturating_sub(state.waiter_bytes),
            "CONTENT_RESOURCE_LIMIT"
        );
        let token = Uuid::new_v4();
        let waiter = demand.waiter;
        state.waiters.insert(
            waiter,
            Waiter {
                token,
                demand,
                deadline: context.deadline,
                cancel: context.cancel.clone(),
                bytes,
                provider,
                queued: Instant::now(),
                admitted: false,
                satisfied: false,
            },
        );
        state.waiter_bytes += bytes;
        Ok(DemandLease {
            scheduler: Arc::downgrade(self),
            waiter,
            token,
        })
    }

    pub fn demands(&self, repository: &str, reference: &str) -> Vec<DemandSnapshot> {
        self.snapshots(Uuid::nil(), repository)
            .into_iter()
            .filter(|snapshot| snapshot.demand.reference == reference)
            .collect()
    }

    pub fn snapshots(&self, provider: Uuid, repository: &str) -> Vec<DemandSnapshot> {
        let mut state = lock(&self.state);
        Self::prune(&mut state, Instant::now());
        state
            .waiters
            .values()
            .filter(|waiter| {
                waiter.provider == provider
                    && waiter.demand.revision.workspace.repository == repository
                    && !waiter.satisfied
            })
            .map(|waiter| DemandSnapshot {
                demand: waiter.demand.clone(),
                deadline: waiter.deadline,
            })
            .collect()
    }

    pub fn work_snapshots(&self, provider: Uuid, repository: &str) -> Vec<DemandSnapshot> {
        let mut snapshots = self.snapshots(provider, repository);
        if snapshots.is_empty() {
            return snapshots;
        }
        let mut state = lock(&self.state);
        if state.waiter_turn.len() >= GLOBAL_DEMAND_WAITERS
            && !state.waiter_turn.contains_key(&provider)
            && let Some(first) = state.waiter_turn.keys().next().copied()
        {
            state.waiter_turn.remove(&first);
        }
        let turn = state.waiter_turn.entry(provider).or_default();
        let len = snapshots.len();
        snapshots.rotate_left(*turn % len);
        *turn = turn.wrapping_add(16);
        snapshots
    }

    /// No descriptor cloning or disk I/O on the daemon's demand probe.
    pub fn eligible_sources(&self, provider: Uuid, repo: &str, sources: &[String]) -> Vec<String> {
        let mut state = lock(&self.state);
        Self::prune(&mut state, Instant::now());
        sources
            .iter()
            .filter(|source| {
                state.waiters.values().any(|w| {
                    w.provider == provider
                        && w.demand.revision.workspace.repository == repo
                        && !w.satisfied
                        && (w.demand.eligible_peers.is_empty()
                            || w.demand.eligible_peers.contains(source))
                })
            })
            .cloned()
            .collect()
    }

    pub fn admitted(&self, waiters: &[Uuid]) {
        let mut state = lock(&self.state);
        Self::prune(&mut state, Instant::now());
        for id in waiters {
            if let Some(waiter) = state.waiters.get_mut(id)
                && !waiter.admitted
            {
                waiter.admitted = true;
                self.counters.record(
                    Phase::DemandAdmission,
                    0,
                    waiter.queued.elapsed().as_micros().min(u64::MAX as u128) as u64,
                );
            }
        }
    }

    pub fn satisfied(&self, id: Uuid) {
        if let Some(waiter) = lock(&self.state).waiters.get_mut(&id) {
            waiter.satisfied = true;
        }
    }

    pub fn active_from(&self, key: TransferKey, peer: &str) -> bool {
        lock(&self.state)
            .active
            .get(&key)
            .is_some_and(|work| work.peer == peer)
    }

    /// Other priority classes each retain one forward-progress slot, globally,
    /// per peer and per repository. Borrowing cannot consume those reservations.
    fn capacity<'a>(
        active: impl Iterator<Item = &'a Active>,
        max: usize,
        priority: DemandPriority,
    ) -> bool {
        let mut count = 0;
        let mut present = [false; 3];
        for work in active {
            count += 1;
            present[rank(work.priority)] = true;
        }
        let reserved = (0..3)
            .filter(|index| *index != rank(priority) && !present[*index])
            .count();
        count < max.saturating_sub(reserved)
    }

    /// None joins an existing installation or waits for bounded capacity. A
    /// caller's independent demand remains registered while another owner works.
    pub fn try_acquire(
        self: &Arc<Self>,
        key: TransferKey,
        repository: &str,
        peer: &str,
        priority: DemandPriority,
        bytes: usize,
    ) -> Result<Option<TransferLease>> {
        ensure!(
            !repository.is_empty()
                && repository.len() <= 128
                && !peer.is_empty()
                && peer.len() <= 128,
            "INVALID_DEMAND_SCOPE"
        );
        ensure!(bytes <= limits::READ_BYTES, "CONTENT_BUFFER_LIMIT");
        let mut state = lock(&self.state);
        if state.active.contains_key(&key)
            || !Self::capacity(state.active.values(), limits::TRANSFER_JOBS, priority)
            || !Self::capacity(
                state
                    .active
                    .values()
                    .filter(|work| work.repository == repository),
                limits::REPOSITORY_JOBS,
                priority,
            )
            || !Self::capacity(
                state.active.values().filter(|work| work.peer == peer),
                limits::PEER_JOBS,
                priority,
            )
        {
            return Ok(None);
        }
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("CONTENT_RESOURCE_LIMIT"))?;
        let token = LeaseToken {
            owner: Uuid::new_v4(),
            generation: state.generation,
        };
        state.active.insert(
            key,
            Active {
                token,
                repository: repository.into(),
                peer: peer.into(),
                priority,
                bytes,
            },
        );
        Ok(Some(TransferLease {
            scheduler: Arc::downgrade(self),
            key,
            token,
        }))
    }

    pub fn release(&self, key: TransferKey, token: LeaseToken) -> bool {
        let mut state = lock(&self.state);
        if state
            .active
            .get(&key)
            .is_some_and(|work| work.token == token)
        {
            state.active.remove(&key);
            true
        } else {
            false
        }
    }

    pub fn owns(&self, key: TransferKey, token: LeaseToken) -> bool {
        lock(&self.state)
            .active
            .get(&key)
            .is_some_and(|work| work.token == token)
    }

    pub fn usage(&self) -> (usize, usize, usize, usize) {
        let mut state = lock(&self.state);
        Self::prune(&mut state, Instant::now());
        (
            state.active.len(),
            state.active.values().map(|work| work.bytes).sum(),
            state.waiters.len(),
            state.waiter_bytes,
        )
    }
}

fn rank(priority: DemandPriority) -> usize {
    match priority {
        DemandPriority::Structural => 0,
        DemandPriority::Foreground => 1,
        DemandPriority::Background => 2,
    }
}
