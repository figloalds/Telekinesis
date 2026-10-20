//! Range-to-transfer preparation on an object-owned secondary connection.
//! The caller holds the returned lease across its bounded network operation.
use super::{
    LocalContent, ReadContext,
    receipt::{self, ReceivedFrame},
    scheduler::{ContentScheduler, TransferKey, TransferLease},
};
use crate::{
    core::{Store, content_objects::AdmittedEdge},
    model::{
        content::{MAX_NODE_BYTES, ObjectKind},
        content_wire::{PrepareMode, Token},
        protocol::LaneSpec,
        workspace::DemandPriority,
    },
};
use anyhow::{Result, ensure};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

pub struct ContentTransfer {
    pub item: AdmittedEdge,
    pub source: String,
    pub priority: DemandPriority,
    lease: TransferLease,
    scheduler: Arc<ContentScheduler>,
}

impl ContentTransfer {
    pub fn token(&self) -> Token {
        let token = self.lease.token();
        Token {
            owner: token.owner,
            generation: token.generation,
        }
    }
    pub fn complete(
        self,
        store: &mut Store,
        provider: &LocalContent,
        bytes: &[u8],
        context: &ReadContext,
    ) -> Result<()> {
        ensure!(
            self.scheduler.owns(self.lease.key(), self.lease.token()),
            "STALE_ADMISSION_GENERATION"
        );
        receipt::receive(
            store,
            provider,
            ReceivedFrame {
                reference: &self.item.reference,
                generation: self.item.generation,
                key: self.item.edge.key,
                bytes,
                source: &self.source,
            },
            context,
        )
    }

    pub fn failed(self, store: &mut Store) -> Result<()> {
        ensure!(
            self.scheduler.owns(self.lease.key(), self.lease.token()),
            "STALE_ADMISSION_GENERATION"
        );
        store.backoff_content_source(self.item.edge.key, &self.source)
    }
}

/// At most 32 verifications and 1 MiB of logical byte work per call. A verified
/// local object heals its index before any network want can be returned.
pub fn prepare_transfer(
    store: &mut Store,
    provider: &LocalContent,
    scheduler: &Arc<ContentScheduler>,
    source: &str,
    context: &ReadContext,
) -> Result<Option<ContentTransfer>> {
    prepare_transfer_scoped(
        store,
        provider,
        scheduler,
        source,
        &TransferSelection {
            mode: PrepareMode::Mixed,
            lane: LaneSpec::new(0, 1),
        },
        context,
    )
}

pub struct TransferSelection {
    pub mode: PrepareMode,
    pub lane: LaneSpec,
}

pub fn prepare_transfer_scoped(
    store: &mut Store,
    provider: &LocalContent,
    scheduler: &Arc<ContentScheduler>,
    source: &str,
    selection: &TransferSelection,
    context: &ReadContext,
) -> Result<Option<ContentTransfer>> {
    Ok(prepare_transfer_slice(store, provider, scheduler, source, selection, context)?.0)
}

pub fn prepare_transfer_slice(
    store: &mut Store,
    provider: &LocalContent,
    scheduler: &Arc<ContentScheduler>,
    source: &str,
    selection: &TransferSelection,
    context: &ReadContext,
) -> Result<(Option<ContentTransfer>, bool)> {
    let mut progress = false;
    let transfer = prepare_inner(
        store,
        provider,
        scheduler,
        source,
        selection,
        context,
        &mut progress,
    )?;
    Ok((transfer, progress))
}

fn prepare_inner(
    store: &mut Store,
    provider: &LocalContent,
    scheduler: &Arc<ContentScheduler>,
    source: &str,
    selection: &TransferSelection,
    context: &ReadContext,
    progress: &mut bool,
) -> Result<Option<ContentTransfer>> {
    ensure!(selection.lane.is_valid(), "INVALID_BULK_LANE");
    context.check()?;
    let demands = scheduler.work_snapshots(provider.identity(), &store.repo);
    let mut candidates: Vec<(AdmittedEdge, DemandPriority, Vec<Uuid>)> = vec![];
    let mut constrained = BTreeSet::new();
    let mut seeded = BTreeSet::new();
    for snapshot in &demands {
        let demand = &snapshot.demand;
        constrained.insert(demand.reference.clone());
        if !matches!(selection.mode, PrepareMode::Foreground | PrepareMode::Mixed) {
            continue;
        }
        if !demand.eligible_peers.is_empty()
            && !demand.eligible_peers.iter().any(|peer| peer == source)
        {
            continue;
        }
        if store.validate_content_demand(demand).is_err() {
            continue;
        }
        if store.content_range_indexed(&demand.reference, demand.range)? {
            scheduler.admitted(&[demand.waiter]);
            scheduler.satisfied(demand.waiter);
            continue;
        }
        // A reader may arrive before the durable admission consumer catches
        // up. Heal the one current root without waiting for a maintenance tick
        // or replaying unrelated roots; aliases also cover a fresh regrant.
        if seeded.insert(demand.reference.clone()) {
            store.seed_content_reference(&demand.reference)?;
        }
        for edge in store.content_range_candidates(&demand.reference, demand.range)? {
            if !selection.lane.owns(&edge.edge.key.digest.to_string()) {
                continue;
            }
            candidates.push((edge, DemandPriority::Foreground, vec![demand.waiter]));
            if candidates.len() >= 32 {
                break;
            }
        }
        if candidates.len() >= 32 {
            break;
        }
    }
    // Serve explicit ranges before examining background history. Alias repair
    // is bounded, but must not add its entire slice to a foreground admission.
    let mut bytes = 0;
    let mut verified = 0;
    for foreground in [true, false] {
        if bytes >= 1024 * 1024 || verified >= 32 {
            break;
        }
        if !foreground {
            if selection.mode == PrepareMode::Foreground {
                break;
            }
            for edge in store.content_transfer_candidates(source, selection.lane, selection.mode)? {
                if candidates.len() >= 32 {
                    break;
                }
                if constrained.contains(&edge.reference) {
                    continue;
                }
                let priority = if edge.edge.key.kind == ObjectKind::Manifest {
                    DemandPriority::Structural
                } else {
                    DemandPriority::Background
                };
                candidates.push((edge, priority, vec![]));
            }
        }
        for (item, priority, waiters) in std::mem::take(&mut candidates) {
            context.check()?;
            if !selection.lane.owns(&item.edge.key.digest.to_string()) {
                continue;
            }
            if verified >= 32 {
                break;
            }
            if store.content_object_verified(item.edge.key)? {
                if item.edge.key.kind == ObjectKind::Manifest
                    && !store.content_edge_expanded(&item)?
                {
                    store.expand_content_edge(provider, &item, context)?;
                    *progress = true;
                    scheduler.admitted(&waiters);
                    return Ok(None);
                }
                continue;
            }
            let cost = if item.edge.key.kind == ObjectKind::Manifest {
                MAX_NODE_BYTES
            } else {
                usize::try_from(item.edge.len).unwrap_or(usize::MAX)
            };
            if cost > 1024 * 1024 - bytes {
                break;
            }
            bytes += cost;
            verified += 1;
            match store.verify_content_edge_prioritized(provider, &item, priority, context) {
                Ok(()) => {
                    *progress = true;
                    if item.edge.key.kind == ObjectKind::Manifest
                        && !store.content_edge_expanded(&item)?
                    {
                        store.expand_content_edge(provider, &item, context)?;
                        scheduler.admitted(&waiters);
                        return Ok(None); // one bounded structural expansion per slice
                    }
                    if item.edge.key.kind == ObjectKind::Bytes {
                        scheduler.admitted(&waiters);
                    }
                    continue;
                }
                Err(error)
                    if matches!(
                        crate::model::errors::code_of(&error).as_str(),
                        "CONTENT_UNAVAILABLE" | "CORRUPT_CONTENT"
                    ) => {}
                Err(error) => return Err(error),
            }
            if !store.content_reference_authorized(&item.reference, item.generation)?
                || !store.content_source_ready(item.edge.key, source)?
            {
                continue;
            }
            let key = TransferKey {
                provider: provider.identity(),
                object: item.edge.key,
            };
            if scheduler.active_from(key, source) {
                scheduler.admitted(&waiters);
                continue;
            }
            if let Some(lease) = scheduler.try_acquire(key, &store.repo, source, priority, cost)? {
                scheduler.admitted(&waiters);
                return Ok(Some(ContentTransfer {
                    item,
                    source: source.into(),
                    priority,
                    lease,
                    scheduler: scheduler.clone(),
                }));
            }
        }
    }
    Ok(None)
}
