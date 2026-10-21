use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        ReadContext,
        scheduler::{ContentScheduler, TransferKey},
    },
    model::{
        content::{ContentKind, Digest, FileContentRef, ObjectKey, ObjectKind},
        workspace::{ByteRange, Demand, DemandPriority, FileRevision, WorkspaceId},
    },
    util::Cancel,
};
use uuid::Uuid;

fn key(n: u64) -> TransferKey {
    TransferKey {
        provider: Uuid::nil(),
        object: ObjectKey {
            kind: ObjectKind::Bytes,
            digest: Digest::bytes(&n.to_le_bytes()),
        },
    }
}

#[test]
fn saturated_background_reserves_immediate_foreground_and_structural_capacity() {
    let scheduler = Arc::new(ContentScheduler::default());
    let mut leases = vec![];
    for i in 0..6 {
        leases.push(
            scheduler
                .try_acquire(
                    key(i),
                    &format!("repo{}", i / 2),
                    &format!("peer{}", i / 2),
                    DemandPriority::Background,
                    65536,
                )
                .unwrap()
                .unwrap(),
        );
    }
    assert!(
        scheduler
            .try_acquire(key(8), "repo3", "peer3", DemandPriority::Background, 65536)
            .unwrap()
            .is_none()
    );
    let started = Instant::now();
    let foreground = scheduler
        .try_acquire(key(9), "repo0", "peer0", DemandPriority::Foreground, 65536)
        .unwrap()
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(250));
    let structural = scheduler
        .try_acquire(key(10), "repo0", "peer0", DemandPriority::Structural, 65536)
        .unwrap()
        .unwrap();
    assert_eq!(scheduler.usage().0, 8);
    drop(foreground);
    drop(structural);
    drop(leases);
    assert_eq!(scheduler.usage().0, 0);
}

#[test]
fn unjoined_streams_share_global_bytes_and_leave_both_other_classes_capacity() {
    let scheduler = Arc::new(ContentScheduler::default());
    let streams: Vec<_> = (0..6)
        .map(|i| {
            scheduler
                .try_reserve_stream(
                    &format!("repo{}", i / 2),
                    &format!("peer{}", i / 2),
                    DemandPriority::Background,
                    1024 * 1024,
                )
                .unwrap()
                .unwrap()
        })
        .collect();
    assert!(
        scheduler
            .try_acquire(
                key(100),
                "extra",
                "extra",
                DemandPriority::Background,
                65536
            )
            .unwrap()
            .is_none()
    );
    assert!(
        scheduler
            .try_reserve_stream("extra", "extra", DemandPriority::Background, 1)
            .unwrap()
            .is_none()
    );
    let foreground = scheduler
        .try_acquire(
            key(101),
            "repo0",
            "peer0",
            DemandPriority::Foreground,
            1024 * 1024,
        )
        .unwrap()
        .unwrap();
    let structural = scheduler
        .try_reserve_stream("repo0", "peer0", DemandPriority::Structural, 1024 * 1024)
        .unwrap()
        .unwrap();
    assert_eq!(scheduler.usage().0, 8);
    assert_eq!(scheduler.usage().1, 8 * 1024 * 1024);
    assert_eq!(scheduler.stream_usage(), (7, 7 * 1024 * 1024));
    assert_eq!(scheduler.joined_usage(), (1, 1024 * 1024));
    drop((foreground, structural, streams));
    assert_eq!(scheduler.usage().0, 0);
}

#[test]
fn mixed_stream_and_joined_jobs_share_peer_and_repository_class_limits() {
    let scheduler = Arc::new(ContentScheduler::default());
    let joined = scheduler
        .try_acquire(key(1), "repo", "peer", DemandPriority::Background, 65536)
        .unwrap()
        .unwrap();
    let stream = scheduler
        .try_reserve_stream("repo", "peer", DemandPriority::Background, 1024 * 1024)
        .unwrap()
        .unwrap();
    assert!(
        scheduler
            .try_reserve_stream("repo", "other", DemandPriority::Background, 1)
            .unwrap()
            .is_none()
    );
    assert!(
        scheduler
            .try_acquire(key(2), "other", "peer", DemandPriority::Background, 1)
            .unwrap()
            .is_none()
    );
    let foreground = scheduler
        .try_reserve_stream("repo", "peer", DemandPriority::Foreground, 1)
        .unwrap()
        .unwrap();
    let structural = scheduler
        .try_acquire(key(3), "repo", "peer", DemandPriority::Structural, 1)
        .unwrap()
        .unwrap();
    assert_eq!(scheduler.usage().0, 4);
    drop((joined, stream, foreground, structural));
    assert_eq!(scheduler.usage().0, 0);
}

#[test]
fn stream_reservations_remain_live_until_all_buffer_and_writer_proofs_drop() {
    let scheduler = Arc::new(ContentScheduler::default());
    let stream = Arc::new(
        scheduler
            .try_reserve_stream("repo", "peer", DemandPriority::Background, 1024 * 1024)
            .unwrap()
            .unwrap(),
    );
    let buffer = stream.clone();
    let writer = stream.clone();
    drop(stream);
    assert_eq!(scheduler.stream_usage(), (1, 1024 * 1024));
    drop(buffer);
    assert_eq!(scheduler.stream_usage().0, 1);
    let other = scheduler
        .try_reserve_stream("repo", "peer", DemandPriority::Background, 1024 * 1024)
        .unwrap()
        .unwrap();
    drop(writer);
    assert_eq!(scheduler.stream_usage().0, 1);
    drop(other);
    assert_eq!(scheduler.usage().0, 0);
    assert!(
        scheduler
            .try_reserve_stream("repo", "peer", DemandPriority::Background, 1024 * 1024 + 1)
            .is_err()
    );
}

#[test]
fn continuous_foreground_cannot_take_the_background_progress_reservation() {
    let scheduler = Arc::new(ContentScheduler::default());
    let mut leases = vec![];
    for i in 0..6 {
        leases.push(
            scheduler
                .try_acquire(
                    key(i),
                    &format!("repo{}", i / 2),
                    &format!("peer{}", i / 2),
                    DemandPriority::Foreground,
                    65536,
                )
                .unwrap()
                .unwrap(),
        );
    }
    assert!(
        scheduler
            .try_acquire(key(10), "repo0", "peer0", DemandPriority::Background, 65536)
            .unwrap()
            .is_some()
    );
}

#[test]
fn joined_installation_and_late_release_preserve_the_new_owners_lease() {
    let scheduler = Arc::new(ContentScheduler::default());
    let first = scheduler
        .try_acquire(key(1), "repo", "peer", DemandPriority::Foreground, 65536)
        .unwrap()
        .unwrap();
    assert!(
        scheduler
            .try_acquire(key(1), "repo", "other", DemandPriority::Foreground, 65536)
            .unwrap()
            .is_none()
    );
    let old = first.token();
    assert!(scheduler.release(key(1), old));
    let second = scheduler
        .try_acquire(key(1), "repo", "peer", DemandPriority::Foreground, 65536)
        .unwrap()
        .unwrap();
    drop(first);
    assert!(!scheduler.release(key(1), old));
    assert!(scheduler.owns(key(1), second.token()));
    assert_eq!(scheduler.usage().0, 1);
}

fn demand(waiter: Uuid) -> Demand {
    Demand {
        waiter,
        revision: FileRevision {
            workspace: WorkspaceId {
                repository: "repo".into(),
                workspace: Uuid::nil(),
            },
            entity: "file".into(),
            generation: 1,
            content: FileContentRef::new(ContentKind::Direct, Digest::bytes(b"abc"), 3).unwrap(),
        },
        reference: "reference".into(),
        range: ByteRange { offset: 0, len: 3 },
        priority: DemandPriority::Foreground,
        eligible_peers: vec!["peer".into()],
    }
}

#[test]
fn cancellation_and_stale_waiter_drop_do_not_cancel_other_readers() {
    let scheduler = Arc::new(ContentScheduler::default());
    let one = ReadContext {
        deadline: Instant::now() + Duration::from_secs(5),
        cancel: Cancel::new(),
    };
    let two = ReadContext {
        deadline: one.deadline,
        cancel: Cancel::new(),
    };
    let id = Uuid::new_v4();
    let old = scheduler.demand(demand(id), &one).unwrap();
    let other = scheduler.demand(demand(Uuid::new_v4()), &two).unwrap();
    one.cancel.cancel();
    assert_eq!(scheduler.demands("repo", "reference").len(), 1);
    let replacement = scheduler.demand(demand(id), &two).unwrap();
    drop(old);
    assert_eq!(scheduler.demands("repo", "reference").len(), 2);
    drop(other);
    drop(replacement);
    assert_eq!(scheduler.usage().2, 0);
}

#[test]
fn one_full_providers_waiter_queue_does_not_consume_anothers_reservation() {
    let scheduler = Arc::new(ContentScheduler::default());
    let context = ReadContext {
        deadline: Instant::now() + Duration::from_secs(5),
        cancel: Cancel::new(),
    };
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let mut guards = vec![];
    for _ in 0..64 {
        guards.push(
            scheduler
                .demand_on(first, demand(Uuid::new_v4()), &context)
                .unwrap(),
        );
    }
    assert!(
        scheduler
            .demand_on(first, demand(Uuid::new_v4()), &context)
            .is_err()
    );
    let independent = scheduler
        .demand_on(second, demand(Uuid::new_v4()), &context)
        .unwrap();
    assert_eq!(scheduler.usage().2, 65);
    drop(independent);
    drop(guards);
    assert_eq!(scheduler.usage().2, 0);
}
