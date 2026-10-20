//! Ordered legacy transfer ownership, including a real catalog startup crash.
use std::{path::Path, sync::Arc};
use tkfs::{
    model::{
        lease::{self, Generation},
        orchestrator::CatalogApi,
    },
    orchestrator::{Config, Network, catalog::Catalog, lease_clock::Clock},
};
fn config(root: &Path) -> Config {
    Config {
        format_version: 2,
        data_directory: root.join("data"),
        default_mount_directory: root.join("mounts"),
        maximum_running: 1,
        network: Network::default(),
    }
}

#[test]
fn a_pool_layout_is_assigned_before_lanes_and_late_old_lanes_cannot_get_a_new_layout() {
    use tkfs::{
        model::orchestrator::{PeerContext, RemoteCatalog},
        util::{Cancel, snapshot::Snapshot},
    };
    let peer = |pool| {
        Arc::new(PeerContext {
            installation: "peer-a".into(),
            endpoint: "ws://fixture".into(),
            local_installation: "local".into(),
            pool,
            catalog: Snapshot::new(RemoteCatalog::default()),
        })
    };
    let clock = Clock::new(29);
    let old = peer(4);
    let old_layout = clock.register_peer(&old).unwrap();
    let old_run = clock.pull(&old, &Cancel::new()).unwrap();
    let new = peer(3);
    let new_layout = clock.register_peer(&new).unwrap();
    let new_run = clock.pull(&new, &Cancel::new()).unwrap();
    let late_old = clock.pull(&old, &Cancel::new()).unwrap();
    assert!(late_old.generation > new_run.generation);
    assert_eq!(late_old.layout, old_layout.run);
    assert_eq!(old_run.layout, old_layout.run);
    assert_eq!(new_run.layout, new_layout.run);
    assert!(new_layout > old_layout);
    assert_eq!(clock.register_peer(&new).unwrap(), new_layout);
    assert_eq!(
        clock.pull(&new, &Cancel::new()).unwrap().layout,
        new_layout.run
    );
    let unknown = peer(3);
    assert_eq!(
        clock.pull(&unknown, &Cancel::new()).unwrap_err().code,
        "CONTENT_RESOURCE_LIMIT"
    );
    let cancelled = Cancel::new();
    cancelled.cancel();
    assert_eq!(
        clock.pull(&old, &cancelled).unwrap_err().code,
        "CONTENT_CANCELLED"
    );
}

#[test]
fn pool_qualified_sessions_are_bounded_canonical_and_classic_transfers_remain_recognizable() {
    let run = lease::PullRun {
        generation: Generation {
            incarnation: 7,
            run: 100,
        },
        layout: 3,
    };
    for base in [
        "pull:peer-a:0/2:a",
        "pull:peer-a:0/2:b",
        "pull:peer-a:0/2:c0000000000000001",
    ] {
        let text = lease::qualify_pull(base, run).unwrap();
        let parsed = lease::parse(&text).unwrap().unwrap();
        assert_eq!(parsed.layout, Some(3));
        assert_eq!(parsed.generation, run.generation);
        assert_eq!(parsed.family, "pull:peer-a");
        assert!(text.len() <= 128);
    }
    let classic = lease::parse_transfer("pull:peer-a:0/2:a").unwrap().unwrap();
    assert_eq!(
        classic.generation,
        Generation {
            incarnation: 0,
            run: 0
        }
    );
    assert_eq!(classic.layout, None);
    assert!(
        lease::parse_transfer("arbitrary opaque session")
            .unwrap()
            .is_none()
    );
    for suffix in [
        "0000000000000000",
        "0000000000000065",
        "000000000000000A",
        "0000000000000003.extra",
        "é00000000000000",
    ] {
        assert!(
            lease::parse(&format!(
                "pull:p:0/1:a~w6.0000000000000007.0000000000000064.{suffix}"
            ))
            .is_err()
        );
    }
    let long = lease::qualify_pull(
        &format!("pull:{}:14/15:c0000000000000001", uuid::Uuid::new_v4()),
        run,
    )
    .unwrap();
    assert!(long.len() <= 128);
}
#[test]
fn ordered_session_extension_is_bounded_canonical_and_preserves_the_classic_base() {
    let g = Generation {
        incarnation: 17,
        run: 4,
    };
    for base in [
        "pull:peer-a:0/2:a",
        "pull:peer-a:0/2:b",
        "pull:peer-a:0/2:c0000000000000001",
        "push:peer-a:0/2",
    ] {
        let text = lease::qualify(base, g).unwrap();
        let owned = lease::parse(&text).unwrap().unwrap();
        assert_eq!(owned.generation, g);
        assert_eq!(owned.peer, "peer-a");
        assert!(text.starts_with(base));
        assert!(text.len() <= 128);
    }
    assert_eq!(
        lease::parse(&lease::qualify("pull:peer-a:0/2:a", g).unwrap())
            .unwrap()
            .unwrap()
            .scope,
        lease::parse(&lease::qualify("pull:peer-a:0/2:b", g).unwrap())
            .unwrap()
            .unwrap()
            .scope
    );
    assert!(lease::parse("classic-arbitrary-session").unwrap().is_none());
    assert!(lease::parse("pull:peer-a:0/2:a").unwrap().is_none());
    for bad in [
        "pull:p:0/0:a~w6.0000000000000001.0000000000000001",
        "pull:p:00/2:a~w6.0000000000000001.0000000000000001",
        "pull:p:0/2:c~w6.0000000000000001.0000000000000001",
        "pull:p:0/2:c0000000000000000~w6.0000000000000001.0000000000000001",
        "pull:p:0/2:c000000000000000A~w6.0000000000000001.0000000000000001",
        "pull:p:0/2:a~w6.0000000000000001.0000000000000000",
        "pull:p:0/2:a~w6.000000000000000A.0000000000000001",
        "pull:p:0/2:a~w6.é00000000000000.0000000000000001",
    ] {
        assert!(lease::parse(bad).is_err(), "{bad}");
    }
    assert!(lease::qualify(&format!("pull:{}:0/1:a", "p".repeat(100)), g).is_err());
}
#[test]
fn a_run_clock_is_monotonic_under_concurrent_lane_creation() {
    let c = Arc::new(Clock::new(5));
    let mut all = std::thread::scope(|s| {
        let mut jobs = vec![];
        for _ in 0..8 {
            let c = c.clone();
            jobs.push(s.spawn(move || (0..100).map(|_| c.next().unwrap()).collect::<Vec<_>>()));
        }
        jobs.into_iter()
            .flat_map(|j| j.join().unwrap())
            .collect::<Vec<_>>()
    });
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 800);
    assert_eq!(
        all[0],
        Generation {
            incarnation: 5,
            run: 1
        }
    );
    assert_eq!(
        all[799],
        Generation {
            incarnation: 5,
            run: 800
        }
    );
}
#[test]
fn a_catalog_crash_after_epoch_commit_does_not_reuse_the_incarnation() {
    const CHILD: &str = "TKFS_TEST_LEASE_CATALOG";
    if let Some(path) = std::env::var_os(CHILD) {
        tkfs::util::fault::configure(Some(tkfs::util::fault::FaultConfig::new(
            "catalog.lease_incarnation_committed",
            tkfs::util::fault::FaultAction::Exit,
        )));
        let _ = Catalog::open(&config(Path::new(&path))).unwrap();
        panic!("catalog must exit at the actual post-commit boundary");
    }
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let first = Catalog::open(&cfg).unwrap();
    assert_eq!(first.lease_incarnation(), 1);
    let installation = first.snapshot().installation.clone();
    assert!(first.close());
    drop(first);
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "a_catalog_crash_after_epoch_commit_does_not_reuse_the_incarnation",
            "--test-threads=1",
        ])
        .env(CHILD, dir.path());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let status = command.status().unwrap();
    assert_eq!(status.code(), Some(tkfs::util::fault::EXIT_CODE));
    let next = Catalog::open(&cfg).unwrap();
    assert_eq!(next.lease_incarnation(), 3);
    assert_eq!(next.snapshot().installation, installation);
    assert!(next.close());
}
#[test]
fn corrupt_or_exhausted_incarnations_fail_closed_instead_of_reusing_an_old_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let c = Catalog::open(&cfg).unwrap();
    assert!(c.close());
    drop(c);
    for text in ["corrupt".to_owned(), u64::MAX.to_string()] {
        let db = rusqlite::Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
        db.execute(
            "UPDATE settings SET value=?1 WHERE key='objects_incarnation'",
            [&text],
        )
        .unwrap();
        drop(db);
        let error = Catalog::open(&cfg).expect_err("invalid epoch must prevent startup");
        assert_eq!(tkfs::model::errors::code_of(&error), "CATALOG_UNAVAILABLE");
        let db = rusqlite::Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
        assert_eq!(
            db.query_row(
                "SELECT value FROM settings WHERE key='objects_incarnation'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            text
        );
    }
}
