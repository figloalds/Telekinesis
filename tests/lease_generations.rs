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
