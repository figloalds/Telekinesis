use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tkfs::{
    core::{Store, content_saves::NamespaceTarget, id},
    model::{
        content::{CHUNK_SIZE, ContentKind},
        ipc::StopMode,
    },
    runtime::{Engine, RpcServer, Shared, content::ContentWait, content_reads, content_writes},
};

fn wait() -> ContentWait {
    ContentWait::fixed(Duration::from_secs(30))
}
struct Running {
    shared: Shared,
    server: RpcServer,
    stopped: bool,
}
impl Running {
    fn start(root: &Path, fresh: bool) -> Self {
        let store = if fresh {
            Store::initialize(root, "native-default", &id()).unwrap()
        } else {
            Store::open_existing(root).unwrap()
        };
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        let ctx = tkfs::worker::build_ctx(shared.clone(), root, false, None);
        let server = tkfs::runtime::server::start_rpc_server(ctx).unwrap();
        Self {
            shared,
            server,
            stopped: false,
        }
    }
    fn stop(&mut self, mode: StopMode) -> tkfs::model::ipc::StopReply {
        // Mount cleanup can briefly own Engine after the last kernel call.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let reply = loop {
            match self.server.stop(mode) {
                Ok(reply) => break reply,
                Err(error)
                    if format!("{error:#}").contains("WORKER_BUSY")
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("{error:#}"),
            }
        };
        self.stopped = true;
        reply
    }
    fn assert_current(&self) {
        let e = tkfs::runtime::lock_engine(&self.shared);
        assert!(
            !e.store
                .db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM config WHERE key='fixture:content_v1')",
                    [],
                    |r| r.get::<_, bool>(0)
                )
                .unwrap()
        );
        assert!(
            tkfs::runtime::content_rpc::capabilities(&e.store)
                .unwrap()
                .active
        );
        assert_eq!(
            e.store
                .db
                .query_row("SELECT count(*) FROM pending_saves", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        assert_eq!(e.store.db.query_row("SELECT count(*) FROM events e, json_each(e.payload,'$.changes') c WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')!='object'", [], |r| r.get::<_, u64>(0)).unwrap(), 0);
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.server.stop(StopMode::Force);
        }
    }
}
fn data() -> Vec<u8> {
    (0..2 * CHUNK_SIZE as usize + 11)
        .map(|n| (n % 251) as u8)
        .collect()
}
fn create(worker: &Running, path: &str) -> (u64, tkfs::core::Entry) {
    content_writes::create(
        &worker.shared,
        path,
        content_writes::CreateOptions {
            writable: true,
            ..Default::default()
        },
        wait(),
    )
    .unwrap()
    .expect("native service was not initialized")
}
fn write(worker: &Running, handle: u64, bytes: &[u8]) {
    assert_eq!(
        content_writes::write(&worker.shared, handle, 0, bytes, false, false, wait()).unwrap(),
        Some(bytes.len())
    );
}
fn read(worker: &Running, handle: u64, bytes: &[u8]) {
    assert_eq!(
        content_reads::read_mount_vec(&worker.shared, handle, 0, bytes.len(), wait())
            .unwrap()
            .unwrap(),
        bytes
    );
}
fn close(worker: &Running, handle: u64) {
    tkfs::runtime::engine::close_handle(&worker.shared, handle).unwrap();
}

#[test]
fn rpc_start_initializes_current_writes_without_a_fixture_marker() {
    let dir = tempfile::tempdir().unwrap();
    let mut worker = Running::start(dir.path(), true);
    worker.assert_current();
    let (directory, entry) =
        content_writes::create_directory(&worker.shared, "dir", Default::default(), wait())
            .unwrap()
            .unwrap();
    let (handle, file) = create(&worker, "dir/file");
    let bytes = data();
    write(&worker, handle, &bytes);
    read(&worker, handle, &bytes);
    assert!(content_writes::flush(&worker.shared, handle, wait()).unwrap());
    {
        let e = tkfs::runtime::lock_engine(&worker.shared);
        let reference = e.store.entry(&file.id).unwrap().content_ref.unwrap();
        assert_eq!(reference.kind, ContentKind::Manifest);
        assert_eq!(reference.total_len, bytes.len() as u64);
    }
    content_writes::rename_directory(
        &worker.shared,
        &entry.id,
        Some(directory),
        None,
        &NamespaceTarget {
            path: "moved".into(),
            parent: entry.parent.clone(),
        },
        false,
        wait(),
    )
    .unwrap();
    assert!(
        content_writes::rename(&worker.shared, &file.id, "moved/other", false, wait()).unwrap()
    );
    assert!(content_writes::unlink(&worker.shared, &file.id, "moved/other", wait()).unwrap());
    read(&worker, handle, &bytes);
    let (replacement, next) = create(&worker, "moved/other");
    assert_ne!(next.id, file.id);
    close(&worker, handle);
    close(&worker, replacement);
    content_writes::unlink(&worker.shared, &next.id, "moved/other", wait()).unwrap();
    content_writes::remove_directory(
        &worker.shared,
        &entry.id,
        &NamespaceTarget {
            path: "moved".into(),
            parent: entry.parent,
        },
        wait(),
    )
    .unwrap();
    close(&worker, directory);
    worker.assert_current();
    assert!(worker.stop(StopMode::Graceful).stopped);
    assert!(
        content_writes::create(&worker.shared, "after-stop", Default::default(), wait()).is_err()
    );
}

#[test]
fn forced_worker_stop_recovers_native_saved_bytes_on_current_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut worker = Running::start(dir.path(), true);
    let (handle, entry) = create(&worker, "recover");
    let bytes = data();
    write(&worker, handle, &bytes);
    let stopped = worker.stop(StopMode::Force);
    assert!(stopped.stopped);
    assert_eq!(stopped.persisted_dirty, 1);
    worker.assert_current();
    drop(worker);
    let mut worker = Running::start(dir.path(), false);
    let handle = tkfs::runtime::lock_engine(&worker.shared)
        .open("recover", None, false)
        .unwrap()
        .0;
    read(&worker, handle, &bytes);
    assert!(content_writes::flush(&worker.shared, handle, wait()).unwrap());
    assert_eq!(
        tkfs::runtime::lock_engine(&worker.shared)
            .store
            .entry(&entry.id)
            .unwrap()
            .content_ref
            .unwrap()
            .total_len,
        bytes.len() as u64
    );
    close(&worker, handle);
    worker.assert_current();
    assert!(worker.stop(StopMode::Graceful).stopped);
}

#[test]
fn unsupported_private_journal_refuses_worker_start_without_publishing_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::initialize(dir.path(), "native-default", &id()).unwrap();
    let journal = dir.path().join("write-sessions-v1.sqlite");
    let db = rusqlite::Connection::open(&journal).unwrap();
    db.pragma_update(None, "application_id", 0x544b5753_u32)
        .unwrap();
    db.pragma_update(None, "user_version", 6).unwrap();
    drop(db);
    let before = std::fs::read(&journal).unwrap();
    let shared = Arc::new(Mutex::new(Engine::new(store)));
    let ctx = tkfs::worker::build_ctx(shared, dir.path(), false, None);
    let error = tkfs::runtime::server::start_rpc_server(ctx)
        .err()
        .expect("unsupported journal was accepted");
    assert!(
        format!("{error:#}").contains("UNSUPPORTED_WRITE_JOURNAL"),
        "{error:#}"
    );
    assert_eq!(std::fs::read(journal).unwrap(), before);
    assert!(!dir.path().join("runtime.token").exists());
    assert!(!dir.path().join("runtime.json").exists());
}

#[test]
fn unsupported_private_journal_preserves_pending_directory_recovery() {
    for direct in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(dir.path(), "native-default", &id()).unwrap();
        let context = || tkfs::content::ReadContext {
            deadline: std::time::Instant::now() + Duration::from_secs(30),
            cancel: tkfs::util::Cancel::new(),
        };
        store.content_workspace_id(&context()).unwrap();
        let entry = store.create("pending-directory", "directory").unwrap();
        let snapshot = store.directory_snapshot(&entry.id, &context()).unwrap();
        let request = uuid::Uuid::new_v4();
        let intent = store
            .stage_directory_delete(request, &snapshot, &context())
            .unwrap();
        let before_projection =
            serde_json::to_value(store.projection(&store.active).unwrap()).unwrap();
        let before_events = serde_json::to_value(store.events().unwrap()).unwrap();
        let journal = dir.path().join("write-sessions-v1.sqlite");
        let db = rusqlite::Connection::open(&journal).unwrap();
        db.pragma_update(None, "application_id", 0x544b5753_u32)
            .unwrap();
        db.pragma_update(None, "user_version", 6).unwrap();
        drop(db);
        let before_journal = std::fs::read(&journal).unwrap();
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        let error = if direct {
            content_writes::NativeWrites::install(&shared, &context()).err()
        } else {
            let ctx = tkfs::worker::build_ctx(shared.clone(), dir.path(), false, None);
            tkfs::runtime::server::start_rpc_server(ctx).err()
        }
        .expect("unsupported journal was accepted");
        assert!(
            format!("{error:#}").contains("UNSUPPORTED_WRITE_JOURNAL"),
            "{error:#}"
        );
        assert_eq!(std::fs::read(&journal).unwrap(), before_journal);
        for suffix in ["-wal", "-shm", "-journal"] {
            assert!(!Path::new(&format!("{}{suffix}", journal.display())).exists());
        }
        assert!(!dir.path().join("runtime.token").exists());
        assert!(!dir.path().join("runtime.json").exists());
        {
            let mut e = tkfs::runtime::lock_engine(&shared);
            assert_eq!(
                e.store.directory_snapshot(&entry.id, &context()).unwrap(),
                snapshot
            );
            assert_eq!(
                e.store
                    .directory_delete_intent(request, &context())
                    .unwrap(),
                Some(intent)
            );
            assert_eq!(
                serde_json::to_value(e.store.projection(&e.store.active).unwrap()).unwrap(),
                before_projection
            );
            assert_eq!(
                serde_json::to_value(e.store.events().unwrap()).unwrap(),
                before_events
            );
            assert!(e.store.verify_derived_state().unwrap().is_empty());
        }
        drop(shared);

        // Replace the test's unsupported fixture with a fresh current journal.
        // The retained directory intent must still recover on the next start.
        std::fs::remove_file(&journal).unwrap();
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(dir.path()).unwrap(),
        )));
        let writes = content_writes::NativeWrites::install(&shared, &context()).unwrap();
        {
            let mut e = tkfs::runtime::lock_engine(&shared);
            assert!(!e.store.entry(&entry.id).unwrap().alive);
            assert!(e.store.lookup("pending-directory").is_err());
            assert!(
                e.store
                    .directory_delete_intents(None, 16, &context())
                    .unwrap()
                    .is_empty()
            );
            assert!(e.store.verify_derived_state().unwrap().is_empty());
        }
        writes.shutdown(&context()).unwrap();
    }
}

#[test]
#[ignore = "explicit actual WinFsp/FUSE mount with default worker initialization"]
fn isolated_kernel_fresh_worker_uses_current_writes_without_a_fixture_marker() {
    const CASE: &str = "isolated_kernel_fresh_worker_uses_current_writes_without_a_fixture_marker";
    if let Some(root) = std::env::var_os("TKFS_NATIVE_DEFAULT_KERNEL_ROOT") {
        use std::io::{Seek, SeekFrom, Write};
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let root = std::path::PathBuf::from(root);
        let mut worker = Running::start(&root.join("store"), true);
        let point = root.join("mount");
        let bytes = data();
        tkfs::runtime::rpc(
            worker.server.discovery(),
            &id(),
            serde_json::json!({
                "op":"import","path":"imported","hex":hex::encode(&bytes)
            }),
        )
        .unwrap();
        worker.server.attach(&point).unwrap();
        let _mounted = Mounted;
        assert_eq!(std::fs::read(point.join("imported")).unwrap(), bytes);
        let mut imported = bytes.clone();
        imported[17..25].copy_from_slice(b"COW edit");
        let mut imported_file = std::fs::OpenOptions::new()
            .write(true)
            .open(point.join("imported"))
            .unwrap();
        imported_file.seek(SeekFrom::Start(17)).unwrap();
        imported_file.write_all(b"COW edit").unwrap();
        imported_file.sync_all().unwrap();
        drop(imported_file);
        assert_eq!(std::fs::read(point.join("imported")).unwrap(), imported);
        std::fs::create_dir(point.join("dir")).unwrap();
        let mut file = std::fs::File::create(point.join("dir/file")).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        std::fs::rename(point.join("dir/file"), point.join("dir/renamed")).unwrap();
        assert_eq!(std::fs::read(point.join("dir/renamed")).unwrap(), bytes);
        worker.assert_current();
        {
            let e = tkfs::runtime::lock_engine(&worker.shared);
            assert_eq!(
                e.store
                    .lookup("dir/renamed")
                    .unwrap()
                    .content_ref
                    .unwrap()
                    .kind,
                ContentKind::Manifest
            );
        }
        std::fs::remove_file(point.join("dir/renamed")).unwrap();
        std::fs::remove_dir(point.join("dir")).unwrap();
        std::fs::remove_file(point.join("imported")).unwrap();
        // WinFsp can deliver Close after the OS deletion call returns. Observe
        // those callbacks before requesting a quiet stop; do not ignore BUSY_VIEW.
        let closed_by = std::time::Instant::now() + Duration::from_secs(5);
        let mut remaining = Vec::new();
        loop {
            if let Some(engine) = tkfs::runtime::try_lock_engine(&worker.shared) {
                remaining = engine
                    .handles
                    .iter()
                    .map(|(id, handle)| (*id, handle.entity.clone(), handle.cleaned))
                    .collect();
                if remaining.is_empty() {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < closed_by,
                "kernel Close callbacks did not drain: {remaining:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(worker.stop(StopMode::Graceful).stopped);
        std::fs::write(root.join("result.json"), serde_json::to_vec(&serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"default_worker_start":true,"fixture_marker":false,"chunked_file":true,"fsync":true,"rename":true,"unlink":true,"directory_remove":true,"kernel_handles_drained":true,"graceful_stop":true,"core_typed_import":true,"imported_file_native_rewrite":true})).unwrap()).unwrap();
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            CASE,
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env("TKFS_NATIVE_DEFAULT_KERNEL_ROOT", root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("result.json")).unwrap()).unwrap();
    assert_eq!(result["fixture_marker"], false);
    println!("WAVE6_DEFAULT_NATIVE {result}");
}

#[test]
#[ignore = "explicit actual WinFsp/FUSE direct mount without RPC initialization"]
fn isolated_kernel_direct_mount_installs_current_writes_and_recovers_saved_without_rpc() {
    const CASE: &str =
        "isolated_kernel_direct_mount_installs_current_writes_and_recovers_saved_without_rpc";
    if let Ok(root) = std::env::var("TKFS_DIRECT_NATIVE_KERNEL_ROOT") {
        use std::{
            io::{Seek, SeekFrom, Write},
            time::Instant,
        };
        use tkfs::{
            content::{ReadContext, write::WriteJournal},
            runtime::content_writes::NativeWrites,
            util::Cancel,
        };
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let context = || ReadContext {
            deadline: Instant::now() + Duration::from_secs(30),
            cancel: Cancel::new(),
        };
        let drain = |shared: &Shared| {
            let until = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(e) = tkfs::runtime::try_lock_engine(shared) {
                    if e.handles.is_empty() {
                        break;
                    }
                    assert!(
                        Instant::now() < until,
                        "kernel handles did not drain: {}",
                        e.handles.len()
                    );
                }
                assert!(
                    Instant::now() < until,
                    "Engine unavailable while draining kernel handles"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        };
        let root = Path::new(&root);
        let _mounted = Mounted;
        // Unsupported input must fail before an OS mount/mountpoint is created,
        // including the public attachment path that never starts an RPC worker.
        {
            let store = Store::initialize(&root.join("bad-state"), "direct-bad", &id()).unwrap();
            let journal = store.root.join("write-sessions-v1.sqlite");
            let db = rusqlite::Connection::open(&journal).unwrap();
            db.execute_batch("PRAGMA application_id=1414223699; PRAGMA user_version=999; CREATE TABLE retained_input(x); INSERT INTO retained_input VALUES(7)").unwrap();
            drop(db);
            let before = std::fs::read(&journal).unwrap();
            let shared = Arc::new(Mutex::new(Engine::new(store)));
            tkfs::mount::bind(&shared).unwrap();
            let point = root.join("bad-mount");
            let error = tkfs::mount::attach(&point).unwrap_err();
            assert!(
                format!("{error:#}").contains("UNSUPPORTED_WRITE_JOURNAL"),
                "{error:#}"
            );
            assert!(!point.exists());
            assert!(tkfs::mount::attached().is_none());
            assert_eq!(std::fs::read(&journal).unwrap(), before);
            for suffix in ["-wal", "-shm", "-journal"] {
                assert!(!Path::new(&format!("{}{suffix}", journal.display())).exists());
            }
        }
        let state = root.join("state");
        let point = root.join("mount");
        let mut store = Store::initialize(&state, "direct-native", &id()).unwrap();
        let imported = store.create("imported", "file").unwrap();
        let bytes = data();
        store
            .write_revision(
                &imported.id,
                imported.revisions.get("content").cloned(),
                &bytes,
            )
            .unwrap();
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        tkfs::mount::bind(&shared).unwrap();
        tkfs::mount::attach(&point).unwrap();
        // Installing now can succeed only by reusing attachment's existing
        // service: a fresh installation refuses an already mounted Engine.
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        assert!(Arc::ptr_eq(
            &writes,
            &NativeWrites::install(&shared, &context()).unwrap()
        ));
        assert_eq!(std::fs::read(point.join("imported")).unwrap(), bytes);
        let mut expected = bytes.clone();
        expected[17..25].copy_from_slice(b"COW edit");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(point.join("imported"))
            .unwrap();
        file.seek(SeekFrom::Start(17)).unwrap();
        file.write_all(b"COW edit").unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert_eq!(std::fs::read(point.join("imported")).unwrap(), expected);
        std::fs::create_dir(point.join("dir")).unwrap();
        let mut file = std::fs::File::create(point.join("dir/file")).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        std::fs::rename(point.join("dir/file"), point.join("dir/renamed")).unwrap();
        assert_eq!(std::fs::read(point.join("dir/renamed")).unwrap(), bytes);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(point.join("dir/renamed"))
            .unwrap();
        file.write_all(b"short replacement").unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert_eq!(
            std::fs::read(point.join("dir/renamed")).unwrap(),
            b"short replacement"
        );
        drain(&shared);
        tkfs::mount::detach(false).unwrap();
        writes.shutdown(&context()).unwrap();
        drop(writes);

        // Seed an exact durable private revision using a separate stopped
        // owner. The next direct attachment, rather than RPC startup, must
        // recover it. Buffered data must not already be canonical.
        {
            let seed = Arc::new(Mutex::new(Engine::new(
                Store::open_existing(&state).unwrap(),
            )));
            let service = NativeWrites::install(&seed, &context()).unwrap();
            let (handle, entry) = content_writes::create(
                &seed,
                "saved",
                content_writes::CreateOptions {
                    writable: true,
                    ..Default::default()
                },
                wait(),
            )
            .unwrap()
            .unwrap();
            content_writes::write(
                &seed,
                handle,
                0,
                b"durable private bytes",
                false,
                false,
                wait(),
            )
            .unwrap();
            assert_eq!(content_writes::retain_dirty(&seed, &context()).unwrap(), 1);
            assert_eq!(
                tkfs::runtime::lock_engine(&seed)
                    .store
                    .entry(&entry.id)
                    .unwrap()
                    .content_ref
                    .unwrap()
                    .total_len,
                0
            );
            assert_eq!(WriteJournal::open(&state).unwrap().pending_count(), 1);
            service.shutdown(&context()).unwrap();
        }
        let recovered = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&state).unwrap(),
        )));
        tkfs::mount::bind(&recovered).unwrap();
        tkfs::mount::attach(&point).unwrap();
        assert_eq!(
            std::fs::read(point.join("saved")).unwrap(),
            b"durable private bytes"
        );
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(point.join("saved"))
            .unwrap();
        file.seek(SeekFrom::End(0)).unwrap();
        file.write_all(b" after reopen").unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert_eq!(
            std::fs::read(point.join("saved")).unwrap(),
            b"durable private bytes after reopen"
        );
        drain(&recovered);
        {
            let e = tkfs::runtime::lock_engine(&recovered);
            assert_eq!(
                e.store
                    .db
                    .query_row("SELECT count(*) FROM pending_saves", [], |r| r
                        .get::<_, usize>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                e.store
                    .db
                    .query_row(
                        "SELECT count(*) FROM config WHERE key='fixture:content_v1'",
                        [],
                        |r| r.get::<_, usize>(0)
                    )
                    .unwrap(),
                0
            );
            assert_eq!(
                e.store
                    .lookup("saved")
                    .unwrap()
                    .content_ref
                    .unwrap()
                    .total_len,
                34
            );
        }
        tkfs::mount::detach(false).unwrap();
        NativeWrites::install(&recovered, &context())
            .unwrap()
            .shutdown(&context())
            .unwrap();
        std::fs::write(root.join("direct-result.json"),serde_json::to_vec(&serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"rpc_startup":false,"default_current_service":true,"idempotent_install":true,"typed_import_rewrite":true,"create_rename_truncate_fsync":true,"current_saved_recovery":true,"unsupported_journal_nonmutating":true,"fixture_marker":false})).unwrap()).unwrap();
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            CASE,
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env("TKFS_DIRECT_NATIVE_KERNEL_ROOT", root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("direct-result.json")).unwrap())
            .unwrap();
    assert_eq!(result["rpc_startup"], false);
    assert_eq!(result["fixture_marker"], false);
    println!("WAVE6_DIRECT_NATIVE {result}");
}
