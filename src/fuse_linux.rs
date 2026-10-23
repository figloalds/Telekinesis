//! Linux FUSE protocol 7.31 adapter. No libfuse build dependency: fusermount3
//! applies the OS mount policy and passes the device using SCM_RIGHTS.
//! No writeback caching/mmap: direct I/O and zero lookup/attribute TTLs retain
//! Engine's save boundaries and pinned writer ancestry. Unsupported POSIX kinds
//! (symlinks, hardlinks, devices), ownership and privileged modes fail explicitly.
//!
//! Re-attachable (REVAMP_PLAN §4.3, WP1.5), with the same names as the WinFsp
//! adapter: [`bind`] sets the engine the callbacks serve, every attach starts a
//! fresh connection and dispatch thread, and [`detach`] runs fence → drain →
//! [force: retain dirty] → unmount (lazily when files are still open, then the
//! dispatch thread is stopped so the connection aborts) → release → unfence.
//! Managed mounts are named `tkfs-<instance>` (subtype `tkfs`) so a stale one
//! can later be proven ours by the previous `worker.json` ([`recover_managed`],
//! invariant 27). Mount failures never write `Engine.health`: a dispatch
//! thread death (an error, or the connection ending because the mount was
//! unmounted outside the worker) or a failed checkout remount reports `Lost`
//! to the controller, and callback failures go to the mount-scoped error
//! counter and log.
//!
//! WP5.2: reads, writes, fsyncs, flushes and size changes run on threads of
//! their own ([`Threads`]: at most [`MAX_THREADED`] at once, the others wait
//! in line, none is refused) and do the work that needs no engine lock first
//! (content waits, the writable copy of a large file, a save's hashing and
//! copying). An fsync (`FUSE_FSYNC`, fsync(2)) is a durability point: it
//! commits what was written before it answers, also during a burst of
//! closes. A flush (`FUSE_FLUSH`, sent by every close(2) of a descriptor)
//! saves the file before it answers, or, during a burst of closes, queues the
//! save for a background saver (`runtime::engine::write_behind`; an fsync
//! waits for it). A release closes through `runtime::engine::close_handle`;
//! an unlink, an rmdir and a rename settle the entry first
//! (`runtime::engine::settle_entity`). A read of content that cannot arrive
//! answers `EIO` (it answered `EAGAIN`, CCR-WP4.INT-2).
//!
//! `MOUNT` is the transition lock: [`bind`], [`attach`], [`detach`] and
//! [`checkout`] hold it across their mount I/O (so they wait for each other);
//! a callback, [`attached`], [`serves`] and [`inflight`] never take it, so
//! readers never wait for a transition.
use crate::{
    core::*,
    model::ipc::timing::DETACH_DRAIN,
    runtime::{Shared, lock_engine},
    util::lock::lock,
    worker::mount_ctl::adapter,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
};

/// The engine callbacks serve. Replaced only while nothing is attached.
static ENGINE: Mutex<Option<Shared>> = Mutex::new(None);
/// The attached instance. Lock order: `MOUNT`, then `ENGINE`, then the engine
/// lock; never taken by a callback.
static MOUNT: Mutex<Option<Mount>> = Mutex::new(None);
const MAX_IO: usize = 1024 * 1024;
struct Mount {
    path: PathBuf,
    /// `path` as `/proc/self/mountinfo` names it (canonicalized at attach).
    canonical: PathBuf,
    source: String,
    /// The `/dev/fuse` dispatch thread.
    worker: Option<JoinHandle<()>>,
    /// Writing to (or dropping) this end stops the dispatch thread.
    wake: Option<UnixStream>,
    /// Set before a detach or checkout ends the connection: the dispatch
    /// thread's exit is then deliberate, not `Lost`.
    stopping: Arc<AtomicBool>,
}
impl Mount {
    /// Whether the dispatch thread still serves the connection.
    fn serving(&self) -> bool {
        self.worker.as_ref().is_some_and(|w| !w.is_finished())
    }
    /// Whether our mount is still at its path (`false` once it was unmounted
    /// outside the worker). An unreadable mount table counts as present.
    fn present(&self) -> bool {
        mounts_at(&self.canonical).map_or(true, |mounts| {
            mounts
                .iter()
                .any(|(fstype, source)| fstype == "fuse.tkfs" && *source == self.source)
        })
    }
}
/// A launched connection: the dispatch thread, its wake end, its stop flag.
type Launched = (JoinHandle<()>, UnixStream, Arc<AtomicBool>);

fn helper_mount(path: &Path, source: &str) -> Result<File> {
    let (parent, child) = UnixStream::pair()?;
    parent.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
    // Only the communication endpoint survives exec. Both sockets were CLOEXEC.
    ensure!(
        unsafe { libc::fcntl(child.as_raw_fd(), libc::F_SETFD, 0) } == 0,
        "FUSE_COMMFD"
    );
    let output = Command::new("fusermount3")
        .args([
            "-o",
            &format!("rw,nosuid,nodev,default_permissions,fsname={source},subtype=tkfs"),
            "--",
        ])
        .arg(path)
        .env("_FUSE_COMMFD", child.as_raw_fd().to_string())
        .output()?;
    drop(child);
    ensure!(
        output.status.success(),
        "FUSE_MOUNT_FAILED: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result = (|| {
        let mut byte = [0u8];
        let mut vector = libc::iovec {
            iov_base: byte.as_mut_ptr().cast(),
            iov_len: 1,
        };
        let mut ancillary = [0u64; 8];
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut vector;
        message.msg_iovlen = 1;
        message.msg_control = ancillary.as_mut_ptr().cast();
        message.msg_controllen = std::mem::size_of_val(&ancillary);
        ensure!(
            unsafe { libc::recvmsg(parent.as_raw_fd(), &mut message, libc::MSG_CMSG_CLOEXEC) } == 1,
            "FUSE_DESCRIPTOR_RECEIVE"
        );
        let header = unsafe { libc::CMSG_FIRSTHDR(&message) };
        ensure!(
            !header.is_null()
                && unsafe {
                    (*header).cmsg_level == libc::SOL_SOCKET
                        && (*header).cmsg_type == libc::SCM_RIGHTS
                        && (*header).cmsg_len >= libc::CMSG_LEN(4) as usize
                },
            "FUSE_DESCRIPTOR_INVALID"
        );
        let fd = unsafe { std::ptr::read_unaligned(libc::CMSG_DATA(header).cast::<i32>()) };
        ensure!(fd >= 0, "FUSE_DESCRIPTOR_INVALID");
        Ok(unsafe { File::from_raw_fd(fd) })
    })();
    if result.is_err() {
        let _ = unmount(path, false);
    }
    result
}
/// `fusermount3 -u` (`-z` for a lazy unmount). A refusal is `BUSY_VIEW`.
fn unmount(path: &Path, lazy: bool) -> Result<()> {
    let mut command = Command::new("fusermount3");
    command.arg("-u");
    if lazy {
        command.arg("-z");
    }
    let output = command.arg("--").arg(path).output()?;
    ensure!(
        output.status.success(),
        "BUSY_VIEW: FUSE unmount refused: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}
/// Mounts `path` and starts its dispatch thread. The engine is marked attached
/// (`adapter::attached`) before the thread starts, so a dispatch death always
/// has the last word on `Engine.mounted`.
fn launch(engine: Shared, path: &Path, source: &str) -> Result<Launched> {
    let device = helper_mount(path, source)?;
    let (wake, woken) = match UnixStream::pair() {
        Ok(pair) => pair,
        Err(error) => {
            drop(device);
            let _ = unmount(path, false);
            return Err(error.into());
        }
    };
    let stopping = Arc::new(AtomicBool::new(false));
    adapter::attached(&engine);
    let spawned = {
        let engine = engine.clone();
        let stopping = stopping.clone();
        let path = path.to_owned();
        std::thread::Builder::new()
            .name("fuse-dispatch".into())
            .spawn(move || {
                let outcome = serve(device, engine.clone(), woken);
                if stopping.load(Ordering::SeqCst) {
                    return; // A detach or checkout ended the connection.
                }
                let error = match outcome {
                    Ok(Exit::Woken) => return,
                    Ok(Exit::DeviceGone) => anyhow::anyhow!(
                        "MOUNT_NOT_RUNNING: the FUSE mount at {} was unmounted outside the worker",
                        path.display()
                    ),
                    Err(error) => anyhow::anyhow!("FUSE_DISPATCH_FAILED: {error:#}"),
                };
                // Mount-scoped: `Lost`, never `Engine.health`. Dirty sessions
                // go to `pending_saves` now (invariant 8): the dead
                // connection's handles keep them referenced until a detach.
                adapter::lost(&engine);
                adapter::report_lost(&error);
            })
    };
    match spawned {
        Ok(worker) => Ok((worker, wake, stopping)),
        Err(error) => {
            // The device closed with the closure: the mount is stale.
            let _ = unmount(path, false);
            {
                let mut e = lock_engine(&engine);
                e.mounted = false;
                e.publish_observation();
            }
            Err(error.into())
        }
    }
}
fn bound() -> Result<Shared> {
    lock(&ENGINE)
        .clone()
        .context("ENGINE_NOT_READY: no engine bound to the mount")
}
/// Sets the engine the callbacks serve. Idempotent for the same `Arc`; a
/// different engine replaces it only while nothing is attached
/// (`ONE_MOUNT_PER_RUNTIME` otherwise). Performs no path check.
pub fn bind(engine: &Shared) -> Result<()> {
    let mount = lock(&MOUNT);
    let mut slot = lock(&ENGINE);
    if let Some(current) = slot.as_ref() {
        if Arc::ptr_eq(current, engine) {
            return Ok(());
        }
        ensure!(
            mount.is_none(),
            "ONE_MOUNT_PER_RUNTIME: another engine owns the attached mount"
        );
    }
    *slot = Some(engine.clone());
    drop(slot);
    crate::runtime::Engine::adopt_shared(engine);
    Ok(())
}
/// `MOUNT_PATH_MUST_NOT_EXIST` unless nothing is at `path`, including a live
/// or stale mount (which `stat` cannot see).
pub fn vacant(path: &Path) -> Result<()> {
    let mounts = mounts_at(path)?;
    ensure!(
        mounts.is_empty(),
        "MOUNT_PATH_MUST_NOT_EXIST: {} is a mount point ({})",
        path.display(),
        mounts
            .iter()
            .map(|(fstype, source)| format!("{fstype} {source}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    adapter::vacant(path)
}
/// Whether `engine` is the engine the callbacks serve.
pub fn serves(engine: &Shared) -> bool {
    lock(&ENGINE)
        .as_ref()
        .is_some_and(|bound| Arc::ptr_eq(bound, engine))
}
/// The path of the attached instance, if any (also while it is lost). Never
/// waits for a transition in progress.
pub fn attached() -> Option<PathBuf> {
    adapter::attached_path()
}
/// Whether the attached instance is no longer in the mount table: unmounted
/// outside the worker, possibly lazily with files still open (the connection
/// then lives on and the dispatch thread has not noticed). `false` while a
/// transition runs. Never waits.
pub fn vanished() -> bool {
    MOUNT
        .try_lock()
        .ok()
        .and_then(|guard| guard.as_ref().map(|mount| !mount.present()))
        .unwrap_or(false)
}
/// Callbacks currently running.
pub fn inflight() -> usize {
    adapter::inflight()
}
/// Mount-scoped callback failures since the process started.
pub fn mount_errors() -> u64 {
    adapter::mount_errors()
}
/// Attaches an unmanaged mount (source `tkfs`) at `path` for the bound engine.
pub fn attach(path: &Path) -> Result<()> {
    attach_source(path, "tkfs")
}
/// Attaches a mount whose source `tkfs-<instance>` names this worker instance.
pub fn attach_managed(path: &Path, instance: &str) -> Result<()> {
    uuid::Uuid::parse_str(instance).context("INVALID_PARAMETER: worker instance")?;
    attach_source(path, &format!("tkfs-{instance}"))
}
/// Legacy entry (`daemon --mount`): bind, then attach (source `tkfs`).
pub fn start(engine: Shared, path: &Path) -> Result<()> {
    bind(&engine)?;
    attach(path)
}
pub fn start_managed(engine: Shared, path: &Path, instance: &str) -> Result<()> {
    bind(&engine)?;
    attach_managed(path, instance)
}
fn attach_source(path: &Path, source: &str) -> Result<()> {
    let mut guard = lock(&MOUNT);
    if let Some(mount) = guard.as_ref() {
        bail!(
            "ONE_MOUNT_PER_RUNTIME: already attached at {}",
            mount.path.display()
        );
    }
    let engine = bound()?;
    vacant(path)?;
    crate::runtime::content_writes::NativeWrites::install(
        &engine,
        &crate::runtime::content_writes::default_context(&engine)?,
    )?;
    std::fs::create_dir(path)?;
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_owned());
    let (worker, wake, stopping) = match launch(engine, path, source) {
        Ok(launched) => launched,
        Err(error) => {
            let _ = std::fs::remove_dir(path);
            return Err(error);
        }
    };
    *guard = Some(Mount {
        path: path.into(),
        canonical,
        source: source.into(),
        worker: Some(worker),
        wake: Some(wake),
        stopping,
    });
    adapter::set_attached_path(Some(path.to_owned()));
    Ok(())
}
/// Stops the dispatch thread (after the unmount) and waits for it.
fn stop_dispatch(mount: &mut Mount) {
    mount.stopping.store(true, Ordering::SeqCst);
    if let Some(mut wake) = mount.wake.take() {
        let _ = wake.write_all(&[1]);
    }
    if let Some(worker) = mount.worker.take() {
        let _ = worker.join();
    }
}
/// Stops the attached instance (no-op when nothing is attached): fence →
/// drain (≤ `DETACH_DRAIN`) → [force: `retain_all_dirty`] → unmount → stop the
/// dispatch thread → remove our mount point → release handles → unfence.
/// A graceful detach is refused with `BUSY_VIEW` (and stays attached,
/// unfenced) while handles are open, a session is dirty, callbacks do not
/// drain, or the kernel refuses the unmount. A force detach unmounts lazily
/// when the kernel still has files open; stopping the dispatch thread then
/// aborts the connection, so those files fail instead of hanging. A lost
/// mount (its dispatch thread ended, or it was unmounted outside the worker)
/// is always detached like a force detach, and one that is no longer in the
/// mount table is not unmounted again.
pub fn detach(force: bool) -> Result<()> {
    let mut guard = lock(&MOUNT);
    let Some(mount) = guard.as_mut() else {
        return Ok(());
    };
    let engine = bound()?;
    let present = mount.present();
    let force = force || !present || !mount.serving();
    adapter::fence(&engine, force)?;
    if !adapter::drain(DETACH_DRAIN) && !force {
        adapter::unfence(&engine);
        bail!(
            "BUSY_VIEW: {} filesystem callbacks still running",
            adapter::inflight()
        );
    }
    if force {
        adapter::persist_dirty(&engine);
    }
    if present {
        // The connection ends with the unmount: deliberately, not `Lost`.
        mount.stopping.store(true, Ordering::SeqCst);
        if let Err(refused) = unmount(&mount.path, false) {
            // A dispatch thread that died meanwhile makes the mount lost.
            if !force && mount.serving() {
                mount.stopping.store(false, Ordering::SeqCst);
                adapter::unfence(&engine);
                return Err(refused);
            }
            // The lazy unmount fails only if the mount is gone meanwhile.
            if let Err(error) = unmount(&mount.path, true)
                && mount.present()
            {
                mount.stopping.store(false, Ordering::SeqCst);
                adapter::unfence(&engine);
                return Err(error.context(format!("{refused:#}")));
            }
        }
    }
    stop_dispatch(mount);
    // Our own mount point (created by attach); never removes content, and
    // never a mount point (whatever was mounted there since is not ours).
    let _ = std::fs::remove_dir(&mount.path);
    *guard = None;
    adapter::set_attached_path(None);
    adapter::release(&engine);
    Ok(())
}
/// Legacy `stop` op: refuses unless the engine is quiet, then detaches
/// gracefully. The fence is lifted afterwards.
pub fn stop(engine: &Shared) -> Result<()> {
    lock_engine(engine).quiet()?;
    detach(false)
}
fn decode_mountinfo(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}
/// `(fstype, source)` of every mount at exactly `path` (`/proc/self/mountinfo`).
fn mounts_at(path: &Path) -> Result<Vec<(String, String)>> {
    let mut found = vec![];
    for line in std::fs::read_to_string("/proc/self/mountinfo")?.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 10 || Path::new(&decode_mountinfo(fields[4])) != path {
            continue;
        }
        let separator = fields
            .iter()
            .position(|field| *field == "-")
            .context("INVALID_MOUNTINFO")?;
        let field = |offset: usize| {
            fields
                .get(separator + offset)
                .map(|value| decode_mountinfo(value))
                .unwrap_or_default()
        };
        found.push((field(1), field(2)));
    }
    Ok(found)
}
/// Unmounts a stale mount of ours at `path` (type `fuse.tkfs`, source
/// `tkfs-<instance>`, no longer responding) and removes its empty mount point.
///
/// Call only after the previous `worker.json` proved `instance` ours, while
/// holding the folder's owner lock (so that worker is gone). Touches nothing
/// when no mount is at `path`; refuses with `MOUNT_PATH_MUST_NOT_EXIST`, and
/// touches nothing, when any mount there is unrelated or still responds
/// (invariant 27).
pub fn recover_managed(path: &Path, instance: &str) -> Result<()> {
    uuid::Uuid::parse_str(instance).context("INVALID_PARAMETER: worker instance")?;
    let source = format!("tkfs-{instance}");
    let mounts = mounts_at(path)?;
    if mounts.is_empty() {
        return Ok(());
    }
    ensure!(
        mounts
            .iter()
            .all(|(fstype, owner)| fstype == "fuse.tkfs" && *owner == source),
        "MOUNT_PATH_MUST_NOT_EXIST: an unrelated mount occupies {}",
        path.display()
    );
    ensure!(
        std::fs::metadata(path).is_err_and(|error| error.raw_os_error() == Some(libc::ENOTCONN)),
        "MOUNT_PATH_MUST_NOT_EXIST: the mount at {} still responds",
        path.display()
    );
    for _ in &mounts {
        unmount(path, false).map_err(|error| {
            anyhow::anyhow!(
                "MOUNT_PATH_MUST_NOT_EXIST: the stale mount at {} was not unmounted: {error:#}",
                path.display()
            )
        })?;
    }
    // The mount point the previous worker created, removed only when empty.
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && metadata.is_dir()
        && std::fs::read_dir(path)?.next().is_none()
    {
        std::fs::remove_dir(path)?;
    }
    Ok(())
}
pub fn checkout(engine: &Shared, request: &str, payload: &Value) -> Result<Value> {
    let mut guard = lock(&MOUNT);
    let mount = guard.as_mut().context("MOUNT_NOT_RUNNING")?;
    {
        let mut e = lock_engine(engine);
        if let Some(result) = e.store.receipt(request, payload)? {
            return Ok(result);
        }
        if let Some(g) = payload["generation"].as_u64() {
            ensure!(g == e.store.generation, "STALE_VIEW");
        }
        e.quiet()?;
        // Refused before the mount stops, not after (`Engine::control` checks
        // it again).
        e.checkout_allowed()?;
        let branch = e
            .store
            .branch(payload["name"].as_str().context("MISSING_ARGUMENT")?)?;
        let projection = e.store.projection(&branch.id)?;
        ensure!(projection.pending.is_empty(), "INCOMPLETE_CAUSAL_HISTORY");
        for entry in projection.entries.values().filter(|v| v.alive) {
            if let Some(object) = &entry.content {
                e.store.verify_object(object)?;
            }
        }
        e.switching = true;
        // Content waits end now (they would hold the drain below).
        adapter::set_fenced(true);
    }
    adapter::drain(DETACH_DRAIN);
    // Unmounted outside the worker: the checkout still runs, then the mount is
    // reported lost instead of mounting again at a path that is no longer ours.
    let present = mount.present();
    if present {
        mount.stopping.store(true, Ordering::SeqCst);
        if let Err(error) = unmount(&mount.path, false) {
            mount.stopping.store(false, Ordering::SeqCst);
            adapter::unfence(engine);
            return Err(error);
        }
    }
    stop_dispatch(mount);
    let result = {
        let mut e = lock_engine(engine);
        e.mounted = false;
        e.control(request, payload)
    };
    let relaunch = if present {
        crate::util::fault::fail("fs.remount")
            .and_then(|()| launch(engine.clone(), &mount.path, &mount.source))
            .map_err(|error| anyhow::anyhow!("REMOUNT_FAILED: {error:#}"))
    } else {
        Err(anyhow::anyhow!(
            "MOUNT_NOT_RUNNING: the FUSE mount at {} was unmounted outside the worker",
            mount.path.display()
        ))
    };
    match relaunch {
        Ok((worker, wake, stopping)) => {
            mount.worker = Some(worker);
            mount.wake = Some(wake);
            mount.stopping = stopping;
            result
        }
        Err(error) => {
            // The mount is gone: Lost (never `Engine.health`).
            let path = mount.path.clone();
            *guard = None;
            adapter::set_attached_path(None);
            let _ = std::fs::remove_dir(&path);
            adapter::release(engine);
            adapter::report_lost(&error);
            Err(error)
        }
    }
}

// Protocol integer fields use native endian as required by linux/fuse.h.
fn u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_ne_bytes(
        bytes
            .get(offset..offset + 4)
            .context("INVALID_PARAMETER")?
            .try_into()?,
    ))
}
fn u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    Ok(u64::from_ne_bytes(
        bytes
            .get(offset..offset + 8)
            .context("INVALID_PARAMETER")?
            .try_into()?,
    ))
}
fn push32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend(value.to_ne_bytes());
}
fn push64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend(value.to_ne_bytes());
}
fn name(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes.split(|v| *v == 0).next().context("INVALID_NAME")?)
        .context("INVALID_NAME: UTF-8 required")
}
fn errno(error: &anyhow::Error) -> i32 {
    let text = format!("{error:#}");
    if text.contains("UNSUPPORTED_FUSE_OPCODE") {
        libc::ENOSYS // Let the kernel fall back (e.g. STATX to GETATTR).
    } else if text.contains("CONTENT_UNAVAILABLE") {
        // Not EAGAIN (CCR-WP4.INT-2): a blocking read that answers "try
        // again" is retried in a loop or misreported; the content could not
        // arrive in time, an I/O error.
        libc::EIO
    } else if text.contains("FILE_NOT_FOUND") {
        libc::ENOENT
    } else if text.contains("NAME_COLLISION") {
        libc::EEXIST
    } else if text.contains("NOT_EMPTY") {
        libc::ENOTEMPTY
    } else if text.contains("NOT_DIRECTORY") {
        libc::ENOTDIR
    } else if text.contains("IS_DIRECTORY") {
        libc::EISDIR
    } else if text.contains("BUSY") {
        libc::EBUSY
    } else if text.contains("ACCESS_DENIED") || text.contains("PERMISSION") {
        libc::EACCES
    } else if text.contains("UNSUPPORTED") {
        libc::EOPNOTSUPP
    } else if text.contains("FILE_TOO_LARGE") {
        libc::EFBIG
    } else if text.contains("INVALID") {
        libc::EINVAL
    } else {
        libc::EIO
    }
}
struct Filesystem {
    by_inode: BTreeMap<u64, String>,
    by_entity: BTreeMap<String, u64>,
    directory_inodes: BTreeMap<(String, String), u64>,
    directory_birth: BTreeMap<u64, String>,
    next_inode: u64,
}
impl Filesystem {
    fn new() -> Self {
        Self {
            by_inode: BTreeMap::from([(1, ROOT.into()), (2, "control".into())]),
            by_entity: BTreeMap::from([(ROOT.into(), 1), ("control".into(), 2)]),
            directory_inodes: BTreeMap::new(),
            directory_birth: BTreeMap::new(),
            next_inode: 3,
        }
    }
    fn inode(&mut self, entity: &str) -> u64 {
        if let Some(inode) = self.by_entity.get(entity) {
            return *inode;
        }
        let inode = self.next_inode;
        self.next_inode += 1;
        self.by_entity.insert(entity.into(), inode);
        self.by_inode.insert(inode, entity.into());
        inode
    }
    fn entry_inode(&mut self, e: &crate::runtime::Engine, entry: &Entry) -> Result<u64> {
        if entry.kind != "directory" || entry.id == ROOT {
            return Ok(self.inode(&entry.id));
        }
        let birth = e
            .handles
            .values()
            .find(|h| {
                h.entity == entry.id
                    && h.directory_basis.as_ref().is_some_and(|basis| {
                        basis.revisions.get("alive") == entry.revisions.get("alive")
                    })
            })
            .and_then(|h| h.directory_birth.as_ref())
            .or_else(|| entry.revisions.get("alive"))
            .context("FILE_NOT_FOUND: directory existence head")?
            .clone();
        let key = (entry.id.clone(), birth.clone());
        if let Some(inode) = self.directory_inodes.get(&key) {
            return Ok(*inode);
        }
        let inode = self.next_inode;
        self.next_inode += 1;
        self.by_inode.insert(inode, entry.id.clone());
        self.directory_inodes.insert(key, inode);
        self.directory_birth.insert(inode, birth);
        Ok(inode)
    }
    fn entry(&self, e: &crate::runtime::Engine, inode: u64) -> Result<Entry> {
        let entity = self.by_inode.get(&inode).context("FILE_NOT_FOUND")?;
        if entity == ROOT {
            return e.store.lookup("");
        }
        if entity == "control" {
            return Ok(Entry {
                id: "control".into(),
                name: ".tkfs-runtime.json".into(),
                parent: ROOT.into(),
                kind: "file".into(),
                alive: true,
                ..Default::default()
            });
        }
        let entry = e.store.entry(entity)?;
        ensure!(entry.alive, "FILE_NOT_FOUND");
        if let Some(birth) = self.directory_birth.get(&inode) {
            ensure!(entry.revisions.get("alive") == Some(birth), "STALE_VIEW");
        }
        Ok(entry)
    }
    /// An open native FD retains its inode after namespace removal. This is
    /// deliberately used only for attribute operations, never path traversal.
    fn retained_entry(
        &self,
        e: &crate::runtime::Engine,
        inode: u64,
        handle: Option<u64>,
    ) -> Result<Entry> {
        let entity = self.by_inode.get(&inode).context("FILE_NOT_FOUND")?;
        if let Some(handle) = handle {
            ensure!(&e.session(handle)?.entry.id == entity, "INVALID_HANDLE");
            let entry = e.handle_entry(handle)?;
            if entry.kind == "directory" {
                if let Some(birth) = self.directory_birth.get(&inode) {
                    ensure!(
                        e.handles[&handle].directory_birth.as_ref() == Some(birth),
                        "INVALID_HANDLE"
                    );
                }
                return Ok(entry);
            }
        }
        if entity == ROOT || entity == "control" {
            return self.entry(e, inode);
        }
        if let Some(birth) = self.directory_birth.get(&inode) {
            if let Some((handle, _)) = e
                .handles
                .iter()
                .find(|(_, h)| &h.entity == entity && h.directory_birth.as_ref() == Some(birth))
            {
                return e.handle_entry(*handle);
            }
            return self.entry(e, inode);
        }
        let current = e.store.entry(entity)?;
        if !current.alive
            && let Some((handle, _)) = e
                .handles
                .iter()
                .find(|(_, h)| &h.entity == entity && h.directory_basis.is_some())
        {
            return e.handle_entry(*handle);
        }
        if let Some(session) = e.sessions.get(entity)
            && session.references > 0
            && session.bytes.cow_file().is_some()
        {
            let mut entry = session.entry.clone();
            entry.alive = e.store.entry(entity)?.alive;
            return Ok(entry);
        }
        self.entry(e, inode)
    }
    fn path(&self, e: &crate::runtime::Engine, entry: &Entry) -> Result<String> {
        let mut entry = entry.clone();
        let mut names = vec![];
        for _ in 0..4096 {
            if entry.id == ROOT {
                names.reverse();
                return Ok(names.join("/"));
            }
            names.push(entry.name);
            entry = if entry.parent == ROOT {
                e.store.lookup("")?
            } else {
                e.store.entry(&entry.parent)?
            };
        }
        bail!("INVALID_NAMESPACE_CYCLE")
    }
    fn child(&self, e: &crate::runtime::Engine, parent: u64, child: &str) -> Result<String> {
        let entry = self.entry(e, parent)?;
        ensure!(entry.kind == "directory", "NOT_DIRECTORY");
        let path = self.path(e, &entry)?;
        Ok(if path.is_empty() {
            child.into()
        } else {
            format!("{path}/{child}")
        })
    }
    fn attr(&mut self, e: &crate::runtime::Engine, entry: &Entry) -> Result<Vec<u8>> {
        let inode = self.entry_inode(e, entry)?;
        let basic = e
            .sessions
            .get(&entry.id)
            .filter(|session| matches!(&session.bytes, crate::staging::Staged::Cow(_)))
            .map_or_else(|| entry.basic_info(), |session| session.entry.basic_info());
        let size = if entry.id == "control" {
            e.discovery.len() as u64
        } else if let Some(session) = e.sessions.get(&entry.id) {
            session.bytes.len() as u64
        } else {
            e.visible_size(entry)? as u64
        };
        let mut out = Vec::with_capacity(88);
        push64(&mut out, inode);
        push64(&mut out, size);
        push64(&mut out, size.div_ceil(512));
        let timestamps = [basic.access_time, basic.write_time, basic.change_time]
            .map(|v| v.saturating_sub(FILETIME_EPOCH));
        for time in timestamps {
            push64(&mut out, time / 10_000_000);
        }
        for time in timestamps {
            push32(&mut out, ((time % 10_000_000) * 100) as u32);
        }
        let default = if entry.kind == "directory" {
            0o755
        } else {
            0o644
        };
        let mut mode = basic.posix_mode.unwrap_or(default);
        if basic.attributes & 1 != 0 {
            mode &= !0o222;
        }
        if entry.id == "control" {
            mode = 0o400;
        }
        push32(
            &mut out,
            mode | if entry.kind == "directory" {
                libc::S_IFDIR
            } else {
                libc::S_IFREG
            },
        );
        push32(
            &mut out,
            if !entry.alive {
                0
            } else if entry.kind == "directory" {
                2
            } else {
                1
            },
        );
        push32(&mut out, unsafe { libc::geteuid() });
        push32(&mut out, unsafe { libc::getegid() });
        push32(&mut out, 0);
        push32(&mut out, 4096);
        push32(&mut out, 0);
        Ok(out)
    }
    fn entry_out(&mut self, e: &crate::runtime::Engine, entry: &Entry) -> Result<Vec<u8>> {
        let inode = self.entry_inode(e, entry)?;
        let mut out = vec![];
        push64(&mut out, inode);
        push64(&mut out, e.store.generation + 1);
        out.resize(40, 0);
        out.extend(self.attr(e, entry)?);
        Ok(out)
    }
    fn attr_out(&mut self, e: &crate::runtime::Engine, entry: &Entry) -> Result<Vec<u8>> {
        let mut out = vec![0; 16];
        out.extend(self.attr(e, entry)?);
        Ok(out)
    }
    fn open_out(handle: u64, directory: bool) -> Vec<u8> {
        let mut out = vec![];
        push64(&mut out, handle);
        push32(&mut out, if directory { 0 } else { 1 });
        push32(&mut out, 0);
        out
    }
    fn dispatch(
        &mut self,
        e: &mut crate::runtime::Engine,
        op: u32,
        inode: u64,
        p: &[u8],
    ) -> Result<Vec<u8>> {
        if op == 26 {
            ensure!(
                u32_at(p, 0)? == 7 && u32_at(p, 4)? >= 31,
                "UNSUPPORTED_FUSE_PROTOCOL"
            );
            let mut out = vec![];
            push32(&mut out, 7);
            push32(&mut out, 31);
            push32(&mut out, 0);
            push32(&mut out, 1 << 5);
            out.extend(16u16.to_ne_bytes());
            out.extend(12u16.to_ne_bytes());
            push32(&mut out, MAX_IO as u32);
            push32(&mut out, 100);
            out.resize(64, 0);
            return Ok(out);
        }
        if op == 17 {
            let mut out = vec![];
            for n in [262144, 131072, 131072, 1_000_000, 999_000] {
                push64(&mut out, n);
            }
            for n in [4096, 255, 4096, 0] {
                push32(&mut out, n);
            }
            out.resize(80, 0);
            return Ok(out);
        }
        // STATX must return ENOSYS before checking namespace liveness, so the
        // kernel can fall back to GETATTR for an open unlinked inode as well.
        if op == 52 {
            bail!("UNSUPPORTED_FUSE_OPCODE: {op}");
        }
        if [5, 6, 13, 21, 22, 24, 39, 43, 47].contains(&op) {
            bail!("UNSUPPORTED_POSIX_OPERATION: opcode={op}");
        }
        if op == 23 {
            return if u32_at(p, 0)? == 0 {
                Ok(vec![0; 8])
            } else {
                Ok(vec![])
            };
        }
        if op == 38 {
            return Ok(vec![]);
        }
        ensure!(!e.switching, "BUSY_VIEW");
        let entry = if op == 3 {
            let handle = if p.len() >= 16 && u32_at(p, 0)? & 1 != 0 {
                Some(u64_at(p, 8)?)
            } else {
                None
            };
            self.retained_entry(e, inode, handle)?
        } else {
            self.entry(e, inode)?
        };
        match op {
            1 => {
                let child = name(p)?;
                let found = if inode == 1 && child == ".tkfs-runtime.json" {
                    self.entry(e, 2)?
                } else {
                    e.store.lookup(&self.child(e, inode, child)?)?
                };
                self.entry_out(e, &found)
            }
            3 => self.attr_out(e, &entry),
            14 | 27 => {
                let directory = op == 27;
                ensure!(
                    directory == (entry.kind == "directory"),
                    if directory {
                        "NOT_DIRECTORY"
                    } else {
                        "IS_DIRECTORY"
                    }
                );
                let flags = u32_at(p, 0)?;
                let (h, _) = e.open(
                    &self.path(e, &entry)?,
                    None,
                    !directory && flags & libc::O_ACCMODE as u32 != 0,
                )?;
                Ok(Self::open_out(h, directory))
            }
            15 => {
                ensure!(
                    e.handle_entry(u64_at(p, 0)?)?.id == "control",
                    "MISSING_CONTENT_DESCRIPTOR"
                );
                let session = e.session(u64_at(p, 0)?)?;
                let size = (u32_at(p, 16)? as usize).min(MAX_IO);
                let mut out = vec![0; size];
                let count = session
                    .bytes
                    .read_at(usize::try_from(u64_at(p, 8)?)?, &mut out)?;
                out.truncate(count);
                Ok(out)
            }
            29 => {
                ensure!(
                    e.handle_entry(u64_at(p, 0)?)?.kind == "directory",
                    "NOT_DIRECTORY"
                );
                e.release_handle(u64_at(p, 0)?)?;
                // Qualification hook: a close-path failure after the handle closed.
                crate::util::fault::fail("fs.close")?;
                Ok(vec![])
            }
            30 => {
                ensure!(
                    e.handle_entry(u64_at(p, 0)?)?.kind == "directory",
                    "NOT_DIRECTORY"
                );
                Ok(vec![])
            }
            28 => {
                let h = u64_at(p, 0)?;
                let mut ordinal = usize::try_from(u64_at(p, 8)?)?;
                let limit = u32_at(p, 16)? as usize;
                let mut out = vec![];
                loop {
                    let Some((child, _)) = e.directory_entry(h, ordinal, ordinal == 0)? else {
                        break;
                    };
                    let name = child.name.as_bytes();
                    let length = (24 + name.len()).next_multiple_of(8);
                    if out.len() + length > limit {
                        break;
                    }
                    let base = out.len();
                    push64(&mut out, self.entry_inode(e, &child)?);
                    push64(&mut out, ordinal as u64 + 1);
                    push32(&mut out, name.len() as u32);
                    push32(&mut out, if child.kind == "directory" { 4 } else { 8 });
                    out.extend(name);
                    out.resize(base + length, 0);
                    ordinal += 1;
                }
                Ok(out)
            }
            34 => Ok(vec![]), // default_permissions performs kernel permission checks.
            36 => bail!("INVALID_INTERRUPT"),
            _ => bail!("UNSUPPORTED_FUSE_OPCODE: {op}"),
        }
    }
}
/// The FUSE opcodes [`serve`] routes itself (`enum fuse_opcode` in
/// `linux/fuse.h`).
const FUSE_SETATTR: u32 = 4;
const FUSE_READ: u32 = 15;
const FUSE_WRITE: u32 = 16;
const FUSE_RELEASE: u32 = 18;
/// fsync(2) of a file.
const FUSE_FSYNC: u32 = 20;
/// Every close(2) of a descriptor (the last one is followed by a release).
const FUSE_FLUSH: u32 = 25;
const FUSE_RELEASEDIR: u32 = 29;
/// Failures on the close path (flush, release) and unexpected I/O errors are
/// counted as mount errors; the rest are expected answers (`ENOENT`, …).
fn counted(op: u32, error: &anyhow::Error) -> bool {
    [FUSE_RELEASE, FUSE_FLUSH, FUSE_RELEASEDIR].contains(&op) || errno(error) == libc::EIO
}
/// Whether [`serve`] runs a request on a thread of its own ([`threaded`]):
/// reads, writes, fsyncs, flushes and size changes, whose work without the
/// engine lock (a content wait, a writable copy, a save) may take long.
/// `body` is the request after its header.
fn runs_threaded(op: u32, _body: &[u8]) -> bool {
    [
        8,
        9,
        35,
        14,
        FUSE_READ,
        FUSE_WRITE,
        FUSE_FSYNC,
        FUSE_FLUSH,
        FUSE_RELEASE,
        10,
        11,
        12,
        45,
    ]
    .contains(&op)
        || op == FUSE_SETATTR
}
/// Why the dispatch loop stopped.
enum Exit {
    /// The wake end was written or closed (a detach or checkout).
    Woken,
    /// The connection ended (`ENODEV` or end of file): the mount was
    /// unmounted, by a detach or checkout or outside the worker.
    DeviceGone,
}
fn serve(mut device: File, engine: Shared, wake: UnixStream) -> Result<Exit> {
    // Non-blocking reads after `poll`: a withdrawn request never blocks the
    // loop, so a stop request is always seen.
    let flags = unsafe { libc::fcntl(device.as_raw_fd(), libc::F_GETFL) };
    ensure!(
        flags >= 0
            && unsafe { libc::fcntl(device.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                == 0,
        "FUSE_DESCRIPTOR_INVALID: {}",
        std::io::Error::last_os_error()
    );
    let fs = Arc::new(Mutex::new(Filesystem::new()));
    let writer = Arc::new(Mutex::new(device.try_clone()?));
    let threads = Threads::new(MAX_THREADED);
    let mut bytes = vec![0; MAX_IO + 4096];
    loop {
        let mut fds = [
            libc::pollfd {
                fd: device.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        if unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) } < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error.into());
        }
        if fds[1].revents != 0 {
            // Detach: the connection aborts once the device closes.
            return Ok(Exit::Woken);
        }
        // Qualification hook: the dispatch thread dies (`Lost`).
        crate::util::fault::fail("fs.dispatch")?;
        let count = match device.read(&mut bytes) {
            Ok(0) => return Ok(Exit::DeviceGone),
            Ok(n) => n,
            Err(e) if e.raw_os_error() == Some(libc::ENODEV) => return Ok(Exit::DeviceGone),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
                ) =>
            {
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let p = &bytes[..count];
        ensure!(
            count >= 40 && u32_at(p, 0)? as usize == count,
            "INVALID_FUSE_PACKET"
        );
        let op = u32_at(p, 4)?;
        if [2, 42].contains(&op) {
            continue;
        }
        // Counted until replied, so a detach can drain callbacks.
        let callback = adapter::callback();
        let unique = u64_at(p, 8)?;
        let inode = u64_at(p, 16)?;
        // Content waits and saves must not stop the single /dev/fuse reader
        // from serving unrelated lookups and metadata requests: reads,
        // writes, fsyncs, flushes and size changes run on threads of their
        // own (WP5.2 added the last three: their saves hash and copy without
        // the engine lock, but they still take time). At most
        // `MAX_THREADED` run at once; the others wait in line for one of
        // those threads, never refused (only reads waiting for downloads are
        // bounded on their own, `CONTENT_WAITERS`).
        if runs_threaded(op, &p[40..]) {
            let body = p[40..].to_vec();
            let engine = engine.clone();
            let fs = fs.clone();
            let writer = writer.clone();
            threads.run(Box::new(move || {
                let _callback = callback;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    threaded(&engine, &fs, op, inode, &body)
                }))
                .unwrap_or_else(|_| Err(anyhow::anyhow!("FUSE_CALLBACK_PANIC")));
                if let Err(error) = &result {
                    adapter::fs_error(op, error, counted(op, error));
                }
                let _ = write_reply(&writer, unique, result);
            }));
            continue;
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if [FUSE_RELEASE, FUSE_RELEASEDIR, 10, 11, 12, 45].contains(&op) {
                fenced(&engine)?;
            }
            match op {
                // The last close: saves without the engine lock what no save
                // covers yet, then the handle goes (`close_handle`).
                FUSE_RELEASE | FUSE_RELEASEDIR => {
                    crate::runtime::engine::close_handle(&engine, u64_at(&p[40..], 0)?)?;
                    // Qualification hook: a close-path failure after the handle closed.
                    crate::util::fault::fail("fs.close")?;
                    return Ok(vec![]);
                }
                _ => {}
            }
            let _foreground = crate::runtime::engine::foreground();
            let mut e = lock_engine(&engine);
            let result = lock(&fs).dispatch(&mut e, op, inode, &p[40..]);
            e.publish_observation();
            result
        }))
        .unwrap_or_else(|_| Err(anyhow::anyhow!("FUSE_CALLBACK_PANIC")));
        if let Err(error) = &result {
            adapter::fs_error(op, error, counted(op, error));
        }
        write_reply(&writer, unique, result)?;
        drop(callback);
    }
}
/// Requests handled on a thread of their own at most at once ([`Threads`]).
const MAX_THREADED: usize = 64;
/// A request [`serve`] hands to [`Threads`]: it replies itself.
type Job = Box<dyn FnOnce() + Send>;
/// The threads [`serve`] runs requests on: one per request, at most `max` at
/// once. Past that a request waits in line (first in, first out) for one of
/// them to finish its own, instead of being refused: a close, an fsync or a
/// write never fails for want of a thread (WP5.2 fix round 2; they used to
/// answer `EIO`). Only reads that wait for downloads are bounded on their
/// own ([`CONTENT_WAITERS`]), and fail at once past that bound.
struct Threads {
    max: usize,
    state: Mutex<ThreadsState>,
}
#[derive(Default)]
struct ThreadsState {
    /// Threads running requests.
    running: usize,
    /// Requests waiting for one of them.
    waiting: std::collections::VecDeque<Job>,
}
impl Threads {
    fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            max: max.max(1),
            state: Mutex::new(ThreadsState::default()),
        })
    }
    /// Runs `job` on a thread of its own, or queues it for the next thread
    /// that finishes. When no thread can be created, `job` runs here, on
    /// the caller (the dispatch thread).
    fn run(self: &Arc<Self>, job: Job) {
        let mut state = lock(&self.state);
        if state.running >= self.max {
            state.waiting.push_back(job);
            return;
        }
        state.running += 1;
        drop(state);
        // The job stays here if the thread cannot be created.
        let slot = Arc::new(Mutex::new(Some(job)));
        let (threads, theirs) = (self.clone(), slot.clone());
        let spawned = std::thread::Builder::new()
            .name("fuse-request".into())
            .spawn(move || {
                let job = lock(&theirs).take();
                if let Some(job) = job {
                    contained(job);
                }
                threads.serve_waiting();
            });
        if spawned.is_err() {
            // Only `serve` queues, so nothing was queued since the count
            // went up: the threads already running serve what waits.
            lock(&self.state).running -= 1;
            let job = lock(&slot).take();
            if let Some(job) = job {
                contained(job);
            }
        }
    }
    /// Runs the waiting requests one after the other until none is left,
    /// then ends this thread's turn (under the same lock as [`Threads::run`]
    /// queues, so a request never waits with no thread to serve it).
    fn serve_waiting(&self) {
        loop {
            let job = {
                let mut state = lock(&self.state);
                match state.waiting.pop_front() {
                    Some(job) => job,
                    None => {
                        state.running -= 1;
                        return;
                    }
                }
            };
            contained(job);
        }
    }
}
/// Runs `job`; a panic ends the job, never the thread serving the line.
fn contained(job: Job) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
}
/// The `setattr` valid bits this adapter applies: mode, size, the times
/// (and their `now` forms), the file handle, `ctime`; never ownership.
const SETATTR_SUPPORTED: u32 = (1 << 0)
    | (1 << 3)
    | (1 << 4)
    | (1 << 5)
    | (1 << 6)
    | (1 << 7)
    | (1 << 8)
    | (1 << 9)
    | (1 << 10)
    | (1 << 11);
/// How long a read waits for content that is downloading (the WinFsp
/// adapter's bounds: FUSE has no request timeout of its own).
const CONTENT_STALL: std::time::Duration = std::time::Duration::from_secs(60);
const CONTENT_BOUND: std::time::Duration = std::time::Duration::from_secs(240);
/// Reads waiting for downloads at once (half of [`MAX_THREADED`]).
static CONTENT_WAITERS: crate::runtime::content::WaitSlots =
    crate::runtime::content::WaitSlots::new(MAX_THREADED / 2);
fn content_wait() -> crate::runtime::content::ContentWait {
    let (stall, bound) = crate::runtime::content::wait_bounds(CONTENT_STALL, CONTENT_BOUND);
    crate::runtime::content::ContentWait {
        stall,
        bound,
        slots: Some(&CONTENT_WAITERS),
        cancelled: adapter::fenced,
    }
}
/// `BUSY_VIEW` while a detach or checkout fences the engine (`dispatch`'s
/// rule, applied before the work a request does without the lock).
fn fenced(engine: &Shared) -> Result<()> {
    ensure!(!lock_engine(engine).switching, "BUSY_VIEW");
    Ok(())
}
/// One request on a thread of its own (`serve`, [`runs_threaded`]): the work
/// that needs no engine lock first (waiting for content, the writable copy
/// of a large file, a save's hashing and copying), then the request under
/// the lock.
fn threaded(
    engine: &Shared,
    fs: &Mutex<Filesystem>,
    op: u32,
    inode: u64,
    body: &[u8],
) -> Result<Vec<u8>> {
    use crate::runtime::engine::{save_handle, save_handle_in_background, write_behind};
    // The work before the lock honours the fence as `dispatch` does: a
    // detach or checkout stops every request but the few `dispatch` exempts.
    fenced(engine)?;
    ensure!(
        lock_engine(engine).native_writes.is_some(),
        "CONTENT_STORE_NOT_INITIALIZED"
    );
    match op {
        8 | 9 | 35 => {
            let (mode, flags, skip) = if op == 8 {
                let mode = u32_at(body, 0)?;
                ensure!(
                    mode & libc::S_IFMT == libc::S_IFREG,
                    "UNSUPPORTED_POSIX_KIND"
                );
                (mode, libc::O_RDWR as u32, 16)
            } else if op == 9 {
                (u32_at(body, 0)?, libc::O_RDONLY as u32, 8)
            } else {
                (u32_at(body, 4)?, u32_at(body, 0)?, 16)
            };
            ensure!(mode & 0o7000 == 0, "UNSUPPORTED_POSIX_MODE");
            let (path, parent) = {
                let e = lock_engine(engine);
                let filesystem = lock(fs);
                (
                    filesystem.child(
                        &e,
                        inode,
                        name(body.get(skip..).context("INVALID_PARAMETER")?)?,
                    )?,
                    filesystem.entry(&e, inode)?.id,
                )
            };
            let create = if op == 9 {
                crate::runtime::content_writes::create_directory
            } else {
                crate::runtime::content_writes::create
            };
            let (handle, created) = create(
                engine,
                &path,
                crate::runtime::content_writes::CreateOptions {
                    writable: flags & libc::O_ACCMODE as u32 != libc::O_RDONLY as u32,
                    posix_mode: Some(mode & 0o777),
                    expected_parent: Some(parent),
                    ..Default::default()
                },
                content_wait(),
            )?
            .context("CONTENT_STORE_NOT_INITIALIZED")?;
            {
                let result = {
                    let e = lock_engine(engine);
                    lock(fs).entry_out(&e, &created).map(|mut out| {
                        if op == 35 {
                            out.extend(Filesystem::open_out(handle, false));
                        }
                        out
                    })
                };
                if op != 35 || result.is_err() {
                    lock_engine(engine).release_handle(handle)?;
                }
                result
            }
        }
        10 | 11 => {
            let (entity, path) = {
                let e = lock_engine(engine);
                let filesystem = lock(fs);
                let path = filesystem.child(&e, inode, name(body)?)?;
                let entry = e.store.lookup(&path)?;
                ensure!(
                    entry.kind == if op == 11 { "directory" } else { "file" },
                    if op == 11 {
                        "NOT_DIRECTORY"
                    } else {
                        "IS_DIRECTORY"
                    }
                );
                (
                    entry.id,
                    crate::core::content_saves::NamespaceTarget {
                        path,
                        parent: entry.parent,
                    },
                )
            };
            let removed = if op == 11 {
                crate::runtime::content_writes::remove_directory(
                    engine,
                    &entity,
                    &path,
                    content_wait(),
                )?
            } else {
                crate::runtime::content_writes::unlink(engine, &entity, &path.path, content_wait())?
            };
            ensure!(removed, "CONTENT_STORE_NOT_INITIALIZED");
            Ok(vec![])
        }
        12 | 45 => {
            let (entity, source, target, replace) = {
                let e = lock_engine(engine);
                let filesystem = lock(fs);
                let newdir = u64_at(body, 0)?;
                let flags = if op == 45 { u32_at(body, 8)? } else { 0 };
                ensure!(flags & !1 == 0, "UNSUPPORTED_RENAME_FLAGS");
                let names = body
                    .get(if op == 45 { 16 } else { 8 }..)
                    .context("INVALID_PARAMETER")?;
                let old = name(names)?;
                let new = name(names.get(old.len() + 1..).context("INVALID_PARAMETER")?)?;
                let source = filesystem.child(&e, inode, old)?;
                let entry = e.store.lookup(&source)?;
                (
                    entry.id,
                    crate::core::content_saves::NamespaceTarget {
                        path: source,
                        parent: entry.parent,
                    },
                    crate::core::content_saves::NamespaceTarget {
                        path: filesystem.child(&e, newdir, new)?,
                        parent: filesystem.entry(&e, newdir)?.id,
                    },
                    flags & 1 == 0,
                )
            };
            if crate::runtime::content_writes::rename_directory(
                engine,
                &entity,
                None,
                Some(&source),
                &target,
                replace,
                content_wait(),
            )? {
                return Ok(vec![]);
            }
            if crate::runtime::content_writes::rename(
                engine,
                &entity,
                &target.path,
                replace,
                content_wait(),
            )? {
                return Ok(vec![]);
            }
            bail!("PERMISSION_DENIED: root/control rename");
        }
        14 => {
            let result = {
                let mut e = lock_engine(engine);
                let result = lock(fs).dispatch(&mut e, op, inode, body)?;
                e.publish_observation();
                result
            };
            let handle = u64_at(&result, 0)?;
            if let Err(error) =
                crate::runtime::content_reads::prepare_open(engine, handle, content_wait())
            {
                let _ = lock_engine(engine).release_handle(handle);
                return Err(error);
            }
            if u32_at(body, 0)? & libc::O_TRUNC as u32 != 0 {
                let truncated = (|| {
                    ensure!(
                        crate::runtime::content_writes::truncate(
                            engine,
                            handle,
                            0,
                            false,
                            content_wait(),
                        )?,
                        "CONTENT_WRITE_REQUIRES_PRIVATE_SESSION"
                    );
                    save_handle(engine, handle)
                })();
                if let Err(error) = truncated {
                    let _ = lock_engine(engine).release_handle(handle);
                    return Err(error);
                }
            }
            Ok(result)
        }
        FUSE_READ | FUSE_WRITE => {
            let handle = u64_at(body, 0)?;
            if op == FUSE_WRITE {
                let size = u32_at(body, 16)? as usize;
                ensure!(size <= MAX_IO, "INVALID_PARAMETER");
                let bytes = body.get(40..40 + size).context("INVALID_PARAMETER")?;
                let count = crate::runtime::content_writes::write(
                    engine,
                    handle,
                    u64_at(body, 8)?,
                    bytes,
                    u32_at(body, 32)? & libc::O_APPEND as u32 != 0,
                    false,
                    content_wait(),
                )?
                .context("CONTENT_WRITE_REQUIRES_PRIVATE_SESSION")?;
                {
                    let mut out = Vec::new();
                    push32(&mut out, count as u32);
                    push32(&mut out, 0);
                    return Ok(out);
                }
            }
            if op == FUSE_READ
                && let Some(bytes) = crate::runtime::content_reads::read_mount_vec(
                    engine,
                    handle,
                    u64_at(body, 8)?,
                    (u32_at(body, 16)? as usize).min(MAX_IO),
                    content_wait(),
                )?
            {
                return Ok(bytes);
            }
            let _foreground = crate::runtime::engine::foreground();
            let mut e = lock_engine(engine);
            let result = lock(fs).dispatch(&mut e, op, inode, body);
            e.publish_observation();
            result
        }
        // fsync(2): a durability point, never write-behind. What was written
        // through the descriptor is committed before the answer, a save that
        // a close queued or a saver runs included (`save_handle` saves it
        // itself, or waits for that save and saves what came after it).
        FUSE_FSYNC => {
            let handle = u64_at(body, 0)?;
            lock_engine(engine).session(handle)?;
            save_handle(engine, handle)?;
            Ok(vec![])
        }
        // A close(2) of a descriptor: during a burst of closes the save goes
        // on in the background (`write_behind`; with the savers' queue full
        // it saves here, before the answer: backpressure); a lone close saves
        // before it is answered.
        FUSE_FLUSH => {
            let handle = u64_at(body, 0)?;
            if write_behind() {
                save_handle_in_background(engine, handle)?;
            } else {
                crate::runtime::engine::save_handle_on_cleanup(engine, handle)?;
            }
            Ok(vec![])
        }
        FUSE_SETATTR => setattr_native(engine, fs, inode, body),
        FUSE_RELEASE => {
            crate::runtime::engine::close_handle(engine, u64_at(body, 0)?)?;
            crate::util::fault::fail("fs.close")?;
            Ok(vec![])
        }
        _ => bail!("UNSUPPORTED_FUSE_OPCODE: {op}"),
    }
}
/// Basic-info changes on a native file use the owned metadata coordinator.
/// Size changes on an open handle remain buffered until fsync/close. A
/// temporary handle for path-only truncate reaches its close boundary below.
fn setattr_native(
    engine: &Shared,
    fs: &Mutex<Filesystem>,
    inode: u64,
    body: &[u8],
) -> Result<Vec<u8>> {
    use crate::core::content_saves::ContentBasicUpdate;
    let valid = u32_at(body, 0)?;
    ensure!(
        valid & !SETATTR_SUPPORTED == 0,
        "UNSUPPORTED_POSIX_OWNERSHIP"
    );
    let mut mode = ContentBasicUpdate::default();
    if valid & 1 != 0 {
        let value = u32_at(body, 68)?;
        ensure!(value & 0o7000 == 0, "UNSUPPORTED_POSIX_MODE");
        mode.posix_mode = Some(value & 0o777);
        mode.readonly = Some(value & 0o222 == 0);
        mode.change_time = Some(filetime(time()));
    }
    let mut times = ContentBasicUpdate::default();
    for (bit, nowbit, offset, ns_offset, slot) in [
        (1 << 4, 1 << 7, 32, 56, 0),
        (1 << 5, 1 << 8, 40, 60, 1),
        (1 << 10, 0, 48, 64, 2),
    ] {
        if valid & (bit | nowbit) != 0 {
            let value = if valid & nowbit != 0 {
                filetime(time())
            } else {
                let ns = u32_at(body, ns_offset)?;
                ensure!(ns < 1_000_000_000, "INVALID_TIMESTAMP");
                u64_at(body, offset)?
                    .checked_mul(10_000_000)
                    .and_then(|v| v.checked_add((ns / 100) as u64))
                    .and_then(|v| v.checked_add(FILETIME_EPOCH))
                    .context("INVALID_TIMESTAMP")?
            };
            match slot {
                0 => times.access_time = Some(value),
                1 => times.write_time = Some(value),
                _ => times.change_time = Some(value),
            }
        }
    }
    mode.validate()?;
    times.validate()?;
    let (handle, temporary) = {
        let mut e = lock_engine(engine);
        let mut filesystem = lock(fs);
        let entry = filesystem.retained_entry(
            &e,
            inode,
            if valid & (1 << 6) != 0 {
                Some(u64_at(body, 8)?)
            } else {
                None
            },
        )?;
        ensure!(e.native_writes.is_some(), "CONTENT_STORE_NOT_INITIALIZED");
        if entry.id == ROOT || entry.id == "control" {
            ensure!(
                entry.id != "control"
                    || (mode.is_empty() && times.is_empty() && valid & (1 << 3) == 0),
                "PERMISSION_DENIED"
            );
            ensure!(
                times.is_empty()
                    && valid & (1 << 3) == 0
                    && mode.posix_mode.is_none_or(|m| m == 0o755),
                "UNSUPPORTED_FS_OPERATION: root metadata"
            );
            return filesystem.attr_out(&e, &entry);
        }
        ensure!(
            entry.kind == "directory"
                || entry.content_ref.is_some()
                || e.sessions
                    .get(&entry.id)
                    .is_some_and(|s| s.bytes.cow_file().is_some()),
            "MISSING_CONTENT_DESCRIPTOR"
        );
        ensure!(
            entry.kind != "directory" || valid & (1 << 3) == 0,
            "IS_DIRECTORY"
        );
        if valid & (1 << 6) != 0 {
            let handle = u64_at(body, 8)?;
            ensure!(e.session(handle)?.entry.id == entry.id, "INVALID_HANDLE");
            (handle, false)
        } else if !entry.alive {
            // Linux can issue fd-originated timestamp updates without FATTR_FH.
            // The retained inode still identifies the same open native entity.
            let handle = e
                .handles
                .iter()
                .find(|(_, h)| {
                    h.entity == entry.id
                        && !h.cleaned
                        && (valid & (1 << 3) == 0 || h.writable)
                        && h.directory_basis.as_ref().is_none_or(|basis| {
                            basis.revisions.get("alive") == entry.revisions.get("alive")
                        })
                })
                .map(|(h, _)| *h)
                .context("INVALID_HANDLE")?;
            (handle, false)
        } else {
            let path = filesystem.path(&e, &entry)?;
            (e.open(&path, None, valid & (1 << 3) != 0)?.0, true)
        }
    };
    let updated = (|| {
        crate::runtime::content_reads::prepare_open(engine, handle, content_wait())?;
        if !mode.is_empty() {
            ensure!(
                crate::runtime::content_writes::update_basic(engine, handle, mode, content_wait())?,
                "STALE_CONTENT_VIEW"
            );
        }
        if valid & (1 << 3) != 0 {
            ensure!(
                crate::runtime::content_writes::truncate(
                    engine,
                    handle,
                    u64_at(body, 16)?,
                    false,
                    content_wait()
                )?,
                "STALE_CONTENT_VIEW"
            );
        }
        if !times.is_empty() {
            ensure!(
                crate::runtime::content_writes::update_basic(
                    engine,
                    handle,
                    times,
                    content_wait()
                )?,
                "STALE_CONTENT_VIEW"
            );
        }
        let e = lock_engine(engine);
        let mut filesystem = lock(fs);
        let entry = filesystem.retained_entry(&e, inode, Some(handle))?;
        filesystem.attr_out(&e, &entry)
    })();
    if temporary {
        let closed = if valid & (1 << 3) != 0 {
            crate::runtime::engine::close_handle(engine, handle)
        } else {
            lock_engine(engine).release_handle(handle)
        };
        let reply = updated?;
        closed?;
        Ok(reply)
    } else {
        updated
    }
}
fn write_reply(
    writer: &std::sync::Mutex<File>,
    unique: u64,
    result: Result<Vec<u8>>,
) -> Result<()> {
    let (error, body) = match result {
        Ok(body) => (0, body),
        Err(error) => (-errno(&error), vec![]),
    };
    let mut reply = Vec::with_capacity(16 + body.len());
    push32(&mut reply, (16 + body.len()) as u32);
    push32(&mut reply, error as u32);
    push64(&mut reply, unique);
    reply.extend(body);
    if let Err(error) = lock(writer).write_all(&reply)
        && ![Some(libc::ENOENT), Some(libc::ENODEV)].contains(&error.raw_os_error())
    {
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packet_bounds_and_errno_are_explicit() {
        assert!(u64_at(&[0; 7], 0).is_err());
        assert_eq!(errno(&anyhow::anyhow!("BUSY_VIEW")), libc::EBUSY);
        assert_eq!(
            errno(&anyhow::anyhow!("UNSUPPORTED_POSIX_OPERATION")),
            libc::EOPNOTSUPP
        );
    }
    #[test]
    fn native_protocol_sizes_and_direct_io_contract() {
        let dir = tempfile::tempdir().unwrap();
        let e = crate::runtime::Engine::new(Store::open(dir.path(), None).unwrap());
        let mut fs = Filesystem::new();
        let root = e.store.lookup("").unwrap();
        assert_eq!(fs.attr(&e, &root).unwrap().len(), 88);
        assert_eq!(fs.entry_out(&e, &root).unwrap().len(), 128);
        assert_eq!(u32_at(&Filesystem::open_out(1, false), 8).unwrap(), 1);
    }
    #[test]
    fn fsyncs_and_flushes_are_told_apart_and_run_on_threads_of_their_own() {
        // `enum fuse_opcode` (linux/fuse.h): FUSE_FSYNC = 20, FUSE_FLUSH = 25
        // (WP5.2 fix round 2: they were swapped, and fsync(2) went
        // write-behind during a burst of closes).
        assert_eq!((FUSE_FSYNC, FUSE_FLUSH), (20, 25));
        assert_eq!((FUSE_RELEASE, FUSE_RELEASEDIR), (18, 29));
        for op in [
            8,
            9,
            35,
            14,
            10,
            11,
            12,
            45,
            FUSE_READ,
            FUSE_WRITE,
            FUSE_FSYNC,
            FUSE_FLUSH,
            FUSE_RELEASE,
        ] {
            assert!(runs_threaded(op, &[0; 64]), "opcode {op}");
        }
        // Native metadata also checkpoints outside Engine, so all valid
        // setattr packets use the threaded route.
        let mut setattr = vec![0u8; 88];
        assert!(runs_threaded(FUSE_SETATTR, &setattr));
        setattr[..4].copy_from_slice(&(1u32 << 3).to_ne_bytes());
        assert!(runs_threaded(FUSE_SETATTR, &setattr));
        assert!(runs_threaded(FUSE_SETATTR, &[]));
        // Lookups, directory releases and directory fsyncs stay on dispatch.
        for op in [1, 3, FUSE_RELEASEDIR, 30] {
            assert!(!runs_threaded(op, &[0; 64]), "opcode {op}");
        }
    }
    #[test]
    fn requests_past_the_thread_bound_wait_in_line_and_none_is_refused() {
        use std::sync::{Condvar, atomic::AtomicUsize};
        let threads = Threads::new(2);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (now, most, done) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        for i in 0..10 {
            let (gate, now, most, done) = (gate.clone(), now.clone(), most.clone(), done.clone());
            threads.run(Box::new(move || {
                let running = now.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(running, Ordering::SeqCst);
                let (open, opened) = &*gate;
                let mut open = open.lock().unwrap();
                while !*open {
                    open = opened.wait(open).unwrap();
                }
                drop(open);
                now.fetch_sub(1, Ordering::SeqCst);
                done.fetch_add(1, Ordering::SeqCst);
                // A panicking request ends, its thread goes on serving.
                assert!(i != 3, "request 3 panics");
            }));
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while now.load(Ordering::SeqCst) < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        {
            let state = lock(&threads.state);
            assert_eq!(state.running, 2, "two threads run requests");
            assert_eq!(state.waiting.len(), 8, "the other requests wait in line");
        }
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        while (done.load(Ordering::SeqCst) < 10 || lock(&threads.state).running > 0)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(done.load(Ordering::SeqCst), 10, "every request ran");
        assert!(
            most.load(Ordering::SeqCst) <= 2,
            "never more than two at once"
        );
        let state = lock(&threads.state);
        assert_eq!((state.running, state.waiting.len()), (0, 0));
    }
    fn current_file_fixture(
        bytes: &[u8],
    ) -> (
        tempfile::TempDir,
        Shared,
        Entry,
        Arc<crate::runtime::content_writes::NativeWrites>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(dir.path(), "fuse-current", &crate::core::id()).unwrap();
        let entry = store.create("f.txt", "file").unwrap();
        store
            .write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
            .unwrap();
        let entry = store.lookup("f.txt").unwrap();
        let engine = Arc::new(Mutex::new(crate::runtime::Engine::new(store)));
        let writes = crate::runtime::content_writes::NativeWrites::install(
            &engine,
            &crate::runtime::content_writes::default_context(&engine).unwrap(),
        )
        .unwrap();
        (dir, engine, entry, writes)
    }
    fn assert_current_bytes(engine: &Shared, expected: &[u8]) {
        use crate::content::ContentProvider;
        let e = lock_engine(engine);
        let reference = e.store.lookup("f.txt").unwrap().content_ref.unwrap();
        let local = crate::content::LocalContent::open(&e.store.root).unwrap();
        let pin = local.pin(&reference).unwrap();
        let mut actual = vec![0; reference.total_len as usize];
        local
            .read_range(
                &pin,
                0,
                &mut actual,
                &crate::content::ReadContext {
                    deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
                    cancel: crate::util::Cancel::new(),
                },
            )
            .unwrap();
        assert_eq!(actual, expected);
    }
    #[test]
    fn threaded_current_metadata_does_not_truncate_or_open_a_write_handle() {
        let bytes = b"keep metadata separate from bytes";
        let (_dir, engine, entry, writes) = current_file_fixture(bytes);
        let before = entry.content_ref;
        let fs = Mutex::new(Filesystem::new());
        let inode = lock(&fs).inode(&entry.id);
        let mut body = vec![0u8; 88];
        body[..4].copy_from_slice(&1u32.to_ne_bytes());
        for mode in [0o644u32, 0o444] {
            body[68..72].copy_from_slice(&mode.to_ne_bytes());
            threaded(&engine, &fs, FUSE_SETATTR, inode, &body).unwrap();
            let e = lock_engine(&engine);
            let after = e.store.lookup("f.txt").unwrap();
            assert_eq!(after.content_ref, before);
            assert_eq!(after.basic_info().posix_mode, Some(mode));
            assert!(e.handles.is_empty() && e.sessions.is_empty());
            drop(e);
            assert_current_bytes(&engine, bytes);
        }
        writes
            .shutdown(&crate::runtime::content_writes::default_context(&engine).unwrap())
            .unwrap();
    }
    #[test]
    fn a_size_change_with_an_ownership_change_is_refused_before_anything_applies() {
        let (_dir, engine, entry, writes) = current_file_fixture(b"keep me");
        let before = entry.clone();
        let fs = Mutex::new(Filesystem::new());
        let inode = lock(&fs).inode(&entry.id);
        let mut body = vec![0u8; 88];
        body[..4].copy_from_slice(&((1u32 << 3) | (1 << 1)).to_ne_bytes());
        let error = threaded(&engine, &fs, FUSE_SETATTR, inode, &body).unwrap_err();
        assert!(format!("{error:#}").contains("UNSUPPORTED_POSIX_OWNERSHIP"));
        let e = lock_engine(&engine);
        let after = e.store.lookup("f.txt").unwrap();
        assert_eq!(after.content_ref, before.content_ref);
        assert_eq!(after.basic_info(), before.basic_info());
        assert!(e.handles.is_empty() && e.sessions.is_empty());
        drop(e);
        assert_current_bytes(&engine, b"keep me");
        writes
            .shutdown(&crate::runtime::content_writes::default_context(&engine).unwrap())
            .unwrap();
    }

    #[test]
    fn virtual_root_and_discovery_keep_their_read_only_callback_semantics() {
        let (_dir, engine, _, writes) = current_file_fixture(b"ordinary file");
        let fs = Mutex::new(Filesystem::new());
        let mut body = vec![0u8; 88];
        body[..4].copy_from_slice(&1u32.to_ne_bytes());
        body[68..72].copy_from_slice(&0o755u32.to_ne_bytes());
        threaded(&engine, &fs, FUSE_SETATTR, 1, &body).unwrap();
        body[68..72].copy_from_slice(&0o700u32.to_ne_bytes());
        assert!(threaded(&engine, &fs, FUSE_SETATTR, 1, &body).is_err());
        assert!(threaded(&engine, &fs, FUSE_SETATTR, 2, &body).is_err());
        body[..4].copy_from_slice(&(1u32 << 7).to_ne_bytes());
        assert!(threaded(&engine, &fs, FUSE_SETATTR, 1, &body).is_err());
        lock_engine(&engine).discovery = b"discovery bytes".to_vec();
        let opened = threaded(&engine, &fs, 14, 2, &[0; 8]).unwrap();
        let handle = u64_at(&opened, 0).unwrap();
        let mut read = vec![0u8; 40];
        read[..8].copy_from_slice(&handle.to_ne_bytes());
        read[16..20].copy_from_slice(&64u32.to_ne_bytes());
        assert_eq!(
            threaded(&engine, &fs, FUSE_READ, 2, &read).unwrap(),
            b"discovery bytes"
        );
        threaded(&engine, &fs, FUSE_FSYNC, 2, &read).unwrap();
        threaded(&engine, &fs, FUSE_FLUSH, 2, &read).unwrap();
        threaded(&engine, &fs, FUSE_RELEASE, 2, &read).unwrap();
        let error = threaded(&engine, &fs, FUSE_FSYNC, 2, &read).unwrap_err();
        assert!(format!("{error:#}").contains("INVALID_HANDLE"));
        let mut truncate = [0u8; 8];
        truncate[..4].copy_from_slice(&(libc::O_RDWR | libc::O_TRUNC).to_ne_bytes());
        assert!(threaded(&engine, &fs, 14, 2, &truncate).is_err());
        let e = lock_engine(&engine);
        assert!(e.handles.is_empty() && e.sessions.is_empty());
        drop(e);
        assert_current_bytes(&engine, b"ordinary file");
        writes
            .shutdown(&crate::runtime::content_writes::default_context(&engine).unwrap())
            .unwrap();
    }

    #[test]
    fn mounted_mutation_never_falls_back_when_current_service_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::initialize(dir.path(), "missing-native", &crate::core::id()).unwrap();
        let engine = Arc::new(Mutex::new(crate::runtime::Engine::new(store)));
        let fs = Mutex::new(Filesystem::new());
        for op in [8, 9, 35, FUSE_WRITE, FUSE_SETATTR, FUSE_FSYNC, FUSE_FLUSH] {
            let error = threaded(&engine, &fs, op, 1, &[0; 88]).unwrap_err();
            assert!(format!("{error:#}").contains("CONTENT_STORE_NOT_INITIALIZED"));
        }
        let e = lock_engine(&engine);
        assert!(e.handles.is_empty() && e.sessions.is_empty());
        assert_eq!(
            e.store
                .db
                .query_row("SELECT count(*) FROM pending_saves", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn directory_inodes_and_handleless_setattr_keep_restored_incarnations_separate() {
        use crate::core::content_saves::{ContentBasicUpdate, NamespaceTarget};
        use crate::runtime::{content::ContentWait, content_writes};
        let dir = tempfile::tempdir().unwrap();
        let store = Store::initialize(dir.path(), "fuse-directory-incarnation", &crate::core::id())
            .unwrap();
        store
            .db
            .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
            .unwrap();
        let engine = Arc::new(Mutex::new(crate::runtime::Engine::new(store)));
        let context = crate::content::ReadContext {
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
            cancel: crate::util::Cancel::new(),
        };
        let wait = ContentWait::fixed(std::time::Duration::from_secs(30));
        let writes = content_writes::NativeWrites::install(&engine, &context).unwrap();
        let (old, entry) = content_writes::create_directory(
            &engine,
            "dir",
            content_writes::CreateOptions::default(),
            wait,
        )
        .unwrap()
        .unwrap();
        let fs = Mutex::new(Filesystem::new());
        let old_inode = {
            let e = lock_engine(&engine);
            let mut fs = lock(&fs);
            assert!(fs.retained_entry(&e, 1, None).unwrap().kind == "directory");
            assert_eq!(fs.retained_entry(&e, 2, None).unwrap().id, "control");
            fs.entry_inode(&e, &entry).unwrap()
        };
        content_writes::remove_directory(
            &engine,
            &entry.id,
            &NamespaceTarget {
                path: "dir".into(),
                parent: ROOT.into(),
            },
            wait,
        )
        .unwrap();
        {
            let e = lock_engine(&engine);
            let mut filesystem = lock(&fs);
            let dead = e.handle_entry(old).unwrap();
            assert_eq!(filesystem.entry_inode(&e, &dead).unwrap(), old_inode);
        }
        let fresh = {
            let mut e = lock_engine(&engine);
            let change = e
                .store
                .current_change(&entry.id, "alive", serde_json::json!(true))
                .unwrap();
            let event = e
                .store
                .event(&e.store.branch(&e.store.active).unwrap(), vec![change]);
            let id = event.id.clone();
            e.store.commit(event, true).unwrap();
            let delta = e.activation_delta(&[id], None).unwrap();
            e.after_activation(&delta);
            e.open("dir", None, false).unwrap().0
        };
        content_writes::update_basic(
            &engine,
            fresh,
            ContentBasicUpdate {
                write_time: Some(303),
                ..Default::default()
            },
            wait,
        )
        .unwrap();
        let fresh_inode = {
            let e = lock_engine(&engine);
            let mut fs = lock(&fs);
            let fresh = e.handle_entry(fresh).unwrap();
            let inode = fs.entry_inode(&e, &fresh).unwrap();
            assert_ne!(inode, old_inode);
            assert!(!fs.retained_entry(&e, old_inode, None).unwrap().alive);
            assert!(fs.entry(&e, old_inode).is_err());
            assert!(fs.entry(&e, inode).unwrap().alive);
            inode
        };
        let mut body = vec![0u8; 88];
        body[..4].copy_from_slice(&(1u32 << 5).to_ne_bytes());
        body[40..48].copy_from_slice(&1_600_000_000u64.to_ne_bytes());
        let error = threaded(&engine, &fs, FUSE_SETATTR, old_inode, &body).unwrap_err();
        assert!(format!("{error:#}").contains("STALE_CONTENT_DIRECTORY"));
        threaded(&engine, &fs, FUSE_SETATTR, fresh_inode, &body).unwrap();
        let e = lock_engine(&engine);
        assert_eq!(
            e.handle_entry(fresh).unwrap().basic_info().write_time,
            FILETIME_EPOCH + 16_000_000_000_000_000
        );
        assert!(!e.handle_entry(old).unwrap().alive);
        assert!(e.store.entry(&entry.id).unwrap().alive);
        drop(e);
        lock_engine(&engine).release_handle(old).unwrap();
        lock_engine(&engine).release_handle(fresh).unwrap();
        writes.shutdown(&context).unwrap();
    }
}
