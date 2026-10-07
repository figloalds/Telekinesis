//! Linux FUSE protocol 7.31 adapter. No libfuse build dependency: fusermount3
//! grants an owner-only mount and passes the device using SCM_RIGHTS.
//! No writeback caching/mmap: direct I/O and zero lookup/attribute TTLs retain
//! Engine's save boundaries and pinned writer ancestry. Unsupported POSIX kinds
//! (symlinks, hardlinks, devices), ownership and privileged modes fail explicitly.
use crate::{core::*, runtime::Shared};
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
    thread::JoinHandle,
};

static MOUNT: Mutex<Option<Mount>> = Mutex::new(None);
const MAX_IO: usize = 1024 * 1024;
struct Mount {
    path: PathBuf,
    source: String,
    worker: Option<JoinHandle<()>>,
}

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
        let _ = Command::new("fusermount3")
            .args(["-u", "--"])
            .arg(path)
            .status();
    }
    result
}
fn launch(engine: Shared, path: &Path, source: &str) -> Result<JoinHandle<()>> {
    let device = helper_mount(path, source)?;
    Ok(std::thread::spawn(move || {
        if let Err(error) = serve(device, engine.clone()) {
            let mut e = engine.lock().unwrap();
            e.health = Some(format!("FUSE_DISPATCH_FAILED: {error:#}"));
            e.mounted = false;
            e.publish_observation();
            eprintln!("FUSE_DISPATCH_FAILED: {error:#}");
        }
    }))
}
pub fn start(engine: Shared, path: &Path) -> Result<()> {
    start_source(engine, path, "tkfs")
}
pub fn start_managed(engine: Shared, path: &Path, instance: &str) -> Result<()> {
    uuid::Uuid::parse_str(instance)?;
    start_source(engine, path, &format!("tkfs-{instance}"))
}
fn start_source(engine: Shared, path: &Path, source: &str) -> Result<()> {
    let mut guard = MOUNT.lock().unwrap();
    ensure!(guard.is_none(), "ONE_MOUNT_PER_RUNTIME");
    ensure!(
        !path.exists(),
        "MOUNT_PATH_MUST_NOT_EXIST: {}",
        path.display()
    );
    std::fs::create_dir(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    let worker = match launch(engine.clone(), path, source) {
        Ok(w) => w,
        Err(e) => {
            let _ = std::fs::remove_dir(path);
            return Err(e);
        }
    };
    *guard = Some(Mount {
        path: path.into(),
        source: source.into(),
        worker: Some(worker),
    });
    let mut e = engine.lock().unwrap();
    e.mounted = true;
    e.publish_observation();
    Ok(())
}
fn unmount(mount: &mut Mount) -> Result<()> {
    let output = Command::new("fusermount3")
        .args(["-u", "--"])
        .arg(&mount.path)
        .output()?;
    ensure!(
        output.status.success(),
        "BUSY_VIEW: FUSE unmount refused: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    if let Some(worker) = mount.worker.take() {
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("FUSE_WORKER_PANIC"))?;
    }
    Ok(())
}
pub fn stop(engine: &Shared) -> Result<()> {
    let mut guard = MOUNT.lock().unwrap();
    {
        let mut e = engine.lock().unwrap();
        e.quiet()?;
        e.switching = true;
    }
    if let Some(mount) = guard.as_mut() {
        if let Err(error) = unmount(mount) {
            engine.lock().unwrap().switching = false;
            return Err(error);
        }
        std::fs::remove_dir(&mount.path)?;
    }
    *guard = None;
    let mut e = engine.lock().unwrap();
    e.mounted = false;
    e.publish_observation();
    Ok(())
}
/// Called only after verifying the persistent worker record and acquiring a free
/// state ownership lock. Never detach a responding or unrelated filesystem.
pub fn recover_managed(path: &Path, instance: &str) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    uuid::Uuid::parse_str(instance)?;
    let source = format!("tkfs-{instance}");
    let decode = |value: &str| {
        value
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\")
    };
    for line in std::fs::read_to_string("/proc/self/mountinfo")?.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 10 || Path::new(&decode(fields[4])) != path {
            continue;
        }
        let separator = fields
            .iter()
            .position(|field| *field == "-")
            .context("INVALID_MOUNTINFO")?;
        ensure!(
            fields.get(separator + 1) == Some(&"fuse.tkfs")
                && fields.get(separator + 2) == Some(&source.as_str()),
            "WORKER_START: unrelated mount occupies reservation"
        );
        ensure!(
            std::fs::metadata(path)
                .is_err_and(|error| error.raw_os_error() == Some(libc::ENOTCONN)),
            "WORKER_START: mount still responds"
        );
        let result = Command::new("fusermount3")
            .args(["-u", "--"])
            .arg(path)
            .output()?;
        ensure!(
            result.status.success(),
            "WORKER_START: disconnected owned FUSE unmount refused"
        );
    }
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        ensure!(
            metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.mode() & 0o777 == 0o700
                && std::fs::read_dir(path)?.next().is_none(),
            "WORKER_START: mount directory occupied"
        );
        std::fs::remove_dir(path)?;
    }
    Ok(())
}
pub fn checkout(engine: &Shared, request: &str, payload: &Value) -> Result<Value> {
    let mut guard = MOUNT.lock().unwrap();
    let mount = guard.as_mut().context("MOUNT_NOT_RUNNING")?;
    {
        let mut e = engine.lock().unwrap();
        if let Some(result) = e.store.receipt(request, payload)? {
            return Ok(result);
        }
        if let Some(g) = payload["generation"].as_u64() {
            ensure!(g == e.store.generation, "STALE_VIEW");
        }
        e.quiet()?;
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
    }
    if let Err(error) = unmount(mount) {
        engine.lock().unwrap().switching = false;
        return Err(error);
    }
    let result = {
        let mut e = engine.lock().unwrap();
        e.mounted = false;
        e.control(request, payload)
    };
    let worker = launch(engine.clone(), &mount.path, &mount.source);
    let mut e = engine.lock().unwrap();
    e.switching = false;
    match worker {
        Ok(worker) => {
            mount.worker = Some(worker);
            e.mounted = true;
            e.publish_observation();
            result
        }
        Err(error) => {
            e.health = Some(format!("REMOUNT_FAILED: {error:#}; restart runtime"));
            e.publish_observation();
            Err(error.context("REMOUNT_FAILED"))
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
    if text.contains("FILE_NOT_FOUND") {
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
    next_inode: u64,
}
impl Filesystem {
    fn new() -> Self {
        Self {
            by_inode: BTreeMap::from([(1, ROOT.into()), (2, "control".into())]),
            by_entity: BTreeMap::from([(ROOT.into(), 1), ("control".into(), 2)]),
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
        Ok(entry)
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
        let inode = self.inode(&entry.id);
        let basic = entry.basic_info();
        let size = if entry.id == "control" {
            e.discovery.len() as u64
        } else if let Some(session) = e.sessions.get(&entry.id) {
            session.bytes.len() as u64
        } else {
            entry
                .content
                .as_ref()
                .map(|v| e.store.object_size(v))
                .transpose()?
                .unwrap_or(0)
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
        push32(&mut out, if entry.kind == "directory" { 2 } else { 1 });
        push32(&mut out, unsafe { libc::geteuid() });
        push32(&mut out, unsafe { libc::getegid() });
        push32(&mut out, 0);
        push32(&mut out, 4096);
        push32(&mut out, 0);
        Ok(out)
    }
    fn entry_out(&mut self, e: &crate::runtime::Engine, entry: &Entry) -> Result<Vec<u8>> {
        let inode = self.inode(&entry.id);
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
    fn set_mode(e: &mut crate::runtime::Engine, entry: &Entry, mode: u32) -> Result<()> {
        ensure!(mode & 0o7000 == 0, "UNSUPPORTED_POSIX_MODE");
        if entry.id == ROOT {
            ensure!(
                mode & 0o777 == 0o755,
                "UNSUPPORTED_FS_OPERATION: root metadata"
            );
            return Ok(());
        }
        ensure!(entry.id != "control", "PERMISSION_DENIED");
        let mut basic = e.store.entry(&entry.id)?.basic_info();
        basic.posix_mode = Some(mode & 0o777);
        basic.attributes = BasicInfo::attributes(
            &entry.kind,
            (basic.attributes & !1) | u32::from(mode & 0o222 == 0),
        )?;
        basic.change_time = filetime(time());
        e.store.set_basic(&entry.id, basic)?;
        if let Some(session) = e.sessions.get_mut(&entry.id) {
            session.entry = e.store.entry(&entry.id)?;
        }
        Ok(())
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
        let entry = self.entry(e, inode)?;
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
            4 => {
                let valid = u32_at(p, 0)?;
                ensure!(
                    valid
                        & !((1 << 0)
                            | (1 << 3)
                            | (1 << 4)
                            | (1 << 5)
                            | (1 << 6)
                            | (1 << 7)
                            | (1 << 8)
                            | (1 << 9)
                            | (1 << 10)
                            | (1 << 11))
                        == 0,
                    "UNSUPPORTED_POSIX_OWNERSHIP"
                );
                if valid & 1 != 0 {
                    Self::set_mode(e, &entry, u32_at(p, 68)?)?;
                }
                let mut temporary = None;
                let h = if valid & (1 << 6) != 0 {
                    u64_at(p, 8)?
                } else if valid & (1 << 3) != 0 {
                    let (h, _) = e.open(&self.path(e, &entry)?, None, true)?;
                    temporary = Some(h);
                    h
                } else {
                    0
                };
                let result = (|| {
                    if valid & (1 << 3) != 0 {
                        e.truncate(h, u64_at(p, 16)?, false)?;
                        e.flush(h)?;
                    }
                    if valid
                        & ((1 << 4)
                            | (1 << 5)
                            | (1 << 7)
                            | (1 << 8)
                            | (1 << 9)
                            | (1 << 10)
                            | (1 << 11))
                        != 0
                    {
                        ensure!(
                            entry.id != ROOT && entry.id != "control",
                            "UNSUPPORTED_FS_OPERATION: root/control timestamps"
                        );
                        let mut basic = e.store.entry(&entry.id)?.basic_info();
                        for (bit, nowbit, offset, ns_offset, slot) in [
                            (1 << 4, 1 << 7, 32, 56, 0),
                            (1 << 5, 1 << 8, 40, 60, 1),
                            (1 << 10, 0, 48, 64, 2),
                        ] {
                            if valid & (bit | nowbit) != 0 {
                                let value = if valid & nowbit != 0 {
                                    filetime(time())
                                } else {
                                    let ns = u32_at(p, ns_offset)?;
                                    ensure!(ns < 1_000_000_000, "INVALID_TIMESTAMP");
                                    u64_at(p, offset)?
                                        .checked_mul(10_000_000)
                                        .and_then(|v| v.checked_add((ns / 100) as u64))
                                        .and_then(|v| v.checked_add(FILETIME_EPOCH))
                                        .context("INVALID_TIMESTAMP")?
                                };
                                match slot {
                                    0 => basic.access_time = value,
                                    1 => basic.write_time = value,
                                    _ => basic.change_time = value,
                                }
                            }
                        }
                        e.store.set_basic(&entry.id, basic)?;
                        if let Some(session) = e.sessions.get_mut(&entry.id) {
                            session.entry = e.store.entry(&entry.id)?;
                            if valid & ((1 << 5) | (1 << 8)) != 0 {
                                session.preserve_times |= 2;
                            }
                            if valid & (1 << 10) != 0 {
                                session.preserve_times |= 4;
                            }
                        }
                    }
                    Ok(())
                })();
                if let Some(h) = temporary {
                    let close = e.close(h);
                    result?;
                    close?;
                } else {
                    result?;
                }
                self.attr_out(e, &self.entry(e, inode)?)
            }
            8 | 9 | 35 => {
                let (mode, skip, flags) = match op {
                    8 => (u32_at(p, 0)?, 16, libc::O_RDWR as u32),
                    9 => (u32_at(p, 0)?, 8, libc::O_RDONLY as u32),
                    _ => (u32_at(p, 4)?, 16, u32_at(p, 0)?),
                };
                ensure!(mode & 0o7000 == 0, "UNSUPPORTED_POSIX_MODE");
                if op == 8 {
                    ensure!(
                        mode & libc::S_IFMT == libc::S_IFREG,
                        "UNSUPPORTED_POSIX_KIND"
                    );
                }
                let path =
                    self.child(e, inode, name(p.get(skip..).context("INVALID_PARAMETER")?)?)?;
                let directory = op == 9;
                let (h, created) = e.open(
                    &path,
                    Some(if directory { "directory" } else { "file" }),
                    !directory && flags & libc::O_ACCMODE as u32 != libc::O_RDONLY as u32,
                )?;
                let result = (|| {
                    Self::set_mode(e, &created, mode)?;
                    let mut out = self.entry_out(e, &e.store.entry(&created.id)?)?;
                    if op == 35 {
                        out.extend(Self::open_out(h, false));
                    }
                    Ok(out)
                })();
                if op != 35 || result.is_err() {
                    e.close(h)?;
                }
                result
            }
            10 | 11 => {
                let path = self.child(e, inode, name(p)?)?;
                let target = e.store.lookup(&path)?;
                ensure!(
                    (op == 11) == (target.kind == "directory"),
                    if op == 11 {
                        "NOT_DIRECTORY"
                    } else {
                        "IS_DIRECTORY"
                    }
                );
                ensure!(
                    !e.handles.values().any(|h| h.entity == target.id),
                    "BUSY_VIEW: unlink open entry"
                );
                e.store.delete(&target.id)?;
                Ok(vec![])
            }
            12 | 45 => {
                let newdir = u64_at(p, 0)?;
                let flags = if op == 45 { u32_at(p, 8)? } else { 0 };
                ensure!(flags & !1 == 0, "UNSUPPORTED_RENAME_FLAGS");
                let names = p
                    .get(if op == 45 { 16 } else { 8 }..)
                    .context("INVALID_PARAMETER")?;
                let old = name(names)?;
                let new = name(names.get(old.len() + 1..).context("INVALID_PARAMETER")?)?;
                let source = e.store.lookup(&self.child(e, inode, old)?)?;
                let target = self.child(e, newdir, new)?;
                if let Ok(replaced) = e.store.lookup(&target) {
                    ensure!(
                        !e.handles
                            .values()
                            .any(|h| h.entity == replaced.id && replaced.id != source.id),
                        "BUSY_VIEW: replace open entry"
                    );
                }
                e.store.rename(&source.id, &target, flags & 1 == 0)?;
                Ok(vec![])
            }
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
                if flags & libc::O_TRUNC as u32 != 0 && !directory {
                    if let Err(error) = e.truncate(h, 0, false).and_then(|()| e.flush(h)) {
                        let _ = e.close(h);
                        return Err(error);
                    }
                }
                Ok(Self::open_out(h, directory))
            }
            15 => {
                let session = e.session(u64_at(p, 0)?)?;
                let size = (u32_at(p, 16)? as usize).min(MAX_IO);
                let mut out = vec![0; size];
                let count = session
                    .bytes
                    .read_at(usize::try_from(u64_at(p, 8)?)?, &mut out)?;
                out.truncate(count);
                Ok(out)
            }
            16 => {
                let size = u32_at(p, 16)? as usize;
                ensure!(size <= MAX_IO, "INVALID_PARAMETER");
                let bytes = p.get(40..40 + size).context("INVALID_PARAMETER")?;
                let count = e.write(
                    u64_at(p, 0)?,
                    u64_at(p, 8)?,
                    bytes,
                    u32_at(p, 32)? & libc::O_APPEND as u32 != 0,
                    false,
                )?;
                let mut out = vec![];
                push32(&mut out, count as u32);
                push32(&mut out, 0);
                Ok(out)
            }
            18 | 29 => {
                e.close(u64_at(p, 0)?)?;
                Ok(vec![])
            }
            20 | 25 | 30 => {
                e.flush(u64_at(p, 0)?)?;
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
                    push64(&mut out, self.inode(&child.id));
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
fn serve(mut device: File, engine: Shared) -> Result<()> {
    let mut fs = Filesystem::new();
    let mut bytes = vec![0; MAX_IO + 4096];
    loop {
        let count = match device.read(&mut bytes) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.raw_os_error() == Some(libc::ENODEV) => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
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
        let unique = u64_at(p, 8)?;
        let inode = u64_at(p, 16)?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut e = engine
                .lock()
                .map_err(|_| anyhow::anyhow!("ENGINE_POISONED"))?;
            let result = fs.dispatch(&mut e, op, inode, &p[40..]);
            e.publish_observation();
            result
        }))
        .unwrap_or_else(|_| Err(anyhow::anyhow!("FUSE_CALLBACK_PANIC")));
        let (error, body) = match result {
            Ok(body) => (0, body),
            Err(error) => {
                let code = errno(&error);
                if code == libc::EIO {
                    eprintln!("FUSE op={op}: {error:#}");
                }
                (-code, vec![])
            }
        };
        let mut reply = Vec::with_capacity(16 + body.len());
        push32(&mut reply, (16 + body.len()) as u32);
        push32(&mut reply, error as u32);
        push64(&mut reply, unique);
        reply.extend(body);
        if let Err(error) = device.write_all(&reply) {
            if error.raw_os_error() == Some(libc::ENOENT) {
                continue;
            }
            if error.raw_os_error() == Some(libc::ENODEV) {
                break;
            }
            return Err(error.into());
        }
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
}
