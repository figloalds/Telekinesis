//! Linux FUSE 7.31 adapter with kernel caching and explicit invalidation.
use crate::{
    content::working::{Basis, lock},
    core::{Entry, EntryKind, ROOT},
    runtime::Engine,
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::net::UnixStream,
    },
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
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

const MAX_IO: usize = 1024 * 1024;
static MOUNT: Mutex<Option<(PathBuf, Arc<AtomicBool>)>> = Mutex::new(None);
#[derive(Default)]
struct Nodes {
    entities: BTreeMap<u64, String>,
    inodes: BTreeMap<String, u64>,
    versions: BTreeMap<(String, String, String), u64>,
    bindings: BTreeMap<u64, (Entry, Basis)>,
    lookups: BTreeMap<u64, u64>,
    opens: BTreeMap<u64, usize>,
    next: u64,
}
impl Nodes {
    fn inode(&mut self, entity: &str) -> u64 {
        if let Some(ino) = self.inodes.get(entity) {
            return *ino;
        }
        self.next += 1;
        let ino = self.next;
        self.entities.insert(ino, entity.into());
        self.inodes.insert(entity.into(), ino);
        ino
    }
    fn file_inode(&mut self, entry: Entry, basis: Basis) -> u64 {
        let key = (
            basis.branch.clone(),
            basis.entity.clone(),
            basis.anchor.clone(),
        );
        if let Some(ino) = self.versions.get(&key) {
            let ino = *ino;
            self.inodes.insert(entry.id.clone(), ino);
            return ino;
        }
        self.next += 1;
        let ino = self.next;
        self.entities.insert(ino, entry.id.clone());
        self.inodes.insert(entry.id.clone(), ino);
        self.versions.insert(key, ino);
        self.bindings.insert(ino, (entry, basis));
        ino
    }
    fn retire(&mut self, ino: u64) {
        if ino == 1
            || self.lookups.get(&ino).copied().unwrap_or(0) != 0
            || self.opens.get(&ino).copied().unwrap_or(0) != 0
        {
            return;
        }
        if let Some(entity) = self.entities.remove(&ino) {
            if self.inodes.get(&entity) == Some(&ino) {
                self.inodes.remove(&entity);
            }
        }
        if let Some((_, basis)) = self.bindings.remove(&ino) {
            self.versions
                .remove(&(basis.branch, basis.entity, basis.anchor));
        }
        self.lookups.remove(&ino);
        self.opens.remove(&ino);
    }
    fn forget(&mut self, ino: u64, count: u64) {
        if let Some(n) = self.lookups.get_mut(&ino) {
            *n = n.saturating_sub(count);
        }
        self.retire(ino);
    }
    fn close(&mut self, ino: u64) {
        if let Some(n) = self.opens.get_mut(&ino) {
            *n = n.saturating_sub(1);
        }
        self.retire(ino);
    }
}
struct Fs {
    engine: Arc<Engine>,
    nodes: Mutex<Nodes>,
    device: File,
}
fn p32(bytes: &mut Vec<u8>, n: u32) {
    bytes.extend(n.to_le_bytes());
}
fn p64(bytes: &mut Vec<u8>, n: u64) {
    bytes.extend(n.to_le_bytes());
}
fn n32(b: &[u8], at: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        b.get(at..at + 4).context("INVALID_REQUEST")?.try_into()?,
    ))
}
fn n64(b: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        b.get(at..at + 8).context("INVALID_REQUEST")?.try_into()?,
    ))
}
fn name(b: &[u8]) -> Result<&str> {
    Ok(std::str::from_utf8(
        b.split(|c| *c == 0).next().context("INVALID_NAME")?,
    )?)
}
fn errno(e: &anyhow::Error) -> i32 {
    let text = format!("{e:#}");
    for (code, value) in [
        ("NOT_FOUND", libc::ENOENT),
        ("ALREADY_EXISTS", libc::EEXIST),
        ("NOT_A_DIRECTORY", libc::ENOTDIR),
        ("IS_A_DIRECTORY", libc::EISDIR),
        ("DIRECTORY_NOT_EMPTY", libc::ENOTEMPTY),
        ("ACCESS_DENIED", libc::EACCES),
        ("INVALID_HANDLE", libc::EBADF),
        ("INVALID_NAME", libc::EINVAL),
        ("UNSUPPORTED", libc::ENOSYS),
        ("NO_DATA", libc::ENODATA),
    ] {
        if text.contains(code) {
            return value;
        }
    }
    e.downcast_ref::<std::io::Error>()
        .and_then(|e| e.raw_os_error())
        .unwrap_or(libc::EIO)
}
impl Fs {
    fn send(&self, id: u64, error: i32, body: &[u8]) -> Result<()> {
        let mut bytes = Vec::with_capacity(16 + body.len());
        p32(&mut bytes, (16 + body.len()) as u32);
        p32(&mut bytes, error as u32);
        p64(&mut bytes, id);
        bytes.extend(body);
        // /dev/fuse consumes one complete reply per write. Invalidation can
        // wait for outstanding inode operations, whose replies must remain
        // able to write concurrently with the notification.
        match (&self.device).write(&bytes) {
            Ok(n) => ensure!(n == bytes.len(), "SHORT_FUSE_REPLY"),
            Err(error) if id == 0 && error.raw_os_error() == Some(libc::ENOENT) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
    fn entity(&self, inode: u64) -> Result<String> {
        lock(&self.nodes)
            .entities
            .get(&inode)
            .cloned()
            .context("NOT_FOUND")
    }
    fn path(&self, inode: u64) -> Result<String> {
        lock(&self.engine.store)
            .projection()
            .path(&self.entity(inode)?)
    }
    fn child(&self, inode: u64, name: &str) -> Result<String> {
        Ok(format!("{}/{}", self.path(inode)?, name))
    }
    fn node(&self, entry: &Entry, lookup: bool) -> Result<(Entry, u64)> {
        let (mut current, basis) = {
            let store = lock(&self.engine.store);
            let current = store.entry(&entry.id)?;
            let basis = (current.kind == EntryKind::File)
                .then(|| store.basis(&current.id))
                .transpose()?;
            (current, basis)
        };
        if current.content == entry.content {
            current.working_len = entry.working_len;
        }
        let mut nodes = lock(&self.nodes);
        let ino = if let Some(basis) = basis {
            nodes.file_inode(current.clone(), basis)
        } else {
            nodes.inode(&current.id)
        };
        if lookup {
            *nodes.lookups.entry(ino).or_default() += 1;
        }
        Ok((current, ino))
    }
    fn attr(&self, e: &Entry, len: u64, ino: u64) -> Vec<u8> {
        let len = e.working_len.unwrap_or(len);
        let mut b = Vec::new();
        p64(&mut b, ino);
        p64(&mut b, len);
        p64(&mut b, len.div_ceil(512));
        let times = [e.times.accessed, e.times.modified, e.times.changed]
            .map(|t| t.saturating_sub(116_444_736_000_000_000));
        for t in times {
            p64(&mut b, t / 10_000_000);
        }
        for t in times {
            p32(&mut b, ((t % 10_000_000) * 100) as u32);
        }
        p32(
            &mut b,
            e.mode
                | if e.kind == EntryKind::Directory {
                    libc::S_IFDIR
                } else {
                    libc::S_IFREG
                },
        );
        p32(
            &mut b,
            if e.deleted {
                0
            } else if e.kind == EntryKind::Directory {
                2
            } else {
                1
            },
        );
        p32(&mut b, unsafe { libc::geteuid() });
        p32(&mut b, unsafe { libc::getegid() });
        p32(&mut b, 0);
        p32(&mut b, 4096);
        p32(&mut b, 0);
        b
    }
    fn entry_out(&self, entry: &Entry) -> Result<Vec<u8>> {
        let (e, ino) = self.node(entry, true)?;
        let mut b = Vec::new();
        p64(&mut b, ino);
        p64(&mut b, 1);
        p64(&mut b, 1);
        p64(&mut b, 1);
        p32(&mut b, 0);
        p32(&mut b, 0);
        b.extend(self.attr(
            &e,
            e.content.as_ref().map(|c| c.total_len).unwrap_or(0),
            ino,
        ));
        Ok(b)
    }
    fn attr_out(&self, e: &Entry, len: u64, ino: u64) -> Vec<u8> {
        let mut b = Vec::new();
        p64(&mut b, 1);
        p32(&mut b, 0);
        p32(&mut b, 0);
        b.extend(self.attr(e, len, ino));
        b
    }
    fn open_out(&self, h: u64) -> Vec<u8> {
        let mut b = Vec::new();
        p64(&mut b, h);
        p32(&mut b, 2);
        p32(&mut b, 0);
        b
    }
    fn notify(&self, path: &str) -> Result<()> {
        let (parent, name) = path.rsplit_once('/').unwrap_or(("/", path));
        let (entity, parent_id) = {
            let store = lock(&self.engine.store);
            (
                store
                    .lookup(path)
                    .ok()
                    .filter(|e| e.kind == EntryKind::Directory)
                    .map(|e| e.id),
                store.lookup(parent).ok().map(|e| e.id),
            )
        };
        let nodes = lock(&self.nodes);
        let inode = entity.and_then(|e| nodes.inodes.get(&e).copied());
        let parent = parent_id.and_then(|e| nodes.inodes.get(&e).copied());
        drop(nodes);
        if let Some(ino) = inode {
            let mut b = Vec::new();
            p64(&mut b, ino);
            p64(&mut b, 0);
            p64(&mut b, 0);
            self.send(0, 2, &b)?;
        }
        if let Some(parent) = parent.filter(|_| !name.is_empty()) {
            let mut b = Vec::new();
            p64(&mut b, parent);
            p32(&mut b, name.len() as u32);
            p32(&mut b, 0);
            b.extend(name.as_bytes());
            b.push(0);
            self.send(0, 3, &b)?;
        }
        Ok(())
    }
    fn dispatch(&self, op: u32, inode: u64, p: &[u8]) -> Result<Vec<u8>> {
        match op {
            26 => {
                ensure!(
                    n32(p, 0)? == 7 && n32(p, 4)? >= 31,
                    "UNSUPPORTED_FUSE_PROTOCOL"
                );
                let mut b = Vec::new();
                for n in [
                    7,
                    31,
                    MAX_IO as u32,
                    (1 << 0) | (1 << 5) | (1 << 16) | (1 << 25),
                ] {
                    p32(&mut b, n);
                }
                b.extend(64u16.to_le_bytes());
                b.extend(48u16.to_le_bytes());
                p32(&mut b, MAX_IO as u32);
                p32(&mut b, 1);
                b.resize(64, 0);
                return Ok(b);
            }
            15 => {
                let size = n32(p, 16)? as usize;
                ensure!(size <= MAX_IO, "INVALID_REQUEST");
                let mut b = vec![0; size];
                let n = self.engine.read(n64(p, 0)?, n64(p, 8)?, &mut b)?;
                b.truncate(n);
                return Ok(b);
            }
            16 => {
                let size = n32(p, 16)? as usize;
                ensure!(size <= MAX_IO, "INVALID_REQUEST");
                let bytes = p.get(40..40 + size).context("INVALID_REQUEST")?;
                let count = self
                    .engine
                    .write(n64(p, 0)?, n64(p, 8)?, bytes, false, false)?;
                let mut b = Vec::new();
                p32(&mut b, count as u32);
                p32(&mut b, 0);
                return Ok(b);
            }
            18 | 29 => {
                self.engine.close(n64(p, 0)?)?;
                lock(&self.nodes).close(inode);
                return Ok(Vec::new());
            }
            20 | 30 => {
                self.engine.flush(Some(n64(p, 0)?))?;
                return Ok(Vec::new());
            }
            25 => return Ok(Vec::new()), // release schedules the seal; fsync is explicit durability.
            34 | 38 => return Ok(Vec::new()),
            17 => {
                let mut b = Vec::new();
                for n in [262144, 131072, 131072, 1_000_000, 999_000] {
                    p64(&mut b, n);
                }
                for n in [4096, 255, 4096, 0] {
                    p32(&mut b, n);
                }
                b.resize(80, 0);
                return Ok(b);
            }
            23 => return Ok(if n32(p, 0)? == 0 { vec![0; 8] } else { vec![] }),
            22 => anyhow::bail!("NO_DATA"),
            _ => {}
        }
        match op {
            1 => {
                let e = self.engine.lookup(&self.child(inode, name(p)?)?)?;
                self.entry_out(&e)
            }
            3 => {
                if p.len() >= 16 && n32(p, 0)? & 1 != 0 {
                    let (e, len) = self.engine.info(n64(p, 8)?)?;
                    return Ok(self.attr_out(&e, len, inode));
                }
                let binding = { lock(&self.nodes).bindings.get(&inode).cloned() };
                if let Some((mut entry, basis)) = binding {
                    {
                        let store = lock(&self.engine.store);
                        if store.projection().anchors.get(&entry.id) == Some(&basis.anchor) {
                            entry = store.entry(&entry.id)?;
                        }
                    }
                    let len = self
                        .engine
                        .working
                        .bound_len(&basis)
                        .unwrap_or(basis.content.total_len);
                    return Ok(self.attr_out(&entry, len, inode));
                }
                let e = self.engine.lookup(&self.path(inode)?)?;
                let len = e.content.as_ref().map(|c| c.total_len).unwrap_or(0);
                Ok(self.attr_out(&e, len, inode))
            }
            14 | 27 => {
                let flags = n32(p, 0)?;
                let writable = flags & libc::O_ACCMODE as u32 != 0;
                let binding = {
                    let mut nodes = lock(&self.nodes);
                    *nodes.opens.entry(inode).or_default() += 1;
                    nodes.bindings.get(&inode).cloned()
                };
                let opened = if let Some((entry, basis)) = binding {
                    self.engine.open_bound(entry, basis, writable)
                } else {
                    self.engine
                        .open_file(&self.path(inode)?, false, op == 27, writable)
                };
                let h = match opened {
                    Ok(h) => h,
                    Err(e) => {
                        lock(&self.nodes).close(inode);
                        return Err(e);
                    }
                };
                if flags & libc::O_TRUNC as u32 != 0 {
                    self.engine.truncate(h, 0, false)?;
                }
                Ok(self.open_out(h))
            }
            35 => {
                let path = self.child(inode, name(p.get(16..).context("INVALID_REQUEST")?)?)?;
                let h = self.engine.open_file(&path, true, false, true)?;
                let (e, _) = self.engine.info(h)?;
                let mut b = self.entry_out(&e)?;
                *lock(&self.nodes).opens.entry(n64(&b, 0)?).or_default() += 1;
                b.extend(self.open_out(h));
                Ok(b)
            }
            9 => {
                let path = self.child(inode, name(p.get(8..).context("INVALID_REQUEST")?)?)?;
                let e = lock(&self.engine.store).create(&path, EntryKind::Directory)?;
                self.entry_out(&e)
            }
            10 | 11 => {
                let path = self.child(inode, name(p)?)?;
                let mut store = lock(&self.engine.store);
                let e = store.lookup(&path)?;
                store.delete(&e.id)?;
                Ok(Vec::new())
            }
            12 | 45 => {
                let start = if op == 45 { 16 } else { 8 };
                if op == 45 {
                    ensure!(n32(p, 8)? & !1 == 0, "UNSUPPORTED_RENAME");
                }
                let names = p.get(start..).context("INVALID_REQUEST")?;
                let old = name(names)?;
                let new = name(names.get(old.len() + 1..).context("INVALID_REQUEST")?)?;
                let path = self.child(inode, old)?;
                let target = self.child(n64(p, 0)?, new)?;
                let h = self.engine.open_file(&path, false, false, false)?;
                let result = self
                    .engine
                    .rename(h, &target, op != 45 || n32(p, 8)? & 1 == 0);
                self.engine.close(h)?;
                result?;
                Ok(Vec::new())
            }
            28 => {
                let h = n64(p, 0)?;
                let offset = n64(p, 8)? as usize;
                let limit = n32(p, 16)? as usize;
                let entries = self.engine.children(h, offset == 0)?;
                let mut b = Vec::new();
                for (index, e) in entries.iter().enumerate().skip(offset) {
                    let name = e.name.as_bytes();
                    let len = (24 + name.len()).next_multiple_of(8);
                    if b.len() + len > limit {
                        break;
                    }
                    let at = b.len();
                    p64(&mut b, self.node(e, false)?.1);
                    p64(&mut b, index as u64 + 1);
                    p32(&mut b, name.len() as u32);
                    p32(&mut b, if e.kind == EntryKind::Directory { 4 } else { 8 });
                    b.extend(name);
                    b.resize(at + len, 0);
                }
                Ok(b)
            }
            4 => {
                let valid = n32(p, 0)?;
                ensure!(valid & ((1 << 1) | (1 << 2)) == 0, "UNSUPPORTED_OWNERSHIP");
                let temporary = valid & (1 << 6) == 0;
                let h = if temporary {
                    self.engine
                        .open_file(&self.path(inode)?, false, false, true)?
                } else {
                    n64(p, 8)?
                };
                if valid & (1 << 3) != 0 {
                    self.engine.truncate(h, n64(p, 16)?, false)?;
                }
                let (e, _) = self.engine.info(h)?;
                let mut times = e.times;
                let stamp = |sec: u64, ns: u32| {
                    sec * 10_000_000 + ns as u64 / 100 + 116_444_736_000_000_000
                };
                if valid & (1 << 4) != 0 {
                    times.accessed = stamp(n64(p, 32)?, n32(p, 56)?);
                }
                if valid & (1 << 5) != 0 {
                    times.modified = stamp(n64(p, 40)?, n32(p, 60)?);
                }
                if valid & (1 << 7) != 0 {
                    times.accessed = crate::core::Times::default().accessed;
                }
                if valid & (1 << 8) != 0 {
                    times.modified = crate::core::Times::default().modified;
                }
                let mode = if valid & 1 != 0 {
                    Some(n32(p, 68)? & 0o7777)
                } else {
                    None
                };
                {
                    let mut store = lock(&self.engine.store);
                    store.set_times(&e.id, times, mode, None)?;
                    *lock(&self.engine.handle(h)?.entry) = store.entry(&e.id)?;
                }
                let (e, len) = self.engine.info(h)?;
                if temporary {
                    self.engine.close(h)?;
                }
                Ok(self.attr_out(&e, len, inode))
            }
            _ => anyhow::bail!("UNSUPPORTED_FUSE_OPCODE: {op}"),
        }
    }
}
pub fn attach(engine: Arc<Engine>, path: &Path) -> Result<()> {
    let mut mount = lock(&MOUNT);
    ensure!(mount.is_none(), "MOUNT_ALREADY_ATTACHED");
    std::fs::create_dir_all(path)?;
    let mut device = helper_mount(path, "tkfs")?;
    let stop = Arc::new(AtomicBool::new(false));
    let fs = Arc::new(Fs {
        engine: engine.clone(),
        nodes: Mutex::new(Nodes::default()),
        device: device.try_clone()?,
    });
    lock(&fs.nodes).inode(ROOT);
    let notify = Arc::downgrade(&fs);
    engine.set_invalidator(Arc::new(move |path, _| {
        if let Some(fs) = notify.upgrade() {
            fs.notify(path)
        } else {
            Ok(())
        }
    }));
    let stopping = stop.clone();
    let inflight = Arc::new(AtomicUsize::new(0));
    std::thread::spawn(move || {
        let mut bytes = vec![0; MAX_IO + 4096];
        while !stopping.load(Ordering::Acquire) {
            let n = match device.read(&mut bytes) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            };
            if n < 40 {
                continue;
            }
            let op = match n32(&bytes, 4) {
                Ok(op) => op,
                Err(_) => continue,
            };
            if matches!(op, 2 | 42) {
                if op == 2 {
                    if let (Ok(ino), Ok(count)) = (n64(&bytes[..n], 16), n64(&bytes[..n], 40)) {
                        lock(&fs.nodes).forget(ino, count);
                    }
                } else if let Ok(count) = n32(&bytes[..n], 40) {
                    for item in bytes[48..n].chunks_exact(16).take(count as usize) {
                        if let (Ok(ino), Ok(count)) = (n64(item, 0), n64(item, 8)) {
                            lock(&fs.nodes).forget(ino, count);
                        }
                    }
                }
                continue;
            }
            while inflight.load(Ordering::Acquire) >= 64 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            inflight.fetch_add(1, Ordering::AcqRel);
            let request = bytes[..n].to_vec();
            let fs = fs.clone();
            let inflight = inflight.clone();
            std::thread::spawn(move || {
                let id = n64(&request, 8).unwrap_or(0);
                let inode = n64(&request, 16).unwrap_or(0);
                let result = fs.dispatch(op, inode, &request[40..]);
                let _ = match result {
                    Ok(body) => fs.send(id, 0, &body),
                    Err(error) => fs.send(id, -errno(&error), &[]),
                };
                inflight.fetch_sub(1, Ordering::AcqRel);
            });
        }
    });
    *mount = Some((path.to_owned(), stop));
    Ok(())
}
pub fn detach() -> Result<()> {
    if let Some((path, stop)) = lock(&MOUNT).take() {
        unmount(&path, true)?;
        stop.store(true, Ordering::Release);
    }
    Ok(())
}
