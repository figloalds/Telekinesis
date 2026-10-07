//! Same-owner Unix management IPC. Filesystem protection and SO_PEERCRED are
//! checked on both ends; no TCP endpoint or claimed UID establishes authority.
use anyhow::{Context, Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{
            ffi::OsStrExt,
            fs::{FileTypeExt, MetadataExt, PermissionsExt},
            net::{UnixListener, UnixStream},
        },
    },
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const MAX_FRAME: usize = 64 * 1024 * 1024;
/// Short stable paths avoid sockaddr_un truncation for deeply nested data roots.
pub fn endpoint(namespace: &str) -> String {
    format!(
        "/tmp/tkfs-ipc-{}/{}.sock",
        unsafe { libc::geteuid() },
        &crate::core::hash(namespace.as_bytes())[..32]
    )
}
pub fn owner_sid() -> Result<String> {
    Ok(format!("uid:{}", unsafe { libc::geteuid() }))
}
pub fn protect_directory(path: &Path) -> Result<()> {
    crate::private_storage::Directory::open(path)?;
    Ok(())
}
fn socket_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(name);
    ensure!(
        path.is_absolute() && name.len() < 108,
        "LOCAL_IPC_SOCKET_PATH_INVALID_OR_TOO_LONG"
    );
    protect_directory(path.parent().context("LOCAL_IPC_PARENT_REQUIRED")?)?;
    Ok(path)
}
fn validate(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_socket()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o077 == 0,
        "LOCAL_IPC_UNSAFE_SOCKET"
    );
    Ok(())
}
/// A blocking connect can hang before stream deadlines apply when the Unix
/// listener backlog is full. Keep descriptor ownership and connect bounded.
pub(crate) fn connect_socket(path: &Path) -> std::io::Result<UnixStream> {
    let bytes = path.as_os_str().as_bytes();
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if bytes.len() >= address.sun_path.len() || bytes.contains(&0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid Unix socket path",
        ));
    }
    address.sun_family = libc::AF_UNIX as _;
    for (target, byte) in address.sun_path.iter_mut().zip(bytes) {
        *target = *byte as _;
    }
    let fd = unsafe {
        libc::socket(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stream = unsafe { UnixStream::from_raw_fd(fd) };
    let length =
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1) as libc::socklen_t;
    if unsafe { libc::connect(fd, (&address as *const libc::sockaddr_un).cast(), length) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINPROGRESS) {
            return Err(error);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut event = libc::pollfd {
            fd,
            events: libc::POLLOUT,
            revents: 0,
        };
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(std::io::ErrorKind::TimedOut.into());
            }
            let result = unsafe { libc::poll(&mut event, 1, remaining.as_millis().max(1) as i32) };
            if result == 0 {
                return Err(std::io::ErrorKind::TimedOut.into());
            }
            if result < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            let mut error = 0i32;
            let mut size = std::mem::size_of::<i32>() as libc::socklen_t;
            if unsafe {
                libc::getsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_ERROR,
                    (&mut error as *mut i32).cast(),
                    &mut size,
                )
            } != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            if error != 0 {
                return Err(std::io::Error::from_raw_os_error(error));
            }
            break;
        }
    }
    stream.set_nonblocking(false)?;
    Ok(stream)
}
pub(crate) fn remove_stale_socket(path: &Path) -> Result<()> {
    validate(path)?;
    match connect_socket(path) {
        Ok(_) => anyhow::bail!("LOCAL_IPC_ALREADY_OWNED"),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ECONNREFUSED | libc::ENOENT)
            ) =>
        {
            fs::remove_file(path).context("LOCAL_IPC_STALE_REMOVE")
        }
        Err(error) => Err(error).context("LOCAL_IPC_CONNECT_PROBE_FAILED"),
    }
}
fn credentials(stream: &UnixStream) -> Result<libc::ucred> {
    let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
    let mut size = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    ensure!(
        unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut credentials as *mut libc::ucred).cast(),
                &mut size,
            )
        } == 0,
        "LOCAL_IPC_CREDENTIALS"
    );
    ensure!(
        credentials.uid == unsafe { libc::geteuid() },
        "LOCAL_IPC_OWNER_MISMATCH"
    );
    Ok(credentials)
}
fn transfer(
    stream: &mut UnixStream,
    bytes: &mut [u8],
    write: bool,
    deadline: Instant,
) -> Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), "LOCAL_IPC_TIMEOUT");
        let count = if write {
            stream.set_write_timeout(Some(remaining))?;
            stream.write(&bytes[offset..])?
        } else {
            stream.set_read_timeout(Some(remaining))?;
            stream.read(&mut bytes[offset..])?
        };
        ensure!(count != 0, "LOCAL_IPC_CLOSED");
        offset += count;
    }
    Ok(())
}
fn read<T: DeserializeOwned>(stream: &mut UnixStream, deadline: Instant) -> Result<T> {
    let mut length = [0; 4];
    transfer(stream, &mut length, false, deadline)?;
    let length = u32::from_le_bytes(length) as usize;
    ensure!(length <= MAX_FRAME, "MANAGEMENT_FRAME_TOO_LARGE");
    let mut bytes = vec![0; length];
    transfer(stream, &mut bytes, false, deadline)?;
    Ok(serde_json::from_slice(&bytes)?)
}
fn write<T: Serialize>(stream: &mut UnixStream, value: &T, deadline: Instant) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAX_FRAME, "MANAGEMENT_FRAME_TOO_LARGE");
    transfer(
        stream,
        &mut (bytes.len() as u32).to_le_bytes(),
        true,
        deadline,
    )?;
    transfer(stream, &mut bytes, true, deadline)
}
pub struct Listener {
    socket: UnixListener,
    pending: Option<UnixStream>,
    path: PathBuf,
    inode: u64,
}
impl Listener {
    pub fn bind(name: &str) -> Result<Self> {
        let path = socket_path(name)?;
        if fs::symlink_metadata(&path).is_ok() {
            remove_stale_socket(&path)?;
        }
        let socket = UnixListener::bind(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        socket.set_nonblocking(true)?;
        let inode = fs::symlink_metadata(&path)?.ino();
        Ok(Self {
            socket,
            pending: None,
            path,
            inode,
        })
    }
    pub fn receive<T: DeserializeOwned>(&mut self) -> Result<Option<T>> {
        self.pending = None;
        let (mut stream, _) = match self.socket.accept() {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error).context("LOCAL_IPC_ACCEPT"),
        };
        credentials(&stream)?;
        let request = read(&mut stream, Instant::now() + Duration::from_secs(3))?;
        self.pending = Some(stream);
        Ok(Some(request))
    }
    pub fn reply<T: Serialize>(&mut self, value: &T) -> Result<()> {
        let mut stream = self.pending.take().context("LOCAL_IPC_NO_REQUEST")?;
        let deadline = Instant::now() + Duration::from_secs(3);
        write(&mut stream, value, deadline)?;
        let mut ack = [0];
        transfer(&mut stream, &mut ack, false, deadline)?;
        ensure!(ack == [1], "LOCAL_IPC_INVALID_ACK");
        Ok(())
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.ino() == self.inode && m.file_type().is_socket())
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}
pub fn call<T: Serialize, R: DeserializeOwned>(name: &str, request: &T) -> Result<R> {
    call_timeout(name, request, 20000)
}
pub fn call_timeout<T: Serialize, R: DeserializeOwned>(
    name: &str,
    request: &T,
    timeout: u32,
) -> Result<R> {
    let path = socket_path(name)?;
    validate(&path)?;
    let mut stream = connect_socket(&path).context("LOCAL_IPC_CONNECT")?;
    let peer = credentials(&stream)?;
    let deadline = Instant::now() + Duration::from_millis(timeout as u64);
    write(&mut stream, request, deadline)?;
    let response: serde_json::Value = read(&mut stream, deadline)?;
    if let Some(pid) = response["result"]["pid"].as_u64() {
        ensure!(pid == peer.pid as u64, "LOCAL_IPC_WORKER_PID_MISMATCH");
    }
    transfer(&mut stream, &mut [1], true, deadline)?;
    Ok(serde_json::from_value(response)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn private_fixture() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        root
    }
    #[test]
    fn live_and_backlogged_sockets_are_never_displaced() {
        let root = private_fixture();
        let path = root.path().join("control.sock");
        let listener = Listener::bind(path.to_str().unwrap()).unwrap();
        let inode = fs::symlink_metadata(&path).unwrap().ino();
        assert!(Listener::bind(path.to_str().unwrap()).is_err());
        let mut streams = Vec::new();
        // Unix connect returns EAGAIN immediately once this small backlog fills.
        assert_eq!(unsafe { libc::listen(listener.socket.as_raw_fd(), 1) }, 0);
        let started = Instant::now();
        for _ in 0..8 {
            match connect_socket(&path) {
                Ok(stream) => streams.push(stream),
                Err(error) => {
                    assert_eq!(error.raw_os_error(), Some(libc::EAGAIN));
                    break;
                }
            }
        }
        assert!(Listener::bind(path.to_str().unwrap()).is_err());
        assert!(started.elapsed() < Duration::from_secs(4));
        assert_eq!(fs::symlink_metadata(&path).unwrap().ino(), inode);
        drop(listener);
        assert!(!path.exists());
    }
    #[test]
    fn unsafe_paths_frames_and_claimed_process_are_rejected() {
        let root = private_fixture();
        let path = root.path().join("control.sock");
        let mut listener = Listener::bind(path.to_str().unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(validate(&path).is_err());
        assert!(Listener::bind(path.to_str().unwrap()).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let mut stream = connect_socket(&path).unwrap();
        stream
            .write_all(&((MAX_FRAME + 1) as u32).to_le_bytes())
            .unwrap();
        assert!(
            listener
                .receive::<serde_json::Value>()
                .unwrap_err()
                .to_string()
                .contains("FRAME_TOO_LARGE")
        );
        drop(stream);
        let name = path.to_str().unwrap().to_owned();
        let client =
            std::thread::spawn(move || call::<_, serde_json::Value>(&name, &serde_json::json!({})));
        loop {
            if listener.receive::<serde_json::Value>().unwrap().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        // The response cannot claim a PID other than the kernel peer PID.
        let _ = listener.reply(&serde_json::json!({"result":{"pid":1}}));
        assert!(
            client
                .join()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("WORKER_PID_MISMATCH")
        );
        drop(listener);
        std::os::unix::fs::symlink(root.path().join("absent"), &path).unwrap();
        assert!(Listener::bind(path.to_str().unwrap()).is_err());
        assert!(Listener::bind(&format!("/{}", "x".repeat(108))).is_err());
    }
    #[test]
    fn stalled_owner_reply_is_bounded() {
        let root = private_fixture();
        let path = root.path().join("control.sock");
        let _listener = Listener::bind(path.to_str().unwrap()).unwrap();
        let started = Instant::now();
        assert!(
            call_timeout::<_, serde_json::Value>(
                path.to_str().unwrap(),
                &serde_json::json!({}),
                100
            )
            .is_err()
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
