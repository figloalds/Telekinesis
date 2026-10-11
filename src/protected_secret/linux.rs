//! systemd 256+ user-scoped credentials, without initializing a host secret.
//!
//! Source contract: systemd v256 src/creds/creds.c and shared/creds-util.{c,h}.
//! There is no systemd-creds --no-ask-password option. Only direct root execution
//! is supported here: non-root IPC could outlive the calling helper and mutate
//! a stale restored host secret after cancellation. A shared flock is held until
//! the direct helper is reaped: systemd must take an exclusive flock to replace
//! a stale host secret, so replacement blocks and is cancelled by our deadline.
//! --with-key=host constrains direct execution. We reject every unscoped or
//! null algorithm before decrypting and before publishing encrypted output.
//! The root administrator is trusted: the held inode cannot prevent unrelated
//! root processes from removing it. No unprivileged actor can replace the checked
//! root-owned path. We never invoke setup, read the host key, or create its path.
use super::{LIMIT, PREFIX};
use anyhow::{Result, bail, ensure};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

const HELPER: &str = "/usr/bin/systemd-creds";
const HOST_SECRET: &str = "/var/lib/systemd/credential.secret";
const UNAVAILABLE: &str = "CREDENTIAL_BACKEND_UNAVAILABLE";
const LOCKED: &str = "CREDENTIAL_LOCKED";
const OUTPUT_LIMIT: usize = LIMIT * 2 - PREFIX.len();
const TIMEOUT: Duration = Duration::from_secs(5);
// Public format identifiers from systemd v256 shared/creds-util.h.
const SCOPED_IDS: [[u8; 16]; 3] = [
    [
        0x55, 0xb9, 0xed, 0x1d, 0x38, 0x59, 0x4d, 0x43, 0xa8, 0x31, 0x9d, 0x2e, 0xbb, 0x33, 0x2a,
        0xc6,
    ],
    [
        0xef, 0x4a, 0xc1, 0x36, 0x79, 0xa9, 0x48, 0x0e, 0xa7, 0xdb, 0x68, 0x89, 0x7f, 0x9f, 0x16,
        0x5d,
    ],
    [
        0xad, 0xbc, 0x4c, 0xa3, 0xef, 0xb6, 0x42, 0x01, 0xba, 0x88, 0x1b, 0x6f, 0x2e, 0x40, 0x95,
        0xea,
    ],
];

#[cfg(test)]
thread_local! { static FORCE_UNAVAILABLE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

/// Isolates the unavailable-backend regression from the machine's real backend.
#[cfg(test)]
pub(super) fn unavailable_fixture<T>(f: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            FORCE_UNAVAILABLE.set(self.0);
        }
    }
    let _reset = Reset(FORCE_UNAVAILABLE.replace(true));
    f()
}

pub(super) fn crypt(context: &str, input: &[u8], encrypt: bool) -> Result<Zeroizing<Vec<u8>>> {
    #[cfg(test)]
    if FORCE_UNAVAILABLE.get() {
        bail!(UNAVAILABLE);
    }
    // Root uses direct encryption/decryption in systemd-creds, never daemon IPC.
    // No privilege elevation is attempted. Delivered Systemd credentials used
    // by existing pairing installations are a separate unchanged backend.
    ensure!(unsafe { libc::geteuid() } == 0, UNAVAILABLE);
    // Check before even spawning --version. No bytes of the host key are read.
    let _host = pin_root_path(Path::new(HOST_SECRET), Kind::HostSecret)?;
    let _helper = pin_root_path(Path::new(HELPER), Kind::Executable)?;
    invoke(context, input, encrypt, |args, bytes| {
        run(HELPER, args, bytes, TIMEOUT).map_err(|_| anyhow::anyhow!(LOCKED))
    })
}

#[derive(Clone, Copy)]
enum Kind {
    HostSecret,
    Executable,
}

fn pin_root_path(path: &Path, kind: Kind) -> Result<Vec<File>> {
    let mut held = Vec::new();
    for ancestor in path.ancestors().skip(1) {
        let file = path_only(ancestor)?;
        let m = file.metadata().map_err(|_| anyhow::anyhow!(UNAVAILABLE))?;
        ensure!(
            m.is_dir() && m.uid() == 0 && m.mode() & 0o022 == 0,
            UNAVAILABLE
        );
        held.push(file);
    }
    let file = match kind {
        Kind::HostSecret => OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| anyhow::anyhow!(UNAVAILABLE))?,
        Kind::Executable => path_only(path)?,
    };
    let m = file.metadata().map_err(|_| anyhow::anyhow!(UNAVAILABLE))?;
    ensure!(m.is_file() && m.uid() == 0 && m.nlink() == 1, UNAVAILABLE);
    match kind {
        // systemd itself requires precisely0400, NOT0600; never repair it here.
        Kind::HostSecret => ensure!(
            m.mode() & 0o7777 == 0o400 && (17..=16 * 1024 * 1024).contains(&m.len()),
            UNAVAILABLE
        ),
        Kind::Executable => ensure!(m.mode() & 0o7022 == 0 && m.mode() & 0o111 != 0, UNAVAILABLE),
    }
    if matches!(kind, Kind::HostSecret) {
        shared_lock(&file)?;
    }
    held.push(file);
    Ok(held)
}

fn shared_lock(file: &File) -> Result<()> {
    ensure!(
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) } == 0,
        LOCKED
    );
    Ok(())
}

fn path_only(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| anyhow::anyhow!(UNAVAILABLE))
}

fn invoke(
    context: &str,
    input: &[u8],
    encrypt: bool,
    mut runner: impl FnMut(&[String], &[u8]) -> Result<Zeroizing<Vec<u8>>>,
) -> Result<Zeroizing<Vec<u8>>> {
    if !encrypt {
        check_scoped(input)?;
    }
    let version = runner(&["--version".into()], &[]).map_err(|_| anyhow::anyhow!(UNAVAILABLE))?;
    let version = std::str::from_utf8(&version).map_err(|_| anyhow::anyhow!(UNAVAILABLE))?;
    let mut words = version.split_whitespace();
    ensure!(words.next() == Some("systemd"), UNAVAILABLE);
    let major = words
        .next()
        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or_else(|| anyhow::anyhow!(UNAVAILABLE))?;
    ensure!(major >= 256, UNAVAILABLE);
    let mut args = vec!["--user".into(), format!("--name={context}")];
    if encrypt {
        args.push("--with-key=host".into());
    }
    args.extend([
        if encrypt { "encrypt" } else { "decrypt" }.into(),
        "-".into(),
        "-".into(),
    ]);
    let output = runner(&args, input).map_err(|_| anyhow::anyhow!(LOCKED))?;
    ensure!(output.len() <= OUTPUT_LIMIT, LOCKED);
    if encrypt {
        check_scoped(&output)?;
    }
    Ok(output)
}

fn check_scoped(encoded: &[u8]) -> Result<()> {
    ensure!(encoded.len() <= OUTPUT_LIMIT, LOCKED);
    let compact = Zeroizing::new(
        encoded
            .iter()
            .copied()
            .filter(|b| !b.is_ascii_whitespace())
            .collect::<Vec<_>>(),
    );
    let binary = Zeroizing::new(
        data_encoding::BASE64
            .decode(&compact)
            .map_err(|_| anyhow::anyhow!(LOCKED))?,
    );
    ensure!(
        binary.len() >= 32 && SCOPED_IDS.iter().any(|id| binary.starts_with(id)),
        LOCKED
    );
    Ok(())
}

fn nonblocking(fd: i32) -> Result<()> {
    // Only private child pipe descriptors, never shared caller descriptors.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    ensure!(
        flags >= 0 && unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0,
        LOCKED
    );
    Ok(())
}

fn run(
    helper: &str,
    args: &[String],
    input: &[u8],
    timeout: Duration,
) -> Result<Zeroizing<Vec<u8>>> {
    let mut child = Command::new(helper)
        .args(args)
        .env_clear()
        .env("LANG", "C")
        .env("SYSTEMD_LOG_LEVEL", "err")
        .env("SYSTEMD_LOG_TARGET", "null")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .map_err(|_| anyhow::anyhow!(LOCKED))?;
    let outcome = (|| {
        let mut stdin = child.stdin.take();
        let mut stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!(LOCKED))?;
        nonblocking(
            stdin
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!(LOCKED))?
                .as_raw_fd(),
        )?;
        nonblocking(stdout.as_raw_fd())?;
        let deadline = Instant::now() + timeout;
        let mut written = 0;
        let mut output = Zeroizing::new(Vec::new());
        let mut eof = false;
        let mut status = None;
        let mut block = Zeroizing::new([0u8; 8192]);
        loop {
            ensure!(Instant::now() < deadline, LOCKED);
            if let Some(pipe) = stdin.as_mut() {
                if written < input.len() {
                    match pipe.write(&input[written..]) {
                        Ok(0) => bail!(LOCKED),
                        Ok(n) => written += n,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(_) => bail!(LOCKED),
                    }
                }
                if written == input.len() {
                    stdin.take();
                }
            }
            if !eof {
                match stdout.read(&mut *block) {
                    Ok(0) => eof = true,
                    Ok(n) => {
                        ensure!(output.len() + n <= OUTPUT_LIMIT, LOCKED);
                        output.extend_from_slice(&block[..n]);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => bail!(LOCKED),
                }
            }
            if status.is_none() {
                status = child.try_wait().map_err(|_| anyhow::anyhow!(LOCKED))?;
            }
            if eof && let Some(status) = status {
                ensure!(status.success() && written == input.len(), LOCKED);
                return Ok(output);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    })();
    if outcome.is_err() {
        // Private process group also removes a helper descendant holding a pipe.
        unsafe {
            libc::kill(-(child.id() as i32), libc::SIGKILL);
        }
        let _ = child.kill();
        let _ = child.wait();
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ciphertext(id: [u8; 16]) -> Vec<u8> {
        let mut bytes = id.to_vec();
        bytes.extend_from_slice(&[1; 64]);
        data_encoding::BASE64.encode(&bytes).into_bytes()
    }
    #[test]
    fn installed_backend_contract_uses_only_scoped_named_pipe_io() {
        let secret = b"synthetic bound material";
        let sealed = ciphertext(SCOPED_IDS[0]);
        for encrypt in [true, false] {
            let mut calls = 0;
            let output =
                invoke(
                    "tkfs-test",
                    if encrypt { secret } else { &sealed },
                    encrypt,
                    |args, input| {
                        calls += 1;
                        if calls == 1 {
                            assert_eq!(args, &["--version"]);
                            assert!(input.is_empty());
                            return Ok(Zeroizing::new(b"systemd 256 (256.9)\n".to_vec()));
                        }
                        assert_eq!(args[0], "--user");
                        assert_eq!(args[1], "--name=tkfs-test");
                        assert!(args.iter().all(|a| !a.contains("synthetic")
                            && a != "setup"
                            && a != "--allow-null"));
                        assert_eq!(&args[args.len() - 2..], &["-", "-"]);
                        if encrypt {
                            assert!(args.contains(&"--with-key=host".into()));
                            assert_eq!(input, secret);
                            Ok(Zeroizing::new(sealed.clone()))
                        } else {
                            assert_eq!(input, &sealed);
                            Ok(Zeroizing::new(secret.to_vec()))
                        }
                    },
                )
                .unwrap();
            assert_eq!(calls, 2);
            assert_eq!(&*output, if encrypt { &sealed[..] } else { secret });
        }
    }
    #[test]
    fn old_missing_locked_and_unscoped_backends_fail_closed() {
        assert!(
            invoke("x", b"secret", true, |_, _| Ok(Zeroizing::new(
                b"systemd 255".to_vec()
            )))
            .unwrap_err()
            .to_string()
                == UNAVAILABLE
        );
        assert!(
            invoke("x", b"secret", true, |_, _| bail!("private stderr"))
                .unwrap_err()
                .to_string()
                == UNAVAILABLE
        );
        let mut calls = 0;
        let error = invoke("x", b"secret", true, |_, _| {
            calls += 1;
            if calls == 1 {
                Ok(Zeroizing::new(b"systemd 256".to_vec()))
            } else {
                bail!("private stderr")
            }
        })
        .unwrap_err();
        assert_eq!(error.to_string(), LOCKED);
        for id in [
            [0; 16],
            [
                0x05, 0x84, 0x69, 0xda, 0xf6, 0xf5, 0x43, 0x24, 0x80, 0x05, 0x49, 0xda, 0x0f, 0x8e,
                0xa2, 0xfb,
            ],
        ] {
            assert!(
                invoke("x", &ciphertext(id), false, |_, _| panic!(
                    "must reject before subprocess"
                ))
                .is_err()
            );
        }
        for id in SCOPED_IDS {
            check_scoped(&ciphertext(id)).unwrap();
        }
    }
    #[test]
    fn metadata_gate_does_not_open_missing_symlink_or_user_owned_secret() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        assert!(pin_root_path(&path, Kind::HostSecret).is_err());
        assert!(!path.exists());
        std::fs::write(&path, b"synthetic").unwrap();
        assert!(pin_root_path(&path, Kind::HostSecret).is_err());
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(pin_root_path(&link, Kind::HostSecret).is_err());
    }
    #[test]
    fn subprocess_io_is_bounded_and_failure_never_returns_stderr() {
        let args = vec!["-c".into(), "cat".into()];
        let input = vec![7; LIMIT];
        assert_eq!(&*run("/bin/sh", &args, &input, TIMEOUT).unwrap(), &input);
        let failure = vec!["-c".into(), "printf 'private-error' >&2; exit 1".into()];
        assert_eq!(
            run("/bin/sh", &failure, b"", TIMEOUT)
                .unwrap_err()
                .to_string(),
            LOCKED
        );
        let flood = vec!["-c".into(), "head -c 262144 /dev/zero".into()];
        assert_eq!(
            run("/bin/sh", &flood, b"", TIMEOUT)
                .unwrap_err()
                .to_string(),
            LOCKED
        );
        let hang = vec!["-c".into(), "sleep 30".into()];
        let start = Instant::now();
        assert_eq!(
            run("/bin/sh", &hang, b"", Duration::from_millis(80))
                .unwrap_err()
                .to_string(),
            LOCKED
        );
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn stale_host_replacement_is_blocked_until_helper_is_killed_and_reaped() {
        // Synthetic file only. Model systemd's stale-header branch: LOCK_EX,
        // then unlink. The parent keeps LOCK_SH through run's kill+wait.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("synthetic-host");
        std::fs::write(&path, b"synthetic stale header and payload").unwrap();
        let held = File::open(&path).unwrap();
        shared_lock(&held).unwrap();
        let inode = held.metadata().unwrap().ino();
        let read_args = vec![path.to_str().unwrap().into()];
        assert_eq!(
            &*run("/bin/cat", &read_args, b"", TIMEOUT).unwrap(),
            b"synthetic stale header and payload"
        );
        let script = "exec 9<\"$1\"; flock -x 9; rm -- \"$1\"";
        let args = vec![
            "-c".into(),
            script.into(),
            "fixture".into(),
            path.to_str().unwrap().into(),
        ];
        assert_eq!(
            run("/bin/sh", &args, b"", Duration::from_millis(80))
                .unwrap_err()
                .to_string(),
            LOCKED
        );
        assert!(path.exists());
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        drop(held);
        // If an authorized helper were left alive, it would now unlink.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"synthetic stale header and payload"
        );
    }
}
