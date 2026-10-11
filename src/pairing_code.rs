//! Reusable enrollment verifier, separate from permanent device identities.
//! Only reset returns the secret. SQLite FULL transactions serialize reset and
//! enrollment; a lost reset response requires a fresh explicit reset.
use anyhow::{Context, Result, ensure};
use rand::Rng;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

pub const CODE_DIGITS: usize = 200;
const ATTEMPTS_PER_MINUTE: u64 = 30;

pub struct ShowOnceCode {
    code: Zeroizing<String>,
    generation: u64,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Duration;

    pub(crate) fn configuration(root: &Path) -> PathBuf {
        let path = root.join("orchestrator.toml");
        let transport = if cfg!(windows) {
            "named-pipe"
        } else {
            "unix-socket"
        };
        std::fs::write(&path, format!("format_version=1\ndata_directory='o1'\n[control]\ntransport='{transport}'\nname='code-test-{}'\n", crate::core::id())).unwrap();
        drop(
            crate::orchestrator::Supervisor::open(
                crate::orchestrator::Config::load(&path).unwrap(),
            )
            .unwrap(),
        );
        path
    }
    #[test]
    fn exactly_200_uniform_decimal_digits_including_leading_zeroes() {
        let code = generate(&mut rand::rngs::mock::StepRng::new(0, 0));
        assert_eq!(*code, "0".repeat(200));
        validate(&code).unwrap();
        for bad in [
            "0".repeat(199),
            "0".repeat(201),
            format!("{}x", "0".repeat(199)),
            format!("{}\n", "0".repeat(200)),
            "٠".repeat(200),
        ] {
            assert_eq!(
                validate(&bad).unwrap_err().to_string(),
                "INVALID_PAIRING_CODE"
            );
        }
    }
    #[test]
    fn only_verifier_is_persisted_reuse_restart_rotation_and_redacted_debug() {
        let temp = tempfile::tempdir().unwrap();
        let path = configuration(temp.path());
        let mut store = Store::for_config(&path).unwrap();
        let first = store.reset().unwrap();
        assert_eq!(first.generation(), 1);
        validate(first.expose()).unwrap();
        assert_eq!(format!("{first:?}"), "ShowOnceCode([REDACTED])");
        store.authorize(first.expose(), || Ok(())).unwrap();
        store.authorize(first.expose(), || Ok(())).unwrap();
        drop(store);
        let mut store = Store::for_config(&path).unwrap();
        store.authorize(first.expose(), || Ok(())).unwrap();
        let second = store.reset().unwrap();
        assert_eq!(second.generation(), 2);
        assert_ne!(first.expose(), second.expose());
        assert!(store.authorize(first.expose(), || Ok(())).is_err());
        store.authorize(second.expose(), || Ok(())).unwrap();
        for entry in std::fs::read_dir(Store::directory_for_config(&path).unwrap()).unwrap() {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            assert!(
                !bytes
                    .windows(CODE_DIGITS)
                    .any(|v| v == first.expose().as_bytes() || v == second.expose().as_bytes())
            );
        }
        assert!(
            !std::fs::read_to_string(&path)
                .unwrap()
                .contains(second.expose())
        );
    }
    #[test]
    fn failed_attempt_budget_persists_and_rotation_restores_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let path = configuration(temp.path());
        let mut store = Store::for_config(&path).unwrap();
        let code = store.reset().unwrap();
        for _ in 0..ATTEMPTS_PER_MINUTE {
            assert!(store.authorize(&"x".repeat(200), || Ok(())).is_err()); // no hashing
            assert_eq!(
                store
                    .authorize(&"0".repeat(200), || Ok(()))
                    .unwrap_err()
                    .to_string(),
                "PAIRING_CODE_REJECTED"
            );
        }
        drop(store);
        let mut store = Store::for_config(&path).unwrap();
        assert_eq!(
            store
                .authorize(code.expose(), || Ok(()))
                .unwrap_err()
                .to_string(),
            "PAIRING_CODE_RATE_LIMITED"
        );
        let fresh = store.reset().unwrap();
        store.authorize(fresh.expose(), || Ok(())).unwrap();
    }
    #[test]
    fn concurrent_rotations_are_atomic_and_admission_fences_reset() {
        let temp = tempfile::tempdir().unwrap();
        let path = configuration(temp.path());
        let mut store = Store::for_config(&path).unwrap();
        let barrier = Arc::new(Barrier::new(5));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let barrier = barrier.clone();
                let path = path.clone();
                std::thread::spawn(move || {
                    let mut store = Store::for_config(&path).unwrap();
                    barrier.wait();
                    store.reset().unwrap()
                })
            })
            .collect();
        barrier.wait();
        let mut codes: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        codes.sort_by_key(ShowOnceCode::generation);
        assert_eq!(
            codes
                .iter()
                .map(ShowOnceCode::generation)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        for code in &codes[..3] {
            assert!(store.authorize(code.expose(), || Ok(())).is_err());
        }
        let (began, received) = mpsc::channel();
        let (finished, done) = mpsc::channel();
        let mut rotate = Store::for_config(&path).unwrap();
        let thread = std::thread::spawn(move || {
            received.recv().unwrap();
            let next = rotate.reset().unwrap();
            finished.send(next).unwrap();
        });
        store
            .authorize(codes[3].expose(), || {
                began.send(()).unwrap();
                assert!(done.recv_timeout(Duration::from_millis(150)).is_err());
                Ok(())
            })
            .unwrap();
        let next = done.recv_timeout(Duration::from_secs(3)).unwrap();
        thread.join().unwrap();
        assert_eq!(next.generation(), 5);
        assert!(store.authorize(codes[3].expose(), || Ok(())).is_err());
        store.authorize(next.expose(), || Ok(())).unwrap();
    }
    #[test]
    fn replacement_config_and_missing_catalog_fail_closed_without_provisioning() {
        let temp = tempfile::tempdir().unwrap();
        let path = configuration(temp.path());
        let mut original = Store::for_config(&path).unwrap();
        let code = original.reset().unwrap();
        let second = temp.path().join("second");
        std::fs::create_dir(&second).unwrap();
        let replacement = configuration(&second);
        let text = std::fs::read_to_string(replacement)
            .unwrap()
            .replace("data_directory='o1'", "data_directory='second/o1'");
        std::fs::write(&path, text).unwrap();
        assert!(Store::for_config(&path).is_err());
        assert!(original.reset().is_err());
        assert!(original.authorize(code.expose(), || Ok(())).is_err());
        let text = std::fs::read_to_string(&path)
            .unwrap()
            .replace("second/o1", "does-not-exist");
        std::fs::write(&path, text).unwrap();
        assert!(Store::for_config(&path).is_err());
        assert!(!temp.path().join("does-not-exist").exists());
    }
    #[test]
    fn verifier_hardlink_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let path = configuration(temp.path());
        let mut store = Store::for_config(&path).unwrap();
        store.reset().unwrap();
        drop(store);
        let verifier = Store::directory_for_config(&path)
            .unwrap()
            .join("verifier.sqlite");
        std::fs::hard_link(&verifier, temp.path().join("alias.sqlite")).unwrap();
        assert!(Store::for_config(&path).is_err());
    }
    #[test]
    #[ignore = "subprocess crash fixture, invoked only by crash_atomicity"]
    fn crash_fixture() {
        let path = std::env::var_os("TKFS_PAIRING_CODE_TEST_CONFIG").expect("fixture path");
        let mut store = Store::for_config(Path::new(&path)).unwrap();
        let _code = store.reset().unwrap();
        panic!("expected crash fault");
    }
    #[test]
    fn crash_atomicity_before_and_after_commit_never_recovers_secret() {
        let temp = tempfile::tempdir().unwrap();
        let path = configuration(temp.path());
        let mut store = Store::for_config(&path).unwrap();
        let old = store.reset().unwrap();
        let crash = |point: &str| {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["pairing_code::tests::crash_fixture", "--exact", "--ignored"])
                .env("TKFS_PAIRING_CODE_TEST_CONFIG", &path)
                .env("TKFS_FAULT", point)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86));
        };
        crash("pairing_code_reset_prepared");
        store.authorize(old.expose(), || Ok(())).unwrap();
        crash("pairing_code_reset_committed");
        assert!(store.authorize(old.expose(), || Ok(())).is_err());
        let fresh = store.reset().unwrap();
        assert_eq!(fresh.generation(), 3);
        store.authorize(fresh.expose(), || Ok(())).unwrap();
    }
}
impl ShowOnceCode {
    pub fn expose(&self) -> &str {
        &self.code
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
impl std::fmt::Debug for ShowOnceCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ShowOnceCode([REDACTED])")
    }
}
pub fn validate(code: &str) -> Result<()> {
    ensure!(
        code.len() == CODE_DIGITS && code.bytes().all(|b| b.is_ascii_digit()),
        "INVALID_PAIRING_CODE"
    );
    Ok(())
}
fn generate(rng: &mut impl rand::RngCore) -> Zeroizing<String> {
    // rand's uniform range uses rejection sampling, including leading zeroes.
    Zeroizing::new(
        (0..CODE_DIGITS)
            .map(|_| char::from(b'0' + rng.gen_range(0..10u8)))
            .collect(),
    )
}
fn verifier(salt: &[u8], code: &str) -> Vec<u8> {
    // This is a machine-generated ~664-bit secret, not a human password. A
    // fixed-cost salted SHA-256 verifier avoids attacker-selected KDF costs.
    let mut digest = Sha256::new();
    digest.update(b"tkfs-enrollment-code-v2\0");
    digest.update(salt);
    digest.update(code.as_bytes());
    digest.finalize().to_vec()
}
pub struct Store {
    config_path: PathBuf,
    binding: String,
    db: Connection,
    _file: std::fs::File,
    _directory: crate::private_storage::Directory,
}
impl Store {
    fn binding(path: &Path) -> Result<String> {
        let config = crate::orchestrator::Config::load(path)?;
        let identity = config.existing_installation()?;
        Ok(serde_json::to_string(&(
            path,
            std::fs::canonicalize(&config.data_directory)?,
            identity,
        ))?)
    }
    fn check_binding(&self) -> Result<()> {
        ensure!(
            Self::binding(&self.config_path)? == self.binding,
            "PAIRING_ORCHESTRATOR_BINDING_MISMATCH"
        );
        Ok(())
    }
    pub fn directory_for_config(path: &Path) -> Result<PathBuf> {
        let path = std::fs::canonicalize(path).context("PAIRING_ORCHESTRATOR_CONFIG_NOT_FOUND")?;
        let mut name = path
            .file_name()
            .context("PAIRING_CONFIG_FILENAME_REQUIRED")?
            .to_os_string();
        name.push(".pairing");
        Ok(path.with_file_name(name))
    }
    pub fn for_config(path: &Path) -> Result<Self> {
        let canonical =
            std::fs::canonicalize(path).context("PAIRING_ORCHESTRATOR_CONFIG_NOT_FOUND")?;
        let binding = Self::binding(&canonical)?;
        let path = Self::directory_for_config(&canonical)?;
        let directory = crate::private_storage::Directory::open(&path)?;
        directory.validate_existing_files()?;
        let path = path.join("verifier.sqlite");
        let file = directory.file(&path, true, false)?;
        let mut db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(3))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS code(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,salt BLOB NOT NULL,verifier BLOB NOT NULL,generation INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS rate(id INTEGER PRIMARY KEY CHECK(id=1),window INTEGER NOT NULL,attempts INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL);
            INSERT OR IGNORE INTO rate VALUES(1,0,0);")?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let bound: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM binding)", [], |r| r.get(0))?;
        let has_code: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM code)", [], |r| r.get(0))?;
        ensure!(bound || !has_code, "PAIRING_ORCHESTRATOR_BINDING_MISSING");
        tx.execute("INSERT OR IGNORE INTO binding VALUES(1,?)", [&binding])?;
        let stored: String =
            tx.query_row("SELECT value FROM binding WHERE id=1", [], |r| r.get(0))?;
        ensure!(stored == binding, "PAIRING_ORCHESTRATOR_BINDING_MISMATCH");
        tx.commit()?;
        directory.validate_existing_files()?;
        Ok(Self {
            config_path: canonical,
            binding,
            db,
            _file: file,
            _directory: directory,
        })
    }
    pub fn reset(&mut self) -> Result<ShowOnceCode> {
        self.check_binding()?;
        let code = generate(&mut rand::rngs::OsRng);
        let salt: [u8; 32] = rand::rngs::OsRng.r#gen();
        let digest = verifier(&salt, &code);
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute("INSERT INTO code VALUES(1,2,?,?,1) ON CONFLICT(id) DO UPDATE SET version=2,salt=excluded.salt,verifier=excluded.verifier,generation=generation+1", params![salt.as_slice(),digest])?;
        let generation =
            tx.query_row("SELECT generation FROM code WHERE id=1", [], |r| r.get(0))?;
        tx.execute("UPDATE rate SET window=0,attempts=0 WHERE id=1", [])?;
        crate::core::fault("pairing_code_reset_prepared");
        tx.commit()?;
        crate::core::fault("pairing_code_reset_committed");
        Ok(ShowOnceCode { code, generation })
    }
    /// Holds the reset fence until the authorized enrollment is committed.
    /// Attempts are committed even on failure and persist across restarts.
    pub(crate) fn authorize<T>(
        &mut self,
        code: &str,
        action: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        validate(code)?;
        self.check_binding()?;
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let now = crate::core::time();
        let (window, attempts): (u64, u64) =
            tx.query_row("SELECT window,attempts FROM rate WHERE id=1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
        let expired = now >= window.saturating_add(60_000);
        let attempts = if expired { 0 } else { attempts };
        ensure!(attempts < ATTEMPTS_PER_MINUTE, "PAIRING_CODE_RATE_LIMITED");
        tx.execute(
            "UPDATE rate SET window=?,attempts=? WHERE id=1",
            params![if expired { now } else { window }, attempts + 1],
        )?;
        let stored: Option<(u32, Vec<u8>, Vec<u8>)> = tx
            .query_row(
                "SELECT version,salt,verifier FROM code WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let accepted = stored.is_some_and(|(version, salt, expected)| {
            version == 2
                && salt.len() == 32
                && expected.len() == 32
                && bool::from(verifier(&salt, code).ct_eq(&expected))
        });
        if !accepted {
            tx.commit()?;
            anyhow::bail!("PAIRING_CODE_REJECTED");
        }
        let result = action();
        tx.commit()?;
        result
    }
}
