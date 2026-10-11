//! Portable desktop client. Never opens worker stores or lifecycle secrets.
use crate::{
    core::id,
    orchestrator::{self, Action, Config, Request},
    runtime::{self, Discovery},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::windows::{fs::OpenOptionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct Paths {
    pub executable: PathBuf,
    pub folder: PathBuf,
    pub config: PathBuf,
}
impl Paths {
    pub fn from_config(executable: PathBuf, config: &Path) -> Result<Self> {
        let config = match fs::canonicalize(config) {
            Ok(path) => path,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && fs::symlink_metadata(config).is_err() =>
            {
                std::path::absolute(config)?
            }
            Err(error) => return Err(error).context("CONFIG_PATH_CANONICALIZE"),
        };
        let folder = fs::canonicalize(config.parent().context("CONFIG_PARENT_REQUIRED")?)
            .context("CONFIG_PARENT_CANONICALIZE")?;
        Ok(Self {
            executable: fs::canonicalize(executable).context("EXECUTABLE_CANONICALIZE")?,
            config: folder.join(config.file_name().context("CONFIG_FILENAME_REQUIRED")?),
            folder,
        })
    }
    pub fn from_executable(executable: PathBuf) -> Result<Self> {
        let executable = fs::canonicalize(executable)?;
        let folder = executable
            .parent()
            .context("APPLICATION_DIRECTORY_MISSING")?
            .to_owned();
        let config = folder.join("orchestrator.toml");
        Self::from_config(executable, &config)
    }
    fn pin(&self) -> PathBuf {
        self.scoped_file("identity")
    }
    fn journal(&self) -> PathBuf {
        self.scoped_file("operation")
    }
    fn scoped_file(&self, purpose: &str) -> PathBuf {
        let scope = crate::core::hash(self.config.to_string_lossy().to_lowercase().as_bytes());
        self.folder
            .join(".tkfs-desktop")
            .join(scope)
            .join(purpose)
            .join("record.json")
    }
    pub(crate) fn metadata_file(&self, purpose: &str) -> PathBuf {
        self.scoped_file(purpose)
    }
    fn legacy_file(&self, purpose: &str) -> PathBuf {
        if purpose == "branch-operation" {
            return self.folder.join(".tkfs-ui-branch-operation.json");
        }
        if self
            .config
            .file_name()
            .is_some_and(|name| name == "orchestrator.toml")
        {
            self.folder.join(format!(".tkfs-ui-{purpose}.json"))
        } else {
            let scope = crate::core::hash(self.config.to_string_lossy().as_bytes());
            self.folder.join(format!(".tkfs-ui-{scope}-{purpose}.json"))
        }
    }
    /// Caller holds the original launch/operation/branch lock. Legacy bytes are
    /// retained; a hash records the migration baseline so an older binary cannot
    /// silently replace a pending request after the new journal advances.
    pub(crate) fn migrate_metadata(&self, purpose: &str) -> Result<()> {
        self.migrate_metadata_if(purpose, |_| Ok(true))
    }
    pub(crate) fn migrate_metadata_if(
        &self,
        purpose: &str,
        accept_legacy: impl FnOnce(&[u8]) -> Result<bool>,
    ) -> Result<()> {
        let target = self.scoped_file(purpose);
        // These are public desktop receipts, not pairing credentials. Preserve
        // support for ordinary writable app folders without repairing their ACL
        // or importing the credential store's strict ancestor policy.
        let mut directory = self.folder.clone();
        for component in target
            .parent()
            .unwrap()
            .strip_prefix(&self.folder)?
            .components()
        {
            directory.push(component);
            match fs::create_dir(&directory) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e.into()),
            }
            drop(crate::local_ipc::storage_handle(&directory, 1)?);
        }
        let legacy = self.legacy_file(purpose);
        let baseline = target.with_extension("legacy-sha256");
        let old = if legacy.try_exists()? {
            Some(read_metadata_file(&legacy)?)
        } else {
            None
        };
        if target.try_exists()? {
            read_metadata_file(&target)?;
        }
        if let Some(bytes) = &old {
            if !accept_legacy(bytes)? {
                ensure!(!baseline.try_exists()?, "LEGACY_DESKTOP_METADATA_CHANGED");
                return Ok(());
            }
        }
        if baseline.try_exists()? {
            let old = old.context("LEGACY_DESKTOP_METADATA_REMOVED")?;
            ensure!(
                read_metadata_file(&baseline)? == crate::core::hash(&old).as_bytes(),
                "LEGACY_DESKTOP_METADATA_CHANGED: an older client changed its receipt; reconcile before continuing"
            );
            ensure!(target.is_file(), "MIGRATED_DESKTOP_METADATA_MISSING");
        } else if let Some(old) = old {
            if target.try_exists()? {
                ensure!(
                    read_metadata_file(&target)? == old,
                    "CONFLICTING_DESKTOP_METADATA: preserve both receipts and reconcile before continuing"
                );
            } else {
                atomic_write(&target, &old)?;
            }
            atomic_write(&baseline, crate::core::hash(&old).as_bytes())?;
        }
        Ok(())
    }
}

fn read_metadata_file(path: &Path) -> Result<Vec<u8>> {
    use std::io::Read;
    // A held native file handle rejects reparse points and hardlink aliases,
    // and blocks replacement while bytes are read. No ACL is changed.
    let mut file = crate::local_ipc::storage_handle(path, 0)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

pub(crate) fn exclusive(path: &Path) -> Result<File> {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        match OpenOptions::new().create(true).truncate(false).write(true).share_mode(0).open(path) {
            Ok(file) => return Ok(file),
            Err(e) if matches!(e.raw_os_error(), Some(32 | 33)) && Instant::now() < deadline => std::thread::sleep(Duration::from_millis(80)),
            Err(e) => return Err(e).context("APPLICATION_FOLDER_NOT_WRITABLE_OR_BUSY: move the portable application to a writable folder"),
        }
    }
}
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", id()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .context("ATOMIC_TEMP_CREATE")?;
        file.write_all(bytes).context("ATOMIC_TEMP_WRITE")?;
        file.sync_all().context("ATOMIC_TEMP_SYNC")?;
        drop(file);
        fs::rename(&temporary, path).context("ATOMIC_REPLACE")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}
pub fn bootstrap(paths: &Paths, data: &Path, mounts: &Path, limit: usize) -> Result<Config> {
    crate::onboarding::bootstrap(&paths.config, data, mounts, limit, false)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    installation_id: String,
    data_directory: PathBuf,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct SavedOperation {
    pub request: Request,
    pub response: Option<Value>,
    pub uncertain: bool,
}

pub fn verify_hello(config: &Config, expected: Option<&str>, hello: &Value) -> Result<()> {
    orchestrator::verify_hello(config, expected, hello)
}
fn request(action: Action, target: Option<String>) -> Request {
    Request {
        version: 1,
        target_installation: target,
        operation_id: None,
        expected_generation: None,
        action,
    }
}
fn absence(error: &anyhow::Error) -> bool {
    error.chain().any(|e| {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|e| matches!(e.raw_os_error(), Some(2 | 3)))
    })
}
fn busy(error: &anyhow::Error) -> bool {
    error.chain().any(|e| {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|e| matches!(e.raw_os_error(), Some(121 | 231)))
    })
}

pub struct Session {
    pub paths: Paths,
    pub config: Config,
    pub identity: String,
    pub hello: Value,
    pub saved: Option<SavedOperation>,
}
impl Session {
    pub fn connect(paths: Paths) -> Result<Self> {
        let _guard = exclusive(&paths.folder.join(".tkfs-launch.lock"))?;
        let config = Config::load(&paths.config)?;
        paths.migrate_metadata("identity")?;
        let pin: Option<Pin> = if paths.pin().try_exists()? {
            Some(
                serde_json::from_slice(&fs::read(paths.pin())?)
                    .context("INVALID_INSTALLATION_PIN")?,
            )
        } else {
            None
        };
        if let Some(pin) = &pin {
            ensure!(
                config.data_directory.join("orchestrator.sqlite").is_file(),
                "REGISTERED_CATALOG_MISSING: restore the original data directory; an empty replacement will not be created"
            );
            ensure!(
                fs::canonicalize(&config.data_directory)? == pin.data_directory,
                "PINNED_DATA_DIRECTORY_MISMATCH"
            );
            if let Some(expected) = &config.installation_id {
                ensure!(expected == &pin.installation_id, "INSTALLATION_ID_MISMATCH");
            }
        }
        let expected = pin
            .as_ref()
            .map(|p| p.installation_id.clone())
            .or(config.installation_id.clone());
        let hello_request = request(Action::Hello, expected.clone());
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut child = None;
        let hello = loop {
            match orchestrator::client(&config, &hello_request) {
                Ok(reply) => {
                    ensure!(
                        reply["ok"] == true,
                        "CONNECTION_REJECTED: {}",
                        reply["error"]
                    );
                    let hello = reply["result"].clone();
                    verify_hello(&config, expected.as_deref(), &hello)
                        .context("SUPERVISOR_IDENTITY_VERIFY")?;
                    break hello;
                }
                Err(e) => {
                    if let Some(process) = &mut child {
                        let process: &mut std::process::Child = process;
                        if let Some(exit) = process.try_wait()? {
                            // A simultaneous launcher elsewhere may own this same registry.
                            if let Ok(reply) = orchestrator::client(&config, &hello_request) {
                                ensure!(
                                    reply["ok"] == true,
                                    "CONNECTION_REJECTED: {}",
                                    reply["error"]
                                );
                                verify_hello(&config, expected.as_deref(), &reply["result"])?;
                                break reply["result"].clone();
                            }
                            let log = fs::read_to_string(
                                paths.folder.join(".tkfs-supervisor-stderr.log"),
                            )
                            .unwrap_or_default();
                            bail!("SUPERVISOR_START_FAILED ({exit}): {log}");
                        }
                    } else if absence(&e) {
                        // Only a proven missing endpoint authorizes launch. Busy/denied/invalid replies do not.
                        let out = File::create(paths.folder.join(".tkfs-supervisor-stdout.log"))?;
                        let err = File::create(paths.folder.join(".tkfs-supervisor-stderr.log"))?;
                        child = Some(
                            Command::new(&paths.executable)
                                .args(["orchestrator", "--defaults-file"])
                                .arg(&paths.config)
                                .creation_flags(0x08000000)
                                .stdin(Stdio::null())
                                .stdout(out)
                                .stderr(err)
                                .env_remove("TKFS_PEER_KEY")
                                .env_remove("TKFS_FAULT")
                                .spawn()
                                .context("SUPERVISOR_LAUNCH")?,
                        );
                    } else if !busy(&e) {
                        return Err(e);
                    }
                    ensure!(
                        Instant::now() < deadline,
                        "SUPERVISOR_READINESS_TIMEOUT: inspect the supervisor log and reconnect; a launched supervisor has not been killed"
                    );
                    std::thread::sleep(Duration::from_millis(150));
                }
            }
        };
        let identity = hello["installation_id"].as_str().unwrap().to_owned();
        if pin.is_none() {
            atomic_write(
                &paths.pin(),
                &serde_json::to_vec(&Pin {
                    installation_id: identity.clone(),
                    data_directory: fs::canonicalize(&config.data_directory)
                        .context("DATA_DIRECTORY_CANONICALIZE")?,
                })?,
            )
            .context("INSTALLATION_PIN_WRITE")?;
        }
        let saved = {
            let _operation_guard = exclusive(&paths.folder.join(".tkfs-operation.lock"))?;
            paths.migrate_metadata("operation")?;
            if paths.journal().try_exists()? {
                Some(
                    serde_json::from_slice(&fs::read(paths.journal())?)
                        .context("INVALID_OPERATION_JOURNAL")?,
                )
            } else {
                None
            }
        };
        Ok(Self {
            paths,
            config,
            identity,
            hello,
            saved,
        })
    }
    pub fn call(&self, action: Action) -> Result<Value> {
        let response =
            orchestrator::client(&self.config, &request(action, Some(self.identity.clone())))?;
        ensure!(
            response["ok"] == true,
            "MANAGEMENT_ERROR: {}",
            response["error"]
        );
        Ok(response["result"].clone())
    }
    pub fn mutate(&mut self, action: Action, generation: u64) -> Result<Value> {
        ensure!(
            !matches!(action, Action::ResetPairingCode {}),
            "PAIRING_CODE_RESET_NOT_JOURNALED: use the owner-local show-once reset path; never retry a lost response automatically"
        );
        ensure!(action.mutates(), "NOT_A_MUTATION");
        let _guard = exclusive(&self.paths.folder.join(".tkfs-operation.lock"))?;
        self.paths.migrate_metadata("operation")?;
        if self.paths.journal().try_exists()? {
            let previous: SavedOperation =
                serde_json::from_slice(&fs::read(self.paths.journal())?)?;
            ensure!(
                !previous.uncertain
                    && previous
                        .response
                        .as_ref()
                        .is_some_and(|r| r["ok"] == true || r["retryable"] != true),
                "OPERATION_PENDING: inspect and retry the original operation first"
            );
        }
        let saved = SavedOperation {
            request: Request {
                version: 1,
                target_installation: Some(self.identity.clone()),
                operation_id: Some(id()),
                expected_generation: Some(generation),
                action,
            },
            response: None,
            uncertain: true,
        };
        self.saved = Some(saved);
        self.persist()?;
        self.send_saved()
    }
    fn persist(&self) -> Result<()> {
        atomic_write(
            &self.paths.journal(),
            &serde_json::to_vec(self.saved.as_ref().context("NO_SAVED_OPERATION")?)?,
        )
    }
    fn send_saved(&mut self) -> Result<Value> {
        let saved = self.saved.as_mut().context("NO_SAVED_OPERATION")?;
        ensure!(
            saved.request.target_installation.as_deref() == Some(&self.identity),
            "OPERATION_INSTALLATION_MISMATCH"
        );
        let response = orchestrator::client(&self.config, &saved.request)?;
        saved.uncertain = false;
        saved.response = Some(response.clone());
        self.persist()?;
        Ok(response)
    }
    pub fn retry(&mut self) -> Result<Value> {
        let _guard = exclusive(&self.paths.folder.join(".tkfs-operation.lock"))?;
        self.paths.migrate_metadata("operation")?;
        self.saved = Some(serde_json::from_slice(&fs::read(self.paths.journal())?)?);
        self.send_saved()
    }
    pub fn operation(&mut self) -> Result<Value> {
        let saved = self.saved.as_ref().context("NO_SAVED_OPERATION")?;
        let result = self.call(Action::Operation {
            operation: saved
                .request
                .operation_id
                .clone()
                .context("NO_OPERATION_ID")?,
        })?;
        if let Some(response) = result.get("response").filter(|r| !r.is_null()) {
            let _guard = exclusive(&self.paths.folder.join(".tkfs-operation.lock"))?;
            self.paths.migrate_metadata("operation")?;
            let current: SavedOperation = serde_json::from_slice(&fs::read(self.paths.journal())?)?;
            if current.request.operation_id == self.saved.as_ref().unwrap().request.operation_id {
                let saved = self.saved.as_mut().unwrap();
                saved.uncertain = false;
                saved.response = Some(response.clone());
                self.persist()?;
            }
        }
        Ok(result)
    }
    pub fn runtime(&self, state: &str, payload: Value) -> Result<Value> {
        self.runtime_request(state, &id(), payload)
    }
    pub fn runtime_request(&self, state: &str, request: &str, payload: Value) -> Result<Value> {
        uuid::Uuid::parse_str(request).context("INVALID_RUNTIME_REQUEST_ID")?;
        uuid::Uuid::parse_str(state).context("INVALID_STATE_ID")?;
        let path = self
            .config
            .data_directory
            .join("states")
            .join(state)
            .join("runtime.json");
        let discovery: Discovery =
            serde_json::from_slice(&fs::read(path).context("STATE_RUNTIME_UNAVAILABLE")?)?;
        ensure!(discovery.format == 1, "RUNTIME_VERSION_MISMATCH");
        let registered = self.call(Action::Inspect {
            state: state.into(),
        })?;
        ensure!(
            discovery.repo == registered["repo_id"]
                && discovery.device == registered["device_id"]
                && json!(discovery.mount) == registered["mount"],
            "RUNTIME_IDENTITY_MISMATCH"
        );
        ensure!(
            discovery.address.starts_with("pipe:tkfs-worker-"),
            "RUNTIME_REQUIRES_OWNER_PIPE"
        );
        ensure!(
            fs::canonicalize(&discovery.state)?
                == fs::canonicalize(self.config.data_directory.join("states").join(state))?,
            "RUNTIME_STATE_PATH_MISMATCH"
        );
        runtime::rpc(&discovery, request, payload)
    }
}

pub fn pick_folder() -> Result<Option<PathBuf>> {
    use windows::Win32::{
        System::Com::{
            CLSCTX_INPROC_SERVER, COINIT_APARTMENTTHREADED, CoCreateInstance, CoInitializeEx,
            CoTaskMemFree, CoUninitialize,
        },
        UI::Shell::{
            FOS_FORCEFILESYSTEM, FOS_PICKFOLDERS, FileOpenDialog, IFileOpenDialog,
            SIGDN_FILESYSPATH,
        },
    };
    unsafe {
        CoInitializeEx(None, COINIT_APARTMENTTHREADED).ok()?;
        let result = (|| -> Result<Option<PathBuf>> {
            let dialog: IFileOpenDialog =
                CoCreateInstance(&FileOpenDialog, None, CLSCTX_INPROC_SERVER)?;
            dialog.SetOptions(dialog.GetOptions()? | FOS_PICKFOLDERS | FOS_FORCEFILESYSTEM)?;
            if let Err(e) = dialog.Show(None) {
                if e.code().0 as u32 == 0x800704c7 {
                    return Ok(None);
                }
                return Err(e.into());
            }
            let path = dialog.GetResult()?.GetDisplayName(SIGDN_FILESYSPATH)?;
            let text = path.to_string();
            CoTaskMemFree(Some(path.0.cast()));
            Ok(Some(PathBuf::from(text?)))
        })();
        CoUninitialize();
        result
    }
}
pub fn open_folder(path: &Path) -> Result<()> {
    ensure!(path.is_dir(), "PROJECT_NOT_MOUNTED");
    Command::new("explorer.exe").arg(path).spawn()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Paths) {
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("app");
        fs::create_dir(&app).unwrap();
        let exe = app.join("tkfs.exe");
        fs::write(&exe, b"fixture").unwrap();
        let paths = Paths::from_executable(exe).unwrap();
        (temp, paths)
    }
    #[test]
    fn pairing_code_reset_never_enters_operation_journal() {
        let (_temp, paths) = fixture();
        fs::write(&paths.config, "format_version=1\ndata_directory='unused'\n[control]\ntransport='named-pipe'\nname='unused-test-pipe'\n").unwrap();
        let config = Config::load(&paths.config).unwrap();
        let mut session = Session {
            paths: paths.clone(),
            config,
            identity: id(),
            hello: Value::Null,
            saved: None,
        };
        let error = session.mutate(Action::ResetPairingCode {}, 0).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("PAIRING_CODE_RESET_NOT_JOURNALED")
        );
        assert!(!paths.journal().exists());
        assert!(!paths.folder.join(".tkfs-operation.lock").exists());
        assert!(session.saved.is_none());
    }
    #[test]
    fn metadata_migrates_pending_bytes_with_pinned_ancestor_and_refuses_legacy_conflict() {
        let (_temp, paths) = fixture();
        let pending = serde_json::to_vec(&SavedOperation {
            request: Request {
                version: 1,
                target_installation: Some(id()),
                operation_id: Some(id()),
                expected_generation: Some(7),
                action: Action::Stop { state: id() },
            },
            response: None,
            uncertain: true,
        })
        .unwrap();
        fs::write(paths.legacy_file("operation"), &pending).unwrap();
        let _held =
            crate::private_storage::Directory::open(&paths.folder.join("protected-registry"))
                .unwrap();
        let _lock = exclusive(&paths.folder.join(".tkfs-operation.lock")).unwrap();
        assert!(atomic_write(&paths.folder.join("blocked.json"), b"fixture").is_err());
        paths.migrate_metadata("operation").unwrap();
        assert_eq!(fs::read(paths.journal()).unwrap(), pending);
        assert_eq!(fs::read(paths.legacy_file("operation")).unwrap(), pending);
        let migrated: SavedOperation =
            serde_json::from_slice(&fs::read(paths.journal()).unwrap()).unwrap();
        assert!(migrated.uncertain && migrated.response.is_none());
        let mut completed = migrated;
        completed.uncertain = false;
        completed.response = Some(json!({"ok":true}));
        atomic_write(&paths.journal(), &serde_json::to_vec(&completed).unwrap()).unwrap();
        paths.migrate_metadata("operation").unwrap();
        fs::write(
            paths.legacy_file("operation"),
            b"older-client-changed-request",
        )
        .unwrap();
        assert!(
            format!("{:#}", paths.migrate_metadata("operation").unwrap_err())
                .contains("LEGACY_DESKTOP_METADATA_CHANGED")
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(paths.journal()).unwrap()).unwrap()["response"]
                ["ok"],
            true
        );
    }
    #[test]
    fn metadata_interrupted_migration_is_idempotent_and_conflicts_fail_closed() {
        let (_temp, paths) = fixture();
        paths.migrate_metadata("identity").unwrap();
        fs::write(paths.legacy_file("identity"), b"original-pin").unwrap();
        atomic_write(&paths.pin(), b"original-pin").unwrap();
        paths.migrate_metadata("identity").unwrap();
        paths.migrate_metadata("identity").unwrap();
        fs::remove_file(paths.pin()).unwrap();
        assert!(paths.migrate_metadata("identity").is_err());
        paths.migrate_metadata("branch-operation").unwrap();
        fs::write(paths.legacy_file("branch-operation"), b"pending-old").unwrap();
        atomic_write(&paths.metadata_file("branch-operation"), b"pending-new").unwrap();
        assert!(
            format!(
                "{:#}",
                paths.migrate_metadata("branch-operation").unwrap_err()
            )
            .contains("CONFLICTING_DESKTOP_METADATA")
        );
        let mut other = paths.clone();
        other.config = paths.folder.join("other.toml");
        assert_ne!(
            paths.metadata_file("branch-operation"),
            other.metadata_file("branch-operation")
        );
    }
    #[test]
    fn public_metadata_supports_ordinary_temp_and_refuses_hardlink_aliases() {
        let ordinary = PathBuf::from(std::env::var_os("LOCALAPPDATA").unwrap()).join("Temp");
        let temp = tempfile::tempdir_in(ordinary).unwrap();
        let executable = temp.path().join("fixture.exe");
        fs::write(&executable, b"fixture").unwrap();
        let paths = Paths::from_executable(executable).unwrap();
        paths.migrate_metadata("identity").unwrap();
        atomic_write(&paths.pin(), b"public-test-record").unwrap();
        paths.migrate_metadata("identity").unwrap();
        fs::hard_link(paths.pin(), temp.path().join("alias.json")).unwrap();
        assert!(paths.migrate_metadata("identity").is_err());
    }
    #[test]
    fn canonical_config_scope_preserves_first_run_and_case_aliases() {
        let (_temp, paths) = fixture();
        let first = Paths::from_config(paths.executable.clone(), &paths.config).unwrap();
        fs::write(&paths.config, b"fixture-config").unwrap();
        let canonical = Paths::from_config(paths.executable.clone(), &paths.config).unwrap();
        assert_eq!(
            Paths::from_executable(paths.executable.clone())
                .unwrap()
                .pin(),
            canonical.pin()
        );
        let case = Paths::from_config(
            paths.executable.clone(),
            &paths.folder.join("ORCHESTRATOR.TOML"),
        )
        .unwrap();
        assert_eq!(first.pin(), canonical.pin());
        assert_eq!(canonical.pin(), case.pin());
        let alias = paths.folder.join("alias.toml");
        match std::os::windows::fs::symlink_file(&paths.config, &alias) {
            Ok(()) => {
                let linked = Paths::from_config(paths.executable.clone(), &alias).unwrap();
                assert_eq!(linked.pin(), canonical.pin());
                let target_folder = paths.folder.join("real-config");
                fs::create_dir(&target_folder).unwrap();
                let target = target_folder.join("control.toml");
                fs::rename(&paths.config, &target).unwrap();
                std::os::windows::fs::symlink_file(&target, &paths.config).unwrap();
                let normal = Paths::from_executable(paths.executable.clone()).unwrap();
                let explicit = Paths::from_config(paths.executable.clone(), &paths.config).unwrap();
                assert_eq!(normal.config, explicit.config);
                assert_eq!(normal.folder, explicit.folder);
                assert_eq!(normal.pin(), explicit.pin());
            }
            Err(error) if error.raw_os_error() == Some(1314) => eprintln!(
                "Symlink fixture unavailable: current token has no symlink privilege; no system policy changed"
            ),
            Err(error) => panic!("symlink fixture: {error}"),
        }
    }
    #[test]
    fn concurrent_first_run_keeps_one_config() {
        let (temp, paths) = fixture();
        let data = temp.path().join("data");
        let mounts = temp.path().join("projects");
        let threads = (0..4)
            .map(|_| {
                let paths = paths.clone();
                let data = data.clone();
                let mounts = mounts.clone();
                std::thread::spawn(move || {
                    bootstrap(&paths, &data, &mounts, 8)
                        .unwrap()
                        .installation_id
                })
            })
            .collect::<Vec<_>>();
        let identities = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        assert!(identities.iter().all(|v| v == &identities[0]));
        let original = fs::read(&paths.config).unwrap();
        assert!(bootstrap(&paths, Path::new("invalid"), Path::new("invalid"), 0).is_ok());
        assert_eq!(original, fs::read(paths.config).unwrap());
    }
    #[test]
    fn invalid_config_is_never_overwritten() {
        let (temp, paths) = fixture();
        fs::write(&paths.config, b"invalid").unwrap();
        assert!(
            bootstrap(
                &paths,
                &temp.path().join("data"),
                &temp.path().join("mounts"),
                8
            )
            .is_err()
        );
        assert_eq!(fs::read(paths.config).unwrap(), b"invalid");
    }
    #[test]
    fn identities_paths_versions_and_capabilities_are_required() {
        let (temp, paths) = fixture();
        let config = bootstrap(
            &paths,
            &temp.path().join("data"),
            &temp.path().join("mounts"),
            8,
        )
        .unwrap();
        let hello = json!({"api_version":1,"registry_version":1,"installation_id":config.installation_id,"data_directory":fs::canonicalize(&config.data_directory).unwrap(),"build_version":env!("CARGO_PKG_VERSION"),"capabilities":["local-management-v1","target-installation-v1","runtime-rpc-v1"]});
        verify_hello(&config, None, &hello).unwrap();
        for (key, value) in [
            ("api_version", json!(2)),
            ("installation_id", json!(id())),
            ("data_directory", json!(paths.folder)),
            ("build_version", json!("old")),
            ("capabilities", json!([])),
        ] {
            let mut wrong = hello.clone();
            wrong[key] = value;
            assert!(verify_hello(&config, None, &wrong).is_err(), "{key}");
        }
    }
    #[test]
    fn occupied_native_folder_preserves_contents() {
        let (temp, paths) = fixture();
        let data = temp.path().join("data");
        fs::create_dir(&data).unwrap();
        fs::write(data.join("keep"), b"precious").unwrap();
        assert!(bootstrap(&paths, &data, &temp.path().join("mounts"), 8).is_err());
        assert_eq!(fs::read(data.join("keep")).unwrap(), b"precious");
        assert!(!paths.config.exists());
    }
    #[test]
    fn read_only_bootstrap_location_is_actionable() {
        let (temp, paths) = fixture();
        let lock = paths.config.with_extension("tkfs-config.lock");
        fs::write(&lock, b"lock").unwrap();
        let mut permissions = fs::metadata(&lock).unwrap().permissions();
        let original_permissions = permissions.clone();
        permissions.set_readonly(true);
        fs::set_permissions(&lock, permissions).unwrap();
        let error = bootstrap(
            &paths,
            &temp.path().join("data"),
            &temp.path().join("mounts"),
            8,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("CONFIG_FOLDER_NOT_WRITABLE_OR_BUSY"));
        assert!(!paths.config.exists());
        fs::set_permissions(lock, original_permissions).unwrap();
    }
    #[test]
    fn pinned_missing_catalog_never_launches_or_recreates() {
        let (temp, paths) = fixture();
        let config = bootstrap(
            &paths,
            &temp.path().join("data"),
            &temp.path().join("mounts"),
            8,
        )
        .unwrap();
        let root = fs::canonicalize(&config.data_directory).unwrap();
        paths.migrate_metadata("identity").unwrap();
        atomic_write(
            &paths.pin(),
            &serde_json::to_vec(&Pin {
                installation_id: config.installation_id.unwrap(),
                data_directory: root,
            })
            .unwrap(),
        )
        .unwrap();
        let error = Session::connect(paths.clone()).err().unwrap();
        assert!(format!("{error:#}").contains("REGISTERED_CATALOG_MISSING"));
        assert!(!config.data_directory.join("orchestrator.sqlite").exists());
        assert!(!paths.folder.join(".tkfs-supervisor-stdout.log").exists());
    }
}
