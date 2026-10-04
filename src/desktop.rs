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
    pub fn from_executable(executable: PathBuf) -> Result<Self> {
        let executable = fs::canonicalize(executable)?;
        let folder = executable
            .parent()
            .context("APPLICATION_DIRECTORY_MISSING")?
            .to_owned();
        Ok(Self {
            config: folder.join("orchestrator.toml"),
            executable,
            folder,
        })
    }
    fn pin(&self) -> PathBuf {
        self.folder.join(".tkfs-ui-identity.json")
    }
    fn journal(&self) -> PathBuf {
        self.folder.join(".tkfs-ui-operation.json")
    }
}

fn exclusive(path: &Path) -> Result<File> {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        match OpenOptions::new().create(true).truncate(false).write(true).share_mode(0).open(path) {
            Ok(file) => return Ok(file),
            Err(e) if matches!(e.raw_os_error(), Some(32 | 33)) && Instant::now() < deadline => std::thread::sleep(Duration::from_millis(80)),
            Err(e) => return Err(e).context("APPLICATION_FOLDER_NOT_WRITABLE_OR_BUSY: move the portable application to a writable folder"),
        }
    }
}
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", id()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}
fn writable(directory: &Path) -> Result<()> {
    let probe = directory.join(format!(".tkfs-write-{}", id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&probe)
        .context("FOLDER_NOT_WRITABLE")?;
    file.write_all(b"probe")?;
    file.sync_all()?;
    drop(file);
    fs::remove_file(probe)?;
    Ok(())
}
fn overlap(a: &Path, b: &Path) -> bool {
    let key = |p: &Path| {
        p.to_string_lossy()
            .trim_end_matches(['\\', '/'])
            .replace('/', "\\")
            .to_lowercase()
    };
    let (a, b) = (key(a), key(b));
    a == b || a.starts_with(&format!("{b}\\")) || b.starts_with(&format!("{a}\\"))
}

pub fn bootstrap(paths: &Paths, data: &Path, mounts: &Path, limit: usize) -> Result<Config> {
    let _guard = exclusive(&paths.folder.join(".tkfs-config.lock"))?;
    if paths.config.try_exists()? {
        return Config::load(&paths.config);
    }
    ensure!(
        data.is_absolute() && mounts.is_absolute(),
        "CHOOSE_ABSOLUTE_FOLDERS"
    );
    ensure!((1..=64).contains(&limit), "WORKER_LIMIT_MUST_BE_1_TO_64");
    writable(&paths.folder)?;
    if data.try_exists()? {
        ensure!(
            data.is_dir() && fs::read_dir(data)?.next().is_none(),
            "DATA_FOLDER_MUST_BE_EMPTY: existing stores are not adopted by this release"
        );
    }
    fs::create_dir_all(data)?;
    fs::create_dir_all(mounts)?;
    let data = fs::canonicalize(data)?;
    let mounts = fs::canonicalize(mounts)?;
    ensure!(
        !overlap(&data, &mounts) && !paths.folder.starts_with(&data),
        "DATA_FOLDER_OVERLAP: keep native data separate from application files and project mount folders"
    );
    writable(&data)?;
    writable(&mounts)?;
    let identity = id();
    let text = toml::to_string_pretty(&json!({"format_version":1,"installation_id":identity,
        "data_directory":data,"default_mount_directory":mounts,
        "control":{"transport":"named-pipe","name":format!("tkfs-{}",id())},
        "network":{"enabled":false},"workers":{"restart_policy":"on-failure","maximum_running":limit}}))?;
    atomic_write(&paths.config, text.as_bytes())?;
    Config::load(&paths.config)
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
    ensure!(
        hello["api_version"] == 1 && hello["registry_version"] == 1,
        "DESKTOP_API_VERSION_MISMATCH"
    );
    ensure!(
        hello["build_version"] == env!("CARGO_PKG_VERSION"),
        "DESKTOP_BUILD_VERSION_MISMATCH: close clients and perform an orderly upgrade"
    );
    let identity = hello["installation_id"]
        .as_str()
        .context("HELLO_IDENTITY_MISSING")?;
    uuid::Uuid::parse_str(identity)?;
    if let Some(expected) = expected.or(config.installation_id.as_deref()) {
        ensure!(
            identity == expected,
            "INSTALLATION_ID_MISMATCH: this pipe belongs to another installation"
        );
    }
    let root =
        fs::canonicalize(&config.data_directory).context("CONFIGURED_DATA_FOLDER_MISSING")?;
    let actual = PathBuf::from(
        hello["data_directory"]
            .as_str()
            .context("HELLO_DATA_DIRECTORY_MISSING")?,
    );
    ensure!(
        root == actual,
        "DATA_DIRECTORY_MISMATCH: this pipe belongs to another installation"
    );
    let capabilities = hello["capabilities"]
        .as_array()
        .context("HELLO_CAPABILITIES_MISSING")?;
    ensure!(
        [
            "local-management-v1",
            "target-installation-v1",
            "runtime-rpc-v1"
        ]
        .iter()
        .all(|cap| capabilities.contains(&json!(cap))),
        "DESKTOP_CAPABILITY_MISMATCH"
    );
    Ok(())
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
                    verify_hello(&config, expected.as_deref(), &hello)?;
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
                    data_directory: fs::canonicalize(&config.data_directory)?,
                })?,
            )?;
        }
        let saved = if paths.journal().try_exists()? {
            Some(
                serde_json::from_slice(&fs::read(paths.journal())?)
                    .context("INVALID_OPERATION_JOURNAL")?,
            )
        } else {
            None
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
        ensure!(action.mutates(), "NOT_A_MUTATION");
        let _guard = exclusive(&self.paths.folder.join(".tkfs-operation.lock"))?;
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
        let address: std::net::SocketAddr = discovery
            .address
            .parse()
            .context("INVALID_RUNTIME_ADDRESS")?;
        ensure!(address.ip().is_loopback(), "RUNTIME_MUST_BE_LOCAL");
        ensure!(
            fs::canonicalize(&discovery.state)?
                == fs::canonicalize(self.config.data_directory.join("states").join(state))?,
            "RUNTIME_STATE_PATH_MISMATCH"
        );
        runtime::rpc(&discovery, &id(), payload)
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
        let lock = paths.folder.join(".tkfs-config.lock");
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
        assert!(format!("{error:#}").contains("APPLICATION_FOLDER_NOT_WRITABLE_OR_BUSY"));
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
