//! Desktop configuration and bounded child ownership. No served paths are opened.
use crate::orchestrator::process::{ObservedProcess, terminate};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Folder {
    pub id: String,
    pub label: String,
    pub state: PathBuf,
    pub mount: Option<PathBuf>,
    #[serde(default = "yes")]
    pub mount_enabled: bool,
    #[serde(default = "yes")]
    pub autostart: bool,
    #[serde(default = "listen")]
    pub listen: String,
    pub advertise: Option<String>,
}
fn yes() -> bool {
    true
}
fn listen() -> String {
    "127.0.0.1:0".into()
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub format_version: u32,
    pub data_directory: PathBuf,
    pub default_mount_directory: PathBuf,
    #[serde(default)]
    pub folders: Vec<Folder>,
}
impl Config {
    pub fn load_or_init(path: &Path, single: Option<&Path>) -> Result<Self> {
        let parent = path.parent().context("CONFIG_PARENT_REQUIRED")?;
        fs::create_dir_all(parent)?;
        let mut config = if path.exists() {
            let value: Self =
                toml::from_str(&fs::read_to_string(path)?).context("INVALID_DESKTOP_CONFIG")?;
            ensure!(
                matches!(value.format_version, 2 | 3),
                "UNSUPPORTED_DESKTOP_CONFIG"
            );
            value
        } else {
            Self {
                format_version: 3,
                data_directory: parent.join("data"),
                default_mount_directory: parent.join("mounts"),
                folders: Vec::new(),
            }
        };
        for directory in [
            &mut config.data_directory,
            &mut config.default_mount_directory,
        ] {
            if directory.is_relative() {
                *directory = parent.join(&*directory);
            }
            *directory = std::path::absolute(&*directory)?;
        }
        for folder in &mut config.folders {
            if folder.state.is_relative() {
                folder.state = parent.join(&folder.state);
            }
            if let Some(mount) = &mut folder.mount
                && mount.is_relative()
                && !mount.to_string_lossy().ends_with(':')
            {
                *mount = parent.join(&*mount);
            }
        }
        if let Some(state) = single {
            let state = std::path::absolute(state)?;
            if !config.folders.iter().any(|folder| folder.state == state) {
                config.folders.push(Folder {
                    id: crate::core::id(),
                    label: state
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .into(),
                    state,
                    mount: None,
                    mount_enabled: false,
                    autostart: true,
                    listen: listen(),
                    advertise: None,
                });
            }
        }
        ensure!(config.folders.len() <= 64, "DESKTOP_FOLDER_LIMIT");
        let mut ids = std::collections::BTreeSet::new();
        let mut states = std::collections::BTreeSet::new();
        for folder in &config.folders {
            ensure!(
                ids.insert(&folder.id) && states.insert(&folder.state),
                "DUPLICATE_DESKTOP_FOLDER"
            );
        }
        fs::create_dir_all(&config.data_directory)?;
        fs::create_dir_all(&config.default_mount_directory)?;
        if config.format_version == 2 {
            let backup = path.with_extension("pre-wave6.toml");
            if !backup.exists() {
                fs::copy(path, backup)?;
            }
        }
        config.format_version = 3;
        config.save(path)?;
        Ok(config)
    }
    pub fn save(&self, path: &Path) -> Result<()> {
        use std::io::Write;
        let temporary = path.with_extension(format!("{}.tmp", crate::core::id()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(toml::to_string_pretty(self)?.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(temporary, path)?;
        Ok(())
    }
}

pub struct Desktop {
    pub config_path: PathBuf,
    pub config: Config,
    executable: PathBuf,
    children: BTreeMap<String, Child>,
    errors: BTreeMap<String, String>,
    blocked: bool,
    _owner: File,
}
impl Desktop {
    pub fn open(path: &Path, single: Option<&Path>, executable: &Path) -> Result<Self> {
        let path = std::path::absolute(path)?;
        fs::create_dir_all(path.parent().context("CONFIG_PARENT_REQUIRED")?)?;
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("desktop.lock"))?;
        #[cfg(windows)]
        owner
            .try_lock()
            .context("DESKTOP_ALREADY_OPEN: another window manages this configuration")?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            ensure!(
                unsafe { libc::flock(owner.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
                "DESKTOP_ALREADY_OPEN: another window manages this configuration"
            );
        }
        let config = Config::load_or_init(&path, single)?;
        let blocked = path.with_extension("blocked.json").exists();
        Ok(Self {
            config_path: path,
            config,
            executable: executable.to_owned(),
            children: BTreeMap::new(),
            errors: BTreeMap::new(),
            blocked,
            _owner: owner,
        })
    }
    pub fn folder(&self, id: &str) -> Result<Folder> {
        self.config
            .folders
            .iter()
            .find(|folder| folder.id == id)
            .cloned()
            .context("FOLDER_NOT_FOUND")
    }
    fn update(&mut self, folder: Folder) -> Result<()> {
        let old = self.config.clone();
        let item = self
            .config
            .folders
            .iter_mut()
            .find(|item| item.id == folder.id)
            .context("FOLDER_NOT_FOUND")?;
        *item = folder;
        if let Err(error) = self.config.save(&self.config_path) {
            self.config = old;
            return Err(error);
        }
        Ok(())
    }
    fn command(&self) -> Command {
        let mut command = Command::new(&self.executable);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        command.stdin(Stdio::null());
        command
    }
    /// A timed-out CLI may be waiting for a daemon callback. Never wait forever
    /// on its termination or start more workers after a stranded process.
    fn run(&mut self, folder: &Folder, arguments: &[String], budget: Duration) -> Result<Value> {
        let cleanup_or_status = arguments.first().is_some_and(|a| a == "rpc")
            && arguments.get(1).is_some_and(|op| {
                matches!(op.as_str(), "status" | "shutdown" | "desktop-snapshot")
            });
        ensure!(
            !self.blocked || cleanup_or_status,
            "DESKTOP_CLEANUP_BLOCKED: see desktop-exit-state.json"
        );
        let directory = folder.state.join("desktop-requests");
        fs::create_dir_all(&directory)?;
        let output = directory.join(format!("{}.json", crate::core::id()));
        let errors = output.with_extension("error");
        let mut command = self.command();
        command
            .arg("--state")
            .arg(&folder.state)
            .args(arguments)
            .stdout(File::create(&output)?)
            .stderr(File::create(&errors)?);
        let mut child = command.spawn()?;
        let end = Instant::now() + budget;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= end {
                let cleanup = terminate(&mut child);
                let stranded = cleanup.is_err();
                self.blocked |= stranded;
                if stranded {
                    fs::write(
                        self.config_path.with_extension("blocked.json"),
                        serde_json::to_vec_pretty(
                            &json!({"pid":child.id(),"state":folder.state,"stranded":true}),
                        )?,
                    )?;
                }
                fs::write(
                    folder.state.join("desktop-exit-state.json"),
                    serde_json::to_vec_pretty(
                        &json!({"pid":child.id(),"operation":arguments.first(),"timed_out":true,"stranded":stranded,"error":cleanup.err().map(|e|e.to_string())}),
                    )?,
                )?;
                anyhow::bail!(
                    "DESKTOP_COMMAND_TIMEOUT: {}; logs {}",
                    arguments.first().unwrap_or(&String::new()),
                    errors.display()
                );
            }
            thread::sleep(Duration::from_millis(10));
        };
        let stdout = fs::read(&output)?;
        let stderr = fs::read_to_string(&errors)?;
        fs::remove_file(&output)?;
        fs::remove_file(&errors)?;
        ensure!(status.success(), "{}", stderr.trim());
        if stdout.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(serde_json::from_slice(&stdout)?)
        }
    }
    pub fn request(&mut self, id: &str, operation: &str, payload: Value) -> Result<Value> {
        let folder = self.folder(id)?;
        self.run(
            &folder,
            &["rpc".into(), operation.into(), payload.to_string()],
            Duration::from_secs(20),
        )
    }
    pub fn start(&mut self, id: &str) -> Result<()> {
        let result = self.start_inner(id);
        if let Err(error) = &result {
            self.errors.insert(id.into(), format!("{error:#}"));
        }
        result
    }
    fn start_inner(&mut self, id: &str) -> Result<()> {
        ensure!(!self.blocked, "DESKTOP_CLEANUP_BLOCKED");
        let mut folder = self.folder(id)?;
        if self
            .run(
                &folder,
                &["rpc".into(), "status".into(), "{}".into()],
                Duration::from_secs(2),
            )
            .is_ok()
        {
            folder.autostart = true;
            self.update(folder)?;
            self.errors.remove(id);
            return Ok(());
        }
        ensure!(!self.blocked, "DESKTOP_CLEANUP_BLOCKED");
        if let Some(child) = self.children.get_mut(id) {
            ensure!(
                child.try_wait()?.is_some(),
                "FOLDER_START_PENDING: daemon is still alive; no duplicate started"
            );
        }
        // Init is idempotent and validates the current store before daemon launch.
        self.run(&folder, &["init".into()], Duration::from_secs(30))?;
        let log_path = folder.state.join("daemon.log");
        // The worker's Ready line carries its loopback token.
        let log = crate::orchestrator::worker::open_private(
            OpenOptions::new().create(true).append(true),
            &log_path,
        )?;
        let mut command = self.command();
        // Workers never listen for peers: `listen`/`advertise` stay in the
        // configuration for the orchestrator, not the worker command.
        command.arg("--state").arg(&folder.state).arg("daemon");
        if let Some(mount) = &folder.mount
            && folder.mount_enabled
        {
            command.arg("--mount").arg(mount);
        }
        command.stdout(log.try_clone()?).stderr(log);
        self.children.insert(id.into(), command.spawn()?);
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            let child = self.children.get_mut(id).unwrap();
            if let Some(status) = child.try_wait()? {
                anyhow::bail!(
                    "DAEMON_START_FAILED ({status}): {}",
                    fs::read_to_string(&log_path)?
                        .chars()
                        .rev()
                        .take(2500)
                        .collect::<String>()
                        .chars()
                        .rev()
                        .collect::<String>()
                );
            }
            if let Ok(status) = self.run(
                &folder,
                &["rpc".into(), "status".into(), "{}".into()],
                Duration::from_secs(2),
            ) {
                if let Some(endpoint) = status["endpoint"].as_str()
                    && folder.advertise.is_none()
                {
                    folder.listen = endpoint
                        .strip_prefix("ws://")
                        .unwrap_or(&folder.listen)
                        .into();
                }
                folder.autostart = true;
                self.update(folder)?;
                self.errors.remove(id);
                return Ok(());
            }
            if self.blocked || Instant::now() >= end {
                let child = self.children.get_mut(id).unwrap();
                let cleanup = terminate(child);
                self.blocked |= cleanup.is_err();
                if self.blocked {
                    fs::write(
                        self.config_path.with_extension("blocked.json"),
                        serde_json::to_vec_pretty(
                            &json!({"pid":child.id(),"state":folder.state,"stranded":true}),
                        )?,
                    )?;
                }
                fs::write(
                    folder.state.join("desktop-exit-state.json"),
                    serde_json::to_vec_pretty(
                        &json!({"pid":child.id(),"startup_timeout":true,"stranded":self.blocked}),
                    )?,
                )?;
                anyhow::bail!("DAEMON_START_TIMEOUT: see {}", log_path.display());
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
    pub fn autostart(&mut self) {
        let ids: Vec<_> = self
            .config
            .folders
            .iter()
            .filter(|f| f.autostart)
            .map(|f| f.id.clone())
            .collect();
        for id in ids {
            if self.blocked {
                break;
            }
            if let Err(error) = self.start(&id) {
                self.errors.insert(id, format!("{error:#}"));
            }
        }
    }
    pub fn stop(&mut self, id: &str) -> Result<()> {
        let mut folder = self.folder(id)?;
        folder.autostart = false;
        self.update(folder.clone())?;
        let started = Instant::now();
        let observed = if self.children.contains_key(id) {
            None
        } else {
            let status = self.run(
                &folder,
                &["rpc".into(), "status".into(), "{}".into()],
                Duration::from_secs(2),
            )?;
            Some(ObservedProcess::open(
                status["pid"].as_u64().context("DAEMON_PID_REQUIRED")? as u32,
            )?)
        };
        // An explicit desktop stop proceeds with files open, as it always has;
        // the worker's `BUSY_VIEW` refusal is for unforced stops.
        let result = self.request(id, "shutdown", json!({"force": true}));
        // A shutdown RPC may report a durability failure. Still wait for exit,
        // but preserve that error even if the daemon subsequently exits cleanly.
        if let Some(child) = self.children.get_mut(id) {
            while child.try_wait()?.is_none() && started.elapsed() < Duration::from_secs(20) {
                thread::sleep(Duration::from_millis(20));
            }
            if child.try_wait()?.is_none() {
                let cleanup = terminate(child);
                self.blocked |= cleanup.is_err();
                if self.blocked {
                    fs::write(
                        self.config_path.with_extension("blocked.json"),
                        serde_json::to_vec_pretty(
                            &json!({"pid":child.id(),"state":folder.state,"stranded":true}),
                        )?,
                    )?;
                }
                fs::write(
                    folder.state.join("desktop-exit-state.json"),
                    serde_json::to_vec_pretty(
                        &json!({"pid":child.id(),"graceful":false,"stranded":self.blocked}),
                    )?,
                )?;
                anyhow::bail!(
                    "DAEMON_STOP_TIMEOUT: forced termination; inspect {}",
                    folder.state.display()
                );
            }
            let exit = child.try_wait()?.context("DAEMON_EXIT_UNCONFIRMED")?;
            self.children.remove(id);
            ensure!(
                exit.success(),
                "DAEMON_SHUTDOWN_FAILED ({exit}): see {}",
                folder.state.join("daemon.log").display()
            );
        } else {
            // Hold the actual process identity across shutdown. A removed
            // control file alone cannot establish termination of a mounted server.
            let observed = observed.unwrap();
            while !observed.exited()? && started.elapsed() < Duration::from_secs(20) {
                thread::sleep(Duration::from_millis(20));
            }
            if !observed.exited()? {
                self.blocked = true;
                fs::write(
                    self.config_path.with_extension("blocked.json"),
                    serde_json::to_vec_pretty(
                        &json!({"pid":observed.pid,"state":folder.state,"stranded":true}),
                    )?,
                )?;
                fs::write(
                    folder.state.join("desktop-exit-state.json"),
                    serde_json::to_vec_pretty(
                        &json!({"pid":observed.pid,"graceful":false,"stranded":true,"owned":false}),
                    )?,
                )?;
                anyhow::bail!("DAEMON_STOP_UNCONFIRMED: no further starts until exit is verified");
            }
            observed.success().context("DAEMON_SHUTDOWN_FAILED")?;
        }
        result?;
        self.errors.remove(id);
        Ok(())
    }
    pub fn create(
        &mut self,
        label: &str,
        repo: Option<&str>,
        mount: Option<PathBuf>,
    ) -> Result<String> {
        ensure!(!label.trim().is_empty(), "FOLDER_NAME_REQUIRED");
        ensure!(self.config.folders.len() < 64, "DESKTOP_FOLDER_LIMIT");
        self.check_mount_point(None, mount.as_deref())?;
        let id = crate::core::id();
        let folder = Folder {
            id: id.clone(),
            label: label.trim().into(),
            state: self.config.data_directory.join(&id),
            mount,
            mount_enabled: true,
            autostart: false,
            listen: listen(),
            advertise: None,
        };
        let mut args = vec!["init".into()];
        if let Some(repo) = repo {
            args.extend(["--repo".into(), repo.into()]);
        }
        self.run(&folder, &args, Duration::from_secs(30))?;
        self.config.folders.push(folder);
        if let Err(error) = self.config.save(&self.config_path) {
            self.config.folders.pop();
            return Err(error);
        }
        self.start(&id)?;
        self.request(&id, "set-label", json!({"label":label}))?;
        Ok(id)
    }
    pub fn configure(
        &mut self,
        id: &str,
        mount: Option<PathBuf>,
        listen: String,
        advertise: Option<String>,
    ) -> Result<()> {
        let mut folder = self.folder(id)?;
        // Mount changes use the qualified whole-daemon stop/start lifecycle.
        ensure!(!folder.autostart, "STOP_FOLDER_BEFORE_CONFIGURING");
        ensure!(
            self.run(
                &folder,
                &["rpc".into(), "status".into(), "{}".into()],
                Duration::from_secs(2)
            )
            .is_err(),
            "STOP_FOLDER_BEFORE_CONFIGURING"
        );
        self.check_mount_point(Some(id), mount.as_deref())?;
        folder.mount = mount;
        folder.mount_enabled = folder.mount.is_some();
        folder.listen = listen;
        folder.advertise = advertise;
        self.update(folder)
    }
    /// Reject mount points another folder uses, the managed directories, or
    /// (on Windows) any existing path: WinFsp creates the mount directory.
    fn check_mount_point(&self, id: Option<&str>, mount: Option<&Path>) -> Result<()> {
        let Some(mount) = mount else { return Ok(()) };
        for directory in [
            &self.config.data_directory,
            &self.config.default_mount_directory,
        ] {
            ensure!(
                !mount
                    .as_os_str()
                    .eq_ignore_ascii_case(directory.as_os_str()),
                "MOUNT_POINT_RESERVED: {} is managed by Telekinesis; use a new path inside it, e.g. {}",
                mount.display(),
                self.config
                    .default_mount_directory
                    .join("<folder name>")
                    .display()
            );
        }
        ensure!(
            !self
                .config
                .folders
                .iter()
                .any(|folder| Some(folder.id.as_str()) != id
                    && folder.mount.as_deref().is_some_and(|other| other
                        .as_os_str()
                        .eq_ignore_ascii_case(mount.as_os_str()))),
            "MOUNT_POINT_IN_USE: another folder already mounts at {}",
            mount.display()
        );
        #[cfg(windows)]
        crate::mount::ensure_mount_point_free(mount)?;
        Ok(())
    }
    pub fn set_mount_enabled(&mut self, id: &str, enabled: bool) -> Result<()> {
        ensure!(!self.blocked, "DESKTOP_CLEANUP_BLOCKED");
        let mut folder = self.folder(id)?;
        ensure!(folder.mount.is_some(), "CONFIGURE_MOUNT_FIRST");
        self.stop(id)?;
        folder.autostart = false;
        folder.mount_enabled = enabled;
        self.update(folder)?;
        self.start(id)
    }
    pub fn snapshot(&mut self, selected: &str) -> Value {
        let mut rows = Vec::new();
        let mut detail = Value::Null;
        for folder in self.config.folders.clone() {
            let result = self.run(
                &folder,
                &[
                    "rpc".into(),
                    if folder.id == selected {
                        "desktop-snapshot"
                    } else {
                        "status"
                    }
                    .into(),
                    "{}".into(),
                ],
                Duration::from_secs(2),
            );
            match result {
                Ok(value) => {
                    let status = value.get("status").unwrap_or(&value).clone();
                    if folder.id == selected {
                        detail = value;
                    }
                    rows.push(json!({"folder":folder,"online":true,"status":status}));
                }
                Err(error) => {
                    let message = self
                        .errors
                        .get(&folder.id)
                        .cloned()
                        .or_else(|| folder.autostart.then(|| format!("{error:#}")));
                    rows.push(json!({"folder":folder,"online":false,"error":message}));
                }
            }
        }
        json!({"config":self.config,"config_path":self.config_path,"folders":rows,"detail":detail,"selected":selected,"blocked":self.blocked})
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn first_launch_initializes_and_reopen_preserves_folders_and_settings() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("orchestrator.toml");
        let state = root.path().join("existing-folder");
        let mut config = Config::load_or_init(&path, Some(&state))?;
        assert!(config.data_directory.is_dir());
        assert!(config.default_mount_directory.is_dir());
        config.folders[0].autostart = false;
        config.folders[0].label = "My files".into();
        config.save(&path)?;
        let reopened = Config::load_or_init(&path, Some(&state))?;
        assert_eq!(reopened.folders.len(), 1);
        assert_eq!(reopened.folders[0].label, "My files");
        assert!(!reopened.folders[0].autostart);
        Ok(())
    }
    #[test]
    fn invalid_configuration_is_reported_without_overwriting_it() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("orchestrator.toml");
        fs::write(&path, "broken configuration")?;
        assert!(Config::load_or_init(&path, None).is_err());
        assert_eq!(fs::read_to_string(path)?, "broken configuration");
        Ok(())
    }
}
