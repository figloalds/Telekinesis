//! `orchestrator.toml` (format 2).
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format_version: u32,
    pub data_directory: PathBuf,
    pub default_mount_directory: PathBuf,
    #[serde(default = "default_limit")]
    pub maximum_running: usize,
    #[serde(default)]
    pub network: Network,
    #[serde(default)]
    pub freshness: Freshness,
}

fn default_limit() -> usize {
    8
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Network {
    /// Peer listen address; empty disables listening.
    pub listen: String,
    /// Address advertised to peers; empty means none.
    pub advertise: String,
    pub connections: usize,
    pub worker_lanes: usize,
    pub sync_interval_ms: u64,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:43180".into(),
            advertise: String::new(),
            connections: 4,
            worker_lanes: 2,
            sync_interval_ms: 500,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Freshness {
    pub open_wait_ms: u64,
}

impl Default for Freshness {
    fn default() -> Self {
        Self { open_wait_ms: 250 }
    }
}

/// Environment variable naming the orchestrator configuration.
pub const CONFIG_ENV: &str = "TKFS_CONFIG";

/// The one configuration path the GUI and every command share: `explicit`
/// (`-f`), else `env` (`TKFS_CONFIG`, ignored when empty), else
/// `orchestrator.toml` next to the executable on Windows and in `cwd`
/// elsewhere. Relative inputs are joined to `cwd`.
pub fn resolve_path(
    explicit: Option<&Path>,
    env: Option<&std::ffi::OsStr>,
    exe: &Path,
    cwd: &Path,
) -> PathBuf {
    if let Some(path) = explicit {
        return cwd.join(path);
    }
    if let Some(path) = env.filter(|value| !value.is_empty()) {
        return cwd.join(path);
    }
    if cfg!(windows) {
        exe.with_file_name("orchestrator.toml")
    } else {
        cwd.join("orchestrator.toml")
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let path = std::path::absolute(path)?;
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                bail!("CONFIG_NOT_FOUND: {} does not exist", path.display())
            }
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", path.display()));
            }
        };
        let mut config: Self = toml::from_str(&text).map_err(|error| {
            anyhow::anyhow!(
                "INVALID_CONFIG: this build requires the folder-first format_version=2 configuration: {error}"
            )
        })?;
        ensure!(
            config.format_version == 2,
            "CONFIG_VERSION_2_REQUIRED: format_version must be 2, found {}",
            config.format_version
        );
        ensure!(
            (1..=64).contains(&config.maximum_running),
            "INVALID_WORKER_LIMIT: maximum_running must be 1..=64"
        );
        ensure!(
            (1..=16).contains(&config.network.connections),
            "INVALID_CONNECTION_COUNT: network.connections must be 1..=16"
        );
        ensure!(
            (1..=8).contains(&config.network.worker_lanes),
            "INVALID_WORKER_LANES: network.worker_lanes must be 1..=8"
        );
        ensure!(
            (50..=60000).contains(&config.network.sync_interval_ms),
            "INVALID_SYNC_INTERVAL: network.sync_interval_ms must be 50..=60000"
        );
        ensure!(
            config.freshness.open_wait_ms <= 5000,
            "INVALID_OPEN_WAIT: freshness.open_wait_ms must be 0..=5000"
        );
        let parent = path
            .parent()
            .context("CONFIG_PARENT_REQUIRED: configuration path has no parent")?;
        for directory in [
            &mut config.data_directory,
            &mut config.default_mount_directory,
        ] {
            ensure!(
                !directory.as_os_str().is_empty(),
                "EMPTY_DIRECTORY: configured directories must not be empty"
            );
            if directory.is_relative() {
                *directory = parent.join(&*directory);
            }
            *directory = std::path::absolute(&*directory)?;
        }
        Ok(config)
    }

    pub fn initialize(path: &Path) -> Result<Self> {
        let path = std::path::absolute(path)?;
        fs::create_dir_all(
            path.parent()
                .context("CONFIG_PARENT_REQUIRED: configuration path has no parent")?,
        )?;
        let config = Self {
            format_version: 2,
            data_directory: "data".into(),
            default_mount_directory: "mounts".into(),
            maximum_running: default_limit(),
            network: Network::default(),
            freshness: Freshness::default(),
        };
        let mut file = match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                bail!("CONFIG_EXISTS: {} already exists", path.display())
            }
            Err(error) => {
                return Err(error).with_context(|| format!("creating {}", path.display()));
            }
        };
        file.write_all(toml::to_string_pretty(&config)?.as_bytes())?;
        drop(file);
        Self::load(&path)
    }

    pub fn load_or_init(path: &Path) -> Result<Self> {
        if path.exists() {
            Self::load(path)
        } else {
            Self::initialize(path)
        }
    }

    pub fn listen(&self) -> Option<&str> {
        Some(self.network.listen.as_str()).filter(|value| !value.is_empty())
    }

    pub fn advertise(&self) -> Option<&str> {
        Some(self.network.advertise.as_str()).filter(|value| !value.is_empty())
    }

    pub fn pool(&self) -> usize {
        self.network.connections.max(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("orchestrator.toml");
        std::fs::write(&path, body).unwrap();
        path
    }

    fn err(result: Result<Config>) -> String {
        format!("{:#}", result.unwrap_err())
    }

    const BASE: &str = "format_version = 2\ndata_directory = 'd'\ndefault_mount_directory = 'm'\n";

    #[test]
    fn resolve_path_prefers_explicit_then_env_then_platform_default() {
        use std::ffi::OsStr;
        let cwd = Path::new(r"C:\work");
        let exe = Path::new(r"C:\Tools\tkfs\tkfs.exe");
        let env = |value| Some(OsStr::new(value));
        assert_eq!(
            resolve_path(Some(Path::new("a.toml")), env("b.toml"), exe, cwd),
            cwd.join("a.toml")
        );
        assert_eq!(resolve_path(None, env("b.toml"), exe, cwd), cwd.join("b.toml"));
        assert_eq!(
            resolve_path(None, env(""), exe, cwd),
            resolve_path(None, None, exe, cwd)
        );
        #[cfg(windows)]
        assert_eq!(
            resolve_path(None, None, exe, cwd),
            PathBuf::from(r"C:\Tools\tkfs\orchestrator.toml")
        );
        #[cfg(not(windows))]
        assert_eq!(resolve_path(None, None, exe, cwd), cwd.join("orchestrator.toml"));
    }

    #[test]
    fn initialize_writes_defaults_and_refuses_to_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orchestrator.toml");
        Config::initialize(&path).unwrap();
        assert!(err(Config::initialize(&path)).starts_with("CONFIG_EXISTS"));
        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.maximum_running, 8);
        assert_eq!(loaded.network.worker_lanes, 2);
        assert_eq!(loaded.network.connections, 4);
        assert_eq!(loaded.network.sync_interval_ms, 500);
        assert_eq!(loaded.network.listen, "127.0.0.1:43180");
        assert_eq!(loaded.network.advertise, "");
        assert_eq!(loaded.freshness.open_wait_ms, 250);
        assert_eq!(loaded.data_directory, dir.path().join("data"));
        assert_eq!(loaded.default_mount_directory, dir.path().join("mounts"));
        let again = Config::load_or_init(&path).unwrap();
        assert_eq!(again.maximum_running, 8);
    }

    #[test]
    fn load_or_init_creates_a_missing_file_and_load_reports_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("orchestrator.toml");
        assert!(err(Config::load(&path)).starts_with("CONFIG_NOT_FOUND"));
        Config::load_or_init(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn unknown_field_is_invalid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(dir.path(), &format!("{BASE}bogus = 1\n"));
        assert!(err(Config::load(&path)).starts_with("INVALID_CONFIG"));
    }

    #[test]
    fn format_3_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            "format_version = 3\ndata_directory = 'd'\ndefault_mount_directory = 'm'\n",
        );
        assert!(err(Config::load(&path)).starts_with("CONFIG_VERSION_2_REQUIRED"));
    }

    #[test]
    fn relative_directories_resolve_against_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        let path = write(
            &sub,
            "format_version = 2\ndata_directory = 'data'\ndefault_mount_directory = 'mounts'\n",
        );
        let config = Config::load(&path).unwrap();
        assert_eq!(config.data_directory, sub.join("data"));
        assert_eq!(config.default_mount_directory, sub.join("mounts"));
        assert!(config.data_directory.is_absolute());
        // Absolute directories are kept as given.
        let absolute = dir.path().join("elsewhere");
        let body = format!(
            "format_version = 2\ndata_directory = {:?}\ndefault_mount_directory = 'm'\n",
            absolute.to_str().unwrap()
        );
        let path = write(&sub, &body);
        assert_eq!(Config::load(&path).unwrap().data_directory, absolute);
    }

    #[test]
    fn ranges_are_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let check = |extra: &str, code: &str| {
            let path = write(dir.path(), &format!("{BASE}{extra}"));
            let message = err(Config::load(&path));
            assert!(message.starts_with(code), "{extra:?} -> {message}");
        };
        check("maximum_running = 0\n", "INVALID_WORKER_LIMIT");
        check("maximum_running = 65\n", "INVALID_WORKER_LIMIT");
        check("[network]\nconnections = 0\n", "INVALID_CONNECTION_COUNT");
        check("[network]\nconnections = 17\n", "INVALID_CONNECTION_COUNT");
        check("[network]\nworker_lanes = 0\n", "INVALID_WORKER_LANES");
        check("[network]\nworker_lanes = 9\n", "INVALID_WORKER_LANES");
        check(
            "[network]\nsync_interval_ms = 49\n",
            "INVALID_SYNC_INTERVAL",
        );
        check("[freshness]\nopen_wait_ms = 5001\n", "INVALID_OPEN_WAIT");

        let path = write(
            dir.path(),
            &format!("{BASE}[network]\nworker_lanes = 8\n[freshness]\nopen_wait_ms = 5000\n"),
        );
        let config = Config::load(&path).unwrap();
        assert_eq!(config.network.worker_lanes, 8);
        assert_eq!(config.freshness.open_wait_ms, 5000);

        let path = write(dir.path(), &format!("{BASE}[network]\nconnections = 1\n"));
        assert_eq!(Config::load(&path).unwrap().pool(), 2);
        let path = write(dir.path(), &format!("{BASE}[network]\nconnections = 6\n"));
        assert_eq!(Config::load(&path).unwrap().pool(), 6);
    }

    #[test]
    fn empty_listen_and_advertise_mean_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = write(
            dir.path(),
            &format!("{BASE}[network]\nlisten = ''\nadvertise = ''\n"),
        );
        let config = Config::load(&path).unwrap();
        assert_eq!(config.listen(), None);
        assert_eq!(config.advertise(), None);
        let path = write(
            dir.path(),
            &format!("{BASE}[network]\nlisten = '0.0.0.0:1'\nadvertise = 'host:2'\n"),
        );
        let config = Config::load(&path).unwrap();
        assert_eq!(config.listen(), Some("0.0.0.0:1"));
        assert_eq!(config.advertise(), Some("host:2"));
    }
}
