use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
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
}
fn default_limit() -> usize {
    8
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Network {
    pub listen: Option<String>,
    pub connections: usize,
    pub sync_interval_ms: u64,
}
impl Default for Network {
    fn default() -> Self {
        Self {
            listen: Some("127.0.0.1:43180".into()),
            connections: 4,
            sync_interval_ms: 500,
        }
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path).context("CONFIG_NOT_FOUND: run tkfs init -f CONFIG")?;
        let mut config: Self = toml::from_str(&fs::read_to_string(&path)?).context(
            "INVALID_CONFIG: this build requires the folder-first format_version=2 configuration",
        )?;
        ensure!(config.format_version == 2, "CONFIG_VERSION_2_REQUIRED");
        ensure!(
            (1..=64).contains(&config.maximum_running),
            "INVALID_WORKER_LIMIT"
        );
        ensure!(
            (1..=16).contains(&config.network.connections),
            "INVALID_CONNECTION_COUNT"
        );
        ensure!(
            (50..=60000).contains(&config.network.sync_interval_ms),
            "INVALID_SYNC_INTERVAL"
        );
        for directory in [
            &mut config.data_directory,
            &mut config.default_mount_directory,
        ] {
            ensure!(!directory.as_os_str().is_empty(), "EMPTY_DIRECTORY");
            if directory.is_relative() {
                *directory = path.parent().unwrap().join(&*directory);
            }
            *directory = std::path::absolute(&*directory)?;
        }
        if config.network.listen.as_deref() == Some("") {
            config.network.listen = None;
        }
        Ok(config)
    }
    pub fn initialize(path: &Path) -> Result<Self> {
        let path = std::path::absolute(path)?;
        fs::create_dir_all(path.parent().context("CONFIG_PARENT_REQUIRED")?)?;
        let config = Self {
            format_version: 2,
            data_directory: "data".into(),
            default_mount_directory: "mounts".into(),
            maximum_running: 8,
            network: Network::default(),
        };
        use std::io::Write;
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?
            .write_all(toml::to_string_pretty(&config)?.as_bytes())?;
        Self::load(&path)
    }
}
