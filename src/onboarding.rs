//! Shared noninteractive configuration bootstrap and receipt-backed CLI creation.
use crate::{
    core::{hash, id},
    orchestrator::{self, Action, Config, Request, Supervisor},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    installation: String,
    data: PathBuf,
    catalog_created: bool,
}
fn pin_path(config: &Path) -> PathBuf {
    config.with_extension("tkfs-bootstrap.json")
}
fn config_path(path: &Path) -> Result<PathBuf> {
    let path = std::path::absolute(path)?;
    Ok(
        fs::canonicalize(path.parent().context("CONFIG_PARENT_REQUIRED")?)?
            .join(path.file_name().context("CONFIG_FILENAME_REQUIRED")?),
    )
}
fn guard_path(config: &Path) -> PathBuf {
    config.with_extension("tkfs-config.lock")
}
fn configuration_guard(path: &Path) -> Result<fs::File> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(6);
    loop {
        match orchestrator::lock(path) {
            Ok(file) => return Ok(file),
            Err(error)
                if format!("{error:#}").contains("ALREADY_OWNED")
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(25))
            }
            Err(error) => return Err(error),
        }
    }
}
fn verify(config_path: &Path, config: &Config) -> Result<()> {
    if pin_path(config_path).try_exists()? {
        let pin: Pin = serde_json::from_slice(&fs::read(pin_path(config_path))?)?;
        ensure!(
            config.installation_id.as_deref() == Some(&pin.installation)
                && fs::canonicalize(&config.data_directory).ok().as_ref() == Some(&pin.data),
            "BOOTSTRAP_CONFIGURATION_IDENTITY_MISMATCH"
        );
        ensure!(
            config.data_directory.is_dir(),
            "REGISTERED_DATA_DIRECTORY_MISSING"
        );
        if pin.catalog_created {
            ensure!(
                config.data_directory.join("orchestrator.sqlite").is_file(),
                "REGISTERED_CATALOG_MISSING"
            );
        }
    }
    Supervisor::validate_catalog(config)
}
pub fn initialize(path: &Path) -> Result<Config> {
    let path = config_path(path)?;
    bootstrap(
        &path,
        &path.parent().unwrap().join(".tkfs/data"),
        &path.parent().unwrap().join("Projects"),
        8,
        true,
    )
}
pub fn bootstrap(
    path: &Path,
    data: &Path,
    mounts: &Path,
    limit: usize,
    relative: bool,
) -> Result<Config> {
    let path = config_path(path)?;
    let _guard = configuration_guard(&guard_path(&path))
        .context("CONFIG_FOLDER_NOT_WRITABLE_OR_BUSY: choose a writable configuration folder")?;
    if fs::symlink_metadata(&path).is_ok() {
        let config = Config::load(&path)?;
        verify(&path, &config)?;
        return Ok(config);
    }
    ensure!(
        data.is_absolute() && mounts.is_absolute() && (1..=64).contains(&limit),
        "INVALID_BOOTSTRAP_PATHS_OR_LIMIT"
    );
    let data = std::path::absolute(data)?;
    let mounts = std::path::absolute(mounts)?;
    ensure!(
        !data.starts_with(&mounts)
            && !mounts.starts_with(&data)
            && !path.parent().unwrap().starts_with(&data),
        "DATA_FOLDER_OVERLAP"
    );
    if data.try_exists()? {
        ensure!(
            data.is_dir() && fs::read_dir(&data)?.next().is_none(),
            "DATA_FOLDER_MUST_BE_EMPTY"
        );
    }
    let installation = if pin_path(&path).try_exists()? {
        let pin: Pin = serde_json::from_slice(&fs::read(pin_path(&path))?)?;
        ensure!(
            !pin.catalog_created && fs::canonicalize(&data).ok().as_ref() == Some(&pin.data),
            "BOOTSTRAP_CONFIGURATION_MISSING_OR_CONFLICTING"
        );
        pin.installation
    } else {
        id()
    };
    let data_value = if relative {
        PathBuf::from(".tkfs/data")
    } else {
        data.clone()
    };
    let mounts_value = if relative {
        PathBuf::from("Projects")
    } else {
        mounts.clone()
    };
    let text = toml::to_string_pretty(
        &json!({"format_version":1,"installation_id":installation,"data_directory":data_value,
        "default_mount_directory":mounts_value,"control":{"transport":if cfg!(windows){"named-pipe"}else{"unix-socket"},"name":format!("tkfs-{}",id())},
        "network":{"enabled":false},"workers":{"restart_policy":"on-failure","maximum_running":limit}}),
    )?;
    #[cfg(windows)]
    fs::create_dir_all(&data)?;
    #[cfg(target_os = "linux")]
    crate::private_storage::Directory::open(&data)?;
    fs::create_dir_all(&mounts)?;
    let data = fs::canonicalize(data)?;
    let mounts = fs::canonicalize(mounts)?;
    ensure!(
        !data.starts_with(&mounts) && !mounts.starts_with(&data),
        "DATA_FOLDER_OVERLAP"
    );
    let temporary = path.with_extension(format!("{}.tmp", id()));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        crate::core::fault("onboarding_config_ready");
        orchestrator::atomic_json(
            &pin_path(&path),
            &Pin {
                installation: installation.clone(),
                data: data.clone(),
                catalog_created: false,
            },
        )?;
        // Publish without replacing an existing file, even if a foreign writer
        // ignores the bootstrap lock. Temporary and final file share a directory.
        fs::hard_link(&temporary, &path)?;
        #[cfg(target_os = "linux")]
        fs::File::open(path.parent().unwrap())?.sync_all()?;
        crate::core::fault("onboarding_config_published");
        Ok(())
    })();
    let _ = fs::remove_file(&temporary);
    result?;
    let config = Config::load(&path)?;
    Ok(config)
}
pub fn open(path: &Path) -> Result<Supervisor> {
    let path = config_path(path)?;
    let _guard = configuration_guard(&guard_path(&path))?;
    let config = Config::load(&path)?;
    verify(&path, &config)?;
    let supervisor = Supervisor::open(config)?;
    if pin_path(&path).try_exists()? {
        let mut pin: Pin = serde_json::from_slice(&fs::read(pin_path(&path))?)?;
        pin.catalog_created = true;
        orchestrator::atomic_json(&pin_path(&path), &pin)?;
    }
    Ok(supervisor)
}
pub fn create(path: &Path, label: &str, explicit_id: Option<&str>) -> Result<Value> {
    let path = config_path(path)?;
    let config = Config::load(&path)?;
    verify(&path, &config)?;
    ensure!(
        !label.trim().is_empty()
            && label.len() <= 256
            && !label.contains(['\\', '/', ':', '*', '?', '"', '<', '>', '|'])
            && !label.ends_with(['.', ' '])
            && label != "."
            && label != "..",
        "VALID_PROJECT_FOLDER_NAME_REQUIRED"
    );
    let parent = config
        .default_mount_directory
        .as_ref()
        .context("MOUNT_PARENT_REQUIRED")?;
    let mount = fs::canonicalize(parent)?.join(label);
    let action = Action::Create {
        label: label.into(),
        mount: Some(mount),
    };
    let hello = orchestrator::client(
        &config,
        &Request {
            version: 1,
            target_installation: config.installation_id.clone(),
            operation_id: None,
            expected_generation: None,
            action: Action::Hello,
        },
    )?;
    let installation = Some(
        hello["result"]["installation_id"]
            .as_str()
            .context("HELLO_IDENTITY_MISSING")?
            .to_owned(),
    );
    let key = hash(format!("{}\n{}", installation.as_deref().unwrap(), label).as_bytes());
    let journal = config.data_directory.join(format!("cli-create-{key}.json"));
    let _guard = configuration_guard(&journal.with_extension("lock"))?;
    let saved_request = journal.try_exists()?;
    let mut request = if saved_request {
        let request: Request = serde_json::from_slice(&fs::read(&journal)?)?;
        ensure!(
            serde_json::to_value(&request.action)? == serde_json::to_value(&action)?
                && request.target_installation == installation,
            "CLI_CREATE_RECEIPT_ARGUMENT_MISMATCH"
        );
        if let Some(id) = explicit_id {
            ensure!(
                request.operation_id.as_deref() == Some(id),
                "CLI_CREATE_REQUEST_ID_MISMATCH"
            );
        }
        request
    } else {
        let reply = orchestrator::client(
            &config,
            &Request {
                version: 1,
                target_installation: config.installation_id.clone(),
                operation_id: None,
                expected_generation: None,
                action: Action::List,
            },
        )?;
        ensure!(reply["ok"] == true, "CATALOG_UNAVAILABLE");
        let operation = explicit_id.map(str::to_owned).unwrap_or_else(id);
        uuid::Uuid::parse_str(&operation)?;
        let request = Request {
            version: 1,
            target_installation: installation.clone(),
            operation_id: Some(operation),
            expected_generation: Some(
                reply["result"]["catalog_generation"]
                    .as_u64()
                    .context("CATALOG_GENERATION_REQUIRED")?,
            ),
            action,
        };
        orchestrator::atomic_json(&journal, &request)?;
        request
    };
    crate::core::fault("onboarding_create_saved");
    let mut replaced_rejection = false;
    for _ in 0..8 {
        let reply = orchestrator::client(&config, &request).with_context(|| {
            format!(
                "CREATE_DELIVERY_UNCERTAIN: rerun tkfs create {label} to retry the saved request"
            )
        })?;
        if reply["ok"] == true {
            return Ok(reply["result"].clone());
        }
        // A persisted terminal rejection with no committed state intent can be
        // replaced on a subsequent invocation (e.g. corrected mount obstruction).
        // Pending, uncertain and executed requests always retain their exact ID.
        let stale = reply["error"]
            .as_str()
            .is_some_and(|error| error.contains("STALE_MANAGEMENT_GENERATION"));
        let rejected_without_intent =
            saved_request && !replaced_rejection && reply["intent_committed"] == false;
        if explicit_id.is_none()
            && reply["retryable"] == false
            && (stale || rejected_without_intent)
        {
            replaced_rejection = true;
            let catalog = orchestrator::client(
                &config,
                &Request {
                    version: 1,
                    target_installation: installation.clone(),
                    operation_id: None,
                    expected_generation: None,
                    action: Action::List,
                },
            )?;
            ensure!(catalog["ok"] == true, "CATALOG_UNAVAILABLE");
            request.operation_id = Some(id());
            request.expected_generation = Some(
                catalog["result"]["catalog_generation"]
                    .as_u64()
                    .context("CATALOG_GENERATION_REQUIRED")?,
            );
            orchestrator::atomic_json(&journal, &request)?;
            continue;
        }
        anyhow::bail!(
            "CREATE_PENDING_OR_REJECTED: {}; rerun the same command or inspect operation {}",
            reply["error"],
            request.operation_id.as_deref().unwrap()
        );
    }
    anyhow::bail!("CREATE_CONTENTION: retry the same command; latest request is saved")
}
