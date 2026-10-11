//! Owner-local supervision of a preprovisioned outbound pairing service.
//! `running` means the service loop owns its registry, not that any grant is
//! caught up. Existing enabled grants are the complete scope of this runner.
use crate::{
    pairing::Identity,
    pairing_service::{Config, Service, Transport},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

const DRAIN: Duration = Duration::from_secs(40);
const RECHECK: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    #[default]
    Outbound,
    Receiver,
}

/// Public metadata only; never contains an enrollment code or a private key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    #[serde(default)]
    pub role: Role,
    #[serde(default)]
    pub orchestrator_installation: Option<String>,
    pub format_version: u32,
    pub config_path: PathBuf,
    pub data_directory: PathBuf,
    pub installation: String,
    pub credential_fingerprint: String,
    pub configuration_hash: String,
}
impl Binding {
    pub fn load(path: &Path) -> Result<Self> {
        Self::inspect(path, Role::Outbound).map(|(binding, _, _)| binding)
    }
    pub fn load_receiver(path: &Path, orchestrator: &Path) -> Result<Self> {
        let (binding, config, _) = Self::inspect(path, Role::Receiver)?;
        ensure!(
            config.orchestrator_config.as_deref()
                == Some(std::fs::canonicalize(orchestrator)?.as_path()),
            "MANAGED_PAIRING_BINDING_CHANGED"
        );
        Ok(binding)
    }
    fn inspect(path: &Path, role: Role) -> Result<(Self, Config, Identity)> {
        let path = std::fs::canonicalize(path)
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_CONFIG_UNAVAILABLE"))?;
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("MANAGED_PAIRING_CONFIG_UNAVAILABLE"))?;
        let directory = crate::private_storage::Directory::open(parent)
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_STORAGE_UNSAFE"))?;
        let _file = directory
            .file(&path, false, false)
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_STORAGE_UNSAFE"))?;
        let mut config = Config::load(&path)
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_CONFIG_UNAVAILABLE"))?;
        let mut orchestrator_installation = None;
        if role == Role::Outbound {
            ensure!(
                config.transport == Transport::Wss
                    && !config.inbound
                    && config.dial
                    && config.listen.is_empty()
                    && config.enrollment_listen.is_empty()
                    && config.enrollment_advertise.is_empty()
                    && config.advertise == "outbound-only",
                "MANAGED_PAIRING_OUTBOUND_WSS_REQUIRED"
            );
        } else {
            ensure!(
                config.transport == Transport::Wss
                    && config.inbound
                    && config.dial
                    && config.orchestrator_config.is_some()
                    && config.advertise.starts_with("wss://")
                    && !config.listen.is_empty(),
                "MANAGED_PAIRING_RECEIVER_WSS_REQUIRED"
            );
            let owner = config.orchestrator_config.as_ref().unwrap();
            orchestrator_installation =
                Some(crate::orchestrator::Config::load(owner)?.existing_installation()?);
        }
        config.data_directory = std::fs::canonicalize(&config.data_directory)
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_REGISTRY_UNAVAILABLE"))?;
        // Read existing trust without initializing or repairing a registry.
        let storage = crate::private_storage::Directory::open(&config.data_directory)
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_STORAGE_UNSAFE"))?;
        storage
            .validate_existing_files()
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_STORAGE_UNSAFE"))?;
        let database = config.data_directory.join("pairs.sqlite");
        let _database = storage
            .file(&database, false, false)
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_REGISTRY_UNAVAILABLE"))?;
        let check_registry = (|| -> Result<()> {
            let db = rusqlite::Connection::open_with_flags(
                &database,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            let (count, identity): (u64, String) =
                db.query_row("SELECT COUNT(*),MIN(id) FROM identity", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
            ensure!(
                count == 1 && identity == config.installation,
                "registry binding"
            );
            db.prepare("SELECT id,certificate,endpoint,revoked FROM pairs LIMIT 0")?;
            db.prepare("SELECT peer,repo,payload,status FROM syncs LIMIT 0")?;
            Ok(())
        })();
        check_registry.map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_REGISTRY_UNAVAILABLE"))?;
        let identity = config
            .identity()
            .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_CREDENTIAL_UNAVAILABLE"))?;
        let configuration_hash = crate::core::hash(
            &serde_json::to_vec(&config)
                .map_err(|_| anyhow::anyhow!("MANAGED_PAIRING_CONFIG_UNAVAILABLE"))?,
        );
        Ok((
            Self {
                role,
                orchestrator_installation,
                format_version: 1,
                config_path: path,
                data_directory: config.data_directory.clone(),
                installation: config.installation.clone(),
                credential_fingerprint: identity.fingerprint(),
                configuration_hash,
            },
            config,
            identity,
        ))
    }
    fn checked(&self) -> Result<(Config, Identity)> {
        let (current, config, identity) = Self::inspect(&self.config_path, self.role)?;
        ensure!(self == &current, "MANAGED_PAIRING_BINDING_CHANGED");
        Ok((config, identity))
    }
}

enum Outcome {
    Stopped,
    StartFailed(&'static str),
    UncertainDrain(Option<Service>),
}
struct Runner {
    binding: Binding,
    stop: Arc<AtomicBool>,
    started: mpsc::Receiver<()>,
    thread: JoinHandle<Outcome>,
    stopping: bool,
}
// Service errors may leave detached per-grant workers in its existing transport.
// Never relaunch for that registry in this process after an uncertain drain.
// A timed-out outer worker retains ownership here rather than losing its handle.
#[derive(Default)]
struct Quarantined {
    _runner: Option<Runner>,
    _service: Option<Service>,
}
static QUARANTINE: OnceLock<Mutex<BTreeMap<PathBuf, Quarantined>>> = OnceLock::new();
fn quarantine() -> &'static Mutex<BTreeMap<PathBuf, Quarantined>> {
    QUARANTINE.get_or_init(|| Mutex::new(BTreeMap::new()))
}
fn failure(error: &anyhow::Error) -> &'static str {
    match error.to_string().as_str() {
        "MANAGED_PAIRING_CONFIG_UNAVAILABLE" => "MANAGED_PAIRING_CONFIG_UNAVAILABLE",
        "MANAGED_PAIRING_STORAGE_UNSAFE" => "MANAGED_PAIRING_STORAGE_UNSAFE",
        "MANAGED_PAIRING_REGISTRY_UNAVAILABLE" => "MANAGED_PAIRING_REGISTRY_UNAVAILABLE",
        "MANAGED_PAIRING_CREDENTIAL_UNAVAILABLE" => "MANAGED_PAIRING_CREDENTIAL_UNAVAILABLE",
        "MANAGED_PAIRING_OUTBOUND_WSS_REQUIRED" => "MANAGED_PAIRING_OUTBOUND_WSS_REQUIRED",
        "MANAGED_PAIRING_BINDING_CHANGED" => "MANAGED_PAIRING_BINDING_CHANGED",
        _ => "MANAGED_PAIRING_SERVICE_UNAVAILABLE",
    }
}

pub struct Controller {
    desired: Option<Binding>,
    runner: Option<Runner>,
    state: &'static str,
    error: Option<&'static str>,
    next_attempt: Instant,
    backoff: Duration,
    next_check: Instant,
    uncertain: bool,
}
impl Default for Controller {
    fn default() -> Self {
        Self::new()
    }
}
impl Controller {
    #[cfg(test)]
    pub(crate) fn simulate_uncertain_drain(&mut self) {
        assert!(self.runner.is_none(), "test hook requires an idle fixture");
        self.uncertain = true;
        self.state = "unavailable";
        self.error = Some("MANAGED_PAIRING_DRAIN_UNCERTAIN");
    }
    pub fn new() -> Self {
        Self {
            desired: None,
            runner: None,
            state: "disabled",
            error: None,
            next_attempt: Instant::now(),
            backoff: Duration::from_secs(1),
            next_check: Instant::now(),
            uncertain: false,
        }
    }
    fn stop(&mut self) {
        if let Some(runner) = &mut self.runner {
            runner.stopping = true;
            runner.stop.store(true, Ordering::Relaxed);
            self.state = "stopping";
        }
    }
    fn unavailable(&mut self, reason: &'static str) {
        self.error = Some(reason);
        self.state = if self.runner.is_some() {
            "stopping"
        } else {
            "unavailable"
        };
        self.next_attempt = Instant::now() + self.backoff;
        self.backoff = (self.backoff * 2).min(Duration::from_secs(60));
    }
    fn harvest(&mut self) {
        if let Some(runner) = &self.runner {
            if runner.started.try_recv().is_ok() && !runner.stopping {
                self.state = "running";
                self.error = None;
                self.backoff = Duration::from_secs(1);
                self.next_check = Instant::now() + RECHECK;
            }
            if !runner.thread.is_finished() {
                return;
            }
        } else {
            return;
        }
        let runner = self.runner.take().unwrap();
        let outcome = runner
            .thread
            .join()
            .unwrap_or(Outcome::UncertainDrain(None));
        match outcome {
            Outcome::Stopped if runner.stopping => {
                self.state = if self.error.is_some() {
                    "unavailable"
                } else {
                    "disabled"
                };
            }
            Outcome::Stopped => self.unavailable("MANAGED_PAIRING_SERVICE_EXITED"),
            Outcome::StartFailed(reason) if !runner.stopping => self.unavailable(reason),
            Outcome::StartFailed(_) => {
                self.state = if self.error.is_some() {
                    "unavailable"
                } else {
                    "disabled"
                };
            }
            Outcome::UncertainDrain(service) => {
                quarantine()
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(
                        runner.binding.data_directory,
                        Quarantined {
                            _runner: None,
                            _service: service,
                        },
                    );
                self.uncertain = true;
                self.unavailable("MANAGED_PAIRING_DRAIN_UNCERTAIN");
            }
        }
    }
    /// Nonblocking desired-state reconciliation. Operational failures are exposed
    /// as sanitized status; they do not terminate the owning local supervisor.
    pub fn reconcile(&mut self, desired: Option<&Binding>) -> Result<()> {
        if self.desired.as_ref() != desired {
            self.desired = desired.cloned();
            self.error = None;
            self.backoff = Duration::from_secs(1);
            self.next_attempt = Instant::now();
            self.stop();
        }
        self.harvest();
        if self.uncertain {
            self.state = "unavailable";
            self.error = Some("MANAGED_PAIRING_DRAIN_UNCERTAIN");
            return Ok(());
        }
        let Some(binding) = self.desired.clone() else {
            self.stop();
            if self.runner.is_none() {
                self.state = "disabled";
                self.error = None;
            }
            return Ok(());
        };
        if let Some(runner) = &self.runner {
            if !runner.stopping && Instant::now() >= self.next_check {
                self.next_check = Instant::now() + RECHECK;
                if let Err(error) = binding.checked() {
                    self.stop();
                    self.unavailable(failure(&error));
                }
            }
            return Ok(());
        }
        if quarantine()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(&binding.data_directory)
        {
            self.uncertain = true;
            self.unavailable("MANAGED_PAIRING_DRAIN_UNCERTAIN");
            return Ok(());
        }
        if Instant::now() < self.next_attempt {
            return Ok(());
        }
        let stop = Arc::new(AtomicBool::new(false));
        let cancelled = stop.clone();
        let expected = binding.clone();
        let (ready, started) = mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("tkfs-managed-pairing".into())
            .spawn(move || {
                let (config, identity) = match expected.checked() {
                    Ok(value) => value,
                    Err(error) => return Outcome::StartFailed(failure(&error)),
                };
                if cancelled.load(Ordering::Relaxed) {
                    return Outcome::Stopped;
                }
                // Pass the exact validated credential into the service, eliminating
                // a reload race between fingerprint validation and service startup.
                let service = match Service::open_identity(config, Arc::new(identity)) {
                    Ok(service) => service,
                    Err(error) => {
                        return Outcome::StartFailed(
                            if error
                                .to_string()
                                .contains("PAIRING_SERVICE_ALREADY_RUNNING")
                            {
                                "MANAGED_PAIRING_OWNERSHIP_CONFLICT"
                            } else {
                                "MANAGED_PAIRING_SERVICE_UNAVAILABLE"
                            },
                        );
                    }
                };
                let _ = ready.send(());
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    service.run_retained(cancelled.clone())
                })) {
                    Ok(Ok(())) => Outcome::Stopped,
                    Ok(Err(_)) | Err(_) => {
                        cancelled.store(true, Ordering::Relaxed);
                        Outcome::UncertainDrain(Some(service))
                    }
                }
            });
        match spawned {
            Ok(thread) => {
                self.runner = Some(Runner {
                    binding,
                    stop,
                    started,
                    thread,
                    stopping: false,
                });
                self.state = "starting";
                self.error = None;
            }
            Err(_) => self.unavailable("MANAGED_PAIRING_THREAD_UNAVAILABLE"),
        }
        Ok(())
    }
    pub fn status(&self) -> Value {
        json!({"state":self.state,"desired_enabled":self.desired.is_some(),"error":self.error,
            "retry_after_ms":if self.uncertain {None}else{Some(self.next_attempt.saturating_duration_since(Instant::now()).as_millis() as u64)},
            "installation":self.desired.as_ref().map(|b|&b.installation),"config_path":self.desired.as_ref().map(|b|&b.config_path),
            "scope":"all-configured-enabled-grants","sync_health":"inspect-per-grant-status"})
    }
    /// Observe actual service completion before permitting a replacement. A
    /// timeout/error cannot be treated as successful disable or retried in-process.
    pub fn shutdown(&mut self) -> Result<()> {
        self.shutdown_budget(DRAIN)
    }
    pub(crate) fn request_stop(&mut self) {
        self.stop();
    }
    pub(crate) fn shutdown_until(&mut self, deadline: Instant) -> Result<()> {
        self.shutdown_budget(deadline.saturating_duration_since(Instant::now()))
    }
    fn shutdown_budget(&mut self, budget: Duration) -> Result<()> {
        // A previous uncertain drain cannot be made safe by another bounded wait.
        ensure!(!self.uncertain, "MANAGED_PAIRING_DRAIN_UNCERTAIN");
        self.reconcile(None)?;
        let deadline = Instant::now() + budget;
        self.harvest();
        while self.runner.is_some() && Instant::now() < deadline {
            self.harvest();
            if self.runner.is_some() {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        if self.runner.is_some() {
            if let Some(runner) = &self.runner {
                quarantine()
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .entry(runner.binding.data_directory.clone())
                    .or_default();
            }
            self.uncertain = true;
            self.state = "unavailable";
            self.error = Some("MANAGED_PAIRING_DRAIN_UNCERTAIN");
        }
        ensure!(!self.uncertain, "MANAGED_PAIRING_DRAIN_UNCERTAIN");
        self.state = "disabled";
        self.error = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disabled_controller_has_no_files_threads_or_enabled_grants() {
        let mut controller = Controller::new();
        for _ in 0..3 {
            controller.reconcile(None).unwrap();
        }
        assert_eq!(controller.status()["state"], "disabled");
        assert_eq!(controller.status()["desired_enabled"], false);
        assert!(controller.runner.is_none());
        controller.shutdown().unwrap();
    }

    #[cfg(windows)]
    mod windows {
        use super::*;
        use crate::{
            core::{Store, id},
            pairing::{CredentialSource, Peer, SyncConfiguration},
            runtime::{self, Engine},
        };
        struct Fixture {
            _temp: tempfile::TempDir,
            path: PathBuf,
            config: Config,
            identity: Arc<Identity>,
        }
        impl Fixture {
            fn new() -> Self {
                let temp = tempfile::tempdir().unwrap();
                crate::local_ipc::protect_directory(temp.path()).unwrap();
                let identity = Arc::new(Identity::generate(&id()).unwrap());
                let credential = CredentialSource::WindowsDpapi {
                    file: temp.path().join("synthetic.dpapi"),
                };
                credential.save_windows(&identity).unwrap();
                let config = Config {
                    format_version: 1,
                    orchestrator_config: None,
                    installation: identity.installation.clone(),
                    data_directory: temp.path().join("registry"),
                    credential,
                    transport: Transport::Wss,
                    inbound: false,
                    dial: true,
                    listen: String::new(),
                    enrollment_listen: String::new(),
                    advertise: "outbound-only".into(),
                    enrollment_advertise: String::new(),
                };
                config.registry().unwrap();
                let path = temp.path().join("client.toml");
                std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
                Self {
                    _temp: temp,
                    path,
                    config,
                    identity,
                }
            }
            fn save(&self) {
                std::fs::write(&self.path, toml::to_string(&self.config).unwrap()).unwrap();
            }
            fn binding(&self) -> Binding {
                Binding::load(&self.path).unwrap()
            }
        }
        fn wait(controller: &mut Controller, binding: Option<&Binding>, state: &str) {
            let deadline = Instant::now() + Duration::from_secs(8);
            loop {
                controller.reconcile(binding).unwrap();
                if controller.status()["state"] == state {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "controller did not reach expected phase: {}",
                    controller.status()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        #[test]
        fn binding_is_read_only_rejects_inbound_drift_and_missing_credential() {
            let mut fixture = Fixture::new();
            let binding = fixture.binding();
            let serialized = serde_json::to_string(&binding).unwrap();
            assert!(!serialized.contains("private_key") && !serialized.contains("\"code\":"));
            assert_eq!(
                serde_json::from_str::<Binding>(&serialized).unwrap(),
                binding
            );
            fixture.config.inbound = true;
            fixture.config.listen = "127.0.0.1:23456".into();
            fixture.config.advertise = "wss://127.0.0.1:23456/tkfs/sync".into();
            fixture.config.enrollment_advertise = "wss://127.0.0.1:23456/tkfs/enroll".into();
            fixture.save();
            assert_eq!(
                Binding::load(&fixture.path).unwrap_err().to_string(),
                "MANAGED_PAIRING_OUTBOUND_WSS_REQUIRED"
            );
            fixture.config.inbound = false;
            fixture.config.listen.clear();
            fixture.config.advertise = "outbound-only".into();
            fixture.config.enrollment_advertise.clear();
            let alternate = CredentialSource::WindowsDpapi {
                file: fixture._temp.path().join("alternate.dpapi"),
            };
            alternate.save_windows(&fixture.identity).unwrap();
            fixture.config.credential = alternate;
            fixture.save();
            assert_eq!(
                binding.checked().unwrap_err().to_string(),
                "MANAGED_PAIRING_BINDING_CHANGED"
            );
            let updated = fixture.binding();
            assert_eq!(
                updated.credential_fingerprint,
                binding.credential_fingerprint
            );
            assert_ne!(updated.configuration_hash, binding.configuration_hash);
            std::fs::remove_file(fixture._temp.path().join("alternate.dpapi")).unwrap();
            assert_eq!(
                Binding::load(&fixture.path).unwrap_err().to_string(),
                "MANAGED_PAIRING_CREDENTIAL_UNAVAILABLE"
            );
            fixture.config.data_directory = fixture._temp.path().join("missing-registry");
            fixture.save();
            assert!(Binding::load(&fixture.path).is_err());
            assert!(!fixture.config.data_directory.exists());
        }
        #[test]
        fn running_is_single_instance_disable_drains_and_binding_change_stops() {
            let mut fixture = Fixture::new();
            let binding = fixture.binding();
            let mut controller = Controller::new();
            wait(&mut controller, Some(&binding), "running");
            let thread = controller.runner.as_ref().unwrap().thread.thread().id();
            for _ in 0..10 {
                controller.reconcile(Some(&binding)).unwrap();
            }
            assert_eq!(
                controller.runner.as_ref().unwrap().thread.thread().id(),
                thread
            );
            assert!(Service::open(fixture.config.clone()).is_err());
            controller.reconcile(None).unwrap();
            assert_eq!(controller.status()["desired_enabled"], false);
            wait(&mut controller, None, "disabled");
            assert!(controller.runner.is_none());
            drop(Service::open(fixture.config.clone()).unwrap());
            wait(&mut controller, Some(&binding), "running");
            fixture.config.credential = CredentialSource::WindowsDpapi {
                file: fixture._temp.path().join("changed.dpapi"),
            };
            fixture
                .config
                .credential
                .save_windows(&fixture.identity)
                .unwrap();
            fixture.save();
            controller.next_check = Instant::now();
            wait(&mut controller, Some(&binding), "unavailable");
            assert_eq!(
                controller.status()["error"],
                "MANAGED_PAIRING_BINDING_CHANGED"
            );
            assert!(controller.runner.is_none());
            controller.shutdown().unwrap();
        }
        #[test]
        fn standalone_owner_is_not_adopted_and_start_failures_back_off() {
            let fixture = Fixture::new();
            let binding = fixture.binding();
            let owner = Service::open(fixture.config.clone()).unwrap();
            let mut controller = Controller::new();
            wait(&mut controller, Some(&binding), "unavailable");
            assert_eq!(
                controller.status()["error"],
                "MANAGED_PAIRING_OWNERSHIP_CONFLICT"
            );
            assert!(controller.status()["retry_after_ms"].as_u64().unwrap() > 0);
            for _ in 0..20 {
                controller.reconcile(Some(&binding)).unwrap();
            }
            assert!(controller.runner.is_none());
            drop(owner);
            controller.next_attempt = Instant::now();
            wait(&mut controller, Some(&binding), "running");
            drop(controller); // Drop also drains a live runner before releasing ownership.
            drop(Service::open(fixture.config.clone()).unwrap());
        }
        #[test]
        fn unavailable_credential_is_sanitized_and_never_provisioned() {
            let fixture = Fixture::new();
            let binding = fixture.binding();
            std::fs::remove_file(fixture._temp.path().join("synthetic.dpapi")).unwrap();
            let mut controller = Controller::new();
            wait(&mut controller, Some(&binding), "unavailable");
            assert_eq!(
                controller.status()["error"],
                "MANAGED_PAIRING_CREDENTIAL_UNAVAILABLE"
            );
            assert!(!fixture._temp.path().join("synthetic.dpapi").exists());
            controller.shutdown().unwrap();
        }
        #[test]
        fn uncertain_drain_inhibits_relaunch_and_retains_unfinished_handle() {
            let fixture = Fixture::new();
            let binding = fixture.binding();
            let mut controller = Controller::new();
            controller.desired = Some(binding.clone());
            let stop = Arc::new(AtomicBool::new(false));
            let stopped = stop.clone();
            let (_ready, started) = mpsc::channel();
            let thread = std::thread::spawn(move || {
                while !stopped.load(Ordering::Relaxed) {
                    std::thread::yield_now();
                }
                std::thread::sleep(Duration::from_millis(150));
                Outcome::Stopped
            });
            controller.runner = Some(Runner {
                binding: binding.clone(),
                stop,
                started,
                thread,
                stopping: false,
            });
            assert!(
                controller
                    .shutdown_budget(Duration::from_millis(10))
                    .is_err()
            );
            drop(controller);
            assert!(
                quarantine()
                    .lock()
                    .unwrap()
                    .get(&binding.data_directory)
                    .unwrap()
                    ._runner
                    .is_some()
            );
            let mut replacement = Controller::new();
            replacement.reconcile(Some(&binding)).unwrap();
            assert_eq!(
                replacement.status()["error"],
                "MANAGED_PAIRING_DRAIN_UNCERTAIN"
            );
            assert!(replacement.runner.is_none());
            std::thread::sleep(Duration::from_millis(180));
        }
        #[test]
        #[ignore = "subprocess ownership fixture"]
        fn standalone_open_fixture() {
            let path = std::env::var_os("TKFS_MANAGED_TEST_CONFIG").unwrap();
            let config = Config::load(Path::new(&path)).unwrap();
            let error = Service::open(config)
                .err()
                .expect("service lock was released");
            assert!(format!("{error:#}").contains("PAIRING_SERVICE_ALREADY_RUNNING"));
        }
        #[test]
        fn outer_failure_retains_cross_process_lock_while_child_is_blocked() {
            let fixture = Fixture::new();
            let binding = fixture.binding();
            let peer_identity = Identity::generate(&id()).unwrap();
            let peer = Peer {
                installation: peer_identity.installation.clone(),
                certificate: peer_identity.certificate.clone(),
                endpoint: "wss://127.0.0.1:1/tkfs/sync".into(),
                revoked: false,
            };
            fixture.config.registry().unwrap().trust(&peer).unwrap();
            let pipe = format!("tkfs-managed-blocked-{}", id());
            let mut listener = crate::local_ipc::Listener::bind(&pipe).unwrap();
            let discovery = runtime::Discovery {
                format: 1,
                address: format!("pipe:{pipe}"),
                token: id(),
                state: "synthetic".into(),
                mount: None,
                repo: id(),
                device: id(),
            };
            let runtime = fixture._temp.path().join("blocked-runtime.json");
            std::fs::write(&runtime, serde_json::to_vec(&discovery).unwrap()).unwrap();
            fixture
                .config
                .registry()
                .unwrap()
                .configure_sync(&SyncConfiguration {
                    peer: peer.installation,
                    repo: discovery.repo,
                    local_runtime: runtime,
                    local_replica: discovery.device,
                    remote_replica: id(),
                    enabled: true,
                })
                .unwrap();
            let (entered, received) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let child = std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(15);
                loop {
                    if listener.receive::<Value>().unwrap().is_some() {
                        entered.send(()).unwrap();
                        let _ = released.recv_timeout(Duration::from_secs(15));
                        let _ = listener.reply(&json!({"ok":false,"error":"SYNTHETIC_RELEASE"}));
                        return;
                    }
                    assert!(Instant::now() < deadline, "grant never entered local IPC");
                }
            });
            let mut controller = Controller::new();
            wait(&mut controller, Some(&binding), "running");
            received.recv_timeout(Duration::from_secs(8)).unwrap();
            crate::wss_transport::fail_run_for_test(&binding.data_directory);
            wait(&mut controller, Some(&binding), "unavailable");
            assert_eq!(
                controller.status()["error"],
                "MANAGED_PAIRING_DRAIN_UNCERTAIN"
            );
            let assert_separate_opener_rejected = || {
                let result = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "managed_pairing::tests::windows::standalone_open_fixture",
                        "--exact",
                        "--ignored",
                    ])
                    .env("TKFS_MANAGED_TEST_CONFIG", &fixture.path)
                    .output()
                    .unwrap();
                assert!(
                    result.status.success(),
                    "{}",
                    String::from_utf8_lossy(&result.stdout)
                );
            };
            assert_separate_opener_rejected();
            drop(controller);
            assert_separate_opener_rejected();
            release.send(()).unwrap();
            child.join().unwrap();
            assert!(
                quarantine()
                    .lock()
                    .unwrap()
                    .get(&binding.data_directory)
                    .unwrap()
                    ._service
                    .is_some()
            );
        }
        #[test]
        fn background_sync_resumes_existing_grants_after_controller_restart_without_code() {
            let fixture = Fixture::new();
            let binding = fixture.binding();
            let server_identity = Arc::new(Identity::generate(&id()).unwrap());
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = reserved.local_addr().unwrap().to_string();
            drop(reserved);
            let server = Config {
                installation: server_identity.installation.clone(),
                data_directory: fixture._temp.path().join("server-registry"),
                inbound: true,
                dial: false,
                listen: address.clone(),
                advertise: format!("wss://{address}/tkfs/sync"),
                enrollment_advertise: format!("wss://{address}/tkfs/enroll"),
                ..fixture.config.clone()
            };
            let remote = Peer {
                installation: server_identity.installation.clone(),
                certificate: server_identity.certificate.clone(),
                endpoint: server.advertise.clone(),
                revoked: false,
            };
            let local = Peer {
                installation: fixture.identity.installation.clone(),
                certificate: fixture.identity.certificate.clone(),
                endpoint: "outbound-only".into(),
                revoked: false,
            };
            fixture.config.registry().unwrap().trust(&remote).unwrap();
            server.registry().unwrap().trust(&local).unwrap();
            let repo = id();
            let local_device = id();
            let remote_device = id();
            let local_path = fixture._temp.path().join("local-worker");
            let remote_path = fixture._temp.path().join("remote-worker");
            let local_engine = Arc::new(Mutex::new(Engine::new(
                Store::initialize(&local_path, &repo, &local_device).unwrap(),
            )));
            let remote_engine = Arc::new(Mutex::new(Engine::new(
                Store::initialize(&remote_path, &repo, &remote_device).unwrap(),
            )));
            runtime::start_rpc(local_engine.clone(), &local_path, None).unwrap();
            runtime::start_rpc(remote_engine.clone(), &remote_path, None).unwrap();
            let grant = SyncConfiguration {
                peer: remote.installation.clone(),
                repo: repo.clone(),
                local_runtime: local_path.join("runtime.json"),
                local_replica: local_device.clone(),
                remote_replica: remote_device.clone(),
                enabled: true,
            };
            fixture
                .config
                .registry()
                .unwrap()
                .configure_sync(&grant)
                .unwrap();
            server
                .registry()
                .unwrap()
                .configure_sync(&SyncConfiguration {
                    peer: local.installation.clone(),
                    repo,
                    local_runtime: remote_path.join("runtime.json"),
                    local_replica: remote_device,
                    remote_replica: local_device,
                    enabled: true,
                })
                .unwrap();
            struct Server {
                stop: Arc<AtomicBool>,
                thread: Option<JoinHandle<Result<()>>>,
            }
            impl Drop for Server {
                fn drop(&mut self) {
                    self.stop.store(true, Ordering::Relaxed);
                    if let Some(thread) = self.thread.take() {
                        thread.join().unwrap().unwrap();
                    }
                }
            }
            let service = Service::open_identity(server, server_identity).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let flag = stop.clone();
            let _server = Server {
                stop,
                thread: Some(std::thread::spawn(move || service.run(flag))),
            };
            let create = |name: &str| {
                let mut engine = remote_engine.lock().unwrap();
                let entry = engine.store.create(name, "file").unwrap();
                engine
                    .store
                    .write_revision(&entry.id, None, b"synthetic managed sync")
                    .unwrap();
            };
            let received = |controller: &mut Controller, name: &str| {
                let deadline = Instant::now() + Duration::from_secs(15);
                loop {
                    controller.reconcile(Some(&binding)).unwrap();
                    if local_engine.lock().unwrap().store.lookup(name).is_ok() {
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "background grant did not synchronize"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                }
            };
            create("first.txt");
            let mut controller = Controller::new();
            received(&mut controller, "first.txt");
            controller.shutdown().unwrap();
            drop(controller);
            create("after-restart.txt");
            let mut restarted = Controller::new();
            received(&mut restarted, "after-restart.txt");
            let mut paused = grant.clone();
            paused.enabled = false;
            fixture
                .config
                .registry()
                .unwrap()
                .configure_sync(&paused)
                .unwrap();
            restarted.shutdown().unwrap();
            assert!(!fixture.config.registry().unwrap().configurations().unwrap()[0].enabled);
            assert!(fixture.config.orchestrator_config.is_none());
            assert!(
                !fixture
                    ._temp
                    .path()
                    .join("orchestrator.toml.pairing")
                    .exists()
            );
        }
    }
}
impl Drop for Controller {
    fn drop(&mut self) {
        if self.shutdown().is_err() {
            if let Some(runner) = self.runner.take() {
                runner.stop.store(true, Ordering::Relaxed);
                quarantine()
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(
                        runner.binding.data_directory.clone(),
                        Quarantined {
                            _runner: Some(runner),
                            _service: None,
                        },
                    );
            }
            // Fixed diagnostic only. No path, credential or underlying error.
            eprintln!("MANAGED_PAIRING_DRAIN_UNCERTAIN: process restart required");
        }
    }
}
