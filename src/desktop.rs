//! The desktop's client of its orchestrator, and the helpers its dialogs
//! use. The desktop starts no worker and mounts nothing (the orchestrator
//! does); it never rewrites a configuration, only creates a missing one on
//! first launch ([`launcher::ensure_running`]).
use crate::orchestrator::{
    config::Config,
    endpoint::ControlClient,
    launcher::{self, START_BOUND, Started},
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

/// The desktop's connection to its orchestrator: a thin wrapper over the
/// control connection plus the configuration it was opened on.
pub struct Client {
    pub config_path: PathBuf,
    pub config: Config,
    client: ControlClient,
}

impl Client {
    /// Attaches to the orchestrator of `config_path`, or starts one from
    /// `exe` within [`START_BOUND`] (see [`launcher::ensure_running`]).
    pub fn open(config_path: &Path, exe: &Path) -> Result<(Self, Started)> {
        let (config, client, started) = launcher::ensure_running(config_path, exe, START_BOUND)?;
        let config_path = std::path::absolute(config_path)?;
        Ok((
            Self {
                config_path,
                config,
                client,
            },
            started,
        ))
    }

    /// A second client on the same configuration, with no spawn: for a
    /// thread that must not wait behind this client's requests.
    /// `ORCHESTRATOR_NOT_RUNNING` when no orchestrator serves it.
    pub fn connect_again(&self) -> Result<Self> {
        Ok(Self {
            config_path: self.config_path.clone(),
            config: self.config.clone(),
            client: ControlClient::connect(&self.config)?,
        })
    }

    /// Replies wait at most `timeout` from now on (the default is the
    /// control client's [`REPLY_TIMEOUT`](crate::orchestrator::endpoint::REPLY_TIMEOUT)).
    pub fn set_reply_timeout(&mut self, timeout: std::time::Duration) {
        self.client.set_reply_timeout(timeout);
    }

    /// The orchestrator's `status` (a read: retried once on a new
    /// connection).
    pub fn status(&mut self) -> Result<Value> {
        self.client.request_read("status", json!({}))
    }

    /// A read-only action (retried once on a new connection).
    pub fn read(&mut self, action: &str, payload: Value) -> Result<Value> {
        self.client.request_read(action, payload)
    }

    /// A mutation: never resent, so a lost connection is `CONNECTION_LOST`.
    pub fn act(&mut self, action: &str, payload: Value) -> Result<Value> {
        self.client.request(action, payload)
    }

    /// `op` on the running worker of folder `state` (its id, label or state
    /// path), through the orchestrator's `folder` action.
    pub fn folder_op(&mut self, state: &str, op: &str, payload: Value) -> Result<Value> {
        self.act(
            "folder",
            json!({"state": state, "op": op, "payload": payload}),
        )
    }
}

/// `dir/label`, the default mount of a folder. `INVALID_FOLDER_NAME` when
/// the trimmed label is not a single path segment: empty, `.`, `..`, or
/// containing a control character or any of `<>:"/\|?*`.
pub fn default_mount(dir: &Path, label: &str) -> Result<PathBuf> {
    let label = label.trim();
    ensure!(
        !label.is_empty()
            && label != "."
            && label != ".."
            && !label
                .chars()
                .any(|c| c.is_control() || r#"<>:"/\|?*"#.contains(c)),
        r#"INVALID_FOLDER_NAME: {label:?} cannot name a mount (use one name without <>:"/\|?* or control characters)"#
    );
    Ok(dir.join(label))
}

/// Where a folder mounts: `None` when headless, the default under `dir` when
/// `path_text` is blank, an absolute path or drive letter (`X:`) as typed
/// (trimmed), and a relative one (`Projects`, `work\Projects`) under `dir`.
/// `INVALID_FOLDER_NAME` when a relative part is not a plain name (`..`,
/// `C:name`, `\name`, reserved characters).
pub fn mount_choice(
    dir: &Path,
    label: &str,
    path_text: &str,
    headless: bool,
) -> Result<Option<PathBuf>> {
    if headless {
        return Ok(None);
    }
    let path_text = path_text.trim();
    if path_text.is_empty() {
        return default_mount(dir, label).map(Some);
    }
    let typed = PathBuf::from(path_text);
    let drive = matches!(path_text.as_bytes(), [letter, b':'] if letter.is_ascii_alphabetic());
    if typed.is_absolute() || drive {
        return Ok(Some(typed));
    }
    let mut mount = dir.to_path_buf();
    for part in typed.components() {
        // Anything but a plain name (`..`, a prefix, a root) is refused here.
        mount = default_mount(&mount, &part.as_os_str().to_string_lossy())?;
    }
    Ok(Some(mount))
}

/// A pairing code as typed or pasted, without any whitespace.
pub fn pairing_code(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace()).collect()
}

/// An error as the dialogs show it: `MOUNT_UNSUPPORTED` says what to do
/// instead; everything else is the full error chain.
pub fn explain(error: &anyhow::Error) -> String {
    let text = format!("{error:#}");
    if text.starts_with("MOUNT_UNSUPPORTED") {
        return "Mounting isn't available in this build yet. Tick \"Headless\" to sync this folder without a mount.".into();
    }
    text
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mount_choice_defaults_to_the_mount_root_and_headless_means_none() {
        let root = Path::new(r"C:\TK\mounts");
        assert_eq!(
            mount_choice(root, "Acme", "  ", false).unwrap(),
            Some(root.join("Acme"))
        );
        assert_eq!(
            mount_choice(root, "Acme", r"D:\X", false).unwrap(),
            Some(PathBuf::from(r"D:\X"))
        );
        assert_eq!(mount_choice(root, "Acme", r"D:\X", true).unwrap(), None);
        assert_eq!(
            mount_choice(root, "Acme", "M:", false).unwrap(),
            Some(PathBuf::from("M:"))
        );
    }
    #[test]
    fn a_relative_mount_goes_under_the_mount_root() {
        let root = Path::new(r"C:\TK\mounts");
        assert_eq!(
            mount_choice(root, "Acme", " Projects ", false).unwrap(),
            Some(root.join("Projects"))
        );
        assert_eq!(
            mount_choice(root, "Acme", "work/Projects", false).unwrap(),
            Some(root.join("work").join("Projects"))
        );
        for typed in ["..", r"..\x", "a/../b", "./a"] {
            let e = mount_choice(root, "Acme", typed, false)
                .unwrap_err()
                .to_string();
            assert!(e.starts_with("INVALID_FOLDER_NAME"), "{typed:?}: {e}");
        }
    }
    #[test]
    fn default_mount_refuses_labels_that_are_not_one_path_segment() {
        for label in ["", " ", ".", "..", "a/b", r"a\b", "a:b", "x?", "con\u{7}"] {
            let e = default_mount(Path::new("m"), label)
                .unwrap_err()
                .to_string();
            assert!(e.starts_with("INVALID_FOLDER_NAME"), "{label:?}: {e}");
        }
    }
    #[test]
    fn pairing_code_strips_whitespace() {
        let code = "ab".repeat(32);
        assert_eq!(pairing_code(&format!("  {code}\r\n")), code);
    }
    #[test]
    fn mount_unsupported_is_explained_not_masked() {
        let e = anyhow::anyhow!(
            "MOUNT_UNSUPPORTED: mounts of orchestrator-managed folders arrive later"
        );
        assert!(explain(&e).starts_with("Mounting isn't available in this build yet."));
        assert_eq!(
            explain(&anyhow::anyhow!("WORKER_LIMIT: 8")),
            "WORKER_LIMIT: 8"
        );
    }
}
