//! Identity-bound desktop branch requests. The runtime owns all stores/receipts.
use crate::{
    core::id,
    desktop::{Paths, Session, atomic_write, exclusive},
    orchestrator::Action,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchContext {
    pub state: String,
    pub active: String,
    pub generation: u64,
}
#[derive(Clone, Debug)]
pub enum BranchAction {
    Create { name: String },
    Checkout { branch: String },
    Publish { name: String, confirmed: bool },
    Checkpoint { message: String },
    Restore { checkpoint: String, name: String },
}
impl BranchAction {
    pub fn payload(&self, generation: u64) -> Result<Value> {
        let mut value = match self {
            Self::Create { name } => json!({"op":"branch", "name":required(name)?}),
            Self::Checkout { branch } => {
                validate_branch_id(branch)?;
                json!({"op":"checkout", "name":branch})
            }
            Self::Publish { name, confirmed } => {
                ensure!(*confirmed, "PUBLICATION_CONFIRMATION_REQUIRED");
                json!({"op":"publish", "name":required(name)?, "current_state_only":true})
            }
            Self::Checkpoint { message } => {
                json!({"op":"checkpoint", "message":required(message)?})
            }
            Self::Restore { checkpoint, name } => {
                uuid::Uuid::parse_str(checkpoint).context("INVALID_CHECKPOINT_ID")?;
                json!({"op":"restore", "checkpoint":checkpoint, "name":required(name)?})
            }
        };
        value["generation"] = json!(generation);
        Ok(value)
    }
}
fn required(value: &str) -> Result<&str> {
    ensure!(!value.trim().is_empty(), "NAME_OR_MESSAGE_REQUIRED");
    Ok(value.trim())
}
fn validate_branch_id(value: &str) -> Result<()> {
    // The repository's original main branch is a deterministic SHA-256 ID;
    // user-created and restored branches use UUIDs. Neither is a branch name.
    ensure!(
        uuid::Uuid::parse_str(value).is_ok()
            || (value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())),
        "INVALID_BRANCH_ID"
    );
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedBranchOperation {
    version: u32,
    pub installation: String,
    pub context: BranchContext,
    repo: String,
    device: String,
    pub request: String,
    pub payload: Value,
    pub response: Option<Value>,
    pub uncertain: bool,
}
impl SavedBranchOperation {
    pub fn pending(&self) -> bool {
        self.uncertain
            || self
                .response
                .as_ref()
                .is_some_and(|v| v["retryable"] == true)
    }
    fn validate(&self, installation: &str) -> Result<()> {
        ensure!(
            self.version == 1 && self.installation == installation,
            "BRANCH_OPERATION_INSTALLATION_MISMATCH"
        );
        for identity in [
            &self.installation,
            &self.context.state,
            &self.repo,
            &self.device,
            &self.request,
        ] {
            uuid::Uuid::parse_str(identity).context("INVALID_BRANCH_OPERATION_IDENTITY")?;
        }
        validate_branch_id(&self.context.active)?;
        ensure!(
            self.payload["generation"].as_u64() == Some(self.context.generation),
            "INVALID_BRANCH_OPERATION_GENERATION"
        );
        ensure!(
            matches!(
                self.payload["op"].as_str(),
                Some("branch" | "checkout" | "publish" | "checkpoint" | "restore")
            ),
            "INVALID_BRANCH_OPERATION"
        );
        if self.payload["op"] == "publish" {
            ensure!(
                self.payload["current_state_only"] == true,
                "INVALID_PUBLICATION_OPERATION"
            );
        }
        Ok(())
    }
}
pub struct BranchClient {
    paths: Paths,
    installation: String,
    pub saved: Option<SavedBranchOperation>,
}
impl BranchClient {
    pub fn open(session: &Session) -> Result<Self> {
        let mut client = Self {
            paths: session.paths.clone(),
            installation: session.identity.clone(),
            saved: None,
        };
        client.reload()?;
        Ok(client)
    }
    fn journal(&self) -> std::path::PathBuf {
        self.paths.folder.join(".tkfs-ui-branch-operation.json")
    }
    pub fn reload(&mut self) -> Result<()> {
        self.saved = if self.journal().try_exists()? {
            let saved: SavedBranchOperation = serde_json::from_slice(&fs::read(self.journal())?)
                .context("INVALID_BRANCH_OPERATION_JOURNAL")?;
            saved.validate(&self.installation)?;
            Some(saved)
        } else {
            None
        };
        Ok(())
    }
    pub fn mutate(
        &mut self,
        session: &Session,
        context: BranchContext,
        action: BranchAction,
    ) -> Result<Value> {
        let payload = action.payload(context.generation)?;
        let _guard = exclusive(&self.paths.folder.join(".tkfs-branch-operation.lock"))?;
        self.reload()?;
        ensure!(
            !self
                .saved
                .as_ref()
                .is_some_and(SavedBranchOperation::pending),
            "BRANCH_OPERATION_PENDING: retry the original branch request first"
        );
        ensure!(
            session.identity == self.installation,
            "BRANCH_OPERATION_INSTALLATION_MISMATCH"
        );
        let status = session.runtime(&context.state, json!({"op":"status"}))?;
        validate_context(&context, &status)?;
        let registered = session.call(Action::Inspect {
            state: context.state.clone(),
        })?;
        self.saved = Some(SavedBranchOperation {
            version: 1,
            installation: self.installation.clone(),
            context,
            repo: registered["repo_id"]
                .as_str()
                .context("REPO_ID_MISSING")?
                .into(),
            device: registered["device_id"]
                .as_str()
                .context("DEVICE_ID_MISSING")?
                .into(),
            request: id(),
            payload,
            response: None,
            uncertain: true,
        });
        self.persist()?; // Always durable before delivery; no optimistic UI update.
        self.send(session)
    }
    pub fn retry(&mut self, session: &Session) -> Result<Value> {
        let _guard = exclusive(&self.paths.folder.join(".tkfs-branch-operation.lock"))?;
        self.reload()?;
        ensure!(
            self.saved
                .as_ref()
                .is_some_and(SavedBranchOperation::pending),
            "NO_PENDING_BRANCH_OPERATION"
        );
        // Do not replace generation or check current active branch here: a lost
        // checkout reply must replay its receipt before the old generation check.
        self.send(session)
    }
    fn persist(&self) -> Result<()> {
        atomic_write(
            &self.journal(),
            &serde_json::to_vec(self.saved.as_ref().context("NO_BRANCH_OPERATION")?)?,
        )
    }
    fn send(&mut self, session: &Session) -> Result<Value> {
        let saved = self.saved.as_ref().context("NO_BRANCH_OPERATION")?;
        saved.validate(&session.identity)?;
        let registered = session.call(Action::Inspect {
            state: saved.context.state.clone(),
        })?;
        ensure!(
            registered["repo_id"] == saved.repo && registered["device_id"] == saved.device,
            "BRANCH_OPERATION_STATE_IDENTITY_MISMATCH"
        );
        self.deliver(|saved| {
            session.runtime_request(&saved.context.state, &saved.request, saved.payload.clone())
        })
    }
    fn deliver(
        &mut self,
        call: impl FnOnce(&SavedBranchOperation) -> Result<Value>,
    ) -> Result<Value> {
        let saved = self.saved.as_mut().context("NO_BRANCH_OPERATION")?;
        let reply = match call(saved) {
            Ok(value) => {
                saved.uncertain = false;
                json!({"ok":true, "result":value})
            }
            Err(error) => {
                let message = format!("{error:#}");
                saved.uncertain = error
                    .chain()
                    .any(|cause| cause.downcast_ref::<std::io::Error>().is_some());
                json!({"ok":false, "error":message, "retryable":saved.uncertain || retryable(&message)})
            }
        };
        saved.response = Some(reply.clone());
        self.persist()?;
        Ok(reply)
    }
}
pub fn validate_context(context: &BranchContext, status: &Value) -> Result<()> {
    ensure!(
        status["busy"] != true && status["stale"] != true,
        "BUSY_VIEW: branch status is sampled; refresh after saving completes"
    );
    ensure!(
        status["branch"]["id"] == context.active
            && status["generation"].as_u64() == Some(context.generation),
        "STALE_VIEW: current branch changed; refresh and review the action again"
    );
    Ok(())
}
fn retryable(message: &str) -> bool {
    ["BUSY_VIEW", "INCOMPLETE_CAUSAL_HISTORY", "REMOUNT_FAILED"]
        .iter()
        .any(|code| message.contains(code))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::Store, runtime::Engine};
    #[test]
    fn publication_requires_confirmation_and_stale_or_sampled_context_is_rejected() {
        assert!(
            BranchAction::Publish {
                name: "visible".into(),
                confirmed: false
            }
            .payload(0)
            .is_err()
        );
        let context = BranchContext {
            state: id(),
            active: id(),
            generation: 4,
        };
        let mut status = json!({"branch":{"id":context.active},"generation":4});
        validate_context(&context, &status).unwrap();
        status["generation"] = json!(5);
        assert!(validate_context(&context, &status).is_err());
        status["generation"] = json!(4);
        status["busy"] = json!(true);
        assert!(validate_context(&context, &status).is_err());
        assert!(!retryable("STALE_VIEW"));
    }
    #[test]
    fn durable_runtime_reply_loss_replays_one_branch_and_busy_checkout_keeps_exact_request() {
        let temp = tempfile::tempdir().unwrap();
        let exe = temp.path().join("tkfs.exe");
        fs::write(&exe, b"fixture").unwrap();
        let store = Store::initialize(&temp.path().join("store"), &id(), &id()).unwrap();
        let mut engine = Engine::new(store);
        let installation = id();
        let context = BranchContext {
            state: id(),
            active: engine.store.active.clone(),
            generation: 0,
        };
        let saved = SavedBranchOperation {
            version: 1,
            installation: installation.clone(),
            context,
            repo: engine.store.repo.clone(),
            device: engine.store.device.clone(),
            request: id(),
            payload: json!({"op":"branch","name":"private","generation":0}),
            response: None,
            uncertain: true,
        };
        let mut client = BranchClient {
            paths: Paths::from_executable(exe).unwrap(),
            installation,
            saved: Some(saved),
        };
        client.persist().unwrap();
        client
            .deliver(|saved| {
                engine.control(&saved.request, &saved.payload)?;
                Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into())
            })
            .unwrap();
        assert!(client.saved.as_ref().unwrap().uncertain);
        client.reload().unwrap();
        let branch = client
            .deliver(|saved| engine.control(&saved.request, &saved.payload))
            .unwrap()["result"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(engine.store.branches().unwrap().len(), 2);
        let saved = client.saved.as_mut().unwrap();
        saved.request = id();
        saved.payload = json!({"op":"checkout","name":branch,"generation":0});
        saved.uncertain = true;
        let request = saved.request.clone();
        let payload = saved.payload.clone();
        let (handle, _) = engine.open("", None, false).unwrap();
        assert_eq!(
            client
                .deliver(|saved| engine.control(&saved.request, &saved.payload))
                .unwrap()["retryable"],
            true
        );
        engine.close(handle).unwrap();
        client.reload().unwrap();
        assert_eq!(client.saved.as_ref().unwrap().request, request);
        assert_eq!(client.saved.as_ref().unwrap().payload, payload);
        client
            .deliver(|saved| engine.control(&saved.request, &saved.payload))
            .unwrap();
        assert_eq!(engine.store.active, branch);
        assert_eq!(engine.store.generation, 1);
        assert!(!client.saved.as_ref().unwrap().pending());
        client.saved.as_mut().unwrap().uncertain = true;
        client.persist().unwrap();
        let root = engine.store.root.clone();
        drop(engine);
        let mut engine = Engine::new(Store::open_existing(&root).unwrap());
        client.reload().unwrap();
        client
            .deliver(|saved| engine.control(&saved.request, &saved.payload))
            .unwrap();
        assert_eq!(
            engine.store.generation, 1,
            "lost checkout reply must replay before obsolete generation is checked"
        );
        assert!(client.saved.as_ref().unwrap().validate(&id()).is_err());
    }
}
