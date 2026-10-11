//! Owner-local receive workflow. Only public identities and transient proof are
//! kept here. Durable state/grants/receipts remain with the existing controllers.
use super::{Action, BranchAction, BranchContext, Controller, View, text};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::path::PathBuf;

pub enum Request {
    Prepare { label: String, repo: String },
    Configure { state: String, peer: String },
    Sync { state: String, peer: String },
    Materialize { state: String, branch: String },
    Stop { state: String },
    Attach { state: String, mount: PathBuf },
    Start { state: String },
}
#[derive(Clone)]
pub struct Proof {
    state: String,
    repo: String,
    device: String,
    peer: String,
    instance: String,
    binding: Value,
    management_generation: u64,
    branch: Option<(String, u64)>,
    stopped: bool,
}
#[derive(Default)]
pub struct Snapshot {
    pub state: String,
    pub repo: String,
    pub device: String,
    pub runtime: String,
    pub status: String,
    pub headless: bool,
    pub running: bool,
    pub caught_up: bool,
    pub materialized: bool,
    pub can_attach: bool,
}
fn generation(state: &Value) -> Result<u64> {
    state["management_generation"]
        .as_u64()
        .context("STATE_GENERATION_MISSING")
}
fn quiet_status(status: &Value, proof: &Proof) -> Result<()> {
    ensure!(
        status["busy"] != true && status["stale"] != true && status["health"].is_null(),
        "RECEIVE_RUNTIME_NOT_READY"
    );
    ensure!(
        status["last_sync"]["caught_up"] == true
            && status["last_sync"]["peer"] == proof.peer
            && status["last_sync"]["replica"] == proof.binding["grant"]["remote_replica"],
        "RECEIVE_FRESH_SYNC_REQUIRED"
    );
    ensure!(
        status["incoming_pending"] == 0
            && status["outgoing_unacknowledged"] == 0
            && status["unflushed_files"] == 0
            && status["pending_causal_events"] == json!([]),
        "RECEIVE_PENDING_DATA"
    );
    if let Some((branch, branch_generation)) = &proof.branch {
        ensure!(
            status["branch"]["id"] == *branch
                && status["branch"]["shared"] == true
                && status["generation"] == *branch_generation,
            "RECEIVE_BRANCH_CHANGED"
        );
    }
    Ok(())
}
impl Controller {
    fn receive_observed(&self, state: &str) -> Result<Value> {
        ensure!(
            self.selected.as_deref() == Some(state),
            "STALE_PROJECT_SELECTION"
        );
        self.session
            .as_ref()
            .context("NOT_CONNECTED")?
            .call(Action::Inspect {
                state: state.into(),
            })
    }
    fn receive_validate(&self, proof: &Proof) -> Result<Value> {
        let observed = self.receive_observed(&proof.state)?;
        ensure!(
            observed["mount"].is_null()
                && observed["repo_id"] == proof.repo
                && observed["device_id"] == proof.device
                && generation(&observed)? == proof.management_generation,
            "RECEIVE_STATE_CHANGED_RESYNC_REQUIRED"
        );
        let binding = self
            .pairing
            .as_ref()
            .context("PAIRING_CONFIG_REQUIRED")?
            .receive_binding(&proof.peer, &proof.repo)?;
        ensure!(
            binding == proof.binding,
            "RECEIVE_GRANT_CHANGED_RESYNC_REQUIRED"
        );
        if proof.stopped {
            ensure!(
                observed["observation"]["status"] == "stopped"
                    && observed["desired_running"] == false,
                "RECEIVE_STOPPED_STATE_CHANGED"
            );
        } else {
            ensure!(
                observed["observation"]["status"] == "running"
                    && observed["observation"]["instance"] == proof.instance,
                "RECEIVE_WORKER_RESTARTED_RESYNC_REQUIRED"
            );
            let status = self
                .session
                .as_ref()
                .unwrap()
                .runtime(&proof.state, json!({"op":"status"}))?;
            quiet_status(&status, proof)?;
        }
        Ok(observed)
    }
    pub fn receive_refresh(&mut self) {
        if self
            .receive_proof
            .as_ref()
            .is_some_and(|proof| self.receive_validate(proof).is_err())
        {
            self.receive_proof = None;
        }
    }
    pub fn receive_snapshot(&self) -> Snapshot {
        let Ok(state) = self.selected_state() else {
            return Snapshot::default();
        };
        let headless = state["mount"].is_null();
        let running = state["observation"]["status"] == "running";
        let proof = self
            .receive_proof
            .as_ref()
            .filter(|p| state["state_id"] == p.state);
        Snapshot {
            state: text(state, "state_id"), repo: text(state, "repo_id"), device: text(state, "device_id"), runtime: text(state, "runtime_path"), headless, running,
            caught_up: proof.is_some(), materialized: proof.is_some_and(|p| p.branch.is_some()), can_attach: proof.is_some_and(|p| p.stopped && p.branch.is_some()),
            status: if !headless && running { "Mounted worker running. Background sync can be enabled below." } else if !headless { "Mount reserved. Start the local worker when ready." } else if proof.is_some_and(|p| p.stopped) { "Branch verification recorded; worker stopped intentionally. Mount reservation rechecks the branch under the state lock." } else if proof.is_some_and(|p| p.branch.is_some()) { "Received shared branch materialized and verified. Stop before attaching its first mount." } else if proof.is_some() { "Sync caught up for this worker. Select and validate a received shared branch." } else if running { "Headless replica ready. Ask the source owner to grant this device access, then discover, configure and sync." } else { "Headless replica stopped. Start it and obtain fresh sync/branch verification before mounting." }.into(),
        }
    }
    pub fn receive_execute(&mut self, pairing_path: PathBuf, request: Request) -> Result<View> {
        let result = (|| {
            if !matches!(request, Request::Prepare { .. } | Request::Start { .. }) {
                ensure!(
                    std::fs::canonicalize(pairing_path)?
                        == self
                            .pairing
                            .as_ref()
                            .context("PAIRING_CONFIG_REQUIRED")?
                            .path(),
                    "PAIRING_CONFIG_CHANGED"
                );
            }
            self.receive_inner(request)
        })();
        if result.is_err() {
            self.receive_proof = None;
        }
        result
    }
    fn receive_inner(&mut self, request: Request) -> Result<View> {
        match request {
            Request::Prepare { label, repo } => {
                self.receive_proof = None;
                uuid::Uuid::parse_str(&repo).context("REPOSITORY_UUID_REQUIRED")?;
                let generation = self
                    .session
                    .as_ref()
                    .context("NOT_CONNECTED")?
                    .call(Action::List)?["catalog_generation"]
                    .as_u64()
                    .context("CATALOG_GENERATION_MISSING")?;
                self.mutation(Action::PrepareReplica { label, repo }, generation)
            }
            Request::Configure { state, peer } => {
                self.receive_proof = None;
                let observed = self.receive_observed(&state)?;
                ensure!(
                    observed["mount"].is_null(),
                    "RECEIVE_REQUIRES_HEADLESS_STATE"
                );
                self.pairing
                    .as_mut()
                    .context("PAIRING_CONFIG_REQUIRED")?
                    .configure(
                        &peer,
                        &text(&observed, "repo_id"),
                        &PathBuf::from(text(&observed, "runtime_path")),
                        true,
                    )?;
                self.refresh(Some((
                    "Local published-data grant saved. Source-owner approval remains separate."
                        .into(),
                    false,
                )))
            }
            Request::Sync { state, peer } => {
                self.receive_proof = None;
                let before = self.receive_observed(&state)?;
                ensure!(
                    before["mount"].is_null() && before["observation"]["status"] == "running",
                    "RECEIVE_RUNNING_HEADLESS_REQUIRED"
                );
                let repo = text(&before, "repo_id");
                let binding = self
                    .pairing
                    .as_ref()
                    .context("PAIRING_CONFIG_REQUIRED")?
                    .receive_binding(&peer, &repo)?;
                ensure!(
                    binding["grant"]["local_replica"] == before["device_id"]
                        && std::fs::canonicalize(
                            binding["grant"]["local_runtime"]
                                .as_str()
                                .context("GRANT_RUNTIME_REQUIRED")?
                        )? == std::fs::canonicalize(text(&before, "runtime_path"))?,
                    "RECEIVE_GRANT_LOCAL_BINDING_MISMATCH"
                );
                let reply =
                    self.pairing
                        .as_mut()
                        .unwrap()
                        .sync(&peer, &repo, self.pair_stop.clone())?;
                if reply["caught_up"] == true {
                    let proof = Proof {
                        state,
                        repo,
                        device: text(&before, "device_id"),
                        peer,
                        instance: text(&before["observation"], "instance"),
                        binding,
                        management_generation: generation(&before)?,
                        branch: None,
                        stopped: false,
                    };
                    ensure!(!proof.instance.is_empty(), "WORKER_INSTANCE_REQUIRED");
                    self.receive_validate(&proof)?;
                    self.receive_proof = Some(proof);
                }
                self.refresh(Some((if self.receive_proof.is_some() { "Initial sync caught up. Choose a received shared branch for validation." } else { "Sync page committed; more data remains. Sync the next page before mounting." }.into(), false)))
            }
            Request::Materialize { state, branch } => {
                let mut proof = self
                    .receive_proof
                    .clone()
                    .context("RECEIVE_FRESH_SYNC_REQUIRED")?;
                ensure!(
                    proof.state == state && !proof.stopped,
                    "RECEIVE_WRONG_STATE_OR_STOPPED"
                );
                self.receive_validate(&proof)?;
                let session = self.session.as_ref().unwrap();
                let branches = session.runtime(&state, json!({"op":"branches"}))?;
                ensure!(
                    branches
                        .as_array()
                        .context("BRANCH_LIST_REQUIRED")?
                        .iter()
                        .any(|b| b["id"] == branch && b["shared"] == true),
                    "RECEIVE_SHARED_BRANCH_REQUIRED"
                );
                let status = session.runtime(&state, json!({"op":"status"}))?;
                let context = BranchContext {
                    state: state.clone(),
                    active: text(&status["branch"], "id"),
                    generation: status["generation"]
                        .as_u64()
                        .context("RUNTIME_GENERATION_REQUIRED")?,
                };
                let reply = self
                    .branch_client
                    .as_mut()
                    .context("BRANCH_CLIENT_NOT_READY")?
                    .mutate(
                        session,
                        context,
                        BranchAction::Checkout {
                            branch: branch.clone(),
                        },
                    )?;
                ensure!(
                    reply["ok"] == true,
                    "RECEIVE_CHECKOUT_PENDING: inspect and retry the original branch request"
                );
                let status = session.runtime(&state, json!({"op":"status"}))?;
                ensure!(
                    status["branch"]["id"] == branch && status["branch"]["shared"] == true,
                    "RECEIVE_CHECKOUT_CHANGED"
                );
                proof.branch = Some((
                    branch,
                    status["generation"]
                        .as_u64()
                        .context("RUNTIME_GENERATION_REQUIRED")?,
                ));
                self.receive_validate(&proof)?;
                self.receive_proof = Some(proof);
                self.refresh(Some(("Shared branch checkout verified its causal history and visible content. Ready to stop and reserve a mount.".into(), false)))
            }
            Request::Stop { state } => {
                let mut proof = self
                    .receive_proof
                    .clone()
                    .context("RECEIVE_MATERIALIZATION_REQUIRED")?;
                ensure!(
                    proof.state == state && proof.branch.is_some() && !proof.stopped,
                    "RECEIVE_MATERIALIZATION_REQUIRED"
                );
                self.receive_validate(&proof)?;
                let reply = self.session.as_mut().unwrap().mutate(
                    Action::Stop {
                        state: state.clone(),
                    },
                    proof.management_generation,
                )?;
                ensure!(
                    reply["ok"] == true,
                    "RECEIVE_STOP_PENDING: retry the exact stop request, then restart headless and revalidate"
                );
                let observed = self.receive_observed(&state)?;
                proof.management_generation = generation(&observed)?;
                proof.stopped = true;
                self.receive_validate(&proof)?;
                self.receive_proof = Some(proof);
                self.refresh(Some(("Headless worker stopped after verified checkout. First mount can now be reserved.".into(), false)))
            }
            Request::Attach { state, mount } => {
                let proof = self
                    .receive_proof
                    .clone()
                    .context("RECEIVE_MATERIALIZATION_REQUIRED")?;
                ensure!(
                    proof.state == state && proof.stopped && proof.branch.is_some(),
                    "RECEIVE_STOP_REQUIRED"
                );
                self.receive_validate(&proof)?;
                self.receive_proof = None;
                let (branch, branch_generation) =
                    proof.branch.context("RECEIVE_MATERIALIZATION_REQUIRED")?;
                self.mutation(
                    Action::AttachMount {
                        state,
                        mount,
                        expected_branch: Some(branch),
                        expected_branch_generation: Some(branch_generation),
                    },
                    proof.management_generation,
                )
            }
            Request::Start { state } => {
                self.receive_proof = None;
                let observed = self.receive_observed(&state)?;
                self.mutation(Action::Start { state }, generation(&observed)?)
            }
        }
    }
}
