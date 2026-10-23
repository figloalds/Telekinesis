//! `Engine::control`: status, inspection and journaled mutations requested
//! over the worker's loopback control endpoint.
use super::{
    engine::Engine,
    notify::{namespace_notifications, visible_paths},
};
use crate::core::*;
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

impl Engine {
    pub fn control(&mut self, request: &str, payload: &Value) -> Result<Value> {
        self.control_prepared(request, payload, None, &hash(&serde_json::to_vec(payload)?))
    }
    pub(crate) fn control_prepared(
        &mut self,
        request: &str,
        payload: &Value,
        prepared: Option<&crate::content::PreparedContent>,
        fingerprint: &str,
    ) -> Result<Value> {
        if let Some(value) = self.store.receipt_fingerprint(request, fingerprint)? {
            return Ok(value);
        }
        if let Some(g) = payload["generation"].as_u64() {
            ensure!(g == self.store.generation, "STALE_VIEW");
        }
        let op = payload["op"].as_str().context("INVALID_REQUEST")?;
        let text =
            |key: &str| -> Result<&str> { payload[key].as_str().context("MISSING_ARGUMENT") };
        match op {
            "status" => {
                let mut s = self.store.status()?;
                s["mounted"] = json!(self.mounted);
                s["open_handles"] = json!(self.handles.len());
                s["unflushed_files"] = json!(self.sessions.values().filter(|s| s.dirty).count());
                s["health"] = json!(self.reported_health());
                s["notification_error"] = json!(self.notification_error);
                s["pending_notifications"] = json!(self.notifications.len());
                s["last_sync"] = json!(self.last_sync);
                s["peers"] = json!(self.peer_sync);
                s["sampled_at_ms"] = json!(time());
                s["caught_up"] = json!(
                    self.last_sync
                        .as_ref()
                        .is_some_and(|v| v["caught_up"] == true
                            && v["at"]
                                .as_u64()
                                .is_some_and(|at| time().saturating_sub(at) < 15000))
                        && s["outgoing_unacknowledged"] == 0
                        // CCR-WP2.3-1 (WP2.INT): staged rows that still need
                        // activation; quarantined and blocked rows are
                        // reported apart and never keep this false (the
                        // worker `status` RPC's rule since WP2.3).
                        && s["incoming_active"] == 0
                        && s["pending_causal_events"] == json!([])
                        && s["unflushed_files"] == 0
                        && self.reported_health().is_none()
                        && !self.sync_peers.is_empty()
                        && self.sync_peers.iter().all(|peer| {
                            self.peer_sync
                                .get(peer)
                                .and_then(|v| v["at"].as_u64())
                                .is_some_and(|at| time().saturating_sub(at) < 15000)
                                && self.store.shared_event_inventory().ok()
                                    == self.store.known_by_peer(peer).ok()
                        })
                );
                s["metadata_caught_up"] = s["caught_up"].clone();
                s["content_complete"] = s["content"]["complete"].clone();
                s["upload_state"] = json!(if !self.store.branch(&self.store.active)?.shared {
                    "local-only"
                } else if self.sync_peers.is_empty() {
                    "peer-not-configured"
                } else if s["outgoing_unacknowledged"] != 0 {
                    "queued"
                } else {
                    "peer-acknowledged"
                });
                return Ok(s);
            }
            "folder-details" => {
                let status = self.control(request, &json!({"op":"status"}))?;
                let offset = usize::try_from(payload["offset"].as_u64().unwrap_or(0))?;
                let mut details = folder_details(&self.store, offset)?;
                details["status"] = status;
                return Ok(details);
            }

            "branches" => return Ok(json!(self.store.branches()?)),
            "conflicts" => return Ok(json!(self.store.conflicts()?)),
            "history" => return self.store.history(),
            "bucket-export" => {
                bail!("BUCKET_EXPORT_REQUIRES_PATH_VALIDATION");
            }
            "state" => return Ok(json!(self.store.projection(&self.store.active)?)),
            "cat-object" => {
                return Ok(json!({"hex":hex::encode(self.store.read_object(text("object")?)?)}));
            }
            "events" => return Ok(json!(self.store.events()?)),
            // WP4.INT re-review: the pending saves, and dropping one that
            // could not be restored (`Engine::discard_pending_save`). Neither
            // waits for the folder to be quiet: open files elsewhere must not
            // keep a broken record.
            "pending-saves" => return self.pending_saves_report(),
            "discard-pending-save" => return self.discard_pending_save(text("entity")?),
            _ => {}
        }
        self.quiet()?;
        if ["resolve", "recover"].contains(&op)
            && let Some(token) = payload["review_token"].as_str()
        {
            ensure!(
                token == hash(&serde_json::to_vec(&self.store.conflicts()?)?),
                "STALE_CONFLICT_REVIEW: refresh and review the changed alternatives"
            );
        }
        let before = self.store.projection(&self.store.active)?;
        // Semantic mutation and retry receipt share one transaction (an outer
        // savepoint, begun `IMMEDIATE` from autocommit). All inner journal,
        // current-state and outbox changes commit together with the receipt.
        let outer = begin_request(&self.store.db)?;
        let old_active = self.store.active.clone();
        let old_generation = self.store.generation;
        let result = (|| -> Result<Value> {
            let result = match op {
                "branch" => json!(self.store.fork(text("name")?, false)?),
                "publish" => {
                    ensure!(
                        payload["current_state_only"] == true,
                        "CURRENT_STATE_PUBLICATION_REQUIRES_EXPLICIT_FLAG"
                    );
                    json!(self.store.fork(text("name")?, true)?)
                }
                "checkout" => {
                    ensure!(!self.mounted, "MOUNT_REBIND_REQUIRED");
                    // Never leave an unrestored save of this branch behind
                    // (WP4.INT confirmation review).
                    self.checkout_allowed()?;
                    self.store.checkout(text("name")?)?;
                    json!({"branch":self.store.active,"generation":self.store.generation})
                }
                "checkpoint" => json!({"checkpoint":self.store.checkpoint(text("message")?)?}),
                "resolve" => {
                    let conflicts: Vec<String> =
                        serde_json::from_value(payload["conflicts"].clone())?;
                    json!({"revision":self.store.resolve(&conflicts,text("revision")?)?})
                }
                "recover" => {
                    let conflicts: Vec<String> =
                        serde_json::from_value(payload["conflicts"].clone())?;
                    json!({"revision":self.store.recover(text("entity")?,text("target")?,&conflicts)?})
                }
                "restore" => json!(self.store.restore(text("checkpoint")?, text("name")?)?),
                // Import is intentionally RPC: the CLI never opens SQLite or CAS.
                "import" => {
                    let path = text("path")?;
                    let prepared = prepared.context("CONTENT_IMPORT_REQUIRES_PREPARATION")?;
                    let e = match self.store.lookup(path) {
                        Ok(e) => e,
                        Err(_) => self.store.create(path, "file")?,
                    };
                    json!({"revision":self.store.write_prepared_content_with_base_and_times(&e.id, e.revisions.get("content").cloned(), e.revisions.get("alive").cloned(), prepared, 0)?})
                }
                "mkdir" => json!(self.store.create(text("path")?, "directory")?),
                "rename" => {
                    let e = self.store.lookup(text("path")?)?;
                    self.store
                        .rename(&e.id, text("target")?, payload["replace"] == true)?;
                    json!({"entity":e.id})
                }
                "delete" => {
                    let e = self.store.lookup(text("path")?)?;
                    self.store.delete(&e.id)?;
                    json!({"entity":e.id})
                }
                _ => bail!("UNKNOWN_OPERATION"),
            };
            self.store.db.execute(
                "INSERT INTO receipts VALUES(?,?,?)",
                rusqlite::params![
                    request,
                    fingerprint,
                    serde_json::to_string(&result)?
                ],
            )?;
            Ok(result)
        })()
        .and_then(|result| {
            commit_request(&self.store.db, outer)?;
            Ok(result)
        });
        if result.is_ok() {
            fault("receipt_committed");
            if op != "checkout" {
                self.notifications.extend(namespace_notifications(
                    &before,
                    &self.store.projection(&self.store.active)?,
                ));
            } else {
                self.notifications.clear();
                self.notification_error = None;
                // The saves of the branch now active can be restored now.
                self.unrecovered_due_now(std::time::Instant::now());
            }
        } else {
            self.store.active = old_active;
            self.store.generation = old_generation;
            rollback_request(&self.store.db, outer)?;
        }
        result
    }
}

/// Opens the transaction of one journaled control request and reports whether
/// it is the outermost one (the connection was in autocommit).
///
/// From autocommit the request starts with `BEGIN IMMEDIATE`, which takes
/// SQLite's writer slot before the request's first read. A deferred
/// transaction reads first and upgrades to a writer on its first write; when
/// another connection to the same WAL database (the bulk transfer view, a
/// pooled secondary store) holds the writer slot or has committed since the
/// read, that upgrade fails at once with `SQLITE_BUSY` or
/// `SQLITE_BUSY_SNAPSHOT`, without running the busy handler, so the 5 s
/// `busy_timeout` never applies. Inside a caller's transaction only the
/// savepoint is opened, as before.
fn begin_request(db: &rusqlite::Connection) -> Result<bool> {
    let outer = db.is_autocommit();
    if outer {
        db.execute_batch("BEGIN IMMEDIATE")?;
    }
    if let Err(error) = db.execute_batch("SAVEPOINT control_request") {
        if outer {
            let _ = db.execute_batch("ROLLBACK");
        }
        return Err(error.into());
    }
    Ok(outer)
}

/// Commits the request: the mutation and its receipt become durable together.
fn commit_request(db: &rusqlite::Connection, outer: bool) -> Result<()> {
    db.execute_batch("RELEASE control_request")?;
    if outer {
        db.execute_batch("COMMIT")?;
    }
    Ok(())
}

/// Discards everything the request wrote, including a failed commit.
fn rollback_request(db: &rusqlite::Connection, outer: bool) -> Result<()> {
    if !outer {
        db.execute_batch("ROLLBACK TO control_request; RELEASE control_request")?;
    } else if !db.is_autocommit() {
        // SQLite may already have rolled the transaction back (for example
        // after an I/O error); then there is nothing left to discard.
        db.execute_batch("ROLLBACK")?;
    }
    Ok(())
}

/// Bind an off-lock import to the same parent and selected file incarnation.
pub(crate) fn import_basis(store: &Store, path: &str) -> Result<(Entry, Option<Entry>)> {
    let (parent, _) = split_path(path)?;
    let parent = store.lookup(&parent)?;
    ensure!(parent.kind == "directory", "NOT_DIRECTORY");
    let entry = match store.lookup(path) {
        Ok(entry) => {
            ensure!(entry.kind == "file", "IS_DIRECTORY");
            Some(entry)
        }
        Err(error)
            if crate::model::errors::ErrorWire::from_error(&error).code == "FILE_NOT_FOUND" =>
        {
            None
        }
        Err(error) => return Err(error),
    };
    Ok((parent, entry))
}

/// A metadata snapshot and typed missing-content report. RPC callers run this
/// on a secondary connection, outside Engine ownership.
pub(crate) fn folder_details(store: &Store, offset: usize) -> Result<Value> {
    crate::util::fault::trigger("folder_details.snapshot")?;
    let conflicts = store.conflicts()?;
    let review_token = hash(&serde_json::to_vec(&conflicts)?);
    let unresolved: Vec<_> = conflicts
        .into_iter()
        .filter(|c| c.resolved_by.is_none())
        .collect();
    let conflict_count = unresolved.len();
    let conflicts: Vec<_> = unresolved.into_iter().skip(offset).take(100).collect();
    let needed: BTreeSet<_> = conflicts
        .iter()
        .flat_map(|c| c.alternatives.iter().map(|v| v.revision.clone()))
        .collect();
    let paths: BTreeMap<_, _> = visible_paths(&store.projection(&store.active)?)
        .into_iter()
        .map(|(path, entry)| (entry.id, path))
        .collect();
    let revision_entities: BTreeMap<_, Vec<_>> = store
        .events()?
        .into_iter()
        .filter(|e| needed.contains(&e.id))
        .map(|e| {
            (
                e.id,
                e.changes
                    .into_iter()
                    .filter(|c| c.field == "location")
                    .map(|c| c.entity)
                    .collect(),
            )
        })
        .collect();
    let (counters, content) = store.content_report()?;
    let missing: Vec<_> = content["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| json!({"path":item["path"],"content":item}))
        .collect();
    Ok(
        json!({"content":content,"content_counters":counters,"conflicts":conflicts,"paths":paths,"review_token":review_token,
        "revision_entities":revision_entities,"offset":offset,"conflict_count":conflict_count,"missing_content":missing}),
    )
}
