//! Owned by WP1.1 — the catalog actor (REVAMP_PLAN §3.2, §14 WP1.1).
//!
//! One thread, `tkfs-catalog`, takes `orchestrator.lock` (and shares it with
//! the [`Catalog`] handle) and owns the only connection to `folders.sqlite`. Everything else reads the immutable
//! [`CatalogSnapshot`] it publishes through a [`Snapshot`] cell, so a read is
//! an `Arc` clone that never waits for a write in progress (RC8).
//!
//! * [`Catalog::open`] takes the lock, opens the database with the legacy
//!   pragmas (`journal_mode=WAL; synchronous=FULL`), runs the legacy
//!   `CREATE TABLE IF NOT EXISTS` statements, then the additive schema of §9
//!   (guarded by `settings('catalog_schema','1')`): `folder_intents`,
//!   `mount_gates` (backfilled from `initial_sync`), `peer_meta` and the
//!   `verifier_generation` setting. Existing tables are never altered: the
//!   positional writers of a downgraded binary keep working.
//! * [`CatalogApi::apply`] runs each [`CatalogCmd`] in one `BEGIN IMMEDIATE`
//!   transaction with cached prepared statements, inside `catch_unwind`, with
//!   [`BUSY_RETRIES`] retries on `SQLITE_BUSY`. The snapshot that includes the
//!   change is built inside the same transaction, published after the commit,
//!   and returned. A command that changes nothing visible publishes nothing;
//!   every other successful command bumps the generation by exactly one.
//!   The caller waits at most [`APPLY_WAIT`] (a slow write never parks a
//!   folder controller or a control connection for good: WP2.INT's
//!   post-integration re-review).
//! * If the thread has died, `apply` answers `CATALOG_UNAVAILABLE` and
//!   `snapshot()` keeps serving the last good snapshot. The thread and the
//!   [`Catalog`] handle share `orchestrator.lock`, so a dead thread never
//!   frees the data directory while the daemon still runs on its handle
//!   (invariant 23): the lock is released once the thread has exited **and**
//!   the handle is closed or dropped.
//! * Gate markers are written twice: the legacy `initial_sync` row (identical
//!   positional SQL) and the `mount_gates` row. Both are deleted together by
//!   `ClearGate`, so a downgraded binary still sees every gate. Changes a
//!   downgraded binary makes to `initial_sync` (a legacy Receive adds a row,
//!   a satisfied legacy gate deletes one) are folded into `mount_gates` on the
//!   next open, and a pairing code it rotated bumps `verifier_generation`
//!   (the setting `verifier_salt` records which verifier the generation
//!   belongs to).
//! * Generations are per process (the first snapshot is generation 1). Every
//!   visible change wakes every `wait_newer` caller, so consumers diff the
//!   records they care about, and `RecordPeerSeen` belongs on session
//!   establishment rather than on every request.
//!
//! [`validate_mount_path`] is the mount-path rule of the legacy
//! `Supervisor::mount_path`, as a function of a snapshot.
//!
//! Fault points ([`faults`]): `catalog.slow_write` fires inside every command
//! transaction, after its writes and before the commit (`hang` holds the
//! transaction, `fail` and `panic` abort it); `catalog.actor` fires on the
//! actor loop outside the per-command guard (`panic` kills the thread).
use super::{Config, lock};
use crate::{
    core::id,
    model::{
        errors::{ErrorWire, code_of},
        orchestrator::{
            CatalogApi, CatalogCmd, CatalogSnapshot, FolderRecord, GateRecord, PeerMeta, PeerRecord,
        },
    },
    pairing_code::Verifier,
    util::{fault, lock::Tracked, log::event, snapshot::Snapshot, time::now_ms},
};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::{
    any::Any,
    collections::BTreeMap,
    fmt,
    fs::{self, File},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, PoisonError,
        atomic::{AtomicU8, AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

/// The actor thread's name (§2.5).
pub const THREAD_NAME: &str = "tkfs-catalog";
/// The additive schema version this build writes (`settings.catalog_schema`).
pub const SCHEMA_VERSION: u32 = 1;
/// Retries of a command that failed with `SQLITE_BUSY` (so at most 4 attempts).
pub const BUSY_RETRIES: u32 = 3;
/// SQLite busy handler budget per attempt. Only the catalog writes the file,
/// so a busy writer slot means an external tool holds it.
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(1);
/// Pause before retry `n` is `n * BUSY_BACKOFF`.
pub const BUSY_BACKOFF: Duration = Duration::from_millis(50);
/// How long [`Catalog::close`] (and `Drop`) waits for the thread to exit
/// ([`Catalog::close_within`] takes another budget).
pub const CLOSE_WAIT: Duration = Duration::from_secs(5);
/// Longest folder label the catalog accepts (the legacy `create()` rule).
pub const MAX_LABEL_BYTES: usize = 200;
/// How long [`CatalogApi::apply`] waits for its command's answer, the
/// commands queued ahead of it included (§2.3: a catalog fault may stop
/// management writes, never the threads that asked for them, a folder's
/// lifecycle or status; post-integration re-review, WP2.INT). A command that
/// has not started by then is withdrawn and never runs
/// (`CATALOG_UNAVAILABLE`); one that is running is answered
/// `REQUEST_TIMEOUT` (it may still be applied). While one command has been
/// running this long, new ones are refused at once (`CATALOG_UNAVAILABLE`)
/// instead of queueing behind it. Above the [`BUSY_RETRIES`] budget of a
/// writer slot held by an external tool (about 4.3 s).
pub const APPLY_WAIT: Duration = Duration::from_secs(5);

/// A queued command's ticket ([`Request::ticket`]).
const QUEUED: u8 = 0;
const STARTED: u8 = 1;
const WITHDRAWN: u8 = 2;

/// Fault points of the catalog actor (`util::fault`).
pub mod faults {
    /// Inside every command transaction, after the writes, before the commit.
    pub const SLOW_WRITE: &str = "catalog.slow_write";
    /// On the actor loop, outside the per-command `catch_unwind`.
    pub const ACTOR: &str = "catalog.actor";
}

/// `settings` keys owned by the catalog.
mod keys {
    pub const INSTALLATION: &str = "installation";
    pub const PAIRING: &str = "pairing";
    pub const CATALOG_SCHEMA: &str = "catalog_schema";
    pub const VERIFIER_GENERATION: &str = "verifier_generation";
    /// The salt of the verifier `verifier_generation` belongs to, so a code
    /// rotated by a downgraded binary still bumps the generation.
    pub const VERIFIER_SALT: &str = "verifier_salt";
}

/// SQL text. The `LEGACY_*` and positional statements are byte-identical to
/// the legacy `Supervisor` (`catalog_db.rs`, 6298d0b).
mod sql {
    pub const LEGACY_SCHEMA: &str = "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS projects(state TEXT PRIMARY KEY,label TEXT NOT NULL,repo TEXT NOT NULL UNIQUE,device TEXT NOT NULL,directory TEXT NOT NULL,mount TEXT,desired INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS peers(endpoint TEXT PRIMARY KEY,code TEXT NOT NULL,installation TEXT NOT NULL,connections INTEGER NOT NULL,revision TEXT NOT NULL);";
    pub const LEGACY_INITIAL_SYNC: &str =
        "CREATE TABLE IF NOT EXISTS initial_sync(state TEXT PRIMARY KEY,endpoint TEXT NOT NULL);";
    pub const INSERT_INSTALLATION: &str = "INSERT OR IGNORE INTO settings VALUES('installation',?)";
    pub const INSERT_PROJECT: &str = "INSERT INTO projects VALUES(?,?,?,?,?,?,1)";
    pub const SET_DESIRED_1: &str = "UPDATE projects SET desired=1 WHERE state=?";
    pub const SET_DESIRED_0: &str = "UPDATE projects SET desired=0 WHERE state=?";
    pub const SET_MOUNT: &str = "UPDATE projects SET mount=? WHERE state=?";
    pub const INSERT_INITIAL_SYNC: &str = "INSERT INTO initial_sync VALUES(?,?)";
    pub const DELETE_INITIAL_SYNC: &str = "DELETE FROM initial_sync WHERE state=?";
    pub const UPSERT_PEER: &str = "INSERT INTO peers VALUES(?,?,?,?,?) ON CONFLICT(endpoint) DO UPDATE SET code=excluded.code,installation=excluded.installation,connections=excluded.connections,revision=excluded.revision";
    pub const DELETE_PEER: &str = "DELETE FROM peers WHERE endpoint=?";
    pub const SET_PAIRING: &str = "INSERT INTO settings VALUES('pairing',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value";

    // Schema 1 (§9): side tables only.
    pub const SCHEMA_1: &str = "CREATE TABLE IF NOT EXISTS folder_intents(state TEXT PRIMARY KEY,sync_enabled INTEGER NOT NULL,mount_enabled INTEGER NOT NULL,updated_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS mount_gates(state TEXT PRIMARY KEY,source_installation TEXT,source_endpoint TEXT NOT NULL,created_ms INTEGER NOT NULL,override_ms INTEGER,satisfied_ms INTEGER);
            CREATE TABLE IF NOT EXISTS peer_meta(installation TEXT PRIMARY KEY,last_seen_ms INTEGER,display_name TEXT,incompatible TEXT,revoked_ms INTEGER);
            INSERT OR IGNORE INTO settings VALUES('verifier_generation','0');";
    pub const GET_SETTING: &str = "SELECT value FROM settings WHERE key=?";
    pub const SET_SETTING: &str =
        "INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
    /// `initial_sync` rows without a `mount_gates` row: the schema-1 backfill,
    /// and gates a downgraded binary created (legacy Receive).
    pub const BACKFILL_GATES: &str = "INSERT OR IGNORE INTO mount_gates(state,source_installation,source_endpoint,created_ms) SELECT s.state,(SELECT p.installation FROM peers p WHERE p.endpoint=s.endpoint),s.endpoint,? FROM initial_sync s";
    /// `mount_gates` rows whose `initial_sync` row is gone: gates a downgraded
    /// binary cleared after its own gate rule was satisfied.
    pub const PRUNE_GATES: &str =
        "DELETE FROM mount_gates WHERE state NOT IN (SELECT state FROM initial_sync)";
    /// Resolves `source_endpoint → installation` through `peers` where unknown.
    pub const RESOLVE_GATES: &str = "UPDATE mount_gates SET source_installation=(SELECT p.installation FROM peers p WHERE p.endpoint=mount_gates.source_endpoint) WHERE source_installation IS NULL";

    pub const FOLDERS: &str = "SELECT p.state,p.label,p.repo,p.device,p.directory,p.mount,p.desired,i.sync_enabled,i.mount_enabled FROM projects p LEFT JOIN folder_intents i ON i.state=p.state ORDER BY p.label,p.state";
    pub const PEERS: &str =
        "SELECT endpoint,code,installation,connections,revision FROM peers ORDER BY endpoint";
    pub const GATES: &str = "SELECT state,source_installation,source_endpoint,created_ms,override_ms FROM mount_gates ORDER BY state";
    pub const PEER_META: &str = "SELECT installation,last_seen_ms,display_name,incompatible,revoked_ms FROM peer_meta ORDER BY installation";
    pub const FOLDER_EXISTS: &str = "SELECT 1 FROM projects WHERE state=?";
    pub const REPO_EXISTS: &str = "SELECT 1 FROM projects WHERE repo=?";
    pub const PEER_INSTALLATION: &str = "SELECT installation FROM peers WHERE endpoint=?";
    pub const INTENTS: &str = "SELECT sync_enabled,mount_enabled FROM folder_intents WHERE state=?";
    pub const UPSERT_INTENTS: &str = "INSERT INTO folder_intents(state,sync_enabled,mount_enabled,updated_ms) VALUES(?,?,?,?) ON CONFLICT(state) DO UPDATE SET sync_enabled=excluded.sync_enabled,mount_enabled=excluded.mount_enabled,updated_ms=excluded.updated_ms";
    pub const INSERT_GATE: &str = "INSERT OR REPLACE INTO mount_gates(state,source_installation,source_endpoint,created_ms,override_ms,satisfied_ms) VALUES(?,?,?,?,?,NULL)";
    pub const OVERRIDE_GATE: &str =
        "UPDATE mount_gates SET override_ms=COALESCE(override_ms,?) WHERE state=?";
    pub const DELETE_GATE: &str = "DELETE FROM mount_gates WHERE state=?";
    pub const RETARGET_GATE: &str = "UPDATE mount_gates SET source_installation=?,source_endpoint=?,satisfied_ms=NULL WHERE state=?";
    pub const RETARGET_INITIAL_SYNC: &str = "UPDATE initial_sync SET endpoint=? WHERE state=?";
    pub const RECORD_PEER_SEEN: &str = "INSERT INTO peer_meta(installation,last_seen_ms,display_name,incompatible) VALUES(?,?,?,?) ON CONFLICT(installation) DO UPDATE SET last_seen_ms=MAX(COALESCE(last_seen_ms,0),excluded.last_seen_ms),display_name=COALESCE(excluded.display_name,display_name),incompatible=excluded.incompatible";
}

// ---------------------------------------------------------------------------
// The public handle

struct Request {
    cmd: CatalogCmd,
    reply: SyncSender<Result<Arc<CatalogSnapshot>, ErrorWire>>,
    /// `QUEUED` until the actor starts the command (`STARTED`), or until the
    /// caller gave up first (`WITHDRAWN`: the actor skips it).
    ticket: Arc<AtomicU8>,
}

/// The catalog actor's handle (implements [`CatalogApi`]). Dropping the last
/// `Arc` closes the mailbox, lets queued commands finish, joins the thread
/// and releases `orchestrator.lock` ([`Catalog::close`]). The lock is held
/// while the thread **or** the handle is alive, so a thread that died leaves
/// the data directory owned by the running daemon (invariant 23).
pub struct Catalog {
    data_directory: PathBuf,
    cell: Arc<Snapshot<CatalogSnapshot>>,
    mailbox: Tracked<Option<Sender<Request>>>,
    /// Set by the thread's last local as it ends, normally or by a panic.
    exit: Arc<Exit>,
    thread: Mutex<Option<JoinHandle<()>>>,
    /// The handle's share of `orchestrator.lock` (the thread holds the other).
    /// Dropped by a `close` that saw the thread exit, or with the handle, so
    /// the data directory stays owned for as long as either is alive.
    lock: Mutex<Option<Arc<File>>>,
    /// When the command the actor is running started (`now_ms`), 0 while it
    /// is idle ([`APPLY_WAIT`]).
    busy_since_ms: Arc<AtomicU64>,
}

impl Catalog {
    /// Starts the `tkfs-catalog` thread on `config.data_directory` and returns
    /// once `orchestrator.lock` is held, the schema is migrated and the first
    /// snapshot (generation 1) is published. Like the legacy
    /// `Supervisor::open`, it creates the data and default mount directories.
    ///
    /// Fails with `ALREADY_RUNNING…` while another orchestrator (or catalog)
    /// holds the lock (invariant 23).
    pub fn open(config: &Config) -> Result<Arc<Catalog>> {
        fs::create_dir_all(&config.data_directory)?;
        fs::create_dir_all(&config.default_mount_directory)?;
        let data_directory = config.data_directory.clone();
        let cell = Arc::new(Snapshot::new(CatalogSnapshot::default()));
        let exit = Arc::new(Exit::default());
        let busy_since_ms = Arc::new(AtomicU64::new(0));
        let (mailbox, requests) = mpsc::channel::<Request>();
        let (opened_tx, opened_rx) = mpsc::sync_channel::<Result<Arc<File>>>(1);
        let thread = {
            let data = data_directory.clone();
            let cell = cell.clone();
            let exit = ExitGuard(exit.clone());
            let busy = busy_since_ms.clone();
            thread::Builder::new()
                .name(THREAD_NAME.into())
                .spawn(move || {
                    // Declared first, so dropped last (also when unwinding):
                    // the exit is signalled after the actor, its connection
                    // and its share of the lock are gone.
                    let _exit = exit;
                    let actor = match Actor::open(&data, cell) {
                        Ok(actor) => actor,
                        Err(error) => {
                            let _ = opened_tx.send(Err(error));
                            return;
                        }
                    };
                    let _ = opened_tx.send(Ok(actor.lock.clone()));
                    drop(opened_tx);
                    actor.run(requests, &busy);
                })
                .context("CATALOG_UNAVAILABLE: cannot spawn the catalog thread")?
        };
        match opened_rx.recv() {
            Ok(Ok(lock)) => Ok(Arc::new(Catalog {
                data_directory,
                cell,
                mailbox: Tracked::new("catalog.mailbox", Some(mailbox)),
                exit,
                thread: Mutex::new(Some(thread)),
                lock: Mutex::new(Some(lock)),
                busy_since_ms,
            })),
            failed => {
                let _ = thread.join();
                match failed {
                    Ok(Err(error)) => Err(error),
                    _ => bail!("CATALOG_UNAVAILABLE: the catalog thread died while opening"),
                }
            }
        }
    }

    /// The data directory whose `folders.sqlite` this catalog owns.
    pub fn data_directory(&self) -> &Path {
        &self.data_directory
    }

    /// Whether the actor thread is running and accepting commands (false
    /// once it has died or [`close`](Self::close) was called).
    pub fn is_alive(&self) -> bool {
        !self.exit.has_exited() && self.mailbox.lock().is_some()
    }

    /// [`close_within`](Self::close_within) [`CLOSE_WAIT`].
    pub fn close(&self) -> bool {
        self.close_within(CLOSE_WAIT)
    }

    /// Stops accepting commands (`apply` answers `CATALOG_UNAVAILABLE`), lets
    /// the queued ones finish, and waits up to `budget` for the thread to
    /// exit, which closes the database. Returns whether the thread has
    /// exited; only then is the handle's share of `orchestrator.lock`
    /// released, so `true` means the data directory is free. A `false`
    /// (a wedged command) settles nothing: every later or concurrent call
    /// waits for the same exit again. `snapshot()` keeps working.
    pub fn close_within(&self, budget: Duration) -> bool {
        self.mailbox.lock().take();
        if !self.exit.wait(budget) {
            event!(warn, "CATALOG_UNAVAILABLE", {
                "detail": "the catalog thread did not exit within the close budget",
                "wait_ms": u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
            });
            return false;
        }
        if let Some(thread) = crate::util::lock::lock(&self.thread).take() {
            let _ = thread.join();
        }
        // Assigned under the mutex, so the file is closed before any other
        // caller can observe `None` and report the directory free.
        *crate::util::lock::lock(&self.lock) = None;
        true
    }
}

impl CatalogApi for Catalog {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.cell.load()
    }

    /// Bounded by [`APPLY_WAIT`] (see there).
    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        if self.exit.has_exited() {
            return Err(unavailable("the catalog thread has exited"));
        }
        let wait_ms = u64::try_from(APPLY_WAIT.as_millis()).unwrap_or(u64::MAX);
        let since = self.busy_since_ms.load(Ordering::Acquire);
        if since != 0 && now_ms().saturating_sub(since) >= wait_ms {
            return Err(unavailable(format!(
                "a catalog write has been running for {} ms; {} was not queued behind it",
                now_ms().saturating_sub(since),
                cmd.name()
            )));
        }
        let name = cmd.name();
        let ticket = Arc::new(AtomicU8::new(QUEUED));
        let (reply, answer) = mpsc::sync_channel(1);
        {
            let mailbox = self.mailbox.lock();
            let Some(sender) = mailbox.as_ref() else {
                return Err(unavailable("the catalog is closed"));
            };
            let request = Request {
                cmd,
                reply,
                ticket: ticket.clone(),
            };
            if sender.send(request).is_err() {
                return Err(unavailable("the catalog thread has exited"));
            }
        }
        match answer.recv_timeout(APPLY_WAIT) {
            Ok(result) => result,
            Err(RecvTimeoutError::Disconnected) => {
                Err(unavailable("the catalog thread died running the command"))
            }
            Err(RecvTimeoutError::Timeout) => {
                let withdrawn = ticket
                    .compare_exchange(QUEUED, WITHDRAWN, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
                if withdrawn {
                    Err(unavailable(format!(
                        "the catalog did not take {name} within {wait_ms} ms; it was withdrawn \
                         and not applied"
                    )))
                } else {
                    Err(ErrorWire::coded(
                        "REQUEST_TIMEOUT",
                        format!(
                            "the catalog command {name} did not finish within {wait_ms} ms; it \
                             may still be applied"
                        ),
                    ))
                }
            }
        }
    }

    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<CatalogSnapshot> {
        self.cell.wait_newer(generation, timeout);
        self.cell.load()
    }
}

impl Drop for Catalog {
    fn drop(&mut self) {
        self.close();
    }
}

impl fmt::Debug for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Catalog")
            .field("data_directory", &self.data_directory)
            .field("generation", &self.cell.generation())
            .field("alive", &self.is_alive())
            .finish_non_exhaustive()
    }
}

fn unavailable(detail: impl fmt::Display) -> ErrorWire {
    ErrorWire::coded("CATALOG_UNAVAILABLE", detail)
}

/// Whether the actor thread has ended; waiters block on the condvar.
#[derive(Default)]
struct Exit {
    exited: Mutex<bool>,
    changed: Condvar,
}

impl Exit {
    fn has_exited(&self) -> bool {
        *crate::util::lock::lock(&self.exited)
    }

    /// Waits up to `budget` for the exit; returns whether it happened.
    fn wait(&self, budget: Duration) -> bool {
        let exited = crate::util::lock::lock(&self.exited);
        let (exited, _) = self
            .changed
            .wait_timeout_while(exited, budget, |exited| !*exited)
            .unwrap_or_else(PoisonError::into_inner);
        *exited
    }
}

/// Signals [`Exit`] when the thread ends, including by a panic.
struct ExitGuard(Arc<Exit>);

impl Drop for ExitGuard {
    fn drop(&mut self) {
        *crate::util::lock::lock(&self.0.exited) = true;
        self.0.changed.notify_all();
    }
}

// ---------------------------------------------------------------------------
// The actor

struct Actor {
    // Field order is drop order: the connection closes before the actor's
    // share of the lock is dropped (the handle holds the other share).
    conn: Connection,
    cell: Arc<Snapshot<CatalogSnapshot>>,
    current: Arc<CatalogSnapshot>,
    lock: Arc<File>,
}

impl Actor {
    fn open(data: &Path, cell: Arc<Snapshot<CatalogSnapshot>>) -> Result<Actor> {
        let lock = lock(&data.join("orchestrator.lock"))?;
        let mut conn = Connection::open(data.join("folders.sqlite"))?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        conn.set_prepared_statement_cache_capacity(64);
        // Every step is idempotent, so a busy writer slot is retried like a
        // command's.
        with_busy_retries(|| {
            conn.execute_batch(sql::LEGACY_SCHEMA)?;
            conn.execute_batch(sql::LEGACY_INITIAL_SYNC)?;
            conn.execute(sql::INSERT_INSTALLATION, [id()])?;
            migrate(&mut conn, now_ms())
        })?;
        let first = Arc::new(load_snapshot(&conn, cell.generation() + 1)?);
        let published = cell.store_arc(first.clone());
        debug_assert_eq!(published, first.generation);
        Ok(Actor {
            conn,
            cell,
            current: first,
            lock: Arc::new(lock),
        })
    }

    fn run(mut self, requests: Receiver<Request>, busy_since_ms: &AtomicU64) {
        while let Ok(request) = requests.recv() {
            // A command whose caller gave up waiting ([`APPLY_WAIT`]) never runs.
            if request
                .ticket
                .compare_exchange(QUEUED, STARTED, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            busy_since_ms.store(now_ms().max(1), Ordering::Release);
            // Outside the per-command guard on purpose: a `panic` action here
            // kills the thread, which is how tests reach CATALOG_UNAVAILABLE.
            if let Err(error) = fault::trigger(faults::ACTOR) {
                busy_since_ms.store(0, Ordering::Release);
                let _ = request.reply.send(Err(ErrorWire::from_error(&error)));
                continue;
            }
            let result = self.execute(request.cmd);
            busy_since_ms.store(0, Ordering::Release);
            let _ = request.reply.send(result);
        }
    }

    fn execute(&mut self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        let name = cmd.name();
        let mut retries = 0;
        loop {
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| self.transact(&cmd)));
            let error = match outcome {
                Ok(Ok(Some(next))) => return Ok(self.publish(next)),
                Ok(Ok(None)) => return Ok(self.current.clone()),
                Ok(Err(error)) => ErrorWire::from_error(&error),
                Err(payload) => {
                    // The transaction guard rolled back while unwinding; make
                    // sure nothing is left open on the connection.
                    self.rollback_leftovers();
                    let message = panic_message(payload.as_ref());
                    event!(error, "CATALOG_UNAVAILABLE", {
                        "command": name,
                        "panic": message,
                    });
                    return Err(ErrorWire::coded(
                        "CATALOG_UNAVAILABLE",
                        format!("catalog command {name} panicked and was rolled back: {message}"),
                    ));
                }
            };
            self.rollback_leftovers();
            if error.code == "SQLITE_BUSY" && retries < BUSY_RETRIES {
                retries += 1;
                thread::sleep(BUSY_BACKOFF * retries);
                continue;
            }
            // A rejected command is the caller's to report; only a writer slot
            // that never freed up is a catalog problem worth a warning.
            if error.code == "SQLITE_BUSY" {
                event!(warn, "SQLITE_BUSY", {
                    "command": name,
                    "message": error.message,
                    "retries": retries,
                });
            } else {
                event!(info, (error.code.clone()), {
                    "command": name,
                    "message": error.message,
                });
            }
            return Err(error);
        }
    }

    /// Runs one command in one transaction. `Some(snapshot)` when the visible
    /// catalog changed, `None` when the command was a no-op.
    fn transact(&mut self, cmd: &CatalogCmd) -> Result<Option<CatalogSnapshot>> {
        let current = &self.current;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        apply_command(&tx, cmd, current, now_ms())?;
        let next = load_snapshot(&tx, current.generation + 1)?;
        fault::trigger(faults::SLOW_WRITE)?;
        tx.commit()?;
        Ok((!same_content(&next, current)).then_some(next))
    }

    fn publish(&mut self, next: CatalogSnapshot) -> Arc<CatalogSnapshot> {
        let next = Arc::new(next);
        let published = self.cell.store_arc(next.clone());
        debug_assert_eq!(published, next.generation);
        self.current = next.clone();
        next
    }

    fn rollback_leftovers(&self) {
        if !self.conn.is_autocommit() {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

/// Runs `step` again (at most [`BUSY_RETRIES`] times) while it fails with
/// `SQLITE_BUSY`.
fn with_busy_retries<T>(mut step: impl FnMut() -> Result<T>) -> Result<T> {
    let mut retries = 0;
    loop {
        match step() {
            Err(error) if code_of(&error) == "SQLITE_BUSY" && retries < BUSY_RETRIES => {
                retries += 1;
                thread::sleep(BUSY_BACKOFF * retries);
            }
            outcome => return outcome,
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

/// Schema migration (guarded by `catalog_schema`) and the legacy reconcile
/// (every open), in one transaction.
fn migrate(conn: &mut Connection, now: u64) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let schema = get_setting(&tx, keys::CATALOG_SCHEMA)?
        .and_then(|value| value.trim().parse::<u32>().ok())
        .unwrap_or(0);
    if schema < SCHEMA_VERSION {
        tx.execute_batch(sql::SCHEMA_1)?;
        set_setting(&tx, keys::CATALOG_SCHEMA, &SCHEMA_VERSION.to_string())?;
    }
    reconcile_legacy(&tx, now)?;
    tx.commit()?;
    Ok(())
}

/// Folds what a downgraded binary may have changed into the side tables:
/// gate rows it added or removed in `initial_sync`, peers it paired (which
/// resolve gate sources), and a pairing code it rotated (which bumps
/// `verifier_generation`). Idempotent; also the schema-1 backfill.
fn reconcile_legacy(tx: &Transaction<'_>, now: u64) -> Result<()> {
    tx.execute(sql::BACKFILL_GATES, [ms(now)])?;
    tx.execute(sql::PRUNE_GATES, [])?;
    tx.execute(sql::RESOLVE_GATES, [])?;
    if let Some(verifier) = read_verifier(tx)? {
        let recorded = get_setting(tx, keys::VERIFIER_SALT)?;
        if recorded.as_deref() != Some(verifier.salt.as_str()) {
            bump_verifier_generation(tx, &verifier)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Commands

fn apply_command(
    tx: &Transaction<'_>,
    cmd: &CatalogCmd,
    current: &CatalogSnapshot,
    now: u64,
) -> Result<()> {
    match cmd {
        CatalogCmd::CreateFolder { record, gate } => create_folder(tx, record, gate, current, now),
        CatalogCmd::SetDesired { state, desired } => {
            let statement = if *desired {
                sql::SET_DESIRED_1
            } else {
                sql::SET_DESIRED_0
            };
            if tx.prepare_cached(statement)?.execute([state])? == 0 {
                bail!("FOLDER_NOT_FOUND: folder {state} is not in the catalog");
            }
            Ok(())
        }
        CatalogCmd::SetMount { state, mount } => {
            require_folder(tx, state)?;
            if let Some(mount) = mount {
                check_mount_free(current, state, mount)?;
            }
            tx.prepare_cached(sql::SET_MOUNT)?
                .execute(params![mount.as_deref().map(path_text), state])?;
            Ok(())
        }
        CatalogCmd::SetIntents {
            state,
            sync_enabled,
            mount_enabled,
        } => {
            require_folder(tx, state)?;
            let (sync, mount) = tx
                .prepare_cached(sql::INTENTS)?
                .query_row([state], |r| {
                    Ok((r.get::<_, bool>(0)?, r.get::<_, bool>(1)?))
                })
                .optional()?
                .unwrap_or((true, true));
            upsert_intents(
                tx,
                state,
                sync_enabled.unwrap_or(sync),
                mount_enabled.unwrap_or(mount),
                now,
            )
        }
        CatalogCmd::OverrideGate { state } => {
            require_folder(tx, state)?;
            tx.prepare_cached(sql::OVERRIDE_GATE)?
                .execute(params![ms(now), state])?;
            Ok(())
        }
        CatalogCmd::ClearGate { state } => {
            require_folder(tx, state)?;
            tx.prepare_cached(sql::DELETE_INITIAL_SYNC)?
                .execute([state])?;
            tx.prepare_cached(sql::DELETE_GATE)?.execute([state])?;
            Ok(())
        }
        CatalogCmd::RetargetGate {
            state,
            source_installation,
            source_endpoint,
        } => {
            require_folder(tx, state)?;
            if source_endpoint.is_empty() && source_installation.is_none() {
                bail!("INVALID_REQUEST: a gate source needs an endpoint or an installation");
            }
            let installation = match source_installation {
                Some(installation) => Some(installation.clone()),
                None => peer_installation(tx, source_endpoint)?,
            };
            tx.prepare_cached(sql::RETARGET_GATE)?.execute(params![
                installation,
                source_endpoint,
                state
            ])?;
            // Keep the legacy marker pointing at the same source for a downgrade.
            tx.prepare_cached(sql::RETARGET_INITIAL_SYNC)?
                .execute(params![source_endpoint, state])?;
            Ok(())
        }
        CatalogCmd::UpsertPeer { record } => upsert_peer(tx, record, current),
        CatalogCmd::RemovePeer { endpoint } => {
            tx.prepare_cached(sql::DELETE_PEER)?.execute([endpoint])?;
            Ok(())
        }
        CatalogCmd::SetVerifier { verifier } => {
            tx.prepare_cached(sql::SET_PAIRING)?
                .execute([serde_json::to_string(verifier)?])?;
            bump_verifier_generation(tx, verifier)
        }
        CatalogCmd::RecordPeerSeen {
            installation,
            at_ms,
            display_name,
            incompatible,
        } => {
            if installation.is_empty() {
                bail!("INVALID_REQUEST: a peer installation is required");
            }
            tx.prepare_cached(sql::RECORD_PEER_SEEN)?.execute(params![
                installation,
                ms(*at_ms),
                display_name,
                incompatible
            ])?;
            Ok(())
        }
    }
}

fn create_folder(
    tx: &Transaction<'_>,
    record: &FolderRecord,
    gate: &Option<GateRecord>,
    current: &CatalogSnapshot,
    now: u64,
) -> Result<()> {
    if record.state.is_empty() || record.repo.is_empty() || record.device.is_empty() {
        bail!("INVALID_REQUEST: a folder needs a state, a repository and a device id");
    }
    if record.label.trim().is_empty() || record.label.len() > MAX_LABEL_BYTES {
        bail!("INVALID_FOLDER_LABEL");
    }
    if exists(tx, sql::FOLDER_EXISTS, &record.state)? {
        bail!(
            "INVALID_REQUEST: folder {} is already in the catalog",
            record.state
        );
    }
    if exists(tx, sql::REPO_EXISTS, &record.repo)? {
        bail!("REPOSITORY_ALREADY_MANAGED");
    }
    if let Some(mount) = &record.mount {
        check_mount_free(current, &record.state, mount)?;
    }
    tx.prepare_cached(sql::INSERT_PROJECT)?.execute(params![
        record.state,
        record.label,
        record.repo,
        record.device,
        path_text(&record.directory),
        record.mount.as_deref().map(path_text),
    ])?;
    if !record.desired_running {
        tx.prepare_cached(sql::SET_DESIRED_0)?
            .execute([&record.state])?;
    }
    upsert_intents(
        tx,
        &record.state,
        record.sync_enabled,
        record.mount_enabled,
        now,
    )?;
    // Like the legacy `create()`, a gate only exists for a folder that has a
    // mount path: it holds the mount, and a headless folder has none to hold.
    if let (Some(gate), Some(_)) = (gate, &record.mount) {
        if gate.source_endpoint.is_empty() && gate.source_installation.is_none() {
            bail!("INVALID_REQUEST: a gate source needs an endpoint or an installation");
        }
        tx.prepare_cached(sql::INSERT_INITIAL_SYNC)?
            .execute(params![record.state, gate.source_endpoint])?;
        let installation = match &gate.source_installation {
            Some(installation) => Some(installation.clone()),
            None => peer_installation(tx, &gate.source_endpoint)?,
        };
        let created_ms = if gate.created_ms == 0 {
            now
        } else {
            gate.created_ms
        };
        tx.prepare_cached(sql::INSERT_GATE)?.execute(params![
            record.state,
            installation,
            gate.source_endpoint,
            ms(created_ms),
            gate.override_ms.map(ms),
        ])?;
    }
    Ok(())
}

fn upsert_peer(tx: &Transaction<'_>, record: &PeerRecord, current: &CatalogSnapshot) -> Result<()> {
    if record.endpoint.is_empty() || record.installation.is_empty() {
        bail!("INVALID_REQUEST: a peer needs an endpoint and an installation");
    }
    if record.installation == current.installation {
        bail!("CANNOT_PAIR_WITH_SELF");
    }
    if !(1..=16).contains(&record.connections) {
        bail!("INVALID_CONNECTION_COUNT");
    }
    // A new revision restarts the peer's lanes (legacy `upsert_peer`). The
    // network layer passes one (§14 WP2.6); an empty one gets a fresh id.
    let revision = if record.revision.is_empty() {
        id()
    } else {
        record.revision.clone()
    };
    tx.prepare_cached(sql::UPSERT_PEER)?.execute(params![
        record.endpoint,
        record.code,
        record.installation,
        record.connections,
        revision,
    ])?;
    tx.prepare_cached(sql::RESOLVE_GATES)?.execute([])?;
    Ok(())
}

fn upsert_intents(
    tx: &Transaction<'_>,
    state: &str,
    sync_enabled: bool,
    mount_enabled: bool,
    now: u64,
) -> Result<()> {
    tx.prepare_cached(sql::UPSERT_INTENTS)?.execute(params![
        state,
        sync_enabled,
        mount_enabled,
        ms(now)
    ])?;
    Ok(())
}

fn bump_verifier_generation(tx: &Transaction<'_>, verifier: &Verifier) -> Result<()> {
    let next = verifier_generation(tx)?.saturating_add(1);
    set_setting(tx, keys::VERIFIER_GENERATION, &next.to_string())?;
    set_setting(tx, keys::VERIFIER_SALT, &verifier.salt)
}

fn require_folder(tx: &Transaction<'_>, state: &str) -> Result<()> {
    if !exists(tx, sql::FOLDER_EXISTS, state)? {
        bail!("FOLDER_NOT_FOUND: folder {state} is not in the catalog");
    }
    Ok(())
}

/// The mount path of `state` must not overlap another folder's mount. Re-run
/// inside the command so two concurrent creates validated against the same
/// snapshot cannot both win (invariant 27); purely lexical, no filesystem.
fn check_mount_free(current: &CatalogSnapshot, state: &str, mount: &Path) -> Result<()> {
    for folder in current
        .folders
        .iter()
        .filter(|folder| folder.state != state)
    {
        if let Some(other) = &folder.mount
            && (mount.starts_with(other) || other.starts_with(mount))
        {
            bail!(
                "MOUNT_OVERLAPS_FOLDER: {} overlaps the mount of folder {} ({})",
                mount.display(),
                folder.label,
                other.display()
            );
        }
    }
    Ok(())
}

fn exists(conn: &Connection, statement: &str, key: &str) -> Result<bool> {
    Ok(conn
        .prepare_cached(statement)?
        .query_row([key], |_| Ok(()))
        .optional()?
        .is_some())
}

fn peer_installation(conn: &Connection, endpoint: &str) -> Result<Option<String>> {
    Ok(conn
        .prepare_cached(sql::PEER_INSTALLATION)?
        .query_row([endpoint], |r| r.get(0))
        .optional()?)
}

fn get_setting(conn: &Connection, key: &str) -> Result<Option<String>> {
    Ok(conn
        .prepare_cached(sql::GET_SETTING)?
        .query_row([key], |r| r.get(0))
        .optional()?)
}

fn set_setting(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.prepare_cached(sql::SET_SETTING)?
        .execute([key, value])?;
    Ok(())
}

fn verifier_generation(conn: &Connection) -> Result<u64> {
    Ok(get_setting(conn, keys::VERIFIER_GENERATION)?
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0))
}

/// The stored pairing verifier. An unparseable value reads as "not set",
/// which is what the legacy `verifier()` reports (`PAIRING_CODE_NOT_SET`).
fn read_verifier(conn: &Connection) -> Result<Option<Verifier>> {
    let Some(value) = get_setting(conn, keys::PAIRING)? else {
        return Ok(None);
    };
    match serde_json::from_str(&value) {
        Ok(verifier) => Ok(Some(verifier)),
        Err(error) => {
            event!(warn, "PAIRING_CODE_NOT_SET", {
                "detail": "settings.pairing does not hold a valid verifier",
                "error": error.to_string(),
            });
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot

/// Reads the whole catalog, sorted as the legacy queries are (folders by
/// label then state, peers by endpoint). Folders without a `folder_intents`
/// row (legacy rows) have both intents enabled.
fn load_snapshot(conn: &Connection, generation: u64) -> Result<CatalogSnapshot> {
    let installation = get_setting(conn, keys::INSTALLATION)?.unwrap_or_default();
    let folders = conn
        .prepare_cached(sql::FOLDERS)?
        .query_map([], |r| {
            Ok(FolderRecord {
                state: r.get(0)?,
                label: r.get(1)?,
                repo: r.get(2)?,
                device: r.get(3)?,
                directory: PathBuf::from(r.get::<_, String>(4)?),
                mount: r.get::<_, Option<String>>(5)?.map(PathBuf::from),
                desired_running: r.get(6)?,
                sync_enabled: r.get::<_, Option<bool>>(7)?.unwrap_or(true),
                mount_enabled: r.get::<_, Option<bool>>(8)?.unwrap_or(true),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let peers = conn
        .prepare_cached(sql::PEERS)?
        .query_map([], |r| {
            Ok(PeerRecord {
                endpoint: r.get(0)?,
                code: r.get(1)?,
                installation: r.get(2)?,
                connections: u32::try_from(r.get::<_, i64>(3)?.max(0)).unwrap_or(u32::MAX),
                revision: r.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let gates = conn
        .prepare_cached(sql::GATES)?
        .query_map([], |r| {
            Ok(GateRecord {
                state: r.get(0)?,
                source_installation: r.get(1)?,
                source_endpoint: r.get(2)?,
                created_ms: unsigned(r.get(3)?),
                override_ms: r.get::<_, Option<i64>>(4)?.map(unsigned),
            })
        })?
        .map(|gate| gate.map(|gate| (gate.state.clone(), gate)))
        .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
    let peer_meta = conn
        .prepare_cached(sql::PEER_META)?
        .query_map([], |r| {
            Ok(PeerMeta {
                installation: r.get(0)?,
                last_seen_ms: r.get::<_, Option<i64>>(1)?.map(unsigned),
                display_name: r.get(2)?,
                incompatible: r.get(3)?,
                revoked_ms: r.get::<_, Option<i64>>(4)?.map(unsigned),
            })
        })?
        .map(|meta| meta.map(|meta| (meta.installation.clone(), meta)))
        .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
    Ok(CatalogSnapshot {
        generation,
        installation,
        folders,
        peers,
        verifier: read_verifier(conn)?,
        verifier_generation: verifier_generation(conn)?,
        gates,
        peer_meta,
    })
}

/// Everything but the generation (`CatalogSnapshot` has no `PartialEq`).
fn same_content(a: &CatalogSnapshot, b: &CatalogSnapshot) -> bool {
    a.installation == b.installation
        && a.folders == b.folders
        && a.peers == b.peers
        && a.verifier == b.verifier
        && a.verifier_generation == b.verifier_generation
        && a.gates == b.gates
        && a.peer_meta == b.peer_meta
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn ms(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unsigned(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Mount paths

/// Validates a requested mount path for the folder whose state directory is
/// `state_dir`, with today's rules (the legacy `Supervisor::mount_path`, in
/// the same order, with the same side effect and the same codes):
///
/// 1. the path is made absolute (`std::path::absolute`);
/// 2. it needs a parent (`MOUNT_PARENT_REQUIRED`), which is **created**, then
///    canonicalized; the path is that parent joined with the requested name
///    (`FOLDER_MOUNT_REQUIRED` without one);
/// 3. it must not overlap the data directory (`MOUNT_OVERLAPS_DATA`), which
///    with the standard layout (`DATA/states/STATE`) also covers the state
///    directory;
/// 4. it must not overlap the state directory, canonicalized through its
///    nearest existing ancestor (`MOUNT_OVERLAPS_STATE`; only reachable for a
///    state directory outside the data directory). The legacy check compared
///    the raw state directory with the canonical path, which on Windows
///    (`\\?\` prefixes) never matched and, with the standard layout, only
///    repeated step 3;
/// 5. it must not exist (`MOUNT_PATH_MUST_NOT_EXIST`);
/// 6. it must not overlap the mount of any folder in `snap`, including the
///    folder's own current mount, as today (`MOUNT_OVERLAPS_FOLDER`).
///
/// Returns the canonical path to store. I/O failures keep their text and,
/// like the legacy `?`, carry no code of their own.
pub fn validate_mount_path(
    snap: &CatalogSnapshot,
    data_dir: &Path,
    state_dir: &Path,
    requested: &Path,
) -> Result<PathBuf, ErrorWire> {
    let path = std::path::absolute(requested).map_err(io_error)?;
    let parent = path.parent().ok_or_else(|| {
        ErrorWire::coded(
            "MOUNT_PARENT_REQUIRED",
            format!("{} has no parent directory", path.display()),
        )
    })?;
    fs::create_dir_all(parent).map_err(io_error)?;
    let parent = fs::canonicalize(parent).map_err(io_error)?;
    let name = path.file_name().ok_or_else(|| {
        ErrorWire::coded(
            "FOLDER_MOUNT_REQUIRED",
            format!("{} does not name a folder", path.display()),
        )
    })?;
    let path = parent.join(name);
    let data = fs::canonicalize(data_dir).map_err(io_error)?;
    if path.starts_with(&data) || data.starts_with(&path) {
        return Err(ErrorWire::coded(
            "MOUNT_OVERLAPS_DATA",
            format!(
                "{} overlaps the data directory {}",
                path.display(),
                data.display()
            ),
        ));
    }
    let state = canonical_lenient(state_dir);
    if path.starts_with(&state) || state.starts_with(&path) {
        return Err(ErrorWire::coded(
            "MOUNT_OVERLAPS_STATE",
            format!(
                "{} overlaps the folder state directory {}",
                path.display(),
                state.display()
            ),
        ));
    }
    if path.exists() {
        return Err(ErrorWire::coded(
            "MOUNT_PATH_MUST_NOT_EXIST",
            format!("{} already exists", path.display()),
        ));
    }
    for folder in &snap.folders {
        if let Some(other) = &folder.mount
            && (path.starts_with(other) || other.starts_with(&path))
        {
            return Err(ErrorWire::coded(
                "MOUNT_OVERLAPS_FOLDER",
                format!(
                    "{} overlaps the mount of folder {} ({})",
                    path.display(),
                    folder.label,
                    other.display()
                ),
            ));
        }
    }
    Ok(path)
}

fn io_error(error: std::io::Error) -> ErrorWire {
    ErrorWire::from_error(&anyhow::Error::from(error))
}

/// The canonical form of `path`, or of its nearest existing ancestor joined
/// with the rest (a state directory is validated before it is created).
fn canonical_lenient(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let mut rest = Vec::new();
    let mut cursor = absolute.as_path();
    loop {
        if let Ok(canonical) = fs::canonicalize(cursor) {
            return rest
                .iter()
                .rev()
                .fold(canonical, |path, part| path.join(part));
        }
        match (cursor.parent(), cursor.file_name()) {
            (Some(parent), Some(name)) => {
                rest.push(name.to_owned());
                cursor = parent;
            }
            _ => return absolute,
        }
    }
}
