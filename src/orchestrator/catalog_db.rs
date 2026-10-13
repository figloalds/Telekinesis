use super::{api::Project, config::Config, lock, supervisor::Supervisor};
use crate::{core::id, pairing_code::Verifier};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

#[derive(Clone)]
pub(crate) struct Peer {
    pub(crate) endpoint: String,
    pub(crate) code: String,
    pub(crate) installation: String,
    pub(crate) connections: usize,
    pub(crate) revision: String,
}
impl Peer {
    pub(crate) fn pool_size(&self) -> usize {
        self.connections.max(2)
    }

    pub(crate) fn bulk_lane(&self, repo: &str) -> usize {
        1 + repo
            .bytes()
            .fold(0usize, |n, b| n.wrapping_mul(31).wrapping_add(b as usize))
            % (self.pool_size() - 1)
    }
}

impl Supervisor {
    pub fn open(config: Config) -> Result<Self> {
        fs::create_dir_all(&config.data_directory)?;
        fs::create_dir_all(&config.default_mount_directory)?;
        let lock = lock(&config.data_directory.join("orchestrator.lock"))?;
        // A new catalog name deliberately avoids reinterpreting the old O1 registry.
        let db = Connection::open(config.data_directory.join("folders.sqlite"))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS projects(state TEXT PRIMARY KEY,label TEXT NOT NULL,repo TEXT NOT NULL UNIQUE,device TEXT NOT NULL,directory TEXT NOT NULL,mount TEXT,desired INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS peers(endpoint TEXT PRIMARY KEY,code TEXT NOT NULL,installation TEXT NOT NULL,connections INTEGER NOT NULL,revision TEXT NOT NULL);")?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS initial_sync(state TEXT PRIMARY KEY,endpoint TEXT NOT NULL);")?;
        db.execute(
            "INSERT OR IGNORE INTO settings VALUES('installation',?)",
            [id()],
        )?;
        let installation = db.query_row(
            "SELECT value FROM settings WHERE key='installation'",
            [],
            |r| r.get(0),
        )?;
        Ok(Self {
            config,
            db,
            installation,
            workers: BTreeMap::new(),
            failures: BTreeMap::new(),
            publication_errors: BTreeMap::new(),
            links: BTreeMap::new(),
            remote_catalogs: BTreeMap::new(),
            restarts: BTreeMap::new(),
            stopping: Arc::new(AtomicBool::new(false)),
            _lock: lock,
        })
    }
    pub fn projects(&self) -> Result<Vec<Project>> {
        Ok(self.db.prepare("SELECT state,label,repo,device,directory,mount,desired FROM projects ORDER BY label,state")?
            .query_map([], |r| Ok(Project { state: r.get(0)?, label: r.get(1)?, repo: r.get(2)?, device: r.get(3)?, directory: PathBuf::from(r.get::<_,String>(4)?), mount: r.get::<_, Option<String>>(5)?.map(PathBuf::from), desired_running: r.get(6)? }))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub(crate) fn project(&self, state: &str) -> Result<Project> {
        self.projects()?
            .into_iter()
            .find(|p| p.state == state)
            .context("FOLDER_NOT_FOUND")
    }
    pub(crate) fn peers(&self) -> Result<Vec<Peer>> {
        Ok(self.db.prepare("SELECT endpoint,code,installation,connections,revision FROM peers ORDER BY endpoint")?
            .query_map([], |r| Ok(Peer { endpoint:r.get(0)?, code:r.get(1)?, installation:r.get(2)?, connections:r.get(3)?, revision:r.get(4)? }))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub(crate) fn verifier(&self) -> Result<Verifier> {
        let value: Option<String> = self
            .db
            .query_row("SELECT value FROM settings WHERE key='pairing'", [], |r| {
                r.get(0)
            })
            .optional()?;
        serde_json::from_str(&value.context("PAIRING_CODE_NOT_SET: reset-pairing-code first")?)
            .map_err(Into::into)
    }
    pub(crate) fn initial_peer(&self, state: &str) -> Result<Option<String>> {
        Ok(self
            .db
            .query_row(
                "SELECT endpoint FROM initial_sync WHERE state=?",
                [state],
                |r| r.get(0),
            )
            .optional()?)
    }
    pub(crate) fn set_desired(&self, state: &str, desired: bool) -> Result<()> {
        if desired {
            self.db
                .execute("UPDATE projects SET desired=1 WHERE state=?", [state])?;
        } else {
            self.db
                .execute("UPDATE projects SET desired=0 WHERE state=?", [state])?;
        }
        Ok(())
    }
    pub(crate) fn set_mount(&self, state: &str, mount: Option<PathBuf>) -> Result<()> {
        self.db.execute(
            "UPDATE projects SET mount=? WHERE state=?",
            params![mount.map(|p| p.to_string_lossy().into_owned()), state],
        )?;
        Ok(())
    }
    pub(crate) fn insert_project_tx(
        tx: &Transaction<'_>,
        state: &str,
        label: &str,
        repo: &str,
        device: &str,
        directory: &Path,
        mount: Option<&Path>,
    ) -> Result<()> {
        tx.execute(
            "INSERT INTO projects VALUES(?,?,?,?,?,?,1)",
            params![
                state,
                label,
                repo,
                device,
                directory.to_string_lossy(),
                mount.map(|p| p.to_string_lossy().into_owned())
            ],
        )?;
        Ok(())
    }
    pub(crate) fn insert_initial_sync_tx(
        tx: &Transaction<'_>,
        state: &str,
        endpoint: &str,
    ) -> Result<()> {
        tx.execute(
            "INSERT INTO initial_sync VALUES(?,?)",
            params![state, endpoint],
        )?;
        Ok(())
    }
    pub(crate) fn upsert_peer(
        &self,
        endpoint: &str,
        code: &str,
        installation: &str,
        connections: usize,
    ) -> Result<()> {
        self.db.execute("INSERT INTO peers VALUES(?,?,?,?,?) ON CONFLICT(endpoint) DO UPDATE SET code=excluded.code,installation=excluded.installation,connections=excluded.connections,revision=excluded.revision", params![endpoint,code,installation,connections,id()])?;
        Ok(())
    }
    pub(crate) fn delete_peer(&self, endpoint: &str) -> Result<()> {
        self.db
            .execute("DELETE FROM peers WHERE endpoint=?", [endpoint])?;
        Ok(())
    }
    pub(crate) fn set_pairing_verifier(&self, verifier: &Verifier) -> Result<()> {
        self.db.execute("INSERT INTO settings VALUES('pairing',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [serde_json::to_string(verifier)?])?;
        Ok(())
    }
    pub(crate) fn delete_initial_sync(&self, state: &str) -> Result<()> {
        self.db
            .execute("DELETE FROM initial_sync WHERE state=?", [state])?;
        Ok(())
    }
}
