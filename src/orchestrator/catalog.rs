//! The orchestrator's catalog (`DATA/folders.sqlite`): the folders it runs,
//! its pairing state and the clock offsets measured against peers.
use super::worker::open_private;
use crate::{
    clock::Offset,
    model::{errors::ErrorWire, protocol::frame::ClientHello},
    pairing_code::Verifier,
    session::AcceptPolicy,
    util::sqlite::CachedConnection,
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

pub const SCHEMA_VERSION: u32 = 1;
pub const FOLDER_LIMIT: usize = 64;
/// Shortest id prefix `resolve` accepts.
const PREFIX_MIN: usize = 4;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS folders(
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,
    repo TEXT NOT NULL,
    mount TEXT,
    desired_running INTEGER NOT NULL,
    paused INTEGER NOT NULL,
    created_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS peers(
    endpoint TEXT PRIMARY KEY,
    installation TEXT NOT NULL,
    credential TEXT NOT NULL,
    connections INTEGER);
CREATE TABLE IF NOT EXISTS paired(installation TEXT PRIMARY KEY, credential TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS peer_meta(
    installation TEXT PRIMARY KEY,
    clock_offset_ms INTEGER NOT NULL,
    clock_rtt_ms INTEGER NOT NULL,
    clock_measured_at INTEGER NOT NULL);";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FolderRecord {
    pub id: String,
    pub label: String,
    pub state: PathBuf,
    pub repo: String,
    pub mount: Option<PathBuf>,
    pub desired_running: bool,
    pub paused: bool,
    pub created_ms: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Peer {
    pub endpoint: String,
    pub installation: String,
    /// Never serialized: listings must not leak the peer credential.
    #[serde(default, skip_serializing)]
    credential: String,
    pub connections: Option<usize>,
}

/// `credential` is an application secret, so `Debug` never prints it.
impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("endpoint", &self.endpoint)
            .field("installation", &self.installation)
            .field("credential", &"<redacted>")
            .field("connections", &self.connections)
            .finish()
    }
}

impl Peer {
    pub fn key(&self) -> Result<[u8; 32]> {
        decode_key(&self.credential)
    }
}

fn decode_key(credential: &str) -> Result<[u8; 32]> {
    let mut key = [0; 32];
    hex::decode_to_slice(credential, &mut key).context("CREDENTIAL_INVALID")?;
    Ok(key)
}

pub struct Catalog {
    db: Mutex<Connection>,
    pub data_directory: PathBuf,
}

impl Catalog {
    pub fn open(data_directory: &Path) -> Result<Arc<Self>> {
        std::fs::create_dir_all(data_directory)
            .with_context(|| format!("CATALOG_UNAVAILABLE: {}", data_directory.display()))?;
        let path = data_directory.join("folders.sqlite");
        // Created owner-only before SQLite opens it (-wal/-shm inherit).
        open_private(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true),
            &path,
        )
        .with_context(|| format!("CATALOG_UNAVAILABLE: {}", path.display()))?;
        let db = Connection::open(&path)
            .with_context(|| format!("CATALOG_UNAVAILABLE: {}", path.display()))?;
        let tables: Vec<String> = db
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let version: Option<u32> = if tables.iter().any(|name| name == "schema") {
            db.query_cached("SELECT version FROM schema", [], |r| r.get(0))
                .optional()?
        } else {
            None
        };
        match version {
            Some(version) => ensure!(
                version == SCHEMA_VERSION,
                "CATALOG_INCOMPATIBLE: {} has schema version {version}, this build uses {SCHEMA_VERSION}",
                path.display()
            ),
            // Fresh only when nothing but an empty `schema` table is there:
            // a database of an earlier release (no `schema` row) is never
            // stamped as this one's.
            None => ensure!(
                tables.iter().all(|name| name == "schema"),
                "CATALOG_INCOMPATIBLE: {} is not a tkfs catalog of this release; move the data \
                 directory aside",
                path.display()
            ),
        }
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let catalog = Arc::new(Self {
            db: Mutex::new(db),
            data_directory: data_directory.to_path_buf(),
        });
        catalog.initialize(version.is_none())?;
        Ok(catalog)
    }

    fn initialize(&self, fresh: bool) -> Result<()> {
        let db = self.db()?;
        let tx = db.unchecked_transaction()?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS schema(version INTEGER NOT NULL);")?;
        tx.execute_batch(SCHEMA)?;
        if fresh {
            tx.execute_cached("INSERT INTO schema VALUES(?1)", [SCHEMA_VERSION])?;
        }
        tx.execute_cached(
            "INSERT OR IGNORE INTO settings VALUES('installation', ?1)",
            [crate::core::id()],
        )?;
        let (_, verifier) = Verifier::generate();
        tx.execute_cached(
            "INSERT OR IGNORE INTO settings VALUES('verifier', ?1)",
            [serde_json::to_string(&verifier)?],
        )?;
        tx.execute_cached(
            "INSERT OR IGNORE INTO settings VALUES('verifier_generation', '1')",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn db(&self) -> Result<MutexGuard<'_, Connection>> {
        Ok(self.db.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn setting(db: &Connection, key: &str) -> Result<Option<String>> {
        Ok(db
            .query_cached("SELECT value FROM settings WHERE key=?1", [key], |r| {
                r.get(0)
            })
            .optional()?)
    }

    pub fn installation(&self) -> Result<String> {
        let db = self.db()?;
        Self::setting(&db, "installation")?.context("CATALOG_CORRUPT: no installation id")
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let db = self.db()?;
        Self::setting(&db, key)
    }

    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        self.db()?.execute_cached(
            "INSERT OR REPLACE INTO settings VALUES(?1, ?2)",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn folders(&self) -> Result<Vec<FolderRecord>> {
        let db = self.db()?;
        Self::all_folders(&db)
    }

    fn all_folders(db: &Connection) -> Result<Vec<FolderRecord>> {
        let mut query = db.prepare_cached(
            "SELECT id,label,state,repo,mount,desired_running,paused,created_ms \
             FROM folders ORDER BY rowid",
        )?;
        Ok(query
            .query_map([], |r| {
                Ok(FolderRecord {
                    id: r.get(0)?,
                    label: r.get(1)?,
                    state: PathBuf::from(r.get::<_, String>(2)?),
                    repo: r.get(3)?,
                    mount: r.get::<_, Option<String>>(4)?.map(PathBuf::from),
                    desired_running: r.get(5)?,
                    paused: r.get(6)?,
                    created_ms: r.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn create_folder(
        &self,
        label: &str,
        repo: &str,
        mount: Option<PathBuf>,
    ) -> Result<FolderRecord> {
        let label = label.trim();
        ensure!(
            !label.is_empty(),
            "FOLDER_NAME_REQUIRED: a folder needs a name"
        );
        let db = self.db()?;
        let existing = Self::all_folders(&db)?;
        ensure!(
            existing.len() < FOLDER_LIMIT,
            "FOLDER_LIMIT: at most {FOLDER_LIMIT} folders"
        );
        ensure!(
            existing.iter().all(|folder| folder.label != label),
            "FOLDER_LABEL_TAKEN: a folder is already named {label:?}"
        );
        let id = crate::core::id();
        let record = FolderRecord {
            state: self.data_directory.join(&id),
            id,
            label: label.to_owned(),
            repo: repo.to_owned(),
            mount,
            desired_running: true,
            paused: false,
            created_ms: crate::util::time::now_ms(),
        };
        db.execute_cached(
            "INSERT INTO folders VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                record.id,
                record.label,
                path_text(&record.state)?,
                record.repo,
                record.mount.as_deref().map(path_text).transpose()?,
                record.desired_running,
                record.paused,
                record.created_ms
            ],
        )?;
        Ok(record)
    }

    /// Exact id, then exact label, then a unique id prefix of at least
    /// [`PREFIX_MIN`] characters.
    pub fn resolve(&self, selector: &str) -> Result<FolderRecord> {
        let folders = self.folders()?;
        if let Some(folder) = folders.iter().find(|f| f.id == selector) {
            return Ok(folder.clone());
        }
        if let Some(folder) = folders.iter().find(|f| f.label == selector) {
            return Ok(folder.clone());
        }
        if selector.len() >= PREFIX_MIN {
            let mut matches = folders.iter().filter(|f| f.id.starts_with(selector));
            if let Some(first) = matches.next() {
                ensure!(
                    matches.next().is_none(),
                    "AMBIGUOUS_FOLDER: {selector:?} matches more than one folder"
                );
                return Ok(first.clone());
            }
        }
        bail!("FOLDER_NOT_FOUND: no folder matches {selector:?}")
    }

    fn update_folder(&self, id: &str, sql: &str, value: impl rusqlite::ToSql) -> Result<()> {
        let changed = self.db()?.execute_cached(sql, params![id, value])?;
        ensure!(changed == 1, "FOLDER_NOT_FOUND: no folder {id:?}");
        Ok(())
    }

    pub fn set_desired(&self, id: &str, running: bool) -> Result<()> {
        self.update_folder(
            id,
            "UPDATE folders SET desired_running=?2 WHERE id=?1",
            running,
        )
    }

    pub fn set_paused(&self, id: &str, paused: bool) -> Result<()> {
        self.update_folder(id, "UPDATE folders SET paused=?2 WHERE id=?1", paused)
    }

    /// Deletes the folder `id`'s row (a `create` whose store could not be
    /// initialized).
    pub fn remove_folder(&self, id: &str) -> Result<()> {
        let changed = self
            .db()?
            .execute_cached("DELETE FROM folders WHERE id=?1", [id])?;
        ensure!(changed == 1, "FOLDER_NOT_FOUND: no folder {id:?}");
        Ok(())
    }

    pub fn set_mount(&self, id: &str, mount: Option<PathBuf>) -> Result<()> {
        let mount = mount.as_deref().map(path_text).transpose()?;
        self.update_folder(id, "UPDATE folders SET mount=?2 WHERE id=?1", mount)
    }

    /// The pairing-code verifier and its generation (starts at 1).
    pub fn verifier(&self) -> Result<(Verifier, u64)> {
        let db = self.db()?;
        let verifier = Self::setting(&db, "verifier")?.context("CATALOG_CORRUPT: no verifier")?;
        let generation = Self::setting(&db, "verifier_generation")?
            .context("CATALOG_CORRUPT: no verifier generation")?;
        Ok((
            serde_json::from_str(&verifier).context("CATALOG_CORRUPT: verifier")?,
            generation
                .parse()
                .context("CATALOG_CORRUPT: verifier generation")?,
        ))
    }

    /// Replaces the verifier and revokes every paired installation in one
    /// transaction. Returns the new code.
    pub fn reset_code(&self) -> Result<String> {
        let db = self.db()?;
        let generation = Self::setting(&db, "verifier_generation")?
            .context("CATALOG_CORRUPT: no verifier generation")?
            .parse::<u64>()
            .context("CATALOG_CORRUPT: verifier generation")?;
        let (code, verifier) = Verifier::generate();
        let tx = db.unchecked_transaction()?;
        tx.execute_cached(
            "INSERT OR REPLACE INTO settings VALUES('verifier', ?1)",
            [serde_json::to_string(&verifier)?],
        )?;
        tx.execute_cached(
            "INSERT OR REPLACE INTO settings VALUES('verifier_generation', ?1)",
            [(generation + 1).to_string()],
        )?;
        tx.execute_cached("DELETE FROM paired", [])?;
        tx.commit()?;
        Ok(code)
    }

    pub fn peers(&self) -> Result<Vec<Peer>> {
        let db = self.db()?;
        let mut query = db.prepare_cached(
            "SELECT endpoint,installation,credential,connections FROM peers ORDER BY rowid",
        )?;
        Ok(query
            .query_map([], peer_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn peer(&self, endpoint: &str) -> Result<Peer> {
        self.db()?
            .query_cached(
                "SELECT endpoint,installation,credential,connections FROM peers WHERE endpoint=?1",
                [endpoint],
                peer_row,
            )
            .optional()?
            .with_context(|| format!("PEER_NOT_FOUND: no peer at {endpoint:?}"))
    }

    /// Outbound pairing: remember the peer and trust its key. An
    /// installation has one credential: its other `peers` rows take the key.
    pub fn save_peer(
        &self,
        endpoint: &str,
        installation: &str,
        key: [u8; 32],
        connections: Option<usize>,
    ) -> Result<()> {
        crate::ws::endpoint_uri(endpoint)?;
        let key = hex::encode(key);
        let db = self.db()?;
        let tx = db.unchecked_transaction()?;
        tx.execute_cached(
            "INSERT OR REPLACE INTO paired VALUES(?1,?2)",
            params![installation, key],
        )?;
        tx.execute_cached(
            "UPDATE peers SET credential=?2 WHERE installation=?1",
            params![installation, key],
        )?;
        tx.execute_cached(
            "INSERT OR REPLACE INTO peers VALUES(?1,?2,?3,?4)",
            params![endpoint, installation, key, connections],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Inbound enrollment: trust the key without a dialable endpoint. An
    /// installation has one credential, so the `peers` rows this side dials
    /// it with take the new key too (a replaced key would be refused).
    /// Returns how many `peers` rows changed.
    pub fn save_paired(&self, installation: &str, key: [u8; 32]) -> Result<usize> {
        let key = hex::encode(key);
        let db = self.db()?;
        let tx = db.unchecked_transaction()?;
        tx.execute_cached(
            "INSERT OR REPLACE INTO paired VALUES(?1,?2)",
            params![installation, key],
        )?;
        let updated = tx.execute_cached(
            "UPDATE peers SET credential=?2 WHERE installation=?1",
            params![installation, key],
        )?;
        tx.commit()?;
        Ok(updated)
    }

    pub fn unpair(&self, installation: &str) -> Result<()> {
        let db = self.db()?;
        let tx = db.unchecked_transaction()?;
        tx.execute_cached("DELETE FROM paired WHERE installation=?1", [installation])?;
        tx.execute_cached("DELETE FROM peers WHERE installation=?1", [installation])?;
        tx.execute_cached(
            "DELETE FROM peer_meta WHERE installation=?1",
            [installation],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Every installation whose credential is trusted (outbound pairings and
    /// inbound enrollments), in pairing order.
    pub fn paired_installations(&self) -> Result<Vec<String>> {
        let db = self.db()?;
        let mut query = db.prepare_cached("SELECT installation FROM paired ORDER BY rowid")?;
        Ok(query
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn paired_key(&self, installation: &str) -> Result<[u8; 32]> {
        let credential: String = self
            .db()?
            .query_cached(
                "SELECT credential FROM paired WHERE installation=?1",
                [installation],
                |r| r.get(0),
            )
            .optional()?
            .context("PEER_REVOKED: this installation is not paired")?;
        decode_key(&credential)
    }

    pub fn set_clock(&self, installation: &str, offset: Offset, measured_at_ms: u64) -> Result<()> {
        self.db()?.execute_cached(
            "INSERT OR REPLACE INTO peer_meta VALUES(?1,?2,?3,?4)",
            params![
                installation,
                offset.offset_ms,
                offset.rtt_ms,
                measured_at_ms
            ],
        )?;
        Ok(())
    }

    pub fn clock(&self, installation: &str) -> Result<Option<(Offset, u64)>> {
        Ok(self
            .db()?
            .query_cached(
                "SELECT clock_offset_ms,clock_rtt_ms,clock_measured_at FROM peer_meta \
                 WHERE installation=?1",
                [installation],
                |r| {
                    Ok((
                        Offset {
                            offset_ms: r.get(0)?,
                            rtt_ms: r.get(1)?,
                        },
                        r.get(2)?,
                    ))
                },
            )
            .optional()?)
    }
}

fn path_text(path: &Path) -> Result<&str> {
    path.to_str()
        .with_context(|| format!("PATH_NOT_UTF8: {}", path.display()))
}

fn peer_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Peer> {
    Ok(Peer {
        endpoint: r.get(0)?,
        installation: r.get(1)?,
        credential: r.get(2)?,
        connections: r.get(3)?,
    })
}

impl AcceptPolicy for Catalog {
    fn installation(&self) -> String {
        Catalog::installation(self).unwrap_or_default()
    }
    fn verifier(&self) -> Option<(Verifier, u64)> {
        Catalog::verifier(self).ok()
    }
    fn admit(&self, _: &ClientHello) -> std::result::Result<(), ErrorWire> {
        Ok(())
    }
    fn legacy_attempt(&self, _: SocketAddr, _: &Value) {}
    fn peer_key(&self, installation: &str) -> std::result::Result<[u8; 32], ErrorWire> {
        self.paired_key(installation)
            .map_err(|e| ErrorWire::coded("PEER_REVOKED", format!("{e:#}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::errors::code_of;

    fn code(error: &anyhow::Error) -> String {
        code_of(error)
    }

    #[test]
    fn open_creates_schema_and_stable_installation() -> Result<()> {
        let home = tempfile::tempdir()?;
        let data = home.path().join("data");
        let first = Catalog::open(&data)?.installation()?;
        assert!(!first.is_empty());
        assert_eq!(Catalog::open(&data)?.installation()?, first);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(data.join("folders.sqlite"))?
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        Ok(())
    }

    #[test]
    fn incompatible_schema_is_refused() -> Result<()> {
        let home = tempfile::tempdir()?;
        let db = rusqlite::Connection::open(home.path().join("folders.sqlite"))?;
        db.execute_batch(
            "CREATE TABLE schema(version INTEGER NOT NULL); INSERT INTO schema VALUES(99);",
        )?;
        drop(db);
        let error = Catalog::open(home.path()).err().expect("must be refused");
        assert_eq!(code(&error), "CATALOG_INCOMPATIBLE");
        Ok(())
    }

    #[test]
    fn a_catalog_of_an_earlier_release_is_refused_not_stamped() -> Result<()> {
        let home = tempfile::tempdir()?;
        let path = home.path().join("folders.sqlite");
        let db = rusqlite::Connection::open(&path)?;
        db.execute_batch(
            "CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT); \
             CREATE TABLE peers(endpoint TEXT PRIMARY KEY, installation TEXT); \
             INSERT INTO settings VALUES('installation', 'old');",
        )?;
        drop(db);
        let error = Catalog::open(home.path()).err().expect("must be refused");
        assert_eq!(code(&error), "CATALOG_INCOMPATIBLE");
        let message = format!("{error:#}");
        assert!(
            message
                .contains("is not a tkfs catalog of this release; move the data directory aside")
                && message.contains(&path.display().to_string()),
            "{message}"
        );
        // Nothing was written: no `schema` table, the old rows as they were.
        let db = rusqlite::Connection::open(&path)?;
        let schema: i64 = db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='schema'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(schema, 0);
        let columns: i64 =
            db.query_row("SELECT count(*) FROM pragma_table_info('peers')", [], |r| {
                r.get(0)
            })?;
        assert_eq!(columns, 2);
        // Refused again on the next start, never stamped.
        assert_eq!(
            code(&Catalog::open(home.path()).err().expect("still refused")),
            "CATALOG_INCOMPATIBLE"
        );
        Ok(())
    }

    #[test]
    fn an_empty_database_and_a_bare_schema_table_are_fresh() -> Result<()> {
        let empty = tempfile::tempdir()?;
        rusqlite::Connection::open(empty.path().join("folders.sqlite"))?;
        Catalog::open(empty.path())?.installation()?;
        // An earlier open that stopped after creating `schema`.
        let bare = tempfile::tempdir()?;
        rusqlite::Connection::open(bare.path().join("folders.sqlite"))?
            .execute_batch("CREATE TABLE schema(version INTEGER NOT NULL);")?;
        let catalog = Catalog::open(bare.path())?;
        catalog.installation()?;
        drop(catalog);
        Catalog::open(bare.path())?;
        Ok(())
    }

    #[test]
    fn remove_folder_frees_the_label() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        let record = catalog.create_folder("Project", "repo-a", None)?;
        catalog.remove_folder(&record.id)?;
        assert!(catalog.folders()?.is_empty());
        assert_eq!(
            code(&catalog.remove_folder(&record.id).unwrap_err()),
            "FOLDER_NOT_FOUND"
        );
        catalog.create_folder("Project", "repo-a", None)?;
        Ok(())
    }

    #[test]
    fn create_resolve_by_id_prefix_and_label() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        let first = catalog.create_folder("Project", "repo-a", None)?;
        let second = catalog.create_folder("Projects", "repo-b", Some("/mnt/p".into()))?;
        assert_eq!(first.state, home.path().join(&first.id));
        assert!(first.desired_running && !first.paused);
        assert_eq!(second.mount, Some(PathBuf::from("/mnt/p")));
        assert_eq!(catalog.resolve("Project")?, first);
        assert_eq!(catalog.resolve("Projects")?, second);
        assert_eq!(catalog.resolve(&first.id)?, first);
        assert_eq!(catalog.resolve(&first.id[..4])?, first);
        assert_eq!(
            code(&catalog.resolve(&first.id[..3]).unwrap_err()),
            "FOLDER_NOT_FOUND"
        );
        assert_eq!(
            code(&catalog.resolve("nothing-like-it").unwrap_err()),
            "FOLDER_NOT_FOUND"
        );
        for (id, label) in [("zzzz-1", "x"), ("zzzz-2", "y")] {
            catalog.db()?.execute(
                "INSERT INTO folders VALUES(?1,?2,'s','r',NULL,1,0,0)",
                rusqlite::params![id, label],
            )?;
        }
        assert_eq!(
            code(&catalog.resolve("zzzz").unwrap_err()),
            "AMBIGUOUS_FOLDER"
        );
        assert_eq!(
            code(&catalog.create_folder("Project", "r", None).unwrap_err()),
            "FOLDER_LABEL_TAKEN"
        );
        assert_eq!(
            code(&catalog.create_folder("  ", "r", None).unwrap_err()),
            "FOLDER_NAME_REQUIRED"
        );
        catalog.set_paused(&first.id, true)?;
        catalog.set_desired(&first.id, false)?;
        catalog.set_mount(&first.id, Some("/mnt/q".into()))?;
        let changed = catalog.resolve(&first.id)?;
        assert!(changed.paused && !changed.desired_running);
        assert_eq!(changed.mount, Some(PathBuf::from("/mnt/q")));
        Ok(())
    }

    #[test]
    fn folder_limit_is_64() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        for n in 0..FOLDER_LIMIT {
            catalog.create_folder(&format!("folder {n}"), "repo", None)?;
        }
        assert_eq!(catalog.folders()?.len(), 64);
        let error = catalog.create_folder("one more", "repo", None).unwrap_err();
        assert_eq!(code(&error), "FOLDER_LIMIT");
        Ok(())
    }

    #[test]
    fn reset_code_revokes_paired_and_bumps_generation() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        catalog.save_paired("peer", [7; 32])?;
        assert_eq!(catalog.paired_key("peer")?, [7; 32]);
        let (before, generation) = catalog.verifier()?;
        assert_eq!(generation, 1);
        let code_text = catalog.reset_code()?;
        assert_eq!(
            code(&catalog.paired_key("peer").unwrap_err()),
            "PEER_REVOKED"
        );
        let (after, generation) = catalog.verifier()?;
        assert_eq!(generation, 2);
        assert!(before != after);
        assert!(!code_text.is_empty());
        let policy: &dyn AcceptPolicy = &*catalog;
        assert_eq!(policy.verifier().map(|(_, g)| g), Some(2));
        assert_eq!(policy.peer_key("peer").unwrap_err().code, "PEER_REVOKED");
        Ok(())
    }

    #[test]
    fn clock_offsets_round_trip() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        let offset = Offset {
            offset_ms: 595,
            rtt_ms: 10,
        };
        catalog.set_clock("x", offset, 1_700_000_000_000)?;
        assert_eq!(catalog.clock("x")?, Some((offset, 1_700_000_000_000)));
        assert_eq!(catalog.clock("y")?, None);
        Ok(())
    }

    #[test]
    fn save_peer_round_trips_and_hides_the_credential() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        let key = [9; 32];
        catalog.save_peer("ws://host:1", "inst", key, Some(3))?;
        assert_eq!(catalog.paired_key("inst")?, key);
        let peer = catalog.peer("ws://host:1")?;
        assert_eq!(peer.key()?, key);
        let peers = catalog.peers()?;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].connections, Some(3));
        assert_eq!(peers[0].installation, "inst");
        assert_eq!(
            code(&catalog.peer("ws://other:2").unwrap_err()),
            "PEER_NOT_FOUND"
        );
        let printed = format!("{peer:?} {peer:#?}");
        assert!(!printed.contains(&hex::encode(key)));
        assert!(printed.contains("<redacted>"));
        let json = serde_json::to_value(&peer)?;
        assert!(json.get("credential").is_none());
        assert_eq!(json["connections"], 3);
        Ok(())
    }

    #[test]
    fn save_peer_refuses_a_non_websocket_endpoint_and_stores_nothing() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        assert!(
            catalog
                .save_peer("http://x", "inst", [1; 32], None)
                .is_err()
        );
        assert!(catalog.peers()?.is_empty());
        assert_eq!(
            code(&catalog.paired_key("inst").unwrap_err()),
            "PEER_REVOKED"
        );
        Ok(())
    }

    #[test]
    fn an_installation_has_one_credential_across_paired_and_peers() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        catalog.save_peer("ws://b:1", "b", [1; 32], None)?;
        catalog.save_peer("ws://other:2", "c", [3; 32], None)?;
        // B enrolls a new key inbound: the row this side dials B with follows.
        assert_eq!(catalog.save_paired("b", [2; 32])?, 1);
        assert_eq!(catalog.paired_key("b")?, [2; 32]);
        assert_eq!(catalog.peer("ws://b:1")?.key()?, [2; 32]);
        assert_eq!(catalog.peer("ws://other:2")?.key()?, [3; 32]);
        // A second endpoint for B takes over the key for every row of B.
        catalog.save_peer("ws://b-alias:1", "b", [4; 32], None)?;
        assert_eq!(catalog.peer("ws://b:1")?.key()?, [4; 32]);
        assert_eq!(catalog.peer("ws://b-alias:1")?.key()?, [4; 32]);
        assert_eq!(catalog.paired_key("b")?, [4; 32]);
        // An inbound-only installation has no rows to update.
        assert_eq!(catalog.save_paired("d", [5; 32])?, 0);
        Ok(())
    }

    #[test]
    fn unpair_removes_peer_pairing_and_clock() -> Result<()> {
        let home = tempfile::tempdir()?;
        let catalog = Catalog::open(home.path())?;
        catalog.save_peer("ws://host:1", "inst", [4; 32], None)?;
        let offset = Offset {
            offset_ms: -5,
            rtt_ms: 2,
        };
        catalog.set_clock("inst", offset, 42)?;
        assert_eq!(catalog.clock("inst")?, Some((offset, 42)));
        catalog.unpair("inst")?;
        assert_eq!(
            code(&catalog.paired_key("inst").unwrap_err()),
            "PEER_REVOKED"
        );
        assert!(catalog.peers()?.is_empty());
        assert_eq!(catalog.clock("inst")?, None);
        Ok(())
    }
}
