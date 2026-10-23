//! One selected catalog layout. Validation never upgrades or repairs authority.
use super::*;
use anyhow::ensure;
use std::sync::OnceLock;

const APPLICATION_ID: i64 = 0x544b4353;
const VERSION: i64 = 1;
const SQL: &str = include_str!("schema.sql");
type Object = (String, String, String, String);

fn catalog(db: &Connection) -> Result<Vec<Object>> {
    let mut query = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name LIMIT 257")?;
    let mut bytes = 0;
    let objects = query
        .query_map([], |row| {
            let text = |index, limit| -> rusqlite::Result<String> {
                let value = row.get_ref(index)?.as_str()?;
                if value.len() > limit {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                Ok(value.to_owned())
            };
            let object: Object = (text(0, 32)?, text(1, 256)?, text(2, 256)?, text(3, 65536)?);
            bytes += object.0.len() + object.1.len() + object.2.len() + object.3.len();
            if bytes > 262_144 {
                return Err(rusqlite::Error::InvalidQuery);
            }
            Ok(object)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("INVALID_CATALOG")?;
    ensure!(objects.len() <= 256, "INVALID_CATALOG: schema object limit");
    Ok(objects)
}

fn expected() -> Result<&'static [Object]> {
    static EXPECTED: OnceLock<std::result::Result<Vec<Object>, String>> = OnceLock::new();
    match EXPECTED.get_or_init(|| {
        (|| {
            let db = Connection::open_in_memory()?;
            db.execute_batch(SQL)?;
            catalog(&db)
        })()
        .map_err(|e: anyhow::Error| format!("{e:#}"))
    }) {
        Ok(objects) => Ok(objects),
        Err(error) => bail!("INVALID_CATALOG: embedded schema: {error}"),
    }
}

/// Returns true only for genuinely empty SQLite input. Each inspection sees
/// one snapshot, including while another initializer commits a fresh catalog.
fn inspect(db: &Connection) -> Result<bool> {
    let _snapshot = if db.is_autocommit() {
        Some(db.unchecked_transaction()?)
    } else {
        None
    };
    let app: i64 = db.query_row("PRAGMA application_id", [], |r| r.get(0))?;
    let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if app == 0 && version == 0 {
        let schema: i64 = db.query_row("PRAGMA schema_version", [], |r| r.get(0))?;
        let empty: bool =
            db.query_row("SELECT NOT EXISTS(SELECT 1 FROM sqlite_schema)", [], |r| {
                r.get(0)
            })?;
        if schema == 0 && empty {
            return Ok(true);
        }
    }
    ensure!(
        app == APPLICATION_ID && version == VERSION,
        "UNSUPPORTED_CATALOG_FORMAT: application_id={app}, version={version}; expected application_id={APPLICATION_ID}, version={VERSION}; use a fresh data directory"
    );
    let actual = catalog(db)?;
    for required in expected()? {
        ensure!(
            actual.iter().any(|object| object == required),
            "INVALID_CATALOG: missing or changed schema object {}",
            required.1
        );
    }
    required_setting(db, keys::INSTALLATION)?;
    required_setting(db, keys::OBJECTS_INCARNATION)?
        .parse::<u64>()
        .context("INVALID_CATALOG: malformed object lease incarnation")?;
    let generation = verifier_generation(db)?;
    let salt = get_setting(db, keys::VERIFIER_SALT)?;
    match read_verifier(db)? {
        Some(verifier) => ensure!(
            generation > 0
                && !verifier.salt.is_empty()
                && salt.as_deref() == Some(verifier.salt.as_str()),
            "INVALID_CATALOG: verifier identity/generation mismatch"
        ),
        None => ensure!(
            generation == 0 && salt.is_none(),
            "INVALID_CATALOG: missing pairing verifier"
        ),
    }
    let broken: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM projects p LEFT JOIN folder_intents i USING(state) WHERE i.state IS NULL)
             OR EXISTS(SELECT 1 FROM folder_intents i LEFT JOIN projects p USING(state) WHERE p.state IS NULL)
             OR EXISTS(SELECT 1 FROM mount_gates g LEFT JOIN projects p USING(state) WHERE p.state IS NULL)",
        [], |r| r.get(0))?;
    ensure!(
        !broken,
        "INVALID_CATALOG: missing folder intent or orphan gate"
    );
    Ok(false)
}

/// Run before creating a lock file or mount directory, so unknown input is
/// refused without writable access or permission changes. The owner repeats it.
pub(super) fn preflight(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    ensure!(
        path.is_file(),
        "INVALID_CATALOG: database path is not a file"
    );
    crate::util::sqlite_format::standalone_header(
        path,
        APPLICATION_ID,
        VERSION,
        "UNSUPPORTED_CATALOG_FORMAT",
    )?;
    let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(BUSY_TIMEOUT)?;
    inspect(&db)?;
    Ok(())
}

pub(super) fn open(path: &Path) -> Result<Connection> {
    preflight(path)?;
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
        | if path.exists() {
            rusqlite::OpenFlags::empty()
        } else {
            rusqlite::OpenFlags::SQLITE_OPEN_CREATE
        };
    let mut db = Connection::open_with_flags(path, flags)?;
    db.busy_timeout(BUSY_TIMEOUT)?;
    let fresh = inspect(&db)?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
    if fresh {
        let parent = path
            .parent()
            .context("INVALID_CATALOG: no data directory")?;
        crate::core::objects::sync_dir(parent)?;
        fault::trigger("catalog.schema.before_initialize")?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure!(
            inspect(&tx)?,
            "INVALID_CATALOG: fresh catalog changed while directory lock held"
        );
        tx.execute_batch(SQL)?;
        for (key, value) in [
            (keys::INSTALLATION, id()),
            (keys::OBJECTS_INCARNATION, "0".into()),
            (keys::VERIFIER_GENERATION, "0".into()),
        ] {
            tx.execute("INSERT INTO settings VALUES(?,?)", [key, &value])?;
        }
        tx.pragma_update(None, "application_id", APPLICATION_ID)?;
        tx.pragma_update(None, "user_version", VERSION)?;
        fault::trigger("catalog.schema.initialized")?;
        tx.commit()?;
        crate::core::objects::sync_dir(parent)?;
        fault::trigger("catalog.schema.committed")?;
    }
    Ok(db)
}
