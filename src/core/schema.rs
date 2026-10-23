//! Selected metadata layout and current derived-index repair. No old-store upgrade.
use super::*;
use std::sync::OnceLock;

const APPLICATION_ID: i64 = 0x544b4d53;
const VERSION: i64 = 3;
const SQL: &str = include_str!("schema.sql");

#[derive(Clone, Debug, PartialEq, Eq)]
struct Object {
    kind: String,
    name: String,
    table: String,
    sql: String,
}
impl Object {
    fn repair(&self) -> Option<&'static str> {
        if ["event_objects", "staged_objects"].contains(&self.table.as_str())
            || self.name.starts_with("event_objects_")
            || self.name.starts_with("staged_objects_")
        {
            Some("objects")
        } else if self.table == "feed_journal" || self.name.starts_with("feed_journal_") {
            Some("feed")
        } else if self.table == "journal_branch" || self.name.starts_with("journal_branch_") {
            Some("journal")
        } else {
            None
        }
    }
}

fn catalog(db: &Connection) -> Result<Vec<Object>> {
    let mut q = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name LIMIT 1025")?;
    let mut bytes = 0usize;
    let objects = q
        .query_map([], |row| {
            let text = |index, limit| -> rusqlite::Result<String> {
                let value = row.get_ref(index)?.as_str()?;
                if value.len() > limit {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                Ok(value.to_owned())
            };
            let object = Object {
                kind: text(0, 32)?,
                name: text(1, 256)?,
                table: text(2, 256)?,
                sql: text(3, 65_536)?,
            };
            bytes += object.kind.len() + object.name.len() + object.table.len() + object.sql.len();
            if bytes > 262_144 {
                return Err(rusqlite::Error::InvalidQuery);
            }
            Ok(object)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("INVALID_STORE_METADATA")?;
    ensure!(
        objects.len() <= 1024,
        "INVALID_STORE_METADATA: schema object limit"
    );
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
        .map_err(|error: anyhow::Error| format!("{error:#}"))
    }) {
        Ok(objects) => Ok(objects),
        Err(error) => bail!("INVALID_STORE_METADATA: embedded schema: {error}"),
    }
}

enum Layout {
    Fresh,
    Current(Vec<Object>),
}

fn identities(db: &Connection, repo: Option<&str>, device: Option<&str>) -> Result<()> {
    let get = |key| -> Result<String> {
        let value: String = db
            .query_row("SELECT value FROM config WHERE key=?", [key], |r| r.get(0))
            .context("INVALID_STORE_METADATA")?;
        ensure!(!value.is_empty(), "INVALID_STORE_METADATA");
        Ok(value)
    };
    let actual_repo = get("repo")?;
    let actual_device = get("device")?;
    let active = get("active")?;
    get("generation")?
        .parse::<u64>()
        .context("INVALID_STORE_METADATA")?;
    get("journal_epoch")?;
    if let Some(repo) = repo {
        if device.is_some() {
            ensure!(actual_repo == repo, "STORE_IDENTITY_MISMATCH");
        } else {
            ensure!(actual_repo == repo, "REPOSITORY_MISMATCH");
        }
    }
    ensure!(
        device.is_none_or(|device| actual_device == device),
        "STORE_IDENTITY_MISMATCH"
    );
    let selected: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM branches WHERE id=?)",
        [active],
        |r| r.get(0),
    )?;
    ensure!(selected, "INVALID_SELECTED_BRANCH");
    Ok(())
}

fn inspect(db: &Connection, repo: Option<&str>, device: Option<&str>) -> Result<Layout> {
    // Fresh initialization may commit between queries. Header, catalog and
    // identities must describe one snapshot, or all belong to the caller's
    // existing writer transaction. Dropping this read transaction releases it.
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
            return Ok(Layout::Fresh);
        }
    }
    ensure!(
        app == APPLICATION_ID && version == VERSION,
        "UNSUPPORTED_STORE_FORMAT: application_id={app}, version={version}; expected application_id={APPLICATION_ID}, version={VERSION}; use a fresh store directory"
    );
    let actual = catalog(db)?;
    let mut repair = Vec::new();
    for required in expected()? {
        match actual
            .iter()
            .find(|object| object.kind == required.kind && object.name == required.name)
        {
            Some(object) => ensure!(
                object == required,
                "INVALID_STORE_METADATA: changed schema object {}",
                required.name
            ),
            None if required.kind == "index" || required.repair().is_some() => {
                repair.push(required.clone())
            }
            None => bail!(
                "INVALID_STORE_METADATA: missing required schema object {}",
                required.name
            ),
        }
    }
    identities(db, repo, device)?;
    Ok(Layout::Current(repair))
}

fn repair(db: &mut Connection, repo: Option<&str>, device: Option<&str>) -> Result<()> {
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let Layout::Current(missing) = inspect(&tx, repo, device)? else {
        bail!("INVALID_STORE_METADATA");
    };
    let mut groups = BTreeSet::new();
    // Tables precede their indexes/triggers. This transaction installs both
    // the missing derived structure and its repair marker or neither.
    for kind in ["table", "index", "trigger"] {
        for object in missing.iter().filter(|object| object.kind == kind) {
            db_progress();
            tx.execute_batch(&object.sql)?;
            if let Some(group) = object.repair().filter(|_| object.kind != "index") {
                groups.insert(group);
            }
        }
    }
    for group in groups {
        tx.execute(
            "DELETE FROM config WHERE key=?",
            [format!("repair:{group}:ready")],
        )?;
        // Losing structure invalidates every earlier partial build checkpoint.
        tx.execute(
            "DELETE FROM config WHERE key LIKE ?",
            [format!("repair:{group}:cursor%")],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Secondary handles validate format/identity but never initialize or repair.
pub(super) fn secondary(
    root: &Path,
    repo: &str,
    device: &str,
    timeout: std::time::Duration,
) -> Result<Connection> {
    let path = root.join("metadata.sqlite");
    ensure!(path.is_file(), "STATE_NOT_INITIALIZED");
    crate::util::sqlite_format::standalone_header(
        &path,
        APPLICATION_ID,
        VERSION,
        "UNSUPPORTED_STORE_FORMAT",
    )?;
    let validate = |db: &Connection| -> Result<()> {
        match inspect(db, Some(repo), Some(device))? {
            Layout::Current(missing) => ensure!(
                missing.is_empty(),
                "INVALID_STORE_METADATA: primary index recovery required"
            ),
            Layout::Fresh => bail!("STATE_NOT_INITIALIZED"),
        }
        Ok(())
    };
    {
        let db = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(timeout)?;
        validate(&db)?;
    }
    let db = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    db.busy_timeout(timeout)?;
    validate(&db)?;
    db.execute_batch("PRAGMA synchronous=FULL;")?;
    Ok(db)
}

fn db_progress() {
    crate::startup::progress("validating current schema");
}

pub(super) fn open(
    root: &Path,
    repo: Option<&str>,
    device: Option<&str>,
    create: bool,
) -> Result<Connection> {
    let path = root.join("metadata.sqlite");
    let exists = path.is_file();
    if !create {
        ensure!(
            exists && root.join("objects").is_dir(),
            "STATE_NOT_INITIALIZED"
        );
    }
    if exists {
        crate::util::sqlite_format::standalone_header(
            &path,
            APPLICATION_ID,
            VERSION,
            "UNSUPPORTED_STORE_FORMAT",
        )?;
        let db = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let layout = inspect(&db, repo, device)?;
        ensure!(
            create || matches!(layout, Layout::Current(_)),
            "STATE_NOT_INITIALIZED"
        );
    } else {
        ensure!(create && !path.exists(), "STATE_NOT_INITIALIZED");
        fs::create_dir_all(root)?;
    }
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
        | if exists {
            rusqlite::OpenFlags::empty()
        } else {
            rusqlite::OpenFlags::SQLITE_OPEN_CREATE
        };
    let mut db = Connection::open_with_flags(&path, flags)?;
    db.busy_timeout(std::time::Duration::from_secs(5))?;
    let layout = inspect(&db, repo, device)?;
    ensure!(
        create || matches!(layout, Layout::Current(_)),
        "STATE_NOT_INITIALIZED"
    );
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
    fs::create_dir_all(root.join("objects"))?;
    match layout {
        Layout::Fresh => {
            objects::sync_dir(root)?;
            crate::util::fault::trigger("store.schema.before_initialize")?;
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            if let Layout::Current(missing) = inspect(&tx, repo, device)? {
                // A concurrent initializer won the writer slot. Adopt its
                // committed identities after validating the caller's seed.
                ensure!(missing.is_empty(), "INVALID_STORE_METADATA");
                tx.commit()?;
                return Ok(db);
            }
            tx.execute_batch(SQL)?;
            tx.execute("INSERT INTO projection_version VALUES(1,0)", [])?;
            let repo = repo.map(str::to_owned).unwrap_or_else(id);
            let device = device.map(str::to_owned).unwrap_or_else(id);
            let main = hash(format!("tkfs-v1/{repo}/main").as_bytes());
            tx.execute("INSERT INTO branches VALUES(?,'main',1)", [&main])?;
            for (key, value) in [
                ("repo", repo),
                ("device", device),
                ("active", main),
                ("generation", "0".to_owned()),
                ("journal_epoch", Uuid::new_v4().simple().to_string()),
                ("content:graph_cursor", String::new()),
                ("repair:objects:ready", "1".to_owned()),
                ("repair:feed:ready", "1".to_owned()),
                ("repair:journal:ready", "1".to_owned()),
            ] {
                tx.execute("INSERT INTO config VALUES(?,?)", [key, &value])?;
            }
            tx.pragma_update(None, "application_id", APPLICATION_ID)?;
            tx.pragma_update(None, "user_version", VERSION)?;
            crate::util::fault::trigger("store.schema.initialized")?;
            tx.commit()?;
            objects::sync_dir(root)?;
            crate::util::fault::trigger("store.schema.committed")?;
        }
        Layout::Current(missing) if !missing.is_empty() => repair(&mut db, repo, device)?,
        Layout::Current(_) => {}
    }
    Ok(db)
}
