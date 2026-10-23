//! The selected private journal format. Opening never upgrades another layout.
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Layout {
    Fresh,
    Current,
}

pub(super) fn standalone_header(path: &Path) -> Result<()> {
    crate::util::sqlite_format::standalone_header(
        path,
        APPLICATION_ID,
        JOURNAL_VERSION,
        "UNSUPPORTED_WRITE_JOURNAL",
    )
}

pub(super) fn inspect(db: &Connection) -> Result<Layout> {
    let app: i64 = db.query_row("PRAGMA application_id", [], |r| r.get(0))?;
    let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if app == APPLICATION_ID && version == JOURNAL_VERSION {
        return Ok(Layout::Current);
    }
    if app == 0 && version == 0 {
        // Views, indexes and triggers are also existing state. Only a genuinely
        // empty database may be initialized, never an unmarked foreign store.
        let objects: u64 = db.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))?;
        let schema: i64 = db.query_row("PRAGMA schema_version", [], |r| r.get(0))?;
        if objects == 0 && schema == 0 {
            return Ok(Layout::Fresh);
        }
    }
    anyhow::bail!(
        "UNSUPPORTED_WRITE_JOURNAL: application_id={app}, version={version}; expected application_id={APPLICATION_ID}, version={JOURNAL_VERSION}; use a fresh store directory"
    )
}

pub(super) fn current(db: &Connection) -> Result<()> {
    ensure!(inspect(db)? == Layout::Current, "UNSUPPORTED_WRITE_JOURNAL");
    Ok(())
}
