//! Read-only eligibility guard shared by selected SQLite-backed store layouts.
use anyhow::{Context, Result, ensure};
use std::path::Path;

/// SQLite can create empty WAL/SHM files even for a read-only connection.
/// Reject unsupported standalone headers without asking SQLite to open them.
/// A nonempty WAL is deliberately left to SQLite: its committed header may be
/// newer than (or recover a torn checkpoint of) the main database header.
pub(crate) fn standalone_header(
    path: &Path,
    application_id: i64,
    selected_version: i64,
    code: &str,
) -> Result<()> {
    use std::io::Read;
    match std::fs::metadata(path.with_extension("sqlite-wal")) {
        Ok(wal) if wal.len() > 0 => return Ok(()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut file = std::fs::File::open(path)?;
    if file.metadata()?.len() == 0 {
        return Ok(());
    }
    let mut header = [0; 100];
    file.read_exact(&mut header).with_context(|| {
        format!("{code}: incomplete SQLite header; use a fresh store directory")
    })?;
    ensure!(
        &header[..16] == b"SQLite format 3\0",
        "{code}: invalid SQLite header; use a fresh store directory"
    );
    // SQLite's published file header: big-endian schema cookie at 40,
    // user_version at 60, application_id at 68. This is only an eligibility
    // guard; inspect() still validates SQLite's authoritative snapshot.
    let app = i64::from(i32::from_be_bytes(header[68..72].try_into()?));
    let version = i64::from(i32::from_be_bytes(header[60..64].try_into()?));
    let schema = u32::from_be_bytes(header[40..44].try_into()?);
    ensure!(
        (app == application_id && version == selected_version)
            || (app == 0 && version == 0 && schema == 0),
        "{code}: application_id={app}, version={version}; expected application_id={application_id}, version={selected_version}; use a fresh store directory"
    );
    Ok(())
}
