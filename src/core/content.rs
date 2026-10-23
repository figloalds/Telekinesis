//! Current file-content producers. Construct and pin bytes before publication.
use super::*;
use crate::content::{LocalContent, PreparedContent, ReadContext};
use std::time::{Duration, Instant};

pub(crate) fn import_context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(240),
        cancel: crate::util::Cancel::new(),
    }
}
impl Store {
    pub(crate) fn check_content_write(&self) -> Result<()> {
        ensure!(
            !self.save_connection_failed,
            "CONTENT_SAVE_CONNECTION_FAILED"
        );
        let synchronous: i64 = self
            .db
            .query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        let mode: String = self
            .db
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        ensure!(
            synchronous >= 2 && mode.eq_ignore_ascii_case("wal"),
            "CONTENT_SAVE_REQUIRES_FULL_WAL"
        );
        Ok(())
    }
    fn check_content_preparation(&self) -> Result<()> {
        ensure!(
            self.db.is_autocommit(),
            "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
        );
        self.check_content_write()
    }

    pub fn write_revision(
        &mut self,
        entity: &str,
        base: Option<String>,
        bytes: &[u8],
    ) -> Result<String> {
        self.check_content_preparation()?;
        let alive = self.current_change(entity, "alive", json!(true))?;
        let local = LocalContent::open(&self.root)?;
        let prepared = local.prepare_import(
            &mut std::io::Cursor::new(bytes),
            bytes.len() as u64,
            &import_context(),
        )?;
        self.write_content_with_parents(entity, base, alive.parents, &prepared, 0)
    }
    pub fn write_with_base(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_base: Option<String>,
        bytes: &[u8],
    ) -> Result<String> {
        self.write_with_base_and_times(entity, content_base, alive_base, bytes, 0)
    }
    pub fn write_with_base_and_times(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_base: Option<String>,
        bytes: &[u8],
        preserve_times: u8,
    ) -> Result<String> {
        self.check_content_preparation()?;
        let local = LocalContent::open(&self.root)?;
        let prepared = local.prepare_import(
            &mut std::io::Cursor::new(bytes),
            bytes.len() as u64,
            &import_context(),
        )?;
        self.write_prepared_content_with_base_and_times(
            entity,
            content_base,
            alive_base,
            &prepared,
            preserve_times,
        )
    }
    /// Consume exactly len bytes with bounded chunk/manifest buffers.
    pub fn write_stream_with_base_and_times(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_base: Option<String>,
        source: &mut impl Read,
        len: u64,
        preserve_times: u8,
    ) -> Result<String> {
        self.check_content_preparation()?;
        let local = LocalContent::open(&self.root)?;
        let prepared = local.prepare_import(source, len, &import_context())?;
        self.write_prepared_content_with_base_and_times(
            entity,
            content_base,
            alive_base,
            &prepared,
            preserve_times,
        )
    }
    /// Metadata only. Keep prepared alive through the caller's outer commit.
    pub fn write_prepared_content_with_base_and_times(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_base: Option<String>,
        prepared: &PreparedContent,
        preserve_times: u8,
    ) -> Result<String> {
        self.write_content_with_parents(
            entity,
            content_base,
            alive_base.into_iter().collect(),
            prepared,
            preserve_times,
        )
    }
    fn write_content_with_parents(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_parents: Vec<String>,
        prepared: &PreparedContent,
        preserve_times: u8,
    ) -> Result<String> {
        self.check_content_write()?;
        prepared.check_store(&self.root)?;
        let b = self.branch(&self.active)?;
        let mut event = self.event(
            &b,
            vec![
                Change {
                    entity: entity.into(),
                    field: "content".into(),
                    parents: content_base.into_iter().collect(),
                    value: json!(prepared.content()),
                },
                Change {
                    entity: entity.into(),
                    field: "alive".into(),
                    parents: alive_parents,
                    value: json!(true),
                },
            ],
        );
        let entry = self.entry(entity)?;
        ensure!(entry.kind == "file", "IS_DIRECTORY");
        if entry.basic.is_some() || preserve_times != 0 {
            let mut basic = entry.basic_info();
            if preserve_times & 2 == 0 {
                basic.write_time = filetime(event.created_ms);
            }
            if preserve_times & 4 == 0 {
                basic.change_time = filetime(event.created_ms);
            }
            basic.attributes = BasicInfo::attributes(&entry.kind, basic.attributes | 0x20)?;
            event
                .changes
                .push(self.current_change(entity, "basic", json!(basic))?);
        }
        let rev = event.id.clone();
        self.commit(event, true)?;
        Ok(rev)
    }
}
