//! Worker settings: the folder store's `config` table (`label`, ...).
use crate::{content::working::lock, runtime::Engine, util::sqlite::CachedConnection};
use anyhow::Result;
use rusqlite::{OptionalExtension, params};
use std::sync::Arc;

pub struct Settings {
    pub engine: Arc<Engine>,
}

impl Settings {
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(lock(&self.engine.store)
            .db
            .query_cached("SELECT value FROM config WHERE key=?1", [key], |r| r.get(0))
            .optional()?)
    }

    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        lock(&self.engine.store).db.execute_cached(
            "INSERT OR REPLACE INTO config VALUES(?1,?2)",
            params![key, value],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_round_trip_through_the_store() -> Result<()> {
        let root = tempfile::tempdir()?;
        let engine = Engine::open(root.path(), None)?;
        let settings = Settings {
            engine: engine.clone(),
        };
        assert_eq!(settings.get("label")?, None);
        settings.set("label", "Docs")?;
        settings.set("label", "Photos")?;
        assert_eq!(settings.get("label")?.as_deref(), Some("Photos"));
        engine.shutdown()?;
        Ok(())
    }
}
