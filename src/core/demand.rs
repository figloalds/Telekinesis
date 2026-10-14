//! Object demand: locally queued requests and what each peer wants from us.
//! Every check is an index lookup (`event_objects`, `object_availability`);
//! nothing here parses the journal.
use super::objects::CORRUPT;
use super::*;

/// Demand older than this is no longer advertised (`requested_ms` expiry).
pub const DEMAND_TTL_MS: u64 = 24 * 60 * 60 * 1000;
/// Most wanted objects advertised per page.
pub const MAX_WANTED: usize = 1024;

impl Store {
    pub fn request_object(&self, object: &str) -> Result<()> {
        valid_object(object)?;
        ensure!(
            self.db
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM event_objects WHERE hash=?)")?
                .query_row([object], |r| r.get::<_, bool>(0))?,
            "UNREFERENCED_OBJECT"
        );
        self.db.execute("INSERT INTO object_requests VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET requested_ms=excluded.requested_ms", params![object, time()])?;
        // A request means a reader found no usable bytes: reconcile the index.
        self.object_available_for_demand(object);
        Ok(())
    }
    /// Objects this replica wants, newest demand first (at most `MAX_WANTED`,
    /// requests younger than `DEMAND_TTL_MS`), excluding available ones (the
    /// cached `objects/` listing, never one `stat` per object).
    pub fn wanted_objects(&self) -> Result<BTreeSet<String>> {
        let since = time().saturating_sub(DEMAND_TTL_MS);
        let presence = self.presence()?;
        let mut statement = self.db.prepare_cached(
            "SELECT r.hash,a.state,z.size FROM object_requests r
             LEFT JOIN object_availability a ON a.hash=r.hash LEFT JOIN object_sizes z ON z.hash=r.hash
             WHERE r.requested_ms>=? ORDER BY r.requested_ms DESC,r.hash",
        )?;
        let mut rows = statement.query([since])?;
        let mut wanted = BTreeSet::new();
        while wanted.len() < MAX_WANTED
            && let Some(row) = rows.next()?
        {
            let object = row.get_ref(0)?.as_str()?;
            let available =
                row.get::<_, Option<i64>>(1)? != Some(CORRUPT) && presence.has(object, row.get(2)?);
            if !available {
                wanted.insert(object.to_owned());
            }
        }
        Ok(wanted)
    }
    pub fn peer_requests(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT hash FROM peer_requests WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    /// Record what `peer` wants (shared, committed references only) and relay
    /// the demand for objects we lack. One transaction, no journal parse.
    pub fn remember_requests(&self, peer: &str, wanted: &BTreeSet<String>) -> Result<()> {
        let presence = self.presence()?;
        self.in_savepoint("remember_requests", || {
            self.db
                .execute("DELETE FROM peer_requests WHERE peer=?", [peer])?;
            for object in wanted {
                if !self.objects_referenced_committed(object)? {
                    continue;
                }
                self.db.execute(
                    "INSERT OR IGNORE INTO peer_requests VALUES(?,?)",
                    params![peer, object],
                )?;
                if !self.available_in(&presence, object)? {
                    self.db.execute("INSERT INTO object_requests VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET requested_ms=excluded.requested_ms", params![object, time()])?;
                }
            }
            Ok(())
        })
    }
}
