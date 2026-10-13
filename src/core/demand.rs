//! Object demand: locally queued requests and what each peer wants from us.
use super::*;

impl Store {
    pub fn request_object(&self, object: &str) -> Result<()> {
        valid_object(object)?;
        ensure!(
            self.events()?.iter().any(|e| e.objects().contains(object)),
            "UNREFERENCED_OBJECT"
        );
        self.db.execute("INSERT INTO object_requests VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET requested_ms=excluded.requested_ms", params![object, time()])?;
        Ok(())
    }
    pub fn wanted_objects(&self) -> Result<BTreeSet<String>> {
        let requests = self
            .db
            .prepare("SELECT hash FROM object_requests ORDER BY requested_ms LIMIT 1024")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(requests
            .into_iter()
            .filter(|h| !self.object_available(h))
            .collect())
    }
    pub fn peer_requests(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT hash FROM peer_requests WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn remember_requests(&self, peer: &str, wanted: &BTreeSet<String>) -> Result<()> {
        let references: BTreeSet<_> = self
            .events()?
            .iter()
            .filter(|e| e.branch.shared)
            .flat_map(Event::objects)
            .collect();
        self.db
            .execute("DELETE FROM peer_requests WHERE peer=?", [peer])?;
        for object in wanted.intersection(&references) {
            self.db.execute(
                "INSERT OR IGNORE INTO peer_requests VALUES(?,?)",
                params![peer, object],
            )?;
            if !self.object_available(object) {
                self.request_object(object)?;
            }
        }
        Ok(())
    }
}
