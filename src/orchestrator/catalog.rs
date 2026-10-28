//! Pairing catalog shares the folder's metadata database.
use crate::util::sqlite::CachedConnection;
use crate::{
    content::working::lock,
    model::{errors::ErrorWire, protocol::frame::ClientHello},
    pairing_code::Verifier,
    runtime::Engine,
    session::{self, AcceptPolicy, Handler, Incoming},
};
use anyhow::{Context, Result};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{net::SocketAddr, sync::Arc};

#[derive(Clone, Serialize, Deserialize)]
pub struct Peer {
    pub endpoint: String,
    pub installation: String,
    credential: String,
}
impl Peer {
    pub fn key(&self) -> Result<[u8; 32]> {
        let mut key = [0; 32];
        hex::decode_to_slice(&self.credential, &mut key)?;
        Ok(key)
    }
}
pub struct Catalog {
    pub engine: Arc<Engine>,
}
/// Pairing-code-authenticated discovery for receiving a fresh current-format folder.
pub fn probe(endpoint: &str, code: &str) -> Result<Value> {
    let hello = session::client_hello(
        &crate::core::id(),
        crate::model::protocol::LaneRole::Probe,
        None,
        1,
    );
    let (conn, _) = session::dial(
        endpoint,
        hello,
        &session::CodeAuth { code: code.into() },
        None,
        session::ConnOptions::default(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let reply = conn
        .call(
            "catalog.list",
            json!({}),
            Vec::new(),
            session::Priority::Control,
            std::time::Duration::from_secs(10),
        )
        .map_err(|e| anyhow::anyhow!("{e}"));
    conn.close();
    Ok(serde_json::from_slice(&reply?.body)?)
}
impl Catalog {
    pub fn inspect_peer(&self, endpoint: &str) -> Result<Value> {
        let peer = self
            .peers()?
            .into_iter()
            .find(|peer| peer.endpoint == endpoint)
            .context("PAIRING_CODE_REQUIRED")?;
        let device = lock(&self.engine.store).device.clone();
        let hello =
            session::client_hello(&device, crate::model::protocol::LaneRole::Probe, None, 1);
        let (conn, _) = session::dial_expecting(
            endpoint,
            hello,
            &session::auth::PeerAuth { key: peer.key()? },
            Some(&peer.installation),
            None,
            session::ConnOptions::default(),
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let reply = conn
            .call(
                "catalog.list",
                json!({}),
                Vec::new(),
                session::Priority::Control,
                std::time::Duration::from_secs(10),
            )
            .map_err(|e| anyhow::anyhow!("{e}"));
        conn.close();
        Ok(serde_json::from_slice(&reply?.body)?)
    }
    pub fn open(engine: Arc<Engine>) -> Result<Arc<Self>> {
        lock(&engine.store).db.execute_batch("CREATE TABLE IF NOT EXISTS peers(endpoint TEXT PRIMARY KEY,installation TEXT NOT NULL,credential TEXT NOT NULL);CREATE TABLE IF NOT EXISTS paired(installation TEXT PRIMARY KEY,credential TEXT NOT NULL);")?;
        let catalog = Arc::new(Self { engine });
        if catalog.get("verifier")?.is_none() {
            let (_, verifier) = Verifier::generate();
            catalog.set("verifier", &serde_json::to_string(&verifier)?)?;
        }
        Ok(catalog)
    }
    fn settings(&self) -> super::settings::Settings {
        super::settings::Settings {
            engine: self.engine.clone(),
        }
    }
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        self.settings().get(key)
    }
    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        self.settings().set(key, value)
    }
    pub fn reset_code(&self) -> Result<String> {
        let (code, verifier) = Verifier::generate();
        self.set("verifier", &serde_json::to_string(&verifier)?)?;
        self.engine.flush(None)?;
        Ok(code)
    }
    pub fn peers(&self) -> Result<Vec<Peer>> {
        let store = lock(&self.engine.store);
        let mut q = store
            .db
            .prepare_cached("SELECT endpoint,installation,credential FROM peers")?;
        Ok(q.query_map([], |r| {
            Ok(Peer {
                endpoint: r.get(0)?,
                installation: r.get(1)?,
                credential: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    pub fn contains(&self, endpoint: &str) -> bool {
        self.peers()
            .is_ok_and(|peers| peers.iter().any(|p| p.endpoint == endpoint))
    }
    pub fn paired_key(&self, installation: &str) -> Result<[u8; 32]> {
        let key: String = lock(&self.engine.store)
            .db
            .query_cached(
                "SELECT credential FROM paired WHERE installation=?1",
                [installation],
                |r| r.get(0),
            )
            .context("PEER_REVOKED")?;
        let mut bytes = [0; 32];
        hex::decode_to_slice(key, &mut bytes)?;
        Ok(bytes)
    }
    fn save(&self, endpoint: &str, installation: &str, key: [u8; 32]) -> Result<()> {
        crate::ws::endpoint_uri(endpoint)?;
        let key = hex::encode(key);
        let mut store = lock(&self.engine.store);
        store.flush_metadata()?;
        let tx = store.db.unchecked_transaction()?;
        tx.execute_cached(
            "INSERT OR REPLACE INTO paired VALUES(?1,?2)",
            params![installation, key],
        )?;
        tx.execute_cached(
            "INSERT OR REPLACE INTO peers VALUES(?1,?2,?3)",
            params![endpoint, installation, key],
        )?;
        tx.commit()?;
        drop(store);
        self.engine.flush(None)?;
        Ok(())
    }
    pub fn pair(&self, endpoint: &str, code: &str) -> Result<String> {
        let installation = lock(&self.engine.store).device.clone();
        let hello = session::client_hello(
            &installation,
            crate::model::protocol::LaneRole::Probe,
            None,
            1,
        );
        let (conn, server) = session::dial(
            endpoint,
            hello,
            &session::CodeAuth { code: code.into() },
            None,
            session::ConnOptions::default(),
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let secret = session::auth::new_secret();
        let advertise = self
            .get("advertise")?
            .context("START_DAEMON_BEFORE_PAIRING")?;
        conn.call(
            "peer.enroll",
            json!({"endpoint":advertise}),
            serde_json::to_vec(&session::auth::enrollment_request(&secret))?,
            session::Priority::Control,
            std::time::Duration::from_secs(10),
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        conn.close();
        self.save(
            endpoint,
            &server.installation,
            session::auth::peer_key(&secret),
        )?;
        Ok(server.installation)
    }
    pub fn unpair(&self, installation: &str) -> Result<()> {
        let mut store = lock(&self.engine.store);
        store.flush_metadata()?;
        let tx = store.db.unchecked_transaction()?;
        tx.execute_cached("DELETE FROM paired WHERE installation=?1", [installation])?;
        tx.execute_cached("DELETE FROM peers WHERE installation=?1", [installation])?;
        tx.commit()?;
        Ok(())
    }
}
impl AcceptPolicy for Catalog {
    fn installation(&self) -> String {
        lock(&self.engine.store).device.clone()
    }
    fn verifier(&self) -> Option<(Verifier, u64)> {
        let value = self.get("verifier").ok()??;
        Some((serde_json::from_str(&value).ok()?, 1))
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
pub struct PeerService {
    pub catalog: Arc<Catalog>,
    pub engine: Arc<Engine>,
    pub peer: String,
}
impl Handler for PeerService {
    fn handle(&self, req: Incoming) {
        if req.op == "catalog.list" {
            // The session has already authenticated by pairing code or a
            // persisted peer key. This is not an unauthenticated discovery API.
            let label = self.catalog.get("label").ok().flatten();
            let store = lock(&self.engine.store);
            req.responder.reply(
                json!({}),
                serde_json::to_vec(&json!({
                    "repo":store.repo,"label":label,"installation":store.device
                }))
                .unwrap(),
            );
            return;
        }
        if req.op == "peer.enroll" {
            let result = (|| -> Result<()> {
                let request: session::auth::EnrollRequest = serde_json::from_slice(&req.body)?;
                let secret = session::auth::enrollment_secret(&request)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                let endpoint = req
                    .header
                    .get("endpoint")
                    .and_then(Value::as_str)
                    .context("ENDPOINT_REQUIRED")?;
                self.catalog
                    .save(endpoint, &self.peer, session::auth::peer_key(&secret))
            })();
            match result {
                Ok(()) => req
                    .responder
                    .reply(json!({}), b"{\"enrolled\":true}".to_vec()),
                Err(error) => req
                    .responder
                    .error(ErrorWire::coded("PAIR_FAILED", format!("{error:#}"))),
            }
            return;
        }
        if self.catalog.paired_key(&self.peer).is_err() {
            req.responder
                .error(ErrorWire::coded("PEER_REVOKED", "peer is not paired"));
            return;
        }
        crate::orchestrator::sync::Service {
            engine: self.engine.clone(),
        }
        .handle(req)
    }
}
