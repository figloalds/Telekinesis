//! Durable proof that verified bytes were actually received for an admitted
//! shared reference. This evidence survives failure of derived SQLite indexes.
use super::{LocalContent, ReadContext};
use crate::{
    core::Store,
    model::content::{Digest, FileContentRef, ObjectKey, ObjectKind},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions, ReadDir},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const MAX_RECEIPT: u64 = 2048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    version: u8,
    repository: String,
    reference: String,
    generation: u64,
    root: FileContentRef,
    key: ObjectKey,
    source: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    payload: Payload,
    checksum: Digest,
}

impl Record {
    fn encode(payload: Payload) -> Result<Vec<u8>> {
        let checksum = Digest::bytes(&serde_json::to_vec(&payload)?);
        let bytes = serde_json::to_vec(&Self { payload, checksum })?;
        ensure!(bytes.len() as u64 <= MAX_RECEIPT, "CONTENT_BUFFER_LIMIT");
        Ok(bytes)
    }

    fn read(path: &Path) -> Result<Payload> {
        let file = File::open(path)?;
        ensure!(
            file.metadata()?.len() <= MAX_RECEIPT,
            "CORRUPT_CONTENT_RECEIPT"
        );
        let mut bytes = vec![];
        file.take(MAX_RECEIPT + 1).read_to_end(&mut bytes)?;
        ensure!(bytes.len() as u64 <= MAX_RECEIPT, "CORRUPT_CONTENT_RECEIPT");
        let record: Self = serde_json::from_slice(&bytes).context("CORRUPT_CONTENT_RECEIPT")?;
        let p = &record.payload;
        ensure!(
            p.version == 1
                && p.repository.len() <= 128
                && p.reference.len() == 64
                && p.generation > 0
                && !p.source.is_empty()
                && p.source.len() <= 128
                && record.checksum == Digest::bytes(&serde_json::to_vec(p)?),
            "CORRUPT_CONTENT_RECEIPT"
        );
        Ok(record.payload)
    }
}

fn prepare(provider: &LocalContent, payload: Payload) -> Result<PathBuf> {
    let dir = provider.receipt_directory();
    match fs::create_dir(&dir) {
        Ok(()) => {
            crate::core::objects::sync_dir(dir.parent().context("CONTENT_STORE_NOT_INITIALIZED")?)?
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let name = Digest::bytes(&serde_json::to_vec(&(
        &payload.reference,
        payload.generation,
        payload.key,
    ))?)
    .to_string();
    let path = dir.join(format!("{name}.json"));
    if path.exists() {
        let old = Record::read(&path)?;
        ensure!(
            old.repository == payload.repository
                && old.reference == payload.reference
                && old.generation == payload.generation
                && old.root == payload.root
                && old.key == payload.key,
            "CORRUPT_CONTENT_RECEIPT"
        );
        return Ok(path);
    }
    let bytes = Record::encode(payload)?;
    let temp = dir.join(format!("{}.part", Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        crate::core::objects::install_object(&temp, &path)?;
        Ok(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

/// The route adapter supplies the authenticated source identity. Verification
/// precedes the durable intent; installation and derived-index writes follow.
/// No metadata engine lock or SQLite write transaction spans this function.
pub struct ReceivedFrame<'a> {
    pub reference: &'a str,
    pub generation: u64,
    pub key: ObjectKey,
    pub bytes: &'a [u8],
    pub source: &'a str,
}

pub fn receive(
    store: &mut Store,
    provider: &LocalContent,
    frame: ReceivedFrame<'_>,
    context: &ReadContext,
) -> Result<()> {
    let ReceivedFrame {
        reference,
        generation,
        key,
        bytes,
        source,
    } = frame;
    context.check()?;
    ensure!(
        !source.is_empty() && source.len() <= 128 && !source.chars().any(char::is_control),
        "INVALID_DEMAND_SCOPE"
    );
    store.validate_content_frame(reference, generation, key, bytes)?;
    let (_, root, _) = store.content_root(reference)?;
    let path = prepare(
        provider,
        Payload {
            version: 1,
            repository: store.repo.clone(),
            reference: reference.into(),
            generation,
            root,
            key,
            source: source.into(),
        },
    )?;
    crate::util::fault::trigger("content.receipt.prepared")?;
    provider.install(key, bytes, context)?;
    crate::util::fault::trigger("content.receipt.installed")?;
    store.record_content_receipt(reference, generation, key)?;
    crate::util::fault::trigger("content.receipt.committed")?;
    fs::remove_file(path)?;
    Ok(())
}

/// The directory iterator is retained between slices: blocked/stale evidence
/// cannot permanently hide later work. Restarting simply begins another pass.
pub struct ReceiptRecovery {
    entries: ReadDir,
}

impl ReceiptRecovery {
    pub fn open(provider: &LocalContent) -> Result<Option<Self>> {
        match fs::read_dir(provider.receipt_directory()) {
            Ok(entries) => Ok(Some(Self { entries })),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// At most `limit` records and bounded buffers; the first failure is returned
    /// after retaining its file. Successful records are retired after commit.
    pub fn step(
        &mut self,
        store: &mut Store,
        provider: &LocalContent,
        limit: usize,
        context: &ReadContext,
    ) -> Result<(usize, bool)> {
        ensure!((1..=256).contains(&limit), "ADMISSION_PAGE_LIMIT");
        let mut completed = 0;
        let mut first_error = None;
        for _ in 0..limit {
            context.check()?;
            let Some(entry) = self.entries.next() else {
                return match first_error {
                    Some(error) => Err(error),
                    None => Ok((completed, true)),
                };
            };
            let entry = entry?;
            if entry.path().extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let result = (|| -> Result<()> {
                let payload = Record::read(&entry.path())?;
                ensure!(payload.repository == store.repo, "REPOSITORY_MISMATCH");
                let (generation, root, granted) = store.content_root(&payload.reference)?;
                ensure!(root == payload.root, "CORRUPT_CONTENT_RECEIPT");
                if generation != payload.generation || !granted {
                    // A withdrawn transfer owns no newer generation's work.
                    fs::remove_file(entry.path())?;
                    return Ok(());
                }
                let bytes = provider.read_blob(
                    payload.key,
                    if payload.key.kind == ObjectKind::Bytes {
                        Some(
                            store
                                .content_edge(&payload.reference, generation, payload.key)?
                                .edge
                                .len,
                        )
                    } else {
                        None
                    },
                    context,
                )?;
                store.validate_content_frame(
                    &payload.reference,
                    generation,
                    payload.key,
                    &bytes,
                )?;
                store.record_content_receipt(&payload.reference, generation, payload.key)?;
                fs::remove_file(entry.path())?;
                Ok(())
            })();
            match result {
                Ok(()) => completed += 1,
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok((completed, false)),
        }
    }
}
