//! Create typed empty files before acknowledging the native open. Byte mutations
//! then use ordinary private COW, FULL checkpoint and canonical-save boundaries.
use super::*;
use crate::core::{
    BasicInfo, Entry,
    content_saves::{ContentCreateRequest, NamespaceTarget},
};
use uuid::Uuid;

#[derive(Clone, Debug, Default)]
pub struct CreateOptions {
    pub writable: bool,
    pub attributes: Option<u32>,
    pub posix_mode: Option<u32>,
    /// Inode-relative adapters bind the parent they looked up, even if its path
    /// is swapped before this function captures the namespace reservation.
    pub expected_parent: Option<String>,
}
pub fn create(
    shared: &Shared,
    path: &str,
    options: CreateOptions,
    wait: ContentWait,
) -> Result<Option<(u64, Entry)>> {
    let context = super::super::content_reads::request_context(shared, wait)?;
    let writes = match engine(shared, &context)?.native_writes.clone() {
        Some(writes) => writes,
        None => return Ok(None),
    };
    let context = writes.executor.namespace_context(&context)?;
    ensure!(path.len() <= 32_768, "CONTENT_SAVE_CAPTURE_LIMIT");
    let (parent_path, name) = crate::core::split_path(path)?;
    ensure!(
        options.posix_mode.is_none_or(|m| m & !0o777 == 0),
        "UNSUPPORTED_POSIX_MODE"
    );
    let _adapter_permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let stamp = crate::core::filetime(crate::core::time());
    let mut attributes = options.attributes.unwrap_or(0x20);
    if let Some(mode) = options.posix_mode {
        attributes = (attributes & !1) | u32::from(mode & 0o222 == 0);
    }
    let basic = BasicInfo {
        posix_mode: options.posix_mode,
        attributes: BasicInfo::attributes("file", attributes)?,
        creation_time: stamp,
        access_time: stamp,
        write_time: stamp,
        change_time: stamp,
    };
    basic.validate()?;
    let (request, reservation) = {
        let e = engine(shared, &context)?;
        let directory = e.store.lookup(&parent_path)?;
        ensure!(
            directory.alive && directory.kind == "directory",
            "NOT_DIRECTORY"
        );
        ensure!(
            options
                .expected_parent
                .as_ref()
                .is_none_or(|id| *id == directory.id),
            "STALE_CONTENT_DESTINATION"
        );
        match e.lookup_visible(path) {
            Ok(_) => anyhow::bail!("NAME_COLLISION"),
            Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => {}
            Err(error) => return Err(error),
        }
        let slot = (
            e.store.active.clone(),
            directory.id.clone(),
            crate::core::name_key(&name)?,
        );
        let reservation = writes.reserve_name(slot)?;
        (
            ContentCreateRequest {
                mutation: Uuid::new_v4(),
                entity: Uuid::new_v4(),
                operation: Uuid::new_v4(),
                branch: e.store.active.clone(),
                view_generation: e.store.generation,
                target: NamespaceTarget {
                    path: path.to_owned(),
                    parent: directory.id,
                },
                basic,
            },
            reservation,
        )
    };
    let key = writes.key(&request.entity.to_string(), &context)?;
    let _entity = lock(&key, &context)?;
    let intent = {
        let mut store =
            super::super::content_reads::retry_sql(&context, || writes.stores.checkout_nowait())?;
        let result = store.apply_content_create(&request, &context);
        let intent = match result {
            Ok(intent) => intent,
            Err(error) => match store.content_create_receipt(&request, &context)? {
                Some(intent) => intent,
                None => return Err(error),
            },
        };
        ensure!(intent.base.content == writes.empty, "CORRUPT_CONTENT_SAVE");
        store.check_content_create_owner(&intent, &context)?;
        intent
    };
    let session = writes
        .journal
        .create(intent.clone(), writes.target.clone(), &context)?;
    let file = SaveFile::new(super::super::content_base::attach(
        shared,
        request.view_generation,
        session,
    ));
    crate::util::fault::trigger("content.create.private_created")?;
    {
        let store =
            super::super::content_reads::retry_sql(&context, || writes.stores.checkout_nowait())?;
        store.check_content_create_owner(&intent, &context)?;
    }
    let mut e = engine(shared, &context)?;
    ensure!(
        e.store.active == request.branch
            && e.store.generation == request.view_generation
            && e.native_writes
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &writes)),
        "STALE_CONTENT_VIEW"
    );
    e.note_secondary_commit();
    let delta = e.activation_delta(&[request.mutation.to_string()], None)?;
    e.after_activation(&delta);
    let entry = e.store.entry(&intent.base.entity)?;
    ensure!(
        entry.alive
            && entry.kind == "file"
            && entry.content_ref.as_ref() == Some(&intent.base.content)
            && entry.revisions.get("content") == intent.content_parent.as_ref()
            && entry.revisions.get("alive") == intent.alive_parent.as_ref(),
        "STALE_CONTENT_VIEW"
    );
    let opened = e.attach_native_created(entry, file, options.writable)?;
    if cfg!(windows) && e.mounted {
        e.queue_notifications(vec![(
            super::super::notify::ROOT_PATH.to_owned(),
            super::super::notify::ACTION_MODIFIED,
        )]);
    }
    e.publish_observation();
    drop(e);
    drop(reservation);
    Ok(Some(opened))
}
