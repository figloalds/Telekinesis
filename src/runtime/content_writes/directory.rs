//! Directory creation uses bounded namespace ownership and metadata durability.
use super::*;
use crate::core::{
    BasicInfo, Entry,
    content_saves::{DirectoryCreateRequest, NamespaceTarget},
};
use uuid::Uuid;
mod changes;
mod deletion;
pub(super) use changes::modify_basic;
pub use changes::{remove_directory, rename_directory};
pub(super) use deletion::{complete_delete, recover_pending, set_delete};

pub fn create_directory(
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
    let _permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let stamp = crate::core::filetime(crate::core::time());
    let mut attributes = options.attributes.unwrap_or(0x10);
    if let Some(mode) = options.posix_mode {
        attributes = (attributes & !1) | u32::from(mode & 0o222 == 0);
    }
    let basic = BasicInfo {
        posix_mode: options.posix_mode,
        attributes: BasicInfo::attributes("directory", attributes)?,
        creation_time: stamp,
        access_time: stamp,
        write_time: stamp,
        change_time: stamp,
    };
    basic.validate()?;
    let (branch, view_generation, target, _reservation) = {
        let e = engine(shared, &context)?;
        let parent = e.store.lookup(&parent_path)?;
        ensure!(parent.alive && parent.kind == "directory", "NOT_DIRECTORY");
        ensure!(
            options
                .expected_parent
                .as_ref()
                .is_none_or(|id| *id == parent.id),
            "STALE_CONTENT_DESTINATION"
        );
        match e.lookup_visible(path) {
            Ok(_) => anyhow::bail!("NAME_COLLISION"),
            Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => {}
            Err(error) => return Err(error),
        }
        let reservation = writes.reserve_name((
            e.store.active.clone(),
            parent.id.clone(),
            crate::core::name_key(&name)?,
        ))?;
        (
            e.store.active.clone(),
            e.store.generation,
            NamespaceTarget {
                path: path.to_owned(),
                parent: parent.id,
            },
            reservation,
        )
    };
    let entity = Uuid::new_v4();
    let key = writes.key(&entity.to_string(), &context)?;
    let _entity = lock(&key, &context)?;
    let request = {
        let mut store =
            super::super::content_reads::retry_sql(&context, || writes.stores.checkout_nowait())?;
        let workspace = super::super::content_reads::retry_sql(&context, || {
            store.content_workspace_id(&context)
        })?;
        let request = DirectoryCreateRequest {
            mutation: Uuid::new_v4(),
            entity,
            workspace,
            branch,
            view_generation,
            target,
            basic,
        };
        if let Err(error) = store.apply_directory_create(&request, &context)
            && store
                .directory_create_receipt(&request, &context)?
                .is_none()
        {
            return Err(error);
        }
        store.validate_directory_create(&request, &context)?;
        request
    };
    crate::util::fault::trigger("content.directory.before_attach")?;
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
    let entry = e.store.validate_directory_create(&request, &context)?;
    let delta = e.activation_delta(&[request.mutation.to_string()], None)?;
    e.after_activation(&delta);
    let opened = e.attach_native_directory(entry, options.writable)?;
    if cfg!(windows) && e.mounted {
        e.queue_notifications(vec![(
            super::super::notify::ROOT_PATH.to_owned(),
            super::super::notify::ACTION_MODIFIED,
        )]);
    }
    e.publish_observation();
    Ok(Some(opened))
}
