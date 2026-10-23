//! Native basic-info decoding and ordering shared by the platform adapters.
use super::*;
use crate::core::content_saves::ContentBasicUpdate;

pub(super) fn decode(
    attributes: u32,
    times: [u64; 4],
    mut disabled: u8,
) -> Result<(ContentBasicUpdate, u8)> {
    let mut fields = ContentBasicUpdate {
        attributes: (attributes != u32::MAX).then_some(attributes),
        ..Default::default()
    };
    for (index, value) in times.into_iter().enumerate() {
        let bit = if index == 0 { 0 } else { 1 << (index - 1) };
        match value {
            0 => {}
            u64::MAX if index != 0 => disabled |= bit,
            value if value == u64::MAX - 1 && index != 0 => disabled &= !bit,
            value if value <= i64::MAX as u64 => match index {
                0 => fields.creation_time = Some(value),
                1 => fields.access_time = Some(value),
                2 => fields.write_time = Some(value),
                _ => fields.change_time = Some(value),
            },
            _ => anyhow::bail!("INVALID_TIMESTAMP"),
        }
    }
    fields.validate()?;
    Ok((fields, disabled))
}

fn modify(
    shared: &Shared,
    handle: u64,
    wait: ContentWait,
    build: impl Fn(u8) -> Result<(ContentBasicUpdate, u8)>,
) -> Result<bool> {
    if directory::modify_basic(shared, handle, wait, &build)? {
        return Ok(true);
    }
    let context = super::super::content_reads::request_context(shared, wait)?;
    let existing = {
        let mut e = engine(shared, &context)?;
        if e.native_writes.is_none() {
            return Ok(false);
        }
        let session = e.session(handle)?;
        if session.entry.kind != "file"
            || (session.bytes.cow_file().is_none() && session.entry.content_ref.is_none())
        {
            return Ok(false);
        }
        let file = session.bytes.cow_file();
        let h = e.handles.get(&handle).context("INVALID_HANDLE")?;
        ensure!(!h.cleaned, "INVALID_HANDLE");
        let (fields, disabled) = build(h.time_update_disabled)?;
        if fields.is_empty() && file.is_none() {
            e.handles.get_mut(&handle).unwrap().time_update_disabled = disabled;
            return Ok(true);
        }
        file
    };
    let file = if let Some(file) = existing {
        file
    } else {
        super::super::content_reads::prepare_open(shared, handle, wait)?;
        prepare_handle(shared, handle, true, &context)?;
        super::file(shared, handle, &context)?.context("UNSUPPORTED_FS_OPERATION")?
    };
    let _mutation = lock(&file.adapter_mutation, &context)?;
    reconcile_metadata(shared, &file, &context)?;
    let (writes, fields, disabled) = {
        let mut e = engine(shared, &context)?;
        ensure!(
            e.session(handle)?
                .bytes
                .cow_file()
                .is_some_and(|current| Arc::ptr_eq(&current, &file)),
            "STALE_CONTENT_VIEW"
        );
        let h = e.handles.get(&handle).context("INVALID_HANDLE")?;
        ensure!(!h.cleaned, "INVALID_HANDLE");
        let (fields, disabled) = build(h.time_update_disabled)?;
        if fields.is_empty() {
            e.handles.get_mut(&handle).unwrap().time_update_disabled = disabled;
            return Ok(true);
        }
        (
            e.native_writes.clone().context("PEER_NEEDS_UPGRADE")?,
            fields,
            disabled,
        )
    };
    writes
        .executor
        .update_basic(file.clone(), handle, fields, disabled, &context)?;
    Ok(true)
}
pub fn set_basic(
    shared: &Shared,
    handle: u64,
    attributes: u32,
    times: [u64; 4],
    wait: ContentWait,
) -> Result<bool> {
    modify(shared, handle, wait, |disabled| {
        decode(attributes, times, disabled)
    })
}
pub fn update_basic(
    shared: &Shared,
    handle: u64,
    fields: ContentBasicUpdate,
    wait: ContentWait,
) -> Result<bool> {
    fields.validate()?;
    modify(shared, handle, wait, |disabled| {
        Ok((fields.clone(), disabled))
    })
}
pub(super) fn reconcile_metadata(
    shared: &Shared,
    file: &Arc<SaveFile>,
    context: &ReadContext,
) -> Result<()> {
    let writes = engine(shared, context)?
        .native_writes
        .clone()
        .context("PEER_NEEDS_UPGRADE")?;
    writes.executor.reconcile_basic(file, context)?;
    writes.executor.reconcile_rename(file, context)?;
    writes.executor.reconcile_unlink(file, context)?;
    writes.executor.reconcile_retirement(file, context)?;
    Ok(())
}
