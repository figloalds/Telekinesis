//! O1 Windows IPC: owner-only DACL, local clients only, both endpoint SIDs checked.
use anyhow::{Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::File,
    os::windows::io::{AsRawHandle, FromRawHandle},
    path::Path,
};

unsafe extern "C" {
    fn tk_pipe_listen(name: *const u16, out: *mut *mut std::ffi::c_void) -> u32;
    fn tk_pipe_accept(pipe: *mut std::ffi::c_void) -> u32;
    fn tk_pipe_authorize(pipe: *mut std::ffi::c_void) -> u32;
    fn tk_pipe_connect(name: *const u16, out: *mut *mut std::ffi::c_void) -> u32;
    fn tk_private_directory(path: *const u16) -> u32;
    fn tk_owner_sid(out: *mut u16, count: u32) -> u32;
    fn tk_pipe_io(
        pipe: *mut std::ffi::c_void,
        buffer: *mut u8,
        length: u32,
        count: *mut u32,
        write: u32,
        timeout: u32,
    ) -> u32;
}
fn check(code: u32) -> Result<()> {
    ensure!(
        code == 0,
        "LOCAL_IPC: {}",
        std::io::Error::from_raw_os_error(code as i32)
    );
    Ok(())
}
fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(Some(0)).collect()
}
fn pipe_name(name: &str) -> Result<Vec<u16>> {
    ensure!(
        !name.is_empty()
            && name.len() <= 150
            && name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "INVALID_PIPE_NAME"
    );
    Ok(wide(&format!(r"\\.\pipe\{name}")))
}
pub fn owner_sid() -> Result<String> {
    let mut out = [0u16; 184];
    check(unsafe { tk_owner_sid(out.as_mut_ptr(), out.len() as u32) })?;
    Ok(String::from_utf16(
        &out[..out.iter().position(|c| *c == 0).unwrap()],
    )?)
}
pub fn protect_directory(path: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    let encoded: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    check(unsafe { tk_private_directory(encoded.as_ptr()) })
}
pub struct Listener(File);
impl Listener {
    pub fn bind(name: &str) -> Result<Self> {
        let name = pipe_name(name)?;
        let mut handle = std::ptr::null_mut();
        check(unsafe { tk_pipe_listen(name.as_ptr(), &mut handle) })?;
        Ok(Self(unsafe { File::from_raw_handle(handle) }))
    }
    pub fn receive<T: DeserializeOwned>(&mut self) -> Result<Option<T>> {
        let code = unsafe { tk_pipe_accept(self.0.as_raw_handle()) };
        if code == 121 {
            return Ok(None);
        }
        check(code)?;
        let value = read(&mut self.0, 3000)?;
        check(unsafe { tk_pipe_authorize(self.0.as_raw_handle()) })?;
        Ok(Some(value))
    }
    pub fn reply<T: Serialize>(&mut self, value: &T) -> Result<()> {
        write(&mut self.0, value)?;
        // DisconnectNamedPipe discards unread bytes. A bounded consumption ACK
        // lets us reuse the instance safely without an unbounded synchronous flush.
        let mut ack = [0];
        transfer(&self.0, &mut ack, false, 3000)?;
        ensure!(ack[0] == 1, "LOCAL_IPC_INVALID_ACK");
        Ok(())
    }
}
const MAX_MANAGEMENT_FRAME: usize = 1024 * 1024;
fn transfer(file: &File, bytes: &mut [u8], write: bool, timeout: u32) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout as u64);
    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = deadline
            .saturating_duration_since(std::time::Instant::now())
            .as_millis();
        ensure!(remaining > 0, "LOCAL_IPC_TIMEOUT");
        let mut count = 0;
        check(unsafe {
            tk_pipe_io(
                file.as_raw_handle(),
                bytes[offset..].as_mut_ptr(),
                (bytes.len() - offset) as u32,
                &mut count,
                u32::from(write),
                remaining.min(u32::MAX as u128) as u32,
            )
        })?;
        ensure!(count > 0, "LOCAL_IPC_CLOSED");
        offset += count as usize;
    }
    Ok(())
}
fn read<T: DeserializeOwned>(file: &mut File, timeout: u32) -> Result<T> {
    let mut len = [0; 4];
    transfer(file, &mut len, false, timeout)?;
    let len = u32::from_le_bytes(len) as usize;
    ensure!(len <= MAX_MANAGEMENT_FRAME, "MANAGEMENT_FRAME_TOO_LARGE");
    let mut bytes = vec![0; len];
    transfer(file, &mut bytes, false, timeout)?;
    Ok(serde_json::from_slice(&bytes)?)
}
fn write<T: Serialize>(file: &mut File, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    ensure!(
        bytes.len() <= MAX_MANAGEMENT_FRAME,
        "MANAGEMENT_FRAME_TOO_LARGE"
    );
    transfer(file, &mut (bytes.len() as u32).to_le_bytes(), true, 3000)?;
    transfer(file, &mut bytes, true, 3000)
}
pub fn call<T: Serialize, R: DeserializeOwned>(name: &str, request: &T) -> Result<R> {
    call_timeout(name, request, 20000)
}
pub fn call_timeout<T: Serialize, R: DeserializeOwned>(
    name: &str,
    request: &T,
    timeout: u32,
) -> Result<R> {
    let name = pipe_name(name)?;
    let mut handle = std::ptr::null_mut();
    check(unsafe { tk_pipe_connect(name.as_ptr(), &mut handle) })?;
    let mut file = unsafe { File::from_raw_handle(handle) };
    write(&mut file, request)?;
    let response = read(&mut file, timeout)?;
    transfer(&file, &mut [1], true, 3000)?;
    Ok(response)
}
