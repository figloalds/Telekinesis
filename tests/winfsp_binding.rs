#![cfg(windows)]
use std::{
    ffi::{CStr, c_void},
    os::windows::ffi::OsStrExt,
};

#[link(name = "kernel32")]
unsafe extern "system" {
    fn LoadLibraryW(path: *const u16) -> *mut c_void;
    fn GetModuleHandleW(name: *const u16) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const u8) -> *mut c_void;
    fn VirtualProtect(address: *mut c_void, length: usize, protection: u32, old: *mut u32) -> i32;
}
unsafe extern "C" {
    fn tk_winfsp_bind_imports(module: *mut c_void) -> u32;
    fn tk_winfsp_error_detail(buffer: *mut u16, count: u32);
    fn tk_mount_start(
        path: *const u16,
        out: *mut usize,
        threads: u32,
        timeout: u32,
        fault: u32,
    ) -> u32;
}
unsafe extern "system" {
    fn __HrLoadAllImportsForDll(name: *const u8) -> i32;
    static mut __pfnDliNotifyHook2: usize;
}

fn load(name: &str) -> *mut c_void {
    let path = std::path::PathBuf::from(std::env::var(name).unwrap());
    let wide: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let module = unsafe { LoadLibraryW(wide.as_ptr()) };
    assert!(
        !module.is_null(),
        "{}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
    module
}
unsafe fn slots() -> (*mut usize, *mut usize, *const u8) {
    let image = unsafe { GetModuleHandleW(std::ptr::null()) } as *mut u8;
    let word = |at: usize| unsafe { (image.add(at) as *const u32).read_unaligned() as usize };
    let pe = word(0x3c);
    let directory = word(pe + 24 + 112 + 13 * 8);
    for index in 0..64 {
        let at = directory + index * 32;
        let name = word(at + 4);
        assert_ne!(name, 0, "missing WinFsp delay-import descriptor");
        let dll = unsafe { CStr::from_ptr(image.add(name).cast()) };
        if dll.to_bytes().eq_ignore_ascii_case(b"winfsp-x64.dll") {
            let thunk = unsafe { (image.add(word(at + 16)) as *const usize).read_unaligned() };
            return unsafe {
                (
                    image.add(word(at + 8)).cast(),
                    image.add(word(at + 12)).cast(),
                    image.add(thunk + 2),
                )
            };
        }
    }
    panic!("invalid fixture delay imports")
}
unsafe fn replace(slot: *mut usize, value: usize) {
    let mut old = 0;
    assert_ne!(
        unsafe { VirtualProtect(slot.cast(), size_of::<usize>(), 4, &mut old) },
        0
    );
    unsafe { slot.write(value) };
    let mut ignored = 0;
    assert_ne!(
        unsafe { VirtualProtect(slot.cast(), size_of::<usize>(), old, &mut ignored) },
        0
    );
}

#[test]
#[ignore = "scripts/check_winfsp_binding.py builds inert DLLs and bounds isolated fixture children"]
fn binding_fixture() {
    // Keep the actual native bridge/import descriptor linked, without invoking
    // any WinFsp API. Every fixture export exits 88 if called accidentally.
    std::hint::black_box(tk_mount_start as *const ());
    std::hint::black_box(tkfs::mount::tk_call as *const ());
    let first = load("TKFS_BINDING_DLL_A");
    let second = load("TKFS_BINDING_DLL_B");
    assert_ne!(
        first, second,
        "fixture DLLs need distinct loaded identities"
    );
    let mode = std::env::var("TKFS_BINDING_CASE").unwrap();
    let status = match mode.as_str() {
        "default-helper-baseline" => unsafe {
            // Reproduce the previous loader's basename selection, only in this
            // inert child: absolute B is loaded, but the default helper uses A.
            let hook = std::ptr::addr_of_mut!(__pfnDliNotifyHook2);
            let saved = hook.read();
            replace(hook, 0);
            assert_eq!(
                __HrLoadAllImportsForDll(c"winfsp-x64.dll".as_ptr().cast()),
                0
            );
            let (module, iat, name) = slots();
            assert_eq!(module.read(), first as usize);
            assert_eq!(iat.read(), GetProcAddress(first, name) as usize);
            assert_ne!(iat.read(), GetProcAddress(second, name) as usize);
            replace(hook, saved);
            println!("default helper selected preloaded A despite absolute B load");
            0
        },
        "preloaded" => unsafe { tk_winfsp_bind_imports(second) },
        "prebound" => unsafe {
            let (module, iat, name) = slots();
            replace(module, first as usize);
            replace(iat, GetProcAddress(first, name) as usize);
            tk_winfsp_bind_imports(second)
        },
        "mixed-iat" => unsafe {
            let (_, iat, name) = slots();
            replace(iat, GetProcAddress(first, name) as usize);
            tk_winfsp_bind_imports(second)
        },
        "freeze" => unsafe {
            assert_eq!(tk_winfsp_bind_imports(first), 0);
            tk_winfsp_bind_imports(second)
        },
        _ => panic!("unknown binding case {mode}"),
    };
    let expected = if matches!(mode.as_str(), "preloaded" | "default-helper-baseline") {
        0
    } else {
        1306
    };
    let mut detail = [0; 2048];
    unsafe { tk_winfsp_error_detail(detail.as_mut_ptr(), detail.len() as u32) };
    let end = detail
        .iter()
        .position(|value| *value == 0)
        .unwrap_or(detail.len());
    assert_eq!(
        status,
        expected,
        "{}",
        String::from_utf16_lossy(&detail[..end])
    );
    println!("binding fixture {mode}: {status}; no WinFsp export invoked");
}
