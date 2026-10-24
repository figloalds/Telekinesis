fn main() {
    println!("cargo:rerun-if-changed=native/mount.c");
    println!("cargo:rerun-if-changed=native/winfsp_loader.c");
    println!("cargo:rerun-if-changed=native/winfsp_driver_manifest.h");
    println!("cargo:rerun-if-env-changed=WINFSP_DIR");
    println!("cargo:rerun-if-env-changed=TKFS_NATIVE_CACHE_PROBE");
    #[cfg(windows)]
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        slint_build::compile("ui/replacement.slint").expect("compile desktop UI");
        let root =
            std::env::var("WINFSP_DIR").unwrap_or_else(|_| "C:/Program Files (x86)/WinFsp".into());
        let mut native = cc::Build::new();
        native
            .file("native/mount.c")
            .file("native/winfsp_loader.c")
            .include(format!("{root}/inc"))
            .define("UNICODE", None)
            .define("_UNICODE", None);
        if let Ok(mode) = std::env::var("TKFS_NATIVE_CACHE_PROBE") {
            let (purge, modified_only) = match mode.as_str() {
                "purge" => ("1", false),
                "standby" => ("0", false),
                "purge-post-modified" => ("1", true),
                "standby-post-modified" => ("0", true),
                _ => panic!("invalid TKFS_NATIVE_CACHE_PROBE mode"),
            };
            println!("cargo:warning=unqualified READ-ONLY native cache probe: {mode}");
            native.define("TKFS_NATIVE_CACHE_PROBE", Some(purge));
            if modified_only {
                native.define("TKFS_NATIVE_CACHE_PROBE_POST_MODIFIED", Some("1"));
            }
        }
        native.compile("tkfs_mount");
        println!("cargo:rustc-link-search=native={root}/lib");
        println!("cargo:rustc-link-lib=winfsp-x64");
        println!("cargo:rustc-link-lib=advapi32");
        println!("cargo:rustc-link-lib=bcrypt");
        println!("cargo:rustc-link-lib=version");
        // Start CLI/core tests without WinFsp's bin on PATH. The mount bridge
        // loads the installed DLL by absolute path before calling WinFsp.
        println!("cargo:rustc-link-lib=delayimp");
        println!("cargo:rustc-link-arg=/DELAYLOAD:winfsp-x64.dll");
        println!("cargo:rustc-link-arg=/DELAY:NOBIND");
    }
}
