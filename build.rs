fn main() {
    println!("cargo:rerun-if-changed=native/mount.c");
    println!("cargo:rerun-if-changed=native/control.c");
    println!("cargo:rerun-if-changed=native/winfsp_loader.c");
    println!("cargo:rerun-if-env-changed=WINFSP_DIR");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        let root =
            std::env::var("WINFSP_DIR").unwrap_or_else(|_| "C:/Program Files (x86)/WinFsp".into());
        cc::Build::new()
            .file("native/mount.c")
            .file("native/control.c")
            .file("native/winfsp_loader.c")
            .include(format!("{root}/inc"))
            .define("UNICODE", None)
            .define("_UNICODE", None)
            .compile("tkfs_mount");
        println!("cargo:rustc-link-search=native={root}/lib");
        println!("cargo:rustc-link-lib=winfsp-x64");
        println!("cargo:rustc-link-lib=advapi32");
        // Start CLI/core tests without WinFsp's bin on PATH. The mount bridge
        // loads the installed DLL by absolute path before calling WinFsp.
        println!("cargo:rustc-link-lib=delayimp");
        println!("cargo:rustc-link-arg=/DELAYLOAD:winfsp-x64.dll");
    }
}
