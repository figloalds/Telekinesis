fn main() {
    println!("cargo:rerun-if-changed=native/mount.c");
    println!("cargo:rerun-if-env-changed=WINFSP_DIR");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        let root =
            std::env::var("WINFSP_DIR").unwrap_or_else(|_| "C:/Program Files (x86)/WinFsp".into());
        cc::Build::new()
            .file("native/mount.c")
            .include(format!("{root}/inc"))
            .define("UNICODE", None)
            .define("_UNICODE", None)
            .compile("tkfs_mount");
        println!("cargo:rustc-link-search=native={root}/lib");
        println!("cargo:rustc-link-lib=winfsp-x64");
        println!("cargo:rustc-link-lib=advapi32");
    }
}
