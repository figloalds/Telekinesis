fn main() {
    // Only `cargo test` on a Windows PC builds this crate for Windows. The core links
    // WinFsp and delays loading it for its own binaries alone, so without this a test
    // binary here cannot start unless WinFsp's bin is on PATH. Nothing here uses WinFsp.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=delayimp");
        println!("cargo:rustc-link-arg=/DELAYLOAD:winfsp-x64.dll");
        println!("cargo:rustc-link-arg=/DELAY:NOBIND");
    }
}
