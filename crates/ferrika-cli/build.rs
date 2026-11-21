// Windows: version information in `ferrika.exe` (Explorer's Details tab,
// AppLocker and inventory tools read it), filled from this package's
// version, description and name. Building without a resource compiler only
// leaves it out, so `cargo install` still works.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        let mut res = winresource::WindowsResource::new();
        res.set("ProductName", "Ferrika")
            .set("FileDescription", "Ferrika Kafka command line")
            .set("OriginalFilename", "ferrika.exe")
            .set("InternalName", "ferrika")
            .set("LegalCopyright", "Ferrika contributors, Apache-2.0");
        if let Err(e) = res.compile() {
            println!("cargo:warning=ferrika.exe is built without version information: {e}");
        }
    }
}
