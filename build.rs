//! Build script to track migration file changes

fn main() {
    println!("cargo:rerun-if-changed=db/migrations");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        // For Python extension modules on macOS, unresolved Python symbols must
        // be looked up dynamically when the module is loaded by the interpreter.
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}