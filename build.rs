//! Build script to track migration file changes

fn main() {
    println!("cargo:rerun-if-changed=db/migrations");
}