fn main() {
    // Migrations are embedded at compile time; cargo cannot see that on its own.
    println!("cargo:rerun-if-changed=migrations");
}
