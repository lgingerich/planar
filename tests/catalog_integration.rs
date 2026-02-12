//! Integration test entrypoint for catalog schema checks.
//!
//! This file wires tests from `tests/integration/` into Cargo's integration
//! test discovery.

#[path = "integration/mod.rs"]
mod integration;
