//! Planar is a transaction-based table metadata catalog with pluggable file formats.
//!
//! The crate is organized around two user-facing modules:
//! - [`catalog`]: table creation, versioned metadata, and optimistic commits.
//! - [`storage`]: Arrow `RecordBatch` readers and writers for supported formats.

/// Catalog APIs for table metadata and transactional mutations.
pub mod catalog;

/// Storage APIs for reading and writing supported file formats.
pub mod storage;
