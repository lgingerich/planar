//! Bounded limits for catalog operations.
//!
//! All operations have explicit upper bounds to prevent unbounded resource usage
//! and tail latency spikes (TigerStyle: "put a limit on everything").

/// Maximum number of files returned per table query.
/// At ~1KB per file metadata, 10_000 files is ~10MB in memory.
pub const MAX_FILES_PER_QUERY: u32 = 10_000;

/// Maximum number of columns per schema.
/// Wide tables are rare; 1_000 is a practical upper bound.
pub const MAX_COLUMNS_PER_SCHEMA: u32 = 1_000;

/// Maximum number of tables returned by a list query.
pub const MAX_TABLES_PER_LIST: u32 = 10_000;

/// Maximum number of operations per mutation.
/// Prevents transactions from becoming too large.
pub const MAX_OPERATIONS_PER_MUTATION: u32 = 1_000;

/// Maximum number of files per append operation.
pub const MAX_FILES_PER_APPEND: u32 = 1_000;

/// Maximum number of files to delete in one operation.
pub const MAX_FILES_PER_DELETE: u32 = 1_000;

/// Maximum number of property keys to remove in one operation.
pub const MAX_PROPERTY_KEYS_TO_REMOVE: u32 = 100;

/// Maximum JSON size in bytes (for properties, partition values).
/// Prevents malicious or accidental huge property blobs.
pub const MAX_JSON_SIZE_BYTES: u32 = 1_048_576; // 1 MB

/// Batch size for multi-row INSERT of files (amortizes round-trips).
/// Must keep total bound parameters under DB limit (e.g. SQLite 999); 8 params per row.
pub const BATCH_INSERT_FILES_CHUNK: u32 = 100;

/// Batch size for UPDATE files WHERE file_uuid IN (...); 2 fixed params + N UUIDs.
pub const BATCH_DELETE_FILES_CHUNK: u32 = 500;

/// Batch size for multi-row INSERT of columns (6 params per row).
pub const BATCH_INSERT_COLUMNS_CHUNK: u32 = 100;

// Invariants (documented; Rust const assertions would require nightly):
// - MAX_FILES_PER_APPEND <= MAX_FILES_PER_QUERY
// - MAX_FILES_PER_DELETE <= MAX_FILES_PER_QUERY
// - MAX_OPERATIONS_PER_MUTATION >= MAX_FILES_PER_APPEND
// - MAX_JSON_SIZE_BYTES <= 10_485_760 (10 MB)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_are_reasonable() {
        assert!(
            MAX_FILES_PER_APPEND <= MAX_FILES_PER_QUERY,
            "append limit must not exceed query limit"
        );
        assert!(
            MAX_FILES_PER_DELETE <= MAX_FILES_PER_QUERY,
            "delete limit must not exceed query limit"
        );
        assert!(
            MAX_OPERATIONS_PER_MUTATION >= MAX_FILES_PER_APPEND,
            "mutation ops limit must allow at least one full append"
        );
        assert!(
            MAX_JSON_SIZE_BYTES <= 10_485_760,
            "JSON size limit must not exceed 10 MB"
        );
    }
}
