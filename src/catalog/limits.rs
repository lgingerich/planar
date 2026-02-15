//! Bounded limits for catalog operations.
//!
//! All operations have explicit upper bounds to prevent unbounded resource usage
//! and tail latency spikes (TigerStyle: "put a limit on everything").

use super::database::DbKind;

// TODO: Review and update all limits below.

/// Maximum number of files returned per table query.
/// At ~1KB per file metadata, 10_000 files is ~10MB in memory.
pub const MAX_FILES_PER_QUERY: u32 = 10_000;

/// Maximum number of columns per schema.
/// Wide tables are rare; 1_000 is a practical upper bound.
pub const MAX_COLUMNS_PER_SCHEMA: u32 = 1_000;

/// Maximum number of tables returned by a list query.
pub const MAX_TABLES_PER_LIST: u32 = 10_000;

/// Maximum number of transactions scanned in one event-log range query.
pub const MAX_TRANSACTIONS_PER_SCAN: u32 = 50_000;

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
/// Must keep total bound parameters under DB limit; 9 params per row.
pub const BATCH_INSERT_FILES_CHUNK: u32 = 100;

/// Batch size for UPDATE files WHERE file_uuid IN (...); 2 fixed params + N UUIDs.
pub const BATCH_DELETE_FILES_CHUNK: u32 = 500;

// PostgreSQL Bind parameter count ceiling.
// Reference: https://www.postgresql.org/docs/current/limits.html
const POSTGRES_MAX_BIND_PARAMS: u32 = 65_535;

// SQLite host parameter ceiling (SQLITE_MAX_VARIABLE_NUMBER default).
// Reference: https://www.sqlite.org/limits.html
const SQLITE_MAX_BIND_PARAMS: u32 = 32_766;

const COLUMN_INSERT_PARAMS_PER_ROW: u32 = 6;
const FILE_INSERT_PARAMS_PER_ROW: u32 = 9;
const DELETE_FILES_FIXED_PARAMS: u32 = 2;

/// Maximum bind parameters allowed for a backend.
pub const fn db_bind_limit(db: DbKind) -> u32 {
    match db {
        DbKind::Sqlite => SQLITE_MAX_BIND_PARAMS,
        DbKind::Postgres => POSTGRES_MAX_BIND_PARAMS,
    }
}

/// Total bind parameters for N rows of `INSERT INTO columns`.
pub const fn column_insert_bind_count(rows: u32) -> u32 {
    rows * COLUMN_INSERT_PARAMS_PER_ROW
}

/// Total bind parameters for N rows of `INSERT INTO files`.
pub const fn file_insert_bind_count(rows: u32) -> u32 {
    rows * FILE_INSERT_PARAMS_PER_ROW
}

/// Total bind parameters for N UUIDs in `UPDATE files ... IN (...)`.
pub const fn delete_files_bind_count(rows: u32) -> u32 {
    DELETE_FILES_FIXED_PARAMS + rows
}

/// DB-specific batch size for multi-row INSERT of columns.
///
/// Formula: floor(max_bind_params / 6), then clamp to `MAX_COLUMNS_PER_SCHEMA`.
pub const fn batch_insert_columns_chunk(db: DbKind) -> u32 {
    let max_rows_from_bind_limit = db_bind_limit(db) / COLUMN_INSERT_PARAMS_PER_ROW;
    if max_rows_from_bind_limit < MAX_COLUMNS_PER_SCHEMA {
        max_rows_from_bind_limit
    } else {
        MAX_COLUMNS_PER_SCHEMA
    }
}

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

    #[test]
    fn batch_insert_columns_chunk_is_db_specific_and_clamped() {
        assert_eq!(batch_insert_columns_chunk(DbKind::Sqlite), 1_000);
        assert_eq!(batch_insert_columns_chunk(DbKind::Postgres), 1_000);
    }

    #[test]
    fn column_insert_shape_is_locked_to_six_binds() {
        assert_eq!(
            column_insert_bind_count(1),
            6,
            "If INSERT INTO columns adds/removes fields, update bind math and tests explicitly."
        );
    }

    #[test]
    fn file_insert_shape_is_locked_to_nine_binds() {
        assert_eq!(
            file_insert_bind_count(1),
            9,
            "If INSERT INTO files adds/removes fields, update bind math and tests explicitly."
        );
    }

    #[test]
    fn delete_files_shape_is_locked_to_two_fixed_params() {
        assert_eq!(
            delete_files_bind_count(0),
            2,
            "If DELETE files query fixed params change, update bind math and tests explicitly."
        );
    }
}
