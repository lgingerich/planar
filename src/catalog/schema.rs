//! Row types that map catalog database records to Rust structs.

use arrow::datatypes::DataType;
use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

/// Database table metadata
#[derive(Clone, Debug, FromRow)]
pub struct Table {
    /// Unique table identifier
    pub table_uuid: Uuid,
    /// Table name
    pub table_name: String,
    /// Table namespace
    pub namespace: String,
    /// Storage location
    pub location: String,
    /// Current schema UUID
    pub current_schema_uuid: Option<Uuid>,
    /// Current transaction ID
    pub current_transaction_id: Option<Uuid>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Table properties
    pub properties: serde_json::Value,
}

/// Transaction record
#[derive(Clone, Debug, FromRow)]
pub struct Transaction {
    /// Transaction identifier
    pub transaction_id: Uuid,
    /// Table UUID
    pub table_uuid: Uuid,
    /// Transaction timestamp
    pub transaction_timestamp: DateTime<Utc>,
    /// Parent transaction ID if this is a child transaction
    pub parent_transaction_id: Option<Uuid>,
}

/// Schema version with columns
#[derive(Clone, Debug, FromRow)]
pub struct Schema {
    /// Schema UUID
    pub schema_uuid: Uuid,
    /// Table UUID
    pub table_uuid: Uuid,
    /// Schema version number
    pub schema_version: i32,
    /// Transaction ID where this schema becomes valid
    pub valid_from_transaction_id: Uuid,
    /// Transaction ID where this schema becomes invalid (if superseded)
    pub valid_to_transaction_id: Option<Uuid>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Column definitions
    pub columns: Vec<Column>,
}

/// Column definition
#[derive(Clone, Debug, FromRow)]
pub struct Column {
    /// Column UUID
    pub column_uuid: Uuid,
    /// Schema UUID this column belongs to
    pub schema_uuid: Uuid,
    /// Column name
    pub column_name: String,
    /// Column type string
    pub column_type: DataType,
    /// Column position in schema
    pub ordinal_position: i32,
    /// Whether the column allows null values
    pub is_nullable: bool,
}

/// File metadata
#[derive(Clone, Debug, FromRow)]
pub struct File {
    /// File UUID
    pub file_uuid: Uuid,
    /// Table UUID this file belongs to
    pub table_uuid: Uuid,
    /// File format (e.g., "parquet", "lance", "vortex")
    pub file_format: String,
    /// File path
    pub file_path: String,
    /// Number of records in the file
    pub record_count: i64,
    /// File size in bytes
    pub file_size_bytes: i64,
    /// Transaction ID where this file was added
    pub added_in_transaction_id: Uuid,
    /// Transaction ID where this file was removed (if deleted)
    pub removed_in_transaction_id: Option<Uuid>,
    /// Partition values if partitioned
    pub partition_values: Option<serde_json::Value>,
    /// Format-specific options for this file
    pub format_options: Option<serde_json::Value>,
}

/// Table statistics at a transaction
#[derive(Clone, Debug, FromRow)]
pub struct TableStats {
    /// Table UUID
    pub table_uuid: Uuid,
    /// Transaction ID these stats are for
    pub transaction_id: Uuid,
    /// Total record count
    pub record_count: i64,
    /// Total file size in bytes
    pub file_size_bytes: i64,
    /// Number of files
    pub file_count: i32,
    /// Last update timestamp
    pub last_updated: DateTime<Utc>,
}

/// Column-level statistics for a file
#[derive(Clone, Debug, FromRow)]
pub struct FileColumnStats {
    /// File UUID
    pub file_uuid: Uuid,
    /// Column name
    pub column_name: String,
    /// Number of null values
    pub null_count: i64,
    /// Number of NaN values
    pub nan_count: i64,
    /// Minimum value (serialized)
    pub min_value: Option<Vec<u8>>,
    /// Maximum value (serialized)
    pub max_value: Option<Vec<u8>>,
    /// Distinct value count
    pub distinct_count: Option<i64>,
}
