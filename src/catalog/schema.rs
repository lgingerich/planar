use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Clone, Debug, FromRow)]
pub struct Table {
    pub table_uuid: Uuid,
    pub table_name: String,
    pub namespace: String,
    pub location: String,
    pub current_schema_uuid: Option<Uuid>,
    pub current_transaction_id: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub properties: serde_json::Value,
}

#[derive(Clone, Debug, FromRow)]
pub struct Transaction {
    pub transaction_id: i64,
    pub table_uuid: Uuid,
    pub transaction_timestamp: DateTime<Utc>,
    pub parent_transaction_id: Option<i64>,
}

#[derive(Clone, Debug, FromRow)]
pub struct Schema {
    pub schema_uuid: Uuid,
    pub table_uuid: Uuid,
    pub schema_version: i32,
    pub valid_from_transaction_id: i64,
    pub valid_to_transaction_id: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug, FromRow)]
pub struct Column {
    pub column_uuid: Uuid,
    pub schema_uuid: Uuid,
    pub column_name: String,
    pub column_type: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
}

#[derive(Clone, Debug, FromRow)]
pub struct File {
    pub file_uuid: Uuid,
    pub table_uuid: Uuid,
    pub file_format: String,
    pub file_path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub added_in_transaction_id: i64,
    pub removed_in_transaction_id: Option<i64>,
    pub partition_values: Option<serde_json::Value>,
}

#[derive(Clone, Debug, FromRow)]
pub struct TableStats {
    pub table_uuid: Uuid,
    pub transaction_id: i64,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub file_count: i32,
    pub last_updated: DateTime<Utc>,
}

#[derive(Clone, Debug, FromRow)]
pub struct FileColumnStats {
    pub file_uuid: Uuid,
    pub column_name: String,
    pub null_count: i64,
    pub nan_count: i64,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
    pub distinct_count: Option<i64>,
}