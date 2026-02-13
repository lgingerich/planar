use sqlx::Row;

use super::{
    CatalogError, Result, TableDelta, TableProperties, TxnEvent, TxnFileChangeKind, TxnId, limits,
    schema,
};

/// Extract UUID from a database row
pub(super) fn uuid_from_row<R>(row: &R, column: &str) -> Result<uuid::Uuid>
where
    R: Row,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
{
    let bytes: Vec<u8> = row.try_get(column).map_err(CatalogError::Storage)?;
    uuid::Uuid::from_slice(&bytes)
        .map_err(|error| CatalogError::InvalidArgument(format!("invalid uuid: {error}")))
}

/// Extract optional UUID from a database row
pub(super) fn uuid_from_row_optional<R>(row: &R, column: &str) -> Result<Option<uuid::Uuid>>
where
    R: Row,
    for<'r> Option<Vec<u8>>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
{
    let bytes: Option<Vec<u8>> = row.try_get(column).map_err(CatalogError::Storage)?;
    match bytes {
        Some(bytes) => uuid::Uuid::from_slice(&bytes)
            .map(Some)
            .map_err(|error| CatalogError::InvalidArgument(format!("invalid uuid: {error}"))),
        None => Ok(None),
    }
}

/// Parse JSON from optional string (defaults to empty object).
/// Enforces MAX_JSON_SIZE_BYTES to prevent unbounded allocation.
pub(super) fn parse_json(value: Option<String>) -> Result<serde_json::Value> {
    match value {
        Some(text) => {
            if text.len() as u32 > limits::MAX_JSON_SIZE_BYTES {
                return Err(CatalogError::LimitExceeded(format!(
                    "JSON size {} bytes exceeds limit of {} bytes",
                    text.len(),
                    limits::MAX_JSON_SIZE_BYTES
                )));
            }
            serde_json::from_str(&text)
                .map_err(|error| CatalogError::InvalidArgument(format!("invalid json: {error}")))
        }
        None => Ok(serde_json::json!({})),
    }
}

/// Parse table properties from optional string (defaults to empty object).
pub(super) fn parse_table_properties(value: Option<String>) -> Result<TableProperties> {
    TableProperties::from_json(parse_json(value)?)
}

/// Parse optional JSON from optional string.
/// Enforces MAX_JSON_SIZE_BYTES when present.
pub(super) fn parse_json_optional(value: Option<String>) -> Result<Option<serde_json::Value>> {
    match value {
        Some(text) => {
            if text.len() as u32 > limits::MAX_JSON_SIZE_BYTES {
                return Err(CatalogError::LimitExceeded(format!(
                    "JSON size {} bytes exceeds limit of {} bytes",
                    text.len(),
                    limits::MAX_JSON_SIZE_BYTES
                )));
            }
            serde_json::from_str(&text)
                .map(Some)
                .map_err(|error| CatalogError::InvalidArgument(format!("invalid json: {error}")))
        }
        None => Ok(None),
    }
}

/// Serialize JSON value to string.
/// Enforces MAX_JSON_SIZE_BYTES to prevent unbounded output.
pub(super) fn serialize_json(value: &serde_json::Value) -> Result<String> {
    let json_str = serde_json::to_string(value)
        .map_err(|error| CatalogError::InvalidArgument(format!("invalid json: {error}")))?;
    if json_str.len() as u32 > limits::MAX_JSON_SIZE_BYTES {
        return Err(CatalogError::LimitExceeded(format!(
            "JSON size {} bytes exceeds limit of {} bytes",
            json_str.len(),
            limits::MAX_JSON_SIZE_BYTES
        )));
    }
    Ok(json_str)
}

/// Serialize optional JSON value to optional string.
pub(super) fn serialize_json_optional(value: Option<&serde_json::Value>) -> Result<Option<String>> {
    value.map(serialize_json).transpose()
}

/// Project a net table delta from ordered transaction events.
pub(super) fn project_delta_range(
    from_transaction_id: TxnId,
    to_transaction_id: TxnId,
    events: &[TxnEvent],
    new_schema: Option<schema::Schema>,
    new_properties: Option<TableProperties>,
) -> TableDelta {
    let mut added_by_uuid = std::collections::HashMap::<uuid::Uuid, schema::File>::new();
    let mut removed_by_uuid = std::collections::HashMap::<uuid::Uuid, schema::File>::new();

    for event in events {
        for change in &event.file_changes {
            let file_uuid = change.file.file_uuid;
            match change.kind {
                TxnFileChangeKind::Added => {
                    // If a file was previously marked removed in this range, a later add
                    // wins for net state at `to_transaction_id`.
                    removed_by_uuid.remove(&file_uuid);
                    added_by_uuid.insert(file_uuid, change.file.clone());
                }
                TxnFileChangeKind::Removed => {
                    // A remove cancels any prior add in this same range.
                    if added_by_uuid.remove(&file_uuid).is_none() {
                        removed_by_uuid.insert(file_uuid, change.file.clone());
                    }
                }
            }
        }
    }

    TableDelta {
        from_transaction_id,
        to_transaction_id,
        added_files: added_by_uuid.into_values().collect(),
        removed_files: removed_by_uuid.into_values().collect(),
        new_schema,
        new_properties,
    }
}

/// Generate a new transaction ID using UUIDv7
///
/// UUIDv7 is time-ordered and can be generated without database coordination,
/// enabling distributed transaction ID generation.
pub(super) fn next_transaction_id() -> TxnId {
    let uuid7_value = uuid7::uuid7();
    uuid::Uuid::from_bytes(*uuid7_value.as_bytes())
}
