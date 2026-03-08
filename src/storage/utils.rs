//! Shared helpers for storage and format handling.

use serde_json::Value;

use crate::storage::{Result, StorageError};

/// Parses a JSON value as a `usize` for format option keys.
pub(crate) fn parse_usize(key: &str, value: &Value) -> Result<usize> {
    value
        .as_u64()
        .map(|v| v as usize)
        .ok_or_else(|| StorageError::Unsupported(format!("{} must be an integer", key)))
}
