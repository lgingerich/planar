//! Format-specific reader/writer implementations and option validation.

/// Lance format implementation.
pub mod lance;
/// Parquet format implementation.
pub mod parquet;
/// Vortex format implementation.
pub mod vortex;

use serde_json::Value;

use crate::storage::{Result, StorageError};
use std::path::Path;

/// Validates writer `format_options` for a file format.
///
/// This function parses the JSON object with the same per-format parsers used by
/// write calls, so successful validation means the options are acceptable.
pub fn validate_format_options(file_format: &str, options: &Value) -> Result<()> {
    match file_format.to_lowercase().as_str() {
        "parquet" => {
            let _ = parquet::parse_write_options(Some(options))?;
            Ok(())
        }
        "lance" => {
            let _ = lance::parse_write_options(Some(options))?;
            Ok(())
        }
        "vortex" => {
            let _ = vortex::parse_write_options(Some(options))?;
            Ok(())
        }
        other => Err(StorageError::Unsupported(format!(
            "Unsupported file format: {}",
            other
        ))),
    }
}

pub(crate) fn path_to_utf8(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Path contains invalid UTF-8",
        ))
    })
}
