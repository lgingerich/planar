//! Error types for storage and format I/O operations.

use thiserror::Error;

use arrow::error::ArrowError;
use lance::Error as LanceError;
use parquet::errors::ParquetError;
use vortex::error::VortexError;

/// Errors produced by storage and file-format operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// I/O error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Arrow error
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),

    /// Parquet error
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),

    /// Lance error
    #[error("Lance error: {0}")]
    Lance(#[from] LanceError),

    /// Vortex error
    #[error("Vortex error: {0}")]
    Vortex(#[from] VortexError),

    /// Feature not yet implemented
    #[error("Feature not yet implemented: {0}")]
    NotYetImplemented(String),

    /// Unsupported operation
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

/// Convenient result alias for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;
