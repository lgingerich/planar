use thiserror::Error;

use arrow::error::ArrowError;
use lance::Error as LanceError;
use parquet::errors::ParquetError;
use vortex::error::VortexError;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),
    
    #[error("Lance error: {0}")]
    Lance(#[from] LanceError),
    
    #[error("Vortex error: {0}")]
    Vortex(#[from] VortexError),
    
    #[error("Feature not yet implemented: {0}")]
    NotYetImplemented(String),
    
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

