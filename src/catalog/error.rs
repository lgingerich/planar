use thiserror::Error;

/// Catalog operation errors
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Table not found
    #[error("table not found: {0}")]
    NotFound(String),
    /// Operation conflict (e.g., concurrent modification)
    #[error("conflict on table: {0}")]
    Conflict(String),
    /// Invalid argument provided
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Feature not yet implemented
    #[error("feature not yet implemented: {0}")]
    NotYetImplemented(String),
    /// Storage/database error
    #[error("storage error: {0}")]
    Storage(#[from] sqlx::Error),
}

/// Result type for catalog operations
pub type Result<T> = std::result::Result<T, CatalogError>;
