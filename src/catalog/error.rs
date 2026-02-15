//! Error types for catalog metadata operations.

use thiserror::Error;

/// Errors returned by catalog operations.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Table not found
    #[error("table not found: {0}")]
    NotFound(String),
    /// Operation conflict
    #[error("conflict on table: {0}")]
    Conflict(String),
    /// Resource limit exceeded
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    /// Invalid argument provided
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// Client protocol is incompatible with table requirements
    #[error(
        "incompatible {operation} protocol for table {table}: client={client_version}, required_min={required_min_version}"
    )]
    ProtocolVersionIncompatible {
        /// Operation being attempted (for example, read or write)
        operation: &'static str,
        /// Table identifier rendered as namespace.name
        table: String,
        /// Client protocol version used by this catalog instance
        client_version: i32,
        /// Minimum protocol version required by the table
        required_min_version: i32,
    },
    /// Feature not yet implemented
    #[error("feature not yet implemented: {0}")]
    NotYetImplemented(String),
    /// Storage/database error
    #[error("storage error: {0}")]
    Storage(#[from] sqlx::Error),
    /// Arrow error
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Convenient result alias for catalog operations.
pub type Result<T> = std::result::Result<T, CatalogError>;
