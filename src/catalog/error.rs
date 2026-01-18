use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("table not found: {0}")]
    NotFound(String),
    #[error("conflict on table: {0}")]
    Conflict(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("feature not yet implemented: {0}")]
    NotYetImplemented(String),
    #[error("storage error: {0}")]
    Storage(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, CatalogError>;
