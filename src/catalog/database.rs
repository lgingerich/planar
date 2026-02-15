//! Database-specific connection tuning for catalog backends.
//!
//! These helpers keep backend-specific setup (for example, SQLite PRAGMAs)
//! separate from portable catalog logic.

/// Supported catalog database backends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DbKind {
    /// SQLite backend.
    Sqlite,
    /// PostgreSQL backend.
    Postgres,
}

/// SQLite-specific catalog connection configuration.
pub mod sqlite {
    use sqlx::Pool;
    use sqlx::Sqlite;

    /// Configures SQLite settings required by the catalog schema.
    ///
    /// Must be called after pool creation and before running migrations.
    /// SQLite does not enforce foreign keys unless explicitly enabled.
    pub async fn configure_pool(pool: &Pool<Sqlite>) -> std::result::Result<(), sqlx::Error> {
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(pool)
            .await?;
        Ok(())
    }
}

/// PostgreSQL-specific catalog connection configuration.
pub mod postgres {
    use sqlx::Pool;
    use sqlx::Postgres;

    /// Configures PostgreSQL settings for catalog usage.
    ///
    /// PostgreSQL defaults are currently sufficient; this is a no-op extension point.
    pub async fn configure_pool(_pool: &Pool<Postgres>) -> std::result::Result<(), sqlx::Error> {
        Ok(())
    }
}
