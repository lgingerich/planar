//! Database-specific connection configuration
//! 
//! Contains initialization code for database-specific settings that aren't
//! portable across databases (e.g., SQLite PRAGMAs, PostgreSQL connection options)

/// SQLite-specific configuration
pub mod sqlite {
    use sqlx::Pool;
    use sqlx::Sqlite;

    /// Configure SQLite connection settings
    /// 
    /// Should be called after creating a pool but before running migrations
    /// SQLite does not enforce foreign keys by default, so we must enable them
    /// explicitly to ensure referential integrity
    pub async fn configure_pool(pool: &Pool<Sqlite>) -> Result<(), sqlx::Error> {
        // SQLite doesn't enforce foreign keys by default - must enable via PRAGMA
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(pool)
            .await?;
        
        Ok(())
    }
}

/// PostgreSQL-specific configuration
pub mod postgres {
    use sqlx::Pool;
    use sqlx::Postgres;

    /// Configure PostgreSQL connection settings
    /// 
    /// PostgreSQL defaults are sufficient for catalog operations (foreign keys
    /// and WAL are enabled by default). Server-level settings are configured
    /// via postgresql.conf rather than per-connection
    pub async fn configure_pool(_pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
        // No configuration needed - PostgreSQL defaults are appropriate
        Ok(())
    }
}
