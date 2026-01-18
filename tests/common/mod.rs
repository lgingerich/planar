/// Shared test utilities and helpers for integration tests.
/// 
/// This module contains reusable test utilities for schema validation,
/// foreign key enforcement tests, and other common test operations.

/// Verify that a table exists in the database.
/// 
/// Returns true if the table exists, false otherwise.
/// This is database-agnostic and works with both SQLite and PostgreSQL.
#[cfg(feature = "sqlite")]
pub async fn table_exists_sqlite(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    table_name: &str,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
    )
    .bind(table_name)
    .fetch_one(pool)
    .await?;

    Ok(exists > 0)
}

#[cfg(feature = "postgres")]
pub async fn table_exists_postgres(
    pool: &sqlx::Pool<sqlx::Postgres>,
    table_name: &str,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1",
    )
    .bind(table_name)
    .fetch_one(pool)
    .await?;

    Ok(exists > 0)
}

/// Verify that an index exists in the database.
#[cfg(feature = "sqlite")]
pub async fn index_exists_sqlite(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    index_name: &str,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
    )
    .bind(index_name)
    .fetch_one(pool)
    .await?;

    Ok(exists > 0)
}

#[cfg(feature = "postgres")]
pub async fn index_exists_postgres(
    pool: &sqlx::Pool<sqlx::Postgres>,
    index_name: &str,
) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pg_indexes WHERE indexname = $1",
    )
    .bind(index_name)
    .fetch_one(pool)
    .await?;

    Ok(exists > 0)
}

