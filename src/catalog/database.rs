//! Database backend abstractions and connection tuning for the catalog.
//!
//! The [`SqlBackend`] trait captures the small set of dialect-specific behaviors
//! the generic `Catalog` impl needs:
//!
//! - Write-transaction begin (`BEGIN IMMEDIATE` for SQLite vs. plain `BEGIN`
//!   for Postgres).
//! - Row-locking SQL fragment (`""` vs. `" FOR UPDATE"`).
//! - Placeholder syntax rewrite (`?N` is the canonical form used in catalog SQL;
//!   Postgres rewrites it to `$N`).
//!
//! Backend-specific connection construction (SQLite PRAGMAs, in-memory pool
//! sizing) lives in the [`sqlite`] and [`postgres`] submodules.

use std::borrow::Cow;

use async_trait::async_trait;
use sqlx::{Pool, Transaction};

/// Supported catalog database backends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DbKind {
    /// SQLite backend.
    Sqlite,
    /// PostgreSQL backend.
    Postgres,
}

/// Dialect-specific behavior the generic catalog impl depends on.
///
/// Implementations cover the small set of operations that genuinely differ
/// between SQLite and Postgres; the rest of the catalog code is written
/// generically against [`sqlx::Database`] and uses [`sqlx::QueryBuilder`] to
/// abstract placeholder syntax.
#[async_trait]
pub trait SqlBackend: sqlx::Database {
    /// Backend identifier for limit lookups and diagnostics.
    const KIND: DbKind;

    /// Begin a write transaction with appropriate isolation/locking.
    ///
    /// SQLite uses `BEGIN IMMEDIATE` to acquire a RESERVED lock up front,
    /// avoiding deadlocks under contention. Postgres uses the default `BEGIN`
    /// and relies on [`Self::lock_clause`] for explicit row locking.
    async fn begin_write(
        pool: &Pool<Self>,
    ) -> std::result::Result<Transaction<'static, Self>, sqlx::Error>;

    /// SQL fragment appended to a `SELECT ... FROM tables` query that needs to
    /// lock the table-head row before commit.
    ///
    /// SQLite returns `""` because `BEGIN IMMEDIATE` already holds the database
    /// write lock. Postgres returns `" FOR UPDATE"`.
    fn lock_clause() -> &'static str;

    /// Rewrite SQL written with `?N` placeholders to the dialect's native form.
    ///
    /// All catalog SQL is written using SQLite-style `?N` placeholders. SQLite
    /// returns the input unchanged; Postgres returns a rewritten copy where
    /// every `?` followed by an ASCII digit becomes `$`.
    ///
    /// IMPORTANT: catalog SQL must not embed `?` followed by a digit inside a
    /// string literal, or it will be rewritten. No current catalog SQL does so.
    fn prepare_sql(sql: &str) -> Cow<'_, str>;
}

/// Rewrite `?N` placeholders to `$N` for Postgres.
///
/// Walks the input character-by-character, replacing only `?` that is directly
/// followed by an ASCII digit. Bare `?` and `?` followed by other characters
/// pass through untouched.
fn rewrite_questionmark_to_dollar(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '?' && matches!(chars.peek(), Some(d) if d.is_ascii_digit()) {
            out.push('$');
            continue;
        }
        out.push(c);
    }
    out
}

#[async_trait]
impl SqlBackend for sqlx::Sqlite {
    const KIND: DbKind = DbKind::Sqlite;

    async fn begin_write(
        pool: &Pool<Self>,
    ) -> std::result::Result<Transaction<'static, Self>, sqlx::Error> {
        pool.begin_with("BEGIN IMMEDIATE").await
    }

    fn lock_clause() -> &'static str {
        ""
    }

    fn prepare_sql(sql: &str) -> Cow<'_, str> {
        Cow::Borrowed(sql)
    }
}

#[async_trait]
impl SqlBackend for sqlx::Postgres {
    const KIND: DbKind = DbKind::Postgres;

    async fn begin_write(
        pool: &Pool<Self>,
    ) -> std::result::Result<Transaction<'static, Self>, sqlx::Error> {
        pool.begin().await
    }

    fn lock_clause() -> &'static str {
        " FOR UPDATE"
    }

    fn prepare_sql(sql: &str) -> Cow<'_, str> {
        Cow::Owned(rewrite_questionmark_to_dollar(sql))
    }
}

/// SQLite-specific pool construction.
///
/// Foreign-key enforcement and journal mode are configured per connection so
/// every member of the pool sees the same settings, and in-memory databases
/// are forced to a single connection so all sessions share the same backing
/// store.
pub mod sqlite {
    use sqlx::Pool;
    use sqlx::Sqlite;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use std::str::FromStr;

    /// Heuristic for in-memory SQLite URLs.
    ///
    /// Recognized forms include `:memory:`, `sqlite::memory:`, and any URL with
    /// `mode=memory` in the query string.
    fn is_in_memory(connection_string: &str) -> bool {
        connection_string.contains(":memory:") || connection_string.contains("mode=memory")
    }

    /// Build a pool with the catalog's required SQLite settings.
    ///
    /// - Foreign keys are enabled per connection (PRAGMA `foreign_keys` is a
    ///   connection-scoped setting in SQLite, so applying it to a single pool
    ///   member would silently leave it off on every other connection).
    /// - WAL mode is enabled for file-backed databases. In-memory databases do
    ///   not support WAL and are left in the default journal mode.
    /// - In-memory URLs cap `max_connections` at 1 because each connection to
    ///   `:memory:` would otherwise create an isolated database, breaking
    ///   migration consistency and writes.
    pub async fn connect(connection_string: &str) -> std::result::Result<Pool<Sqlite>, sqlx::Error> {
        let in_memory = is_in_memory(connection_string);

        let mut connect_opts =
            SqliteConnectOptions::from_str(connection_string)?.foreign_keys(true);
        if !in_memory {
            connect_opts = connect_opts.journal_mode(SqliteJournalMode::Wal);
        }

        let max_connections = if in_memory { 1 } else { 10 };
        SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(connect_opts)
            .await
    }
}

/// PostgreSQL-specific pool construction.
pub mod postgres {
    use sqlx::Pool;
    use sqlx::Postgres;
    use sqlx::postgres::PgPoolOptions;

    /// Build a pool against the given PostgreSQL connection string.
    ///
    /// PostgreSQL enforces foreign keys by default, so no per-connection
    /// configuration is required.
    pub async fn connect(
        connection_string: &str,
    ) -> std::result::Result<Pool<Postgres>, sqlx::Error> {
        PgPoolOptions::new().connect(connection_string).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewriter_passes_sqlite_sql_unchanged() {
        let sql = "SELECT 1 FROM tables WHERE namespace = ?1 AND table_name = ?2";
        assert_eq!(<sqlx::Sqlite as SqlBackend>::prepare_sql(sql).as_ref(), sql);
    }

    #[test]
    fn rewriter_converts_question_marks_to_dollars_for_postgres() {
        let sql = "SELECT 1 FROM tables WHERE namespace = ?1 AND table_name = ?2";
        let prepared = <sqlx::Postgres as SqlBackend>::prepare_sql(sql);
        assert_eq!(
            prepared.as_ref(),
            "SELECT 1 FROM tables WHERE namespace = $1 AND table_name = $2"
        );
    }

    #[test]
    fn rewriter_preserves_bare_question_mark_and_punctuation() {
        // Bare `?` (not followed by digit) is preserved; only `?<digit>` is rewritten.
        let sql = "SELECT '?' || '?abc' || ?1 FROM x WHERE y = ?2";
        let prepared = rewrite_questionmark_to_dollar(sql);
        assert_eq!(prepared, "SELECT '?' || '?abc' || $1 FROM x WHERE y = $2");
    }

    #[test]
    fn rewriter_handles_multi_digit_placeholder_indices() {
        let sql = "VALUES (?1, ?2, ?10, ?100)";
        let prepared = rewrite_questionmark_to_dollar(sql);
        assert_eq!(prepared, "VALUES ($1, $2, $10, $100)");
    }
}
