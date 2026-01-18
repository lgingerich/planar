/// Integration tests for the catalog module.
/// 
/// These tests verify that migrations work correctly across different
/// database backends (SQLite, PostgreSQL) and that the schema is properly
/// created with all tables, indexes, and constraints.

#[cfg(feature = "sqlite")]
mod sqlite_tests;

#[cfg(feature = "postgres")]
mod postgres_tests;
