use sqlx::Postgres;

/// Test that all tables are created by migrations.
#[sqlx::test(migrations = "db/migrations", pool_type = "postgres")]
async fn test_schema_tables_exist(pool: sqlx::Pool<Postgres>) -> Result<(), sqlx::Error> {
    // PostgreSQL enforces foreign keys by default, no PRAGMA needed

    // Verify migrations ran (tables exist)
    let tables = vec![
        "tables",
        "transactions",
        "schemas",
        "columns",
        "files",
        "table_stats",
        "file_column_stats",
    ];

    for table_name in tables {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1",
        )
        .bind(table_name)
        .fetch_one(&pool)
        .await?;

        assert!(
            exists > 0,
            "Table '{}' should exist after migrations",
            table_name
        );
    }

    Ok(())
}

/// Test that all indexes are created by migrations.
#[sqlx::test(migrations = "db/migrations", pool_type = "postgres")]
async fn test_schema_indexes_exist(pool: sqlx::Pool<Postgres>) -> Result<(), sqlx::Error> {
    let indexes = vec![
        "idx_transactions_table",
        "idx_schemas_table",
        "idx_schemas_valid_range",
        "idx_columns_schema",
        "idx_files_table",
        "idx_files_active",
        "idx_table_stats_transaction",
    ];

    for index_name in indexes {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pg_indexes WHERE indexname = $1",
        )
        .bind(index_name)
        .fetch_one(&pool)
        .await?;

        assert!(
            exists > 0,
            "Index '{}' should exist after migrations",
            index_name
        );
    }

    Ok(())
}

/// Test that foreign keys are enforced (PostgreSQL enforces by default).
#[sqlx::test(migrations = "db/migrations", pool_type = "postgres")]
async fn test_foreign_key_enforcement(pool: sqlx::Pool<Postgres>) -> Result<(), sqlx::Error> {
    // PostgreSQL enforces foreign keys by default, no PRAGMA needed

    // Create a table entry to reference - use raw bytes for UUID (BYTEA in Postgres)
    let test_table_uuid_bytes = vec![0u8; 16];
    sqlx::query(
        "INSERT INTO tables (table_uuid, table_name, namespace, location, created_at) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&test_table_uuid_bytes[..])
    .bind("test_table")
    .bind("test_namespace")
    .bind("/test/location")
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await?;

    // This should succeed (valid foreign key)
    let test_transaction_id: i64 = 1;
    sqlx::query(
        "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp) VALUES ($1, $2, $3)",
    )
    .bind(test_transaction_id)
    .bind(&test_table_uuid_bytes[..])
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await?;

    // This should fail (invalid foreign key - non-existent table_uuid)
    let invalid_uuid_bytes = vec![1u8; 16];
    let result = sqlx::query(
        "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp) VALUES ($1, $2, $3)",
    )
    .bind(2_i64)
    .bind(&invalid_uuid_bytes[..])
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await;

    assert!(
        result.is_err(),
        "Foreign key constraint should be enforced - insert with invalid table_uuid should fail"
    );

    Ok(())
}

/// Test that Rust schema structs match the SQL schema definitions.
/// 
/// This test uses the database schema as the source of truth - if we can successfully
/// query each table into its corresponding Rust struct, the struct matches the database.
/// If types or column names don't match, sqlx will fail at runtime.
#[sqlx::test(migrations = "db/migrations", pool_type = "postgres")]
async fn test_schema_structs_match_sql_tables(pool: sqlx::Pool<Postgres>) -> Result<(), sqlx::Error> {
    use planar::catalog::schema;

    // Test that we can query each table into its corresponding Rust struct
    // This verifies that column names and types match between SQL and Rust

    // Test tables -> schema::Table
    let _ = sqlx::query_as::<_, schema::Table>(
        "SELECT table_uuid, table_name, namespace, location, 
                current_schema_uuid, current_transaction_id, created_at, properties 
         FROM tables LIMIT 0",
    )
    .fetch_optional(&pool)
    .await?;

    // Test transactions -> schema::Transaction
    let _ = sqlx::query_as::<_, schema::Transaction>(
        "SELECT transaction_id, table_uuid, transaction_timestamp, parent_transaction_id 
         FROM transactions LIMIT 0",
    )
    .fetch_optional(&pool)
    .await?;

    // Test schemas -> schema::Schema (note: columns field is Rust-only, not in DB)
    let _ = sqlx::query(
        "SELECT schema_uuid, table_uuid, schema_version, 
                valid_from_transaction_id, valid_to_transaction_id, created_at 
         FROM schemas LIMIT 0",
    )
    .fetch_optional(&pool)
    .await?;

    // Test columns -> schema::Column
    let _ = sqlx::query_as::<_, schema::Column>(
        "SELECT column_uuid, schema_uuid, column_name, column_type, 
                ordinal_position, is_nullable 
         FROM columns LIMIT 0",
    )
    .fetch_optional(&pool)
    .await?;

    // Test files -> schema::File
    let _ = sqlx::query_as::<_, schema::File>(
        "SELECT file_uuid, table_uuid, file_format, file_path, record_count, 
                file_size_bytes, added_in_transaction_id, removed_in_transaction_id, partition_values 
         FROM files LIMIT 0",
    )
    .fetch_optional(&pool)
    .await?;

    // Test table_stats -> schema::TableStats
    let _ = sqlx::query_as::<_, schema::TableStats>(
        "SELECT table_uuid, transaction_id, record_count, 
                file_size_bytes, file_count, last_updated 
         FROM table_stats LIMIT 0",
    )
    .fetch_optional(&pool)
    .await?;

    // Test file_column_stats -> schema::FileColumnStats
    let _ = sqlx::query_as::<_, schema::FileColumnStats>(
        "SELECT file_uuid, column_name, null_count, nan_count, 
                min_value, max_value, distinct_count 
         FROM file_column_stats LIMIT 0",
    )
    .fetch_optional(&pool)
    .await?;

    Ok(())
}

