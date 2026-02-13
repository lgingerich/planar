use planar::catalog::database;
use sqlx::Sqlite;

/// Test that all tables are created by migrations.
#[sqlx::test(migrations = "db/migrations")]
async fn test_schema_tables_exist(pool: sqlx::Pool<Sqlite>) -> Result<(), sqlx::Error> {
    // Configure SQLite PRAGMAs (foreign keys, etc.)
    database::sqlite::configure_pool(&pool).await?;

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
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
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
#[sqlx::test(migrations = "db/migrations")]
async fn test_schema_indexes_exist(pool: sqlx::Pool<Sqlite>) -> Result<(), sqlx::Error> {
    // Configure SQLite PRAGMAs
    database::sqlite::configure_pool(&pool).await?;

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
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
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

/// Test that foreign keys are enforced when PRAGMA foreign_keys is enabled.
#[sqlx::test(migrations = "db/migrations")]
async fn test_foreign_key_enforcement(pool: sqlx::Pool<Sqlite>) -> Result<(), sqlx::Error> {
    // Configure SQLite PRAGMAs - this is critical for foreign key enforcement
    database::sqlite::configure_pool(&pool).await?;

    // Verify foreign keys are enabled
    let fk_enabled: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await?;
    assert_eq!(fk_enabled, 1, "Foreign keys should be enabled");

    // Create a table entry to reference - use raw bytes for UUID
    let test_table_uuid_bytes = vec![0u8; 16];
    sqlx::query(
        "INSERT INTO tables (table_uuid, table_name, namespace, location, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
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
        "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp) VALUES (?1, ?2, ?3)",
    )
    .bind(test_transaction_id)
    .bind(&test_table_uuid_bytes[..])
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await?;

    // This should fail (invalid foreign key - non-existent table_uuid)
    let invalid_uuid_bytes = vec![1u8; 16];
    let result = sqlx::query(
        "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp) VALUES (?1, ?2, ?3)",
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
#[sqlx::test(migrations = "db/migrations")]
async fn test_schema_structs_match_sql_tables(pool: sqlx::Pool<Sqlite>) -> Result<(), sqlx::Error> {
    use arrow::datatypes::DataType;
    use planar::catalog::{
        Catalog, ColumnSpec, FileSpec, SchemaSpec, SqlCatalog, TableIdent, TableProperties,
        schema,
    };
    use std::sync::Arc;
    use uuid::Uuid;

    // Configure SQLite PRAGMAs
    database::sqlite::configure_pool(&pool).await?;

    // Seed one end-to-end table so decode checks run against real rows.
    let catalog = Arc::new(SqlCatalog::new(pool.clone()));
    let ident = TableIdent::new("test", "schema_sync");
    let table = catalog
        .clone()
        .create_table(
            ident.clone(),
            "/tmp/schema_sync".to_string(),
            SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int64)),
            Some(TableProperties::new()),
        )
        .await
        .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
    table
        .append_file(FileSpec::new(
            "parquet",
            "/tmp/schema_sync/part-0.parquet",
            1,
            128,
        ))
        .await
        .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;

    let table_uuid: Uuid = sqlx::query_scalar(
        "SELECT table_uuid FROM tables WHERE namespace = ?1 AND table_name = ?2 LIMIT 1",
    )
    .bind("test")
    .bind("schema_sync")
    .fetch_one(&pool)
    .await?;

    let transaction_id: Uuid =
        sqlx::query_scalar("SELECT transaction_id FROM transactions WHERE table_uuid = ?1 LIMIT 1")
            .bind(table_uuid)
            .fetch_one(&pool)
            .await?;

    let schema_uuid: Uuid =
        sqlx::query_scalar("SELECT schema_uuid FROM schemas WHERE table_uuid = ?1 LIMIT 1")
            .bind(table_uuid)
            .fetch_one(&pool)
            .await?;

    let file_uuid: Uuid =
        sqlx::query_scalar("SELECT file_uuid FROM files WHERE table_uuid = ?1 LIMIT 1")
            .bind(table_uuid)
            .fetch_one(&pool)
            .await?;

    sqlx::query(
        "INSERT OR IGNORE INTO table_stats
            (table_uuid, transaction_id, record_count, file_size_bytes, file_count, last_updated)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind(table_uuid)
    .bind(transaction_id)
    .bind(1_i64)
    .bind(128_i64)
    .bind(1_i32)
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await?;

    sqlx::query(
        "INSERT OR IGNORE INTO file_column_stats
            (file_uuid, column_name, null_count, nan_count, min_value, max_value, distinct_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(file_uuid)
    .bind("id")
    .bind(0_i64)
    .bind(0_i64)
    .bind(Some(vec![1_u8]))
    .bind(Some(vec![1_u8]))
    .bind(Some(1_i64))
    .execute(&pool)
    .await?;

    // Test tables -> schema::Table
    let _ = sqlx::query_as::<_, schema::Table>(
        "SELECT table_uuid, table_name, namespace, location, 
                current_schema_uuid, current_transaction_id, created_at, properties 
         FROM tables
         WHERE table_uuid = ?1",
    )
    .bind(table_uuid)
    .fetch_optional(&pool)
    .await?;

    // Test transactions -> schema::Transaction
    let _ = sqlx::query_as::<_, schema::Transaction>(
        "SELECT transaction_id, table_uuid, transaction_timestamp, parent_transaction_id 
         FROM transactions
         WHERE transaction_id = ?1",
    )
    .bind(transaction_id)
    .fetch_optional(&pool)
    .await?;

    // Test schemas -> schema::Schema (note: columns field is Rust-only, not in DB)
    // We'll test the database columns separately, then verify Schema struct can be built
    let _ = sqlx::query(
        "SELECT schema_uuid, table_uuid, schema_version, 
                valid_from_transaction_id, valid_to_transaction_id, created_at 
         FROM schemas
         WHERE schema_uuid = ?1",
    )
    .bind(schema_uuid)
    .fetch_optional(&pool)
    .await?;

    #[derive(sqlx::FromRow)]
    struct ColumnRow {
        column_uuid: Uuid,
        schema_uuid: Uuid,
        column_name: String,
        column_type: Vec<u8>,
        ordinal_position: i32,
        is_nullable: bool,
    }

    // Test columns table shape and decode the encoded Arrow type payload.
    let column = sqlx::query_as::<_, ColumnRow>(
        "SELECT column_uuid, schema_uuid, column_name, column_type, 
                ordinal_position, is_nullable 
         FROM columns
         WHERE schema_uuid = ?1",
    )
    .bind(schema_uuid)
    .fetch_one(&pool)
    .await?;
    assert_eq!(column.schema_uuid, schema_uuid);
    assert_eq!(column.column_name, "id");
    assert_eq!(column.ordinal_position, 1);
    assert!(!column.is_nullable);
    assert!(!column.column_uuid.is_nil());
    let decoded_type = planar::catalog::data_type::decode_data_type(&column.column_type)
        .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
    assert_eq!(decoded_type, DataType::Int64);

    // Test files -> schema::File
    let _ = sqlx::query_as::<_, schema::File>(
        "SELECT file_uuid, table_uuid, file_format, file_path, record_count, 
                file_size_bytes, added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
         FROM files
         WHERE file_uuid = ?1",
    )
    .bind(file_uuid)
    .fetch_optional(&pool)
    .await?;

    // Test table_stats -> schema::TableStats
    let _ = sqlx::query_as::<_, schema::TableStats>(
        "SELECT table_uuid, transaction_id, record_count, 
                file_size_bytes, file_count, last_updated 
         FROM table_stats
         WHERE table_uuid = ?1",
    )
    .bind(table_uuid)
    .fetch_optional(&pool)
    .await?;

    // Test file_column_stats -> schema::FileColumnStats
    let _ = sqlx::query_as::<_, schema::FileColumnStats>(
        "SELECT file_uuid, column_name, null_count, nan_count, 
                min_value, max_value, distinct_count 
         FROM file_column_stats
         WHERE file_uuid = ?1",
    )
    .bind(file_uuid)
    .fetch_optional(&pool)
    .await?;

    Ok(())
}

/// Validate event-range projection behavior under larger file counts.
#[sqlx::test(migrations = "db/migrations")]
async fn test_transaction_event_range_projection(pool: sqlx::Pool<Sqlite>) -> Result<(), sqlx::Error> {
    use arrow::datatypes::DataType;
    use planar::catalog::{Catalog, ColumnSpec, FileSpec, SchemaSpec, SqlCatalog, TableIdent};
    use std::sync::Arc;

    database::sqlite::configure_pool(&pool).await?;

    let catalog = Arc::new(SqlCatalog::new(pool.clone()));
    let ident = TableIdent::new("scale", "event_log");
    let table = catalog
        .clone()
        .create_table(
            ident.clone(),
            "/tmp/scale_event_log".to_string(),
            SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int64)),
            None,
        )
        .await
        .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;

    let initial_txn = table
        .current_transaction_id()
        .await
        .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;

    // Simulate sustained incremental commits.
    let total_files = 400;
    for i in 0..total_files {
        let file = FileSpec::new(
            "parquet",
            format!("/tmp/scale_event_log/part-{i:05}.parquet"),
            1,
            128,
        );
        table
            .append_file(file)
            .await
            .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
    }

    let head_txn = table
        .current_transaction_id()
        .await
        .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
    let events = table
        .list_transaction_events(Some(initial_txn), head_txn)
        .await
        .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;

    assert_eq!(events.len(), total_files as usize);
    let change_count: usize = events.iter().map(|event| event.file_changes.len()).sum();
    assert_eq!(change_count, total_files as usize);

    let delta = table
        .diff(initial_txn, head_txn)
        .await
        .map_err(|err| sqlx::Error::Protocol(err.to_string()))?;
    assert_eq!(delta.added_files.len(), total_files as usize);
    assert!(delta.removed_files.is_empty());

    Ok(())
}
