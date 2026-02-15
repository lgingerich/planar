use arrow::datatypes::DataType;
use planar::catalog::{Catalog, ColumnSpec, FileSpec, SchemaSpec, SqlCatalog, TableIdent};
use planar::storage::Format;

async fn commit_append_file(
    table: &planar::catalog::TableHandle,
    file: FileSpec,
) -> planar::catalog::Result<planar::catalog::CommitResult> {
    table.write(None).await?.append_file(file).commit().await
}

async fn commit_update_schema(
    table: &planar::catalog::TableHandle,
    schema: SchemaSpec,
) -> planar::catalog::Result<planar::catalog::CommitResult> {
    table
        .write(None)
        .await?
        .update_schema(schema)
        .commit()
        .await
}

/// Test successful schema evolution with safe type widening
#[tokio::test]
async fn test_safe_type_widening() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = SqlCatalog::in_memory().await?;

    // Create table with Int32 column
    let table_ident = TableIdent::new("test", "users");
    let initial_schema = SchemaSpec::new()
        .with_column(ColumnSpec::new("id", DataType::Int32))
        .with_column(ColumnSpec::new("name", DataType::Utf8));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/users".to_string(),
            initial_schema,
            None,
        )
        .await?;

    // Add a file
    let file = FileSpec::new(Format::Parquet, "/test/users/part-0.parquet", 100, 1024);
    commit_append_file(&table, file).await?;

    let view_before = table.read().await?;
    assert_eq!(view_before.schema.columns[0].column_type, DataType::Int32);

    // Evolve Int32 -> Int64 (safe widening)
    let new_schema = SchemaSpec::new()
        .with_column(ColumnSpec::new("id", DataType::Int64))
        .with_column(ColumnSpec::new("name", DataType::Utf8));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(result.is_ok(), "Int32 -> Int64 should be allowed");

    // Verify new schema is in effect
    let view_after = table.read().await?;
    assert_eq!(view_after.schema.columns[0].column_type, DataType::Int64);
    assert_eq!(view_after.schema.schema_version, 2);

    // Verify time travel still works - old schema at old transaction
    let view_at_old_txn = table.read_at(view_before.transaction_id).await?;
    assert_eq!(
        view_at_old_txn.schema.columns[0].column_type,
        DataType::Int32
    );
    assert_eq!(view_at_old_txn.schema.schema_version, 1);

    Ok(())
}

/// Test that unsafe type narrowing is rejected
#[tokio::test]
async fn test_unsafe_type_narrowing_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = SqlCatalog::in_memory().await?;

    // Create table with Int64 column
    let table_ident = TableIdent::new("test", "metrics");
    let initial_schema = SchemaSpec::new().with_column(ColumnSpec::new("value", DataType::Int64));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/metrics".to_string(),
            initial_schema,
            None,
        )
        .await?;

    // Add a file
    let file = FileSpec::new(Format::Parquet, "/test/metrics/part-0.parquet", 100, 1024);
    commit_append_file(&table, file).await?;

    // Attempt to narrow Int64 -> Int32 (unsafe)
    let new_schema = SchemaSpec::new().with_column(ColumnSpec::new("value", DataType::Int32));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(result.is_err(), "Int64 -> Int32 should be rejected");

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("cannot evolve"),
        "Error should mention evolution failure"
    );
    assert!(
        error_msg.contains("value"),
        "Error should mention column name"
    );

    Ok(())
}

/// Test timestamp precision increase
#[tokio::test]
async fn test_timestamp_precision_increase() -> Result<(), Box<dyn std::error::Error>> {
    use arrow::datatypes::TimeUnit;

    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "events");
    let initial_schema = SchemaSpec::new().with_column(ColumnSpec::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
    ));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/events".to_string(),
            initial_schema,
            None,
        )
        .await?;

    // Evolve Millisecond -> Microsecond (safe)
    let new_schema = SchemaSpec::new().with_column(ColumnSpec::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    ));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(
        result.is_ok(),
        "Timestamp precision increase should be allowed"
    );

    // Verify new precision
    let view = table.read().await?;
    if let DataType::Timestamp(unit, _) = &view.schema.columns[0].column_type {
        assert_eq!(*unit, TimeUnit::Microsecond);
    } else {
        panic!("Expected Timestamp type");
    }

    Ok(())
}

/// Test timestamp precision decrease is rejected
#[tokio::test]
async fn test_timestamp_precision_decrease_rejected() -> Result<(), Box<dyn std::error::Error>> {
    use arrow::datatypes::TimeUnit;

    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "events");
    let initial_schema = SchemaSpec::new().with_column(ColumnSpec::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    ));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/events".to_string(),
            initial_schema,
            None,
        )
        .await?;

    // Attempt to decrease precision Microsecond -> Millisecond (unsafe)
    let new_schema = SchemaSpec::new().with_column(ColumnSpec::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
    ));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(
        result.is_err(),
        "Timestamp precision decrease should be rejected"
    );

    Ok(())
}

/// Test that timezone changes are rejected
#[tokio::test]
async fn test_timestamp_timezone_change_rejected() -> Result<(), Box<dyn std::error::Error>> {
    use arrow::datatypes::TimeUnit;

    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "events");
    let initial_schema = SchemaSpec::new().with_column(ColumnSpec::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    ));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/events".to_string(),
            initial_schema,
            None,
        )
        .await?;

    // Attempt to change timezone
    let new_schema = SchemaSpec::new().with_column(ColumnSpec::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
    ));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(result.is_err(), "Timezone change should be rejected");

    Ok(())
}

/// Test making a column nullable (safe)
#[tokio::test]
async fn test_making_column_nullable() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "products");
    let initial_schema = SchemaSpec::new()
        .with_column(ColumnSpec::new("id", DataType::Int64)) // non-nullable
        .with_column(ColumnSpec::new("name", DataType::Utf8));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/products".to_string(),
            initial_schema,
            None,
        )
        .await?;

    let view_before = table.read().await?;
    assert!(!view_before.schema.columns[0].is_nullable);

    // Make id nullable (safe)
    let new_schema = SchemaSpec::new()
        .with_column(ColumnSpec::new("id", DataType::Int64).nullable())
        .with_column(ColumnSpec::new("name", DataType::Utf8));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(result.is_ok(), "Making column nullable should be allowed");

    let view_after = table.read().await?;
    assert!(view_after.schema.columns[0].is_nullable);

    Ok(())
}

/// Test making a column non-nullable (unsafe)
#[tokio::test]
async fn test_making_column_non_nullable_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "products");
    let initial_schema = SchemaSpec::new()
        .with_column(ColumnSpec::new("id", DataType::Int64).nullable()) // nullable
        .with_column(ColumnSpec::new("name", DataType::Utf8));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/products".to_string(),
            initial_schema,
            None,
        )
        .await?;

    // Attempt to make id non-nullable (unsafe - existing nulls would violate)
    let new_schema = SchemaSpec::new()
        .with_column(ColumnSpec::new("id", DataType::Int64)) // non-nullable
        .with_column(ColumnSpec::new("name", DataType::Utf8));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(
        result.is_err(),
        "Making nullable column non-nullable should be rejected"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("nullable") || error_msg.contains("non-nullable"));

    Ok(())
}

/// Test adding new columns (always safe)
#[tokio::test]
async fn test_adding_new_columns() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "users");
    let initial_schema = SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int64));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/users".to_string(),
            initial_schema,
            None,
        )
        .await?;

    let view_before = table.read().await?;
    assert_eq!(view_before.schema.columns.len(), 1);

    // Add new columns
    let new_schema = SchemaSpec::new()
        .with_column(ColumnSpec::new("id", DataType::Int64))
        .with_column(ColumnSpec::new("email", DataType::Utf8).nullable())
        .with_column(ColumnSpec::new("age", DataType::Int32).nullable());

    let result = commit_update_schema(&table, new_schema).await;
    assert!(result.is_ok(), "Adding new columns should be allowed");

    let view_after = table.read().await?;
    assert_eq!(view_after.schema.columns.len(), 3);
    assert_eq!(view_after.schema.columns[1].column_name, "email");
    assert_eq!(view_after.schema.columns[2].column_name, "age");

    Ok(())
}

/// Test incompatible type changes are rejected
#[tokio::test]
async fn test_incompatible_type_change_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "data");
    let initial_schema = SchemaSpec::new().with_column(ColumnSpec::new("value", DataType::Utf8));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/data".to_string(),
            initial_schema,
            None,
        )
        .await?;

    // Attempt to change Utf8 -> Int64 (incompatible)
    let new_schema = SchemaSpec::new().with_column(ColumnSpec::new("value", DataType::Int64));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(
        result.is_err(),
        "Incompatible type change should be rejected"
    );

    Ok(())
}

/// Test float widening
#[tokio::test]
async fn test_float_widening() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "measurements");
    let initial_schema =
        SchemaSpec::new().with_column(ColumnSpec::new("temperature", DataType::Float32));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/measurements".to_string(),
            initial_schema,
            None,
        )
        .await?;

    // Evolve Float32 -> Float64 (safe)
    let new_schema =
        SchemaSpec::new().with_column(ColumnSpec::new("temperature", DataType::Float64));

    let result = commit_update_schema(&table, new_schema).await;
    assert!(result.is_ok(), "Float32 -> Float64 should be allowed");

    let view = table.read().await?;
    assert_eq!(view.schema.columns[0].column_type, DataType::Float64);

    Ok(())
}

/// Test multiple schema evolutions in sequence
#[tokio::test]
async fn test_multiple_schema_evolutions() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = SqlCatalog::in_memory().await?;

    let table_ident = TableIdent::new("test", "evolving");
    let schema_v1 = SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int8));

    let table = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/test/evolving".to_string(),
            schema_v1,
            None,
        )
        .await?;

    let v1 = table.read().await?;
    assert_eq!(v1.schema.schema_version, 1);
    assert_eq!(v1.schema.columns[0].column_type, DataType::Int8);

    // Evolution 1: Int8 -> Int16
    let schema_v2 = SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int16));
    commit_update_schema(&table, schema_v2).await?;

    let v2 = table.read().await?;
    assert_eq!(v2.schema.schema_version, 2);
    assert_eq!(v2.schema.columns[0].column_type, DataType::Int16);

    // Evolution 2: Int16 -> Int32
    let schema_v3 = SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int32));
    commit_update_schema(&table, schema_v3).await?;

    let v3 = table.read().await?;
    assert_eq!(v3.schema.schema_version, 3);
    assert_eq!(v3.schema.columns[0].column_type, DataType::Int32);

    // Evolution 3: Int32 -> Int64
    let schema_v4 = SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int64));
    commit_update_schema(&table, schema_v4).await?;

    let v4 = table.read().await?;
    assert_eq!(v4.schema.schema_version, 4);
    assert_eq!(v4.schema.columns[0].column_type, DataType::Int64);

    // Verify time travel to each version
    assert_eq!(
        table.read_at(v1.transaction_id).await?.schema.columns[0].column_type,
        DataType::Int8
    );
    assert_eq!(
        table.read_at(v2.transaction_id).await?.schema.columns[0].column_type,
        DataType::Int16
    );
    assert_eq!(
        table.read_at(v3.transaction_id).await?.schema.columns[0].column_type,
        DataType::Int32
    );
    assert_eq!(
        table.read_at(v4.transaction_id).await?.schema.columns[0].column_type,
        DataType::Int64
    );

    Ok(())
}
