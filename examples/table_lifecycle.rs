//! Table lifecycle example demonstrating Planar's core features.
//!
//! This example demonstrates the complete table lifecycle with Planar's developer-friendly API.
//!
//! **Features shown:**
//! - **Simplified catalog initialization**: `SqlCatalog::in_memory()` for easy setup
//! - **Builder pattern for schemas**: `SchemaSpec::new().with_column(...)`
//! - **Builder pattern for files**: `FileSpec::new(...).with_partition_values(...)`
//! - **Convenience methods**: No manual transaction ID tracking needed
//!   - `table_handle.append_file()` - automatically uses current transaction ID
//!   - `table_handle.append_files()` - same for multiple files
//!   - `table_handle.delete_files()` - convenience wrapper
//!   - `table_handle.set_properties()` - convenience wrapper
//! - **Time travel queries**: Read table state at any transaction ID
//! - **Transaction deltas**: Compute changes between two transaction IDs
//! - **Table listing**: List all tables or filter by namespace
//!
//! This example uses an in-memory SQLite database, making it easy to run without any external dependencies.

use arrow::datatypes::{DataType, TimeUnit};
use planar::catalog::{Catalog, ColumnSpec, FileSpec, SchemaSpec, SqlCatalog, TableIdent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Planar Table Lifecycle Example\n");

    // ============================================================================
    // Step 1: Initialize Catalog
    // ============================================================================
    println!("📦 Step 1: Setting up catalog...");

    // Create an in-memory SQLite catalog (perfect for examples and testing)
    let catalog = SqlCatalog::in_memory().await?;

    println!("✅ Catalog initialized\n");

    // ============================================================================
    // Step 2: Create a Table
    // ============================================================================
    println!("📋 Step 2: Creating a table...");

    let table_ident = TableIdent::new("sales", "transactions");

    // Using builder pattern for schema definition
    let schema = SchemaSpec::new()
        .with_column(ColumnSpec::new("id", DataType::Int64))
        .with_column(ColumnSpec::new("customer_id", DataType::Int64))
        .with_column(ColumnSpec::new("amount", DataType::Float64))
        .with_column(
            ColumnSpec::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            )
            .nullable(),
        );

    let properties = serde_json::json!({
        "description": "Sales transactions table",
        "owner": "analytics-team",
        "format": "parquet"
    });

    let table_handle = catalog
        .clone()
        .create_table(
            table_ident.clone(),
            "/data/sales/transactions".to_string(),
            schema,
            Some(properties),
        )
        .await?;

    println!(
        "✅ Table created: {}.{}",
        table_ident.namespace, table_ident.name
    );

    let initial_view = table_handle.read().await?;
    println!("   Transaction ID: {}", initial_view.transaction_id);
    println!();

    // ============================================================================
    // Step 3: Add Files
    // ============================================================================
    println!("📁 Step 3: Adding initial data files...");

    // Using builder pattern for file specification
    let file1 = FileSpec::new(
        "parquet",
        "/data/sales/transactions/part-00000.parquet",
        1000,
        245760,
    )
    .with_partition_values(serde_json::json!({"date": "2024-01-01"}));

    let file2 = FileSpec::new(
        "parquet",
        "/data/sales/transactions/part-00001.parquet",
        1500,
        368640,
    )
    .with_partition_values(serde_json::json!({"date": "2024-01-02"}));

    // Convenience method automatically uses current transaction ID
    let commit_result = table_handle.append_files(vec![file1, file2]).await?;

    println!("✅ Committed transaction {}", commit_result.transaction_id);

    let view_after_txn1 = table_handle.read().await?;
    println!("   Total files: {}", view_after_txn1.files.len());
    println!(
        "   Total records: {}",
        view_after_txn1
            .files
            .iter()
            .map(|f| f.record_count)
            .sum::<i64>()
    );
    println!();

    // ============================================================================
    // Step 4: Add Single File
    // ============================================================================
    println!("📁 Step 4: Adding a single file...");

    let file3 = FileSpec::new(
        "parquet",
        "/data/sales/transactions/part-00002.parquet",
        2000,
        491520,
    )
    .with_partition_values(serde_json::json!({"date": "2024-01-03"}));

    // Convenience method for single file
    let commit_result = table_handle.append_file(file3).await?;

    println!("✅ Committed transaction {}", commit_result.transaction_id);

    let view_after_txn2 = table_handle.read().await?;
    println!("   Total files: {}", view_after_txn2.files.len());
    println!(
        "   Total records: {}",
        view_after_txn2
            .files
            .iter()
            .map(|f| f.record_count)
            .sum::<i64>()
    );
    println!();

    // ============================================================================
    // Step 5: Time Travel Query
    // ============================================================================
    println!("⏰ Step 5: Time travel query...");

    // Read the table at a previous transaction ID
    let historical_view = table_handle.read_at(view_after_txn1.transaction_id).await?;

    println!(
        "✅ Read table at transaction {}",
        historical_view.transaction_id
    );
    println!("   Files at this point: {}", historical_view.files.len());
    println!();

    // ============================================================================
    // Step 6: Compute Delta
    // ============================================================================
    println!("🔍 Step 6: Computing delta between transactions...");

    // Compute what changed between two transaction IDs
    let delta = table_handle
        .diff(
            view_after_txn1.transaction_id,
            view_after_txn2.transaction_id,
        )
        .await?;

    println!("✅ Delta computed:");
    println!("   From transaction: {}", delta.from_transaction_id);
    println!("   To transaction: {}", delta.to_transaction_id);
    println!("   Files added: {}", delta.added_files.len());
    println!("   Files removed: {}", delta.removed_files.len());

    if !delta.added_files.is_empty() {
        println!("   Added files:");
        for file in &delta.added_files {
            println!("     - {} ({} records)", file.file_path, file.record_count);
        }
    }
    println!();

    // ============================================================================
    // Step 7: Delete File
    // ============================================================================
    println!("🗑️  Step 7: Deleting a file...");

    let file_to_delete = view_after_txn2.files[0].file_uuid;

    // Convenience method automatically uses current transaction ID
    let commit_result = table_handle.delete_files(vec![file_to_delete]).await?;

    println!("✅ Committed transaction {}", commit_result.transaction_id);

    let view_after_txn3 = table_handle.read().await?;
    println!("   Total files: {}", view_after_txn3.files.len());
    println!(
        "   Total records: {}",
        view_after_txn3
            .files
            .iter()
            .map(|f| f.record_count)
            .sum::<i64>()
    );
    println!();

    // ============================================================================
    // Step 8: Update Properties
    // ============================================================================
    println!("⚙️  Step 8: Updating table properties...");

    let updated_properties = serde_json::json!({
        "description": "Sales transactions table",
        "owner": "analytics-team",
        "format": "parquet",
        "last_updated": "2024-01-15",
        "retention_days": 365
    });

    // Convenience method automatically uses current transaction ID
    let commit_result = table_handle.set_properties(updated_properties).await?;

    println!("✅ Committed transaction {}", commit_result.transaction_id);

    let final_view = table_handle.read().await?;
    println!("   Updated properties: {}", final_view.properties);
    println!();

    // ============================================================================
    // Step 9: List Tables
    // ============================================================================
    println!("📊 Step 9: Listing all tables...");

    let all_tables = catalog.list_tables(None).await?;
    println!("✅ Found {} table(s):", all_tables.len());
    for table in &all_tables {
        println!("   - {}.{}", table.namespace, table.name);
    }

    let sales_tables = catalog.list_tables(Some("sales")).await?;
    println!("   Tables in 'sales' namespace: {}", sales_tables.len());
    println!();

    // ============================================================================
    // Summary
    // ============================================================================
    println!("📈 Summary:");
    println!("   Table: {}.{}", table_ident.namespace, table_ident.name);
    println!("   Current transaction: {}", final_view.transaction_id);
    println!("   Total files: {}", final_view.files.len());
    println!(
        "   Total records: {}",
        final_view.files.iter().map(|f| f.record_count).sum::<i64>()
    );
    println!(
        "   Total size: {} bytes",
        final_view
            .files
            .iter()
            .map(|f| f.file_size_bytes)
            .sum::<i64>()
    );

    if let Some(stats) = &final_view.stats {
        println!("   Table stats available:");
        println!("     Record count: {}", stats.record_count);
        println!("     File count: {}", stats.file_count);
    }

    println!("\n✅ Example completed successfully!");

    Ok(())
}
