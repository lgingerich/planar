//! Catalog module for managing table metadata and transactions

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use sqlx::{Database, Pool, Row};

/// Database-specific configuration
pub mod database;
/// Catalog error types
pub mod error;
/// Schema definitions
pub mod schema;

pub use error::{CatalogError, Result};

/// Transaction identifier
pub type TxnId = i64;

/// Table identifier consisting of namespace and name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdent {
    /// Table namespace
    pub namespace: String,
    /// Table name
    pub name: String,
}

impl TableIdent {
    /// Create a new table identifier
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

/// Snapshot of a table at a specific transaction
#[derive(Debug, Clone)]
pub struct TableView {
    /// Table identifier
    pub ident: TableIdent,
    /// Unique table UUID
    pub table_uuid: uuid::Uuid,
    /// Transaction ID this view represents
    pub transaction_id: TxnId,
    /// Table schema
    pub schema: schema::Schema,
    /// Files in this table version
    pub files: Vec<schema::File>,
    /// Table properties
    pub properties: serde_json::Value,
    /// Optional table statistics
    pub stats: Option<schema::TableStats>,
}

/// Difference between two table versions
#[derive(Debug, Clone)]
pub struct TableDelta {
    /// Starting transaction ID
    pub from_transaction_id: TxnId,
    /// Ending transaction ID
    pub to_transaction_id: TxnId,
    /// Files added in this range
    pub added_files: Vec<schema::File>,
    /// Files removed in this range
    pub removed_files: Vec<schema::File>,
    /// New schema if changed
    pub new_schema: Option<schema::Schema>,
    /// New properties if changed
    pub new_properties: Option<serde_json::Value>,
}

/// Result of committing a mutation
#[derive(Debug, Clone)]
pub struct CommitResult {
    /// Transaction ID of the commit
    pub transaction_id: TxnId,
    /// Optional updated table view
    pub table_view: Option<TableView>,
}

/// Column specification for schema creation
#[derive(Debug, Clone)]
pub struct ColumnSpec {
    /// Column name
    pub name: String,
    /// Column type string
    pub column_type: String,
    /// Whether the column allows null values
    pub is_nullable: bool,
}

impl ColumnSpec {
    /// Create a new column specification (non-nullable by default)
    pub fn new(name: impl Into<String>, column_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: column_type.into(),
            is_nullable: false,
        }
    }

    /// Mark the column as nullable.
    pub fn nullable(mut self) -> Self {
        self.is_nullable = true;
        self
    }
}

/// Schema specification for table creation
#[derive(Debug, Clone)]
pub struct SchemaSpec {
    /// Column specifications
    pub columns: Vec<ColumnSpec>,
}

impl SchemaSpec {
    /// Create an empty schema specification
    pub fn new() -> Self {
        Self { columns: Vec::new() }
    }

    /// Add a column to the schema
    pub fn with_column(mut self, column: ColumnSpec) -> Self {
        self.columns.push(column);
        self
    }

    /// Add multiple columns to the schema
    pub fn with_columns(mut self, columns: Vec<ColumnSpec>) -> Self {
        self.columns.extend(columns);
        self
    }
}

impl Default for SchemaSpec {
    fn default() -> Self {
        Self::new()
    }
}

/// File specification for mutations
#[derive(Debug, Clone)]
pub struct FileSpec {
    /// Optional file UUID (generated if not provided)
    pub file_uuid: Option<uuid::Uuid>,
    /// File format (e.g., "parquet", "lance", "vortex")
    pub file_format: String,
    /// File path
    pub file_path: String,
    /// Number of records in the file
    pub record_count: i64,
    /// File size in bytes
    pub file_size_bytes: i64,
    /// Optional partition values
    pub partition_values: Option<serde_json::Value>,
}

impl FileSpec {
    /// Create a new file specification
    pub fn new(
        file_format: impl Into<String>,
        file_path: impl Into<String>,
        record_count: i64,
        file_size_bytes: i64,
    ) -> Self {
        Self {
            file_uuid: None,
            file_format: file_format.into(),
            file_path: file_path.into(),
            record_count,
            file_size_bytes,
            partition_values: None,
        }
    }

    /// Set the file UUID
    pub fn with_uuid(mut self, uuid: uuid::Uuid) -> Self {
        self.file_uuid = Some(uuid);
        self
    }

    /// Set partition values
    pub fn with_partition_values(mut self, values: serde_json::Value) -> Self {
        self.partition_values = Some(values);
        self
    }
}

/// Mutation operation type
#[derive(Debug, Clone)]
pub enum MutationOp {
    /// Append files to the table
    AppendFiles(Vec<FileSpec>),
    /// Delete files by UUID
    DeleteFiles(Vec<uuid::Uuid>),
    /// Update the table schema
    UpdateSchema(SchemaSpec),
    /// Set table properties (replaces existing)
    SetProperties(serde_json::Value),
    /// Remove properties by key
    RemoveProperties(Vec<String>),
}

/// Collection of mutation operations to apply atomically
#[derive(Debug, Clone, Default)]
pub struct Mutation {
    /// Operations to apply
    pub operations: Vec<MutationOp>,
}

/// Builder for constructing table mutations
pub struct MutationBuilder {
    /// Catalog instance for committing mutations
    catalog: Arc<dyn Catalog>,
    /// Table identifier
    ident: TableIdent,
    /// Base transaction ID for optimistic concurrency control
    base_transaction_id: TxnId,
    /// Mutation operations to apply
    mutation: Mutation,
}

impl MutationBuilder {
    /// Create a new mutation builder
    fn new(catalog: Arc<dyn Catalog>, ident: TableIdent, base_transaction_id: TxnId) -> Self {
        Self {
            catalog,
            ident,
            base_transaction_id,
            mutation: Mutation::default(),
        }
    }

    /// Append multiple files to the table
    pub fn append_files(mut self, files: Vec<FileSpec>) -> Self {
        self.mutation.operations.push(MutationOp::AppendFiles(files));
        self
    }

    /// Append a single file to the table
    pub fn append_file(mut self, file: FileSpec) -> Self {
        self.mutation.operations.push(MutationOp::AppendFiles(vec![file]));
        self
    }

    /// Delete files by UUID
    pub fn delete_files(mut self, file_uuids: Vec<uuid::Uuid>) -> Self {
        self.mutation
            .operations
            .push(MutationOp::DeleteFiles(file_uuids));
        self
    }

    /// Update the table schema
    pub fn update_schema(mut self, schema: SchemaSpec) -> Self {
        self.mutation.operations.push(MutationOp::UpdateSchema(schema));
        self
    }

    /// Set table properties (replaces existing)
    pub fn set_properties(mut self, properties: serde_json::Value) -> Self {
        self.mutation
            .operations
            .push(MutationOp::SetProperties(properties));
        self
    }

    /// Remove properties by key
    pub fn remove_properties(mut self, keys: Vec<String>) -> Self {
        self.mutation
            .operations
            .push(MutationOp::RemoveProperties(keys));
        self
    }

    /// Commit the mutation to the catalog
    pub async fn commit(self) -> Result<CommitResult> {
        self.catalog
            .commit(&self.ident, self.base_transaction_id, self.mutation)
            .await
    }
}

/// Handle for interacting with a table
#[derive(Clone)]
pub struct TableHandle {
    /// Catalog instance for table operations
    catalog: Arc<dyn Catalog>,
    /// Table identifier
    ident: TableIdent,
}

impl TableHandle {
    /// Create a new table handle
    pub fn new(catalog: Arc<dyn Catalog>, ident: TableIdent) -> Self {
        Self { catalog, ident }
    }

    /// Get the table identifier
    pub fn ident(&self) -> &TableIdent {
        &self.ident
    }

    /// Read the current table view
    pub async fn read(&self) -> Result<TableView> {
        self.catalog.read_table(&self.ident, None).await
    }

    /// Read the table view at a specific transaction
    pub async fn read_at(&self, transaction_id: TxnId) -> Result<TableView> {
        self.catalog
            .read_table(&self.ident, Some(transaction_id))
            .await
    }

    /// Compute the difference between two transaction versions
    pub async fn diff(&self, from_transaction_id: TxnId, to_transaction_id: TxnId) -> Result<TableDelta> {
        self.catalog
            .diff_table(&self.ident, from_transaction_id, to_transaction_id)
            .await
    }

    /// Get a mutation builder. If `base_transaction_id` is `None`, uses the current transaction ID.
    pub async fn write(&self, base_transaction_id: Option<TxnId>) -> Result<MutationBuilder> {
        let txn_id = match base_transaction_id {
            Some(id) => id,
            None => {
                let view = self.read().await?;
                view.transaction_id
            }
        };
        Ok(MutationBuilder::new(self.catalog.clone(), self.ident.clone(), txn_id))
    }

    /// Append a single file using the current transaction ID
    pub async fn append_file(&self, file: FileSpec) -> Result<CommitResult> {
        self.write(None)
            .await?
            .append_file(file)
            .commit()
            .await
    }

    /// Append multiple files using the current transaction ID
    pub async fn append_files(&self, files: Vec<FileSpec>) -> Result<CommitResult> {
        self.write(None)
            .await?
            .append_files(files)
            .commit()
            .await
    }

    /// Delete files using the current transaction ID
    pub async fn delete_files(&self, file_uuids: Vec<uuid::Uuid>) -> Result<CommitResult> {
        self.write(None)
            .await?
            .delete_files(file_uuids)
            .commit()
            .await
    }

    /// Update schema using the current transaction ID
    pub async fn update_schema(&self, schema: SchemaSpec) -> Result<CommitResult> {
        self.write(None)
            .await?
            .update_schema(schema)
            .commit()
            .await
    }

    /// Set properties using the current transaction ID
    pub async fn set_properties(&self, properties: serde_json::Value) -> Result<CommitResult> {
        self.write(None)
            .await?
            .set_properties(properties)
            .commit()
            .await
    }
}

/// Catalog trait for managing table metadata
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Create a new table
    async fn create_table(
        self: Arc<Self>,
        ident: TableIdent,
        location: String,
        schema: SchemaSpec,
        properties: Option<serde_json::Value>,
    ) -> Result<TableHandle>;

    /// Load a table handle if it exists
    async fn load_table(self: Arc<Self>, ident: TableIdent) -> Result<Option<TableHandle>>;

    /// List tables, optionally filtered by namespace
    async fn list_tables(&self, namespace: Option<&str>) -> Result<Vec<TableIdent>>;

    /// Drop a table
    async fn drop_table(&self, ident: &TableIdent) -> Result<()>;

    /// Read a table view at a specific transaction (or current if None)
    async fn read_table(&self, ident: &TableIdent, at_transaction_id: Option<TxnId>)
        -> Result<TableView>;

    /// Compute the difference between two transaction versions
    async fn diff_table(
        &self,
        ident: &TableIdent,
        from_transaction_id: TxnId,
        to_transaction_id: TxnId,
    ) -> Result<TableDelta>;

    /// Commit a mutation to a table
    async fn commit(
        &self,
        ident: &TableIdent,
        base_transaction_id: TxnId,
        mutation: Mutation,
    ) -> Result<CommitResult>;
}

/// SQL-backed catalog implementation for managing table metadata
pub struct SqlCatalog<DB>
where
    DB: Database,
{
    /// Database connection pool
    pool: Pool<DB>,
}

impl<DB> SqlCatalog<DB>
where
    DB: Database,
    <DB as Database>::Connection: sqlx::migrate::Migrate,
{
    /// Create a new SQL catalog with a connection pool
    pub fn new(pool: Pool<DB>) -> Self {
        Self { pool }
    }

    /// Initialize the database schema by running migrations
    pub async fn initialize_schema(&self) -> std::result::Result<(), sqlx::Error> {
        let migrator = sqlx::migrate!("db/migrations");
        migrator.run(&self.pool).await?;
        Ok(())
    }

}

impl SqlCatalog<sqlx::Sqlite> {
    /// Configure SQLite-specific database settings
    pub async fn configure_database(&self) -> std::result::Result<(), sqlx::Error> {
        database::sqlite::configure_pool(&self.pool).await
    }

    /// Create and initialize a SQLite catalog from a connection string.
    /// This is a convenience method that handles pool creation, configuration, and schema initialization.
    pub async fn from_connection_string(
        connection_string: &str,
    ) -> std::result::Result<Arc<Self>, sqlx::Error> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(connection_string)
            .await?;
        let catalog = Arc::new(Self::new(pool));
        catalog.configure_database().await?;
        catalog.initialize_schema().await?;
        Ok(catalog)
    }

    /// Create an in-memory SQLite catalog
    /// This is a convenience method for testing and examples
    pub async fn in_memory() -> std::result::Result<Arc<Self>, sqlx::Error> {
        Self::from_connection_string("sqlite::memory:").await
    }
}

impl SqlCatalog<sqlx::Postgres> {
    /// Configure PostgreSQL-specific database settings
    pub async fn configure_database(&self) -> std::result::Result<(), sqlx::Error> {
        database::postgres::configure_pool(&self.pool).await
    }

    /// Create and initialize a PostgreSQL catalog from a connection string
    /// This is a convenience method that handles pool creation, configuration, and schema initialization
    pub async fn from_connection_string(
        connection_string: &str,
    ) -> std::result::Result<Arc<Self>, sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(connection_string)
            .await?;
        let catalog = Arc::new(Self::new(pool));
        catalog.configure_database().await?;
        catalog.initialize_schema().await?;
        Ok(catalog)
    }
}

#[async_trait]
impl Catalog for SqlCatalog<sqlx::Sqlite> {
    async fn create_table(
        self: Arc<Self>,
        ident: TableIdent,
        location: String,
        schema: SchemaSpec,
        properties: Option<serde_json::Value>,
    ) -> Result<TableHandle> {
        if schema.columns.is_empty() {
            return Err(CatalogError::InvalidArgument(
                "schema must include at least one column".to_string(),
            ));
        }

        let mut tx = self.pool.begin().await?;

        let exists = sqlx::query_scalar::<sqlx::Sqlite, i64>(
            "SELECT 1 FROM tables WHERE namespace = ?1 AND table_name = ?2 LIMIT 1",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(tx.as_mut())
        .await?
        .is_some();

        if exists {
            return Err(CatalogError::Conflict(format!(
                "table already exists: {}.{}",
                ident.namespace, ident.name
            )));
        }

        let table_uuid = uuid::Uuid::new_v4();
        let created_at = chrono::Utc::now();
        let properties_value = properties.unwrap_or_else(|| serde_json::json!({}));
        let properties_text = serialize_json(&properties_value)?;

        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO tables (table_uuid, table_name, namespace, location, created_at, properties)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(ident.name.as_str())
        .bind(ident.namespace.as_str())
        .bind(location.as_str())
        .bind(created_at)
        .bind(properties_text.as_str())
        .execute(tx.as_mut())
        .await?;

        let transaction_id = next_transaction_id(&mut tx).await?;
        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp)
             VALUES (?1, ?2, ?3)",
        )
        .bind(transaction_id)
        .bind(table_uuid.as_bytes().as_slice())
        .bind(created_at)
        .execute(tx.as_mut())
        .await?;

        let schema_uuid = uuid::Uuid::new_v4();
        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO schemas (schema_uuid, table_uuid, schema_version, valid_from_transaction_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .bind(1_i32)
        .bind(transaction_id)
        .bind(created_at)
        .execute(tx.as_mut())
        .await?;

        for (index, column) in schema.columns.iter().enumerate() {
            let column_uuid = uuid::Uuid::new_v4();
            let ordinal_position = (index + 1) as i32;

            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(column_uuid.as_bytes().as_slice())
            .bind(schema_uuid.as_bytes().as_slice())
            .bind(column.name.as_str())
            .bind(column.column_type.as_str())
            .bind(ordinal_position)
            .bind(column.is_nullable)
            .execute(tx.as_mut())
            .await?;
        }

        sqlx::query::<sqlx::Sqlite>(
            "UPDATE tables
             SET current_schema_uuid = ?1, current_transaction_id = ?2
             WHERE table_uuid = ?3",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .bind(transaction_id)
        .bind(table_uuid.as_bytes().as_slice())
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;

        let catalog: Arc<dyn Catalog> = self.clone();
        Ok(TableHandle::new(catalog, ident))
    }

    async fn load_table(self: Arc<Self>, ident: TableIdent) -> Result<Option<TableHandle>> {
        let exists = sqlx::query_scalar::<sqlx::Sqlite, i64>(
            "SELECT 1 FROM tables WHERE namespace = ?1 AND table_name = ?2 LIMIT 1",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(&self.pool)
        .await?
        .is_some();

        if !exists {
            return Ok(None);
        }

        let catalog: Arc<dyn Catalog> = self.clone();
        Ok(Some(TableHandle::new(catalog, ident)))
    }

    async fn list_tables(&self, namespace: Option<&str>) -> Result<Vec<TableIdent>> {
        let rows = if let Some(namespace) = namespace {
            sqlx::query::<sqlx::Sqlite>("SELECT namespace, table_name FROM tables WHERE namespace = ?1")
                .bind(namespace)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query::<sqlx::Sqlite>("SELECT namespace, table_name FROM tables")
                .fetch_all(&self.pool)
                .await?
        };

        let mut tables = Vec::with_capacity(rows.len());
        for row in rows {
            let namespace: String = row.try_get("namespace")?;
            let name: String = row.try_get("table_name")?;
            tables.push(TableIdent { namespace, name });
        }

        Ok(tables)
    }

    async fn drop_table(&self, ident: &TableIdent) -> Result<()> {
        let affected = sqlx::query::<sqlx::Sqlite>("DELETE FROM tables WHERE namespace = ?1 AND table_name = ?2")
            .bind(ident.namespace.as_str())
            .bind(ident.name.as_str())
            .execute(&self.pool)
            .await?
            .rows_affected();

        if affected == 0 {
            return Err(CatalogError::NotFound(format!(
                "{}.{}",
                ident.namespace, ident.name
            )));
        }

        Ok(())
    }

    async fn read_table(
        &self,
        ident: &TableIdent,
        at_transaction_id: Option<TxnId>,
    ) -> Result<TableView> {
        let table_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT table_uuid, current_schema_uuid, current_transaction_id, properties
             FROM tables WHERE namespace = ?1 AND table_name = ?2",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(&self.pool)
        .await?;

        let table_row = match table_row {
            Some(row) => row,
            None => {
                return Err(CatalogError::NotFound(format!(
                    "{}.{}",
                    ident.namespace, ident.name
                )))
            }
        };

        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let _current_schema_uuid = uuid_from_row_optional(&table_row, "current_schema_uuid")?;
        let current_transaction_id: Option<TxnId> =
            table_row.try_get("current_transaction_id")?;
        let properties = parse_json(table_row.try_get("properties")?)?;

        let current_transaction_id = current_transaction_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })?;
        let effective_transaction_id = at_transaction_id.unwrap_or(current_transaction_id);
        if effective_transaction_id > current_transaction_id {
            return Err(CatalogError::InvalidArgument(
                "requested transaction is newer than current".to_string(),
            ));
        }

        let schema_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT schema_uuid, table_uuid, schema_version, valid_from_transaction_id, valid_to_transaction_id, created_at
             FROM schemas
             WHERE table_uuid = ?1
               AND valid_from_transaction_id <= ?2
               AND (valid_to_transaction_id IS NULL OR valid_to_transaction_id > ?2)
             LIMIT 1",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id)
        .fetch_optional(&self.pool)
        .await?;

        let schema_row = schema_row.ok_or_else(|| {
            CatalogError::NotFound(format!(
                "no schema for {}.{} at transaction {}",
                ident.namespace, ident.name, effective_transaction_id
            ))
        })?;

        let schema_uuid = uuid_from_row(&schema_row, "schema_uuid")?;

        let column_rows = sqlx::query::<sqlx::Sqlite>(
            "SELECT column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable
             FROM columns WHERE schema_uuid = ?1 ORDER BY ordinal_position",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await?;

        let mut columns = Vec::with_capacity(column_rows.len());
        for row in column_rows {
            let column = schema::Column {
                column_uuid: uuid_from_row(&row, "column_uuid")?,
                schema_uuid: uuid_from_row(&row, "schema_uuid")?,
                column_name: row.try_get("column_name")?,
                column_type: row.try_get("column_type")?,
                ordinal_position: row.try_get("ordinal_position")?,
                is_nullable: row.try_get("is_nullable")?,
            };
            columns.push(column);
        }

        let schema = schema::Schema {
            schema_uuid,
            table_uuid: uuid_from_row(&schema_row, "table_uuid")?,
            schema_version: schema_row.try_get("schema_version")?,
            valid_from_transaction_id: schema_row.try_get("valid_from_transaction_id")?,
            valid_to_transaction_id: schema_row.try_get("valid_to_transaction_id")?,
            created_at: schema_row.try_get("created_at")?,
            columns,
        };

        let file_rows = sqlx::query::<sqlx::Sqlite>(
            "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                    added_in_transaction_id, removed_in_transaction_id, partition_values
             FROM files
             WHERE table_uuid = ?1
               AND added_in_transaction_id <= ?2
               AND (removed_in_transaction_id IS NULL OR removed_in_transaction_id > ?2)",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id)
        .fetch_all(&self.pool)
        .await?;

        let mut files = Vec::with_capacity(file_rows.len());
        for row in file_rows {
            let partition_values: Option<String> = row.try_get("partition_values")?;
            let file = schema::File {
                file_uuid: uuid_from_row(&row, "file_uuid")?,
                table_uuid: uuid_from_row(&row, "table_uuid")?,
                file_format: row.try_get("file_format")?,
                file_path: row.try_get("file_path")?,
                record_count: row.try_get("record_count")?,
                file_size_bytes: row.try_get("file_size_bytes")?,
                added_in_transaction_id: row.try_get("added_in_transaction_id")?,
                removed_in_transaction_id: row.try_get("removed_in_transaction_id")?,
                partition_values: parse_json_optional(partition_values)?,
            };
            files.push(file);
        }

        let stats_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT table_uuid, transaction_id, record_count, file_size_bytes, file_count, last_updated
             FROM table_stats WHERE table_uuid = ?1 AND transaction_id = ?2",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id)
        .fetch_optional(&self.pool)
        .await?;

        let stats = if let Some(row) = stats_row {
            Some(schema::TableStats {
                table_uuid: uuid_from_row(&row, "table_uuid")?,
                transaction_id: row.try_get("transaction_id")?,
                record_count: row.try_get("record_count")?,
                file_size_bytes: row.try_get("file_size_bytes")?,
                file_count: row.try_get("file_count")?,
                last_updated: row.try_get("last_updated")?,
            })
        } else {
            None
        };

        Ok(TableView {
            ident: ident.clone(),
            table_uuid,
            transaction_id: effective_transaction_id,
            schema,
            files,
            properties,
            stats,
        })
    }

    async fn diff_table(
        &self,
        ident: &TableIdent,
        from_transaction_id: TxnId,
        to_transaction_id: TxnId,
    ) -> Result<TableDelta> {
        let from_view = self.read_table(ident, Some(from_transaction_id)).await?;
        let to_view = self.read_table(ident, Some(to_transaction_id)).await?;

        let from_ids: HashSet<uuid::Uuid> =
            from_view.files.iter().map(|file| file.file_uuid).collect();
        let to_ids: HashSet<uuid::Uuid> =
            to_view.files.iter().map(|file| file.file_uuid).collect();

        let added_files = to_view
            .files
            .iter()
            .filter(|file| !from_ids.contains(&file.file_uuid))
            .cloned()
            .collect();

        let removed_files = from_view
            .files
            .iter()
            .filter(|file| !to_ids.contains(&file.file_uuid))
            .cloned()
            .collect();

        let new_schema = if from_view.schema.schema_uuid != to_view.schema.schema_uuid {
            Some(to_view.schema.clone())
        } else {
            None
        };

        let new_properties = if from_view.properties != to_view.properties {
            Some(to_view.properties)
        } else {
            None
        };

        Ok(TableDelta {
            from_transaction_id,
            to_transaction_id,
            added_files,
            removed_files,
            new_schema,
            new_properties,
        })
    }

    async fn commit(
        &self,
        ident: &TableIdent,
        base_transaction_id: TxnId,
        mutation: Mutation,
    ) -> Result<CommitResult> {
        if mutation.operations.is_empty() {
            return Err(CatalogError::InvalidArgument(
                "mutation has no operations".to_string(),
            ));
        }

        let mut tx = self.pool.begin().await?;

        let table_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT table_uuid, current_schema_uuid, current_transaction_id, properties
             FROM tables WHERE namespace = ?1 AND table_name = ?2",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(tx.as_mut())
        .await?;

        let table_row = match table_row {
            Some(row) => row,
            None => {
                return Err(CatalogError::NotFound(format!(
                    "{}.{}",
                    ident.namespace, ident.name
                )))
            }
        };

        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let current_schema_uuid = uuid_from_row_optional(&table_row, "current_schema_uuid")?;
        let current_transaction_id: Option<TxnId> =
            table_row.try_get("current_transaction_id")?;
        let mut properties_value = parse_json(table_row.try_get("properties")?)?;

        let current_transaction_id = current_transaction_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })?;

        if current_transaction_id != base_transaction_id {
            return Err(CatalogError::Conflict(format!(
                "base transaction {} does not match current {}",
                base_transaction_id, current_transaction_id
            )));
        }

        let transaction_id = next_transaction_id(&mut tx).await?;
        let transaction_timestamp = chrono::Utc::now();

        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp, parent_transaction_id)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(transaction_id)
        .bind(table_uuid.as_bytes().as_slice())
        .bind(transaction_timestamp)
        .bind(current_transaction_id)
        .execute(tx.as_mut())
        .await?;

        let mut new_schema_uuid = current_schema_uuid;

        for op in mutation.operations {
            match op {
                MutationOp::AppendFiles(files) => {
                    for file in files {
                        let file_uuid = file.file_uuid.unwrap_or_else(uuid::Uuid::new_v4);
                        let partition_text = serialize_json_optional(file.partition_values.as_ref())?;

                        sqlx::query::<sqlx::Sqlite>(
                            "INSERT INTO files (file_uuid, table_uuid, file_format, file_path, record_count,
                                                 file_size_bytes, added_in_transaction_id, partition_values)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        )
                        .bind(file_uuid.as_bytes().as_slice())
                        .bind(table_uuid.as_bytes().as_slice())
                        .bind(file.file_format.as_str())
                        .bind(file.file_path.as_str())
                        .bind(file.record_count)
                        .bind(file.file_size_bytes)
                        .bind(transaction_id)
                        .bind(partition_text.as_deref())
                        .execute(tx.as_mut())
                        .await?;
                    }
                }
                MutationOp::DeleteFiles(file_uuids) => {
                    for file_uuid in file_uuids {
                        sqlx::query::<sqlx::Sqlite>(
                            "UPDATE files SET removed_in_transaction_id = ?1
                             WHERE file_uuid = ?2 AND table_uuid = ?3 AND removed_in_transaction_id IS NULL",
                        )
                        .bind(transaction_id)
                        .bind(file_uuid.as_bytes().as_slice())
                        .bind(table_uuid.as_bytes().as_slice())
                        .execute(tx.as_mut())
                        .await?;
                    }
                }
                MutationOp::UpdateSchema(schema_spec) => {
                    let current_schema_uuid = current_schema_uuid.ok_or_else(|| {
                        CatalogError::InvalidArgument(
                            "cannot update schema without a current schema".to_string(),
                        )
                    })?;

                    let current_schema_version: i32 = sqlx::query_scalar::<sqlx::Sqlite, i32>(
                        "SELECT schema_version FROM schemas WHERE schema_uuid = ?1",
                    )
                    .bind(current_schema_uuid.as_bytes().as_slice())
                    .fetch_one(tx.as_mut())
                    .await?;

                    let schema_uuid = uuid::Uuid::new_v4();
                    let schema_version = current_schema_version + 1;

                    sqlx::query::<sqlx::Sqlite>(
                        "INSERT INTO schemas (schema_uuid, table_uuid, schema_version, valid_from_transaction_id, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                    )
                    .bind(schema_uuid.as_bytes().as_slice())
                    .bind(table_uuid.as_bytes().as_slice())
                    .bind(schema_version)
                    .bind(transaction_id)
                    .bind(transaction_timestamp)
                    .execute(tx.as_mut())
                    .await?;

                    sqlx::query::<sqlx::Sqlite>(
                        "UPDATE schemas SET valid_to_transaction_id = ?1 WHERE schema_uuid = ?2",
                    )
                    .bind(transaction_id)
                    .bind(current_schema_uuid.as_bytes().as_slice())
                    .execute(tx.as_mut())
                    .await?;

                    for (index, column) in schema_spec.columns.iter().enumerate() {
                        let column_uuid = uuid::Uuid::new_v4();
                        let ordinal_position = (index + 1) as i32;

                        sqlx::query::<sqlx::Sqlite>(
                            "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        )
                        .bind(column_uuid.as_bytes().as_slice())
                        .bind(schema_uuid.as_bytes().as_slice())
                        .bind(column.name.as_str())
                        .bind(column.column_type.as_str())
                        .bind(ordinal_position)
                        .bind(column.is_nullable)
                        .execute(tx.as_mut())
                        .await?;
                    }

                    new_schema_uuid = Some(schema_uuid);
                }
                MutationOp::SetProperties(properties) => {
                    properties_value = properties;
                }
                MutationOp::RemoveProperties(keys) => {
                    if keys.is_empty() {
                        continue;
                    }
                    let map = properties_value
                        .as_object_mut()
                        .ok_or_else(|| CatalogError::InvalidArgument("properties must be an object".to_string()))?;
                    for key in keys {
                        map.remove(&key);
                    }
                }
            }
        }

        let schema_uuid_to_set = new_schema_uuid.or(current_schema_uuid);
        let properties_text = serialize_json(&properties_value)?;

        sqlx::query::<sqlx::Sqlite>(
            "UPDATE tables
             SET current_transaction_id = ?1, current_schema_uuid = ?2, properties = ?3
             WHERE table_uuid = ?4",
        )
        .bind(transaction_id)
        .bind(schema_uuid_to_set.map(|uuid| uuid.as_bytes().to_vec()))
        .bind(properties_text.as_str())
        .bind(table_uuid.as_bytes().as_slice())
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;

        Ok(CommitResult {
            transaction_id,
            table_view: None,
        })
    }
}

#[async_trait]
impl Catalog for SqlCatalog<sqlx::Postgres> {
    async fn create_table(
        self: Arc<Self>,
        ident: TableIdent,
        location: String,
        schema: SchemaSpec,
        properties: Option<serde_json::Value>,
    ) -> Result<TableHandle> {
        if schema.columns.is_empty() {
            return Err(CatalogError::InvalidArgument(
                "schema must include at least one column".to_string(),
            ));
        }

        let mut tx = self.pool.begin().await?;

        let exists = sqlx::query_scalar::<sqlx::Postgres, i64>(
            "SELECT 1 FROM tables WHERE namespace = $1 AND table_name = $2 LIMIT 1",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(tx.as_mut())
        .await?
        .is_some();

        if exists {
            return Err(CatalogError::Conflict(format!(
                "table already exists: {}.{}",
                ident.namespace, ident.name
            )));
        }

        let table_uuid = uuid::Uuid::new_v4();
        let created_at = chrono::Utc::now();
        let properties_value = properties.unwrap_or_else(|| serde_json::json!({}));
        let properties_text = serialize_json(&properties_value)?;

        sqlx::query::<sqlx::Postgres>(
            "INSERT INTO tables (table_uuid, table_name, namespace, location, created_at, properties)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(ident.name.as_str())
        .bind(ident.namespace.as_str())
        .bind(location.as_str())
        .bind(created_at)
        .bind(properties_text.as_str())
        .execute(tx.as_mut())
        .await?;

        let transaction_id = next_transaction_id(&mut tx).await?;
        sqlx::query::<sqlx::Postgres>(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp)
             VALUES ($1, $2, $3)",
        )
        .bind(transaction_id)
        .bind(table_uuid.as_bytes().as_slice())
        .bind(created_at)
        .execute(tx.as_mut())
        .await?;

        let schema_uuid = uuid::Uuid::new_v4();
        sqlx::query::<sqlx::Postgres>(
            "INSERT INTO schemas (schema_uuid, table_uuid, schema_version, valid_from_transaction_id, created_at)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .bind(1_i32)
        .bind(transaction_id)
        .bind(created_at)
        .execute(tx.as_mut())
        .await?;

        for (index, column) in schema.columns.iter().enumerate() {
            let column_uuid = uuid::Uuid::new_v4();
            let ordinal_position = (index + 1) as i32;

            sqlx::query::<sqlx::Postgres>(
                "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(column_uuid.as_bytes().as_slice())
            .bind(schema_uuid.as_bytes().as_slice())
            .bind(column.name.as_str())
            .bind(column.column_type.as_str())
            .bind(ordinal_position)
            .bind(column.is_nullable)
            .execute(tx.as_mut())
            .await?;
        }

        sqlx::query::<sqlx::Postgres>(
            "UPDATE tables
             SET current_schema_uuid = $1, current_transaction_id = $2
             WHERE table_uuid = $3",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .bind(transaction_id)
        .bind(table_uuid.as_bytes().as_slice())
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;

        let catalog: Arc<dyn Catalog> = self.clone();
        Ok(TableHandle::new(catalog, ident))
    }

    async fn load_table(self: Arc<Self>, ident: TableIdent) -> Result<Option<TableHandle>> {
        let exists = sqlx::query_scalar::<sqlx::Postgres, i64>(
            "SELECT 1 FROM tables WHERE namespace = $1 AND table_name = $2 LIMIT 1",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(&self.pool)
        .await?
        .is_some();

        if !exists {
            return Ok(None);
        }

        let catalog: Arc<dyn Catalog> = self.clone();
        Ok(Some(TableHandle::new(catalog, ident)))
    }

    async fn list_tables(&self, namespace: Option<&str>) -> Result<Vec<TableIdent>> {
        let rows = if let Some(namespace) = namespace {
            sqlx::query::<sqlx::Postgres>("SELECT namespace, table_name FROM tables WHERE namespace = $1")
                .bind(namespace)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query::<sqlx::Postgres>("SELECT namespace, table_name FROM tables")
                .fetch_all(&self.pool)
                .await?
        };

        let mut tables = Vec::with_capacity(rows.len());
        for row in rows {
            let namespace: String = row.try_get("namespace")?;
            let name: String = row.try_get("table_name")?;
            tables.push(TableIdent { namespace, name });
        }

        Ok(tables)
    }

    async fn drop_table(&self, ident: &TableIdent) -> Result<()> {
        let affected = sqlx::query::<sqlx::Postgres>("DELETE FROM tables WHERE namespace = $1 AND table_name = $2")
            .bind(ident.namespace.as_str())
            .bind(ident.name.as_str())
            .execute(&self.pool)
            .await?
            .rows_affected();

        if affected == 0 {
            return Err(CatalogError::NotFound(format!(
                "{}.{}",
                ident.namespace, ident.name
            )));
        }

        Ok(())
    }

    async fn read_table(
        &self,
        ident: &TableIdent,
        at_transaction_id: Option<TxnId>,
    ) -> Result<TableView> {
        let table_row = sqlx::query::<sqlx::Postgres>(
            "SELECT table_uuid, current_schema_uuid, current_transaction_id, properties
             FROM tables WHERE namespace = $1 AND table_name = $2",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(&self.pool)
        .await?;

        let table_row = match table_row {
            Some(row) => row,
            None => {
                return Err(CatalogError::NotFound(format!(
                    "{}.{}",
                    ident.namespace, ident.name
                )))
            }
        };

        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let _current_schema_uuid = uuid_from_row_optional(&table_row, "current_schema_uuid")?;
        let current_transaction_id: Option<TxnId> =
            table_row.try_get("current_transaction_id")?;
        let properties = parse_json(table_row.try_get("properties")?)?;

        let current_transaction_id = current_transaction_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })?;
        let effective_transaction_id = at_transaction_id.unwrap_or(current_transaction_id);
        if effective_transaction_id > current_transaction_id {
            return Err(CatalogError::InvalidArgument(
                "requested transaction is newer than current".to_string(),
            ));
        }

        let schema_row = sqlx::query::<sqlx::Postgres>(
            "SELECT schema_uuid, table_uuid, schema_version, valid_from_transaction_id, valid_to_transaction_id, created_at
             FROM schemas
             WHERE table_uuid = $1
               AND valid_from_transaction_id <= $2
               AND (valid_to_transaction_id IS NULL OR valid_to_transaction_id > $2)
             LIMIT 1",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id)
        .fetch_optional(&self.pool)
        .await?;

        let schema_row = schema_row.ok_or_else(|| {
            CatalogError::NotFound(format!(
                "no schema for {}.{} at transaction {}",
                ident.namespace, ident.name, effective_transaction_id
            ))
        })?;

        let schema_uuid = uuid_from_row(&schema_row, "schema_uuid")?;

        let column_rows = sqlx::query::<sqlx::Postgres>(
            "SELECT column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable
             FROM columns WHERE schema_uuid = $1 ORDER BY ordinal_position",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await?;

        let mut columns = Vec::with_capacity(column_rows.len());
        for row in column_rows {
            let column = schema::Column {
                column_uuid: uuid_from_row(&row, "column_uuid")?,
                schema_uuid: uuid_from_row(&row, "schema_uuid")?,
                column_name: row.try_get("column_name")?,
                column_type: row.try_get("column_type")?,
                ordinal_position: row.try_get("ordinal_position")?,
                is_nullable: row.try_get("is_nullable")?,
            };
            columns.push(column);
        }

        let schema = schema::Schema {
            schema_uuid,
            table_uuid: uuid_from_row(&schema_row, "table_uuid")?,
            schema_version: schema_row.try_get("schema_version")?,
            valid_from_transaction_id: schema_row.try_get("valid_from_transaction_id")?,
            valid_to_transaction_id: schema_row.try_get("valid_to_transaction_id")?,
            created_at: schema_row.try_get("created_at")?,
            columns,
        };

        let file_rows = sqlx::query::<sqlx::Postgres>(
            "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                    added_in_transaction_id, removed_in_transaction_id, partition_values
             FROM files
             WHERE table_uuid = $1
               AND added_in_transaction_id <= $2
               AND (removed_in_transaction_id IS NULL OR removed_in_transaction_id > $2)",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id)
        .fetch_all(&self.pool)
        .await?;

        let mut files = Vec::with_capacity(file_rows.len());
        for row in file_rows {
            let partition_values: Option<String> = row.try_get("partition_values")?;
            let file = schema::File {
                file_uuid: uuid_from_row(&row, "file_uuid")?,
                table_uuid: uuid_from_row(&row, "table_uuid")?,
                file_format: row.try_get("file_format")?,
                file_path: row.try_get("file_path")?,
                record_count: row.try_get("record_count")?,
                file_size_bytes: row.try_get("file_size_bytes")?,
                added_in_transaction_id: row.try_get("added_in_transaction_id")?,
                removed_in_transaction_id: row.try_get("removed_in_transaction_id")?,
                partition_values: parse_json_optional(partition_values)?,
            };
            files.push(file);
        }

        let stats_row = sqlx::query::<sqlx::Postgres>(
            "SELECT table_uuid, transaction_id, record_count, file_size_bytes, file_count, last_updated
             FROM table_stats WHERE table_uuid = $1 AND transaction_id = $2",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id)
        .fetch_optional(&self.pool)
        .await?;

        let stats = if let Some(row) = stats_row {
            Some(schema::TableStats {
                table_uuid: uuid_from_row(&row, "table_uuid")?,
                transaction_id: row.try_get("transaction_id")?,
                record_count: row.try_get("record_count")?,
                file_size_bytes: row.try_get("file_size_bytes")?,
                file_count: row.try_get("file_count")?,
                last_updated: row.try_get("last_updated")?,
            })
        } else {
            None
        };

        Ok(TableView {
            ident: ident.clone(),
            table_uuid,
            transaction_id: effective_transaction_id,
            schema,
            files,
            properties,
            stats,
        })
    }

    async fn diff_table(
        &self,
        ident: &TableIdent,
        from_transaction_id: TxnId,
        to_transaction_id: TxnId,
    ) -> Result<TableDelta> {
        let from_view = self.read_table(ident, Some(from_transaction_id)).await?;
        let to_view = self.read_table(ident, Some(to_transaction_id)).await?;

        let from_ids: HashSet<uuid::Uuid> =
            from_view.files.iter().map(|file| file.file_uuid).collect();
        let to_ids: HashSet<uuid::Uuid> =
            to_view.files.iter().map(|file| file.file_uuid).collect();

        let added_files = to_view
            .files
            .iter()
            .filter(|file| !from_ids.contains(&file.file_uuid))
            .cloned()
            .collect();

        let removed_files = from_view
            .files
            .iter()
            .filter(|file| !to_ids.contains(&file.file_uuid))
            .cloned()
            .collect();

        let new_schema = if from_view.schema.schema_uuid != to_view.schema.schema_uuid {
            Some(to_view.schema.clone())
        } else {
            None
        };

        let new_properties = if from_view.properties != to_view.properties {
            Some(to_view.properties)
        } else {
            None
        };

        Ok(TableDelta {
            from_transaction_id,
            to_transaction_id,
            added_files,
            removed_files,
            new_schema,
            new_properties,
        })
    }

    async fn commit(
        &self,
        ident: &TableIdent,
        base_transaction_id: TxnId,
        mutation: Mutation,
    ) -> Result<CommitResult> {
        if mutation.operations.is_empty() {
            return Err(CatalogError::InvalidArgument(
                "mutation has no operations".to_string(),
            ));
        }

        let mut tx = self.pool.begin().await?;

        let table_row = sqlx::query::<sqlx::Postgres>(
            "SELECT table_uuid, current_schema_uuid, current_transaction_id, properties
             FROM tables WHERE namespace = $1 AND table_name = $2",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(tx.as_mut())
        .await?;

        let table_row = match table_row {
            Some(row) => row,
            None => {
                return Err(CatalogError::NotFound(format!(
                    "{}.{}",
                    ident.namespace, ident.name
                )))
            }
        };

        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let current_schema_uuid = uuid_from_row_optional(&table_row, "current_schema_uuid")?;
        let current_transaction_id: Option<TxnId> =
            table_row.try_get("current_transaction_id")?;
        let mut properties_value = parse_json(table_row.try_get("properties")?)?;

        let current_transaction_id = current_transaction_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })?;

        if current_transaction_id != base_transaction_id {
            return Err(CatalogError::Conflict(format!(
                "base transaction {} does not match current {}",
                base_transaction_id, current_transaction_id
            )));
        }

        let transaction_id = next_transaction_id(&mut tx).await?;
        let transaction_timestamp = chrono::Utc::now();

        sqlx::query::<sqlx::Postgres>(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp, parent_transaction_id)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(transaction_id)
        .bind(table_uuid.as_bytes().as_slice())
        .bind(transaction_timestamp)
        .bind(current_transaction_id)
        .execute(tx.as_mut())
        .await?;

        let mut new_schema_uuid = current_schema_uuid;

        for op in mutation.operations {
            match op {
                MutationOp::AppendFiles(files) => {
                    for file in files {
                        let file_uuid = file.file_uuid.unwrap_or_else(uuid::Uuid::new_v4);
                        let partition_text = serialize_json_optional(file.partition_values.as_ref())?;

                        sqlx::query::<sqlx::Postgres>(
                            "INSERT INTO files (file_uuid, table_uuid, file_format, file_path, record_count,
                                                 file_size_bytes, added_in_transaction_id, partition_values)
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                        )
                        .bind(file_uuid.as_bytes().as_slice())
                        .bind(table_uuid.as_bytes().as_slice())
                        .bind(file.file_format.as_str())
                        .bind(file.file_path.as_str())
                        .bind(file.record_count)
                        .bind(file.file_size_bytes)
                        .bind(transaction_id)
                        .bind(partition_text.as_deref())
                        .execute(tx.as_mut())
                        .await?;
                    }
                }
                MutationOp::DeleteFiles(file_uuids) => {
                    for file_uuid in file_uuids {
                        sqlx::query::<sqlx::Postgres>(
                            "UPDATE files SET removed_in_transaction_id = $1
                             WHERE file_uuid = $2 AND table_uuid = $3 AND removed_in_transaction_id IS NULL",
                        )
                        .bind(transaction_id)
                        .bind(file_uuid.as_bytes().as_slice())
                        .bind(table_uuid.as_bytes().as_slice())
                        .execute(tx.as_mut())
                        .await?;
                    }
                }
                MutationOp::UpdateSchema(schema_spec) => {
                    let current_schema_uuid = current_schema_uuid.ok_or_else(|| {
                        CatalogError::InvalidArgument(
                            "cannot update schema without a current schema".to_string(),
                        )
                    })?;

                    let current_schema_version: i32 = sqlx::query_scalar::<sqlx::Postgres, i32>(
                        "SELECT schema_version FROM schemas WHERE schema_uuid = $1",
                    )
                    .bind(current_schema_uuid.as_bytes().as_slice())
                    .fetch_one(tx.as_mut())
                    .await?;

                    let schema_uuid = uuid::Uuid::new_v4();
                    let schema_version = current_schema_version + 1;

                    sqlx::query::<sqlx::Postgres>(
                        "INSERT INTO schemas (schema_uuid, table_uuid, schema_version, valid_from_transaction_id, created_at)
                         VALUES ($1, $2, $3, $4, $5)",
                    )
                    .bind(schema_uuid.as_bytes().as_slice())
                    .bind(table_uuid.as_bytes().as_slice())
                    .bind(schema_version)
                    .bind(transaction_id)
                    .bind(transaction_timestamp)
                    .execute(tx.as_mut())
                    .await?;

                    sqlx::query::<sqlx::Postgres>(
                        "UPDATE schemas SET valid_to_transaction_id = $1 WHERE schema_uuid = $2",
                    )
                    .bind(transaction_id)
                    .bind(current_schema_uuid.as_bytes().as_slice())
                    .execute(tx.as_mut())
                    .await?;

                    for (index, column) in schema_spec.columns.iter().enumerate() {
                        let column_uuid = uuid::Uuid::new_v4();
                        let ordinal_position = (index + 1) as i32;

                        sqlx::query::<sqlx::Postgres>(
                            "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                             VALUES ($1, $2, $3, $4, $5, $6)",
                        )
                        .bind(column_uuid.as_bytes().as_slice())
                        .bind(schema_uuid.as_bytes().as_slice())
                        .bind(column.name.as_str())
                        .bind(column.column_type.as_str())
                        .bind(ordinal_position)
                        .bind(column.is_nullable)
                        .execute(tx.as_mut())
                        .await?;
                    }

                    new_schema_uuid = Some(schema_uuid);
                }
                MutationOp::SetProperties(properties) => {
                    properties_value = properties;
                }
                MutationOp::RemoveProperties(keys) => {
                    if keys.is_empty() {
                        continue;
                    }
                    let map = properties_value
                        .as_object_mut()
                        .ok_or_else(|| CatalogError::InvalidArgument("properties must be an object".to_string()))?;
                    for key in keys {
                        map.remove(&key);
                    }
                }
            }
        }

        let schema_uuid_to_set = new_schema_uuid.or(current_schema_uuid);
        let properties_text = serialize_json(&properties_value)?;

        sqlx::query::<sqlx::Postgres>(
            "UPDATE tables
             SET current_transaction_id = $1, current_schema_uuid = $2, properties = $3
             WHERE table_uuid = $4",
        )
        .bind(transaction_id)
        .bind(schema_uuid_to_set.map(|uuid| uuid.as_bytes().to_vec()))
        .bind(properties_text.as_str())
        .bind(table_uuid.as_bytes().as_slice())
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;

        Ok(CommitResult {
            transaction_id,
            table_view: None,
        })
    }
}

/// Extract UUID from a database row
fn uuid_from_row<R>(row: &R, column: &str) -> Result<uuid::Uuid>
where
    R: Row,
    for<'r> Vec<u8>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
{
    let bytes: Vec<u8> = row
        .try_get(column)
        .map_err(CatalogError::Storage)?;
    uuid::Uuid::from_slice(&bytes)
        .map_err(|error| CatalogError::InvalidArgument(format!("invalid uuid: {error}")))
}

/// Extract optional UUID from a database row
fn uuid_from_row_optional<R>(row: &R, column: &str) -> Result<Option<uuid::Uuid>>
where
    R: Row,
    for<'r> Option<Vec<u8>>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'c> &'c str: sqlx::ColumnIndex<R>,
{
    let bytes: Option<Vec<u8>> = row
        .try_get(column)
        .map_err(CatalogError::Storage)?;
    match bytes {
        Some(bytes) => uuid::Uuid::from_slice(&bytes)
            .map(Some)
            .map_err(|error| CatalogError::InvalidArgument(format!("invalid uuid: {error}"))),
        None => Ok(None),
    }
}

/// Parse JSON from optional string (defaults to empty object)
fn parse_json(value: Option<String>) -> Result<serde_json::Value> {
    match value {
        Some(text) => serde_json::from_str(&text)
            .map_err(|error| CatalogError::InvalidArgument(format!("invalid json: {error}"))),
        None => Ok(serde_json::json!({})),
    }
}

/// Parse optional JSON from optional string
fn parse_json_optional(value: Option<String>) -> Result<Option<serde_json::Value>> {
    match value {
        Some(text) => serde_json::from_str(&text)
            .map(Some)
            .map_err(|error| CatalogError::InvalidArgument(format!("invalid json: {error}"))),
        None => Ok(None),
    }
}

/// Serialize JSON value to string
fn serialize_json(value: &serde_json::Value) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|error| CatalogError::InvalidArgument(format!("invalid json: {error}")))
}

/// Serialize optional JSON value to optional string
fn serialize_json_optional(value: Option<&serde_json::Value>) -> Result<Option<String>> {
    value.map(serialize_json).transpose()
}

/// Get the next transaction ID from the database
async fn next_transaction_id<'a, DB>(
    tx: &mut sqlx::Transaction<'a, DB>,
) -> Result<TxnId>
where
    DB: Database,
    for<'r> i64: sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    for<'q> <DB as Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
    for<'r> usize: sqlx::ColumnIndex<<DB as Database>::Row>,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
{
    let next_id: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(transaction_id), 0) + 1 FROM transactions",
    )
    .fetch_one(tx.as_mut())
    .await?;
    Ok(next_id)
}
