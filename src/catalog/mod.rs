//! Catalog module for managing table metadata and transactions

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{Database, Pool, Row};

/// Database-specific configuration
pub mod database;
mod helpers;
mod models;

/// Data type definitions
pub mod data_type;
/// Catalog error types
pub mod error;
/// Bounded limits for catalog operations
pub mod limits;
/// Schema definitions
pub mod schema;

pub use data_type::{can_evolve_to, decode_data_type, encode_data_type};
pub use error::{CatalogError, Result};
use helpers::{
    next_transaction_id, parse_json_optional, parse_table_properties, project_delta_range,
    serialize_json, serialize_json_optional, uuid_from_row, uuid_from_row_optional,
};
pub use models::*;

const INITIAL_SCHEMA_VERSION: i32 = 1;

/// Client protocol versions used for compatibility checks.
#[derive(Clone, Copy, Debug)]
pub struct ProtocolVersions {
    /// Client reader protocol version.
    pub reader: i32,
    /// Client writer protocol version.
    pub writer: i32,
}

impl Default for ProtocolVersions {
    fn default() -> Self {
        Self {
            reader: 1,
            writer: 1,
        }
    }
}

/// Transactional catalog API for table metadata.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Creates a table and returns an attached [`TableHandle`].
    async fn create_table(
        self: Arc<Self>,
        ident: TableIdent,
        location: String,
        schema: SchemaSpec,
        properties: Option<TableProperties>,
    ) -> Result<TableHandle>;

    /// Loads a table handle if the table exists.
    async fn load_table(self: Arc<Self>, ident: TableIdent) -> Result<Option<TableHandle>>;

    /// Lists tables, optionally filtered by namespace.
    async fn list_tables(&self, namespace: Option<&str>) -> Result<Vec<TableIdent>>;

    /// Drops a table and its metadata.
    async fn drop_table(&self, ident: &TableIdent) -> Result<()>;

    /// Resolves the current transaction head for a table.
    async fn current_transaction_id(&self, ident: &TableIdent) -> Result<TxnId>;

    /// Lists ordered transaction events in a bounded range.
    async fn list_transaction_events(
        &self,
        ident: &TableIdent,
        cursor: TxnRangeCursor,
    ) -> Result<Vec<TxnEvent>>;

    /// Reads a table snapshot at a transaction (or current when `None`).
    async fn read_table(
        &self,
        ident: &TableIdent,
        at_transaction_id: Option<TxnId>,
    ) -> Result<TableView>;

    /// Computes metadata delta between two transactions.
    async fn diff_table(
        &self,
        ident: &TableIdent,
        from_transaction_id: TxnId,
        to_transaction_id: TxnId,
    ) -> Result<TableDelta>;

    /// Commits a mutation using optimistic concurrency control.
    ///
    /// Implementations should reject commits when `base_transaction_id` does not
    /// match the current table transaction.
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
    /// Client protocol versions bound to this catalog instance.
    client_protocol: ProtocolVersions,
}

impl<DB> SqlCatalog<DB>
where
    DB: Database,
    <DB as Database>::Connection: sqlx::migrate::Migrate,
{
    /// Create a new SQL catalog with a connection pool
    pub fn new(pool: Pool<DB>) -> Self {
        Self::new_with_protocol_versions(pool, ProtocolVersions::default())
    }

    /// Create a new SQL catalog with explicit client protocol versions.
    pub fn new_with_protocol_versions(pool: Pool<DB>, client_protocol: ProtocolVersions) -> Self {
        Self {
            pool,
            client_protocol,
        }
    }

    fn reader_version(&self) -> i32 {
        self.client_protocol.reader
    }

    fn writer_version(&self) -> i32 {
        self.client_protocol.writer
    }

    fn ensure_reader_protocol_compatible(
        &self,
        ident: &TableIdent,
        required_min_version: i32,
    ) -> Result<()> {
        let client_version = self.reader_version();
        if client_version < required_min_version {
            return Err(CatalogError::ProtocolVersionIncompatible {
                operation: "read",
                table: ident.to_string(),
                client_version,
                required_min_version,
            });
        }
        Ok(())
    }

    fn ensure_writer_protocol_compatible(
        &self,
        ident: &TableIdent,
        required_min_version: i32,
    ) -> Result<()> {
        let client_version = self.writer_version();
        if client_version < required_min_version {
            return Err(CatalogError::ProtocolVersionIncompatible {
                operation: "write",
                table: ident.to_string(),
                client_version,
                required_min_version,
            });
        }
        Ok(())
    }
}

impl SqlCatalog<sqlx::Sqlite> {
    async fn begin_immediate(
        &self,
    ) -> std::result::Result<sqlx::Transaction<'static, sqlx::Sqlite>, sqlx::Error> {
        self.pool.begin_with("BEGIN IMMEDIATE").await
    }

    /// Initialize the SQLite schema by running SQLite migrations.
    pub async fn initialize_schema(&self) -> std::result::Result<(), sqlx::Error> {
        let migrator = sqlx::migrate!("db/migrations/sqlite");
        migrator.run(&self.pool).await?;
        Ok(())
    }

    /// Configure SQLite-specific database settings
    pub async fn configure_database(&self) -> std::result::Result<(), sqlx::Error> {
        database::sqlite::configure_pool(&self.pool).await
    }

    /// Create and initialize a SQLite catalog from a connection string.
    /// This is a convenience method that handles pool creation, configuration, and schema initialization.
    pub async fn from_connection_string(
        connection_string: &str,
    ) -> std::result::Result<Arc<Self>, sqlx::Error> {
        Self::from_connection_string_with_protocol_versions(
            connection_string,
            ProtocolVersions::default(),
        )
        .await
    }

    /// Create and initialize a SQLite catalog with explicit protocol versions.
    pub async fn from_connection_string_with_protocol_versions(
        connection_string: &str,
        client_protocol: ProtocolVersions,
    ) -> std::result::Result<Arc<Self>, sqlx::Error> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect(connection_string)
            .await?;
        let catalog = Arc::new(Self::new_with_protocol_versions(pool, client_protocol));
        catalog.configure_database().await?;
        catalog.initialize_schema().await?;
        Ok(catalog)
    }

    /// Create an in-memory SQLite catalog
    pub async fn in_memory() -> std::result::Result<Arc<Self>, sqlx::Error> {
        Self::in_memory_with_protocol_versions(ProtocolVersions::default()).await
    }

    /// Create an in-memory SQLite catalog with explicit protocol versions.
    pub async fn in_memory_with_protocol_versions(
        client_protocol: ProtocolVersions,
    ) -> std::result::Result<Arc<Self>, sqlx::Error> {
        Self::from_connection_string_with_protocol_versions("sqlite::memory:", client_protocol)
            .await
    }
}

impl SqlCatalog<sqlx::Postgres> {
    /// Initialize the PostgreSQL schema by running PostgreSQL migrations.
    pub async fn initialize_schema(&self) -> std::result::Result<(), sqlx::Error> {
        let migrator = sqlx::migrate!("db/migrations/postgres");
        migrator.run(&self.pool).await?;
        Ok(())
    }

    /// Configure PostgreSQL-specific database settings
    pub async fn configure_database(&self) -> std::result::Result<(), sqlx::Error> {
        database::postgres::configure_pool(&self.pool).await
    }

    /// Create and initialize a PostgreSQL catalog from a connection string
    /// This is a convenience method that handles pool creation, configuration, and schema initialization
    pub async fn from_connection_string(
        connection_string: &str,
    ) -> std::result::Result<Arc<Self>, sqlx::Error> {
        Self::from_connection_string_with_protocol_versions(
            connection_string,
            ProtocolVersions::default(),
        )
        .await
    }

    /// Create and initialize a PostgreSQL catalog with explicit protocol versions.
    pub async fn from_connection_string_with_protocol_versions(
        connection_string: &str,
        client_protocol: ProtocolVersions,
    ) -> std::result::Result<Arc<Self>, sqlx::Error> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect(connection_string)
            .await?;
        let catalog = Arc::new(Self::new_with_protocol_versions(pool, client_protocol));
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
        properties: Option<TableProperties>,
    ) -> Result<TableHandle> {
        schema.validate()?;

        let mut tx = self.begin_immediate().await?;

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
        let properties_value = properties.unwrap_or_default();
        let properties_text = serialize_json(&properties_value.to_json())?;
        let min_reader_version = self.reader_version();
        let min_writer_version = self.writer_version();

        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO tables (
                table_uuid, table_name, namespace, location, created_at, properties,
                min_reader_version, min_writer_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(ident.name.as_str())
        .bind(ident.namespace.as_str())
        .bind(location.as_str())
        .bind(created_at)
        .bind(properties_text.as_str())
        .bind(min_reader_version)
        .bind(min_writer_version)
        .execute(tx.as_mut())
        .await?;

        let transaction_id = next_transaction_id();
        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp)
             VALUES (?1, ?2, ?3)",
        )
        .bind(transaction_id.as_bytes().as_slice())
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
        .bind(INITIAL_SCHEMA_VERSION)
        .bind(transaction_id.as_bytes().as_slice())
        .bind(created_at)
        .execute(tx.as_mut())
        .await?;

        // Chunk columns per statement so total bind parameters stay under DB limits.
        let col_chunk_size = limits::batch_insert_columns_chunk(database::DbKind::Sqlite);
        debug_assert!(
            limits::column_insert_bind_count(col_chunk_size)
                <= limits::db_bind_limit(database::DbKind::Sqlite),
            "column insert chunk exceeds SQLite bind parameter limit"
        );
        let mut row_data = Vec::with_capacity(col_chunk_size as usize);
        for (chunk_index, col_chunk) in schema.columns.chunks(col_chunk_size as usize).enumerate() {
            row_data.clear();
            for (i, column) in col_chunk.iter().enumerate() {
                let ordinal_position = (chunk_index * col_chunk_size as usize + i + 1) as i32;
                let encoded_type = encode_data_type(&column.column_type)?;
                row_data.push((
                    uuid::Uuid::new_v4(),
                    column.name.clone(),
                    encoded_type,
                    ordinal_position,
                    column.is_nullable,
                ));
            }
            let row_placeholders: String = (0..row_data.len())
                .map(|_| "(?, ?, ?, ?, ?, ?)")
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                 VALUES {}",
                row_placeholders
            );
            let mut query = sqlx::query::<sqlx::Sqlite>(&sql);
            for (column_uuid, name, encoded_type, ordinal_position, is_nullable) in &row_data {
                query = query
                    .bind(column_uuid.as_bytes().as_slice())
                    .bind(schema_uuid.as_bytes().as_slice())
                    .bind(name.as_str())
                    .bind(encoded_type.as_slice())
                    .bind(*ordinal_position)
                    .bind(*is_nullable);
            }
            query.execute(tx.as_mut()).await?;
        }

        sqlx::query::<sqlx::Sqlite>(
            "UPDATE tables
             SET current_schema_uuid = ?1, current_transaction_id = ?2
             WHERE table_uuid = ?3",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .bind(transaction_id.as_bytes().as_slice())
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
        let list_limit = limits::MAX_TABLES_PER_LIST as i64 + 1;
        let rows = if let Some(namespace) = namespace {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT namespace, table_name FROM tables WHERE namespace = ?1 LIMIT ?2",
            )
            .bind(namespace)
            .bind(list_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Sqlite>("SELECT namespace, table_name FROM tables LIMIT ?1")
                .bind(list_limit)
                .fetch_all(&self.pool)
                .await?
        };

        if rows.len() > limits::MAX_TABLES_PER_LIST as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "table count exceeds limit of {}",
                limits::MAX_TABLES_PER_LIST
            )));
        }

        let capacity = std::cmp::min(rows.len(), limits::MAX_TABLES_PER_LIST as usize);
        let mut tables = Vec::with_capacity(capacity);
        for row in rows {
            let namespace: String = row.try_get("namespace")?;
            let name: String = row.try_get("table_name")?;
            tables.push(TableIdent { namespace, name });
        }

        Ok(tables)
    }

    async fn drop_table(&self, ident: &TableIdent) -> Result<()> {
        let affected = sqlx::query::<sqlx::Sqlite>(
            "DELETE FROM tables WHERE namespace = ?1 AND table_name = ?2",
        )
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

    async fn current_transaction_id(&self, ident: &TableIdent) -> Result<TxnId> {
        let table_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT current_transaction_id
             FROM tables WHERE namespace = ?1 AND table_name = ?2",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(&self.pool)
        .await?;

        let table_row = table_row
            .ok_or_else(|| CatalogError::NotFound(format!("{}.{}", ident.namespace, ident.name)))?;
        let txn_id = uuid_from_row_optional(&table_row, "current_transaction_id")?;
        txn_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })
    }

    async fn list_transaction_events(
        &self,
        ident: &TableIdent,
        cursor: TxnRangeCursor,
    ) -> Result<Vec<TxnEvent>> {
        let table_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT table_uuid, current_transaction_id
             FROM tables WHERE namespace = ?1 AND table_name = ?2",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(&self.pool)
        .await?;
        let table_row = table_row
            .ok_or_else(|| CatalogError::NotFound(format!("{}.{}", ident.namespace, ident.name)))?;
        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let current_transaction_id = uuid_from_row_optional(&table_row, "current_transaction_id")?
            .ok_or_else(|| {
                CatalogError::InvalidArgument("table has no current transaction".to_string())
            })?;

        if cursor.to_inclusive.as_u128() > current_transaction_id.as_u128() {
            return Err(CatalogError::InvalidArgument(
                "requested transaction is newer than current".to_string(),
            ));
        }
        if let Some(from_exclusive) = cursor.from_exclusive {
            if from_exclusive.as_u128() > cursor.to_inclusive.as_u128() {
                return Err(CatalogError::InvalidArgument(
                    "from transaction must be <= to transaction".to_string(),
                ));
            }
        }

        let txn_scan_limit = limits::MAX_TRANSACTIONS_PER_SCAN as i64 + 1;
        let txn_rows = if let Some(from_exclusive) = cursor.from_exclusive {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT transaction_id
                 FROM transactions
                 WHERE table_uuid = ?1
                   AND transaction_id > ?2
                   AND transaction_id <= ?3
                 ORDER BY transaction_id
                 LIMIT ?4",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(from_exclusive.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(txn_scan_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT transaction_id
                 FROM transactions
                 WHERE table_uuid = ?1
                   AND transaction_id <= ?2
                 ORDER BY transaction_id
                 LIMIT ?3",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(txn_scan_limit)
            .fetch_all(&self.pool)
            .await?
        };

        if txn_rows.len() > limits::MAX_TRANSACTIONS_PER_SCAN as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "transaction scan exceeds limit of {}",
                limits::MAX_TRANSACTIONS_PER_SCAN
            )));
        }

        let mut transaction_ids = Vec::with_capacity(txn_rows.len());
        for row in txn_rows {
            transaction_ids.push(uuid_from_row(&row, "transaction_id")?);
        }
        let mut index_by_txn = std::collections::HashMap::with_capacity(transaction_ids.len());
        let mut events = Vec::with_capacity(transaction_ids.len());
        for (index, transaction_id) in transaction_ids.into_iter().enumerate() {
            index_by_txn.insert(transaction_id, index);
            events.push(TxnEvent {
                transaction_id,
                file_changes: Vec::new(),
                schema_change: None,
            });
        }

        // Sentinel-row pattern: fetch one extra row to detect truncation.
        // If we get MAX+1 rows, we fail with LimitExceeded instead of silently dropping events.
        let added_scan_limit = limits::MAX_FILES_PER_QUERY as i64 + 1;
        let added_rows = if let Some(from_exclusive) = cursor.from_exclusive {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                        added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 FROM files
                 WHERE table_uuid = ?1
                   AND added_in_transaction_id > ?2
                   AND added_in_transaction_id <= ?3
                 LIMIT ?4",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(from_exclusive.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(added_scan_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                        added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 FROM files
                 WHERE table_uuid = ?1
                   AND added_in_transaction_id <= ?2
                 LIMIT ?3",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(added_scan_limit)
            .fetch_all(&self.pool)
            .await?
        };
        if added_rows.len() > limits::MAX_FILES_PER_QUERY as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "added file events exceed limit of {}",
                limits::MAX_FILES_PER_QUERY
            )));
        }

        for row in added_rows {
            let partition_values: Option<String> = row.try_get("partition_values")?;
            let format_options: Option<String> = row.try_get("format_options")?;
            let added_in_transaction_id = uuid_from_row(&row, "added_in_transaction_id")?;
            if let Some(index) = index_by_txn.get(&added_in_transaction_id) {
                events[*index].file_changes.push(TxnFileChange {
                    transaction_id: added_in_transaction_id,
                    kind: TxnFileChangeKind::Added,
                    file: schema::File {
                        file_uuid: uuid_from_row(&row, "file_uuid")?,
                        table_uuid: uuid_from_row(&row, "table_uuid")?,
                        file_format: row.try_get("file_format")?,
                        file_path: row.try_get("file_path")?,
                        record_count: row.try_get("record_count")?,
                        file_size_bytes: row.try_get("file_size_bytes")?,
                        added_in_transaction_id,
                        removed_in_transaction_id: uuid_from_row_optional(
                            &row,
                            "removed_in_transaction_id",
                        )?,
                        partition_values: parse_json_optional(partition_values)?,
                        format_options: parse_json_optional(format_options)?,
                    },
                });
            }
        }

        let removed_scan_limit = limits::MAX_FILES_PER_QUERY as i64 + 1;
        let removed_rows = if let Some(from_exclusive) = cursor.from_exclusive {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                        added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 FROM files
                 WHERE table_uuid = ?1
                   AND removed_in_transaction_id IS NOT NULL
                   AND removed_in_transaction_id > ?2
                   AND removed_in_transaction_id <= ?3
                 LIMIT ?4",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(from_exclusive.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(removed_scan_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                        added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 FROM files
                 WHERE table_uuid = ?1
                   AND removed_in_transaction_id IS NOT NULL
                   AND removed_in_transaction_id <= ?2
                 LIMIT ?3",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(removed_scan_limit)
            .fetch_all(&self.pool)
            .await?
        };
        if removed_rows.len() > limits::MAX_FILES_PER_QUERY as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "removed file events exceed limit of {}",
                limits::MAX_FILES_PER_QUERY
            )));
        }

        for row in removed_rows {
            let partition_values: Option<String> = row.try_get("partition_values")?;
            let format_options: Option<String> = row.try_get("format_options")?;
            let removed_in_transaction_id =
                uuid_from_row_optional(&row, "removed_in_transaction_id")?.ok_or_else(|| {
                    CatalogError::InvalidArgument("missing removed transaction id".to_string())
                })?;
            if let Some(index) = index_by_txn.get(&removed_in_transaction_id) {
                events[*index].file_changes.push(TxnFileChange {
                    transaction_id: removed_in_transaction_id,
                    kind: TxnFileChangeKind::Removed,
                    file: schema::File {
                        file_uuid: uuid_from_row(&row, "file_uuid")?,
                        table_uuid: uuid_from_row(&row, "table_uuid")?,
                        file_format: row.try_get("file_format")?,
                        file_path: row.try_get("file_path")?,
                        record_count: row.try_get("record_count")?,
                        file_size_bytes: row.try_get("file_size_bytes")?,
                        added_in_transaction_id: uuid_from_row(&row, "added_in_transaction_id")?,
                        removed_in_transaction_id: Some(removed_in_transaction_id),
                        partition_values: parse_json_optional(partition_values)?,
                        format_options: parse_json_optional(format_options)?,
                    },
                });
            }
        }

        let schema_scan_limit = limits::MAX_COLUMNS_PER_SCHEMA as i64 + 1;
        let schema_rows = if let Some(from_exclusive) = cursor.from_exclusive {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT schema_uuid, table_uuid, schema_version, valid_from_transaction_id, valid_to_transaction_id, created_at
                 FROM schemas
                 WHERE table_uuid = ?1
                   AND valid_from_transaction_id > ?2
                   AND valid_from_transaction_id <= ?3
                 LIMIT ?4",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(from_exclusive.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(schema_scan_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Sqlite>(
                "SELECT schema_uuid, table_uuid, schema_version, valid_from_transaction_id, valid_to_transaction_id, created_at
                 FROM schemas
                 WHERE table_uuid = ?1
                   AND valid_from_transaction_id <= ?2
                 LIMIT ?3",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(schema_scan_limit)
            .fetch_all(&self.pool)
            .await?
        };
        if schema_rows.len() > limits::MAX_COLUMNS_PER_SCHEMA as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "schema change events exceed limit of {}",
                limits::MAX_COLUMNS_PER_SCHEMA
            )));
        }

        for schema_row in schema_rows {
            let schema_uuid = uuid_from_row(&schema_row, "schema_uuid")?;
            let valid_from_transaction_id =
                uuid_from_row(&schema_row, "valid_from_transaction_id")?;
            let column_rows = sqlx::query::<sqlx::Sqlite>(
                "SELECT column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable
                 FROM columns WHERE schema_uuid = ?1 ORDER BY ordinal_position LIMIT ?2",
            )
            .bind(schema_uuid.as_bytes().as_slice())
            .bind(limits::MAX_COLUMNS_PER_SCHEMA as i64)
            .fetch_all(&self.pool)
            .await?;

            let mut columns = Vec::with_capacity(column_rows.len());
            for row in column_rows {
                let column_type_bytes: Vec<u8> = row.try_get("column_type")?;
                let column_type = decode_data_type(&column_type_bytes)?;
                columns.push(schema::Column {
                    column_uuid: uuid_from_row(&row, "column_uuid")?,
                    schema_uuid: uuid_from_row(&row, "schema_uuid")?,
                    column_name: row.try_get("column_name")?,
                    column_type,
                    ordinal_position: row.try_get("ordinal_position")?,
                    is_nullable: row.try_get("is_nullable")?,
                });
            }

            if let Some(index) = index_by_txn.get(&valid_from_transaction_id) {
                events[*index].schema_change = Some(TxnSchemaChange {
                    transaction_id: valid_from_transaction_id,
                    schema: schema::Schema {
                        schema_uuid,
                        table_uuid: uuid_from_row(&schema_row, "table_uuid")?,
                        schema_version: schema_row.try_get("schema_version")?,
                        valid_from_transaction_id,
                        valid_to_transaction_id: uuid_from_row_optional(
                            &schema_row,
                            "valid_to_transaction_id",
                        )?,
                        created_at: schema_row.try_get("created_at")?,
                        columns,
                    },
                });
            }
        }

        Ok(events)
    }

    async fn read_table(
        &self,
        ident: &TableIdent,
        at_transaction_id: Option<TxnId>,
    ) -> Result<TableView> {
        let table_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT table_uuid, current_schema_uuid, current_transaction_id, properties,
                    min_reader_version, min_writer_version
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
                )));
            }
        };

        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let _current_schema_uuid = uuid_from_row_optional(&table_row, "current_schema_uuid")?;
        let current_transaction_id = uuid_from_row_optional(&table_row, "current_transaction_id")?;
        let min_reader_version: i32 = table_row.try_get("min_reader_version")?;
        let properties = parse_table_properties(table_row.try_get("properties")?)?;
        self.ensure_reader_protocol_compatible(ident, min_reader_version)?;

        let current_transaction_id = current_transaction_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })?;
        let effective_transaction_id = at_transaction_id.unwrap_or(current_transaction_id);
        // UUIDv7 is time-ordered, compare using u128 representation
        if effective_transaction_id.as_u128() > current_transaction_id.as_u128() {
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
        .bind(effective_transaction_id.as_bytes().as_slice())
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
             FROM columns WHERE schema_uuid = ?1 ORDER BY ordinal_position LIMIT ?2",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .bind(limits::MAX_COLUMNS_PER_SCHEMA as i64 + 1)
        .fetch_all(&self.pool)
        .await?;

        if column_rows.len() > limits::MAX_COLUMNS_PER_SCHEMA as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "column count exceeds limit of {}",
                limits::MAX_COLUMNS_PER_SCHEMA
            )));
        }

        let capacity = std::cmp::min(column_rows.len(), limits::MAX_COLUMNS_PER_SCHEMA as usize);
        let mut columns = Vec::with_capacity(capacity);
        for row in column_rows {
            let column_type_bytes: Vec<u8> = row.try_get("column_type")?;
            let column_type = decode_data_type(&column_type_bytes)?;

            let column = schema::Column {
                column_uuid: uuid_from_row(&row, "column_uuid")?,
                schema_uuid: uuid_from_row(&row, "schema_uuid")?,
                column_name: row.try_get("column_name")?,
                column_type,
                ordinal_position: row.try_get("ordinal_position")?,
                is_nullable: row.try_get("is_nullable")?,
            };
            columns.push(column);
        }

        let schema = schema::Schema {
            schema_uuid,
            table_uuid: uuid_from_row(&schema_row, "table_uuid")?,
            schema_version: schema_row.try_get("schema_version")?,
            valid_from_transaction_id: uuid_from_row(&schema_row, "valid_from_transaction_id")?,
            valid_to_transaction_id: uuid_from_row_optional(
                &schema_row,
                "valid_to_transaction_id",
            )?,
            created_at: schema_row.try_get("created_at")?,
            columns,
        };

        let file_rows = sqlx::query::<sqlx::Sqlite>(
            "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                    added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
             FROM files
             WHERE table_uuid = ?1
               AND added_in_transaction_id <= ?2
               AND (removed_in_transaction_id IS NULL OR removed_in_transaction_id > ?2)
             LIMIT ?3",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id.as_bytes().as_slice())
        .bind(limits::MAX_FILES_PER_QUERY as i64 + 1)
        .fetch_all(&self.pool)
        .await?;

        if file_rows.len() > limits::MAX_FILES_PER_QUERY as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "file count exceeds limit of {}",
                limits::MAX_FILES_PER_QUERY
            )));
        }

        let capacity = std::cmp::min(file_rows.len(), limits::MAX_FILES_PER_QUERY as usize);
        let mut files = Vec::with_capacity(capacity);
        for row in file_rows {
            let partition_values: Option<String> = row.try_get("partition_values")?;
            let format_options: Option<String> = row.try_get("format_options")?;
            let file = schema::File {
                file_uuid: uuid_from_row(&row, "file_uuid")?,
                table_uuid: uuid_from_row(&row, "table_uuid")?,
                file_format: row.try_get("file_format")?,
                file_path: row.try_get("file_path")?,
                record_count: row.try_get("record_count")?,
                file_size_bytes: row.try_get("file_size_bytes")?,
                added_in_transaction_id: uuid_from_row(&row, "added_in_transaction_id")?,
                removed_in_transaction_id: uuid_from_row_optional(
                    &row,
                    "removed_in_transaction_id",
                )?,
                partition_values: parse_json_optional(partition_values)?,
                format_options: parse_json_optional(format_options)?,
            };
            files.push(file);
        }

        let stats_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT table_uuid, transaction_id, record_count, file_size_bytes, file_count, last_updated
             FROM table_stats WHERE table_uuid = ?1 AND transaction_id = ?2",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await?;

        let stats = if let Some(row) = stats_row {
            Some(schema::TableStats {
                table_uuid: uuid_from_row(&row, "table_uuid")?,
                transaction_id: uuid_from_row(&row, "transaction_id")?,
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
        let events = self
            .list_transaction_events(
                ident,
                TxnRangeCursor {
                    from_exclusive: Some(from_transaction_id),
                    to_inclusive: to_transaction_id,
                },
            )
            .await?;

        let from_view = self.read_table(ident, Some(from_transaction_id)).await?;
        let to_view = self.read_table(ident, Some(to_transaction_id)).await?;
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

        Ok(project_delta_range(
            from_transaction_id,
            to_transaction_id,
            &events,
            new_schema,
            new_properties,
        ))
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

        if mutation.operations.len() as u32 > limits::MAX_OPERATIONS_PER_MUTATION {
            return Err(CatalogError::LimitExceeded(format!(
                "mutation has {} operations, exceeds limit of {}",
                mutation.operations.len(),
                limits::MAX_OPERATIONS_PER_MUTATION
            )));
        }
        mutation.validate()?;

        let mut tx = self.begin_immediate().await?;

        let table_row = sqlx::query::<sqlx::Sqlite>(
            "SELECT table_uuid, current_schema_uuid, current_transaction_id, properties,
                    min_reader_version, min_writer_version
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
                )));
            }
        };

        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let current_schema_uuid = uuid_from_row_optional(&table_row, "current_schema_uuid")?;
        let current_transaction_id = uuid_from_row_optional(&table_row, "current_transaction_id")?;
        let min_writer_version: i32 = table_row.try_get("min_writer_version")?;
        let mut properties_value = parse_table_properties(table_row.try_get("properties")?)?;
        self.ensure_writer_protocol_compatible(ident, min_writer_version)?;

        let current_transaction_id = current_transaction_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })?;

        if current_transaction_id != base_transaction_id {
            return Err(CatalogError::Conflict(format!(
                "base transaction {} does not match current {}",
                base_transaction_id, current_transaction_id
            )));
        }

        let transaction_id = next_transaction_id();
        let transaction_timestamp = chrono::Utc::now();

        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp, parent_transaction_id)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(transaction_id.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .bind(transaction_timestamp)
        .bind(current_transaction_id.as_bytes().as_slice())
        .execute(tx.as_mut())
        .await?;

        let mut new_schema_uuid = current_schema_uuid;

        for op in mutation.operations {
            match op {
                MutationOp::AppendFiles(files) => {
                    if files.len() as u32 > limits::MAX_FILES_PER_APPEND {
                        return Err(CatalogError::LimitExceeded(format!(
                            "cannot append {} files, exceeds limit of {}",
                            files.len(),
                            limits::MAX_FILES_PER_APPEND
                        )));
                    }
                    // Chunk file inserts to keep per-statement bind count bounded.
                    let chunk_size = limits::BATCH_INSERT_FILES_CHUNK;
                    debug_assert!(
                        limits::file_insert_bind_count(chunk_size)
                            <= limits::db_bind_limit(database::DbKind::Sqlite),
                        "file insert chunk exceeds SQLite bind parameter limit"
                    );
                    let mut row_data = Vec::with_capacity(chunk_size as usize);
                    for chunk in files.chunks(chunk_size as usize) {
                        row_data.clear();
                        for f in chunk {
                            f.validate()?;
                            let file_uuid = f.file_uuid.unwrap_or_else(uuid::Uuid::new_v4);
                            let partition_text =
                                serialize_json_optional(f.partition_values.as_ref())?;
                            let format_text = serialize_json_optional(f.format_options.as_ref())?;
                            row_data.push((
                                file_uuid,
                                f.file_format.as_str(),
                                f.file_path.clone(),
                                f.record_count,
                                f.file_size_bytes,
                                partition_text,
                                format_text,
                            ));
                        }
                        let row_placeholders: String = (0..row_data.len())
                            .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            "INSERT INTO files (file_uuid, table_uuid, file_format, file_path, record_count,
                                                 file_size_bytes, added_in_transaction_id, partition_values, format_options)
                             VALUES {}",
                            row_placeholders
                        );
                        let mut query = sqlx::query::<sqlx::Sqlite>(&sql);
                        for (file_uuid, format, path, rc, size, part, format_opts) in &row_data {
                            query = query
                                .bind(file_uuid.as_bytes().as_slice())
                                .bind(table_uuid.as_bytes().as_slice())
                                .bind(*format)
                                .bind(path.as_str())
                                .bind(*rc)
                                .bind(*size)
                                .bind(transaction_id.as_bytes().as_slice())
                                .bind(part.as_deref())
                                .bind(format_opts.as_deref());
                        }
                        query.execute(tx.as_mut()).await?;
                    }
                }
                MutationOp::DeleteFiles(file_uuids) => {
                    if file_uuids.len() as u32 > limits::MAX_FILES_PER_DELETE {
                        return Err(CatalogError::LimitExceeded(format!(
                            "cannot delete {} files, exceeds limit of {}",
                            file_uuids.len(),
                            limits::MAX_FILES_PER_DELETE
                        )));
                    }
                    // Chunk deletes so `IN (...)` stays under DB bind parameter limits.
                    let chunk_size = limits::BATCH_DELETE_FILES_CHUNK;
                    debug_assert!(
                        limits::delete_files_bind_count(chunk_size)
                            <= limits::db_bind_limit(database::DbKind::Sqlite),
                        "file delete chunk exceeds SQLite bind parameter limit"
                    );
                    for chunk in file_uuids.chunks(chunk_size as usize) {
                        let in_placeholders: String = (0..chunk.len())
                            .map(|i| format!("?{}", i + 3))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            "UPDATE files SET removed_in_transaction_id = ?1
                             WHERE table_uuid = ?2 AND removed_in_transaction_id IS NULL AND file_uuid IN ({})",
                            in_placeholders
                        );
                        let mut query = sqlx::query::<sqlx::Sqlite>(&sql)
                            .bind(transaction_id.as_bytes().as_slice())
                            .bind(table_uuid.as_bytes().as_slice());
                        for file_uuid in chunk {
                            query = query.bind(file_uuid.as_bytes().as_slice());
                        }
                        query.execute(tx.as_mut()).await?;
                    }
                }
                MutationOp::UpdateSchema(schema_spec) => {
                    schema_spec.validate()?;

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

                    // Fetch current schema columns for validation
                    let current_column_rows = sqlx::query::<sqlx::Sqlite>(
                        "SELECT column_name, column_type, is_nullable FROM columns WHERE schema_uuid = ?1",
                    )
                    .bind(current_schema_uuid.as_bytes().as_slice())
                    .fetch_all(tx.as_mut())
                    .await?;

                    // Build map of current column names to (DataType, nullability)
                    let mut current_columns = std::collections::HashMap::new();
                    for row in current_column_rows {
                        let column_name: String = row.try_get("column_name")?;
                        let column_type_bytes: Vec<u8> = row.try_get("column_type")?;
                        let column_type = decode_data_type(&column_type_bytes)?;
                        let is_nullable: bool = row.try_get("is_nullable")?;
                        current_columns.insert(column_name, (column_type, is_nullable));
                    }

                    let new_column_names = schema_spec
                        .columns
                        .iter()
                        .map(|column| column.name.as_str())
                        .collect::<std::collections::HashSet<_>>();
                    for existing_column_name in current_columns.keys() {
                        if !new_column_names.contains(existing_column_name.as_str()) {
                            return Err(CatalogError::InvalidArgument(format!(
                                "cannot drop existing column '{}' via UpdateSchema",
                                existing_column_name
                            )));
                        }
                    }

                    // Validate schema evolution for each column in new schema
                    for new_column in &schema_spec.columns {
                        if let Some((old_type, old_nullable)) =
                            current_columns.get(&new_column.name)
                        {
                            // Column exists - validate evolution

                            // Check type evolution
                            if !can_evolve_to(old_type, &new_column.column_type) {
                                return Err(CatalogError::InvalidArgument(format!(
                                    "invalid schema evolution for column '{}': cannot evolve {:?} to {:?}",
                                    new_column.name, old_type, new_column.column_type
                                )));
                            }

                            // Check nullability evolution
                            // Making nullable -> non-nullable is unsafe (existing nulls would violate constraint)
                            if *old_nullable && !new_column.is_nullable {
                                return Err(CatalogError::InvalidArgument(format!(
                                    "invalid schema evolution for column '{}': cannot change from nullable to non-nullable (existing nulls would violate constraint)",
                                    new_column.name
                                )));
                            }
                        }
                        // New columns are always allowed (they default to null for existing data)
                    }

                    let schema_uuid = uuid::Uuid::new_v4();
                    let schema_version = current_schema_version + 1;

                    sqlx::query::<sqlx::Sqlite>(
                        "INSERT INTO schemas (schema_uuid, table_uuid, schema_version, valid_from_transaction_id, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                    )
                    .bind(schema_uuid.as_bytes().as_slice())
                    .bind(table_uuid.as_bytes().as_slice())
                    .bind(schema_version)
                    .bind(transaction_id.as_bytes().as_slice())
                    .bind(transaction_timestamp)
                    .execute(tx.as_mut())
                    .await?;

                    sqlx::query::<sqlx::Sqlite>(
                        "UPDATE schemas SET valid_to_transaction_id = ?1 WHERE schema_uuid = ?2",
                    )
                    .bind(transaction_id.as_bytes().as_slice())
                    .bind(current_schema_uuid.as_bytes().as_slice())
                    .execute(tx.as_mut())
                    .await?;

                    let col_chunk_size =
                        limits::batch_insert_columns_chunk(database::DbKind::Sqlite);
                    debug_assert!(
                        limits::column_insert_bind_count(col_chunk_size)
                            <= limits::db_bind_limit(database::DbKind::Sqlite),
                        "column insert chunk exceeds SQLite bind parameter limit"
                    );
                    let mut row_data = Vec::with_capacity(col_chunk_size as usize);
                    for (chunk_index, col_chunk) in schema_spec
                        .columns
                        .chunks(col_chunk_size as usize)
                        .enumerate()
                    {
                        row_data.clear();
                        for (i, column) in col_chunk.iter().enumerate() {
                            let ordinal_position =
                                (chunk_index * col_chunk_size as usize + i + 1) as i32;
                            let encoded_type = encode_data_type(&column.column_type)?;
                            row_data.push((
                                uuid::Uuid::new_v4(),
                                column.name.clone(),
                                encoded_type,
                                ordinal_position,
                                column.is_nullable,
                            ));
                        }
                        let row_placeholders: String = (0..row_data.len())
                            .map(|_| "(?, ?, ?, ?, ?, ?)")
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                             VALUES {}",
                            row_placeholders
                        );
                        let mut query = sqlx::query::<sqlx::Sqlite>(&sql);
                        for (column_uuid, name, encoded_type, ordinal_position, is_nullable) in
                            &row_data
                        {
                            query = query
                                .bind(column_uuid.as_bytes().as_slice())
                                .bind(schema_uuid.as_bytes().as_slice())
                                .bind(name.as_str())
                                .bind(encoded_type.as_slice())
                                .bind(*ordinal_position)
                                .bind(*is_nullable);
                        }
                        query.execute(tx.as_mut()).await?;
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
                    if keys.len() as u32 > limits::MAX_PROPERTY_KEYS_TO_REMOVE {
                        return Err(CatalogError::LimitExceeded(format!(
                            "cannot remove {} property keys, exceeds limit of {}",
                            keys.len(),
                            limits::MAX_PROPERTY_KEYS_TO_REMOVE
                        )));
                    }
                    for key in keys {
                        properties_value.remove(key.as_str());
                    }
                }
            }
        }

        let schema_uuid_to_set = new_schema_uuid.or(current_schema_uuid);
        let properties_text = serialize_json(&properties_value.to_json())?;

        sqlx::query::<sqlx::Sqlite>(
            "UPDATE tables
             SET current_transaction_id = ?1, current_schema_uuid = ?2, properties = ?3
             WHERE table_uuid = ?4",
        )
        .bind(transaction_id.as_bytes().as_slice())
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
        properties: Option<TableProperties>,
    ) -> Result<TableHandle> {
        schema.validate()?;

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
        let properties_value = properties.unwrap_or_default();
        let properties_text = serialize_json(&properties_value.to_json())?;
        let min_reader_version = self.reader_version();
        let min_writer_version = self.writer_version();

        sqlx::query::<sqlx::Postgres>(
            "INSERT INTO tables (
                table_uuid, table_name, namespace, location, created_at, properties,
                min_reader_version, min_writer_version
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(ident.name.as_str())
        .bind(ident.namespace.as_str())
        .bind(location.as_str())
        .bind(created_at)
        .bind(properties_text.as_str())
        .bind(min_reader_version)
        .bind(min_writer_version)
        .execute(tx.as_mut())
        .await?;

        let transaction_id = next_transaction_id();
        sqlx::query::<sqlx::Postgres>(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp)
             VALUES ($1, $2, $3)",
        )
        .bind(transaction_id.as_bytes().as_slice())
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
        .bind(INITIAL_SCHEMA_VERSION)
        .bind(transaction_id.as_bytes().as_slice())
        .bind(created_at)
        .execute(tx.as_mut())
        .await?;

        // Chunk columns per statement so total bind parameters stay under DB limits.
        let col_chunk_size = limits::batch_insert_columns_chunk(database::DbKind::Postgres);
        debug_assert!(
            limits::column_insert_bind_count(col_chunk_size)
                <= limits::db_bind_limit(database::DbKind::Postgres),
            "column insert chunk exceeds Postgres bind parameter limit"
        );
        let mut row_data = Vec::with_capacity(col_chunk_size as usize);
        for (chunk_index, col_chunk) in schema.columns.chunks(col_chunk_size as usize).enumerate() {
            row_data.clear();
            for (i, column) in col_chunk.iter().enumerate() {
                let ordinal_position = (chunk_index * col_chunk_size as usize + i + 1) as i32;
                let encoded_type = encode_data_type(&column.column_type)?;
                row_data.push((
                    uuid::Uuid::new_v4(),
                    column.name.clone(),
                    encoded_type,
                    ordinal_position,
                    column.is_nullable,
                ));
            }
            let mut param = 1u32;
            let row_placeholders: String = (0..row_data.len())
                .map(|_| {
                    let s = format!(
                        "(${}, ${}, ${}, ${}, ${}, ${})",
                        param,
                        param + 1,
                        param + 2,
                        param + 3,
                        param + 4,
                        param + 5
                    );
                    param += 6;
                    s
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                 VALUES {}",
                row_placeholders
            );
            let mut query = sqlx::query::<sqlx::Postgres>(&sql);
            for (column_uuid, name, encoded_type, ordinal_position, is_nullable) in &row_data {
                query = query
                    .bind(column_uuid.as_bytes().as_slice())
                    .bind(schema_uuid.as_bytes().as_slice())
                    .bind(name.as_str())
                    .bind(encoded_type.as_slice())
                    .bind(*ordinal_position)
                    .bind(*is_nullable);
            }
            query.execute(tx.as_mut()).await?;
        }

        sqlx::query::<sqlx::Postgres>(
            "UPDATE tables
             SET current_schema_uuid = $1, current_transaction_id = $2
             WHERE table_uuid = $3",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .bind(transaction_id.as_bytes().as_slice())
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
        let list_limit = limits::MAX_TABLES_PER_LIST as i64 + 1;
        let rows = if let Some(namespace) = namespace {
            sqlx::query::<sqlx::Postgres>(
                "SELECT namespace, table_name FROM tables WHERE namespace = $1 LIMIT $2",
            )
            .bind(namespace)
            .bind(list_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Postgres>("SELECT namespace, table_name FROM tables LIMIT $1")
                .bind(list_limit)
                .fetch_all(&self.pool)
                .await?
        };

        if rows.len() > limits::MAX_TABLES_PER_LIST as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "table count exceeds limit of {}",
                limits::MAX_TABLES_PER_LIST
            )));
        }

        let capacity = std::cmp::min(rows.len(), limits::MAX_TABLES_PER_LIST as usize);
        let mut tables = Vec::with_capacity(capacity);
        for row in rows {
            let namespace: String = row.try_get("namespace")?;
            let name: String = row.try_get("table_name")?;
            tables.push(TableIdent { namespace, name });
        }

        Ok(tables)
    }

    async fn drop_table(&self, ident: &TableIdent) -> Result<()> {
        let affected = sqlx::query::<sqlx::Postgres>(
            "DELETE FROM tables WHERE namespace = $1 AND table_name = $2",
        )
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

    async fn current_transaction_id(&self, ident: &TableIdent) -> Result<TxnId> {
        let table_row = sqlx::query::<sqlx::Postgres>(
            "SELECT current_transaction_id
             FROM tables WHERE namespace = $1 AND table_name = $2",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(&self.pool)
        .await?;

        let table_row = table_row
            .ok_or_else(|| CatalogError::NotFound(format!("{}.{}", ident.namespace, ident.name)))?;
        let txn_id = uuid_from_row_optional(&table_row, "current_transaction_id")?;
        txn_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })
    }

    async fn list_transaction_events(
        &self,
        ident: &TableIdent,
        cursor: TxnRangeCursor,
    ) -> Result<Vec<TxnEvent>> {
        let table_row = sqlx::query::<sqlx::Postgres>(
            "SELECT table_uuid, current_transaction_id
             FROM tables WHERE namespace = $1 AND table_name = $2",
        )
        .bind(ident.namespace.as_str())
        .bind(ident.name.as_str())
        .fetch_optional(&self.pool)
        .await?;
        let table_row = table_row
            .ok_or_else(|| CatalogError::NotFound(format!("{}.{}", ident.namespace, ident.name)))?;
        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let current_transaction_id = uuid_from_row_optional(&table_row, "current_transaction_id")?
            .ok_or_else(|| {
                CatalogError::InvalidArgument("table has no current transaction".to_string())
            })?;

        if cursor.to_inclusive.as_u128() > current_transaction_id.as_u128() {
            return Err(CatalogError::InvalidArgument(
                "requested transaction is newer than current".to_string(),
            ));
        }
        if let Some(from_exclusive) = cursor.from_exclusive {
            if from_exclusive.as_u128() > cursor.to_inclusive.as_u128() {
                return Err(CatalogError::InvalidArgument(
                    "from transaction must be <= to transaction".to_string(),
                ));
            }
        }

        let txn_scan_limit = limits::MAX_TRANSACTIONS_PER_SCAN as i64 + 1;
        let txn_rows = if let Some(from_exclusive) = cursor.from_exclusive {
            sqlx::query::<sqlx::Postgres>(
                "SELECT transaction_id
                 FROM transactions
                 WHERE table_uuid = $1
                   AND transaction_id > $2
                   AND transaction_id <= $3
                 ORDER BY transaction_id
                 LIMIT $4",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(from_exclusive.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(txn_scan_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Postgres>(
                "SELECT transaction_id
                 FROM transactions
                 WHERE table_uuid = $1
                   AND transaction_id <= $2
                 ORDER BY transaction_id
                 LIMIT $3",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(txn_scan_limit)
            .fetch_all(&self.pool)
            .await?
        };

        if txn_rows.len() > limits::MAX_TRANSACTIONS_PER_SCAN as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "transaction scan exceeds limit of {}",
                limits::MAX_TRANSACTIONS_PER_SCAN
            )));
        }

        let mut transaction_ids = Vec::with_capacity(txn_rows.len());
        for row in txn_rows {
            transaction_ids.push(uuid_from_row(&row, "transaction_id")?);
        }
        let mut index_by_txn = std::collections::HashMap::with_capacity(transaction_ids.len());
        let mut events = Vec::with_capacity(transaction_ids.len());
        for (index, transaction_id) in transaction_ids.into_iter().enumerate() {
            index_by_txn.insert(transaction_id, index);
            events.push(TxnEvent {
                transaction_id,
                file_changes: Vec::new(),
                schema_change: None,
            });
        }

        // Sentinel-row pattern: fetch one extra row to detect truncation.
        // If we get MAX+1 rows, we fail with LimitExceeded instead of silently dropping events.
        let added_scan_limit = limits::MAX_FILES_PER_QUERY as i64 + 1;
        let added_rows = if let Some(from_exclusive) = cursor.from_exclusive {
            sqlx::query::<sqlx::Postgres>(
                "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                        added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 FROM files
                 WHERE table_uuid = $1
                   AND added_in_transaction_id > $2
                   AND added_in_transaction_id <= $3
                 LIMIT $4",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(from_exclusive.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(added_scan_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Postgres>(
                "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                        added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 FROM files
                 WHERE table_uuid = $1
                   AND added_in_transaction_id <= $2
                 LIMIT $3",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(added_scan_limit)
            .fetch_all(&self.pool)
            .await?
        };
        if added_rows.len() > limits::MAX_FILES_PER_QUERY as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "added file events exceed limit of {}",
                limits::MAX_FILES_PER_QUERY
            )));
        }

        for row in added_rows {
            let partition_values: Option<String> = row.try_get("partition_values")?;
            let format_options: Option<String> = row.try_get("format_options")?;
            let added_in_transaction_id = uuid_from_row(&row, "added_in_transaction_id")?;
            if let Some(index) = index_by_txn.get(&added_in_transaction_id) {
                events[*index].file_changes.push(TxnFileChange {
                    transaction_id: added_in_transaction_id,
                    kind: TxnFileChangeKind::Added,
                    file: schema::File {
                        file_uuid: uuid_from_row(&row, "file_uuid")?,
                        table_uuid: uuid_from_row(&row, "table_uuid")?,
                        file_format: row.try_get("file_format")?,
                        file_path: row.try_get("file_path")?,
                        record_count: row.try_get("record_count")?,
                        file_size_bytes: row.try_get("file_size_bytes")?,
                        added_in_transaction_id,
                        removed_in_transaction_id: uuid_from_row_optional(
                            &row,
                            "removed_in_transaction_id",
                        )?,
                        partition_values: parse_json_optional(partition_values)?,
                        format_options: parse_json_optional(format_options)?,
                    },
                });
            }
        }

        let removed_scan_limit = limits::MAX_FILES_PER_QUERY as i64 + 1;
        let removed_rows = if let Some(from_exclusive) = cursor.from_exclusive {
            sqlx::query::<sqlx::Postgres>(
                "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                        added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 FROM files
                 WHERE table_uuid = $1
                   AND removed_in_transaction_id IS NOT NULL
                   AND removed_in_transaction_id > $2
                   AND removed_in_transaction_id <= $3
                 LIMIT $4",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(from_exclusive.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(removed_scan_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Postgres>(
                "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                        added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 FROM files
                 WHERE table_uuid = $1
                   AND removed_in_transaction_id IS NOT NULL
                   AND removed_in_transaction_id <= $2
                 LIMIT $3",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(removed_scan_limit)
            .fetch_all(&self.pool)
            .await?
        };
        if removed_rows.len() > limits::MAX_FILES_PER_QUERY as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "removed file events exceed limit of {}",
                limits::MAX_FILES_PER_QUERY
            )));
        }

        for row in removed_rows {
            let partition_values: Option<String> = row.try_get("partition_values")?;
            let format_options: Option<String> = row.try_get("format_options")?;
            let removed_in_transaction_id =
                uuid_from_row_optional(&row, "removed_in_transaction_id")?.ok_or_else(|| {
                    CatalogError::InvalidArgument("missing removed transaction id".to_string())
                })?;
            if let Some(index) = index_by_txn.get(&removed_in_transaction_id) {
                events[*index].file_changes.push(TxnFileChange {
                    transaction_id: removed_in_transaction_id,
                    kind: TxnFileChangeKind::Removed,
                    file: schema::File {
                        file_uuid: uuid_from_row(&row, "file_uuid")?,
                        table_uuid: uuid_from_row(&row, "table_uuid")?,
                        file_format: row.try_get("file_format")?,
                        file_path: row.try_get("file_path")?,
                        record_count: row.try_get("record_count")?,
                        file_size_bytes: row.try_get("file_size_bytes")?,
                        added_in_transaction_id: uuid_from_row(&row, "added_in_transaction_id")?,
                        removed_in_transaction_id: Some(removed_in_transaction_id),
                        partition_values: parse_json_optional(partition_values)?,
                        format_options: parse_json_optional(format_options)?,
                    },
                });
            }
        }

        let schema_scan_limit = limits::MAX_COLUMNS_PER_SCHEMA as i64 + 1;
        let schema_rows = if let Some(from_exclusive) = cursor.from_exclusive {
            sqlx::query::<sqlx::Postgres>(
                "SELECT schema_uuid, table_uuid, schema_version, valid_from_transaction_id, valid_to_transaction_id, created_at
                 FROM schemas
                 WHERE table_uuid = $1
                   AND valid_from_transaction_id > $2
                   AND valid_from_transaction_id <= $3
                 LIMIT $4",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(from_exclusive.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(schema_scan_limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query::<sqlx::Postgres>(
                "SELECT schema_uuid, table_uuid, schema_version, valid_from_transaction_id, valid_to_transaction_id, created_at
                 FROM schemas
                 WHERE table_uuid = $1
                   AND valid_from_transaction_id <= $2
                 LIMIT $3",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(cursor.to_inclusive.as_bytes().as_slice())
            .bind(schema_scan_limit)
            .fetch_all(&self.pool)
            .await?
        };
        if schema_rows.len() > limits::MAX_COLUMNS_PER_SCHEMA as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "schema change events exceed limit of {}",
                limits::MAX_COLUMNS_PER_SCHEMA
            )));
        }

        for schema_row in schema_rows {
            let schema_uuid = uuid_from_row(&schema_row, "schema_uuid")?;
            let valid_from_transaction_id =
                uuid_from_row(&schema_row, "valid_from_transaction_id")?;
            let column_rows = sqlx::query::<sqlx::Postgres>(
                "SELECT column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable
                 FROM columns WHERE schema_uuid = $1 ORDER BY ordinal_position LIMIT $2",
            )
            .bind(schema_uuid.as_bytes().as_slice())
            .bind(limits::MAX_COLUMNS_PER_SCHEMA as i64)
            .fetch_all(&self.pool)
            .await?;

            let mut columns = Vec::with_capacity(column_rows.len());
            for row in column_rows {
                let column_type_bytes: Vec<u8> = row.try_get("column_type")?;
                let column_type = decode_data_type(&column_type_bytes)?;
                columns.push(schema::Column {
                    column_uuid: uuid_from_row(&row, "column_uuid")?,
                    schema_uuid: uuid_from_row(&row, "schema_uuid")?,
                    column_name: row.try_get("column_name")?,
                    column_type,
                    ordinal_position: row.try_get("ordinal_position")?,
                    is_nullable: row.try_get("is_nullable")?,
                });
            }

            if let Some(index) = index_by_txn.get(&valid_from_transaction_id) {
                events[*index].schema_change = Some(TxnSchemaChange {
                    transaction_id: valid_from_transaction_id,
                    schema: schema::Schema {
                        schema_uuid,
                        table_uuid: uuid_from_row(&schema_row, "table_uuid")?,
                        schema_version: schema_row.try_get("schema_version")?,
                        valid_from_transaction_id,
                        valid_to_transaction_id: uuid_from_row_optional(
                            &schema_row,
                            "valid_to_transaction_id",
                        )?,
                        created_at: schema_row.try_get("created_at")?,
                        columns,
                    },
                });
            }
        }

        Ok(events)
    }

    async fn read_table(
        &self,
        ident: &TableIdent,
        at_transaction_id: Option<TxnId>,
    ) -> Result<TableView> {
        let table_row = sqlx::query::<sqlx::Postgres>(
            "SELECT table_uuid, current_schema_uuid, current_transaction_id, properties,
                    min_reader_version, min_writer_version
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
                )));
            }
        };

        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let _current_schema_uuid = uuid_from_row_optional(&table_row, "current_schema_uuid")?;
        let current_transaction_id = uuid_from_row_optional(&table_row, "current_transaction_id")?;
        let min_reader_version: i32 = table_row.try_get("min_reader_version")?;
        let properties = parse_table_properties(table_row.try_get("properties")?)?;
        self.ensure_reader_protocol_compatible(ident, min_reader_version)?;

        let current_transaction_id = current_transaction_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })?;
        let effective_transaction_id = at_transaction_id.unwrap_or(current_transaction_id);
        // UUIDv7 is time-ordered, compare using u128 representation
        if effective_transaction_id.as_u128() > current_transaction_id.as_u128() {
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
        .bind(effective_transaction_id.as_bytes().as_slice())
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
             FROM columns WHERE schema_uuid = $1 ORDER BY ordinal_position LIMIT $2",
        )
        .bind(schema_uuid.as_bytes().as_slice())
        .bind(limits::MAX_COLUMNS_PER_SCHEMA as i64 + 1)
        .fetch_all(&self.pool)
        .await?;

        if column_rows.len() > limits::MAX_COLUMNS_PER_SCHEMA as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "column count exceeds limit of {}",
                limits::MAX_COLUMNS_PER_SCHEMA
            )));
        }

        let capacity = std::cmp::min(column_rows.len(), limits::MAX_COLUMNS_PER_SCHEMA as usize);
        let mut columns = Vec::with_capacity(capacity);
        for row in column_rows {
            let column_type_bytes: Vec<u8> = row.try_get("column_type")?;
            let column_type = decode_data_type(&column_type_bytes)?;

            let column = schema::Column {
                column_uuid: uuid_from_row(&row, "column_uuid")?,
                schema_uuid: uuid_from_row(&row, "schema_uuid")?,
                column_name: row.try_get("column_name")?,
                column_type,
                ordinal_position: row.try_get("ordinal_position")?,
                is_nullable: row.try_get("is_nullable")?,
            };
            columns.push(column);
        }

        let schema = schema::Schema {
            schema_uuid,
            table_uuid: uuid_from_row(&schema_row, "table_uuid")?,
            schema_version: schema_row.try_get("schema_version")?,
            valid_from_transaction_id: uuid_from_row(&schema_row, "valid_from_transaction_id")?,
            valid_to_transaction_id: uuid_from_row_optional(
                &schema_row,
                "valid_to_transaction_id",
            )?,
            created_at: schema_row.try_get("created_at")?,
            columns,
        };

        let file_rows = sqlx::query::<sqlx::Postgres>(
            "SELECT file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                    added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
             FROM files
             WHERE table_uuid = $1
               AND added_in_transaction_id <= $2
               AND (removed_in_transaction_id IS NULL OR removed_in_transaction_id > $2)
             LIMIT $3",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id.as_bytes().as_slice())
        .bind(limits::MAX_FILES_PER_QUERY as i64 + 1)
        .fetch_all(&self.pool)
        .await?;

        if file_rows.len() > limits::MAX_FILES_PER_QUERY as usize {
            return Err(CatalogError::LimitExceeded(format!(
                "file count exceeds limit of {}",
                limits::MAX_FILES_PER_QUERY
            )));
        }

        let capacity = std::cmp::min(file_rows.len(), limits::MAX_FILES_PER_QUERY as usize);
        let mut files = Vec::with_capacity(capacity);
        for row in file_rows {
            let partition_values: Option<String> = row.try_get("partition_values")?;
            let format_options: Option<String> = row.try_get("format_options")?;
            let file = schema::File {
                file_uuid: uuid_from_row(&row, "file_uuid")?,
                table_uuid: uuid_from_row(&row, "table_uuid")?,
                file_format: row.try_get("file_format")?,
                file_path: row.try_get("file_path")?,
                record_count: row.try_get("record_count")?,
                file_size_bytes: row.try_get("file_size_bytes")?,
                added_in_transaction_id: uuid_from_row(&row, "added_in_transaction_id")?,
                removed_in_transaction_id: uuid_from_row_optional(
                    &row,
                    "removed_in_transaction_id",
                )?,
                partition_values: parse_json_optional(partition_values)?,
                format_options: parse_json_optional(format_options)?,
            };
            files.push(file);
        }

        let stats_row = sqlx::query::<sqlx::Postgres>(
            "SELECT table_uuid, transaction_id, record_count, file_size_bytes, file_count, last_updated
             FROM table_stats WHERE table_uuid = $1 AND transaction_id = $2",
        )
        .bind(table_uuid.as_bytes().as_slice())
        .bind(effective_transaction_id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await?;

        let stats = if let Some(row) = stats_row {
            Some(schema::TableStats {
                table_uuid: uuid_from_row(&row, "table_uuid")?,
                transaction_id: uuid_from_row(&row, "transaction_id")?,
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
        let events = self
            .list_transaction_events(
                ident,
                TxnRangeCursor {
                    from_exclusive: Some(from_transaction_id),
                    to_inclusive: to_transaction_id,
                },
            )
            .await?;

        let from_view = self.read_table(ident, Some(from_transaction_id)).await?;
        let to_view = self.read_table(ident, Some(to_transaction_id)).await?;
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

        Ok(project_delta_range(
            from_transaction_id,
            to_transaction_id,
            &events,
            new_schema,
            new_properties,
        ))
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

        if mutation.operations.len() as u32 > limits::MAX_OPERATIONS_PER_MUTATION {
            return Err(CatalogError::LimitExceeded(format!(
                "mutation has {} operations, exceeds limit of {}",
                mutation.operations.len(),
                limits::MAX_OPERATIONS_PER_MUTATION
            )));
        }
        mutation.validate()?;

        let mut tx = self.pool.begin().await?;

        let table_row = sqlx::query::<sqlx::Postgres>(
            "SELECT table_uuid, current_schema_uuid, current_transaction_id, properties,
                    min_reader_version, min_writer_version
             FROM tables WHERE namespace = $1 AND table_name = $2
             FOR UPDATE",
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
                )));
            }
        };

        let table_uuid = uuid_from_row(&table_row, "table_uuid")?;
        let current_schema_uuid = uuid_from_row_optional(&table_row, "current_schema_uuid")?;
        let current_transaction_id = uuid_from_row_optional(&table_row, "current_transaction_id")?;
        let min_writer_version: i32 = table_row.try_get("min_writer_version")?;
        let mut properties_value = parse_table_properties(table_row.try_get("properties")?)?;
        self.ensure_writer_protocol_compatible(ident, min_writer_version)?;

        let current_transaction_id = current_transaction_id.ok_or_else(|| {
            CatalogError::InvalidArgument("table has no current transaction".to_string())
        })?;

        if current_transaction_id != base_transaction_id {
            return Err(CatalogError::Conflict(format!(
                "base transaction {} does not match current {}",
                base_transaction_id, current_transaction_id
            )));
        }

        let transaction_id = next_transaction_id();
        let transaction_timestamp = chrono::Utc::now();

        sqlx::query::<sqlx::Postgres>(
            "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp, parent_transaction_id)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(transaction_id.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .bind(transaction_timestamp)
        .bind(current_transaction_id.as_bytes().as_slice())
        .execute(tx.as_mut())
        .await?;

        let mut new_schema_uuid = current_schema_uuid;

        for op in mutation.operations {
            match op {
                MutationOp::AppendFiles(files) => {
                    if files.len() as u32 > limits::MAX_FILES_PER_APPEND {
                        return Err(CatalogError::LimitExceeded(format!(
                            "cannot append {} files, exceeds limit of {}",
                            files.len(),
                            limits::MAX_FILES_PER_APPEND
                        )));
                    }
                    // Chunk file inserts to keep per-statement bind count bounded.
                    let chunk_size = limits::BATCH_INSERT_FILES_CHUNK;
                    debug_assert!(
                        limits::file_insert_bind_count(chunk_size)
                            <= limits::db_bind_limit(database::DbKind::Postgres),
                        "file insert chunk exceeds Postgres bind parameter limit"
                    );
                    let mut row_data = Vec::with_capacity(chunk_size as usize);
                    for chunk in files.chunks(chunk_size as usize) {
                        row_data.clear();
                        for f in chunk {
                            f.validate()?;
                            let file_uuid = f.file_uuid.unwrap_or_else(uuid::Uuid::new_v4);
                            let partition_text =
                                serialize_json_optional(f.partition_values.as_ref())?;
                            let format_text = serialize_json_optional(f.format_options.as_ref())?;
                            row_data.push((
                                file_uuid,
                                f.file_format.as_str(),
                                f.file_path.clone(),
                                f.record_count,
                                f.file_size_bytes,
                                partition_text,
                                format_text,
                            ));
                        }
                        let mut param = 1u32;
                        let row_placeholders: String = (0..row_data.len())
                            .map(|_| {
                                let s = format!(
                                    "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                                    param,
                                    param + 1,
                                    param + 2,
                                    param + 3,
                                    param + 4,
                                    param + 5,
                                    param + 6,
                                    param + 7,
                                    param + 8
                                );
                                param += 9;
                                s
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            "INSERT INTO files (file_uuid, table_uuid, file_format, file_path, record_count,
                                                 file_size_bytes, added_in_transaction_id, partition_values, format_options)
                             VALUES {}",
                            row_placeholders
                        );
                        let mut query = sqlx::query::<sqlx::Postgres>(&sql);
                        for (file_uuid, format, path, rc, size, part, format_opts) in &row_data {
                            query = query
                                .bind(file_uuid.as_bytes().as_slice())
                                .bind(table_uuid.as_bytes().as_slice())
                                .bind(*format)
                                .bind(path.as_str())
                                .bind(*rc)
                                .bind(*size)
                                .bind(transaction_id.as_bytes().as_slice())
                                .bind(part.as_deref())
                                .bind(format_opts.as_deref());
                        }
                        query.execute(tx.as_mut()).await?;
                    }
                }
                MutationOp::DeleteFiles(file_uuids) => {
                    if file_uuids.len() as u32 > limits::MAX_FILES_PER_DELETE {
                        return Err(CatalogError::LimitExceeded(format!(
                            "cannot delete {} files, exceeds limit of {}",
                            file_uuids.len(),
                            limits::MAX_FILES_PER_DELETE
                        )));
                    }
                    // Chunk deletes so `IN (...)` stays under DB bind parameter limits.
                    let chunk_size = limits::BATCH_DELETE_FILES_CHUNK;
                    debug_assert!(
                        limits::delete_files_bind_count(chunk_size)
                            <= limits::db_bind_limit(database::DbKind::Postgres),
                        "file delete chunk exceeds Postgres bind parameter limit"
                    );
                    for chunk in file_uuids.chunks(chunk_size as usize) {
                        let in_placeholders: String = (1..=chunk.len())
                            .map(|i| format!("${}", i + 2))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            "UPDATE files SET removed_in_transaction_id = $1
                             WHERE table_uuid = $2 AND removed_in_transaction_id IS NULL AND file_uuid IN ({})",
                            in_placeholders
                        );
                        let mut query = sqlx::query::<sqlx::Postgres>(&sql)
                            .bind(transaction_id.as_bytes().as_slice())
                            .bind(table_uuid.as_bytes().as_slice());
                        for file_uuid in chunk {
                            query = query.bind(file_uuid.as_bytes().as_slice());
                        }
                        query.execute(tx.as_mut()).await?;
                    }
                }
                MutationOp::UpdateSchema(schema_spec) => {
                    schema_spec.validate()?;

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

                    // Fetch current schema columns for validation
                    let current_column_rows = sqlx::query::<sqlx::Postgres>(
                        "SELECT column_name, column_type, is_nullable FROM columns WHERE schema_uuid = $1",
                    )
                    .bind(current_schema_uuid.as_bytes().as_slice())
                    .fetch_all(tx.as_mut())
                    .await?;

                    // Build map of current column names to (DataType, nullability)
                    let mut current_columns = std::collections::HashMap::new();
                    for row in current_column_rows {
                        let column_name: String = row.try_get("column_name")?;
                        let column_type_bytes: Vec<u8> = row.try_get("column_type")?;
                        let column_type = decode_data_type(&column_type_bytes)?;
                        let is_nullable: bool = row.try_get("is_nullable")?;
                        current_columns.insert(column_name, (column_type, is_nullable));
                    }

                    let new_column_names = schema_spec
                        .columns
                        .iter()
                        .map(|column| column.name.as_str())
                        .collect::<std::collections::HashSet<_>>();
                    for existing_column_name in current_columns.keys() {
                        if !new_column_names.contains(existing_column_name.as_str()) {
                            return Err(CatalogError::InvalidArgument(format!(
                                "cannot drop existing column '{}' via UpdateSchema",
                                existing_column_name
                            )));
                        }
                    }

                    // Validate schema evolution for each column in new schema
                    for new_column in &schema_spec.columns {
                        if let Some((old_type, old_nullable)) =
                            current_columns.get(&new_column.name)
                        {
                            // Column exists - validate evolution

                            // Check type evolution
                            if !can_evolve_to(old_type, &new_column.column_type) {
                                return Err(CatalogError::InvalidArgument(format!(
                                    "invalid schema evolution for column '{}': cannot evolve {:?} to {:?}",
                                    new_column.name, old_type, new_column.column_type
                                )));
                            }

                            // Check nullability evolution
                            // Making nullable -> non-nullable is unsafe (existing nulls would violate constraint)
                            if *old_nullable && !new_column.is_nullable {
                                return Err(CatalogError::InvalidArgument(format!(
                                    "invalid schema evolution for column '{}': cannot change from nullable to non-nullable (existing nulls would violate constraint)",
                                    new_column.name
                                )));
                            }
                        }
                        // New columns are always allowed (they default to null for existing data)
                    }

                    let schema_uuid = uuid::Uuid::new_v4();
                    let schema_version = current_schema_version + 1;

                    sqlx::query::<sqlx::Postgres>(
                        "INSERT INTO schemas (schema_uuid, table_uuid, schema_version, valid_from_transaction_id, created_at)
                         VALUES ($1, $2, $3, $4, $5)",
                    )
                    .bind(schema_uuid.as_bytes().as_slice())
                    .bind(table_uuid.as_bytes().as_slice())
                    .bind(schema_version)
                    .bind(transaction_id.as_bytes().as_slice())
                    .bind(transaction_timestamp)
                    .execute(tx.as_mut())
                    .await?;

                    sqlx::query::<sqlx::Postgres>(
                        "UPDATE schemas SET valid_to_transaction_id = $1 WHERE schema_uuid = $2",
                    )
                    .bind(transaction_id.as_bytes().as_slice())
                    .bind(current_schema_uuid.as_bytes().as_slice())
                    .execute(tx.as_mut())
                    .await?;

                    let col_chunk_size =
                        limits::batch_insert_columns_chunk(database::DbKind::Postgres);
                    debug_assert!(
                        limits::column_insert_bind_count(col_chunk_size)
                            <= limits::db_bind_limit(database::DbKind::Postgres),
                        "column insert chunk exceeds Postgres bind parameter limit"
                    );
                    let mut row_data = Vec::with_capacity(col_chunk_size as usize);
                    for (chunk_index, col_chunk) in schema_spec
                        .columns
                        .chunks(col_chunk_size as usize)
                        .enumerate()
                    {
                        row_data.clear();
                        for (i, column) in col_chunk.iter().enumerate() {
                            let ordinal_position =
                                (chunk_index * col_chunk_size as usize + i + 1) as i32;
                            let encoded_type = encode_data_type(&column.column_type)?;
                            row_data.push((
                                uuid::Uuid::new_v4(),
                                column.name.clone(),
                                encoded_type,
                                ordinal_position,
                                column.is_nullable,
                            ));
                        }
                        let mut param = 1u32;
                        let row_placeholders: String = (0..row_data.len())
                            .map(|_| {
                                let s = format!(
                                    "(${}, ${}, ${}, ${}, ${}, ${})",
                                    param,
                                    param + 1,
                                    param + 2,
                                    param + 3,
                                    param + 4,
                                    param + 5
                                );
                                param += 6;
                                s
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                             VALUES {}",
                            row_placeholders
                        );
                        let mut query = sqlx::query::<sqlx::Postgres>(&sql);
                        for (column_uuid, name, encoded_type, ordinal_position, is_nullable) in
                            &row_data
                        {
                            query = query
                                .bind(column_uuid.as_bytes().as_slice())
                                .bind(schema_uuid.as_bytes().as_slice())
                                .bind(name.as_str())
                                .bind(encoded_type.as_slice())
                                .bind(*ordinal_position)
                                .bind(*is_nullable);
                        }
                        query.execute(tx.as_mut()).await?;
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
                    if keys.len() as u32 > limits::MAX_PROPERTY_KEYS_TO_REMOVE {
                        return Err(CatalogError::LimitExceeded(format!(
                            "cannot remove {} property keys, exceeds limit of {}",
                            keys.len(),
                            limits::MAX_PROPERTY_KEYS_TO_REMOVE
                        )));
                    }
                    for key in keys {
                        properties_value.remove(key.as_str());
                    }
                }
            }
        }

        let schema_uuid_to_set = new_schema_uuid.or(current_schema_uuid);
        let properties_text = serialize_json(&properties_value.to_json())?;

        sqlx::query::<sqlx::Postgres>(
            "UPDATE tables
             SET current_transaction_id = $1, current_schema_uuid = $2, properties = $3
             WHERE table_uuid = $4",
        )
        .bind(transaction_id.as_bytes().as_slice())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Format;
    use arrow::datatypes::DataType;

    async fn create_sqlite_catalog_with_table() -> (Arc<SqlCatalog<sqlx::Sqlite>>, TableIdent, TableView) {
        let catalog = SqlCatalog::in_memory()
            .await
            .expect("in-memory catalog should initialize");
        let ident = TableIdent::new("test_ns", "test_table");
        catalog
            .clone()
            .create_table(
                ident.clone(),
                "memory://test".to_string(),
                SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int64)),
                None,
            )
            .await
            .expect("table creation should succeed");
        let view = catalog
            .read_table(&ident, None)
            .await
            .expect("table should be readable");
        (catalog, ident, view)
    }

    #[test]
    fn schema_spec_validate_rejects_duplicate_column_names() {
        let schema = SchemaSpec::new()
            .with_column(ColumnSpec::new("id", DataType::Int64))
            .with_column(ColumnSpec::new("id", DataType::Int64));
        let err = schema.validate().expect_err("schema should be invalid");
        assert!(err.to_string().contains("duplicate column name"));
    }

    #[test]
    fn schema_spec_validate_rejects_empty_column_name() {
        let schema = SchemaSpec::new().with_column(ColumnSpec::new("   ", DataType::Int64));
        let err = schema.validate().expect_err("schema should be invalid");
        assert!(err.to_string().contains("column name cannot be empty"));
    }

    #[test]
    fn schema_spec_validate_accepts_valid_schema() {
        let schema = SchemaSpec::new()
            .with_column(ColumnSpec::new("id", DataType::Int64))
            .with_column(ColumnSpec::new("value", DataType::Utf8).nullable());
        schema.validate().expect("schema should be valid");
    }

    #[test]
    fn file_spec_validate_rejects_non_object_partition_values() {
        let file = FileSpec::new(Format::Parquet, "/tmp/part-0.parquet", 1, 1)
            .with_partition_values(serde_json::json!(["not-object"]));
        let err = file.validate().expect_err("file spec should be invalid");
        assert!(
            err.to_string()
                .contains("partition_values must be a JSON object")
        );
    }

    #[test]
    fn file_spec_validate_rejects_non_object_format_options() {
        let file = FileSpec::new(Format::Parquet, "/tmp/part-0.parquet", 1, 1)
            .with_format_options(serde_json::json!(["not-object"]));
        let err = file.validate().expect_err("file spec should be invalid");
        assert!(
            err.to_string()
                .contains("format_options must be a JSON object")
        );
    }

    #[test]
    fn file_spec_validate_accepts_valid_payload() {
        let file = FileSpec::new(Format::Parquet, "/tmp/part-0.parquet", 1, 1)
            .with_partition_values(serde_json::json!({"date": "2026-02-12"}))
            .with_format_options(serde_json::json!({"compression": "zstd"}));
        file.validate().expect("file spec should be valid");
    }

    #[test]
    fn property_key_rejects_empty_value() {
        let err = PropertyKey::new("   ").expect_err("key should be invalid");
        assert!(err.to_string().contains("property key cannot be empty"));
    }

    #[test]
    fn property_key_accepts_non_empty_value() {
        let key = PropertyKey::new("owner").expect("key should be valid");
        assert_eq!(key.as_str(), "owner");
    }

    #[test]
    fn mutation_validate_rejects_duplicate_file_deletes() {
        let file_uuid = uuid::Uuid::new_v4();
        let mutation = Mutation {
            operations: vec![MutationOp::DeleteFiles(vec![file_uuid, file_uuid])],
        };
        let err = mutation.validate().expect_err("mutation should be invalid");
        assert!(err.to_string().contains("duplicate file UUID"));
    }

    #[test]
    fn mutation_validate_rejects_duplicate_property_keys() {
        let key1 = PropertyKey::new("owner").expect("valid");
        let key2 = PropertyKey::new("owner").expect("valid");
        let mutation = Mutation {
            operations: vec![MutationOp::RemoveProperties(vec![key1, key2])],
        };
        let err = mutation.validate().expect_err("mutation should be invalid");
        assert!(err.to_string().contains("duplicate property key"));
    }

    #[test]
    fn mutation_validate_rejects_multiple_schema_updates() {
        let s1 = SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int64));
        let s2 = SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int64));
        let mutation = Mutation {
            operations: vec![MutationOp::UpdateSchema(s1), MutationOp::UpdateSchema(s2)],
        };
        let err = mutation.validate().expect_err("mutation should be invalid");
        assert!(err.to_string().contains("at most one UpdateSchema"));
    }

    #[tokio::test]
    async fn list_transaction_events_rejects_added_file_overflow() {
        let (catalog, ident, table_view) = create_sqlite_catalog_with_table().await;
        let table_uuid = table_view.table_uuid;
        let base_transaction_id = table_view.transaction_id;

        let overflow_count = limits::MAX_FILES_PER_QUERY as usize + 1;
        let mut last_transaction_id = base_transaction_id;
        for i in 0..overflow_count {
            let transaction_id = next_transaction_id();
            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp, parent_transaction_id)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(transaction_id.as_bytes().as_slice())
            .bind(table_uuid.as_bytes().as_slice())
            .bind(chrono::Utc::now())
            .bind(last_transaction_id.as_bytes().as_slice())
            .execute(&catalog.pool)
            .await
            .expect("transaction insert should succeed");

            let file_uuid = uuid::Uuid::new_v4();
            let file_path = format!("/tmp/overflow-added-{i}.parquet");
            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO files (
                    file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                    added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )
            .bind(file_uuid.as_bytes().as_slice())
            .bind(table_uuid.as_bytes().as_slice())
            .bind("parquet")
            .bind(file_path)
            .bind(1_i64)
            .bind(1_i64)
            .bind(transaction_id.as_bytes().as_slice())
            .bind(Option::<Vec<u8>>::None)
            .bind(Option::<String>::None)
            .bind(Option::<String>::None)
            .execute(&catalog.pool)
            .await
            .expect("file insert should succeed");

            last_transaction_id = transaction_id;
        }

        sqlx::query::<sqlx::Sqlite>(
            "UPDATE tables SET current_transaction_id = ?1 WHERE table_uuid = ?2",
        )
        .bind(last_transaction_id.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .execute(&catalog.pool)
        .await
        .expect("table head update should succeed");

        let err = catalog
            .list_transaction_events(
                &ident,
                TxnRangeCursor {
                    from_exclusive: Some(base_transaction_id),
                    to_inclusive: last_transaction_id,
                },
            )
            .await
            .expect_err("added-file overflow should fail");
        assert!(matches!(err, CatalogError::LimitExceeded(_)));
        assert!(err.to_string().contains("added file events exceed limit"));
    }

    #[tokio::test]
    async fn list_transaction_events_rejects_removed_file_overflow() {
        let (catalog, ident, table_view) = create_sqlite_catalog_with_table().await;
        let table_uuid = table_view.table_uuid;
        let base_transaction_id = table_view.transaction_id;

        let overflow_count = limits::MAX_FILES_PER_QUERY as usize + 1;
        let mut last_transaction_id = base_transaction_id;
        for i in 0..overflow_count {
            let transaction_id = next_transaction_id();
            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp, parent_transaction_id)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(transaction_id.as_bytes().as_slice())
            .bind(table_uuid.as_bytes().as_slice())
            .bind(chrono::Utc::now())
            .bind(last_transaction_id.as_bytes().as_slice())
            .execute(&catalog.pool)
            .await
            .expect("transaction insert should succeed");

            let file_uuid = uuid::Uuid::new_v4();
            let file_path = format!("/tmp/overflow-removed-{i}.parquet");
            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO files (
                    file_uuid, table_uuid, file_format, file_path, record_count, file_size_bytes,
                    added_in_transaction_id, removed_in_transaction_id, partition_values, format_options
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )
            .bind(file_uuid.as_bytes().as_slice())
            .bind(table_uuid.as_bytes().as_slice())
            .bind("parquet")
            .bind(file_path)
            .bind(1_i64)
            .bind(1_i64)
            .bind(base_transaction_id.as_bytes().as_slice())
            .bind(transaction_id.as_bytes().as_slice())
            .bind(Option::<String>::None)
            .bind(Option::<String>::None)
            .execute(&catalog.pool)
            .await
            .expect("file insert should succeed");

            last_transaction_id = transaction_id;
        }

        sqlx::query::<sqlx::Sqlite>(
            "UPDATE tables SET current_transaction_id = ?1 WHERE table_uuid = ?2",
        )
        .bind(last_transaction_id.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .execute(&catalog.pool)
        .await
        .expect("table head update should succeed");

        let err = catalog
            .list_transaction_events(
                &ident,
                TxnRangeCursor {
                    from_exclusive: Some(base_transaction_id),
                    to_inclusive: last_transaction_id,
                },
            )
            .await
            .expect_err("removed-file overflow should fail");
        assert!(matches!(err, CatalogError::LimitExceeded(_)));
        assert!(err.to_string().contains("removed file events exceed limit"));
    }

    #[tokio::test]
    async fn list_transaction_events_rejects_schema_change_overflow() {
        let (catalog, ident, table_view) = create_sqlite_catalog_with_table().await;
        let table_uuid = table_view.table_uuid;
        let base_transaction_id = table_view.transaction_id;

        let overflow_count = limits::MAX_COLUMNS_PER_SCHEMA as usize + 1;
        let mut last_transaction_id = base_transaction_id;
        for i in 0..overflow_count {
            let transaction_id = next_transaction_id();
            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO transactions (transaction_id, table_uuid, transaction_timestamp, parent_transaction_id)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(transaction_id.as_bytes().as_slice())
            .bind(table_uuid.as_bytes().as_slice())
            .bind(chrono::Utc::now())
            .bind(last_transaction_id.as_bytes().as_slice())
            .execute(&catalog.pool)
            .await
            .expect("transaction insert should succeed");

            let schema_uuid = uuid::Uuid::new_v4();
            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO schemas (
                    schema_uuid, table_uuid, schema_version, valid_from_transaction_id, valid_to_transaction_id, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(schema_uuid.as_bytes().as_slice())
            .bind(table_uuid.as_bytes().as_slice())
            .bind((i as i32) + 100)
            .bind(transaction_id.as_bytes().as_slice())
            .bind(Option::<Vec<u8>>::None)
            .bind(chrono::Utc::now())
            .execute(&catalog.pool)
            .await
            .expect("schema insert should succeed");

            last_transaction_id = transaction_id;
        }

        sqlx::query::<sqlx::Sqlite>(
            "UPDATE tables SET current_transaction_id = ?1 WHERE table_uuid = ?2",
        )
        .bind(last_transaction_id.as_bytes().as_slice())
        .bind(table_uuid.as_bytes().as_slice())
        .execute(&catalog.pool)
        .await
        .expect("table head update should succeed");

        let err = catalog
            .list_transaction_events(
                &ident,
                TxnRangeCursor {
                    from_exclusive: Some(base_transaction_id),
                    to_inclusive: last_transaction_id,
                },
            )
            .await
            .expect_err("schema overflow should fail");
        assert!(matches!(err, CatalogError::LimitExceeded(_)));
        assert!(err.to_string().contains("schema change events exceed limit"));
    }

    #[tokio::test]
    async fn schema_update_rejects_implicit_column_drop() {
        let (catalog, ident, table_view) = create_sqlite_catalog_with_table().await;
        let handle = TableHandle::new(catalog.clone(), ident.clone());

        let base_txn = table_view.transaction_id;
        let create_builder = handle
            .write(Some(base_txn))
            .await
            .expect("builder should be created");
        create_builder
            .update_schema(
                SchemaSpec::new()
                    .with_column(ColumnSpec::new("id", DataType::Int64))
                    .with_column(ColumnSpec::new("value", DataType::Utf8).nullable()),
            )
            .commit()
            .await
            .expect("adding a new column should succeed");

        let current_txn = handle
            .current_transaction_id()
            .await
            .expect("current transaction should be available");
        let drop_builder = handle
            .write(Some(current_txn))
            .await
            .expect("builder should be created");
        let err = drop_builder
            .update_schema(SchemaSpec::new().with_column(ColumnSpec::new("id", DataType::Int64)))
            .commit()
            .await
            .expect_err("implicit drop should fail");

        assert!(matches!(err, CatalogError::InvalidArgument(_)));
        assert!(err.to_string().contains("cannot drop existing column"));
        assert!(err.to_string().contains("value"));
    }

    #[tokio::test]
    async fn list_tables_allows_exactly_max_then_rejects_overflow() {
        let catalog = SqlCatalog::in_memory()
            .await
            .expect("in-memory catalog should initialize");

        for i in 0..limits::MAX_TABLES_PER_LIST {
            let table_uuid = uuid::Uuid::new_v4();
            let table_name = format!("table_{i}");
            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO tables (
                    table_uuid, table_name, namespace, location, current_schema_uuid,
                    current_transaction_id, created_at, properties, min_reader_version, min_writer_version
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )
            .bind(table_uuid.as_bytes().as_slice())
            .bind(table_name)
            .bind("ns")
            .bind("memory://bulk")
            .bind(Option::<Vec<u8>>::None)
            .bind(Option::<Vec<u8>>::None)
            .bind(chrono::Utc::now())
            .bind("{}")
            .bind(1_i32)
            .bind(1_i32)
            .execute(&catalog.pool)
            .await
            .expect("table insert should succeed");
        }

        let at_limit = catalog
            .list_tables(Some("ns"))
            .await
            .expect("exactly-at-limit listing should succeed");
        assert_eq!(at_limit.len(), limits::MAX_TABLES_PER_LIST as usize);

        let overflow_uuid = uuid::Uuid::new_v4();
        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO tables (
                table_uuid, table_name, namespace, location, current_schema_uuid,
                current_transaction_id, created_at, properties, min_reader_version, min_writer_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )
        .bind(overflow_uuid.as_bytes().as_slice())
        .bind("table_overflow")
        .bind("ns")
        .bind("memory://bulk")
        .bind(Option::<Vec<u8>>::None)
        .bind(Option::<Vec<u8>>::None)
        .bind(chrono::Utc::now())
        .bind("{}")
        .bind(1_i32)
        .bind(1_i32)
        .execute(&catalog.pool)
        .await
        .expect("overflow table insert should succeed");

        let err = catalog
            .list_tables(Some("ns"))
            .await
            .expect_err("overflow listing should fail");
        assert!(matches!(err, CatalogError::LimitExceeded(_)));
        assert!(err.to_string().contains("table count exceeds limit"));
    }

    #[tokio::test]
    async fn read_table_allows_exactly_max_columns_then_rejects_overflow() {
        let (catalog, ident, table_view) = create_sqlite_catalog_with_table().await;
        let schema_uuid = table_view.schema.schema_uuid;

        sqlx::query::<sqlx::Sqlite>("DELETE FROM columns WHERE schema_uuid = ?1")
            .bind(schema_uuid.as_bytes().as_slice())
            .execute(&catalog.pool)
            .await
            .expect("clear schema columns should succeed");

        let encoded_type = encode_data_type(&DataType::Int64).expect("type encoding should succeed");
        for i in 0..limits::MAX_COLUMNS_PER_SCHEMA {
            let column_uuid = uuid::Uuid::new_v4();
            let column_name = format!("col_{i}");
            sqlx::query::<sqlx::Sqlite>(
                "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(column_uuid.as_bytes().as_slice())
            .bind(schema_uuid.as_bytes().as_slice())
            .bind(column_name)
            .bind(encoded_type.as_slice())
            .bind((i + 1) as i32)
            .bind(false)
            .execute(&catalog.pool)
            .await
            .expect("column insert should succeed");
        }

        let at_limit = catalog
            .read_table(&ident, None)
            .await
            .expect("exactly-at-limit read should succeed");
        assert_eq!(at_limit.schema.columns.len(), limits::MAX_COLUMNS_PER_SCHEMA as usize);

        let overflow_column_uuid = uuid::Uuid::new_v4();
        sqlx::query::<sqlx::Sqlite>(
            "INSERT INTO columns (column_uuid, schema_uuid, column_name, column_type, ordinal_position, is_nullable)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(overflow_column_uuid.as_bytes().as_slice())
        .bind(schema_uuid.as_bytes().as_slice())
        .bind("col_overflow")
        .bind(encoded_type.as_slice())
        .bind((limits::MAX_COLUMNS_PER_SCHEMA + 1) as i32)
        .bind(false)
        .execute(&catalog.pool)
        .await
        .expect("overflow column insert should succeed");

        let err = catalog
            .read_table(&ident, None)
            .await
            .expect_err("overflow read should fail");
        assert!(matches!(err, CatalogError::LimitExceeded(_)));
        assert!(err.to_string().contains("column count exceeds limit"));
    }
}
