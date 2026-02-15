use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::storage::{Format, file_format};

use super::{Catalog, CatalogError, Result, limits, schema};

/// Transaction identifier
pub type TxnId = uuid::Uuid;

/// Logical table identifier as `{namespace}.{name}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdent {
    /// Table namespace
    pub namespace: String,
    /// Table name
    pub name: String,
}

/// Typed table properties wrapper.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableProperties {
    pub(super) values: serde_json::Map<String, serde_json::Value>,
}

/// Strongly-typed table property key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PropertyKey(pub(super) String);

/// Immutable table snapshot at a specific transaction.
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
    /// Typed table properties
    pub properties: TableProperties,
    /// Optional table statistics
    pub stats: Option<schema::TableStats>,
}

/// Difference between two table snapshots.
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
    pub new_properties: Option<TableProperties>,
}

/// Ordered transaction range selection for event scans.
#[derive(Debug, Clone, Copy)]
pub struct TxnRangeCursor {
    /// Start transaction (exclusive). `None` means from table genesis.
    pub from_exclusive: Option<TxnId>,
    /// End transaction (inclusive).
    pub to_inclusive: TxnId,
}

/// File-level transaction event kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnFileChangeKind {
    /// File became visible in this transaction.
    Added,
    /// File became invisible in this transaction.
    Removed,
}

/// File-level transaction event.
#[derive(Debug, Clone)]
pub struct TxnFileChange {
    /// Transaction that emitted this file change.
    pub transaction_id: TxnId,
    /// Kind of file change.
    pub kind: TxnFileChangeKind,
    /// Full file metadata.
    pub file: schema::File,
}

/// Schema-level transaction event.
#[derive(Debug, Clone)]
pub struct TxnSchemaChange {
    /// Transaction that emitted this schema change.
    pub transaction_id: TxnId,
    /// Schema that became active at this transaction.
    pub schema: schema::Schema,
}

/// Event-log record for one transaction.
#[derive(Debug, Clone)]
pub struct TxnEvent {
    /// Transaction ID.
    pub transaction_id: TxnId,
    /// File changes emitted by this transaction.
    pub file_changes: Vec<TxnFileChange>,
    /// Optional schema change emitted by this transaction.
    pub schema_change: Option<TxnSchemaChange>,
}

/// Result returned by a successful commit.
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
    pub column_type: DataType,
    /// Whether the column allows null values
    pub is_nullable: bool,
}

/// Schema specification used when creating or evolving a table.
#[derive(Debug, Clone)]
pub struct SchemaSpec {
    /// Column specifications
    pub columns: Vec<ColumnSpec>,
}

/// File metadata supplied by mutation operations.
#[derive(Debug, Clone)]
pub struct FileSpec {
    /// Optional file UUID (generated if not provided)
    pub file_uuid: Option<uuid::Uuid>,
    /// File format
    pub file_format: Format,
    /// File path
    pub file_path: String,
    /// Number of records in the file
    pub record_count: i64,
    /// File size in bytes
    pub file_size_bytes: i64,
    /// Optional partition values
    pub partition_values: Option<serde_json::Value>,
    /// Optional format-specific options
    pub format_options: Option<serde_json::Value>,
}

/// Atomic table mutation operation.
#[derive(Debug, Clone)]
pub enum MutationOp {
    /// Append files to the table
    AppendFiles(Vec<FileSpec>),
    /// Delete files by UUID
    DeleteFiles(Vec<uuid::Uuid>),
    /// Update the table schema
    UpdateSchema(SchemaSpec),
    /// Set table properties (replaces existing)
    SetProperties(TableProperties),
    /// Remove properties by key
    RemoveProperties(Vec<PropertyKey>),
}

/// Collection of operations applied atomically in one commit.
#[derive(Debug, Clone, Default)]
pub struct Mutation {
    /// Operations to apply
    pub operations: Vec<MutationOp>,
}

/// Builder for constructing and committing table mutations.
pub struct MutationBuilder {
    /// Catalog instance for committing mutations
    pub(super) catalog: Arc<dyn super::Catalog>,
    /// Table identifier
    pub(super) ident: TableIdent,
    /// Base transaction ID for optimistic concurrency control
    pub(super) base_transaction_id: TxnId,
    /// Mutation operations to apply
    pub(super) mutation: Mutation,
}

/// Handle for interacting with a table
#[derive(Clone)]
pub struct TableHandle {
    /// Catalog instance for table operations
    pub(super) catalog: Arc<dyn super::Catalog>,
    /// Table identifier
    pub(super) ident: TableIdent,
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

impl std::fmt::Display for TableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

impl TableProperties {
    /// Create empty table properties.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse table properties from JSON, requiring an object.
    pub fn from_json(value: serde_json::Value) -> Result<Self> {
        match value {
            serde_json::Value::Object(values) => Ok(Self { values }),
            _ => Err(CatalogError::InvalidArgument(
                "table properties must be a JSON object".to_string(),
            )),
        }
    }

    /// Borrow table properties as a JSON object map.
    pub fn as_map(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.values
    }

    /// Convert table properties into a JSON object value.
    pub fn into_json(self) -> serde_json::Value {
        serde_json::Value::Object(self.values)
    }

    /// Clone table properties into a JSON object value.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::Value::Object(self.values.clone())
    }

    /// Replace or insert a property value.
    pub fn insert(&mut self, key: String, value: serde_json::Value) {
        self.values.insert(key, value);
    }

    /// Remove a property key if present.
    pub fn remove(&mut self, key: &str) {
        self.values.remove(key);
    }
}

impl TryFrom<serde_json::Value> for TableProperties {
    type Error = CatalogError;

    fn try_from(value: serde_json::Value) -> Result<Self> {
        Self::from_json(value)
    }
}

impl From<TableProperties> for serde_json::Value {
    fn from(value: TableProperties) -> Self {
        value.into_json()
    }
}

impl std::fmt::Display for TableProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let json = serde_json::to_string(&self.values).map_err(|_| std::fmt::Error)?;
        f.write_str(&json)
    }
}

impl PropertyKey {
    /// Create a validated property key.
    pub fn new(key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        if key.trim().is_empty() {
            return Err(CatalogError::InvalidArgument(
                "property key cannot be empty".to_string(),
            ));
        }
        Ok(Self(key))
    }

    /// Borrow the property key string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PropertyKey {
    type Error = CatalogError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl ColumnSpec {
    /// Create a new column specification (non-nullable by default)
    pub fn new(name: impl Into<String>, column_type: DataType) -> Self {
        Self {
            name: name.into(),
            column_type,
            is_nullable: false,
        }
    }

    /// Mark the column as nullable.
    pub fn nullable(mut self) -> Self {
        self.is_nullable = true;
        self
    }
}

impl SchemaSpec {
    /// Create an empty schema specification
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
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

    /// Validate schema shape and column naming constraints.
    pub fn validate(&self) -> Result<()> {
        if self.columns.is_empty() {
            return Err(CatalogError::InvalidArgument(
                "schema must include at least one column".to_string(),
            ));
        }
        if self.columns.len() as u32 > limits::MAX_COLUMNS_PER_SCHEMA {
            return Err(CatalogError::LimitExceeded(format!(
                "schema has {} columns, exceeds limit of {}",
                self.columns.len(),
                limits::MAX_COLUMNS_PER_SCHEMA
            )));
        }
        let mut seen = std::collections::HashSet::with_capacity(self.columns.len());
        for column in &self.columns {
            if column.name.trim().is_empty() {
                return Err(CatalogError::InvalidArgument(
                    "column name cannot be empty".to_string(),
                ));
            }
            if !seen.insert(column.name.as_str()) {
                return Err(CatalogError::InvalidArgument(format!(
                    "duplicate column name: '{}'",
                    column.name
                )));
            }
        }
        Ok(())
    }
}

impl Default for SchemaSpec {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSpec {
    /// Create a new file specification
    pub fn new(
        file_format: Format,
        file_path: impl Into<String>,
        record_count: i64,
        file_size_bytes: i64,
    ) -> Self {
        Self {
            file_uuid: None,
            file_format,
            file_path: file_path.into(),
            record_count,
            file_size_bytes,
            partition_values: None,
            format_options: None,
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

    /// Set format-specific options from JSON
    pub fn with_format_options(mut self, options: serde_json::Value) -> Self {
        self.format_options = Some(options);
        self
    }

    /// Set format-specific options from JSON with validation
    pub fn with_format_options_checked(mut self, options: serde_json::Value) -> Result<Self> {
        file_format::validate_format_options(self.file_format.as_str(), &options)
            .map_err(|err| CatalogError::InvalidArgument(err.to_string()))?;
        self.format_options = Some(options);
        Ok(self)
    }

    /// Validate file metadata shape and bounds.
    pub fn validate(&self) -> Result<()> {
        if self.file_path.trim().is_empty() {
            return Err(CatalogError::InvalidArgument(
                "file path cannot be empty".to_string(),
            ));
        }
        if self.record_count < 0 {
            return Err(CatalogError::InvalidArgument(
                "record_count cannot be negative".to_string(),
            ));
        }
        if self.file_size_bytes < 0 {
            return Err(CatalogError::InvalidArgument(
                "file_size_bytes cannot be negative".to_string(),
            ));
        }
        if let Some(partition_values) = &self.partition_values {
            if !partition_values.is_object() {
                return Err(CatalogError::InvalidArgument(
                    "partition_values must be a JSON object".to_string(),
                ));
            }
        }
        if let Some(format_options) = &self.format_options {
            if !format_options.is_object() {
                return Err(CatalogError::InvalidArgument(
                    "format_options must be a JSON object".to_string(),
                ));
            }
            file_format::validate_format_options(self.file_format.as_str(), format_options)
                .map_err(|err| CatalogError::InvalidArgument(err.to_string()))?;
        }
        Ok(())
    }
}

impl Mutation {
    /// Validate mutation semantics before persistence.
    pub fn validate(&self) -> Result<()> {
        let mut update_schema_ops = 0_u32;
        let mut set_properties_ops = 0_u32;
        for op in &self.operations {
            match op {
                MutationOp::UpdateSchema(_) => {
                    update_schema_ops += 1;
                    if update_schema_ops > 1 {
                        return Err(CatalogError::InvalidArgument(
                            "mutation can include at most one UpdateSchema operation".to_string(),
                        ));
                    }
                }
                MutationOp::SetProperties(_) => {
                    set_properties_ops += 1;
                    if set_properties_ops > 1 {
                        return Err(CatalogError::InvalidArgument(
                            "mutation can include at most one SetProperties operation".to_string(),
                        ));
                    }
                }
                MutationOp::DeleteFiles(file_uuids) => {
                    let mut seen = std::collections::HashSet::with_capacity(file_uuids.len());
                    for file_uuid in file_uuids {
                        if !seen.insert(*file_uuid) {
                            return Err(CatalogError::InvalidArgument(format!(
                                "duplicate file UUID in DeleteFiles: {}",
                                file_uuid
                            )));
                        }
                    }
                }
                MutationOp::RemoveProperties(keys) => {
                    let mut seen = std::collections::HashSet::with_capacity(keys.len());
                    for key in keys {
                        if !seen.insert(key.as_str()) {
                            return Err(CatalogError::InvalidArgument(format!(
                                "duplicate property key in RemoveProperties: {}",
                                key.as_str()
                            )));
                        }
                    }
                }
                MutationOp::AppendFiles(_) => {}
            }
        }
        Ok(())
    }
}

impl MutationBuilder {
    /// Create a new mutation builder
    pub(super) fn new(
        catalog: Arc<dyn Catalog>,
        ident: TableIdent,
        base_transaction_id: TxnId,
    ) -> Self {
        Self {
            catalog,
            ident,
            base_transaction_id,
            mutation: Mutation::default(),
        }
    }

    /// Append multiple files to the table.
    pub fn append_files(mut self, files: Vec<FileSpec>) -> Self {
        self.mutation
            .operations
            .push(MutationOp::AppendFiles(files));
        self
    }

    /// Append a single file to the table.
    pub fn append_file(mut self, file: FileSpec) -> Self {
        self.mutation
            .operations
            .push(MutationOp::AppendFiles(vec![file]));
        self
    }

    /// Delete files by UUID.
    pub fn delete_files(mut self, file_uuids: Vec<uuid::Uuid>) -> Self {
        self.mutation
            .operations
            .push(MutationOp::DeleteFiles(file_uuids));
        self
    }

    /// Update the table schema.
    pub fn update_schema(mut self, schema: SchemaSpec) -> Self {
        self.mutation
            .operations
            .push(MutationOp::UpdateSchema(schema));
        self
    }

    /// Set table properties (replaces existing).
    pub fn set_properties(mut self, properties: TableProperties) -> Self {
        self.mutation
            .operations
            .push(MutationOp::SetProperties(properties));
        self
    }

    /// Remove properties by key.
    pub fn remove_properties(mut self, keys: Vec<PropertyKey>) -> Self {
        self.mutation
            .operations
            .push(MutationOp::RemoveProperties(keys));
        self
    }

    /// Commit the mutation to the catalog.
    pub async fn commit(self) -> Result<CommitResult> {
        self.catalog
            .commit(&self.ident, self.base_transaction_id, self.mutation)
            .await
    }
}

impl TableHandle {
    /// Create a new table handle.
    pub fn new(catalog: Arc<dyn Catalog>, ident: TableIdent) -> Self {
        Self { catalog, ident }
    }

    /// Get the table identifier.
    pub fn ident(&self) -> &TableIdent {
        &self.ident
    }

    /// Read the current table view.
    pub async fn read(&self) -> Result<TableView> {
        self.catalog.read_table(&self.ident, None).await
    }

    /// Read the table view at a specific transaction.
    pub async fn read_at(&self, transaction_id: TxnId) -> Result<TableView> {
        self.catalog
            .read_table(&self.ident, Some(transaction_id))
            .await
    }

    /// Get the current transaction head for this table.
    pub async fn current_transaction_id(&self) -> Result<TxnId> {
        self.catalog.current_transaction_id(&self.ident).await
    }

    /// List ordered transaction events in a bounded range.
    pub async fn list_transaction_events(
        &self,
        from_exclusive: Option<TxnId>,
        to_inclusive: TxnId,
    ) -> Result<Vec<TxnEvent>> {
        self.catalog
            .list_transaction_events(
                &self.ident,
                TxnRangeCursor {
                    from_exclusive,
                    to_inclusive,
                },
            )
            .await
    }

    /// Compute the difference between two transaction versions.
    pub async fn diff(
        &self,
        from_transaction_id: TxnId,
        to_transaction_id: TxnId,
    ) -> Result<TableDelta> {
        self.catalog
            .diff_table(&self.ident, from_transaction_id, to_transaction_id)
            .await
    }

    /// Create a mutation builder pinned to a base transaction.
    pub async fn write(&self, base_transaction_id: Option<TxnId>) -> Result<MutationBuilder> {
        let txn_id = match base_transaction_id {
            Some(id) => id,
            None => self.catalog.current_transaction_id(&self.ident).await?,
        };
        Ok(MutationBuilder::new(
            self.catalog.clone(),
            self.ident.clone(),
            txn_id,
        ))
    }
}
