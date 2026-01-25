# Change Data Capture (CDC)

## Purpose

This document specifies Planar's change data capture (CDC) system. CDC enables consumers to read incremental changes to a table, supporting use cases like streaming pipelines, data replication, incremental ETL, and real-time analytics.

## Motivation

Without CDC, consumers must either:

1. **Full table scans**: Re-read the entire table to detect changes. Inefficient and doesn't scale.

2. **Timestamp filtering**: Query rows by a timestamp column. Requires application-level conventions, misses deletes, and doesn't capture schema changes.

3. **External change tracking**: Use triggers, log mining, or replication slots. Adds complexity and often requires database-specific tooling.

Planar's transaction-based architecture naturally tracks changes: each transaction records which files were added or removed. CDC exposes this information in a consumable format, enabling:

- **Incremental processing**: Read only rows that changed since last checkpoint
- **Data replication**: Replicate table changes to downstream systems
- **Streaming pipelines**: Feed changes to Kafka, Flink, or other streaming systems
- **Audit trails**: Track who changed what and when
- **Cache invalidation**: Know when cached data is stale

## Design Principles

1. **Transaction-aligned**: Changes are organized by transaction, preserving the atomicity of commits.

2. **Pull-based API**: Consumers request changes for a transaction range. No server-side push infrastructure required.

3. **Row-level granularity**: Report individual row changes (insert, update, delete), not just file changes.

4. **Schema-aware**: Include schema information with changes to handle schema evolution.

5. **Checkpoint-friendly**: Enable consumers to track progress and resume from any transaction.

6. **Compaction-safe**: Handle the case where compaction rewrites files, preserving change semantics.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane (DB)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ transactions│  │    files    │  │  deletion   │  │     cdc_metadata    │ │
│  │             │  │             │  │  _vectors   │  │                     │ │
│  │ txn_id      │  │ file_uuid   │  │ dv_uuid     │  │ table_uuid          │ │
│  │ timestamp   │  │ added_in    │  │ file_uuid   │  │ cdc_enabled         │ │
│  │ parent_txn  │  │ removed_in  │  │ cardinality │  │ retention_txns      │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                            CDC Query │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CDC Consumer                                    │
│                                                                             │
│   read_changes(from_txn, to_txn) -> Stream<ChangeEvent>                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## CDC Event Model

### Event Types

```rust
/// Type of change operation
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeOperation {
    /// Row was inserted
    Insert,
    /// Row was updated (previous value available)
    Update,
    /// Row was deleted
    Delete,
}

/// A single row change event
#[derive(Clone, Debug)]
pub struct ChangeEvent {
    /// Transaction that made this change
    pub transaction_id: Uuid,
    /// Transaction timestamp
    pub transaction_timestamp: DateTime<Utc>,
    /// Type of change
    pub operation: ChangeOperation,
    /// Table UUID
    pub table_uuid: Uuid,
    /// Schema at time of change
    pub schema: Arc<Schema>,
    /// The row data (after-image for insert/update, before-image for delete)
    pub row: Row,
    /// For updates: the previous row value
    pub previous_row: Option<Row>,
    /// Source file UUID
    pub source_file: Uuid,
    /// Row position within source file
    pub row_position: u32,
}

/// A batch of changes within a transaction
#[derive(Clone, Debug)]
pub struct TransactionChanges {
    /// Transaction metadata
    pub transaction_id: Uuid,
    pub transaction_timestamp: DateTime<Utc>,
    pub parent_transaction_id: Option<Uuid>,
    /// Schema valid at this transaction
    pub schema: Arc<Schema>,
    /// All changes in this transaction
    pub changes: Vec<ChangeEvent>,
}
```

### Row Representation

```rust
/// A row of data with named columns
#[derive(Clone, Debug)]
pub struct Row {
    /// Column values keyed by column name
    pub values: HashMap<String, ScalarValue>,
}

/// Scalar value matching Planar's type system
#[derive(Clone, Debug)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Decimal128(i128, u8, i8), // value, precision, scale
    String(String),
    Binary(Vec<u8>),
    Date32(i32),
    Date64(i64),
    Timestamp(i64, TimeUnit, Option<Arc<str>>),
    // Complex types
    List(Vec<ScalarValue>),
    Struct(HashMap<String, ScalarValue>),
    Map(Vec<(ScalarValue, ScalarValue)>),
}
```

## Design Options

### Option 1: Transaction-Delta API (Recommended)

Expose changes as the delta between two transactions. Changes are derived by comparing file states.

**How it works**:
1. Consumer specifies `from_transaction_id` and `to_transaction_id`
2. System computes files added/removed between transactions
3. For added files: emit INSERT events for all rows
4. For removed files with deletion vectors: emit DELETE events for deleted rows
5. For removed files without DVs: emit DELETE events for all rows (file was replaced)

**Advantages**:
- Simple implementation using existing metadata
- No additional storage required
- Works with time travel (any transaction range)

**Disadvantages**:
- Cannot distinguish UPDATE from DELETE+INSERT without additional tracking
- Requires reading file contents to emit row-level events
- Large transaction ranges may require significant I/O

### Option 2: Explicit Change Log

Maintain a separate change log table that records every row-level operation.

**How it works**:
1. Every write operation logs individual row changes to a `change_log` table
2. CDC consumers read from the change log
3. Change log is periodically compacted/archived

**Advantages**:
- True row-level change history
- Can distinguish UPDATE from DELETE+INSERT
- Efficient for high-frequency small changes

**Disadvantages**:
- Significant storage overhead
- Write amplification (every row change logged twice)
- Additional complexity in commit path
- Retention management for change log

### Option 3: File-Level CDC with External Inference

Only expose file-level changes. Let consumers infer row-level changes.

**How it works**:
1. Consumer receives: "file X added", "file Y removed"
2. Consumer reads files and computes diff themselves

**Advantages**:
- Minimal overhead
- Simple implementation

**Disadvantages**:
- Pushes complexity to consumers
- No standard format
- Difficult to handle deletion vectors correctly

### Current Recommendation

**Option 1 (Transaction-Delta API)** is recommended for initial implementation:
- Leverages existing transaction semantics
- No additional storage requirements
- Provides row-level events with reasonable effort
- Can be enhanced with Option 2 for UPDATE tracking later

## API Design

### Core CDC Interface

```rust
/// CDC reader for a table
pub struct CdcReader {
    catalog: Arc<SqlCatalog>,
    table_uuid: Uuid,
}

impl CdcReader {
    /// Create a CDC reader for a table
    pub async fn new(catalog: Arc<SqlCatalog>, table_uuid: Uuid) -> Result<Self, CdcError> {
        // Verify CDC is enabled for this table
        let table = catalog.get_table(table_uuid).await?;
        if !table.cdc_enabled() {
            return Err(CdcError::CdcNotEnabled(table_uuid));
        }
        
        Ok(Self { catalog, table_uuid })
    }
    
    /// Read changes between two transactions
    pub async fn read_changes(
        &self,
        from_transaction_id: Uuid,
        to_transaction_id: Uuid,
        options: CdcOptions,
    ) -> Result<CdcStream, CdcError> {
        // Validate transaction range
        self.validate_transaction_range(from_transaction_id, to_transaction_id).await?;
        
        // Build change stream
        CdcStream::new(
            self.catalog.clone(),
            self.table_uuid,
            from_transaction_id,
            to_transaction_id,
            options,
        ).await
    }
    
    /// Get the earliest transaction available for CDC
    pub async fn earliest_transaction(&self) -> Result<Uuid, CdcError> {
        self.catalog.get_earliest_retained_transaction(self.table_uuid).await
    }
    
    /// Get the latest transaction available for CDC
    pub async fn latest_transaction(&self) -> Result<Uuid, CdcError> {
        self.catalog.get_current_transaction(self.table_uuid).await
    }
    
    /// Read changes since a checkpoint, returning new checkpoint
    pub async fn read_changes_since(
        &self,
        checkpoint: &CdcCheckpoint,
        options: CdcOptions,
    ) -> Result<(CdcStream, CdcCheckpoint), CdcError> {
        let latest = self.latest_transaction().await?;
        let stream = self.read_changes(checkpoint.transaction_id, latest, options).await?;
        let new_checkpoint = CdcCheckpoint {
            table_uuid: self.table_uuid,
            transaction_id: latest,
            timestamp: Utc::now(),
        };
        Ok((stream, new_checkpoint))
    }
}
```

### CDC Options

```rust
/// Options for CDC reads
#[derive(Clone, Debug, Default)]
pub struct CdcOptions {
    /// Filter by operation types (default: all)
    pub operation_filter: Option<Vec<ChangeOperation>>,
    /// Include only these columns (default: all)
    pub column_filter: Option<Vec<String>>,
    /// Maximum number of events to return
    pub limit: Option<usize>,
    /// Include schema with each event (vs. once per transaction)
    pub include_schema_per_event: bool,
    /// For updates, include previous row value
    pub include_previous_row: bool,
}
```

### CDC Stream

```rust
/// Streaming iterator over CDC events
pub struct CdcStream {
    catalog: Arc<SqlCatalog>,
    table_uuid: Uuid,
    options: CdcOptions,
    // Transaction iterator state
    transactions: Vec<Transaction>,
    current_txn_index: usize,
    // Current transaction's changes
    current_changes: Option<VecDeque<ChangeEvent>>,
}

impl CdcStream {
    pub async fn new(
        catalog: Arc<SqlCatalog>,
        table_uuid: Uuid,
        from_txn: Uuid,
        to_txn: Uuid,
        options: CdcOptions,
    ) -> Result<Self, CdcError> {
        // Get all transactions in range (ordered by timestamp)
        let transactions = catalog
            .list_transactions_in_range(table_uuid, from_txn, to_txn)
            .await?;
        
        Ok(Self {
            catalog,
            table_uuid,
            options,
            transactions,
            current_txn_index: 0,
            current_changes: None,
        })
    }
    
    /// Get next batch of changes
    pub async fn next_batch(&mut self, batch_size: usize) -> Result<Vec<ChangeEvent>, CdcError> {
        let mut batch = Vec::with_capacity(batch_size);
        
        while batch.len() < batch_size {
            // Get changes from current transaction
            if let Some(changes) = &mut self.current_changes {
                while batch.len() < batch_size {
                    if let Some(event) = changes.pop_front() {
                        batch.push(event);
                    } else {
                        break;
                    }
                }
            }
            
            // Move to next transaction if current is exhausted
            if self.current_changes.as_ref().map(|c| c.is_empty()).unwrap_or(true) {
                if self.current_txn_index >= self.transactions.len() {
                    break; // No more transactions
                }
                
                let txn = &self.transactions[self.current_txn_index];
                self.current_changes = Some(
                    self.compute_transaction_changes(txn).await?.into()
                );
                self.current_txn_index += 1;
            }
        }
        
        Ok(batch)
    }
    
    /// Compute changes for a single transaction
    async fn compute_transaction_changes(
        &self,
        txn: &Transaction,
    ) -> Result<Vec<ChangeEvent>, CdcError> {
        let mut changes = Vec::new();
        let schema = self.catalog
            .get_schema_at(self.table_uuid, txn.transaction_id)
            .await?;
        let schema = Arc::new(schema);
        
        // Get files added in this transaction
        let added_files = self.catalog
            .list_files_added_in(self.table_uuid, txn.transaction_id)
            .await?;
        
        // Get files removed in this transaction (and their deletion vectors)
        let removed_files = self.catalog
            .list_files_removed_in(self.table_uuid, txn.transaction_id)
            .await?;
        
        // Get deletion vectors added in this transaction
        let added_dvs = self.catalog
            .list_deletion_vectors_added_in(self.table_uuid, txn.transaction_id)
            .await?;
        
        // Emit INSERT events for added files
        for file in added_files {
            let reader = self.open_file_reader(&file).await?;
            let mut row_position = 0u32;
            
            while let Some(batch) = reader.next_batch().await? {
                for row in batch.rows() {
                    if self.options.matches_filter(&ChangeOperation::Insert) {
                        changes.push(ChangeEvent {
                            transaction_id: txn.transaction_id,
                            transaction_timestamp: txn.transaction_timestamp,
                            operation: ChangeOperation::Insert,
                            table_uuid: self.table_uuid,
                            schema: schema.clone(),
                            row: self.project_row(row, &schema),
                            previous_row: None,
                            source_file: file.file_uuid,
                            row_position,
                        });
                    }
                    row_position += 1;
                }
            }
        }
        
        // Emit DELETE events for rows in added deletion vectors
        for dv_record in added_dvs {
            let dv = self.load_deletion_vector(&dv_record).await?;
            let file = self.catalog.get_file(dv_record.file_uuid).await?;
            
            // Need to read the file to get deleted row values
            if self.options.include_row_values() {
                let reader = self.open_file_reader(&file).await?;
                let mut row_position = 0u32;
                
                while let Some(batch) = reader.next_batch().await? {
                    for row in batch.rows() {
                        if dv.is_deleted(row_position) {
                            if self.options.matches_filter(&ChangeOperation::Delete) {
                                changes.push(ChangeEvent {
                                    transaction_id: txn.transaction_id,
                                    transaction_timestamp: txn.transaction_timestamp,
                                    operation: ChangeOperation::Delete,
                                    table_uuid: self.table_uuid,
                                    schema: schema.clone(),
                                    row: self.project_row(row, &schema),
                                    previous_row: None,
                                    source_file: file.file_uuid,
                                    row_position,
                                });
                            }
                        }
                        row_position += 1;
                    }
                }
            } else {
                // Emit deletes without row values
                for row_position in dv.deleted_rows.iter() {
                    if self.options.matches_filter(&ChangeOperation::Delete) {
                        changes.push(ChangeEvent {
                            transaction_id: txn.transaction_id,
                            transaction_timestamp: txn.transaction_timestamp,
                            operation: ChangeOperation::Delete,
                            table_uuid: self.table_uuid,
                            schema: schema.clone(),
                            row: Row::empty(),
                            previous_row: None,
                            source_file: file.file_uuid,
                            row_position,
                        });
                    }
                }
            }
        }
        
        // Handle fully removed files (not via DVs - e.g., compaction or full file delete)
        for file in removed_files {
            // Check if this removal was due to compaction
            // If so, we should not emit DELETE events (data still exists in new file)
            if self.is_compaction_removal(&file, txn).await? {
                continue; // Skip - compaction preserves data
            }
            
            // Emit DELETE for all rows in the removed file
            let reader = self.open_file_reader(&file).await?;
            let mut row_position = 0u32;
            
            while let Some(batch) = reader.next_batch().await? {
                for row in batch.rows() {
                    if self.options.matches_filter(&ChangeOperation::Delete) {
                        changes.push(ChangeEvent {
                            transaction_id: txn.transaction_id,
                            transaction_timestamp: txn.transaction_timestamp,
                            operation: ChangeOperation::Delete,
                            table_uuid: self.table_uuid,
                            schema: schema.clone(),
                            row: self.project_row(row, &schema),
                            previous_row: None,
                            source_file: file.file_uuid,
                            row_position,
                        });
                    }
                    row_position += 1;
                }
            }
        }
        
        Ok(changes)
    }
}
```

### Checkpoint Management

```rust
/// Checkpoint for CDC progress tracking
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcCheckpoint {
    /// Table being tracked
    pub table_uuid: Uuid,
    /// Last processed transaction ID
    pub transaction_id: Uuid,
    /// When checkpoint was created
    pub timestamp: DateTime<Utc>,
}

impl CdcCheckpoint {
    /// Create initial checkpoint at table's earliest retained transaction
    pub async fn initial(catalog: &SqlCatalog, table_uuid: Uuid) -> Result<Self, CdcError> {
        let earliest = catalog.get_earliest_retained_transaction(table_uuid).await?;
        Ok(Self {
            table_uuid,
            transaction_id: earliest,
            timestamp: Utc::now(),
        })
    }
    
    /// Serialize to JSON for storage
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
    
    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self, CdcError> {
        serde_json::from_str(json).map_err(|e| CdcError::InvalidCheckpoint(e.to_string()))
    }
}
```

## Update Detection

Planar does not natively distinguish UPDATE from DELETE+INSERT. However, updates can be inferred:

### Option A: Row Identity Matching

If the table has a primary key or unique identifier:

```rust
impl CdcStream {
    /// Detect updates by matching row identities between deletes and inserts
    fn detect_updates(&self, changes: &mut Vec<ChangeEvent>, key_columns: &[String]) {
        let mut deletes_by_key: HashMap<Vec<ScalarValue>, ChangeEvent> = HashMap::new();
        let mut inserts_by_key: HashMap<Vec<ScalarValue>, ChangeEvent> = HashMap::new();
        let mut other_changes = Vec::new();
        
        // Partition changes
        for change in changes.drain(..) {
            let key = self.extract_key(&change.row, key_columns);
            match change.operation {
                ChangeOperation::Delete => {
                    deletes_by_key.insert(key, change);
                }
                ChangeOperation::Insert => {
                    inserts_by_key.insert(key, change);
                }
                _ => other_changes.push(change),
            }
        }
        
        // Match deletes with inserts to form updates
        for (key, insert) in inserts_by_key {
            if let Some(delete) = deletes_by_key.remove(&key) {
                changes.push(ChangeEvent {
                    operation: ChangeOperation::Update,
                    row: insert.row,
                    previous_row: Some(delete.row),
                    ..insert
                });
            } else {
                changes.push(insert);
            }
        }
        
        // Remaining deletes are true deletes
        changes.extend(deletes_by_key.into_values());
        changes.extend(other_changes);
    }
}
```

### Option B: Table Metadata for Key Columns

```rust
/// CDC configuration in table properties
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcConfig {
    /// Whether CDC is enabled
    pub enabled: bool,
    /// Columns that form the row identity for update detection
    pub key_columns: Option<Vec<String>>,
    /// Retention policy for CDC (min transactions to keep)
    pub retention_transactions: Option<u64>,
}
```

## Ordering Guarantees

### Transaction Order

Changes are always returned in transaction order (by `transaction_timestamp`):

```sql
-- Transactions are ordered by timestamp
SELECT transaction_id, transaction_timestamp
FROM transactions
WHERE table_uuid = ?
  AND transaction_timestamp > (SELECT transaction_timestamp FROM transactions WHERE transaction_id = :from_txn)
  AND transaction_timestamp <= (SELECT transaction_timestamp FROM transactions WHERE transaction_id = :to_txn)
ORDER BY transaction_timestamp ASC;
```

### Intra-Transaction Order

Within a transaction, the order of changes is:
1. **Deletes first**: All DELETE events before INSERTs (to support UPDATE detection)
2. **File order**: Changes ordered by file (for locality)
3. **Row position**: Within a file, ordered by row position

This ordering enables consumers to process changes correctly even when they arrive in batches.

## Schema Evolution in CDC

When schema changes occur, CDC events include the schema valid at that transaction:

```rust
pub struct SchemaChangeEvent {
    pub transaction_id: Uuid,
    pub old_schema: Option<Arc<Schema>>,
    pub new_schema: Arc<Schema>,
    pub change_type: SchemaChangeType,
}

pub enum SchemaChangeType {
    /// Initial schema creation
    Created,
    /// Column added
    ColumnAdded { column: Column },
    /// Column removed
    ColumnRemoved { column_name: String },
    /// Column type changed
    ColumnTypeChanged { column_name: String, old_type: String, new_type: String },
    /// Column renamed
    ColumnRenamed { old_name: String, new_name: String },
}
```

CDC streams can include schema change events:

```rust
pub enum CdcEvent {
    /// Row change
    RowChange(ChangeEvent),
    /// Schema change
    SchemaChange(SchemaChangeEvent),
}
```

## Compaction Handling

Compaction rewrites files without changing the logical data. CDC must handle this correctly:

### Problem

When compaction runs:
1. Old files are marked with `removed_in_transaction_id`
2. New files are added with `added_in_transaction_id`

Naive CDC would emit DELETE for old files and INSERT for new files, incorrectly reporting changes.

### Solution: Compaction Markers

```sql
-- Add compaction tracking to transactions
ALTER TABLE transactions ADD COLUMN is_compaction BOOLEAN DEFAULT FALSE;

-- Or track at file level
ALTER TABLE files ADD COLUMN replaces_file_uuid BLOB REFERENCES files(file_uuid);
```

CDC uses this to filter out compaction-related changes:

```rust
impl CdcStream {
    async fn is_compaction_removal(&self, file: &File, txn: &Transaction) -> Result<bool, CdcError> {
        // Check if transaction is marked as compaction
        if txn.is_compaction {
            return Ok(true);
        }
        
        // Check if file was replaced by another file in same transaction
        let replacement = self.catalog
            .find_file_replacing(file.file_uuid, txn.transaction_id)
            .await?;
        
        Ok(replacement.is_some())
    }
}
```

## Streaming Buffer Integration

When the streaming buffer is enabled, CDC can include buffered (uncommitted) changes:

```rust
pub struct CdcOptions {
    // ... existing options ...
    
    /// Include uncommitted changes from streaming buffer
    pub include_buffered: bool,
}

impl CdcStream {
    async fn include_buffered_changes(&self) -> Result<Vec<ChangeEvent>, CdcError> {
        let buffer = self.catalog.get_streaming_buffer(self.table_uuid).await?;
        
        if let Some(buffer) = buffer {
            // Get buffered rows not yet flushed
            let buffered = buffer.get_buffered_rows().await?;
            
            // Convert to CDC events (all are INSERTs from buffer)
            let events = buffered.into_iter().map(|row| ChangeEvent {
                transaction_id: Uuid::nil(), // Sentinel for uncommitted
                transaction_timestamp: Utc::now(),
                operation: ChangeOperation::Insert,
                table_uuid: self.table_uuid,
                schema: buffer.schema.clone(),
                row,
                previous_row: None,
                source_file: Uuid::nil(), // Not yet in a file
                row_position: 0,
            }).collect();
            
            Ok(events)
        } else {
            Ok(Vec::new())
        }
    }
}
```

See [streaming_buffer.md](streaming_buffer.md) for buffer architecture.

## External System Integration

### Kafka Connector (Future)

```rust
pub struct KafkaCdcSink {
    producer: KafkaProducer,
    topic: String,
    cdc_reader: CdcReader,
    checkpoint: CdcCheckpoint,
}

impl KafkaCdcSink {
    pub async fn run(&mut self) -> Result<(), CdcError> {
        loop {
            let (stream, new_checkpoint) = self.cdc_reader
                .read_changes_since(&self.checkpoint, CdcOptions::default())
                .await?;
            
            while let Some(batch) = stream.next_batch(100).await? {
                for event in batch {
                    let key = self.event_key(&event);
                    let value = serde_json::to_vec(&event)?;
                    self.producer.send(&self.topic, key, value).await?;
                }
            }
            
            // Commit checkpoint after successful publish
            self.checkpoint = new_checkpoint;
            self.save_checkpoint().await?;
            
            // Wait before polling again
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
```

### Flink Table Source (Future)

CDC can expose a Flink-compatible changelog stream format (Debezium-style):

```json
{
    "before": null,
    "after": {"id": 1, "name": "Alice", "amount": 100},
    "source": {
        "table": "orders",
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000"
    },
    "op": "c",
    "ts_ms": 1234567890000
}
```

## Implementation Phases

### Phase 1: Basic CDC API

1. Add CDC configuration to table properties
2. Implement `CdcReader` with `read_changes` method
3. Implement transaction-based change computation
4. Add checkpoint serialization

### Phase 2: Row-Level Events

1. Implement file reading for INSERT events
2. Integrate with deletion vectors for DELETE events
3. Add column projection

### Phase 3: Update Detection

1. Implement row identity matching
2. Add key column configuration to table properties
3. Combine DELETE+INSERT into UPDATE events

### Phase 4: Compaction Handling

1. Add compaction markers to transactions
2. Filter out compaction-related changes
3. Add file replacement tracking

### Phase 5: Streaming Integration

1. Add buffered change support
2. Implement Kafka connector
3. Document Flink/Spark integration patterns

## Testing Strategy

### Unit Tests

- Change event serialization/deserialization
- Checkpoint persistence
- Transaction range validation
- Update detection with key matching

### Integration Tests

- End-to-end CDC from write to read
- CDC across schema changes
- CDC with deletion vectors
- CDC with compaction
- Checkpoint resume correctness

### Performance Tests

- Large transaction range processing
- High-frequency change throughput
- Memory usage for large CDC streams

## Configuration

### Table-Level CDC Configuration

```sql
-- Enable CDC via table properties
UPDATE tables 
SET properties = json_set(properties, '$.cdc', json('{"enabled": true, "key_columns": ["id"]}'))
WHERE table_uuid = ?;
```

### System-Level Configuration

```rust
pub struct CdcSystemConfig {
    /// Default batch size for CDC reads
    pub default_batch_size: usize,
    /// Maximum events per CDC request
    pub max_events_per_request: usize,
    /// Enable CDC by default for new tables
    pub default_enabled: bool,
}
```

## Open Questions

1. **Exactly-once delivery**: How do we guarantee exactly-once semantics for CDC consumers? Should Planar provide this, or leave it to consumers?

2. **Retention coupling**: Should CDC retention be coupled with transaction retention? If transactions are expired, CDC cannot report those changes.

3. **Large transaction handling**: How do we handle transactions with millions of row changes? Stream in chunks? Paginate?

4. **Conflict with compaction**: If compaction runs frequently, it may remove fine-grained change history. Should compaction preserve CDC metadata?

5. **Cross-table CDC**: Can CDC span multiple tables? For example, tracking changes across a star schema?

6. **CDC backfill**: When CDC is enabled on an existing table, should we backfill historical changes from retained transactions?

## References

- [Delta Lake Change Data Feed](https://docs.delta.io/latest/delta-change-data-feed.html)
- [Apache Iceberg Incremental Read](https://iceberg.apache.org/docs/latest/spark-queries/#incremental-read)
- [Debezium CDC Format](https://debezium.io/documentation/reference/stable/connectors/index.html)
- [Flink CDC Connectors](https://ververica.github.io/flink-cdc-connectors/)
- [db_control_plane.md](db_control_plane.md) - Transaction model
- [deletion_vectors.md](deletion_vectors.md) - Row-level deletes
- [streaming_buffer.md](streaming_buffer.md) - Buffered writes
