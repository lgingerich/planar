# Deletion Vectors

## Purpose

This document specifies Planar's approach to row-level deletes using deletion vectors. Deletion vectors enable efficient selective row deletion without rewriting entire data files, solving the "small file problem" that arises when marking entire files as removed for sparse deletes.

## Motivation

Planar's current delete model operates at file granularity: when rows are deleted, the entire file is marked with `removed_in_transaction_id` and a new file is written containing only the surviving rows. This approach has significant drawbacks:

1. **Write amplification**: Deleting a single row requires rewriting an entire file (potentially hundreds of MB).

2. **Small file proliferation**: Frequent small deletes create many small files, degrading read performance.

3. **Compaction pressure**: The system must constantly compact to merge small files created by deletes.

4. **CDC complexity**: Row-level change tracking requires reading both old and new files to diff.

5. **Transaction overhead**: Each delete creates new file metadata entries even for minimal changes.

Deletion vectors solve these problems by tracking deleted row positions separately from the data file. The data file remains unchanged; readers simply skip rows marked as deleted.

## Design Principles

1. **Non-destructive deletes**: Data files are immutable. Deletions are recorded separately, preserving the original data for time travel and recovery.

2. **Efficient storage**: Deletion vectors use compressed bitmap representations (Roaring bitmaps) to minimize storage overhead.

3. **Read-time filtering**: Readers apply deletion vectors during scan, filtering out deleted rows without file rewrites.

4. **Eventual materialization**: Compaction periodically materializes deletions by rewriting files, reclaiming space and simplifying reads.

5. **Transaction consistency**: Deletion vectors follow the same transaction semantics as file additions/removals.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane (DB)                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                          deletion_vectors                            │   │
│  │  deletion_vector_uuid | file_uuid | dv_path | cardinality | txn_id  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                              files                                   │   │
│  │  file_uuid | ... | record_count | active_deletion_vector_uuid       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Data Plane (Object Storage)                       │
│                                                                             │
│   s3://bucket/tables/{uuid}/data/00001.parquet                              │
│   s3://bucket/tables/{uuid}/deletion_vectors/00001-dv-{uuid}.bin            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Design Options

### Option 1: Sidecar Deletion Vector Files (Recommended)

Store deletion vectors as separate files alongside data files. Each data file can have zero or one active deletion vector.

**Format**: Binary file containing a serialized Roaring bitmap of deleted row positions (0-indexed).

**Path convention**: `{table_location}/deletion_vectors/{data_file_name}-dv-{uuid}.bin`

**Advantages**:
- Simple file lifecycle (delete DV file when data file is compacted)
- Can be cached independently of data files
- Works with any data file format (Parquet, Lance, Vortex)
- Easy to inspect and debug

**Disadvantages**:
- Additional file per data file with deletions
- Extra I/O to read deletion vector

### Option 2: Embedded in Data File Metadata

Store deletion vectors in the data file's metadata section (e.g., Parquet footer key-value metadata).

**Advantages**:
- Single file to read
- No separate file management

**Disadvantages**:
- Requires rewriting file footer (not truly immutable)
- Format-specific implementation
- Harder to update incrementally

### Option 3: Database-Only Storage

Store deletion bitmaps directly in the control plane database as BLOB columns.

**Advantages**:
- Single source of truth
- Transactional updates
- No additional files

**Disadvantages**:
- Database bloat for large deletion vectors
- Not suitable for tables with millions of deleted rows
- Increases metadata query latency

### Current Recommendation

**Option 1 (Sidecar Files)** is recommended for initial implementation:
- Preserves data file immutability
- Scales to large deletion vectors
- Simple to implement and debug
- Consistent with Delta Lake's approach

## Deletion Vector Format

### File Format

Deletion vectors are stored as binary files containing serialized Roaring bitmaps:

```
┌──────────────────────────────────────────┐
│ Magic bytes: "PLDV" (4 bytes)            │
├──────────────────────────────────────────┤
│ Version: 1 (1 byte)                      │
├──────────────────────────────────────────┤
│ Checksum: CRC32 (4 bytes)                │
├──────────────────────────────────────────┤
│ Serialized Roaring bitmap (variable)     │
└──────────────────────────────────────────┘
```

### Roaring Bitmap

Roaring bitmaps efficiently represent sets of integers (row positions). They automatically choose between:
- Array containers for sparse regions
- Bitmap containers for dense regions
- Run-length encoding for sequential runs

This provides excellent compression for both sparse deletes (few scattered rows) and bulk deletes (contiguous ranges).

### Rust Implementation

```rust
use roaring::RoaringBitmap;

pub struct DeletionVector {
    /// The bitmap of deleted row positions (0-indexed)
    pub deleted_rows: RoaringBitmap,
}

impl DeletionVector {
    /// Create an empty deletion vector
    pub fn new() -> Self {
        Self {
            deleted_rows: RoaringBitmap::new(),
        }
    }
    
    /// Mark a row as deleted
    pub fn delete_row(&mut self, row_index: u32) {
        self.deleted_rows.insert(row_index);
    }
    
    /// Mark multiple rows as deleted
    pub fn delete_rows(&mut self, row_indices: impl IntoIterator<Item = u32>) {
        self.deleted_rows.extend(row_indices);
    }
    
    /// Check if a row is deleted
    pub fn is_deleted(&self, row_index: u32) -> bool {
        self.deleted_rows.contains(row_index)
    }
    
    /// Number of deleted rows
    pub fn cardinality(&self) -> u64 {
        self.deleted_rows.len()
    }
    
    /// Serialize to bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"PLDV"); // Magic
        buf.push(1); // Version
        
        let bitmap_bytes = self.deleted_rows.serialize::<roaring::Portable>();
        let checksum = crc32fast::hash(&bitmap_bytes);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&bitmap_bytes);
        
        buf
    }
    
    /// Deserialize from bytes
    pub fn deserialize(bytes: &[u8]) -> Result<Self, DeletionVectorError> {
        if bytes.len() < 9 {
            return Err(DeletionVectorError::InvalidFormat("Too short"));
        }
        
        if &bytes[0..4] != b"PLDV" {
            return Err(DeletionVectorError::InvalidFormat("Bad magic"));
        }
        
        let version = bytes[4];
        if version != 1 {
            return Err(DeletionVectorError::UnsupportedVersion(version));
        }
        
        let checksum = u32::from_le_bytes(bytes[5..9].try_into().unwrap());
        let bitmap_bytes = &bytes[9..];
        
        if crc32fast::hash(bitmap_bytes) != checksum {
            return Err(DeletionVectorError::ChecksumMismatch);
        }
        
        let deleted_rows = RoaringBitmap::deserialize_from(bitmap_bytes)
            .map_err(|e| DeletionVectorError::DeserializationFailed(e.to_string()))?;
        
        Ok(Self { deleted_rows })
    }
    
    /// Merge another deletion vector into this one
    pub fn merge(&mut self, other: &DeletionVector) {
        self.deleted_rows |= &other.deleted_rows;
    }
}
```

## Schema Changes

### New Table: `deletion_vectors`

```sql
CREATE TABLE IF NOT EXISTS deletion_vectors (
    deletion_vector_uuid BLOB PRIMARY KEY,
    file_uuid BLOB NOT NULL,
    deletion_vector_path TEXT NOT NULL,
    cardinality BIGINT NOT NULL,
    added_in_transaction_id BLOB NOT NULL,
    removed_in_transaction_id BLOB,
    FOREIGN KEY(file_uuid) REFERENCES files(file_uuid),
    FOREIGN KEY(added_in_transaction_id) REFERENCES transactions(transaction_id),
    FOREIGN KEY(removed_in_transaction_id) REFERENCES transactions(transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_deletion_vectors_file 
    ON deletion_vectors(file_uuid);

CREATE INDEX IF NOT EXISTS idx_deletion_vectors_active 
    ON deletion_vectors(file_uuid, added_in_transaction_id, removed_in_transaction_id);
```

### Modified Table: `files` (Optional Enhancement)

Add a column to quickly identify the active deletion vector:

```sql
ALTER TABLE files ADD COLUMN active_deletion_vector_uuid BLOB 
    REFERENCES deletion_vectors(deletion_vector_uuid);
```

This denormalization speeds up read queries but requires careful maintenance during commits.

## Write Path

### Delete Operation Flow

```
┌────────┐      ┌──────────────┐      ┌───────────┐
│ Writer │      │ Object Store │      │ Control DB│
└───┬────┘      └──────┬───────┘      └─────┬─────┘
    │                  │                    │
    │  1. Identify rows to delete           │
    │  (by predicate or row IDs)            │
    │                  │                    │
    │  2. Group deletes by file             │
    │                  │                    │
    │  3. For each affected file:           │
    │     - Load existing DV (if any)       │
    │     - Add new deleted row positions   │
    │     - Write new DV file               │
    │─────────────────>│                    │
    │                  │                    │
    │  4. BEGIN TRANSACTION                 │
    │──────────────────────────────────────>│
    │                  │                    │
    │  5. Validate base transaction         │
    │──────────────────────────────────────>│
    │                  │                    │
    │  6. For each file:                    │
    │     - Mark old DV as removed          │
    │     - Insert new DV record            │
    │     - Update file stats               │
    │──────────────────────────────────────>│
    │                  │                    │
    │  7. COMMIT                            │
    │──────────────────────────────────────>│
    │                  │                    │
```

### Delete Implementation

```rust
pub struct DeleteOperation {
    /// File UUID to delete from
    pub file_uuid: Uuid,
    /// Row positions to delete (0-indexed)
    pub row_positions: Vec<u32>,
}

impl SqlCatalog {
    pub async fn delete_rows(
        &self,
        table_uuid: Uuid,
        base_transaction_id: Uuid,
        deletes: Vec<DeleteOperation>,
    ) -> Result<Uuid, CatalogError> {
        let storage = self.get_object_store(&table_uuid).await?;
        let mut mutations = Vec::new();
        
        for delete in deletes {
            // Load existing deletion vector if present
            let existing_dv = self.get_active_deletion_vector(delete.file_uuid).await?;
            
            let mut dv = existing_dv
                .map(|e| self.load_deletion_vector(&storage, &e.deletion_vector_path).await)
                .transpose()?
                .unwrap_or_else(DeletionVector::new);
            
            // Add new deletions
            dv.delete_rows(delete.row_positions);
            
            // Write new deletion vector file
            let dv_uuid = Uuid::new_v4();
            let dv_path = format!(
                "deletion_vectors/{}-dv-{}.bin",
                delete.file_uuid, dv_uuid
            );
            
            storage.put(&dv_path, dv.serialize().into()).await?;
            
            // Record the mutation
            mutations.push(MutationOp::AddDeletionVector {
                file_uuid: delete.file_uuid,
                deletion_vector_uuid: dv_uuid,
                deletion_vector_path: dv_path,
                cardinality: dv.cardinality(),
            });
            
            // Mark old DV as removed if exists
            if let Some(old_dv) = existing_dv {
                mutations.push(MutationOp::RemoveDeletionVector {
                    deletion_vector_uuid: old_dv.deletion_vector_uuid,
                });
            }
        }
        
        // Commit the transaction
        self.commit(table_uuid, base_transaction_id, mutations).await
    }
}
```

### Predicate-Based Deletes

For SQL-style `DELETE WHERE` operations, the writer must:

1. Scan files to identify matching rows
2. Record the row positions of matches
3. Create deletion vectors as above

```rust
impl SqlCatalog {
    pub async fn delete_where(
        &self,
        table_uuid: Uuid,
        base_transaction_id: Uuid,
        predicate: &Predicate,
    ) -> Result<Uuid, CatalogError> {
        // Get files visible at base transaction
        let files = self.list_files_at(table_uuid, base_transaction_id).await?;
        let storage = self.get_object_store(&table_uuid).await?;
        
        let mut deletes = Vec::new();
        
        for file in files {
            // Skip files where predicate cannot match (using stats)
            if !predicate.may_match(&file.column_stats) {
                continue;
            }
            
            // Scan file to find matching rows
            let reader = self.open_file(&storage, &file).await?;
            let matching_rows = reader.find_matching_rows(predicate).await?;
            
            if !matching_rows.is_empty() {
                deletes.push(DeleteOperation {
                    file_uuid: file.file_uuid,
                    row_positions: matching_rows,
                });
            }
        }
        
        self.delete_rows(table_uuid, base_transaction_id, deletes).await
    }
}
```

## Read Path

### File Visibility with Deletion Vectors

The existing file visibility predicate must be extended to include deletion vectors:

```sql
-- Get files with their active deletion vectors for a transaction
SELECT 
    f.file_uuid,
    f.file_path,
    f.file_format,
    f.record_count,
    dv.deletion_vector_path,
    dv.cardinality as deleted_count
FROM files f
LEFT JOIN deletion_vectors dv ON dv.file_uuid = f.file_uuid
    AND dv.added_in_transaction_id <= :txn_id
    AND (dv.removed_in_transaction_id IS NULL 
         OR dv.removed_in_transaction_id > :txn_id)
WHERE f.table_uuid = :table_uuid
    AND f.added_in_transaction_id <= :txn_id
    AND (f.removed_in_transaction_id IS NULL 
         OR f.removed_in_transaction_id > :txn_id);
```

### Applying Deletion Vectors During Scan

```rust
pub struct FileWithDeletions {
    pub file: File,
    pub deletion_vector: Option<DeletionVector>,
}

impl FileWithDeletions {
    /// Effective record count after deletions
    pub fn effective_record_count(&self) -> i64 {
        let deleted = self.deletion_vector
            .as_ref()
            .map(|dv| dv.cardinality() as i64)
            .unwrap_or(0);
        self.file.record_count - deleted
    }
}

pub struct DeletionAwareReader {
    inner: Box<dyn RecordBatchReader>,
    deletion_vector: Option<DeletionVector>,
    current_row_offset: u32,
}

impl DeletionAwareReader {
    pub fn new(
        inner: Box<dyn RecordBatchReader>,
        deletion_vector: Option<DeletionVector>,
    ) -> Self {
        Self {
            inner,
            deletion_vector,
            current_row_offset: 0,
        }
    }
}

impl Iterator for DeletionAwareReader {
    type Item = Result<RecordBatch, ArrowError>;
    
    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.inner.next()?;
        
        match batch {
            Ok(batch) => {
                let filtered = match &self.deletion_vector {
                    Some(dv) => {
                        // Build selection vector for non-deleted rows
                        let num_rows = batch.num_rows() as u32;
                        let mut selection = Vec::with_capacity(num_rows as usize);
                        
                        for i in 0..num_rows {
                            let global_row = self.current_row_offset + i;
                            if !dv.is_deleted(global_row) {
                                selection.push(i as usize);
                            }
                        }
                        
                        self.current_row_offset += num_rows;
                        
                        // Use Arrow's take kernel to filter
                        filter_record_batch(&batch, &selection)
                    }
                    None => {
                        self.current_row_offset += batch.num_rows() as u32;
                        Ok(batch)
                    }
                };
                
                Some(filtered)
            }
            Err(e) => Some(Err(e)),
        }
    }
}
```

### Statistics Adjustment

When deletion vectors are present, reported statistics must account for deletions:

```rust
impl FileWithDeletions {
    /// Adjusted column statistics accounting for deletions
    pub fn adjusted_stats(&self) -> Option<AdjustedColumnStats> {
        // If no deletions, return original stats
        if self.deletion_vector.is_none() {
            return Some(AdjustedColumnStats::from(&self.file.column_stats));
        }
        
        // With deletions, min/max may be invalid (deleted rows might hold extremes)
        // Conservative approach: invalidate min/max, adjust counts
        let dv = self.deletion_vector.as_ref().unwrap();
        
        Some(AdjustedColumnStats {
            null_count: None,  // Cannot know without re-scanning
            nan_count: None,
            min_value: None,   // May have been deleted
            max_value: None,   // May have been deleted
            distinct_count: None,
            row_count: self.effective_record_count(),
        })
    }
}
```

**Note**: Accurate statistics after deletions require either:
1. Re-scanning the file (expensive)
2. Materializing deletions through compaction
3. Maintaining incremental statistics (complex)

The conservative approach (invalidating min/max) is recommended initially.

## Compaction Integration

### When to Materialize Deletions

Compaction should materialize deletions when:

1. **Deletion ratio exceeds threshold**: e.g., >20% of file rows are deleted
2. **Deletion vector size is large**: e.g., DV file exceeds 1MB
3. **During regular compaction**: Always materialize deletions when rewriting files

```rust
pub struct CompactionPolicy {
    /// Materialize deletions when this fraction of rows are deleted
    pub deletion_ratio_threshold: f64,
    /// Materialize deletions when DV exceeds this size in bytes
    pub max_deletion_vector_size: usize,
}

impl CompactionPolicy {
    pub fn should_materialize(&self, file: &FileWithDeletions) -> bool {
        if let Some(dv) = &file.deletion_vector {
            let deletion_ratio = dv.cardinality() as f64 / file.file.record_count as f64;
            if deletion_ratio >= self.deletion_ratio_threshold {
                return true;
            }
            
            // Check DV size (approximate from cardinality)
            let estimated_dv_size = dv.deleted_rows.serialized_size();
            if estimated_dv_size >= self.max_deletion_vector_size {
                return true;
            }
        }
        
        false
    }
}
```

### Compaction Protocol with Deletion Vectors

```
1. Select files for compaction (including those with deletions)
2. For each file:
   a. Read data file
   b. Load deletion vector (if any)
   c. Filter out deleted rows during read
3. Write new file(s) with only live rows
4. Commit transaction:
   a. Add new files
   b. Remove old files (set removed_in_transaction_id)
   c. Remove old deletion vectors (set removed_in_transaction_id)
```

After compaction, the new files have no deletion vectors (all rows are live).

## Transaction Semantics

### Conflict Detection

Delete operations can conflict with:

1. **Concurrent deletes to same file**: Both transactions may create DVs for the same file. Resolution: merge DVs (union of deleted rows).

2. **Concurrent file removal**: If another transaction removed the file, the delete is void.

3. **Concurrent compaction**: If another transaction compacted the file, row positions may have changed.

For conflict detection during commit:

```rust
impl SqlCatalog {
    async fn validate_delete_conflicts(
        &self,
        table_uuid: Uuid,
        base_txn: Uuid,
        current_txn: Uuid,
        deletes: &[DeleteOperation],
    ) -> Result<(), CatalogError> {
        // Check each affected file
        for delete in deletes {
            // Verify file still exists at current transaction
            let file_exists = self.file_exists_at(delete.file_uuid, current_txn).await?;
            if !file_exists {
                return Err(CatalogError::Conflict(format!(
                    "File {} was removed by concurrent transaction",
                    delete.file_uuid
                )));
            }
            
            // Check if file was compacted (replaced by new file)
            let was_compacted = self.file_was_compacted(delete.file_uuid, base_txn, current_txn).await?;
            if was_compacted {
                return Err(CatalogError::Conflict(format!(
                    "File {} was compacted by concurrent transaction, row positions may be invalid",
                    delete.file_uuid
                )));
            }
        }
        
        Ok(())
    }
}
```

### Concurrent Delete Resolution

When two transactions both delete from the same file:

```rust
impl SqlCatalog {
    async fn resolve_concurrent_deletes(
        &self,
        file_uuid: Uuid,
        my_dv: &DeletionVector,
        current_txn: Uuid,
    ) -> Result<DeletionVector, CatalogError> {
        // Check if there's a DV added between base and current
        if let Some(concurrent_dv) = self.get_deletion_vector_added_after(file_uuid, base_txn, current_txn).await? {
            // Merge: union of both deletion sets
            let mut merged = my_dv.clone();
            let other = self.load_deletion_vector(&concurrent_dv.deletion_vector_path).await?;
            merged.merge(&other);
            return Ok(merged);
        }
        
        Ok(my_dv.clone())
    }
}
```

## Time Travel

Deletion vectors support time travel like other metadata:

```rust
impl SqlCatalog {
    pub async fn read_at(
        &self,
        table_uuid: Uuid,
        transaction_id: Uuid,
    ) -> Result<Vec<FileWithDeletions>, CatalogError> {
        // Get files visible at transaction
        let files = self.list_files_at(table_uuid, transaction_id).await?;
        
        // For each file, get the deletion vector active at that transaction
        let mut result = Vec::new();
        for file in files {
            let dv = self.get_deletion_vector_at(file.file_uuid, transaction_id).await?;
            result.push(FileWithDeletions {
                file,
                deletion_vector: dv,
            });
        }
        
        Ok(result)
    }
}
```

## Integration with Other Components

### CDC Events

Deletion vectors enable row-level DELETE events in CDC:

```rust
pub enum CdcEvent {
    Insert { row: Row, file_uuid: Uuid, row_position: u32 },
    Delete { row: Row, file_uuid: Uuid, row_position: u32 },
    // UPDATE = DELETE + INSERT
}
```

See [cdc.md](cdc.md) for full CDC design.

### Statistics

When deletion vectors are present:
- `record_count` in `files` table reflects original count
- Effective count = `record_count - deletion_vector.cardinality`
- Min/max statistics become unreliable (may reference deleted rows)

### Streaming Buffer

The streaming buffer can accumulate deletes in memory before flushing to deletion vectors. See [streaming_buffer.md](streaming_buffer.md).

## Implementation Phases

### Phase 1: Basic Deletion Vectors

1. Add `deletion_vectors` table to schema
2. Implement `DeletionVector` serialization/deserialization
3. Add deletion vector support to read path
4. Implement `delete_rows` operation
5. Add integration tests

### Phase 2: Predicate Deletes

1. Implement `delete_where` with predicate scanning
2. Add statistics-based file pruning for deletes
3. Add conflict detection and resolution

### Phase 3: Compaction Integration

1. Add deletion ratio threshold to compaction policy
2. Implement deletion materialization during compaction
3. Add metrics for deletion vector overhead

### Phase 4: Optimizations

1. Lazy deletion vector loading (only load when needed)
2. Deletion vector caching
3. Batched deletion vector updates (multiple deletes in one DV)
4. Statistics maintenance after deletions

## Testing Strategy

### Unit Tests

- Deletion vector serialization round-trip
- Roaring bitmap operations (insert, contains, merge)
- Read path filtering with deletion vectors
- Conflict detection and resolution

### Integration Tests

- End-to-end delete operation
- Time travel with deletion vectors
- Concurrent delete handling
- Compaction with deletion vectors

### Performance Tests

- Deletion vector size vs. cardinality
- Read performance with various deletion ratios
- Compaction performance with deletion vectors

## Open Questions

1. **Row position stability**: How do we handle row positions when files use different orderings or encodings? Should we use a stable row identifier instead of position?

2. **Update semantics**: Should UPDATE be implemented as DELETE + INSERT, or should there be a separate update mechanism?

3. **Deletion vector format versioning**: How do we handle format changes? The version byte provides forward compatibility, but what's the upgrade path?

4. **Multi-file deletes atomicity**: When deleting rows across multiple files, should all-or-nothing semantics be enforced, or can partial deletes be allowed?

5. **Deletion vector garbage collection**: When is it safe to physically delete old deletion vector files? Same retention policy as data files?

## References

- [Delta Lake Deletion Vectors](https://docs.delta.io/latest/delta-deletion-vectors.html)
- [Apache Iceberg Position Delete Files](https://iceberg.apache.org/spec/#position-delete-files)
- [Roaring Bitmaps](https://roaringbitmap.org/)
- [db_control_plane.md](db_control_plane.md) - Transaction and commit protocol
- [data_types.md](data_types.md) - Type system for predicate evaluation
