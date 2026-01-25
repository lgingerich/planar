# Partitioning Strategy

## Purpose

This document specifies Planar's partitioning system. Partitioning organizes data files by column values, enabling efficient query pruning and improved data locality for common access patterns.

## Motivation

Without partitioning, queries must scan all files to find matching rows. For large tables, this becomes prohibitively expensive:

1. **Query performance**: A query filtering on `date = '2024-01-15'` should only read files containing that date, not the entire table.

2. **Data locality**: Files grouped by partition values can be managed together (e.g., drop all data for a specific customer).

3. **Maintenance efficiency**: Operations like compaction and retention can target specific partitions.

4. **Parallel processing**: Different partitions can be processed independently.

Planar supports partitioning through metadata-driven partition pruning, where partition values are stored in the control plane and used to filter files before reading.

## Design Principles

1. **Metadata-driven**: Partition values are stored in the `files` table, not encoded in file paths. This enables flexible partition schemes without physical file reorganization.

2. **Transform-based**: Partition values can be derived from source columns using transforms (year, month, bucket, truncate), similar to Iceberg's hidden partitioning.

3. **Evolution-friendly**: Partition schemes can evolve without rewriting data. New partition specs apply to new files only.

4. **Query-transparent**: Queries don't need to know about partitioning; the optimizer handles pruning automatically.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane (DB)                             │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        partition_specs                               │   │
│  │  spec_id | table_uuid | spec_version | fields (JSON)                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                            files                                     │   │
│  │  file_uuid | ... | partition_spec_id | partition_values (JSON)      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                            Partition │ Pruning
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Data Plane (Object Storage)                       │
│                                                                             │
│   s3://bucket/tables/{uuid}/data/00001.parquet  (year=2024, month=1)        │
│   s3://bucket/tables/{uuid}/data/00002.parquet  (year=2024, month=2)        │
│   s3://bucket/tables/{uuid}/data/00003.parquet  (year=2024, month=2)        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Design Options

### Option 1: Hive-Style Partitioning

Encode partition values in file paths: `s3://bucket/table/year=2024/month=01/data.parquet`

**Advantages**:
- Widely understood convention
- Works with external tools that expect Hive paths
- Visual organization in object storage

**Disadvantages**:
- Path changes require file moves (expensive)
- Partition evolution requires data migration
- Naming collisions with special characters
- Limited transform support

### Option 2: Metadata-Only Partitioning (Recommended)

Store partition values in metadata; files can be anywhere.

**Advantages**:
- No file movement for partition changes
- Flexible transform support
- Partition evolution without rewrite
- Works with any file naming convention

**Disadvantages**:
- External tools can't infer partitions from paths
- Slightly more metadata per file

### Option 3: Hybrid Approach

Store partition values in both paths and metadata.

**Advantages**:
- Best of both worlds
- External tool compatibility

**Disadvantages**:
- Complexity of keeping path and metadata in sync
- Extra storage for redundant information

### Current Recommendation

**Option 2 (Metadata-Only)** is recommended:
- Aligns with Planar's metadata-driven architecture
- Enables partition evolution without data movement
- Supports complex transforms (bucket, truncate)
- External access can query metadata for partition values

## Partition Specification

### Partition Field

A partition field defines how to derive a partition value from a source column:

```rust
/// A single partition field
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionField {
    /// Unique identifier within the spec
    pub field_id: u32,
    /// Source column name
    pub source_column: String,
    /// Transform to apply (identity if None)
    pub transform: PartitionTransform,
    /// Name of the partition field (defaults to column name)
    pub name: String,
}

/// Transform functions for partition values
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PartitionTransform {
    /// Use value as-is
    Identity,
    /// Extract year from timestamp/date
    Year,
    /// Extract month from timestamp/date (YYYYMM format)
    Month,
    /// Extract day from timestamp/date (YYYYMMDD format)
    Day,
    /// Extract hour from timestamp (YYYYMMDDHH format)
    Hour,
    /// Hash into N buckets
    Bucket(u32),
    /// Truncate to width (for strings: characters, for numbers: significant figures)
    Truncate(u32),
    /// Void transform (always produces null, used for dropping partition fields)
    Void,
}
```

### Partition Specification

A partition spec is a collection of partition fields:

```rust
/// A partition specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// Unique spec identifier
    pub spec_id: u32,
    /// Ordered list of partition fields
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create an unpartitioned spec
    pub fn unpartitioned() -> Self {
        Self {
            spec_id: 0,
            fields: Vec::new(),
        }
    }
    
    /// Check if spec has any partition fields
    pub fn is_partitioned(&self) -> bool {
        !self.fields.is_empty() && 
        !self.fields.iter().all(|f| matches!(f.transform, PartitionTransform::Void))
    }
    
    /// Compute partition values for a row
    pub fn partition_values(&self, row: &Row, schema: &Schema) -> Result<PartitionValues, PartitionError> {
        let mut values = HashMap::new();
        
        for field in &self.fields {
            let source_value = row.get(&field.source_column)
                .ok_or_else(|| PartitionError::MissingColumn(field.source_column.clone()))?;
            
            let partition_value = apply_transform(&field.transform, source_value, schema)?;
            values.insert(field.name.clone(), partition_value);
        }
        
        Ok(PartitionValues(values))
    }
}

/// Partition values for a file
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionValues(pub HashMap<String, PartitionValue>);

/// A single partition value
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PartitionValue {
    Null,
    Boolean(bool),
    Int(i64),
    String(String),
    Binary(Vec<u8>),
}
```

### Transform Implementation

```rust
/// Apply a transform to a source value
pub fn apply_transform(
    transform: &PartitionTransform,
    value: &ScalarValue,
    schema: &Schema,
) -> Result<PartitionValue, PartitionError> {
    match transform {
        PartitionTransform::Identity => {
            scalar_to_partition_value(value)
        }
        
        PartitionTransform::Year => {
            match value {
                ScalarValue::Date32(days) => {
                    let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163)
                        .ok_or(PartitionError::InvalidDate)?;
                    Ok(PartitionValue::Int(date.year() as i64))
                }
                ScalarValue::Timestamp(micros, _, _) => {
                    let dt = DateTime::from_timestamp_micros(*micros)
                        .ok_or(PartitionError::InvalidTimestamp)?;
                    Ok(PartitionValue::Int(dt.year() as i64))
                }
                ScalarValue::Null => Ok(PartitionValue::Null),
                _ => Err(PartitionError::InvalidTransform("Year requires date/timestamp".into())),
            }
        }
        
        PartitionTransform::Month => {
            match value {
                ScalarValue::Date32(days) => {
                    let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163)
                        .ok_or(PartitionError::InvalidDate)?;
                    let month_value = date.year() * 12 + date.month() as i32 - 1;
                    Ok(PartitionValue::Int(month_value as i64))
                }
                ScalarValue::Timestamp(micros, _, _) => {
                    let dt = DateTime::from_timestamp_micros(*micros)
                        .ok_or(PartitionError::InvalidTimestamp)?;
                    let month_value = dt.year() * 12 + dt.month() as i32 - 1;
                    Ok(PartitionValue::Int(month_value as i64))
                }
                ScalarValue::Null => Ok(PartitionValue::Null),
                _ => Err(PartitionError::InvalidTransform("Month requires date/timestamp".into())),
            }
        }
        
        PartitionTransform::Day => {
            match value {
                ScalarValue::Date32(days) => {
                    Ok(PartitionValue::Int(*days as i64))
                }
                ScalarValue::Timestamp(micros, _, _) => {
                    let days = *micros / (1_000_000 * 60 * 60 * 24);
                    Ok(PartitionValue::Int(days))
                }
                ScalarValue::Null => Ok(PartitionValue::Null),
                _ => Err(PartitionError::InvalidTransform("Day requires date/timestamp".into())),
            }
        }
        
        PartitionTransform::Hour => {
            match value {
                ScalarValue::Timestamp(micros, _, _) => {
                    let hours = *micros / (1_000_000 * 60 * 60);
                    Ok(PartitionValue::Int(hours))
                }
                ScalarValue::Null => Ok(PartitionValue::Null),
                _ => Err(PartitionError::InvalidTransform("Hour requires timestamp".into())),
            }
        }
        
        PartitionTransform::Bucket(n) => {
            if *n == 0 {
                return Err(PartitionError::InvalidTransform("Bucket count must be > 0".into()));
            }
            
            match value {
                ScalarValue::Null => Ok(PartitionValue::Null),
                _ => {
                    let hash = murmur3_hash(value)?;
                    let bucket = (hash & i32::MAX as u32) % n;
                    Ok(PartitionValue::Int(bucket as i64))
                }
            }
        }
        
        PartitionTransform::Truncate(width) => {
            match value {
                ScalarValue::String(s) => {
                    let truncated: String = s.chars().take(*width as usize).collect();
                    Ok(PartitionValue::String(truncated))
                }
                ScalarValue::Int64(v) => {
                    let truncated = (*v / *width as i64) * *width as i64;
                    Ok(PartitionValue::Int(truncated))
                }
                ScalarValue::Int32(v) => {
                    let truncated = (*v as i64 / *width as i64) * *width as i64;
                    Ok(PartitionValue::Int(truncated))
                }
                ScalarValue::Null => Ok(PartitionValue::Null),
                _ => Err(PartitionError::InvalidTransform("Truncate requires string or integer".into())),
            }
        }
        
        PartitionTransform::Void => {
            Ok(PartitionValue::Null)
        }
    }
}

/// Murmur3 hash for bucket transform
fn murmur3_hash(value: &ScalarValue) -> Result<u32, PartitionError> {
    use murmur3::murmur3_32;
    use std::io::Cursor;
    
    let bytes = scalar_to_bytes(value)?;
    let hash = murmur3_32(&mut Cursor::new(&bytes), 0)
        .map_err(|e| PartitionError::HashError(e.to_string()))?;
    
    Ok(hash)
}
```

## Schema Changes

### New Table: `partition_specs`

```sql
CREATE TABLE IF NOT EXISTS partition_specs (
    spec_id INTEGER NOT NULL,
    table_uuid BLOB NOT NULL,
    spec_version INTEGER NOT NULL,
    fields TEXT NOT NULL, -- JSON array of PartitionField
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (table_uuid, spec_id),
    FOREIGN KEY (table_uuid) REFERENCES tables(table_uuid)
);

CREATE INDEX IF NOT EXISTS idx_partition_specs_table
    ON partition_specs(table_uuid);
```

### Modified Table: `files`

```sql
-- Add partition spec reference and partition values
ALTER TABLE files ADD COLUMN partition_spec_id INTEGER;
-- partition_values column already exists as TEXT (JSON)
```

### Table Properties

```sql
-- Current partition spec stored in table properties
UPDATE tables 
SET properties = json_set(properties, '$.current_partition_spec_id', 1)
WHERE table_uuid = ?;
```

## Partition Pruning

### Query Planning

When a query has predicates on partition columns, the optimizer rewrites the query to skip non-matching files:

```rust
/// Partition pruning during query planning
pub struct PartitionPruner {
    spec: PartitionSpec,
}

impl PartitionPruner {
    /// Check if a file might contain rows matching the predicate
    pub fn file_may_match(
        &self,
        file: &File,
        predicate: &Predicate,
    ) -> Result<bool, PruneError> {
        // Get file's partition values
        let partition_values: PartitionValues = serde_json::from_str(
            file.partition_values.as_deref().unwrap_or("{}")
        )?;
        
        // Check each predicate against partition values
        for pred in predicate.conjuncts() {
            if let Some(partition_pred) = self.convert_to_partition_predicate(pred)? {
                if !partition_pred.matches(&partition_values) {
                    return Ok(false); // Definitely doesn't match
                }
            }
        }
        
        Ok(true) // May match
    }
    
    /// Convert a data predicate to a partition predicate
    fn convert_to_partition_predicate(
        &self,
        predicate: &Predicate,
    ) -> Result<Option<PartitionPredicate>, PruneError> {
        match predicate {
            Predicate::Eq(column, value) => {
                // Find partition field for this column
                if let Some(field) = self.spec.fields.iter().find(|f| f.source_column == *column) {
                    let partition_value = apply_transform(&field.transform, value, &Schema::empty())?;
                    Ok(Some(PartitionPredicate::Eq(field.name.clone(), partition_value)))
                } else {
                    Ok(None) // Not a partition column
                }
            }
            
            Predicate::Lt(column, value) => {
                if let Some(field) = self.spec.fields.iter().find(|f| f.source_column == *column) {
                    // For monotonic transforms (year, month, day, hour, truncate), Lt propagates
                    if field.transform.is_monotonic() {
                        let partition_value = apply_transform(&field.transform, value, &Schema::empty())?;
                        Ok(Some(PartitionPredicate::Le(field.name.clone(), partition_value)))
                    } else {
                        Ok(None) // Can't prune with non-monotonic transform
                    }
                } else {
                    Ok(None)
                }
            }
            
            Predicate::Gt(column, value) => {
                if let Some(field) = self.spec.fields.iter().find(|f| f.source_column == *column) {
                    if field.transform.is_monotonic() {
                        let partition_value = apply_transform(&field.transform, value, &Schema::empty())?;
                        Ok(Some(PartitionPredicate::Ge(field.name.clone(), partition_value)))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            
            Predicate::In(column, values) => {
                if let Some(field) = self.spec.fields.iter().find(|f| f.source_column == *column) {
                    let partition_values: Vec<PartitionValue> = values.iter()
                        .map(|v| apply_transform(&field.transform, v, &Schema::empty()))
                        .collect::<Result<_, _>>()?;
                    Ok(Some(PartitionPredicate::In(field.name.clone(), partition_values)))
                } else {
                    Ok(None)
                }
            }
            
            Predicate::IsNull(column) => {
                if let Some(field) = self.spec.fields.iter().find(|f| f.source_column == *column) {
                    Ok(Some(PartitionPredicate::IsNull(field.name.clone())))
                } else {
                    Ok(None)
                }
            }
            
            _ => Ok(None), // Can't prune with this predicate type
        }
    }
}

/// Predicate operating on partition values
#[derive(Clone, Debug)]
pub enum PartitionPredicate {
    Eq(String, PartitionValue),
    Ne(String, PartitionValue),
    Lt(String, PartitionValue),
    Le(String, PartitionValue),
    Gt(String, PartitionValue),
    Ge(String, PartitionValue),
    In(String, Vec<PartitionValue>),
    IsNull(String),
    IsNotNull(String),
}

impl PartitionPredicate {
    /// Check if partition values match this predicate
    pub fn matches(&self, values: &PartitionValues) -> bool {
        match self {
            PartitionPredicate::Eq(name, expected) => {
                values.0.get(name).map(|v| v == expected).unwrap_or(false)
            }
            PartitionPredicate::Ne(name, expected) => {
                values.0.get(name).map(|v| v != expected).unwrap_or(true)
            }
            PartitionPredicate::Lt(name, bound) => {
                values.0.get(name).map(|v| v < bound).unwrap_or(false)
            }
            PartitionPredicate::Le(name, bound) => {
                values.0.get(name).map(|v| v <= bound).unwrap_or(false)
            }
            PartitionPredicate::Gt(name, bound) => {
                values.0.get(name).map(|v| v > bound).unwrap_or(false)
            }
            PartitionPredicate::Ge(name, bound) => {
                values.0.get(name).map(|v| v >= bound).unwrap_or(false)
            }
            PartitionPredicate::In(name, allowed) => {
                values.0.get(name).map(|v| allowed.contains(v)).unwrap_or(false)
            }
            PartitionPredicate::IsNull(name) => {
                values.0.get(name).map(|v| *v == PartitionValue::Null).unwrap_or(true)
            }
            PartitionPredicate::IsNotNull(name) => {
                values.0.get(name).map(|v| *v != PartitionValue::Null).unwrap_or(false)
            }
        }
    }
}
```

### SQL-Based Pruning

Partition pruning can be pushed to the database query:

```sql
-- List files matching partition predicate
SELECT f.*
FROM files f
WHERE f.table_uuid = :table_uuid
  AND f.added_in_transaction_id <= :txn_id
  AND (f.removed_in_transaction_id IS NULL OR f.removed_in_transaction_id > :txn_id)
  -- Partition filter pushed to SQL
  AND json_extract(f.partition_values, '$.year') = :year_value
  AND json_extract(f.partition_values, '$.month') >= :month_start
  AND json_extract(f.partition_values, '$.month') <= :month_end;
```

## Partition Evolution

### Evolution Operations

```rust
/// Operations for evolving partition specs
pub enum PartitionEvolution {
    /// Add a new partition field
    AddField(PartitionField),
    /// Remove a partition field (converts to Void transform)
    RemoveField(String),
    /// Replace a partition field's transform
    ReplaceTransform { field_name: String, new_transform: PartitionTransform },
}

impl SqlCatalog {
    /// Evolve partition specification
    pub async fn evolve_partition_spec(
        &self,
        table_uuid: Uuid,
        evolutions: Vec<PartitionEvolution>,
    ) -> Result<PartitionSpec, CatalogError> {
        let table = self.get_table(table_uuid).await?;
        let current_spec = self.get_partition_spec(table_uuid, table.current_partition_spec_id()).await?;
        
        let mut new_fields = current_spec.fields.clone();
        let mut next_field_id = new_fields.iter().map(|f| f.field_id).max().unwrap_or(0) + 1;
        
        for evolution in evolutions {
            match evolution {
                PartitionEvolution::AddField(mut field) => {
                    // Assign new field ID
                    field.field_id = next_field_id;
                    next_field_id += 1;
                    new_fields.push(field);
                }
                
                PartitionEvolution::RemoveField(name) => {
                    // Convert to Void transform (preserves field ID)
                    if let Some(field) = new_fields.iter_mut().find(|f| f.name == name) {
                        field.transform = PartitionTransform::Void;
                    }
                }
                
                PartitionEvolution::ReplaceTransform { field_name, new_transform } => {
                    if let Some(field) = new_fields.iter_mut().find(|f| f.name == field_name) {
                        field.transform = new_transform;
                    }
                }
            }
        }
        
        // Create new spec version
        let new_spec = PartitionSpec {
            spec_id: current_spec.spec_id + 1,
            fields: new_fields,
        };
        
        // Save new spec
        self.save_partition_spec(table_uuid, &new_spec).await?;
        
        // Update table's current spec
        self.update_table_partition_spec(table_uuid, new_spec.spec_id).await?;
        
        Ok(new_spec)
    }
}
```

### Evolution Compatibility

When reading tables with evolved partition specs:

```rust
/// Handle files written with different partition specs
pub fn normalize_partition_values(
    file: &File,
    file_spec: &PartitionSpec,
    current_spec: &PartitionSpec,
) -> PartitionValues {
    let file_values: PartitionValues = serde_json::from_str(
        file.partition_values.as_deref().unwrap_or("{}")
    ).unwrap_or_default();
    
    let mut normalized = HashMap::new();
    
    for current_field in &current_spec.fields {
        // Find corresponding field in file's spec
        let file_field = file_spec.fields.iter()
            .find(|f| f.field_id == current_field.field_id);
        
        let value = if let Some(ff) = file_field {
            // Field existed when file was written
            file_values.0.get(&ff.name).cloned().unwrap_or(PartitionValue::Null)
        } else {
            // Field was added after file was written
            PartitionValue::Null
        };
        
        normalized.insert(current_field.name.clone(), value);
    }
    
    PartitionValues(normalized)
}
```

## Write Path

### Partitioned Writes

When writing data to a partitioned table:

```rust
impl SqlCatalog {
    /// Write partitioned data
    pub async fn write_partitioned(
        &self,
        table_uuid: Uuid,
        data: RecordBatch,
    ) -> Result<Vec<FileSpec>, CatalogError> {
        let table = self.get_table(table_uuid).await?;
        let spec = self.get_partition_spec(table_uuid, table.current_partition_spec_id()).await?;
        let schema = self.get_current_schema(table_uuid).await?;
        
        if !spec.is_partitioned() {
            // Non-partitioned table: write single file
            let file_path = self.generate_file_path(&table, None).await?;
            return Ok(vec![FileSpec {
                path: file_path,
                data,
                partition_values: None,
            }]);
        }
        
        // Partition the data
        let partitioned = partition_record_batch(&data, &spec, &schema)?;
        
        let mut file_specs = Vec::new();
        for (partition_values, partition_data) in partitioned {
            let file_path = self.generate_file_path(&table, Some(&partition_values)).await?;
            file_specs.push(FileSpec {
                path: file_path,
                data: partition_data,
                partition_values: Some(partition_values),
            });
        }
        
        Ok(file_specs)
    }
}

/// Partition a RecordBatch by partition spec
fn partition_record_batch(
    batch: &RecordBatch,
    spec: &PartitionSpec,
    schema: &Schema,
) -> Result<Vec<(PartitionValues, RecordBatch)>, PartitionError> {
    // Compute partition value for each row
    let mut partition_indices: HashMap<PartitionValues, Vec<usize>> = HashMap::new();
    
    for row_idx in 0..batch.num_rows() {
        let row = extract_row(batch, row_idx)?;
        let partition_values = spec.partition_values(&row, schema)?;
        
        partition_indices.entry(partition_values)
            .or_insert_with(Vec::new)
            .push(row_idx);
    }
    
    // Create a batch for each partition
    let mut result = Vec::new();
    for (partition_values, indices) in partition_indices {
        let indices_array = UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
        let partition_batch = take_record_batch(batch, &indices_array)?;
        result.push((partition_values, partition_batch));
    }
    
    Ok(result)
}
```

## Partition Statistics

### Per-Partition Statistics

```sql
-- Aggregate statistics per partition
CREATE TABLE IF NOT EXISTS partition_stats (
    table_uuid BLOB NOT NULL,
    partition_values TEXT NOT NULL, -- JSON
    transaction_id BLOB NOT NULL,
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    file_count INTEGER NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    PRIMARY KEY (table_uuid, partition_values),
    FOREIGN KEY (table_uuid) REFERENCES tables(table_uuid),
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
);
```

### Statistics Computation

```rust
impl SqlCatalog {
    /// Compute statistics for all partitions
    pub async fn compute_partition_stats(
        &self,
        table_uuid: Uuid,
        transaction_id: Uuid,
    ) -> Result<Vec<PartitionStats>, CatalogError> {
        let files = self.list_files_at(table_uuid, transaction_id).await?;
        
        let mut stats_by_partition: HashMap<String, PartitionStats> = HashMap::new();
        
        for file in files {
            let partition_key = file.partition_values.clone().unwrap_or_else(|| "{}".to_string());
            
            let stats = stats_by_partition.entry(partition_key.clone()).or_insert_with(|| {
                PartitionStats {
                    table_uuid,
                    partition_values: partition_key,
                    transaction_id,
                    record_count: 0,
                    file_size_bytes: 0,
                    file_count: 0,
                    last_updated: Utc::now(),
                }
            });
            
            stats.record_count += file.record_count;
            stats.file_size_bytes += file.file_size_bytes;
            stats.file_count += 1;
        }
        
        Ok(stats_by_partition.into_values().collect())
    }
}
```

## Partition Maintenance

### Dropping Partitions

```rust
impl SqlCatalog {
    /// Drop all data in a partition
    pub async fn drop_partition(
        &self,
        table_uuid: Uuid,
        base_transaction_id: Uuid,
        partition_predicate: &PartitionPredicate,
    ) -> Result<Uuid, CatalogError> {
        // Find all files matching the partition
        let files = self.list_files_at(table_uuid, base_transaction_id).await?;
        let spec = self.get_current_partition_spec(table_uuid).await?;
        let pruner = PartitionPruner { spec };
        
        let mut mutations = Vec::new();
        
        for file in files {
            let partition_values: PartitionValues = serde_json::from_str(
                file.partition_values.as_deref().unwrap_or("{}")
            )?;
            
            if partition_predicate.matches(&partition_values) {
                mutations.push(MutationOp::RemoveFile { file_uuid: file.file_uuid });
            }
        }
        
        // Commit the partition drop
        self.commit(table_uuid, base_transaction_id, mutations).await
    }
}
```

### Per-Partition Compaction

```rust
impl CompactionPlanner {
    /// Plan compaction for a specific partition
    pub async fn plan_partition_compaction(
        &self,
        table_uuid: Uuid,
        partition_values: &PartitionValues,
        transaction_id: Uuid,
    ) -> Result<CompactionPlan, CompactionError> {
        let all_files = self.catalog.list_files_at(table_uuid, transaction_id).await?;
        
        // Filter to partition
        let partition_files: Vec<_> = all_files.into_iter()
            .filter(|f| {
                let file_pv: PartitionValues = serde_json::from_str(
                    f.partition_values.as_deref().unwrap_or("{}")
                ).unwrap_or_default();
                &file_pv == partition_values
            })
            .collect();
        
        // Apply standard compaction planning to partition files
        self.plan_file_compaction(partition_files).await
    }
}
```

## API Examples

### Create Partitioned Table

```rust
// Create a table partitioned by year and month
let partition_spec = PartitionSpec {
    spec_id: 1,
    fields: vec![
        PartitionField {
            field_id: 1,
            source_column: "created_at".to_string(),
            transform: PartitionTransform::Year,
            name: "year".to_string(),
        },
        PartitionField {
            field_id: 2,
            source_column: "created_at".to_string(),
            transform: PartitionTransform::Month,
            name: "month".to_string(),
        },
    ],
};

catalog.create_table(CreateTableRequest {
    name: "events".to_string(),
    namespace: "analytics".to_string(),
    schema: schema,
    partition_spec: Some(partition_spec),
    ..Default::default()
}).await?;
```

### Query with Partition Pruning

```rust
// Query that benefits from partition pruning
let predicate = Predicate::And(
    Box::new(Predicate::Ge("created_at", ScalarValue::Date32(19000))), // 2022-01-01
    Box::new(Predicate::Lt("created_at", ScalarValue::Date32(19365))), // 2023-01-01
);

// Optimizer converts to partition predicate: year = 2022
let files = catalog.list_files_with_pruning(table_uuid, txn_id, &predicate).await?;
// Only returns files in year=2022 partitions
```

### Evolve Partition Spec

```rust
// Add bucket partitioning on customer_id
catalog.evolve_partition_spec(table_uuid, vec![
    PartitionEvolution::AddField(PartitionField {
        field_id: 0, // Will be assigned
        source_column: "customer_id".to_string(),
        transform: PartitionTransform::Bucket(16),
        name: "customer_bucket".to_string(),
    }),
]).await?;
```

## Implementation Phases

### Phase 1: Basic Partitioning

1. Add `partition_specs` table
2. Implement partition spec storage
3. Implement identity transform
4. Add partition values to file writes
5. Basic partition pruning in file listing

### Phase 2: Transform Functions

1. Implement temporal transforms (year, month, day, hour)
2. Implement bucket transform with Murmur3 hash
3. Implement truncate transform
4. Add transform validation

### Phase 3: Partition Evolution

1. Implement spec versioning
2. Add evolution operations
3. Handle files with different specs
4. Add void transform for field removal

### Phase 4: Optimization

1. SQL-based partition pruning
2. Per-partition statistics
3. Partition-aware compaction
4. Dynamic partition pruning

## Testing Strategy

### Unit Tests

- Transform function correctness
- Partition predicate evaluation
- Partition value serialization
- Evolution operation validity

### Integration Tests

- End-to-end partitioned writes
- Partition pruning effectiveness
- Partition evolution compatibility
- Mixed-spec file reading

### Performance Tests

- Pruning effectiveness (measure files skipped)
- Partition overhead (compare to non-partitioned)
- Large partition counts

## Open Questions

1. **Partition count limits**: Should there be a maximum number of partitions? High-cardinality partitioning can create millions of files.

2. **Partition sorting**: Should files within a partition be sorted? This could improve query performance but adds write overhead.

3. **Partition balancing**: How do we handle partition skew (one partition much larger than others)?

4. **Nested partitioning**: Should transforms support nested columns (e.g., `struct_col.timestamp_field`)?

5. **Expression transforms**: Should we support arbitrary expressions as transforms, not just predefined functions?

## References

- [Apache Iceberg Partitioning](https://iceberg.apache.org/spec/#partitioning)
- [Delta Lake Partitioning](https://docs.delta.io/latest/delta-batch.html#partition-data)
- [Hive Partitioning](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables)
- [db_control_plane.md](db_control_plane.md) - Transaction and file management
- [compaction.md](compaction.md) - Partition-aware compaction
