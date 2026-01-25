# Compaction Strategies

## Purpose

This document specifies Planar's compaction system. Compaction rewrites data files to improve read performance, reduce storage overhead, and materialize pending deletions from deletion vectors.

## Motivation

Over time, tables accumulate inefficiencies that degrade performance:

1. **Small files**: Frequent small writes create many tiny files. Each file has overhead (metadata, object storage API calls), and readers must open many files.

2. **Deletion vectors**: Row-level deletes accumulate in deletion vectors. Readers must apply these filters, and storage isn't reclaimed.

3. **Fragmentation**: Related data may be scattered across files, reducing locality for common queries.

4. **Schema drift**: Files written with different schemas may have suboptimal column layouts.

5. **Compression inefficiency**: Small files compress poorly; larger files achieve better ratios.

Compaction addresses these issues by periodically rewriting files into an optimized layout.

## Design Principles

1. **Non-blocking**: Compaction runs in the background without blocking reads or writes.

2. **Incremental**: Compact subsets of files rather than entire tables, limiting resource usage.

3. **Transactional**: Compaction is a normal transaction (add new files, remove old files), preserving consistency.

4. **Configurable**: Different tables have different needs; compaction policies should be tunable.

5. **Partition-aware**: Compaction respects partition boundaries, compacting within partitions.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Compaction Coordinator                             │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │ Trigger         │    │ Planner         │    │ Executor                │  │
│  │                 │    │                 │    │                         │  │
│  │ - Thresholds    │───>│ - File selection│───>│ - Read old files        │  │
│  │ - Schedule      │    │ - Bin packing   │    │ - Apply deletions       │  │
│  │ - Manual        │    │ - Z-ordering    │    │ - Write new files       │  │
│  └─────────────────┘    └─────────────────┘    │ - Commit transaction    │  │
│                                                └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Commit
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane (DB)                             │
│                                                                             │
│  Old files: removed_in_transaction_id = compaction_txn                      │
│  New files: added_in_transaction_id = compaction_txn                        │
│  Old DVs: removed_in_transaction_id = compaction_txn                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Compaction Types

### Bin-Packing Compaction

Merge small files into larger files without changing data order.

**Goal**: Reduce file count, improve read efficiency.

**Algorithm**:
1. Identify small files (below target size threshold)
2. Group files into bins that sum to approximately target size
3. Read files in each bin
4. Write combined data to new file(s)
5. Commit: remove old files, add new files

```rust
pub struct BinPackingConfig {
    /// Target file size in bytes (default: 256 MB)
    pub target_file_size: usize,
    /// Minimum file size to be considered "small" (default: 16 MB)
    pub small_file_threshold: usize,
    /// Maximum files to compact in one operation (default: 100)
    pub max_files_per_compaction: usize,
    /// Minimum file count to trigger compaction (default: 10)
    pub min_files_to_compact: usize,
}

impl Default for BinPackingConfig {
    fn default() -> Self {
        Self {
            target_file_size: 256 * 1024 * 1024,      // 256 MB
            small_file_threshold: 16 * 1024 * 1024,   // 16 MB
            max_files_per_compaction: 100,
            min_files_to_compact: 10,
        }
    }
}
```

### Z-Order Compaction

Rewrite files with data sorted by Z-order curve for multi-dimensional locality.

**Goal**: Improve query performance for filters on multiple columns.

**Algorithm**:
1. Select files for compaction
2. Read all data
3. Compute Z-order key for each row based on clustering columns
4. Sort data by Z-order key
5. Write new files with sorted data
6. Commit: remove old files, add new files

```rust
pub struct ZOrderConfig {
    /// Columns to include in Z-order curve
    pub clustering_columns: Vec<String>,
    /// Target file size after Z-ordering
    pub target_file_size: usize,
    /// Number of interleave bits per column (default: 8)
    pub interleave_bits: u8,
}

/// Compute Z-order key for a row
fn compute_z_order_key(row: &Row, columns: &[String], bits_per_column: u8) -> u64 {
    let mut z_value = 0u64;
    let total_bits = columns.len() as u8 * bits_per_column;
    
    for bit_position in 0..total_bits {
        let column_idx = (bit_position % columns.len() as u8) as usize;
        let bit_in_column = bit_position / columns.len() as u8;
        
        let column_value = row.get(&columns[column_idx])
            .map(|v| normalize_to_u64(v))
            .unwrap_or(0);
        
        let bit = (column_value >> (bits_per_column - 1 - bit_in_column)) & 1;
        z_value |= bit << (total_bits - 1 - bit_position);
    }
    
    z_value
}
```

### Sorting Compaction

Sort files by specific columns for range query optimization.

**Goal**: Maximize min/max pruning effectiveness for sorted columns.

```rust
pub struct SortingConfig {
    /// Columns to sort by, in order
    pub sort_columns: Vec<SortColumn>,
    /// Target file size
    pub target_file_size: usize,
}

pub struct SortColumn {
    pub name: String,
    pub direction: SortDirection,
    pub nulls: NullsOrder,
}

pub enum SortDirection {
    Ascending,
    Descending,
}

pub enum NullsOrder {
    First,
    Last,
}
```

### Deletion Materialization

Rewrite files to physically remove deleted rows.

**Goal**: Reclaim storage, eliminate DV read overhead.

```rust
pub struct DeletionMaterializationConfig {
    /// Materialize when deletion ratio exceeds this (default: 0.1 = 10%)
    pub deletion_ratio_threshold: f64,
    /// Materialize when DV file exceeds this size (default: 1 MB)
    pub max_deletion_vector_size: usize,
    /// Always materialize during bin-packing (default: true)
    pub materialize_on_compaction: bool,
}
```

## Trigger Strategies

### Threshold-Based Triggers

```rust
pub struct ThresholdTrigger {
    /// Trigger when small file count exceeds this
    pub small_file_count_threshold: usize,
    /// Trigger when small file ratio exceeds this
    pub small_file_ratio_threshold: f64,
    /// Trigger when total file count exceeds this
    pub total_file_count_threshold: usize,
    /// Trigger when deletion ratio exceeds this
    pub deletion_ratio_threshold: f64,
}

impl ThresholdTrigger {
    pub fn should_compact(&self, stats: &TableStats, files: &[FileWithDeletions]) -> bool {
        // Count small files
        let small_count = files.iter()
            .filter(|f| f.file.file_size_bytes < self.small_file_size_threshold as i64)
            .count();
        
        if small_count >= self.small_file_count_threshold {
            return true;
        }
        
        // Check small file ratio
        let small_ratio = small_count as f64 / files.len() as f64;
        if small_ratio >= self.small_file_ratio_threshold {
            return true;
        }
        
        // Check total file count
        if files.len() >= self.total_file_count_threshold {
            return true;
        }
        
        // Check deletion ratio
        let total_rows: i64 = files.iter().map(|f| f.file.record_count).sum();
        let deleted_rows: u64 = files.iter()
            .filter_map(|f| f.deletion_vector.as_ref())
            .map(|dv| dv.cardinality())
            .sum();
        
        if total_rows > 0 {
            let deletion_ratio = deleted_rows as f64 / total_rows as f64;
            if deletion_ratio >= self.deletion_ratio_threshold {
                return true;
            }
        }
        
        false
    }
}
```

### Scheduled Triggers

```rust
pub struct ScheduledTrigger {
    /// Cron expression for scheduled compaction
    pub schedule: String,  // e.g., "0 2 * * *" for 2 AM daily
    /// Only compact if minimum changes since last compaction
    pub min_changes_since_last: usize,
}
```

### Manual Triggers

```rust
impl SqlCatalog {
    /// Manually trigger compaction
    pub async fn compact(
        &self,
        table_uuid: Uuid,
        options: CompactionOptions,
    ) -> Result<CompactionResult, CatalogError> {
        let planner = CompactionPlanner::new(self.clone(), options.config);
        let plan = planner.plan(table_uuid, options.partition_filter).await?;
        
        if plan.is_empty() {
            return Ok(CompactionResult::NothingToCompact);
        }
        
        let executor = CompactionExecutor::new(self.clone());
        executor.execute(table_uuid, plan).await
    }
}

pub struct CompactionOptions {
    /// Compaction configuration
    pub config: CompactionConfig,
    /// Optional partition filter (compact only matching partitions)
    pub partition_filter: Option<PartitionPredicate>,
    /// Dry run (plan but don't execute)
    pub dry_run: bool,
}
```

## Compaction Planner

### File Selection

```rust
pub struct CompactionPlanner {
    catalog: SqlCatalog,
    config: CompactionConfig,
}

impl CompactionPlanner {
    /// Plan compaction for a table
    pub async fn plan(
        &self,
        table_uuid: Uuid,
        partition_filter: Option<&PartitionPredicate>,
    ) -> Result<CompactionPlan, CompactionError> {
        let transaction_id = self.catalog.get_current_transaction(table_uuid).await?;
        let files = self.catalog.list_files_with_deletions(table_uuid, transaction_id).await?;
        
        // Apply partition filter if provided
        let files = if let Some(filter) = partition_filter {
            files.into_iter()
                .filter(|f| {
                    let pv: PartitionValues = serde_json::from_str(
                        f.file.partition_values.as_deref().unwrap_or("{}")
                    ).unwrap_or_default();
                    filter.matches(&pv)
                })
                .collect()
        } else {
            files
        };
        
        // Group files by partition
        let files_by_partition = self.group_by_partition(&files);
        
        let mut plan = CompactionPlan::new();
        
        for (partition_values, partition_files) in files_by_partition {
            let partition_plan = self.plan_partition_compaction(partition_files)?;
            if !partition_plan.is_empty() {
                plan.add_partition(partition_values, partition_plan);
            }
        }
        
        Ok(plan)
    }
    
    /// Plan compaction for files within a single partition
    fn plan_partition_compaction(
        &self,
        files: Vec<FileWithDeletions>,
    ) -> Result<Vec<CompactionGroup>, CompactionError> {
        match self.config.compaction_type {
            CompactionType::BinPacking => self.plan_bin_packing(files),
            CompactionType::ZOrder(ref z_config) => self.plan_z_order(files, z_config),
            CompactionType::Sorting(ref sort_config) => self.plan_sorting(files, sort_config),
        }
    }
    
    /// Bin-packing file selection using first-fit decreasing algorithm
    fn plan_bin_packing(
        &self,
        mut files: Vec<FileWithDeletions>,
    ) -> Result<Vec<CompactionGroup>, CompactionError> {
        let config = &self.config.bin_packing;
        
        // Filter to small files
        let small_files: Vec<_> = files.iter()
            .filter(|f| f.file.file_size_bytes < config.small_file_threshold as i64)
            .cloned()
            .collect();
        
        // Also include files with high deletion ratio
        let high_deletion_files: Vec<_> = files.iter()
            .filter(|f| {
                if let Some(dv) = &f.deletion_vector {
                    let ratio = dv.cardinality() as f64 / f.file.record_count as f64;
                    ratio >= self.config.deletion_materialization.deletion_ratio_threshold
                } else {
                    false
                }
            })
            .cloned()
            .collect();
        
        // Merge and deduplicate
        let mut candidates: Vec<_> = small_files.into_iter()
            .chain(high_deletion_files)
            .collect();
        candidates.sort_by_key(|f| f.file.file_uuid);
        candidates.dedup_by_key(|f| f.file.file_uuid);
        
        if candidates.len() < config.min_files_to_compact {
            return Ok(Vec::new()); // Not enough files to compact
        }
        
        // Sort by size descending (first-fit decreasing)
        candidates.sort_by(|a, b| b.effective_size_bytes().cmp(&a.effective_size_bytes()));
        
        // Bin packing
        let mut groups: Vec<CompactionGroup> = Vec::new();
        
        for file in candidates.into_iter().take(config.max_files_per_compaction) {
            let file_size = file.effective_size_bytes();
            
            // Find a bin with enough room
            let target_bin = groups.iter_mut()
                .find(|g| g.total_size + file_size <= config.target_file_size as i64);
            
            if let Some(bin) = target_bin {
                bin.add_file(file);
            } else {
                // Start new bin
                let mut new_group = CompactionGroup::new();
                new_group.add_file(file);
                groups.push(new_group);
            }
        }
        
        // Filter out groups with single file (unless it has deletions to materialize)
        groups.retain(|g| {
            g.files.len() > 1 || 
            g.files.iter().any(|f| f.deletion_vector.is_some())
        });
        
        Ok(groups)
    }
    
    /// Z-order compaction planning
    fn plan_z_order(
        &self,
        files: Vec<FileWithDeletions>,
        z_config: &ZOrderConfig,
    ) -> Result<Vec<CompactionGroup>, CompactionError> {
        // For Z-ordering, we typically compact all files in the partition
        // into optimally-sized output files
        
        if files.is_empty() {
            return Ok(Vec::new());
        }
        
        let total_size: i64 = files.iter().map(|f| f.effective_size_bytes()).sum();
        let target = z_config.target_file_size as i64;
        let num_output_files = ((total_size as f64 / target as f64).ceil() as usize).max(1);
        
        // Create a single group containing all files
        let mut group = CompactionGroup::new();
        group.compaction_type = CompactionGroupType::ZOrder {
            columns: z_config.clustering_columns.clone(),
            num_output_files,
        };
        
        for file in files {
            group.add_file(file);
        }
        
        Ok(vec![group])
    }
}

/// A group of files to compact together
pub struct CompactionGroup {
    pub files: Vec<FileWithDeletions>,
    pub total_size: i64,
    pub compaction_type: CompactionGroupType,
}

pub enum CompactionGroupType {
    BinPack,
    ZOrder { columns: Vec<String>, num_output_files: usize },
    Sort { columns: Vec<SortColumn> },
}

impl CompactionGroup {
    pub fn new() -> Self {
        Self {
            files: Vec::new(),
            total_size: 0,
            compaction_type: CompactionGroupType::BinPack,
        }
    }
    
    pub fn add_file(&mut self, file: FileWithDeletions) {
        self.total_size += file.effective_size_bytes();
        self.files.push(file);
    }
}

/// Full compaction plan
pub struct CompactionPlan {
    pub partitions: Vec<(PartitionValues, Vec<CompactionGroup>)>,
}

impl CompactionPlan {
    pub fn is_empty(&self) -> bool {
        self.partitions.iter().all(|(_, groups)| groups.is_empty())
    }
    
    pub fn total_input_files(&self) -> usize {
        self.partitions.iter()
            .flat_map(|(_, groups)| groups.iter())
            .map(|g| g.files.len())
            .sum()
    }
    
    pub fn total_input_bytes(&self) -> i64 {
        self.partitions.iter()
            .flat_map(|(_, groups)| groups.iter())
            .map(|g| g.total_size)
            .sum()
    }
}
```

## Compaction Executor

```rust
pub struct CompactionExecutor {
    catalog: SqlCatalog,
    storage: Arc<dyn ObjectStore>,
}

impl CompactionExecutor {
    /// Execute a compaction plan
    pub async fn execute(
        &self,
        table_uuid: Uuid,
        plan: CompactionPlan,
    ) -> Result<CompactionResult, CompactionError> {
        let base_transaction_id = self.catalog.get_current_transaction(table_uuid).await?;
        let table = self.catalog.get_table(table_uuid).await?;
        let format = table.default_file_format();
        
        let mut mutations = Vec::new();
        let mut output_files = Vec::new();
        let mut total_input_files = 0;
        let mut total_input_bytes = 0i64;
        let mut total_output_files = 0;
        let mut total_output_bytes = 0i64;
        
        for (partition_values, groups) in plan.partitions {
            for group in groups {
                let result = self.compact_group(
                    &table,
                    &partition_values,
                    &group,
                    format,
                ).await?;
                
                total_input_files += group.files.len();
                total_input_bytes += group.total_size;
                total_output_files += result.new_files.len();
                total_output_bytes += result.new_files.iter().map(|f| f.file_size_bytes).sum::<i64>();
                
                // Mark old files as removed
                for file in &group.files {
                    mutations.push(MutationOp::RemoveFile { file_uuid: file.file.file_uuid });
                    
                    // Also remove deletion vector if present
                    if file.deletion_vector.is_some() {
                        if let Some(dv_uuid) = self.catalog.get_dv_uuid(file.file.file_uuid).await? {
                            mutations.push(MutationOp::RemoveDeletionVector { deletion_vector_uuid: dv_uuid });
                        }
                    }
                }
                
                // Add new files
                for new_file in result.new_files {
                    mutations.push(MutationOp::AddFile(new_file.clone()));
                    output_files.push(new_file);
                }
            }
        }
        
        if mutations.is_empty() {
            return Ok(CompactionResult::NothingToCompact);
        }
        
        // Mark transaction as compaction (for CDC filtering)
        let transaction_id = self.catalog.commit_with_metadata(
            table_uuid,
            base_transaction_id,
            mutations,
            TransactionMetadata { is_compaction: true },
        ).await?;
        
        Ok(CompactionResult::Success {
            transaction_id,
            input_files: total_input_files,
            input_bytes: total_input_bytes,
            output_files: total_output_files,
            output_bytes: total_output_bytes,
            files: output_files,
        })
    }
    
    /// Compact a single group of files
    async fn compact_group(
        &self,
        table: &Table,
        partition_values: &PartitionValues,
        group: &CompactionGroup,
        format: Format,
    ) -> Result<GroupCompactionResult, CompactionError> {
        // Read all data from input files, applying deletion vectors
        let mut batches = Vec::new();
        
        for file in &group.files {
            let reader = ReaderEnum::new(format);
            let batch = if let Some(dv) = &file.deletion_vector {
                // Read with deletion filtering
                let raw_batch = reader.read(Path::new(&file.file.file_path))?;
                apply_deletion_vector(&raw_batch, dv)?
            } else {
                reader.read(Path::new(&file.file.file_path))?
            };
            batches.push(batch);
        }
        
        // Concatenate all batches
        let schema = batches[0].schema();
        let combined = arrow::compute::concat_batches(&schema, &batches)?;
        
        // Apply compaction-specific transformations
        let processed = match &group.compaction_type {
            CompactionGroupType::BinPack => combined,
            CompactionGroupType::ZOrder { columns, num_output_files } => {
                self.z_order_sort(&combined, columns)?
            }
            CompactionGroupType::Sort { columns } => {
                self.sort_batch(&combined, columns)?
            }
        };
        
        // Split into target-sized files
        let target_size = match &group.compaction_type {
            CompactionGroupType::ZOrder { num_output_files, .. } => {
                (processed.num_rows() / num_output_files).max(1)
            }
            _ => self.estimate_rows_for_size(&processed, 256 * 1024 * 1024),
        };
        
        let split_batches = self.split_batch(&processed, target_size)?;
        
        // Write output files
        let mut new_files = Vec::new();
        let writer = WriterEnum::new(format);
        
        for batch in split_batches {
            let file_uuid = Uuid::new_v4();
            let file_path = format!(
                "{}/data/{}.{}",
                table.location,
                file_uuid,
                format.as_str()
            );
            
            writer.write(&batch, Path::new(&file_path))?;
            
            let file_size = self.storage.head(&file_path).await?.size as i64;
            
            new_files.push(FileSpec {
                file_uuid,
                table_uuid: table.table_uuid,
                file_format: format.as_str().to_string(),
                file_path,
                record_count: batch.num_rows() as i64,
                file_size_bytes: file_size,
                partition_values: Some(serde_json::to_string(partition_values)?),
            });
        }
        
        Ok(GroupCompactionResult { new_files })
    }
    
    /// Sort batch by Z-order key
    fn z_order_sort(
        &self,
        batch: &RecordBatch,
        columns: &[String],
    ) -> Result<RecordBatch, CompactionError> {
        // Compute Z-order keys for all rows
        let mut z_keys = Vec::with_capacity(batch.num_rows());
        
        for row_idx in 0..batch.num_rows() {
            let row = extract_row(batch, row_idx)?;
            let z_key = compute_z_order_key(&row, columns, 8);
            z_keys.push(z_key);
        }
        
        // Create sort indices
        let mut indices: Vec<usize> = (0..batch.num_rows()).collect();
        indices.sort_by_key(|&i| z_keys[i]);
        
        // Reorder batch
        let indices_array = UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
        take_record_batch(batch, &indices_array)
    }
    
    /// Sort batch by specified columns
    fn sort_batch(
        &self,
        batch: &RecordBatch,
        columns: &[SortColumn],
    ) -> Result<RecordBatch, CompactionError> {
        let sort_columns: Vec<_> = columns.iter()
            .map(|c| {
                let array = batch.column_by_name(&c.name)
                    .ok_or_else(|| CompactionError::MissingColumn(c.name.clone()))?;
                Ok(SortColumn {
                    values: array.clone(),
                    options: Some(SortOptions {
                        descending: matches!(c.direction, SortDirection::Descending),
                        nulls_first: matches!(c.nulls, NullsOrder::First),
                    }),
                })
            })
            .collect::<Result<_, CompactionError>>()?;
        
        let indices = lexsort_to_indices(&sort_columns, None)?;
        take_record_batch(batch, &indices)
    }
}

pub enum CompactionResult {
    NothingToCompact,
    Success {
        transaction_id: Uuid,
        input_files: usize,
        input_bytes: i64,
        output_files: usize,
        output_bytes: i64,
        files: Vec<FileSpec>,
    },
}
```

## Conflict Handling

Compaction can conflict with concurrent writes:

```rust
impl CompactionExecutor {
    /// Handle compaction conflicts with retry
    pub async fn execute_with_retry(
        &self,
        table_uuid: Uuid,
        plan: CompactionPlan,
        max_retries: usize,
    ) -> Result<CompactionResult, CompactionError> {
        let mut retries = 0;
        
        loop {
            match self.execute(table_uuid, plan.clone()).await {
                Ok(result) => return Ok(result),
                Err(CompactionError::Conflict(msg)) => {
                    retries += 1;
                    if retries >= max_retries {
                        return Err(CompactionError::MaxRetriesExceeded { retries, last_error: msg });
                    }
                    
                    // Re-plan with updated state
                    let planner = CompactionPlanner::new(self.catalog.clone(), self.config.clone());
                    let new_plan = planner.plan(table_uuid, None).await?;
                    
                    if new_plan.is_empty() {
                        return Ok(CompactionResult::NothingToCompact);
                    }
                    
                    // Exponential backoff
                    let delay = Duration::from_millis(100 * 2u64.pow(retries as u32));
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
```

### Concurrent Write Safety

When compaction commits, it validates that compacted files haven't changed:

```rust
impl SqlCatalog {
    async fn validate_compaction_commit(
        &self,
        table_uuid: Uuid,
        base_txn: Uuid,
        current_txn: Uuid,
        files_to_remove: &[Uuid],
    ) -> Result<(), CatalogError> {
        for file_uuid in files_to_remove {
            // Check file still exists at current transaction
            let file_exists = self.file_exists_at(*file_uuid, current_txn).await?;
            if !file_exists {
                return Err(CatalogError::Conflict(format!(
                    "File {} was removed by concurrent transaction",
                    file_uuid
                )));
            }
            
            // Check file hasn't been modified (new DV added)
            let dv_changed = self.deletion_vector_changed(*file_uuid, base_txn, current_txn).await?;
            if dv_changed {
                return Err(CatalogError::Conflict(format!(
                    "File {} was modified by concurrent transaction (deletion vector changed)",
                    file_uuid
                )));
            }
        }
        
        Ok(())
    }
}
```

## Maintenance Daemon Integration

Compaction integrates with the unified maintenance daemon (see [db_control_plane.md](db_control_plane.md)):

```rust
pub struct CompactionWorker {
    catalog: SqlCatalog,
    config: CompactionWorkerConfig,
}

pub struct CompactionWorkerConfig {
    /// How often to check for compaction needs
    pub check_interval: Duration,
    /// Compaction configuration per table (or default)
    pub table_configs: HashMap<Uuid, CompactionConfig>,
    /// Default compaction configuration
    pub default_config: CompactionConfig,
    /// Maximum concurrent compaction operations
    pub max_concurrent_compactions: usize,
}

impl CompactionWorker {
    /// Run the compaction worker loop
    pub async fn run(&self) -> Result<(), CompactionError> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_compactions));
        
        loop {
            // Get all tables
            let tables = self.catalog.list_tables().await?;
            
            for table in tables {
                let config = self.config.table_configs
                    .get(&table.table_uuid)
                    .cloned()
                    .unwrap_or_else(|| self.config.default_config.clone());
                
                // Check if compaction is needed
                let stats = self.catalog.get_table_stats(table.table_uuid).await?;
                let files = self.catalog.list_files_with_deletions(
                    table.table_uuid,
                    table.current_transaction_id.unwrap(),
                ).await?;
                
                let trigger = ThresholdTrigger::from(&config);
                if trigger.should_compact(&stats, &files) {
                    // Acquire semaphore permit
                    let permit = semaphore.clone().acquire_owned().await?;
                    
                    // Spawn compaction task
                    let catalog = self.catalog.clone();
                    tokio::spawn(async move {
                        let planner = CompactionPlanner::new(catalog.clone(), config);
                        let plan = planner.plan(table.table_uuid, None).await;
                        
                        if let Ok(plan) = plan {
                            if !plan.is_empty() {
                                let executor = CompactionExecutor::new(catalog);
                                let _ = executor.execute_with_retry(table.table_uuid, plan, 3).await;
                            }
                        }
                        
                        drop(permit); // Release semaphore
                    });
                }
            }
            
            tokio::time::sleep(self.config.check_interval).await;
        }
    }
}
```

## Configuration

### Table-Level Configuration

```sql
-- Set compaction configuration in table properties
UPDATE tables 
SET properties = json_set(properties, '$.compaction', json('{
    "enabled": true,
    "type": "bin_packing",
    "target_file_size_mb": 256,
    "small_file_threshold_mb": 16,
    "max_files_per_compaction": 100,
    "deletion_ratio_threshold": 0.1,
    "z_order_columns": ["timestamp", "user_id"]
}'))
WHERE table_uuid = ?;
```

### Configuration Schema

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Whether auto-compaction is enabled
    pub enabled: bool,
    /// Type of compaction
    pub compaction_type: CompactionType,
    /// Bin-packing configuration
    pub bin_packing: BinPackingConfig,
    /// Deletion materialization configuration
    pub deletion_materialization: DeletionMaterializationConfig,
    /// Z-order configuration (if type is ZOrder)
    pub z_order: Option<ZOrderConfig>,
    /// Sorting configuration (if type is Sorting)
    pub sorting: Option<SortingConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompactionType {
    /// Simple bin-packing of small files
    BinPacking,
    /// Z-order clustering
    ZOrder,
    /// Sort by specific columns
    Sorting,
}
```

## Metrics and Monitoring

```rust
pub struct CompactionMetrics {
    /// Number of compaction operations completed
    pub compactions_completed: Counter,
    /// Number of compaction operations failed
    pub compactions_failed: Counter,
    /// Total bytes read for compaction
    pub bytes_read: Counter,
    /// Total bytes written by compaction
    pub bytes_written: Counter,
    /// Compaction duration histogram
    pub compaction_duration: Histogram,
    /// Current number of pending compaction tasks
    pub pending_compactions: Gauge,
}

impl CompactionExecutor {
    async fn execute_with_metrics(
        &self,
        table_uuid: Uuid,
        plan: CompactionPlan,
        metrics: &CompactionMetrics,
    ) -> Result<CompactionResult, CompactionError> {
        let start = Instant::now();
        metrics.pending_compactions.inc();
        
        let result = self.execute(table_uuid, plan).await;
        
        metrics.pending_compactions.dec();
        metrics.compaction_duration.observe(start.elapsed().as_secs_f64());
        
        match &result {
            Ok(CompactionResult::Success { input_bytes, output_bytes, .. }) => {
                metrics.compactions_completed.inc();
                metrics.bytes_read.add(*input_bytes as u64);
                metrics.bytes_written.add(*output_bytes as u64);
            }
            Ok(CompactionResult::NothingToCompact) => {
                // Not counted as completion
            }
            Err(_) => {
                metrics.compactions_failed.inc();
            }
        }
        
        result
    }
}
```

## Implementation Phases

### Phase 1: Basic Bin-Packing

1. Implement file selection based on size threshold
2. Implement first-fit decreasing bin packing
3. Implement compaction executor
4. Add manual compaction API
5. Integration tests

### Phase 2: Deletion Materialization

1. Integrate with deletion vectors
2. Add deletion ratio threshold trigger
3. Apply deletion vectors during compaction read
4. Verify deletion vector cleanup

### Phase 3: Automated Compaction

1. Implement threshold-based triggers
2. Implement scheduled triggers
3. Build compaction worker
4. Integrate with maintenance daemon

### Phase 4: Advanced Compaction

1. Implement Z-order compaction
2. Implement sorting compaction
3. Add partition-aware compaction
4. Add conflict retry logic

### Phase 5: Optimization

1. Add compaction metrics
2. Implement concurrent compaction limits
3. Add compaction cost estimation
4. Optimize large table handling

## Testing Strategy

### Unit Tests

- Bin-packing algorithm correctness
- Z-order key computation
- Trigger condition evaluation
- Conflict detection logic

### Integration Tests

- End-to-end compaction execution
- Concurrent write safety
- Deletion vector materialization
- Partition-aware compaction

### Performance Tests

- Compaction throughput (GB/hour)
- Memory usage during compaction
- Impact on concurrent read/write performance
- Large file handling

## Open Questions

1. **Compaction priorities**: How do we prioritize compaction when multiple tables need it? By file count? By deletion ratio? By age?

2. **Resource limits**: How do we limit compaction resource usage (CPU, memory, I/O)? Should there be per-table limits?

3. **Partial compaction**: Should we support partial compaction (compact some files now, others later)? How do we track progress?

4. **Compaction history**: Should we retain compaction history for debugging? How long?

5. **External compaction**: Should external systems be able to trigger compaction via API? What authentication/authorization is needed?

## References

- [Delta Lake Optimize](https://docs.delta.io/latest/optimizations-oss.html)
- [Apache Iceberg Compaction](https://iceberg.apache.org/docs/latest/maintenance/#compact-data-files)
- [Z-Order Curve](https://en.wikipedia.org/wiki/Z-order_curve)
- [db_control_plane.md](db_control_plane.md) - Maintenance daemon design
- [deletion_vectors.md](deletion_vectors.md) - Deletion vector integration
- [partitioning.md](partitioning.md) - Partition-aware compaction
