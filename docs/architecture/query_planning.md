# Query Planning and Optimization

## Purpose

This document specifies Planar's query planning and optimization system. Query planning transforms logical queries into efficient physical execution plans, leveraging table statistics, file metadata, and format capabilities to minimize data scanned.

## Motivation

Efficient query execution requires intelligent planning:

1. **File pruning**: Skip files that cannot contain matching rows based on partition values and column statistics.

2. **Column projection**: Read only the columns needed for the query, reducing I/O.

3. **Predicate pushdown**: Push filters into file readers to skip data early.

4. **Statistics usage**: Use min/max, null counts, and distinct counts to estimate selectivity and plan joins.

5. **Format optimization**: Leverage format-specific features (Parquet row groups, Lance indexes).

Without query planning, queries would scan entire tables, wasting I/O and compute resources.

## Design Principles

1. **Statistics-driven**: Planning decisions are based on accurate, up-to-date statistics.

2. **Format-aware**: Each file format has different capabilities; plans adapt accordingly.

3. **Layered optimization**: Multiple optimization passes (logical, physical, runtime).

4. **Integration-ready**: Designed to integrate with query engines like DataFusion.

5. **Incremental improvement**: Start simple, add optimizations progressively.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Query Planning Pipeline                           │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   Query     │    │  Logical    │    │  Physical   │    │  Execution  │  │
│  │   Input     │───>│  Planning   │───>│  Planning   │───>│             │  │
│  │             │    │             │    │             │    │             │  │
│  │ - Predicate │    │ - Normalize │    │ - File      │    │ - Scan      │  │
│  │ - Columns   │    │ - Simplify  │    │   pruning   │    │   files     │  │
│  │ - Options   │    │ - Reorder   │    │ - Pushdown  │    │ - Apply     │  │
│  │             │    │             │    │ - Cost est  │    │   filters   │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                          Statistics  │  File Metadata
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane (DB)                             │
│                                                                             │
│  table_stats | file_column_stats | files | partition_specs                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Statistics Model

### Table-Level Statistics

```rust
/// Table-level statistics
#[derive(Clone, Debug)]
pub struct TableStatistics {
    /// Total row count
    pub row_count: u64,
    /// Total file size in bytes
    pub total_size_bytes: u64,
    /// Number of files
    pub file_count: usize,
    /// Statistics timestamp
    pub computed_at: DateTime<Utc>,
    /// Transaction ID these stats are valid for
    pub transaction_id: Uuid,
}
```

### Column-Level Statistics

```rust
/// Column-level statistics aggregated across files
#[derive(Clone, Debug)]
pub struct ColumnStatistics {
    /// Column name
    pub column_name: String,
    /// Data type
    pub data_type: DataType,
    /// Number of null values
    pub null_count: Option<u64>,
    /// Number of distinct values (approximate via HyperLogLog)
    pub distinct_count: Option<u64>,
    /// Minimum value
    pub min_value: Option<ScalarValue>,
    /// Maximum value
    pub max_value: Option<ScalarValue>,
    /// Average value (for numeric types)
    pub avg_value: Option<f64>,
    /// Histogram (for range queries)
    pub histogram: Option<Histogram>,
}

/// Histogram for value distribution
#[derive(Clone, Debug)]
pub struct Histogram {
    /// Histogram type
    pub histogram_type: HistogramType,
    /// Bucket boundaries
    pub buckets: Vec<ScalarValue>,
    /// Count per bucket
    pub counts: Vec<u64>,
}

#[derive(Clone, Debug)]
pub enum HistogramType {
    /// Equal-width buckets
    Equiwidth,
    /// Equal-height (equi-depth) buckets
    Equiheight,
    /// Singleton values (for low-cardinality columns)
    Singleton,
}
```

### File-Level Statistics

```rust
/// Statistics for a single file
#[derive(Clone, Debug)]
pub struct FileStatistics {
    /// File UUID
    pub file_uuid: Uuid,
    /// Row count in this file
    pub row_count: u64,
    /// File size in bytes
    pub file_size_bytes: u64,
    /// Per-column statistics
    pub column_stats: HashMap<String, FileColumnStatistics>,
    /// Partition values (for partition pruning)
    pub partition_values: Option<PartitionValues>,
}

/// Column statistics for a file
#[derive(Clone, Debug)]
pub struct FileColumnStatistics {
    /// Null count
    pub null_count: Option<u64>,
    /// NaN count (for floats)
    pub nan_count: Option<u64>,
    /// Min value
    pub min_value: Option<ScalarValue>,
    /// Max value
    pub max_value: Option<ScalarValue>,
    /// Distinct count (from Parquet/file stats)
    pub distinct_count: Option<u64>,
}
```

## Query Representation

### Scan Request

```rust
/// A request to scan a table
#[derive(Clone, Debug)]
pub struct ScanRequest {
    /// Table to scan
    pub table_uuid: Uuid,
    /// Transaction to read at
    pub transaction_id: Uuid,
    /// Columns to read (None = all)
    pub projection: Option<Vec<String>>,
    /// Filter predicate
    pub predicate: Option<Predicate>,
    /// Limit on rows returned
    pub limit: Option<usize>,
    /// Sort order (for sorted scans)
    pub sort: Option<Vec<SortExpr>>,
}

/// Filter predicate
#[derive(Clone, Debug)]
pub enum Predicate {
    // Comparison predicates
    Eq(String, ScalarValue),
    Ne(String, ScalarValue),
    Lt(String, ScalarValue),
    Le(String, ScalarValue),
    Gt(String, ScalarValue),
    Ge(String, ScalarValue),
    
    // Range predicate
    Between(String, ScalarValue, ScalarValue),
    
    // List membership
    In(String, Vec<ScalarValue>),
    NotIn(String, Vec<ScalarValue>),
    
    // Null checks
    IsNull(String),
    IsNotNull(String),
    
    // String predicates
    Like(String, String),
    StartsWith(String, String),
    
    // Logical operators
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    /// Get all columns referenced by this predicate
    pub fn columns(&self) -> Vec<&str> {
        match self {
            Predicate::Eq(col, _) |
            Predicate::Ne(col, _) |
            Predicate::Lt(col, _) |
            Predicate::Le(col, _) |
            Predicate::Gt(col, _) |
            Predicate::Ge(col, _) |
            Predicate::Between(col, _, _) |
            Predicate::In(col, _) |
            Predicate::NotIn(col, _) |
            Predicate::IsNull(col) |
            Predicate::IsNotNull(col) |
            Predicate::Like(col, _) |
            Predicate::StartsWith(col, _) => vec![col.as_str()],
            
            Predicate::And(left, right) |
            Predicate::Or(left, right) => {
                let mut cols = left.columns();
                cols.extend(right.columns());
                cols
            }
            
            Predicate::Not(inner) => inner.columns(),
        }
    }
    
    /// Convert to conjunctive normal form (AND of ORs)
    pub fn to_cnf(&self) -> Vec<Predicate> {
        match self {
            Predicate::And(left, right) => {
                let mut cnf = left.to_cnf();
                cnf.extend(right.to_cnf());
                cnf
            }
            _ => vec![self.clone()],
        }
    }
}
```

## Logical Planning

### Logical Plan

```rust
/// Logical query plan
#[derive(Clone, Debug)]
pub enum LogicalPlan {
    /// Table scan
    Scan {
        table_uuid: Uuid,
        transaction_id: Uuid,
        projection: Option<Vec<String>>,
    },
    
    /// Filter rows
    Filter {
        input: Box<LogicalPlan>,
        predicate: Predicate,
    },
    
    /// Project columns
    Projection {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    
    /// Limit rows
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
    },
    
    /// Sort rows
    Sort {
        input: Box<LogicalPlan>,
        sort_exprs: Vec<SortExpr>,
    },
}

/// Logical plan builder
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Start with a table scan
    pub fn scan(table_uuid: Uuid, transaction_id: Uuid) -> Self {
        Self {
            plan: LogicalPlan::Scan {
                table_uuid,
                transaction_id,
                projection: None,
            },
        }
    }
    
    /// Add a filter
    pub fn filter(mut self, predicate: Predicate) -> Self {
        self.plan = LogicalPlan::Filter {
            input: Box::new(self.plan),
            predicate,
        };
        self
    }
    
    /// Add a projection
    pub fn project(mut self, columns: Vec<String>) -> Self {
        self.plan = LogicalPlan::Projection {
            input: Box::new(self.plan),
            columns,
        };
        self
    }
    
    /// Add a limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.plan = LogicalPlan::Limit {
            input: Box::new(self.plan),
            limit,
        };
        self
    }
    
    /// Build the final plan
    pub fn build(self) -> LogicalPlan {
        self.plan
    }
}
```

### Logical Optimization

```rust
/// Logical plan optimizer
pub struct LogicalOptimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl LogicalOptimizer {
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(PredicatePushdown),
                Box::new(ProjectionPushdown),
                Box::new(PredicateSimplification),
                Box::new(ConstantFolding),
            ],
        }
    }
    
    /// Optimize a logical plan
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut optimized = plan;
        
        for rule in &self.rules {
            optimized = rule.apply(optimized);
        }
        
        optimized
    }
}

/// Optimization rule trait
trait OptimizationRule: Send + Sync {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}

/// Push predicates down to scan
struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                match *input {
                    LogicalPlan::Scan { table_uuid, transaction_id, projection } => {
                        // Predicate can be pushed to scan level
                        LogicalPlan::Filter {
                            input: Box::new(LogicalPlan::Scan {
                                table_uuid,
                                transaction_id,
                                projection,
                            }),
                            predicate,
                        }
                    }
                    other => LogicalPlan::Filter {
                        input: Box::new(self.apply(other)),
                        predicate,
                    },
                }
            }
            LogicalPlan::Projection { input, columns } => {
                LogicalPlan::Projection {
                    input: Box::new(self.apply(*input)),
                    columns,
                }
            }
            other => other,
        }
    }
}

/// Simplify predicates (constant folding, etc.)
struct PredicateSimplification;

impl OptimizationRule for PredicateSimplification {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let simplified = simplify_predicate(predicate);
                match simplified {
                    // TRUE filter is eliminated
                    Predicate::Eq(_, ScalarValue::Boolean(true)) => self.apply(*input),
                    // FALSE filter produces empty result
                    Predicate::Eq(_, ScalarValue::Boolean(false)) => {
                        LogicalPlan::Limit {
                            input,
                            limit: 0,
                        }
                    }
                    _ => LogicalPlan::Filter {
                        input: Box::new(self.apply(*input)),
                        predicate: simplified,
                    },
                }
            }
            other => other,
        }
    }
}

fn simplify_predicate(pred: Predicate) -> Predicate {
    match pred {
        // x = x -> true
        Predicate::Eq(ref col, ref val) if is_column_ref_to_self(col, val) => {
            Predicate::Eq("_const".to_string(), ScalarValue::Boolean(true))
        }
        // x AND true -> x
        Predicate::And(left, right) => {
            let left = simplify_predicate(*left);
            let right = simplify_predicate(*right);
            
            match (&left, &right) {
                (Predicate::Eq(_, ScalarValue::Boolean(true)), _) => right,
                (_, Predicate::Eq(_, ScalarValue::Boolean(true))) => left,
                (Predicate::Eq(_, ScalarValue::Boolean(false)), _) |
                (_, Predicate::Eq(_, ScalarValue::Boolean(false))) => {
                    Predicate::Eq("_const".to_string(), ScalarValue::Boolean(false))
                }
                _ => Predicate::And(Box::new(left), Box::new(right)),
            }
        }
        other => other,
    }
}
```

## Physical Planning

### Physical Plan

```rust
/// Physical execution plan
#[derive(Clone, Debug)]
pub struct PhysicalPlan {
    /// Files to scan
    pub file_scans: Vec<FileScan>,
    /// Columns to read
    pub projection: Vec<String>,
    /// Filter to apply after reading
    pub residual_filter: Option<Predicate>,
    /// Limit
    pub limit: Option<usize>,
    /// Estimated cost
    pub cost: PlanCost,
}

/// A scan of a single file
#[derive(Clone, Debug)]
pub struct FileScan {
    /// File to scan
    pub file: FileMetadata,
    /// Columns to read from this file
    pub projection: Vec<String>,
    /// Filter that can be pushed to the reader
    pub pushdown_filter: Option<Predicate>,
    /// Format-specific hints
    pub hints: ScanHints,
}

/// Format-specific scan hints
#[derive(Clone, Debug, Default)]
pub struct ScanHints {
    /// Parquet: row groups to scan (None = all)
    pub row_groups: Option<Vec<usize>>,
    /// Lance: fragments to scan
    pub fragments: Option<Vec<u64>>,
    /// Batch size hint
    pub batch_size: Option<usize>,
}

/// Estimated cost of a plan
#[derive(Clone, Debug, Default)]
pub struct PlanCost {
    /// Estimated rows to scan
    pub rows_scanned: u64,
    /// Estimated bytes to read
    pub bytes_read: u64,
    /// Estimated rows returned
    pub rows_returned: u64,
    /// Number of files to scan
    pub files_scanned: usize,
}
```

### Physical Planner

```rust
/// Physical plan generator
pub struct PhysicalPlanner {
    catalog: Arc<SqlCatalog>,
}

impl PhysicalPlanner {
    /// Generate physical plan from logical plan
    pub async fn plan(
        &self,
        logical: &LogicalPlan,
    ) -> Result<PhysicalPlan, PlanError> {
        match logical {
            LogicalPlan::Filter { input, predicate } => {
                let mut physical = self.plan(input).await?;
                physical = self.apply_filter(physical, predicate).await?;
                Ok(physical)
            }
            
            LogicalPlan::Scan { table_uuid, transaction_id, projection } => {
                self.plan_scan(*table_uuid, *transaction_id, projection.clone(), None).await
            }
            
            LogicalPlan::Projection { input, columns } => {
                let mut physical = self.plan(input).await?;
                physical.projection = columns.clone();
                // Update file scans to only read needed columns
                for scan in &mut physical.file_scans {
                    scan.projection = columns.clone();
                }
                Ok(physical)
            }
            
            LogicalPlan::Limit { input, limit } => {
                let mut physical = self.plan(input).await?;
                physical.limit = Some(*limit);
                physical.cost.rows_returned = physical.cost.rows_returned.min(*limit as u64);
                Ok(physical)
            }
            
            LogicalPlan::Sort { input, sort_exprs } => {
                // Sort requires full scan, then sort
                let physical = self.plan(input).await?;
                // Sorting is done post-scan, not in planner
                Ok(physical)
            }
        }
    }
    
    /// Plan a table scan with pruning
    async fn plan_scan(
        &self,
        table_uuid: Uuid,
        transaction_id: Uuid,
        projection: Option<Vec<String>>,
        predicate: Option<&Predicate>,
    ) -> Result<PhysicalPlan, PlanError> {
        // Get all files visible at transaction
        let files = self.catalog.list_files_at(table_uuid, transaction_id).await?;
        
        // Get partition spec for partition pruning
        let partition_spec = self.catalog.get_partition_spec(table_uuid).await?;
        
        // Get table statistics
        let table_stats = self.catalog.get_table_statistics(table_uuid, transaction_id).await?;
        
        // Prune files based on predicate
        let pruned_files = if let Some(pred) = predicate {
            self.prune_files(&files, pred, &partition_spec).await?
        } else {
            files
        };
        
        // Build file scans
        let mut file_scans = Vec::new();
        let mut total_cost = PlanCost::default();
        
        for file in pruned_files {
            // Get file-level statistics
            let file_stats = self.catalog.get_file_statistics(file.file_uuid).await?;
            
            // Determine which parts of file to scan (e.g., row groups)
            let hints = self.compute_scan_hints(&file, predicate, &file_stats).await?;
            
            // Determine pushable predicates
            let pushdown_filter = predicate.and_then(|p| self.extract_pushable_predicates(p, &file));
            
            let scan = FileScan {
                file: file.clone(),
                projection: projection.clone().unwrap_or_else(|| vec![]),
                pushdown_filter,
                hints,
            };
            
            // Update cost estimate
            total_cost.files_scanned += 1;
            total_cost.bytes_read += file.file_size_bytes as u64;
            total_cost.rows_scanned += file.record_count as u64;
            
            file_scans.push(scan);
        }
        
        // Estimate selectivity if we have a predicate
        let selectivity = predicate
            .map(|p| self.estimate_selectivity(p, &table_stats))
            .unwrap_or(1.0);
        
        total_cost.rows_returned = (total_cost.rows_scanned as f64 * selectivity) as u64;
        
        // Compute residual filter (parts not pushed down)
        let residual_filter = predicate.and_then(|p| {
            self.compute_residual_filter(p, &file_scans)
        });
        
        Ok(PhysicalPlan {
            file_scans,
            projection: projection.unwrap_or_default(),
            residual_filter,
            limit: None,
            cost: total_cost,
        })
    }
    
    /// Apply filter optimization to physical plan
    async fn apply_filter(
        &self,
        mut plan: PhysicalPlan,
        predicate: &Predicate,
    ) -> Result<PhysicalPlan, PlanError> {
        // Re-prune files with the new predicate
        let partition_spec = self.catalog.get_partition_spec_for_plan(&plan).await?;
        
        let mut remaining_files = Vec::new();
        
        for scan in plan.file_scans {
            if self.file_may_match(&scan.file, predicate, &partition_spec).await? {
                // Update pushdown filter
                let combined_filter = match scan.pushdown_filter {
                    Some(existing) => Some(Predicate::And(
                        Box::new(existing),
                        Box::new(predicate.clone()),
                    )),
                    None => Some(predicate.clone()),
                };
                
                remaining_files.push(FileScan {
                    pushdown_filter: combined_filter,
                    ..scan
                });
            }
        }
        
        plan.file_scans = remaining_files;
        plan.cost.files_scanned = plan.file_scans.len();
        
        // Update residual filter
        plan.residual_filter = Some(match plan.residual_filter {
            Some(existing) => Predicate::And(
                Box::new(existing),
                Box::new(predicate.clone()),
            ),
            None => predicate.clone(),
        });
        
        Ok(plan)
    }
}
```

## File Pruning

### Partition Pruning

```rust
impl PhysicalPlanner {
    /// Prune files based on partition values
    async fn prune_by_partition(
        &self,
        files: &[FileMetadata],
        predicate: &Predicate,
        partition_spec: &PartitionSpec,
    ) -> Vec<FileMetadata> {
        let partition_predicates = self.extract_partition_predicates(predicate, partition_spec);
        
        if partition_predicates.is_empty() {
            return files.to_vec();
        }
        
        files.iter()
            .filter(|file| {
                let partition_values = file.partition_values.as_ref();
                partition_predicates.iter().all(|pp| {
                    pp.matches(partition_values.unwrap_or(&PartitionValues::default()))
                })
            })
            .cloned()
            .collect()
    }
}
```

### Statistics-Based Pruning

```rust
impl PhysicalPlanner {
    /// Prune files based on column statistics
    async fn prune_by_statistics(
        &self,
        files: Vec<FileMetadata>,
        predicate: &Predicate,
    ) -> Result<Vec<FileMetadata>, PlanError> {
        let mut result = Vec::new();
        
        for file in files {
            let stats = self.catalog.get_file_column_stats(file.file_uuid).await?;
            
            if self.predicate_may_match(predicate, &stats) {
                result.push(file);
            }
        }
        
        Ok(result)
    }
    
    /// Check if predicate might match based on file statistics
    fn predicate_may_match(
        &self,
        predicate: &Predicate,
        stats: &HashMap<String, FileColumnStatistics>,
    ) -> bool {
        match predicate {
            Predicate::Eq(col, value) => {
                stats.get(col).map(|s| {
                    // Value must be between min and max
                    let in_range = match (&s.min_value, &s.max_value) {
                        (Some(min), Some(max)) => value >= min && value <= max,
                        _ => true, // No stats, be conservative
                    };
                    in_range
                }).unwrap_or(true)
            }
            
            Predicate::Lt(col, value) => {
                stats.get(col).map(|s| {
                    // Min must be less than value
                    s.min_value.as_ref().map(|min| min < value).unwrap_or(true)
                }).unwrap_or(true)
            }
            
            Predicate::Gt(col, value) => {
                stats.get(col).map(|s| {
                    // Max must be greater than value
                    s.max_value.as_ref().map(|max| max > value).unwrap_or(true)
                }).unwrap_or(true)
            }
            
            Predicate::Between(col, low, high) => {
                stats.get(col).map(|s| {
                    // Ranges must overlap
                    let min_ok = s.max_value.as_ref().map(|max| max >= low).unwrap_or(true);
                    let max_ok = s.min_value.as_ref().map(|min| min <= high).unwrap_or(true);
                    min_ok && max_ok
                }).unwrap_or(true)
            }
            
            Predicate::IsNull(col) => {
                stats.get(col).map(|s| {
                    s.null_count.map(|c| c > 0).unwrap_or(true)
                }).unwrap_or(true)
            }
            
            Predicate::IsNotNull(col) => {
                stats.get(col).map(|s| {
                    // File might have non-null values
                    true // Can't prune without knowing total row count
                }).unwrap_or(true)
            }
            
            Predicate::And(left, right) => {
                // Both must potentially match
                self.predicate_may_match(left, stats) && 
                self.predicate_may_match(right, stats)
            }
            
            Predicate::Or(left, right) => {
                // Either might match
                self.predicate_may_match(left, stats) || 
                self.predicate_may_match(right, stats)
            }
            
            Predicate::Not(inner) => {
                // Conservative: may match unless inner definitely matches all
                true
            }
            
            _ => true, // Conservative default
        }
    }
}
```

### Row Group Pruning (Parquet)

```rust
impl PhysicalPlanner {
    /// Compute row groups to scan for a Parquet file
    async fn prune_row_groups(
        &self,
        file: &FileMetadata,
        predicate: Option<&Predicate>,
    ) -> Result<Option<Vec<usize>>, PlanError> {
        if file.file_format != "parquet" || predicate.is_none() {
            return Ok(None); // Scan all row groups
        }
        
        let predicate = predicate.unwrap();
        
        // Read Parquet metadata
        let parquet_metadata = self.read_parquet_metadata(&file.file_path).await?;
        
        let mut selected_row_groups = Vec::new();
        
        for (rg_idx, rg_meta) in parquet_metadata.row_groups().iter().enumerate() {
            let mut may_match = true;
            
            for col_name in predicate.columns() {
                if let Some(col_idx) = parquet_metadata.schema().column_index(col_name) {
                    let col_chunk = rg_meta.column(col_idx);
                    
                    if let Some(stats) = col_chunk.statistics() {
                        let file_stats = FileColumnStatistics {
                            min_value: stats.min_value_opt(),
                            max_value: stats.max_value_opt(),
                            null_count: Some(stats.null_count() as u64),
                            nan_count: None,
                            distinct_count: stats.distinct_count().map(|c| c as u64),
                        };
                        
                        if !self.predicate_may_match(predicate, &[(col_name.to_string(), file_stats)].into()) {
                            may_match = false;
                            break;
                        }
                    }
                }
            }
            
            if may_match {
                selected_row_groups.push(rg_idx);
            }
        }
        
        if selected_row_groups.len() == parquet_metadata.row_groups().len() {
            Ok(None) // All row groups selected, no need for hint
        } else {
            Ok(Some(selected_row_groups))
        }
    }
}
```

## Selectivity Estimation

```rust
impl PhysicalPlanner {
    /// Estimate predicate selectivity (fraction of rows that match)
    fn estimate_selectivity(
        &self,
        predicate: &Predicate,
        stats: &TableStatistics,
    ) -> f64 {
        match predicate {
            Predicate::Eq(col, _) => {
                // Estimate: 1 / distinct_count
                stats.column_stats.get(col)
                    .and_then(|s| s.distinct_count)
                    .map(|d| 1.0 / d.max(1) as f64)
                    .unwrap_or(0.1) // Default 10% selectivity
            }
            
            Predicate::Lt(col, value) | Predicate::Le(col, value) |
            Predicate::Gt(col, value) | Predicate::Ge(col, value) => {
                // Estimate based on min/max range
                stats.column_stats.get(col)
                    .and_then(|s| {
                        match (&s.min_value, &s.max_value) {
                            (Some(min), Some(max)) => {
                                let range = scalar_diff(max, min)?;
                                let position = match predicate {
                                    Predicate::Lt(_, v) | Predicate::Le(_, v) => {
                                        scalar_diff(v, min)?
                                    }
                                    Predicate::Gt(_, v) | Predicate::Ge(_, v) => {
                                        scalar_diff(max, v)?
                                    }
                                    _ => unreachable!(),
                                };
                                Some((position / range).clamp(0.0, 1.0))
                            }
                            _ => None,
                        }
                    })
                    .unwrap_or(0.33) // Default 33% selectivity
            }
            
            Predicate::Between(col, low, high) => {
                // Range fraction
                stats.column_stats.get(col)
                    .and_then(|s| {
                        match (&s.min_value, &s.max_value) {
                            (Some(min), Some(max)) => {
                                let total_range = scalar_diff(max, min)?;
                                let query_range = scalar_diff(high, low)?;
                                Some((query_range / total_range).clamp(0.0, 1.0))
                            }
                            _ => None,
                        }
                    })
                    .unwrap_or(0.25)
            }
            
            Predicate::In(col, values) => {
                // num_values / distinct_count
                let num_values = values.len() as f64;
                stats.column_stats.get(col)
                    .and_then(|s| s.distinct_count)
                    .map(|d| (num_values / d.max(1) as f64).min(1.0))
                    .unwrap_or((num_values * 0.1).min(1.0))
            }
            
            Predicate::IsNull(col) => {
                // null_count / row_count
                stats.column_stats.get(col)
                    .and_then(|s| s.null_count)
                    .map(|n| n as f64 / stats.row_count.max(1) as f64)
                    .unwrap_or(0.01)
            }
            
            Predicate::IsNotNull(col) => {
                1.0 - self.estimate_selectivity(&Predicate::IsNull(col.clone()), stats)
            }
            
            Predicate::And(left, right) => {
                // Independence assumption: multiply selectivities
                let left_sel = self.estimate_selectivity(left, stats);
                let right_sel = self.estimate_selectivity(right, stats);
                left_sel * right_sel
            }
            
            Predicate::Or(left, right) => {
                // P(A OR B) = P(A) + P(B) - P(A AND B)
                let left_sel = self.estimate_selectivity(left, stats);
                let right_sel = self.estimate_selectivity(right, stats);
                (left_sel + right_sel - left_sel * right_sel).min(1.0)
            }
            
            Predicate::Not(inner) => {
                1.0 - self.estimate_selectivity(inner, stats)
            }
            
            _ => 0.5, // Default 50% for unknown predicates
        }
    }
}
```

## DataFusion Integration

### TableProvider Implementation

```rust
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::physical_plan::ExecutionPlan;

/// Planar table provider for DataFusion
pub struct PlanarTableProvider {
    catalog: Arc<SqlCatalog>,
    table_uuid: Uuid,
    transaction_id: Uuid,
    schema: Arc<ArrowSchema>,
}

#[async_trait]
impl TableProvider for PlanarTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    
    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Convert DataFusion filters to Planar predicates
        let predicate = convert_filters_to_predicate(filters);
        
        // Convert projection indices to column names
        let column_projection = projection.map(|indices| {
            indices.iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });
        
        // Build scan request
        let request = ScanRequest {
            table_uuid: self.table_uuid,
            transaction_id: self.transaction_id,
            projection: column_projection,
            predicate,
            limit,
            sort: None,
        };
        
        // Plan the scan
        let planner = PhysicalPlanner::new(self.catalog.clone());
        let logical = LogicalPlanBuilder::scan(self.table_uuid, self.transaction_id)
            .filter(predicate.unwrap_or(Predicate::True))
            .project(projection_columns)
            .limit(limit.unwrap_or(usize::MAX))
            .build();
        
        let physical = planner.plan(&logical).await?;
        
        // Convert to DataFusion ExecutionPlan
        Ok(Arc::new(PlanarExec::new(physical, self.schema.clone())))
    }
    
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Indicate which filters we can handle
        filters.iter()
            .map(|filter| {
                if is_pushable_filter(filter) {
                    Ok(TableProviderFilterPushDown::Exact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect()
    }
}

/// Planar execution plan for DataFusion
pub struct PlanarExec {
    physical_plan: PhysicalPlan,
    schema: SchemaRef,
}

impl ExecutionPlan for PlanarExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.physical_plan.file_scans.len())
    }
    
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let file_scan = &self.physical_plan.file_scans[partition];
        
        // Create reader for the file
        let reader = create_reader(file_scan)?;
        
        // Apply projection and pushdown filter
        let stream = reader.execute(
            &file_scan.projection,
            file_scan.pushdown_filter.as_ref(),
            file_scan.hints.clone(),
        )?;
        
        // Apply residual filter if needed
        if let Some(residual) = &self.physical_plan.residual_filter {
            Ok(Box::pin(FilteredStream::new(stream, residual.clone())))
        } else {
            Ok(stream)
        }
    }
    
    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics {
            num_rows: Precision::Inexact(self.physical_plan.cost.rows_returned as usize),
            total_byte_size: Precision::Inexact(self.physical_plan.cost.bytes_read as usize),
            column_statistics: vec![], // TODO: propagate column stats
        })
    }
}
```

## Statistics Maintenance

### Automatic Statistics Collection

```rust
pub struct StatisticsCollector {
    catalog: SqlCatalog,
}

impl StatisticsCollector {
    /// Collect statistics for a table
    pub async fn collect(
        &self,
        table_uuid: Uuid,
        transaction_id: Uuid,
    ) -> Result<TableStatistics, StatsError> {
        let files = self.catalog.list_files_at(table_uuid, transaction_id).await?;
        
        let mut total_rows = 0u64;
        let mut total_bytes = 0u64;
        let mut column_stats: HashMap<String, ColumnStatsAccumulator> = HashMap::new();
        
        for file in &files {
            total_rows += file.record_count as u64;
            total_bytes += file.file_size_bytes as u64;
            
            // Get file-level column stats
            let file_stats = self.catalog.get_file_column_stats(file.file_uuid).await?;
            
            for (col_name, stats) in file_stats {
                let acc = column_stats.entry(col_name.clone())
                    .or_insert_with(ColumnStatsAccumulator::new);
                acc.merge(&stats);
            }
        }
        
        // Build final statistics
        let column_statistics: HashMap<String, ColumnStatistics> = column_stats
            .into_iter()
            .map(|(name, acc)| (name, acc.finalize()))
            .collect();
        
        let stats = TableStatistics {
            row_count: total_rows,
            total_size_bytes: total_bytes,
            file_count: files.len(),
            computed_at: Utc::now(),
            transaction_id,
            column_statistics,
        };
        
        // Store statistics
        self.catalog.store_table_statistics(table_uuid, &stats).await?;
        
        Ok(stats)
    }
}

/// Accumulator for merging column statistics
struct ColumnStatsAccumulator {
    null_count: u64,
    min_value: Option<ScalarValue>,
    max_value: Option<ScalarValue>,
    distinct_hll: HyperLogLog,
}

impl ColumnStatsAccumulator {
    fn merge(&mut self, file_stats: &FileColumnStatistics) {
        if let Some(nc) = file_stats.null_count {
            self.null_count += nc;
        }
        
        // Update min
        if let Some(ref min) = file_stats.min_value {
            self.min_value = Some(match &self.min_value {
                Some(current) if min < current => min.clone(),
                Some(current) => current.clone(),
                None => min.clone(),
            });
        }
        
        // Update max
        if let Some(ref max) = file_stats.max_value {
            self.max_value = Some(match &self.max_value {
                Some(current) if max > current => max.clone(),
                Some(current) => current.clone(),
                None => max.clone(),
            });
        }
    }
    
    fn finalize(self) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Some(self.null_count),
            distinct_count: Some(self.distinct_hll.count()),
            min_value: self.min_value,
            max_value: self.max_value,
            avg_value: None,
            histogram: None,
        }
    }
}
```

## Implementation Phases

### Phase 1: Basic Planning

1. Implement Predicate data structure
2. Implement LogicalPlan and LogicalPlanBuilder
3. Implement basic PhysicalPlan
4. File listing without pruning

### Phase 2: File Pruning

1. Partition pruning
2. Statistics-based file pruning
3. Cost estimation

### Phase 3: Predicate Pushdown

1. Parquet predicate pushdown
2. Row group pruning
3. Lance/Vortex pushdown

### Phase 4: DataFusion Integration

1. TableProvider implementation
2. ExecutionPlan implementation
3. Statistics propagation

### Phase 5: Advanced Optimization

1. Selectivity estimation
2. Histogram support
3. Join planning (future)

## Testing Strategy

### Unit Tests

- Predicate simplification
- File pruning correctness
- Selectivity estimation accuracy
- CNF conversion

### Integration Tests

- End-to-end query planning
- DataFusion integration
- Statistics collection

### Performance Tests

- Pruning effectiveness (files skipped)
- Planning latency
- Query execution time

## Open Questions

1. **Join planning**: How do we plan joins between Planar tables? Should we support cross-table statistics?

2. **Adaptive execution**: Should plans adapt based on runtime statistics?

3. **Caching**: Should we cache physical plans for repeated queries?

4. **Cost model tuning**: How do we calibrate cost estimates for different environments?

5. **Histograms**: When should we collect and use histograms vs simple min/max?

## References

- [DataFusion Query Planning](https://arrow.apache.org/datafusion/user-guide/concepts.html)
- [Apache Calcite Cost-Based Optimization](https://calcite.apache.org/docs/algebra.html)
- [Iceberg Query Planning](https://iceberg.apache.org/docs/latest/spark-queries/)
- [file_formats.md](file_formats.md) - Format-specific capabilities
- [partitioning.md](partitioning.md) - Partition pruning
- [data_types.md](data_types.md) - Type system for predicates
