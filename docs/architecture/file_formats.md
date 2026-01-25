# File Format Implementations

## Purpose

This document specifies Planar's multi-format file support, detailing the implementation requirements for Parquet, Lance, and Vortex formats. It covers type conversions, statistics extraction, predicate pushdown, and format-specific optimizations.

## Motivation

Planar's format-flexible design enables choosing the best format for each workload:

1. **Parquet**: Industry standard with broad ecosystem support. Best for interchange and compatibility.

2. **Lance**: Modern columnar format optimized for ML workloads. Efficient random access and versioning.

3. **Vortex**: Compressed columnar format with adaptive encoding. Best for storage efficiency.

Supporting multiple formats requires:
- Consistent type mappings across formats
- Unified statistics model
- Format-aware query optimization
- Seamless reading of mixed-format tables

## Current Implementation Status

The storage module ([src/storage/mod.rs](../../src/storage/mod.rs)) defines the `Reader` and `Writer` traits and a `Format` enum. Individual format implementations exist as stubs:

- [src/storage/file_format/parquet.rs](../../src/storage/file_format/parquet.rs) - Stub
- [src/storage/file_format/lance.rs](../../src/storage/file_format/lance.rs) - Stub
- [src/storage/file_format/vortex.rs](../../src/storage/file_format/vortex.rs) - Stub

This document specifies the complete implementation for each format.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Planar Type System                                │
│                        (data_types.md canonical types)                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                 │                 │
                    ▼                 ▼                 ▼
        ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
        │    Parquet    │   │     Lance     │   │    Vortex     │
        │   Converter   │   │   Converter   │   │   Converter   │
        └───────────────┘   └───────────────┘   └───────────────┘
                    │                 │                 │
                    ▼                 ▼                 ▼
        ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
        │   .parquet    │   │    .lance     │   │   .vortex     │
        │    files      │   │    files      │   │    files      │
        └───────────────┘   └───────────────┘   └───────────────┘
```

## Unified Interfaces

### Reader Trait

```rust
use std::path::Path;
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;

/// Extended reader trait with format-specific capabilities
pub trait Reader: Send + Sync {
    /// Read entire file into a single RecordBatch
    fn read(&self, path: &Path) -> Result<RecordBatch>;
    
    /// Read file schema without reading data
    fn read_schema(&self, path: &Path) -> Result<Arc<ArrowSchema>>;
    
    /// Create a streaming reader for large files
    fn read_stream(&self, path: &Path, batch_size: usize) -> Result<Box<dyn RecordBatchReader>>;
    
    /// Read with column projection (only read specified columns)
    fn read_projected(&self, path: &Path, columns: &[String]) -> Result<RecordBatch>;
    
    /// Read with predicate pushdown (filter at read time)
    fn read_filtered(
        &self, 
        path: &Path, 
        predicate: &Predicate,
        columns: Option<&[String]>,
    ) -> Result<RecordBatch>;
    
    /// Extract statistics from file without reading data
    fn read_statistics(&self, path: &Path) -> Result<FileStatistics>;
    
    /// Get row count without reading data (if available)
    fn row_count(&self, path: &Path) -> Result<Option<u64>>;
}
```

### Writer Trait

```rust
/// Extended writer trait with format-specific options
pub trait Writer: Send + Sync {
    /// Write a RecordBatch to a file
    fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()>;
    
    /// Write with specific options
    fn write_with_options(
        &self, 
        batch: &RecordBatch, 
        path: &Path,
        options: &WriteOptions,
    ) -> Result<()>;
    
    /// Create a streaming writer for incremental writes
    fn create_writer(&self, path: &Path, schema: &ArrowSchema) -> Result<Box<dyn BatchWriter>>;
}

/// Options for writing files
#[derive(Clone, Debug)]
pub struct WriteOptions {
    /// Target row group size (Parquet)
    pub row_group_size: Option<usize>,
    /// Compression codec
    pub compression: Compression,
    /// Whether to write statistics
    pub write_statistics: bool,
    /// Dictionary encoding threshold (Parquet)
    pub dictionary_threshold: Option<f64>,
    /// Sort columns for clustering
    pub sort_columns: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default)]
pub enum Compression {
    None,
    #[default]
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}
```

### Statistics Model

```rust
/// Unified file statistics across formats
#[derive(Clone, Debug)]
pub struct FileStatistics {
    /// Total row count
    pub row_count: u64,
    /// File size in bytes
    pub file_size_bytes: u64,
    /// Per-column statistics
    pub column_stats: HashMap<String, ColumnStatistics>,
}

/// Per-column statistics
#[derive(Clone, Debug)]
pub struct ColumnStatistics {
    /// Number of null values
    pub null_count: Option<u64>,
    /// Number of NaN values (for floating point)
    pub nan_count: Option<u64>,
    /// Minimum value (serialized)
    pub min_value: Option<Vec<u8>>,
    /// Maximum value (serialized)
    pub max_value: Option<Vec<u8>>,
    /// Distinct value count (approximate)
    pub distinct_count: Option<u64>,
    /// Total size in bytes (uncompressed)
    pub total_uncompressed_size: Option<u64>,
    /// Total size in bytes (compressed)
    pub total_compressed_size: Option<u64>,
}

impl ColumnStatistics {
    /// Check if a predicate might match based on min/max
    pub fn may_contain(&self, predicate: &Predicate, data_type: &DataType) -> bool {
        match predicate {
            Predicate::Eq(value) => {
                self.value_in_range(value, data_type)
            }
            Predicate::Lt(value) => {
                // May match if min < value
                self.min_less_than(value, data_type)
            }
            Predicate::Gt(value) => {
                // May match if max > value
                self.max_greater_than(value, data_type)
            }
            Predicate::Between(low, high) => {
                // May match if ranges overlap
                self.ranges_overlap(low, high, data_type)
            }
            Predicate::IsNull => {
                self.null_count.map(|c| c > 0).unwrap_or(true)
            }
            Predicate::IsNotNull => {
                // May match if not all values are null
                true
            }
            _ => true, // Conservative: may match
        }
    }
}
```

## Parquet Implementation

### Overview

Parquet is the primary format for Planar, offering:
- Excellent compression ratios
- Rich statistics in file footer
- Row group-level predicate pushdown
- Broad ecosystem compatibility

### Dependencies

```toml
[dependencies]
parquet = { version = "53", features = ["arrow", "async"] }
arrow = { version = "53", features = ["prettyprint"] }
```

### Type Mapping

| Planar Type | Parquet Physical | Parquet Logical | Notes |
|-------------|------------------|-----------------|-------|
| `boolean` | `BOOLEAN` | - | Direct mapping |
| `int8` | `INT32` | `INT(8, signed)` | Stored as INT32 |
| `int16` | `INT32` | `INT(16, signed)` | Stored as INT32 |
| `int32` | `INT32` | `INT(32, signed)` | Direct mapping |
| `int64` | `INT64` | `INT(64, signed)` | Direct mapping |
| `uint8` | `INT32` | `INT(8, unsigned)` | Stored as INT32 |
| `uint16` | `INT32` | `INT(16, unsigned)` | Stored as INT32 |
| `uint32` | `INT32` | `INT(32, unsigned)` | Stored as INT32 |
| `uint64` | `INT64` | `INT(64, unsigned)` | Stored as INT64 |
| `float32` | `FLOAT` | - | Direct mapping |
| `float64` | `DOUBLE` | - | Direct mapping |
| `decimal128(p,s)` | `FIXED_LEN_BYTE_ARRAY(16)` | `DECIMAL(p,s)` | 16-byte fixed |
| `decimal256(p,s)` | `FIXED_LEN_BYTE_ARRAY(32)` | `DECIMAL(p,s)` | 32-byte fixed |
| `string` | `BYTE_ARRAY` | `STRING` | UTF-8 encoded |
| `large_string` | `BYTE_ARRAY` | `STRING` | Same as string |
| `binary` | `BYTE_ARRAY` | - | Raw bytes |
| `large_binary` | `BYTE_ARRAY` | - | Same as binary |
| `fixed_binary(n)` | `FIXED_LEN_BYTE_ARRAY(n)` | - | Fixed size |
| `date32` | `INT32` | `DATE` | Days since epoch |
| `date64` | `INT64` | `DATE` | Milliseconds, converted |
| `timestamp(us,tz)` | `INT64` | `TIMESTAMP(MICROS,adj)` | Microseconds |
| `timestamp(ns,tz)` | `INT64` | `TIMESTAMP(NANOS,adj)` | Nanoseconds |
| `time32(s)` | `INT32` | `TIME(MILLIS,adj)` | Seconds to millis |
| `time64(us)` | `INT64` | `TIME(MICROS,adj)` | Microseconds |
| `list(T)` | `repeated group` | `LIST` | Nested list |
| `struct(...)` | `group` | - | Nested struct |
| `map(K,V)` | `repeated group` | `MAP` | Key-value pairs |

### Implementation

```rust
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use std::sync::Arc;

pub struct ParquetReader {
    /// Default batch size for streaming reads
    batch_size: usize,
}

impl ParquetReader {
    pub fn new() -> Self {
        Self { batch_size: 8192 }
    }
    
    pub fn with_batch_size(batch_size: usize) -> Self {
        Self { batch_size }
    }
}

impl Reader for ParquetReader {
    fn read(&self, path: &Path) -> Result<RecordBatch> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        
        // Collect all batches into one
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| StorageError::ArrowError(e.to_string()))
    }
    
    fn read_schema(&self, path: &Path) -> Result<Arc<ArrowSchema>> {
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let schema = reader.metadata().file_metadata().schema_descr();
        let arrow_schema = parquet::arrow::parquet_to_arrow_schema(
            schema, 
            reader.metadata().file_metadata().key_value_metadata()
        )?;
        Ok(Arc::new(arrow_schema))
    }
    
    fn read_stream(&self, path: &Path, batch_size: usize) -> Result<Box<dyn RecordBatchReader>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(batch_size);
        let reader = builder.build()?;
        Ok(Box::new(reader))
    }
    
    fn read_projected(&self, path: &Path, columns: &[String]) -> Result<RecordBatch> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        
        // Build projection mask
        let schema = builder.schema();
        let projection: Vec<usize> = columns.iter()
            .filter_map(|name| schema.index_of(name).ok())
            .collect();
        
        let reader = builder
            .with_projection(ProjectionMask::leaves(builder.parquet_schema(), projection))
            .build()?;
        
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| StorageError::ArrowError(e.to_string()))
    }
    
    fn read_filtered(
        &self,
        path: &Path,
        predicate: &Predicate,
        columns: Option<&[String]>,
    ) -> Result<RecordBatch> {
        let file = File::open(path)?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        
        // Apply column projection
        if let Some(cols) = columns {
            let schema = builder.schema();
            let projection: Vec<usize> = cols.iter()
                .filter_map(|name| schema.index_of(name).ok())
                .collect();
            builder = builder.with_projection(
                ProjectionMask::leaves(builder.parquet_schema(), projection)
            );
        }
        
        // Convert predicate to Parquet row filter
        if let Some(filter) = predicate_to_row_filter(predicate, builder.schema()) {
            builder = builder.with_row_filter(filter);
        }
        
        let reader = builder.build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(ArrowSchema::empty())));
        }
        
        arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| StorageError::ArrowError(e.to_string()))
    }
    
    fn read_statistics(&self, path: &Path) -> Result<FileStatistics> {
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        
        let mut row_count = 0u64;
        let mut column_stats: HashMap<String, ColumnStatistics> = HashMap::new();
        
        // Aggregate statistics from all row groups
        for rg in 0..metadata.num_row_groups() {
            let rg_meta = metadata.row_group(rg);
            row_count += rg_meta.num_rows() as u64;
            
            for col_idx in 0..rg_meta.num_columns() {
                let col_meta = rg_meta.column(col_idx);
                let col_path = col_meta.column_path().string();
                
                let stats = column_stats.entry(col_path.clone()).or_insert_with(|| {
                    ColumnStatistics {
                        null_count: Some(0),
                        nan_count: None,
                        min_value: None,
                        max_value: None,
                        distinct_count: None,
                        total_uncompressed_size: Some(0),
                        total_compressed_size: Some(0),
                    }
                });
                
                // Accumulate sizes
                if let Some(ref mut size) = stats.total_uncompressed_size {
                    *size += col_meta.uncompressed_size() as u64;
                }
                if let Some(ref mut size) = stats.total_compressed_size {
                    *size += col_meta.compressed_size() as u64;
                }
                
                // Extract min/max from statistics
                if let Some(col_stats) = col_meta.statistics() {
                    if let Some(ref mut null_count) = stats.null_count {
                        *null_count += col_stats.null_count() as u64;
                    }
                    
                    if col_stats.has_min_max_set() {
                        // Update min (keep smallest)
                        let min_bytes = col_stats.min_bytes();
                        if stats.min_value.is_none() || min_bytes < stats.min_value.as_ref().unwrap() {
                            stats.min_value = Some(min_bytes.to_vec());
                        }
                        
                        // Update max (keep largest)
                        let max_bytes = col_stats.max_bytes();
                        if stats.max_value.is_none() || max_bytes > stats.max_value.as_ref().unwrap() {
                            stats.max_value = Some(max_bytes.to_vec());
                        }
                    }
                    
                    if let Some(distinct) = col_stats.distinct_count() {
                        stats.distinct_count = Some(distinct as u64);
                    }
                }
            }
        }
        
        Ok(FileStatistics {
            row_count,
            file_size_bytes: metadata.file_metadata().num_rows() as u64,
            column_stats,
        })
    }
    
    fn row_count(&self, path: &Path) -> Result<Option<u64>> {
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        Ok(Some(reader.metadata().file_metadata().num_rows() as u64))
    }
}

pub struct ParquetWriter {
    properties: WriterProperties,
}

impl ParquetWriter {
    pub fn new() -> Self {
        Self {
            properties: WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                .build(),
        }
    }
    
    pub fn with_properties(properties: WriterProperties) -> Self {
        Self { properties }
    }
}

impl Writer for ParquetWriter {
    fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(self.properties.clone()))?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }
    
    fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: &WriteOptions,
    ) -> Result<()> {
        let mut props_builder = WriterProperties::builder();
        
        // Apply compression
        props_builder = match options.compression {
            Compression::None => props_builder.set_compression(parquet::basic::Compression::UNCOMPRESSED),
            Compression::Snappy => props_builder.set_compression(parquet::basic::Compression::SNAPPY),
            Compression::Gzip => props_builder.set_compression(parquet::basic::Compression::GZIP(Default::default())),
            Compression::Lz4 => props_builder.set_compression(parquet::basic::Compression::LZ4),
            Compression::Zstd => props_builder.set_compression(parquet::basic::Compression::ZSTD(Default::default())),
        };
        
        // Apply row group size
        if let Some(size) = options.row_group_size {
            props_builder = props_builder.set_max_row_group_size(size);
        }
        
        // Apply statistics
        if options.write_statistics {
            props_builder = props_builder.set_statistics_enabled(
                parquet::file::properties::EnabledStatistics::Page
            );
        }
        
        let properties = props_builder.build();
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }
    
    fn create_writer(&self, path: &Path, schema: &ArrowSchema) -> Result<Box<dyn BatchWriter>> {
        let file = File::create(path)?;
        let writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(self.properties.clone()))?;
        Ok(Box::new(ParquetBatchWriter { inner: writer }))
    }
}

struct ParquetBatchWriter {
    inner: ArrowWriter<File>,
}

impl BatchWriter for ParquetBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch)?;
        Ok(())
    }
    
    fn close(self: Box<Self>) -> Result<()> {
        self.inner.close()?;
        Ok(())
    }
}
```

### Row Group Handling

Parquet files are organized into row groups for efficient partial reads:

```rust
/// Row group-level predicate pushdown
pub fn prune_row_groups(
    metadata: &ParquetMetaData,
    predicate: &Predicate,
    schema: &ArrowSchema,
) -> Vec<usize> {
    let mut selected_row_groups = Vec::new();
    
    for rg_idx in 0..metadata.num_row_groups() {
        let rg_meta = metadata.row_group(rg_idx);
        
        // Check if predicate might match this row group
        let may_match = predicate.columns().iter().all(|col_name| {
            // Find column in row group
            let col_idx = schema.index_of(col_name).ok();
            if col_idx.is_none() {
                return true; // Column not found, be conservative
            }
            
            let col_meta = rg_meta.column(col_idx.unwrap());
            if let Some(stats) = col_meta.statistics() {
                // Use statistics to check if predicate might match
                predicate.may_match_statistics(col_name, stats)
            } else {
                true // No statistics, be conservative
            }
        });
        
        if may_match {
            selected_row_groups.push(rg_idx);
        }
    }
    
    selected_row_groups
}
```

## Lance Implementation

### Overview

Lance is a modern columnar format designed for ML workloads:
- Efficient random access by row ID
- Built-in versioning and time travel
- Optimized for vector similarity search
- Faster writes than Parquet

### Dependencies

```toml
[dependencies]
lance = "0.17"
lance-io = "0.17"
```

### Type Mapping

Lance uses Arrow types internally with some restrictions:

| Planar Type | Lance Support | Notes |
|-------------|---------------|-------|
| `boolean` | Yes | Direct mapping |
| `int8` - `int64` | Yes | Direct mapping |
| `uint8` - `uint64` | Yes | Direct mapping |
| `float32`, `float64` | Yes | Direct mapping |
| `decimal128` | Yes | Direct mapping |
| `decimal256` | No | Not supported in Lance |
| `string` | Yes | Direct mapping |
| `large_string` | No | Use `string` instead |
| `binary` | Yes | Direct mapping |
| `large_binary` | No | Use `binary` instead |
| `date32`, `date64` | Yes | Direct mapping |
| `timestamp` | Yes | All time units supported |
| `list` | Yes | Direct mapping |
| `large_list` | No | Use `list` instead |
| `struct` | Yes | Direct mapping |
| `map` | Limited | Converted to struct |
| `fixed_size_list` | Yes | Direct mapping |
| `fixed_size_binary` | Yes | Direct mapping |

### Implementation

```rust
use lance::dataset::Dataset;
use lance::io::ObjectStore;

pub struct LanceReader {
    /// Runtime for async operations
    runtime: tokio::runtime::Runtime,
}

impl LanceReader {
    pub fn new() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().unwrap(),
        }
    }
}

impl Reader for LanceReader {
    fn read(&self, path: &Path) -> Result<RecordBatch> {
        self.runtime.block_on(async {
            let dataset = Dataset::open(path.to_str().unwrap()).await?;
            let scanner = dataset.scan();
            let batches = scanner.try_into_stream().await?
                .try_collect::<Vec<_>>().await?;
            
            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(dataset.schema().into()));
            }
            
            arrow::compute::concat_batches(&batches[0].schema(), &batches)
                .map_err(|e| StorageError::ArrowError(e.to_string()))
        })
    }
    
    fn read_schema(&self, path: &Path) -> Result<Arc<ArrowSchema>> {
        self.runtime.block_on(async {
            let dataset = Dataset::open(path.to_str().unwrap()).await?;
            Ok(Arc::new(dataset.schema().into()))
        })
    }
    
    fn read_projected(&self, path: &Path, columns: &[String]) -> Result<RecordBatch> {
        self.runtime.block_on(async {
            let dataset = Dataset::open(path.to_str().unwrap()).await?;
            let scanner = dataset.scan()
                .project(columns)?;
            let batches = scanner.try_into_stream().await?
                .try_collect::<Vec<_>>().await?;
            
            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(Arc::new(ArrowSchema::empty())));
            }
            
            arrow::compute::concat_batches(&batches[0].schema(), &batches)
                .map_err(|e| StorageError::ArrowError(e.to_string()))
        })
    }
    
    fn read_filtered(
        &self,
        path: &Path,
        predicate: &Predicate,
        columns: Option<&[String]>,
    ) -> Result<RecordBatch> {
        self.runtime.block_on(async {
            let dataset = Dataset::open(path.to_str().unwrap()).await?;
            let mut scanner = dataset.scan();
            
            // Apply column projection
            if let Some(cols) = columns {
                scanner = scanner.project(cols)?;
            }
            
            // Apply filter
            let filter_expr = predicate_to_lance_filter(predicate)?;
            scanner = scanner.filter(filter_expr)?;
            
            let batches = scanner.try_into_stream().await?
                .try_collect::<Vec<_>>().await?;
            
            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(Arc::new(ArrowSchema::empty())));
            }
            
            arrow::compute::concat_batches(&batches[0].schema(), &batches)
                .map_err(|e| StorageError::ArrowError(e.to_string()))
        })
    }
    
    fn read_statistics(&self, path: &Path) -> Result<FileStatistics> {
        self.runtime.block_on(async {
            let dataset = Dataset::open(path.to_str().unwrap()).await?;
            
            // Lance statistics are per-fragment
            let mut row_count = 0u64;
            let column_stats = HashMap::new(); // Lance doesn't expose column stats directly
            
            for fragment in dataset.fragments() {
                row_count += fragment.count_rows().await? as u64;
            }
            
            Ok(FileStatistics {
                row_count,
                file_size_bytes: 0, // Would need to sum fragment sizes
                column_stats,
            })
        })
    }
    
    fn row_count(&self, path: &Path) -> Result<Option<u64>> {
        self.runtime.block_on(async {
            let dataset = Dataset::open(path.to_str().unwrap()).await?;
            Ok(Some(dataset.count_rows(None).await? as u64))
        })
    }
}

pub struct LanceWriter {
    runtime: tokio::runtime::Runtime,
}

impl LanceWriter {
    pub fn new() -> Self {
        Self {
            runtime: tokio::runtime::Runtime::new().unwrap(),
        }
    }
}

impl Writer for LanceWriter {
    fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        self.runtime.block_on(async {
            let reader = RecordBatchIterator::new(
                vec![Ok(batch.clone())].into_iter(),
                batch.schema(),
            );
            
            Dataset::write(reader, path.to_str().unwrap(), None).await?;
            Ok(())
        })
    }
    
    fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: &WriteOptions,
    ) -> Result<()> {
        self.runtime.block_on(async {
            let reader = RecordBatchIterator::new(
                vec![Ok(batch.clone())].into_iter(),
                batch.schema(),
            );
            
            let write_params = lance::dataset::WriteParams {
                max_rows_per_file: options.row_group_size.unwrap_or(1024 * 1024),
                ..Default::default()
            };
            
            Dataset::write(reader, path.to_str().unwrap(), Some(write_params)).await?;
            Ok(())
        })
    }
}

/// Convert Planar predicate to Lance filter expression
fn predicate_to_lance_filter(predicate: &Predicate) -> Result<String> {
    match predicate {
        Predicate::Eq(col, val) => Ok(format!("{} = {}", col, format_value(val))),
        Predicate::Ne(col, val) => Ok(format!("{} != {}", col, format_value(val))),
        Predicate::Lt(col, val) => Ok(format!("{} < {}", col, format_value(val))),
        Predicate::Le(col, val) => Ok(format!("{} <= {}", col, format_value(val))),
        Predicate::Gt(col, val) => Ok(format!("{} > {}", col, format_value(val))),
        Predicate::Ge(col, val) => Ok(format!("{} >= {}", col, format_value(val))),
        Predicate::IsNull(col) => Ok(format!("{} IS NULL", col)),
        Predicate::IsNotNull(col) => Ok(format!("{} IS NOT NULL", col)),
        Predicate::And(left, right) => {
            let l = predicate_to_lance_filter(left)?;
            let r = predicate_to_lance_filter(right)?;
            Ok(format!("({}) AND ({})", l, r))
        }
        Predicate::Or(left, right) => {
            let l = predicate_to_lance_filter(left)?;
            let r = predicate_to_lance_filter(right)?;
            Ok(format!("({}) OR ({})", l, r))
        }
        _ => Err(StorageError::Unsupported("Predicate not supported by Lance".into())),
    }
}
```

### Fragment-Level Operations

Lance organizes data into fragments (similar to row groups):

```rust
/// Read specific fragments for targeted access
pub async fn read_fragments(
    dataset: &Dataset,
    fragment_ids: &[u64],
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    
    for &frag_id in fragment_ids {
        let fragment = dataset.get_fragment(frag_id).await?
            .ok_or_else(|| StorageError::NotFound(format!("Fragment {}", frag_id)))?;
        
        let scanner = fragment.scan();
        let frag_batches = scanner.try_into_stream().await?
            .try_collect::<Vec<_>>().await?;
        batches.extend(frag_batches);
    }
    
    Ok(batches)
}
```

## Vortex Implementation

### Overview

Vortex is a compressed columnar format with:
- Adaptive encoding selection per column
- Better compression than Parquet for many workloads
- Direct compute on compressed data
- Lazy decompression

### Dependencies

```toml
[dependencies]
vortex = "0.1"
vortex-array = "0.1"
vortex-dtype = "0.1"
```

### Type Mapping

Vortex uses its own type system but maps well to Arrow:

| Planar Type | Vortex DType | Notes |
|-------------|--------------|-------|
| `boolean` | `Bool` | Direct mapping |
| `int8` - `int64` | `I8` - `I64` | Direct mapping |
| `uint8` - `uint64` | `U8` - `U64` | Direct mapping |
| `float32` | `F32` | Direct mapping |
| `float64` | `F64` | Direct mapping |
| `decimal128(p,s)` | `Decimal(p,s,128)` | With precision |
| `string` | `Utf8` | Direct mapping |
| `binary` | `Binary` | Direct mapping |
| `timestamp` | `Timestamp` | With time unit |
| `list` | `List` | Direct mapping |
| `struct` | `Struct` | Direct mapping |

### Implementation

```rust
use vortex::array::Array;
use vortex::arrow::FromArrow;
use vortex::file::{VortexFileWriter, VortexFileReader};

pub struct VortexReader;

impl VortexReader {
    pub fn new() -> Self {
        Self
    }
}

impl Reader for VortexReader {
    fn read(&self, path: &Path) -> Result<RecordBatch> {
        let file = File::open(path)?;
        let reader = VortexFileReader::new(file)?;
        
        // Read all arrays and convert to Arrow
        let vortex_array = reader.read_all()?;
        let arrow_array = vortex_array.to_arrow()?;
        
        // Convert to RecordBatch
        if let Some(struct_array) = arrow_array.as_any().downcast_ref::<StructArray>() {
            Ok(RecordBatch::from(struct_array))
        } else {
            Err(StorageError::InvalidFormat("Expected struct array".into()))
        }
    }
    
    fn read_schema(&self, path: &Path) -> Result<Arc<ArrowSchema>> {
        let file = File::open(path)?;
        let reader = VortexFileReader::new(file)?;
        let dtype = reader.dtype();
        
        // Convert Vortex dtype to Arrow schema
        let arrow_type = dtype.to_arrow()?;
        if let arrow::datatypes::DataType::Struct(fields) = arrow_type {
            Ok(Arc::new(ArrowSchema::new(fields)))
        } else {
            Err(StorageError::InvalidFormat("Expected struct type".into()))
        }
    }
    
    fn read_projected(&self, path: &Path, columns: &[String]) -> Result<RecordBatch> {
        let file = File::open(path)?;
        let reader = VortexFileReader::new(file)?;
        
        // Vortex supports column projection natively
        let vortex_array = reader.read_columns(columns)?;
        let arrow_array = vortex_array.to_arrow()?;
        
        if let Some(struct_array) = arrow_array.as_any().downcast_ref::<StructArray>() {
            Ok(RecordBatch::from(struct_array))
        } else {
            Err(StorageError::InvalidFormat("Expected struct array".into()))
        }
    }
    
    fn read_statistics(&self, path: &Path) -> Result<FileStatistics> {
        let file = File::open(path)?;
        let reader = VortexFileReader::new(file)?;
        let metadata = reader.metadata();
        
        // Extract statistics from Vortex metadata
        let mut column_stats = HashMap::new();
        
        for (col_name, col_meta) in metadata.column_metadata() {
            let stats = ColumnStatistics {
                null_count: col_meta.null_count(),
                nan_count: None, // Vortex doesn't track NaN separately
                min_value: col_meta.min_value().map(|v| v.to_bytes()),
                max_value: col_meta.max_value().map(|v| v.to_bytes()),
                distinct_count: col_meta.distinct_count(),
                total_uncompressed_size: Some(col_meta.uncompressed_size()),
                total_compressed_size: Some(col_meta.compressed_size()),
            };
            column_stats.insert(col_name.clone(), stats);
        }
        
        Ok(FileStatistics {
            row_count: metadata.row_count(),
            file_size_bytes: metadata.file_size(),
            column_stats,
        })
    }
    
    fn row_count(&self, path: &Path) -> Result<Option<u64>> {
        let file = File::open(path)?;
        let reader = VortexFileReader::new(file)?;
        Ok(Some(reader.metadata().row_count()))
    }
}

pub struct VortexWriter;

impl VortexWriter {
    pub fn new() -> Self {
        Self
    }
}

impl Writer for VortexWriter {
    fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = VortexFileWriter::new(file)?;
        
        // Convert Arrow to Vortex
        let struct_array = StructArray::from(batch.clone());
        let vortex_array = Array::from_arrow(&struct_array)?;
        
        writer.write_array(&vortex_array)?;
        writer.finish()?;
        
        Ok(())
    }
    
    fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: &WriteOptions,
    ) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = VortexFileWriter::new(file)?;
        
        // Configure compression (Vortex auto-selects encodings)
        if let Compression::None = options.compression {
            writer.set_compression_enabled(false);
        }
        
        let struct_array = StructArray::from(batch.clone());
        let vortex_array = Array::from_arrow(&struct_array)?;
        
        writer.write_array(&vortex_array)?;
        writer.finish()?;
        
        Ok(())
    }
}
```

### Compression-Aware Operations

Vortex can compute directly on compressed data:

```rust
/// Filter without full decompression
pub fn filter_compressed(
    array: &Array,
    predicate: &Predicate,
) -> Result<Array> {
    // Vortex pushes predicates into the compressed representation
    // when possible, avoiding full decompression
    let filter_mask = evaluate_predicate_on_array(array, predicate)?;
    array.filter(&filter_mask)
}
```

## Format Selection Guidelines

### Workload-Based Selection

| Workload | Recommended Format | Rationale |
|----------|-------------------|-----------|
| Analytics (OLAP) | Parquet | Best query engine support, excellent compression |
| ML/AI pipelines | Lance | Fast random access, vector search support |
| Storage-constrained | Vortex | Best compression ratios |
| Data exchange | Parquet | Universal compatibility |
| Real-time updates | Lance | Efficient append and update operations |
| Archive/cold storage | Vortex | Maximum compression |

### Per-Table Configuration

```rust
/// Format configuration in table properties
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FormatConfig {
    /// Default format for new files
    pub default_format: Format,
    /// Allowed formats (for validation)
    pub allowed_formats: Vec<Format>,
    /// Format-specific options
    pub parquet_options: Option<ParquetFormatOptions>,
    pub lance_options: Option<LanceFormatOptions>,
    pub vortex_options: Option<VortexFormatOptions>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParquetFormatOptions {
    pub compression: Compression,
    pub row_group_size: usize,
    pub enable_dictionary: bool,
    pub enable_statistics: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LanceFormatOptions {
    pub max_rows_per_fragment: usize,
    pub enable_index: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VortexFormatOptions {
    pub compression_level: u8,
    pub enable_cascading: bool,
}
```

## Mixed-Format Tables

Planar supports tables with files in multiple formats:

```rust
/// Read from mixed-format table
pub async fn read_mixed_format_table(
    catalog: &SqlCatalog,
    table_uuid: Uuid,
    transaction_id: Uuid,
) -> Result<Vec<RecordBatch>> {
    let files = catalog.list_files_at(table_uuid, transaction_id).await?;
    let mut batches = Vec::new();
    
    for file in files {
        let format = Format::from_str(&file.file_format)?;
        let reader = ReaderEnum::new(format);
        let batch = reader.read(Path::new(&file.file_path))?;
        batches.push(batch);
    }
    
    Ok(batches)
}
```

### Schema Normalization

When reading mixed formats, schemas must be compatible:

```rust
/// Normalize schemas across formats
pub fn normalize_schemas(batches: &[RecordBatch]) -> Result<Arc<ArrowSchema>> {
    if batches.is_empty() {
        return Ok(Arc::new(ArrowSchema::empty()));
    }
    
    let mut merged_schema = batches[0].schema();
    
    for batch in &batches[1..] {
        merged_schema = Arc::new(ArrowSchema::try_merge(vec![
            merged_schema.as_ref().clone(),
            batch.schema().as_ref().clone(),
        ])?);
    }
    
    Ok(merged_schema)
}
```

## Statistics Normalization

Different formats store statistics differently. Planar normalizes to a common model:

```rust
/// Extract and normalize statistics from any format
pub fn extract_normalized_statistics(
    path: &Path,
    format: Format,
) -> Result<FileStatistics> {
    let reader = ReaderEnum::new(format);
    let raw_stats = reader.read_statistics(path)?;
    
    // Normalize statistics values to Planar's canonical representation
    let mut normalized = FileStatistics {
        row_count: raw_stats.row_count,
        file_size_bytes: raw_stats.file_size_bytes,
        column_stats: HashMap::new(),
    };
    
    for (col_name, col_stats) in raw_stats.column_stats {
        // Convert min/max to canonical byte representation
        let normalized_stats = ColumnStatistics {
            null_count: col_stats.null_count,
            nan_count: col_stats.nan_count,
            min_value: col_stats.min_value, // Already in bytes
            max_value: col_stats.max_value,
            distinct_count: col_stats.distinct_count,
            total_uncompressed_size: col_stats.total_uncompressed_size,
            total_compressed_size: col_stats.total_compressed_size,
        };
        normalized.column_stats.insert(col_name, normalized_stats);
    }
    
    Ok(normalized)
}
```

## Implementation Phases

### Phase 1: Parquet (MVP)

1. Implement full Parquet reader with all capabilities
2. Implement Parquet writer with configurable options
3. Add statistics extraction
4. Add predicate pushdown
5. Add row group-level filtering

### Phase 2: Lance

1. Implement Lance reader/writer
2. Add fragment-level operations
3. Handle type restrictions (no large types)
4. Add async support throughout

### Phase 3: Vortex

1. Implement Vortex reader/writer
2. Add compression-aware statistics
3. Integrate with type system

### Phase 4: Unified Interface

1. Implement mixed-format reading
2. Add format selection heuristics
3. Add statistics normalization
4. Add format conversion utilities

## Testing Strategy

### Unit Tests

- Type conversion round-trips for each format
- Statistics extraction accuracy
- Predicate pushdown correctness
- Schema compatibility checks

### Integration Tests

- End-to-end read/write for each format
- Mixed-format table queries
- Large file handling
- Concurrent read/write operations

### Performance Tests

- Read throughput by format
- Write throughput by format
- Compression ratios comparison
- Statistics-based pruning effectiveness

## Open Questions

1. **Format versioning**: How do we handle format version upgrades? Should we migrate files on read?

2. **Async consistency**: Lance is async-first, Parquet sync-first. How do we provide a consistent API?

3. **Statistics reliability**: Different formats have different statistics quality. Should we re-compute statistics after write?

4. **Predicate compatibility**: Not all predicates are pushable to all formats. How do we handle this gracefully?

5. **Memory management**: Large files can exceed memory. What's our streaming/chunking strategy for each format?

## References

- [Apache Parquet Format Specification](https://parquet.apache.org/docs/file-format/)
- [Lance Format Documentation](https://lancedb.github.io/lance/)
- [Vortex Project](https://github.com/spiraldb/vortex)
- [Arrow Type System](https://arrow.apache.org/docs/format/Columnar.html)
- [data_types.md](data_types.md) - Planar type system
- [src/storage/mod.rs](../../src/storage/mod.rs) - Current storage interface
