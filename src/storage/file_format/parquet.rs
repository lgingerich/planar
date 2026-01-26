//! Parquet file format implementation
//!
//! Provides readers and writers for Apache Parquet files using Arrow RecordBatches.
//! Use the trait methods for simple cases, or the `_with_options`/`_with_properties` 
//! methods for format-specific configuration like compression and encoding.

use std::{fs::File, path::Path};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, ArrowReaderOptions};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use crate::storage::{Reader, Writer, Result};
use crate::storage::StorageError;

/// Parquet file reader
///
/// Reads Parquet files and returns all row groups concatenated into a single RecordBatch.
/// Use [`read_with_options`](Self::read_with_options) for format-specific features like
/// page indices and metadata handling.
#[derive(Debug, Default)]
pub struct ParquetReader;

impl ParquetReader {
    /// Creates a new Parquet reader
    pub fn new() -> Self {
        Self
    }

    /// Reads a Parquet file with custom Arrow reader options
    ///
    /// Provides access to Parquet-specific features like page indices, metadata handling,
    /// and batch size configuration through [`ArrowReaderOptions`].
    pub fn read_with_options(&self, path: &Path, options: ArrowReaderOptions) -> Result<RecordBatch> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
        let schema = builder.schema().clone();
        let reader = builder.build()?;
        
        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
        
        match batches.len() {
            0 => Err(StorageError::Parquet(ParquetError::General("empty parquet file".to_string()))),
            1 => Ok(batches.into_iter().next().expect("length checked")),
            _ => arrow::compute::concat_batches(&schema, &batches).map_err(Into::into),
        }
    }
}

#[async_trait]
impl Reader for ParquetReader {
    async fn read(&self, path: &Path) -> Result<RecordBatch> {
        self.read_with_options(path, ArrowReaderOptions::default())
    }
}

/// Parquet file writer
///
/// Writes Arrow RecordBatches to Parquet files. Use [`write_with_options`](Self::write_with_options)
/// for format-specific configuration like compression, encoding, and row group sizing.
#[derive(Debug, Default)]
pub struct ParquetWriter;

impl ParquetWriter {
    /// Creates a new Parquet writer
    pub fn new() -> Self {
        Self
    }

    /// Writes a RecordBatch with custom writer options
    ///
    /// Provides access to Parquet-specific features like compression algorithms,
    /// encoding schemes, row group sizing, statistics, and dictionary encoding
    /// through [`WriterProperties`].
    pub fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: WriterProperties
    ) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(options))?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }
}

#[async_trait]
impl Writer for ParquetWriter {
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        self.write_with_options(batch, path, WriterProperties::default())
    }
}
