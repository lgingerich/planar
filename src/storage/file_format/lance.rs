//! Lance file format implementation
//!
//! Provides readers and writers for Lance columnar format using Arrow RecordBatches.
//! Lance is a modern columnar data format designed for ML workloads with versioning,
//! random access, and efficient updates.

use std::path::Path;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchIterator;
use async_trait::async_trait;
use lance::dataset::{Dataset, WriteParams};
use crate::storage::{Reader, Writer, Result};

/// Lance file reader
///
/// Reads Lance datasets and returns all fragments concatenated into Arrow RecordBatches.
/// Use [`read_with_options`](Self::read_with_options) for format-specific features like
/// batch sizing and column projection.
#[derive(Debug, Default)]
pub struct LanceReader;

impl LanceReader {
    /// Creates a new Lance reader
    pub fn new() -> Self {
        Self
    }

    /// Reads a Lance dataset with custom scanner options
    ///
    /// Provides access to Lance-specific features like batch sizing, column projection,
    /// I/O buffer configuration, and scan ordering.
    /// 
    /// # Example
    /// ```ignore
    /// let reader = LanceReader::new();
    /// let batch = reader.read_with_options(path, |scanner| {
    ///     scanner.batch_size(8192)
    /// }).await?;
    /// ```
    pub async fn read_with_options<F>(&self, path: &Path, configure: F) -> Result<RecordBatch>
    where
        F: FnOnce(&mut lance::dataset::scanner::Scanner) -> &mut lance::dataset::scanner::Scanner,
    {
        let uri = path.to_string_lossy().to_string();
        let dataset = Dataset::open(&uri).await?;
        
        let mut scanner = dataset.scan();
        configure(&mut scanner);
        let batch = scanner.try_into_batch().await?;
        
        Ok(batch)
    }
}

#[async_trait]
impl Reader for LanceReader {
    async fn read(&self, path: &Path) -> Result<RecordBatch> {
        let uri = path.to_string_lossy().to_string();
        let dataset = Dataset::open(&uri).await?;
        
        let scanner = dataset.scan();
        let batch = scanner.try_into_batch().await?;
        
        Ok(batch)
    }
}

/// Lance file writer
///
/// Writes Arrow RecordBatches to Lance datasets. Use [`write_with_options`](Self::write_with_options)
/// for format-specific configuration like storage options and write mode.
#[derive(Debug, Default)]
pub struct LanceWriter;

impl LanceWriter {
    /// Creates a new Lance writer
    pub fn new() -> Self {
        Self
    }

    /// Writes a RecordBatch with custom write options
    ///
    /// Provides access to Lance-specific features like storage options, write modes,
    /// and dataset configuration through [`WriteParams`].
    pub async fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: WriteParams
    ) -> Result<()> {
        let uri = path.to_string_lossy().to_string();
        let schema = batch.schema();
        let batches = vec![batch.clone()];
        
        let reader = RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema
        );
        
        Dataset::write(reader, &uri, Some(options)).await?;
        Ok(())
    }
}

#[async_trait]
impl Writer for LanceWriter {
    /// Writes a RecordBatch with default options
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        self.write_with_options(batch, path, WriteParams::default()).await
    }
}
