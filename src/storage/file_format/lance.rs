//! Lance file format implementation
//!
//! Provides readers and writers for Lance columnar format using Arrow RecordBatches.
//! Lance is a modern columnar data format designed for ML workloads with versioning,
//! random access, and efficient updates.

use std::path::Path;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchIterator;
use async_trait::async_trait;
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteParams};
use lance::dataset::write::WriteMode;
use serde_json::Value;
use crate::storage::{Reader, RecordBatchStream, Result, StorageError, Writer};
use crate::storage::file_format::path_to_utf8;

/// Lance file reader
///
/// Reads Lance datasets and returns all fragments concatenated into Arrow RecordBatches.
/// Use [`read_with_options`](Self::read_with_options) for format-specific features like
/// batch sizing and column projection.
#[derive(Debug, Default)]
pub struct LanceReader;

#[derive(Debug, Clone, Default)]
pub struct LanceReadOptions {
    pub batch_size: Option<usize>,
    pub columns: Option<Vec<String>>,
    pub filter: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub scan_in_order: Option<bool>,
    pub io_buffer_size: Option<u64>,
}

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
    /// let options = LanceReadOptions {
    ///     batch_size: Some(8192),
    ///     columns: None,
    ///     filter: None,
    ///     limit: None,
    ///     offset: None,
    ///     scan_in_order: None,
    ///     io_buffer_size: None,
    /// };
    /// let batch = reader.read_with_options(path, &options).await?;
    /// ```
    pub async fn read_with_options(
        &self,
        path: &Path,
        options: &LanceReadOptions,
    ) -> Result<RecordBatch> {
        let uri = path_to_utf8(path)?;
        let dataset = Dataset::open(uri).await?;

        let mut scanner = dataset.scan();
        apply_read_options(&mut scanner, options)?;
        let batch = scanner.try_into_batch().await?;

        Ok(batch)
    }

    pub async fn read_stream(
        &self,
        path: &Path,
        options: &LanceReadOptions,
    ) -> Result<RecordBatchStream> {
        let uri = path_to_utf8(path)?;
        let dataset = Dataset::open(uri).await?;

        let mut scanner = dataset.scan();
        apply_read_options(&mut scanner, options)?;
        let stream = scanner.try_into_stream().await?;
        let stream = stream.map_err(Into::into);
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl Reader for LanceReader {
    async fn read(&self, path: &Path) -> Result<RecordBatch> {
        self.read_with_options(path, &LanceReadOptions::default())
            .await
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

    /// Writes a RecordBatch with write parameters
    ///
    /// Provides access to Lance-specific features like storage options, write modes,
    /// and dataset configuration through JSON `format_options`.
    pub async fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: &WriteParams,
    ) -> Result<()> {
        let uri = path_to_utf8(path)?;
        let schema = batch.schema();
        let batches = vec![batch.clone()];

        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        Dataset::write(reader, uri, Some(options.clone())).await?;
        Ok(())
    }

    pub async fn write_stream(
        &self,
        stream: RecordBatchStream,
        path: &Path,
        options: &WriteParams,
    ) -> Result<()> {
        let uri = path_to_utf8(path)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let Some(first) = batches.first() else {
            return Err(StorageError::Unsupported(
                "write_stream requires at least one RecordBatch".to_string(),
            ));
        };
        let schema = first.schema();
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        Dataset::write(reader, uri, Some(options.clone())).await?;
        Ok(())
    }
}

pub fn parse_write_options(options: Option<&Value>) -> Result<WriteParams> {
    let Some(options) = options else {
        return Ok(WriteParams::default());
    };
    let object = options.as_object().ok_or_else(|| {
        StorageError::Unsupported("format_options must be a JSON object".to_string())
    })?;

    let mut params = WriteParams::default();
    for (key, value) in object {
        match key.as_str() {
            "max_rows_per_file" => {
                params.max_rows_per_file = parse_usize(key, value)?;
            }
            "max_rows_per_group" => {
                params.max_rows_per_group = parse_usize(key, value)?;
            }
            "max_bytes_per_file" => {
                params.max_bytes_per_file = parse_usize(key, value)?;
            }
            "mode" => {
                let val = value
                    .as_str()
                    .ok_or_else(|| StorageError::Unsupported("mode must be a string".to_string()))?;
                params.mode = parse_write_mode(val)?;
            }
            "enable_stable_row_ids" => {
                let val = value
                    .as_bool()
                    .ok_or_else(|| StorageError::Unsupported("enable_stable_row_ids must be a bool".to_string()))?;
                params.enable_stable_row_ids = val;
            }
            "enable_v2_manifest_paths" => {
                let val = value
                    .as_bool()
                    .ok_or_else(|| StorageError::Unsupported("enable_v2_manifest_paths must be a bool".to_string()))?;
                params.enable_v2_manifest_paths = val;
            }
            _ => {
                return Err(StorageError::Unsupported(format!(
                    "Unsupported option '{}' for format 'lance'",
                    key
                )))
            }
        }
    }

    Ok(params)
}

fn parse_usize(key: &str, value: &Value) -> Result<usize> {
    value
        .as_u64()
        .map(|v| v as usize)
        .ok_or_else(|| StorageError::Unsupported(format!("{} must be an integer", key)))
}

fn parse_write_mode(value: &str) -> Result<WriteMode> {
    WriteMode::try_from(value)
        .map_err(|_| StorageError::Unsupported(format!("Unsupported write mode: {}", value)))
}

#[async_trait]
impl Writer for LanceWriter {
    /// Writes a RecordBatch with default options
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        self.write_with_options(batch, path, &WriteParams::default())
            .await
    }
}

fn apply_read_options(
    scanner: &mut lance::dataset::scanner::Scanner,
    options: &LanceReadOptions,
) -> Result<()> {
    if let Some(batch_size) = options.batch_size {
        scanner.batch_size(batch_size);
    }
    if let Some(columns) = options.columns.as_ref() {
        scanner.project(columns)?;
    }
    if let Some(filter) = options.filter.as_ref() {
        scanner.filter(filter)?;
    }
    if options.limit.is_some() || options.offset.is_some() {
        scanner.limit(options.limit, options.offset)?;
    }
    if let Some(scan_in_order) = options.scan_in_order {
        scanner.scan_in_order(scan_in_order);
    }
    if let Some(io_buffer_size) = options.io_buffer_size {
        scanner.io_buffer_size(io_buffer_size);
    }
    Ok(())
}
