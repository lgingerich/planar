//! Lance file format implementation
//!
//! Provides readers and writers for Lance columnar format using Arrow RecordBatches.
//! Lance is a modern columnar data format designed for ML workloads with versioning,
//! random access, and efficient updates.

use crate::storage::file_format::path_to_utf8;
use crate::storage::{Reader, RecordBatchStream, Result, StorageError, Writer};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchIterator;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use serde_json::Value;
use std::path::Path;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

/// Lance file reader
///
/// Reads Lance datasets and returns all fragments concatenated into Arrow RecordBatches.
/// Use [`read_with_options`](Self::read_with_options) for format-specific features like
/// batch sizing and column projection.
#[derive(Debug, Default)]
pub struct LanceReader;

/// Read-time options for Lance scans.
#[derive(Debug, Clone, Default)]
pub struct LanceReadOptions {
    /// Optional record batch size for scanning.
    pub batch_size: Option<usize>,
    /// Optional column projection list.
    pub columns: Option<Vec<String>>,
    /// Optional filter expression string.
    pub filter: Option<String>,
    /// Optional row limit.
    pub limit: Option<i64>,
    /// Optional row offset.
    pub offset: Option<i64>,
    /// Optional scan ordering preference.
    pub scan_in_order: Option<bool>,
    /// Optional I/O buffer size in bytes.
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

    /// Streams a Lance dataset as a RecordBatch stream.
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
        options: WriteParams,
    ) -> Result<()> {
        let uri = path_to_utf8(path)?;
        let schema = batch.schema();
        let batches = vec![batch.clone()];

        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        Dataset::write(reader, uri, Some(options)).await?;
        Ok(())
    }

    /// Writes a stream of RecordBatches to a Lance dataset.
    pub async fn write_stream(
        &self,
        mut stream: RecordBatchStream,
        path: &Path,
        options: WriteParams,
    ) -> Result<()> {
        let uri = path_to_utf8(path)?;
        let first = match stream.next().await {
            Some(batch) => batch?,
            None => {
                return Err(StorageError::Unsupported(
                    "write_stream requires at least one RecordBatch".to_string(),
                ));
            }
        };
        let schema = first.schema();
        let (tx, rx) = mpsc::channel::<std::result::Result<RecordBatch, ArrowError>>(2);
        let expected_schema = schema.clone();

        let pump = tokio::spawn(async move {
            while let Some(batch) = stream.next().await {
                let batch = match batch {
                    Ok(batch) => {
                        if batch.schema() != expected_schema {
                            let err = schema_mismatch_error(&expected_schema, batch.schema());
                            let _ = tx.send(Err(err)).await;
                            return;
                        }
                        batch
                    }
                    Err(err) => {
                        let err = ArrowError::ExternalError(Box::new(err));
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                };
                if tx.send(Ok(batch)).await.is_err() {
                    break;
                }
            }
        });

        let reader = StreamRecordBatchReader::new(schema, first, rx);
        let params = options;
        let uri = uri.to_string();
        let handle = Handle::current();

        let write_result = tokio::task::spawn_blocking(move || {
            handle.block_on(Dataset::write(reader, &uri, Some(params)))
        })
        .await
        .map_err(|err| StorageError::Unsupported(format!("write_stream join error: {err}")))?
        .map_err(Into::into);

        let pump_result = pump
            .await
            .map_err(|err| StorageError::Unsupported(format!("write_stream pump error: {err}")));

        if let Err(err) = write_result {
            let _ = pump_result?;
            return Err(err);
        }
        pump_result?;

        Ok(())
    }
}

/// Parses Lance write options from JSON into `WriteParams`.
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
                let val = value.as_str().ok_or_else(|| {
                    StorageError::Unsupported("mode must be a string".to_string())
                })?;
                params.mode = parse_write_mode(val)?;
            }
            "enable_stable_row_ids" => {
                let val = value.as_bool().ok_or_else(|| {
                    StorageError::Unsupported("enable_stable_row_ids must be a bool".to_string())
                })?;
                params.enable_stable_row_ids = val;
            }
            "enable_v2_manifest_paths" => {
                let val = value.as_bool().ok_or_else(|| {
                    StorageError::Unsupported("enable_v2_manifest_paths must be a bool".to_string())
                })?;
                params.enable_v2_manifest_paths = val;
            }
            _ => {
                return Err(StorageError::Unsupported(format!(
                    "Unsupported option '{}' for format 'lance'",
                    key
                )));
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
        self.write_with_options(batch, path, WriteParams::default())
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

struct StreamRecordBatchReader {
    schema: SchemaRef,
    first: Option<std::result::Result<RecordBatch, ArrowError>>,
    rx: mpsc::Receiver<std::result::Result<RecordBatch, ArrowError>>,
}

impl StreamRecordBatchReader {
    fn new(
        schema: SchemaRef,
        first: RecordBatch,
        rx: mpsc::Receiver<std::result::Result<RecordBatch, ArrowError>>,
    ) -> Self {
        Self {
            schema,
            first: Some(Ok(first)),
            rx,
        }
    }
}

impl Iterator for StreamRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first) = self.first.take() {
            return Some(first);
        }
        self.rx.blocking_recv()
    }
}

impl arrow_array::RecordBatchReader for StreamRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn schema_mismatch_error(expected: &SchemaRef, actual: SchemaRef) -> ArrowError {
    ArrowError::SchemaError(format!(
        "Schema mismatch in stream: expected {:?}, got {:?}",
        expected, actual
    ))
}

#[cfg(test)]
mod tests {
    use super::{LanceReadOptions, LanceReader, LanceWriter};
    use crate::storage::{StorageError, Writer};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use futures::stream;
    use lance::dataset::{Dataset, WriteMode, WriteParams};
    use std::sync::Arc;

    #[tokio::test]
    async fn write_stream_round_trip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("record batch");

        let path = std::env::temp_dir().join(format!("planar_lance_test_{}", uuid::Uuid::new_v4()));

        let stream = Box::pin(stream::iter(vec![Ok(batch.clone()), Ok(batch.clone())]));
        LanceWriter::new()
            .write_stream(stream, &path, Default::default())
            .await
            .expect("write stream");

        let reader = LanceReader::new();
        let read = reader
            .read_with_options(&path, &LanceReadOptions::default())
            .await
            .expect("read");

        assert_eq!(read.num_columns(), batch.num_columns());
        assert_eq!(read.num_rows(), batch.num_rows() * 2);
        for idx in 0..read.num_columns() {
            assert!(read.column(idx).len() == batch.column(idx).len() * 2);
        }
    }

    #[tokio::test]
    async fn write_stream_empty_is_error() {
        let path =
            std::env::temp_dir().join(format!("planar_lance_empty_{}", uuid::Uuid::new_v4()));
        let stream = Box::pin(stream::empty());
        let err = LanceWriter::new()
            .write_stream(stream, &path, Default::default())
            .await
            .expect_err("empty stream should error");
        assert!(
            err.to_string()
                .contains("write_stream requires at least one RecordBatch")
        );
    }

    #[tokio::test]
    async fn write_stream_propagates_stream_error() {
        let path = std::env::temp_dir().join(format!("planar_lance_err_{}", uuid::Uuid::new_v4()));
        let stream = Box::pin(stream::iter(vec![Err(StorageError::Unsupported(
            "boom".to_string(),
        ))]));
        let err = LanceWriter::new()
            .write_stream(stream, &path, Default::default())
            .await
            .expect_err("stream error should surface");
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn write_stream_schema_mismatch_is_error() {
        let schema_a = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let batch_a =
            RecordBatch::try_new(schema_a, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
                .expect("batch a");
        let batch_b = RecordBatch::try_new(
            schema_b,
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .expect("batch b");

        let path = std::env::temp_dir().join(format!(
            "planar_lance_schema_mismatch_{}",
            uuid::Uuid::new_v4()
        ));
        let stream = Box::pin(stream::iter(vec![Ok(batch_a), Ok(batch_b)]));
        let err = LanceWriter::new()
            .write_stream(stream, &path, Default::default())
            .await
            .expect_err("schema mismatch should error");
        assert!(err.to_string().contains("Schema mismatch"));
    }

    #[tokio::test]
    async fn write_with_options_append_mode_appends_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("record batch");

        let path =
            std::env::temp_dir().join(format!("planar_lance_append_{}", uuid::Uuid::new_v4()));

        LanceWriter::new()
            .write(&batch, &path)
            .await
            .expect("initial write");

        let options = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };
        LanceWriter::new()
            .write_with_options(&batch, &path, options)
            .await
            .expect("append write");

        let reader = LanceReader::new();
        let read = reader
            .read_with_options(&path, &LanceReadOptions::default())
            .await
            .expect("read");
        assert_eq!(read.num_rows(), batch.num_rows() * 2);
    }

    #[tokio::test]
    async fn write_stream_append_mode_appends_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("record batch");

        let path = std::env::temp_dir().join(format!(
            "planar_lance_stream_append_{}",
            uuid::Uuid::new_v4()
        ));

        LanceWriter::new()
            .write(&batch, &path)
            .await
            .expect("initial write");

        let options = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };
        let expected_rows = batch.num_rows() * 2;
        let stream = Box::pin(stream::iter(vec![Ok(batch.clone())]));
        LanceWriter::new()
            .write_stream(stream, &path, options)
            .await
            .expect("append stream write");

        let reader = LanceReader::new();
        let read = reader
            .read_with_options(&path, &LanceReadOptions::default())
            .await
            .expect("read");
        assert_eq!(read.num_rows(), expected_rows);
    }

    #[tokio::test]
    async fn write_with_options_max_rows_per_file_splits_fragments() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("record batch");

        let path =
            std::env::temp_dir().join(format!("planar_lance_max_rows_{}", uuid::Uuid::new_v4()));

        let options = WriteParams {
            max_rows_per_file: 1,
            ..Default::default()
        };
        LanceWriter::new()
            .write_with_options(&batch, &path, options)
            .await
            .expect("write with max_rows_per_file");

        let uri = path.to_str().expect("path utf-8");
        let dataset = Dataset::open(uri).await.expect("open dataset");
        let fragments = dataset.get_fragments();
        assert!(
            fragments.len() > 1,
            "expected multiple fragments for max_rows_per_file=1, got {}",
            fragments.len()
        );
    }
}
