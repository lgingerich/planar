//! Parquet file format implementation
//!
//! Provides readers and writers for Apache Parquet files using Arrow RecordBatches.
//! Use the trait methods for simple cases, or the `_with_options`/`_with_properties` 
//! methods for format-specific configuration like compression and encoding.

use std::path::Path;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_writer::{AsyncArrowWriter, AsyncFileWriter};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::{BloomFilterPosition, WriterProperties, WriterPropertiesBuilder, WriterVersion};
use serde_json::Value;
use std::str::FromStr;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use crate::storage::{Reader, RecordBatchStream, Result, StorageError, Writer};

/// Parquet file reader
///
/// Reads Parquet files and returns all row groups concatenated into a single RecordBatch.
/// Use [`read_with_options`](Self::read_with_options) for format-specific features like
/// page indices and metadata handling.
#[derive(Debug, Default)]
pub struct ParquetReader;

/// Read-time options for Parquet scans.
#[derive(Debug, Clone)]
pub struct ParquetReadOptions {
    /// Low-level Arrow reader configuration (e.g., page index usage).
    pub arrow_options: ArrowReaderOptions,
    /// Optional record batch size for streaming reads.
    pub batch_size: Option<usize>,
}

impl Default for ParquetReadOptions {
    fn default() -> Self {
        Self {
            arrow_options: ArrowReaderOptions::default(),
            batch_size: None,
        }
    }
}

impl ParquetReader {
    /// Creates a new Parquet reader
    pub fn new() -> Self {
        Self
    }

    /// Reads a Parquet file with custom Arrow reader options
    ///
    /// Provides access to Parquet-specific features like page indices, metadata handling,
    /// and batch size configuration through [`ArrowReaderOptions`].
    pub async fn read_with_options(
        &self,
        path: &Path,
        options: &ParquetReadOptions,
    ) -> Result<RecordBatch> {
        let file = File::open(path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(
            file,
            options.arrow_options.clone(),
        )
        .await?;
        let builder = apply_read_options(builder, options);

        let stream = builder.build()?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        match batches.len() {
            0 => Err(StorageError::Parquet(ParquetError::General(
                "empty parquet file".to_string(),
            ))),
            1 => Ok(batches.into_iter().next().expect("length checked")),
            _ => {
                let schema = batches[0].schema();
                arrow::compute::concat_batches(&schema, &batches).map_err(Into::into)
            }
        }
    }

    /// Streams a Parquet file as a RecordBatch stream.
    pub async fn read_stream(
        &self,
        path: &Path,
        options: &ParquetReadOptions,
    ) -> Result<RecordBatchStream> {
        let file = File::open(path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new_with_options(
            file,
            options.arrow_options.clone(),
        )
        .await?;
        let builder = apply_read_options(builder, options);

        let stream = builder.build()?;
        let stream = stream.map(|result| result.map_err(Into::into));
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl Reader for ParquetReader {
    async fn read(&self, path: &Path) -> Result<RecordBatch> {
        self.read_with_options(path, &ParquetReadOptions::default())
            .await
    }
}

fn apply_read_options<T>(
    mut builder: ParquetRecordBatchStreamBuilder<T>,
    options: &ParquetReadOptions,
) -> ParquetRecordBatchStreamBuilder<T> {
    if let Some(batch_size) = options.batch_size {
        builder = builder.with_batch_size(batch_size);
    }
    builder
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

    /// Writes a RecordBatch with JSON writer options
    ///
    /// Provides access to Parquet-specific features like compression algorithms,
    /// encoding schemes, row group sizing, statistics, and dictionary encoding
    /// through JSON `format_options`.
    pub async fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: &WriterProperties,
    ) -> Result<()> {
        let file = File::create(path).await?;
        let mut writer = AsyncArrowWriter::try_new(TokioAsyncWriter::new(file), batch.schema(), Some(options.clone()))?;
        writer.write(batch).await?;
        writer.close().await?;
        Ok(())
    }

    /// Writes a stream of RecordBatches to a Parquet file.
    pub async fn write_stream(
        &self,
        mut stream: RecordBatchStream,
        path: &Path,
        options: &WriterProperties,
    ) -> Result<()> {
        let file = File::create(path).await?;
        let first = match stream.next().await {
            Some(batch) => batch?,
            None => {
                return Err(StorageError::Unsupported(
                    "write_stream requires at least one RecordBatch".to_string(),
                ))
            }
        };
        let expected_schema = first.schema();
        let mut writer = AsyncArrowWriter::try_new(
            TokioAsyncWriter::new(file),
            expected_schema.clone(),
            Some(options.clone()),
        )?;

        writer.write(&first).await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.schema() != expected_schema {
                return Err(StorageError::Unsupported(format!(
                    "Schema mismatch in stream: expected {:?}, got {:?}",
                    expected_schema,
                    batch.schema()
                )));
            }
            writer.write(&batch).await?;
        }

        writer.close().await?;
        Ok(())
    }
}

/// Parses Parquet writer options from JSON into `WriterProperties`.
pub fn parse_write_options(options: Option<&Value>) -> Result<WriterProperties> {
    let Some(options) = options else {
        return Ok(WriterProperties::builder().build());
    };
    let object = options.as_object().ok_or_else(|| {
        StorageError::Unsupported("format_options must be a JSON object".to_string())
    })?;

    let mut builder = WriterPropertiesBuilder::default();
    for (key, value) in object {
        match key.as_str() {
            "compression" => {
                let val = value
                    .as_str()
                    .ok_or_else(|| StorageError::Unsupported("compression must be a string".to_string()))?;
                builder = builder.set_compression(parse_compression(val)?);
            }
            "max_row_group_size" => {
                builder = builder.set_max_row_group_size(parse_usize(key, value)?);
            }
            "data_page_size_limit" => {
                builder = builder.set_data_page_size_limit(parse_usize(key, value)?);
            }
            "data_page_row_count_limit" => {
                builder = builder.set_data_page_row_count_limit(parse_usize(key, value)?);
            }
            "write_batch_size" => {
                builder = builder.set_write_batch_size(parse_usize(key, value)?);
            }
            "writer_version" => {
                let val = value
                    .as_str()
                    .ok_or_else(|| StorageError::Unsupported("writer_version must be a string".to_string()))?;
                builder = builder.set_writer_version(parse_writer_version(val)?);
            }
            "bloom_filter_position" => {
                let val = value
                    .as_str()
                    .ok_or_else(|| StorageError::Unsupported("bloom_filter_position must be a string".to_string()))?;
                builder = builder.set_bloom_filter_position(parse_bloom_filter_position(val)?);
            }
            "created_by" => {
                let val = value
                    .as_str()
                    .ok_or_else(|| StorageError::Unsupported("created_by must be a string".to_string()))?;
                builder = builder.set_created_by(val.to_string());
            }
            "offset_index_disabled" => {
                let val = value
                    .as_bool()
                    .ok_or_else(|| StorageError::Unsupported("offset_index_disabled must be a bool".to_string()))?;
                builder = builder.set_offset_index_disabled(val);
            }
            _ => {
                return Err(StorageError::Unsupported(format!(
                    "Unsupported option '{}' for format 'parquet'",
                    key
                )))
            }
        }
    }

    Ok(builder.build())
}

fn parse_usize(key: &str, value: &Value) -> Result<usize> {
    value
        .as_u64()
        .map(|v| v as usize)
        .ok_or_else(|| StorageError::Unsupported(format!("{} must be an integer", key)))
}

fn parse_compression(value: &str) -> Result<Compression> {
    match value.to_lowercase().as_str() {
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "brotli" => Ok(Compression::BROTLI(Default::default())),
        "lz4" => Ok(Compression::LZ4),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        other => Err(StorageError::Unsupported(format!(
            "Unsupported compression: {}",
            other
        ))),
    }
}

fn parse_writer_version(value: &str) -> Result<WriterVersion> {
    WriterVersion::from_str(value)
        .map_err(|err| StorageError::Unsupported(err.to_string()))
}

fn parse_bloom_filter_position(value: &str) -> Result<BloomFilterPosition> {
    match value.to_lowercase().as_str() {
        "afterrowgroup" | "after_row_group" => Ok(BloomFilterPosition::AfterRowGroup),
        "end" => Ok(BloomFilterPosition::End),
        other => Err(StorageError::Unsupported(format!(
            "Unsupported bloom_filter_position: {}",
            other
        ))),
    }
}

#[async_trait]
impl Writer for ParquetWriter {
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        self.write_with_options(batch, path, &WriterProperties::builder().build())
            .await
    }
}

struct TokioAsyncWriter {
    inner: File,
}

impl TokioAsyncWriter {
    fn new(inner: File) -> Self {
        Self { inner }
    }
}

impl AsyncFileWriter for TokioAsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.inner.write_all(&bs).await?;
            Ok(())
        }
        .boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.inner.flush().await?;
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::{ParquetReadOptions, ParquetReader, ParquetWriter};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use futures::TryStreamExt;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use futures::stream;
    use crate::storage::StorageError;

    #[tokio::test]
    async fn read_matches_stream_read() {
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

        let path = std::env::temp_dir().join(format!(
            "planar_parquet_test_{}.parquet",
            uuid::Uuid::new_v4()
        ));

        ParquetWriter::new()
            .write_with_options(&batch, &path, &WriterProperties::builder().build())
            .await
            .expect("write parquet");

        let reader = ParquetReader::new();
        let direct = reader.read(&path).await.expect("read parquet");
        let stream = reader
            .read_stream(&path, &ParquetReadOptions::default())
            .await
            .expect("stream parquet");
        let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collect stream");
        let streamed = match batches.len() {
            0 => panic!("no batches returned"),
            1 => batches.into_iter().next().expect("length checked"),
            _ => concat_batches(&direct.schema(), &batches).expect("concat"),
        };

        assert_eq!(direct.num_rows(), streamed.num_rows());
        assert_eq!(direct.num_columns(), streamed.num_columns());
        for idx in 0..direct.num_columns() {
            assert!(direct.column(idx).equals(streamed.column(idx).as_ref()));
        }
    }

    #[tokio::test]
    async fn write_stream_propagates_stream_error() {
        let path = std::env::temp_dir().join(format!(
            "planar_parquet_err_{}.parquet",
            uuid::Uuid::new_v4()
        ));
        let stream = Box::pin(stream::iter(vec![Err(StorageError::Unsupported(
            "boom".to_string(),
        ))]));
        let err = ParquetWriter::new()
            .write_stream(stream, &path, &WriterProperties::builder().build())
            .await
            .expect_err("stream error should surface");
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn write_stream_schema_mismatch_is_error() {
        let schema_a = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let batch_a = RecordBatch::try_new(
            schema_a,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("batch a");
        let batch_b = RecordBatch::try_new(
            schema_b,
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .expect("batch b");

        let path = std::env::temp_dir().join(format!(
            "planar_parquet_schema_mismatch_{}.parquet",
            uuid::Uuid::new_v4()
        ));
        let stream = Box::pin(stream::iter(vec![Ok(batch_a), Ok(batch_b)]));
        let err = ParquetWriter::new()
            .write_stream(stream, &path, &WriterProperties::builder().build())
            .await
            .expect_err("schema mismatch should error");
        assert!(err.to_string().contains("Schema mismatch"));
    }
}
