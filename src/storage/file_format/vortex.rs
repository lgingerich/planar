//! Vortex file format implementation
//!
//! Provides readers and writers for Vortex columnar format using Arrow RecordBatches.
//! Vortex is a next-generation format designed for high-performance data processing
//! with fast random access and pluggable encoding strategies.
//!
//! **Note**: Vortex integration requires conversion between Arrow and Vortex array types.
//! Use [`read_with_options`](VortexReader::read_with_options) and
//! [`write_with_options`](VortexWriter::write_with_options) for format-specific configuration.

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use vortex::arrow::FromArrowArray;
use vortex::dtype::DType;
use vortex::error::VortexError;
use vortex::stream::ArrayStreamExt;
use vortex::ArrayRef;
use vortex::file::{VortexOpenOptions, OpenOptionsSessionExt, VortexWriteOptions};
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;
use serde_json::Value;

use crate::storage::{Reader, RecordBatchStream, Result, StorageError, Writer};
use crate::storage::file_format::path_to_utf8;

/// Vortex file reader
///
/// Reads Vortex files and returns Arrow RecordBatches.
/// Use [`read_with_options`](Self::read_with_options) for format-specific features like
/// initial read size, segment caching, and dtype configuration.
#[derive(Debug, Default)]
pub struct VortexReader;

/// Read-time options for Vortex scans.
#[derive(Debug, Clone, Default)]
pub struct VortexReadOptions {
    /// Optional initial read size hint in bytes.
    pub initial_read_size: Option<usize>,
    /// Optional segment cache toggle.
    pub segment_cache: Option<bool>,
}

impl VortexReader {
    /// Creates a new Vortex reader
    pub fn new() -> Self {
        Self
    }

    /// Reads a Vortex file with custom open options
    ///
    /// Provides access to Vortex-specific features like segment caching,
    /// initial read sizing, and dtype configuration through a closure that
    /// configures [`VortexOpenOptions`].
    ///
    /// # Example
    /// ```ignore
    /// let reader = VortexReader::new();
    /// let options = VortexReadOptions {
    ///     initial_read_size: Some(8192),
    ///     segment_cache: Some(false),
    /// };
    /// let batch = reader.read_with_options(path, &options).await?;
    /// ```
    pub async fn read_with_options(
        &self,
        path: &Path,
        options: &VortexReadOptions,
    ) -> Result<RecordBatch> {
        let path_str = path_to_utf8(path)?;
        let open_options = apply_read_options(options);

        let array = open_options
            .open(path_str)
            .await?
            .scan()?
            .into_array_stream()?
            .read_all()
            .await?;

        let batch = RecordBatch::try_from(array.as_ref())?;
        Ok(batch)
    }

    /// Streams a Vortex file as a RecordBatch stream.
    pub async fn read_stream(
        &self,
        path: &Path,
        options: &VortexReadOptions,
    ) -> Result<RecordBatchStream> {
        let path_str = path_to_utf8(path)?;
        let open_options = apply_read_options(options);

        let stream = ArrayStreamExt::boxed(open_options
            .open(path_str)
            .await?
            .scan()?
            .into_array_stream()?);

        let stream = stream.map(|result: std::result::Result<ArrayRef, VortexError>| {
            result
                .map_err(Into::into)
                .and_then(|array: ArrayRef| RecordBatch::try_from(array.as_ref()).map_err(Into::into))
        });

        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl Reader for VortexReader {
    async fn read(&self, path: &Path) -> Result<RecordBatch> {
        self.read_with_options(path, &VortexReadOptions::default())
            .await
    }
}

/// Vortex file writer
///
/// Writes Arrow RecordBatches to Vortex files. Use [`write_with_options`](Self::write_with_options)
/// for format-specific configuration like layout strategy and file statistics.
#[derive(Debug, Default)]
pub struct VortexWriter;

impl VortexWriter {
    /// Creates a new Vortex writer
    pub fn new() -> Self {
        Self
    }

    /// Writes a RecordBatch with Vortex write options
    ///
    /// Provides access to Vortex-specific features like layout strategies,
    /// file statistics configuration, and dtype exclusion through JSON
    /// `format_options`.
    pub async fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: &VortexWriteOptions,
    ) -> Result<()> {
        let vortex_array = ArrayRef::from_arrow(batch.clone(), false);
        let stream = vortex_array.to_array_stream();

        let mut file = tokio::fs::File::create(path).await?;
        let write_opts = options.clone();
        let _summary = write_opts.write(&mut file, stream).await?;

        Ok(())
    }

    /// Writes a stream of RecordBatches to a Vortex file.
    pub async fn write_stream(
        &self,
        stream: RecordBatchStream,
        path: &Path,
        options: &VortexWriteOptions,
    ) -> Result<()> {
        let mut source = stream;
        let first = match source.next().await {
            Some(batch) => batch?,
            None => {
                return Err(StorageError::Unsupported(
                    "write_stream requires at least one RecordBatch".to_string(),
                ))
            }
        };
        let first_array = ArrayRef::from_arrow(first, false);
        let dtype = first_array.dtype().clone();
        let expected_dtype = dtype.clone();

        let rest = source.map(move |batch| match batch {
            Ok(batch) => {
                let array = ArrayRef::from_arrow(batch, false);
                if array.dtype() != &expected_dtype {
                    return Err(vortex_dtype_mismatch_error(&expected_dtype, array.dtype()));
                }
                Ok(array)
            }
            Err(err) => Err(VortexError::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                err,
            ))),
        });
        let stream = futures::stream::once(async move { Ok(first_array) }).chain(rest);
        let array_stream = RecordBatchArrayStream::new(dtype, Box::pin(stream));

        let mut file = tokio::fs::File::create(path).await?;
        let write_opts = options.clone();
        let _summary = write_opts.write(&mut file, array_stream).await?;
        Ok(())
    }
}

/// Parses Vortex write options from JSON into `VortexWriteOptions`.
pub fn parse_write_options(options: Option<&Value>) -> Result<VortexWriteOptions> {
    let Some(options) = options else {
        return Ok(VortexWriteOptions::new(VortexSession::default()));
    };
    let object = options.as_object().ok_or_else(|| {
        StorageError::Unsupported("format_options must be a JSON object".to_string())
    })?;

    let mut parsed = VortexWriteOptions::new(VortexSession::default());
    for (key, value) in object {
        match key.as_str() {
            "exclude_dtype" => {
                let val = value
                    .as_bool()
                    .ok_or_else(|| StorageError::Unsupported("exclude_dtype must be a bool".to_string()))?;
                if val {
                    parsed = parsed.exclude_dtype();
                }
            }
            _ => {
                return Err(StorageError::Unsupported(format!(
                    "Unsupported option '{}' for format 'vortex'",
                    key
                )))
            }
        }
    }

    Ok(parsed)
}

#[async_trait]
impl Writer for VortexWriter {
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        self.write_with_options(
            batch,
            path,
            &VortexWriteOptions::new(VortexSession::default()),
        )
        .await
    }
}

fn apply_read_options(options: &VortexReadOptions) -> VortexOpenOptions {
    let session = VortexSession::default();
    let mut open_options = session.open_options();
    if let Some(size) = options.initial_read_size {
        open_options = open_options.with_initial_read_size(size);
    }
    if let Some(segment_cache) = options.segment_cache {
        if !segment_cache {
            open_options = open_options.without_segment_cache();
        }
    }
    open_options
}

struct RecordBatchArrayStream {
    dtype: DType,
    inner: Pin<Box<dyn Stream<Item = std::result::Result<ArrayRef, VortexError>> + Send>>,
}

impl RecordBatchArrayStream {
    fn new(
        dtype: DType,
        inner: Pin<Box<dyn Stream<Item = std::result::Result<ArrayRef, VortexError>> + Send>>,
    ) -> Self {
        Self { dtype, inner }
    }
}

impl Stream for RecordBatchArrayStream {
    type Item = std::result::Result<ArrayRef, VortexError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

impl vortex::stream::ArrayStream for RecordBatchArrayStream {
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

fn vortex_dtype_mismatch_error(expected: &DType, actual: &DType) -> VortexError {
    VortexError::from(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "dtype mismatch in stream: expected {:?}, got {:?}",
            expected, actual
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::{VortexReadOptions, VortexReader, VortexWriter};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use futures::stream;
    use std::sync::Arc;
    use vortex::file::VortexWriteOptions;
    use vortex::session::VortexSession;
    use crate::storage::StorageError;

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

        let path = std::env::temp_dir().join(format!(
            "planar_vortex_test_{}.vortex",
            uuid::Uuid::new_v4()
        ));

        let stream = Box::pin(stream::iter(vec![Ok(batch.clone()), Ok(batch.clone())]));
        let options = VortexWriteOptions::new(VortexSession::default());
        VortexWriter::new()
            .write_stream(stream, &path, &options)
            .await
            .expect("write stream");

        let reader = VortexReader::new();
        let read = reader
            .read_with_options(&path, &VortexReadOptions::default())
            .await
            .expect("read");

        assert_eq!(read.num_columns(), batch.num_columns());
        assert_eq!(read.num_rows(), batch.num_rows() * 2);
    }

    #[tokio::test]
    async fn write_stream_empty_is_error() {
        let path = std::env::temp_dir().join(format!(
            "planar_vortex_empty_{}.vortex",
            uuid::Uuid::new_v4()
        ));
        let stream = Box::pin(stream::empty());
        let options = VortexWriteOptions::new(VortexSession::default());
        let err = VortexWriter::new()
            .write_stream(stream, &path, &options)
            .await
            .expect_err("empty stream should error");
        assert!(err.to_string().contains("write_stream requires at least one RecordBatch"));
    }

    #[tokio::test]
    async fn write_stream_propagates_stream_error() {
        let path = std::env::temp_dir().join(format!(
            "planar_vortex_err_{}.vortex",
            uuid::Uuid::new_v4()
        ));
        let stream = Box::pin(stream::iter(vec![Err(StorageError::Unsupported(
            "boom".to_string(),
        ))]));
        let options = VortexWriteOptions::new(VortexSession::default());
        let err = VortexWriter::new()
            .write_stream(stream, &path, &options)
            .await
            .expect_err("stream error should surface");
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn write_stream_dtype_mismatch_is_error() {
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
            "planar_vortex_dtype_mismatch_{}.vortex",
            uuid::Uuid::new_v4()
        ));
        let stream = Box::pin(stream::iter(vec![Ok(batch_a), Ok(batch_b)]));
        let options = VortexWriteOptions::new(VortexSession::default());
        let err = VortexWriter::new()
            .write_stream(stream, &path, &options)
            .await
            .expect_err("dtype mismatch should error");
        assert!(err.to_string().contains("dtype mismatch"));
    }

    #[tokio::test]
    async fn write_with_options_round_trip() {
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
            "planar_vortex_write_opts_{}.vortex",
            uuid::Uuid::new_v4()
        ));

        let options = VortexWriteOptions::new(VortexSession::default()).exclude_dtype();
        VortexWriter::new()
            .write_with_options(&batch, &path, &options)
            .await
            .expect("write with options");

        let reader = VortexReader::new();
        let read = reader
            .read_with_options(&path, &VortexReadOptions::default())
            .await
            .expect("read");

        assert_eq!(read.num_columns(), batch.num_columns());
        assert_eq!(read.num_rows(), batch.num_rows());
    }

    #[tokio::test]
    async fn write_stream_with_options_round_trip() {
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
            "planar_vortex_stream_write_opts_{}.vortex",
            uuid::Uuid::new_v4()
        ));

        let options = VortexWriteOptions::new(VortexSession::default()).exclude_dtype();
        let stream = Box::pin(stream::iter(vec![Ok(batch.clone()), Ok(batch.clone())]));
        VortexWriter::new()
            .write_stream(stream, &path, &options)
            .await
            .expect("write stream with options");

        let reader = VortexReader::new();
        let read = reader
            .read_with_options(&path, &VortexReadOptions::default())
            .await
            .expect("read");

        assert_eq!(read.num_columns(), batch.num_columns());
        assert_eq!(read.num_rows(), batch.num_rows() * 2);
    }

    #[tokio::test]
    async fn write_with_options_exclude_dtype_reduces_file_size() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Int64, false),
            Field::new("c3", DataType::Int64, false),
            Field::new("c4", DataType::Int64, false),
            Field::new("c5", DataType::Int64, false),
            Field::new("c6", DataType::Int64, false),
            Field::new("c7", DataType::Int64, false),
            Field::new("c8", DataType::Int64, false),
        ]));
        let base = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(base.clone()),
                Arc::new(base.clone()),
                Arc::new(base.clone()),
                Arc::new(base.clone()),
                Arc::new(base.clone()),
                Arc::new(base.clone()),
                Arc::new(base.clone()),
                Arc::new(base),
            ],
        )
        .expect("record batch");

        let path_default = std::env::temp_dir().join(format!(
            "planar_vortex_default_dtype_{}.vortex",
            uuid::Uuid::new_v4()
        ));
        let path_exclude = std::env::temp_dir().join(format!(
            "planar_vortex_exclude_dtype_{}.vortex",
            uuid::Uuid::new_v4()
        ));

        let default_opts = VortexWriteOptions::new(VortexSession::default());
        VortexWriter::new()
            .write_with_options(&batch, &path_default, &default_opts)
            .await
            .expect("write default");

        let exclude_opts = VortexWriteOptions::new(VortexSession::default()).exclude_dtype();
        VortexWriter::new()
            .write_with_options(&batch, &path_exclude, &exclude_opts)
            .await
            .expect("write exclude_dtype");

        let default_size = std::fs::metadata(&path_default)
            .expect("default metadata")
            .len();
        let exclude_size = std::fs::metadata(&path_exclude)
            .expect("exclude metadata")
            .len();

        assert!(
            exclude_size < default_size,
            "expected exclude_dtype to reduce size (default {}, exclude {})",
            default_size,
            exclude_size
        );
    }
}
