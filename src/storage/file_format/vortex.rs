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
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use vortex::arrow::FromArrowArray;
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

#[derive(Debug, Clone, Default)]
pub struct VortexReadOptions {
    pub initial_read_size: Option<usize>,
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

    pub async fn read_stream(
        &self,
        path: &Path,
        options: &VortexReadOptions,
    ) -> Result<RecordBatchStream> {
        let path_str = path_to_utf8(path)?;
        let open_options = apply_read_options(options);

        let stream = open_options
            .open(path_str)
            .await?
            .scan()?
            .into_array_stream()?
            .boxed();

        let stream = stream.map(|result| {
            result
                .map_err(Into::into)
                .and_then(|array| RecordBatch::try_from(array.as_ref()).map_err(Into::into))
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
        let _summary = options.write(&mut file, stream).await?;

        Ok(())
    }

    pub async fn write_stream(
        &self,
        stream: RecordBatchStream,
        path: &Path,
        options: &VortexWriteOptions,
    ) -> Result<()> {
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let Some(first) = batches.first() else {
            return Err(StorageError::Unsupported(
                "write_stream requires at least one RecordBatch".to_string(),
            ));
        };
        let batch = if batches.len() == 1 {
            first.clone()
        } else {
            concat_batches(&first.schema(), &batches)?
        };
        self.write_with_options(&batch, path, options).await
    }
}

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
