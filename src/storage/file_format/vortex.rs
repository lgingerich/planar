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
use arrow_array::RecordBatch;
use async_trait::async_trait;
use vortex::arrow::FromArrowArray;
use vortex::stream::ArrayStreamExt;
use vortex::ArrayRef;
use vortex::file::{VortexOpenOptions, OpenOptionsSessionExt, VortexWriteOptions};
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

use crate::storage::{Reader, Writer, Result};
use crate::storage::StorageError;

/// Vortex file reader
///
/// Reads Vortex files and returns Arrow RecordBatches.
/// Use [`read_with_options`](Self::read_with_options) for format-specific features like
/// initial read size, segment caching, and dtype configuration.
#[derive(Debug, Default)]
pub struct VortexReader;

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
    /// let batch = reader.read_with_options(path, |opts| {
    ///     opts.with_initial_read_size(8192)
    ///         .without_segment_cache()
    /// }).await?;
    /// ```
    pub async fn read_with_options<F>(
        &self,
        path: &Path,
        configure: F,
    ) -> Result<RecordBatch>
    where
        F: FnOnce(VortexOpenOptions) -> VortexOpenOptions,
    {
        let session = VortexSession::default();

        let path_str = path.to_str().ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Path contains invalid UTF-8"
            ))
        })?;

        let options = configure(session.open_options());

        let array = options
            .open(path_str)
            .await?
            .scan()?
            .into_array_stream()?
            .read_all()
            .await?;

        let batch = RecordBatch::try_from(array.as_ref())?;
        Ok(batch)
    }
}

#[async_trait]
impl Reader for VortexReader {
    async fn read(&self, path: &Path) -> Result<RecordBatch> {
        self.read_with_options(path, |opts| opts).await
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

    /// Writes a RecordBatch with custom write options
    ///
    /// Provides access to Vortex-specific features like layout strategies,
    /// file statistics configuration, and dtype exclusion through a closure
    /// that configures [`VortexWriteOptions`].
    ///
    /// # Example
    /// ```ignore
    /// let writer = VortexWriter::new();
    /// writer.write_with_options(batch, path, |opts| {
    ///     opts.exclude_dtype()
    /// }).await?;
    /// ```
    pub async fn write_with_options<F>(
        &self,
        batch: &RecordBatch,
        path: &Path,
        configure: F,
    ) -> Result<()>
    where
        F: FnOnce(VortexWriteOptions) -> VortexWriteOptions,
    {
        let session = VortexSession::default();

        let vortex_array = ArrayRef::from_arrow(batch.clone(), false);
        let stream = vortex_array.to_array_stream();

        let mut file = tokio::fs::File::create(path).await?;
        let options = configure(VortexWriteOptions::new(session));
        let _summary = options.write(&mut file, stream).await?;

        Ok(())
    }
}

#[async_trait]
impl Writer for VortexWriter {
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        self.write_with_options(batch, path, |opts| opts).await
    }
}
