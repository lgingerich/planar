//! Storage abstractions and format dispatch for Arrow `RecordBatch` I/O.

/// Error types returned by storage operations.
pub mod error;
/// Format-specific readers, writers, and option parsing.
pub mod file_format;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::stream::Stream;
use lance::dataset::WriteParams;
use parquet::file::properties::WriterProperties;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use vortex::file::VortexWriteOptions;

pub use error::{Result, StorageError};

use file_format::lance::{LanceReadOptions, LanceReader, LanceWriter};
use file_format::parquet::{ParquetReadOptions, ParquetReader, ParquetWriter};
use file_format::vortex::{VortexReadOptions, VortexReader, VortexWriter};

/// Async stream of Arrow `RecordBatch` values.
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Reads a file format into Arrow batches.
#[async_trait]
pub trait Reader: Send + Sync {
    /// Reads `path` and returns all rows as one batch.
    async fn read(&self, path: &Path) -> Result<RecordBatch>;
}

/// Writes Arrow batches to a file format.
#[async_trait]
pub trait Writer: Send + Sync {
    /// Writes `batch` to `path`.
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()>;
}

/// Format-specific read options used by [`ReaderEnum`].
#[derive(Debug)]
pub enum FormatReadOptions {
    /// Parquet reader options
    Parquet(ParquetReadOptions),
    /// Lance reader options
    Lance(LanceReadOptions),
    /// Vortex reader options
    Vortex(VortexReadOptions),
}

/// Format-specific write options used by [`WriterEnum`].
pub enum FormatWriteOptions {
    /// Parquet writer properties
    Parquet(WriterProperties),
    /// Lance write parameters
    Lance(WriteParams),
    /// Vortex write options
    Vortex(VortexWriteOptions),
}

impl std::fmt::Debug for FormatWriteOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatWriteOptions::Parquet(_) => f.debug_tuple("Parquet").finish(),
            FormatWriteOptions::Lance(_) => f.debug_tuple("Lance").finish(),
            FormatWriteOptions::Vortex(_) => f.debug_tuple("Vortex").finish(),
        }
    }
}

/// Supported on-disk file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    /// Parquet format.
    Parquet,
    /// Lance format.
    Lance,
    /// Vortex format.
    Vortex,
}

impl FromStr for Format {
    type Err = StorageError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(Format::Parquet),
            "lance" => Ok(Format::Lance),
            "vortex" => Ok(Format::Vortex),
            _ => Err(StorageError::Unsupported(format!(
                "File format '{}' is not supported. Supported formats: parquet, lance, vortex",
                s
            ))),
        }
    }
}

impl Format {
    /// Returns the canonical lowercase format name.
    pub fn as_str(&self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Lance => "lance",
            Format::Vortex => "vortex",
        }
    }
}

/// Type-safe reader dispatch over supported formats.
#[derive(Debug)]
pub enum ReaderEnum {
    /// Parquet reader
    Parquet(ParquetReader),
    /// Lance reader
    Lance(LanceReader),
    /// Vortex reader
    Vortex(VortexReader),
}

impl ReaderEnum {
    /// Creates a reader for `format`.
    pub fn new(format: Format) -> Self {
        match format {
            Format::Parquet => ReaderEnum::Parquet(ParquetReader::new()),
            Format::Lance => ReaderEnum::Lance(LanceReader::new()),
            Format::Vortex => ReaderEnum::Vortex(VortexReader::new()),
        }
    }

    /// Reads a file using options matching the active format variant.
    pub async fn read_with_options(
        &self,
        path: &Path,
        options: &FormatReadOptions,
    ) -> Result<RecordBatch> {
        match (self, options) {
            (ReaderEnum::Parquet(r), FormatReadOptions::Parquet(options)) => {
                r.read_with_options(path, options).await
            }
            (ReaderEnum::Lance(r), FormatReadOptions::Lance(options)) => {
                r.read_with_options(path, options).await
            }
            (ReaderEnum::Vortex(r), FormatReadOptions::Vortex(options)) => {
                r.read_with_options(path, options).await
            }
            _ => Err(StorageError::Unsupported(
                "read options do not match reader format".to_string(),
            )),
        }
    }

    /// Streams a file with options matching the active format variant.
    pub async fn read_stream(
        &self,
        path: &Path,
        options: &FormatReadOptions,
    ) -> Result<RecordBatchStream> {
        match (self, options) {
            (ReaderEnum::Parquet(r), FormatReadOptions::Parquet(options)) => {
                r.read_stream(path, options).await
            }
            (ReaderEnum::Lance(r), FormatReadOptions::Lance(options)) => {
                r.read_stream(path, options).await
            }
            (ReaderEnum::Vortex(r), FormatReadOptions::Vortex(options)) => {
                r.read_stream(path, options).await
            }
            _ => Err(StorageError::Unsupported(
                "read options do not match reader format".to_string(),
            )),
        }
    }
}

impl FromStr for ReaderEnum {
    type Err = StorageError;

    fn from_str(format_str: &str) -> Result<Self> {
        let format = Format::from_str(format_str)?;
        Ok(Self::new(format))
    }
}

#[async_trait]
impl Reader for ReaderEnum {
    async fn read(&self, path: &Path) -> Result<RecordBatch> {
        match self {
            ReaderEnum::Parquet(r) => r.read(path).await,
            ReaderEnum::Lance(r) => r.read(path).await,
            ReaderEnum::Vortex(r) => r.read(path).await,
        }
    }
}

/// Type-safe writer dispatch over supported formats.
#[derive(Debug)]
pub enum WriterEnum {
    /// Parquet writer.
    Parquet(ParquetWriter),
    /// Lance writer.
    Lance(LanceWriter),
    /// Vortex writer.
    Vortex(VortexWriter),
}

impl WriterEnum {
    /// Creates a writer for `format`.
    pub fn new(format: Format) -> Self {
        match format {
            Format::Parquet => WriterEnum::Parquet(ParquetWriter::new()),
            Format::Lance => WriterEnum::Lance(LanceWriter::new()),
            Format::Vortex => WriterEnum::Vortex(VortexWriter::new()),
        }
    }

    /// Writes one batch with options matching the active format variant.
    pub async fn write_with_options(
        &self,
        batch: &RecordBatch,
        path: &Path,
        options: FormatWriteOptions,
    ) -> Result<()> {
        match (self, options) {
            (WriterEnum::Parquet(w), FormatWriteOptions::Parquet(options)) => {
                w.write_with_options(batch, path, options).await
            }
            (WriterEnum::Lance(w), FormatWriteOptions::Lance(options)) => {
                w.write_with_options(batch, path, options).await
            }
            (WriterEnum::Vortex(w), FormatWriteOptions::Vortex(options)) => {
                w.write_with_options(batch, path, options).await
            }
            _ => Err(StorageError::Unsupported(
                "write options do not match writer format".to_string(),
            )),
        }
    }

    /// Writes a stream with options matching the active format variant.
    pub async fn write_stream(
        &self,
        stream: RecordBatchStream,
        path: &Path,
        options: FormatWriteOptions,
    ) -> Result<()> {
        match (self, options) {
            (WriterEnum::Parquet(w), FormatWriteOptions::Parquet(options)) => {
                w.write_stream(stream, path, options).await
            }
            (WriterEnum::Lance(w), FormatWriteOptions::Lance(options)) => {
                w.write_stream(stream, path, options).await
            }
            (WriterEnum::Vortex(w), FormatWriteOptions::Vortex(options)) => {
                w.write_stream(stream, path, options).await
            }
            _ => Err(StorageError::Unsupported(
                "write options do not match writer format".to_string(),
            )),
        }
    }
}

impl FromStr for WriterEnum {
    type Err = StorageError;

    fn from_str(format_str: &str) -> Result<Self> {
        let format = Format::from_str(format_str)?;
        Ok(Self::new(format))
    }
}

#[async_trait]
impl Writer for WriterEnum {
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        match self {
            WriterEnum::Parquet(w) => w.write(batch, path).await,
            WriterEnum::Lance(w) => w.write(batch, path).await,
            WriterEnum::Vortex(w) => w.write(batch, path).await,
        }
    }
}
