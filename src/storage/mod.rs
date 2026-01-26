//! Storage module for file format operations

/// Error types for storage operations
pub mod error;
/// File format implementations
pub mod file_format;

use std::path::Path;
use std::str::FromStr;
use arrow_array::RecordBatch;
use async_trait::async_trait;

pub use error::{Result, StorageError};

use file_format::lance::{LanceReader, LanceWriter};
use file_format::parquet::{ParquetReader, ParquetWriter};
use file_format::vortex::{VortexReader, VortexWriter};

/// Trait for reading file formats into Arrow RecordBatches
#[async_trait]
pub trait Reader: Send + Sync {
    /// Read a file and return an Arrow RecordBatch
    async fn read(&self, path: &Path) -> Result<RecordBatch>;
}

/// Trait for writing Arrow RecordBatches to file formats
#[async_trait]
pub trait Writer: Send + Sync {
    /// Write a RecordBatch to a file
    async fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()>;
}

/// Supported file formats
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
    /// Get format as string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Format::Parquet => "parquet",
            Format::Lance => "lance",
            Format::Vortex => "vortex",
        }
    }
}

/// Reader enum for type-safe dispatch
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
    /// Create a reader for the given format
    pub fn new(format: Format) -> Self {
        match format {
            Format::Parquet => ReaderEnum::Parquet(ParquetReader::new()),
            Format::Lance => ReaderEnum::Lance(LanceReader::new()),
            Format::Vortex => ReaderEnum::Vortex(VortexReader::new()),
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

/// Writer enum for type-safe dispatch
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
    /// Create a writer for the given format
    pub fn new(format: Format) -> Self {
        match format {
            Format::Parquet => WriterEnum::Parquet(ParquetWriter::new()),
            Format::Lance => WriterEnum::Lance(LanceWriter::new()),
            Format::Vortex => WriterEnum::Vortex(VortexWriter::new()),
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
