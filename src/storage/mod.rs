pub mod error;
pub mod file_format;

use std::path::Path;
use arrow_array::RecordBatch;

pub use error::{Result, StorageError};

use file_format::lance::{LanceReader, LanceWriter};
use file_format::parquet::{ParquetReader, ParquetWriter};
use file_format::vortex::{VortexReader, VortexWriter};

/// Trait for reading file formats into Arrow RecordBatches
pub trait Reader: Send + Sync {
    fn read(&self, path: &Path) -> Result<RecordBatch>;
}

/// Trait for writing Arrow RecordBatches to file formats
pub trait Writer: Send + Sync {
    fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()>;
}

/// File format enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    Parquet,
    Lance,
    Vortex,
}

impl Format {
    pub fn from_str(s: &str) -> Result<Self> {
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
    Parquet(ParquetReader),
    Lance(LanceReader),
    Vortex(VortexReader),
}

impl ReaderEnum {
    pub fn new(format: Format) -> Self {
        match format {
            Format::Parquet => ReaderEnum::Parquet(ParquetReader::new()),
            Format::Lance => ReaderEnum::Lance(LanceReader::new()),
            Format::Vortex => ReaderEnum::Vortex(VortexReader::new()),
        }
    }
    
    pub fn from_str(format_str: &str) -> Result<Self> {
        let format = Format::from_str(format_str)?;
        Ok(Self::new(format))
    }
}

impl Reader for ReaderEnum {
    fn read(&self, path: &Path) -> Result<RecordBatch> {
        match self {
            ReaderEnum::Parquet(r) => r.read(path),
            ReaderEnum::Lance(r) => r.read(path),
            ReaderEnum::Vortex(r) => r.read(path),
        }
    }
}

/// Writer enum for type-safe dispatch
#[derive(Debug)]
pub enum WriterEnum {
    Parquet(ParquetWriter),
    Lance(LanceWriter),
    Vortex(VortexWriter),
}

impl WriterEnum {
    pub fn new(format: Format) -> Self {
        match format {
            Format::Parquet => WriterEnum::Parquet(ParquetWriter::new()),
            Format::Lance => WriterEnum::Lance(LanceWriter::new()),
            Format::Vortex => WriterEnum::Vortex(VortexWriter::new()),
        }
    }
    
    pub fn from_str(format_str: &str) -> Result<Self> {
        let format = Format::from_str(format_str)?;
        Ok(Self::new(format))
    }
}

impl Writer for WriterEnum {
    fn write(&self, batch: &RecordBatch, path: &Path) -> Result<()> {
        match self {
            WriterEnum::Parquet(w) => w.write(batch, path),
            WriterEnum::Lance(w) => w.write(batch, path),
            WriterEnum::Vortex(w) => w.write(batch, path),
        }
    }
}
