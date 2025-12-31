use std::path::Path;
use arrow_array::RecordBatch;
use crate::storage::{Reader, Writer, Result, StorageError};

/// Parquet reader implementation
/// 
/// Struct-based implementation of the Reader trait.
/// This is a concrete type that can be used directly or through the trait.
#[derive(Debug, Default)]
pub struct ParquetReader;

impl ParquetReader {
    pub fn new() -> Self {
        Self
    }
}

impl Reader for ParquetReader {
    fn read(&self, _path: &Path) -> Result<RecordBatch> {
        Err(StorageError::NotYetImplemented("Parquet reading".to_string()))
    }
}

/// Parquet writer implementation
#[derive(Debug, Default)]
pub struct ParquetWriter;

impl ParquetWriter {
    pub fn new() -> Self {
        Self
    }
}

impl Writer for ParquetWriter {
    fn write(&self, _batch: &RecordBatch, _path: &Path) -> Result<()> {
        Err(StorageError::NotYetImplemented("Parquet writing".to_string()))
    }
}
