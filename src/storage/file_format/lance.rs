use std::path::Path;
use arrow_array::RecordBatch;
use crate::storage::{Reader, Writer, Result, StorageError};

/// Lance reader implementation
#[derive(Debug, Default)]
pub struct LanceReader;

impl LanceReader {
    /// Create a new Lance reader
    pub fn new() -> Self {
        Self
    }
}

impl Reader for LanceReader {
    fn read(&self, _path: &Path) -> Result<RecordBatch> {
        Err(StorageError::NotYetImplemented("Lance reading".to_string()))
    }
}

/// Lance writer implementation
#[derive(Debug, Default)]
pub struct LanceWriter;

impl LanceWriter {
    /// Create a new Lance writer
    pub fn new() -> Self {
        Self
    }
}

impl Writer for LanceWriter {
    fn write(&self, _batch: &RecordBatch, _path: &Path) -> Result<()> {
        Err(StorageError::NotYetImplemented("Lance writing".to_string()))
    }
}
