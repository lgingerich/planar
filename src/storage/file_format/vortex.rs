use std::path::Path;
use arrow_array::RecordBatch;
use crate::storage::{Reader, Writer, Result, StorageError};

/// Vortex reader implementation
#[derive(Debug, Default)]
pub struct VortexReader;

impl VortexReader {
    /// Create a new Vortex reader
    pub fn new() -> Self {
        Self
    }
}

impl Reader for VortexReader {
    fn read(&self, _path: &Path) -> Result<RecordBatch> {
        Err(StorageError::NotYetImplemented("Vortex reading".to_string()))
    }
}

/// Vortex writer implementation
#[derive(Debug, Default)]
pub struct VortexWriter;

impl VortexWriter {
    /// Create a new Vortex writer
    pub fn new() -> Self {
        Self
    }
}

impl Writer for VortexWriter {
    fn write(&self, _batch: &RecordBatch, _path: &Path) -> Result<()> {
        Err(StorageError::NotYetImplemented("Vortex writing".to_string()))
    }
}
