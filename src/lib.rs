//! Transaction-based open table format

/// Catalog module for managing table metadata
pub mod catalog;

/// Storage module for file format operations
pub mod storage;

mod python;

use pyo3::prelude::*;

#[pymodule]
fn _native(py: Python, module: &PyModule) -> PyResult<()> {
    python::init_module(py, module)
}
