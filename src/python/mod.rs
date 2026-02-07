use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use futures::{stream, TryStreamExt};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyType};
use pyo3::IntoPyObjectExt;
use once_cell::sync::Lazy;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::future::Future;
use tokio::runtime::{Builder, Runtime};

use crate::catalog::{
    Catalog, CatalogError as CoreCatalogError, ColumnSpec, CommitResult, FileSpec, SchemaSpec,
    TableDelta, TableHandle, TableIdent, TableView,
};
use crate::storage::{
    file_format::lance::LanceReadOptions,
    file_format::lance::parse_write_options as parse_lance_write_options,
    file_format::parquet::{ParquetReadOptions, ParquetReader, ParquetWriter},
    file_format::parquet::parse_write_options as parse_parquet_write_options,
    file_format::vortex::{VortexReadOptions, VortexReader, VortexWriter},
    file_format::vortex::parse_write_options as parse_vortex_write_options,
    RecordBatchStream,
};
use crate::storage::StorageError as CoreStorageError;

create_exception!(planar, PlanarError, PyException);
create_exception!(planar, CatalogError, PlanarError);
create_exception!(planar, StorageError, PlanarError);

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for Python API")
});

fn block_on_py<T, F>(py: Python, fut: F) -> PyResult<T>
where
    T: Send,
    F: Future<Output = PyResult<T>> + Send,
{
    py.detach(|| RUNTIME.block_on(fut))
}

fn catalog_error_to_py(err: CoreCatalogError) -> PyErr {
    PyErr::new::<CatalogError, _>(err.to_string())
}

fn storage_error_to_py(err: crate::storage::StorageError) -> PyErr {
    PyErr::new::<StorageError, _>(err.to_string())
}

fn py_to_json_value(value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if value.is_none() {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(val) = value.extract::<bool>() {
        return Ok(serde_json::Value::Bool(val));
    }
    if let Ok(val) = value.extract::<i64>() {
        return Ok(serde_json::Value::Number(val.into()));
    }
    if let Ok(val) = value.extract::<f64>() {
        let number = serde_json::Number::from_f64(val).ok_or_else(|| {
            PyErr::new::<PyValueError, _>("Invalid floating point value")
        })?;
        return Ok(serde_json::Value::Number(number));
    }
    if let Ok(val) = value.extract::<String>() {
        return Ok(serde_json::Value::String(val));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for item in list.iter() {
            items.push(py_to_json_value(&item)?);
        }
        return Ok(serde_json::Value::Array(items));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, val) in dict.iter() {
            let key_str = key.extract::<String>()?;
            map.insert(key_str, py_to_json_value(&val)?);
        }
        return Ok(serde_json::Value::Object(map));
    }

    Err(PyErr::new::<PyValueError, _>(
        "Unsupported JSON value type",
    ))
}

fn json_value_to_py(py: Python, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(val) => val.into_py_any(py),
        serde_json::Value::Number(val) => {
            if let Some(num) = val.as_i64() {
                num.into_py_any(py)
            } else if let Some(num) = val.as_f64() {
                num.into_py_any(py)
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(val) => val.into_py_any(py),
        serde_json::Value::Array(values) => {
            let list = PyList::empty(py);
            for item in values {
                list.append(json_value_to_py(py, item)?)?;
            }
            list.into_py_any(py)
        }
        serde_json::Value::Object(values) => {
            let dict = PyDict::new(py);
            for (key, val) in values {
                dict.set_item(key, json_value_to_py(py, val)?)?;
            }
            dict.into_py_any(py)
        }
    }
}

fn record_batches_to_ipc(batches: Vec<RecordBatch>) -> std::result::Result<Vec<u8>, CoreStorageError> {
    let Some(first) = batches.first() else {
        return Err(CoreStorageError::Unsupported(
            "expected at least one RecordBatch".to_string(),
        ));
    };

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, first.schema().as_ref())?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(buffer)
}

fn ipc_to_record_batches(data: &[u8]) -> std::result::Result<Vec<RecordBatch>, CoreStorageError> {
    let mut reader = StreamReader::try_new(Cursor::new(data), None)?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }
    Ok(batches)
}

fn time_unit_to_str(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

fn time_unit_from_str(value: &str) -> PyResult<TimeUnit> {
    match value {
        "s" => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        _ => Err(PyErr::new::<PyValueError, _>(format!(
            "Unsupported time unit: {}",
            value
        ))),
    }
}

fn field_to_spec(py: Python, field: &Field) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", field.name())?;
    dict.set_item("nullable", field.is_nullable())?;
    dict.set_item("dtype", data_type_to_spec(py, field.data_type())?)?;
    dict.into_py_any(py)
}

fn field_from_spec(py: Python, spec: &Bound<'_, PyAny>) -> PyResult<Field> {
    let dict = spec.downcast::<PyDict>()?;
    let name: String = dict
        .get_item("name")?
        .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing field name"))?
        .extract()?;
    let nullable: bool = dict
        .get_item("nullable")?
        .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing field nullable"))?
        .extract()?;
    let dtype_spec = dict
        .get_item("dtype")?
        .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing field dtype"))?;
    let data_type = data_type_from_spec(py, &dtype_spec)?;
    Ok(Field::new(name, data_type, nullable))
}

fn data_type_to_spec(py: Python, data_type: &DataType) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    match data_type {
        DataType::Null => {
            dict.set_item("kind", "null")?;
        }
        DataType::Boolean => {
            dict.set_item("kind", "bool")?;
        }
        DataType::Int8 => dict.set_item("kind", "int8")?,
        DataType::Int16 => dict.set_item("kind", "int16")?,
        DataType::Int32 => dict.set_item("kind", "int32")?,
        DataType::Int64 => dict.set_item("kind", "int64")?,
        DataType::UInt8 => dict.set_item("kind", "uint8")?,
        DataType::UInt16 => dict.set_item("kind", "uint16")?,
        DataType::UInt32 => dict.set_item("kind", "uint32")?,
        DataType::UInt64 => dict.set_item("kind", "uint64")?,
        DataType::Float32 => dict.set_item("kind", "float32")?,
        DataType::Float64 => dict.set_item("kind", "float64")?,
        DataType::Utf8 => dict.set_item("kind", "string")?,
        DataType::LargeUtf8 => dict.set_item("kind", "large_string")?,
        DataType::Binary => dict.set_item("kind", "binary")?,
        DataType::LargeBinary => dict.set_item("kind", "large_binary")?,
        DataType::FixedSizeBinary(size) => {
            dict.set_item("kind", "fixed_size_binary")?;
            dict.set_item("byte_width", *size)?;
        }
        DataType::Date32 => dict.set_item("kind", "date32")?,
        DataType::Date64 => dict.set_item("kind", "date64")?,
        DataType::Time32(unit) => {
            dict.set_item("kind", "time32")?;
            dict.set_item("unit", time_unit_to_str(unit))?;
        }
        DataType::Time64(unit) => {
            dict.set_item("kind", "time64")?;
            dict.set_item("unit", time_unit_to_str(unit))?;
        }
        DataType::Duration(unit) => {
            dict.set_item("kind", "duration")?;
            dict.set_item("unit", time_unit_to_str(unit))?;
        }
        DataType::Timestamp(unit, tz) => {
            dict.set_item("kind", "timestamp")?;
            dict.set_item("unit", time_unit_to_str(unit))?;
            match tz {
                Some(value) => dict.set_item("tz", value.as_ref())?,
                None => dict.set_item("tz", py.None())?,
            };
        }
        DataType::Decimal128(precision, scale) => {
            dict.set_item("kind", "decimal128")?;
            dict.set_item("precision", *precision)?;
            dict.set_item("scale", *scale)?;
        }
        DataType::Decimal256(precision, scale) => {
            dict.set_item("kind", "decimal256")?;
            dict.set_item("precision", *precision)?;
            dict.set_item("scale", *scale)?;
        }
        DataType::List(field) => {
            dict.set_item("kind", "list")?;
            dict.set_item("field", field_to_spec(py, field)?)?;
        }
        DataType::LargeList(field) => {
            dict.set_item("kind", "large_list")?;
            dict.set_item("field", field_to_spec(py, field)?)?;
        }
        DataType::FixedSizeList(field, size) => {
            dict.set_item("kind", "fixed_size_list")?;
            dict.set_item("field", field_to_spec(py, field)?)?;
            dict.set_item("size", *size)?;
        }
        DataType::Struct(fields) => {
            dict.set_item("kind", "struct")?;
            let list = PyList::empty(py);
            for field in fields {
                list.append(field_to_spec(py, field)?)?;
            }
            dict.set_item("fields", list)?;
        }
        other => {
            return Err(PyErr::new::<PyValueError, _>(format!(
                "Unsupported DataType for Python bindings: {:?}",
                other
            )))
        }
    }
    dict.into_py_any(py)
}

fn data_type_from_spec(py: Python, spec: &Bound<'_, PyAny>) -> PyResult<DataType> {
    let dict = spec.downcast::<PyDict>()?;
    let kind: String = dict
        .get_item("kind")?
        .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing dtype kind"))?
        .extract()?;

    match kind.as_str() {
        "null" => Ok(DataType::Null),
        "bool" => Ok(DataType::Boolean),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" => Ok(DataType::Float32),
        "float64" => Ok(DataType::Float64),
        "string" => Ok(DataType::Utf8),
        "large_string" => Ok(DataType::LargeUtf8),
        "binary" => Ok(DataType::Binary),
        "large_binary" => Ok(DataType::LargeBinary),
        "fixed_size_binary" => {
            let size: i32 = dict
                .get_item("byte_width")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing byte_width"))?
                .extract()?;
            Ok(DataType::FixedSizeBinary(size))
        }
        "date32" => Ok(DataType::Date32),
        "date64" => Ok(DataType::Date64),
        "time32" => {
            let unit: String = dict
                .get_item("unit")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing time unit"))?
                .extract()?;
            Ok(DataType::Time32(time_unit_from_str(&unit)?))
        }
        "time64" => {
            let unit: String = dict
                .get_item("unit")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing time unit"))?
                .extract()?;
            Ok(DataType::Time64(time_unit_from_str(&unit)?))
        }
        "duration" => {
            let unit: String = dict
                .get_item("unit")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing duration unit"))?
                .extract()?;
            Ok(DataType::Duration(time_unit_from_str(&unit)?))
        }
        "timestamp" => {
            let unit: String = dict
                .get_item("unit")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing timestamp unit"))?
                .extract()?;
            let tz: Option<String> = dict
                .get_item("tz")?
                .map(|value| value.extract::<Option<String>>())
                .transpose()?
                .flatten();
            Ok(DataType::Timestamp(
                time_unit_from_str(&unit)?,
                tz.map(|v| v.into()),
            ))
        }
        "decimal128" => {
            let precision: u8 = dict
                .get_item("precision")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing precision"))?
                .extract()?;
            let scale: i8 = dict
                .get_item("scale")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing scale"))?
                .extract()?;
            Ok(DataType::Decimal128(precision, scale))
        }
        "decimal256" => {
            let precision: u8 = dict
                .get_item("precision")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing precision"))?
                .extract()?;
            let scale: i8 = dict
                .get_item("scale")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing scale"))?
                .extract()?;
            Ok(DataType::Decimal256(precision, scale))
        }
        "list" => {
            let field_spec = dict
                .get_item("field")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing list field"))?;
            Ok(DataType::List(Arc::new(field_from_spec(py, &field_spec)?)))
        }
        "large_list" => {
            let field_spec = dict
                .get_item("field")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing list field"))?;
            Ok(DataType::LargeList(Arc::new(field_from_spec(py, &field_spec)?)))
        }
        "fixed_size_list" => {
            let field_spec = dict
                .get_item("field")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing list field"))?;
            let size: i32 = dict
                .get_item("size")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing list size"))?
                .extract()?;
            Ok(DataType::FixedSizeList(
                Arc::new(field_from_spec(py, &field_spec)?),
                size,
            ))
        }
        "struct" => {
            let fields_value = dict
                .get_item("fields")?
                .ok_or_else(|| PyErr::new::<PyValueError, _>("Missing struct fields"))?;
            let fields = fields_value.downcast::<PyList>()?;
            let mut converted = Vec::with_capacity(fields.len());
            for field in fields.iter() {
                converted.push(field_from_spec(py, &field)?);
            }
            Ok(DataType::Struct(converted.into()))
        }
        _ => Err(PyErr::new::<PyValueError, _>(format!(
            "Unsupported dtype kind: {}",
            kind
        ))),
    }
}

#[pyclass(name = "TableIdent")]
#[derive(Clone)]
pub struct PyTableIdent {
    namespace: String,
    name: String,
}

#[pymethods]
impl PyTableIdent {
    #[new]
    fn new(namespace: String, name: String) -> Self {
        Self { namespace, name }
    }

    #[getter]
    fn namespace(&self) -> &str {
        &self.namespace
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }
}

impl PyTableIdent {
    fn to_core(&self) -> TableIdent {
        TableIdent::new(self.namespace.clone(), self.name.clone())
    }
}

#[pyclass(name = "ColumnSpec")]
#[derive(Clone)]
pub struct PyColumnSpec {
    inner: ColumnSpec,
}

#[pymethods]
impl PyColumnSpec {
    #[new]
    fn new(py: Python, name: String, dtype_spec: &Bound<'_, PyAny>) -> PyResult<Self> {
        let data_type = data_type_from_spec(py, dtype_spec)?;
        Ok(Self {
            inner: ColumnSpec::new(name, data_type),
        })
    }

    fn nullable(&mut self) -> PyResult<()> {
        self.inner = self.inner.clone().nullable();
        Ok(())
    }
}

#[pyclass(name = "SchemaSpec")]
#[derive(Clone, Default)]
pub struct PySchemaSpec {
    columns: Vec<ColumnSpec>,
}

#[pymethods]
impl PySchemaSpec {
    #[new]
    fn new() -> Self {
        Self { columns: Vec::new() }
    }

    fn with_column(&mut self, column: PyRef<PyColumnSpec>) -> PyResult<()> {
        self.columns.push(column.inner.clone());
        Ok(())
    }
}

impl PySchemaSpec {
    fn to_core(&self) -> SchemaSpec {
        SchemaSpec {
            columns: self.columns.clone(),
        }
    }
}

#[pyclass(name = "FileSpec")]
#[derive(Clone)]
pub struct PyFileSpec {
    inner: FileSpec,
}

#[pymethods]
impl PyFileSpec {
    #[new]
    fn new(
        file_format: String,
        file_path: String,
        record_count: i64,
        file_size_bytes: i64,
        format_options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let mut inner = FileSpec::new(file_format, file_path, record_count, file_size_bytes);
        if let Some(options) = format_options {
            if !options.is_none() {
                let json = py_to_json_value(options)?;
                inner = inner
                    .with_format_options_checked(json)
                    .map_err(catalog_error_to_py)?;
            }
        }
        Ok(Self { inner })
    }

    fn with_partition_values(&mut self, values: &Bound<'_, PyAny>) -> PyResult<()> {
        let json_values = py_to_json_value(values)?;
        self.inner = self.inner.clone().with_partition_values(json_values);
        Ok(())
    }
}

#[pyclass(name = "Catalog")]
#[derive(Clone)]
pub struct PyCatalog {
    inner: Arc<dyn Catalog>,
}

#[pymethods]
impl PyCatalog {
    #[classmethod]
    fn in_memory(_cls: &Bound<'_, PyType>, py: Python) -> PyResult<Self> {
        block_on_py(py, async move {
            let catalog = crate::catalog::SqlCatalog::<sqlx::Sqlite>::in_memory()
                .await
                .map_err(|err| catalog_error_to_py(err.into()))?;
            let catalog: Arc<dyn Catalog> = catalog;
            Ok(PyCatalog { inner: catalog })
        })
    }

    #[classmethod]
    fn from_connection_string(
        _cls: &Bound<'_, PyType>,
        py: Python,
        connection_string: String,
    ) -> PyResult<Self> {
        block_on_py(py, async move {
            let is_postgres = connection_string.starts_with("postgres://")
                || connection_string.starts_with("postgresql://");

            let catalog: Arc<dyn Catalog> = if is_postgres {
                crate::catalog::SqlCatalog::<sqlx::Postgres>::from_connection_string(
                    &connection_string,
                )
                .await
                .map_err(|err| catalog_error_to_py(err.into()))?
            } else {
                crate::catalog::SqlCatalog::<sqlx::Sqlite>::from_connection_string(&connection_string)
                    .await
                    .map_err(|err| catalog_error_to_py(err.into()))?
            };

            Ok(PyCatalog { inner: catalog })
        })
    }

    fn create_table(
        &self,
        py: Python,
        ident: PyRef<PyTableIdent>,
        location: String,
        schema: PyRef<PySchemaSpec>,
        properties: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyTableHandle> {
        let catalog = self.inner.clone();
        let ident = ident.to_core();
        let schema = schema.to_core();
        let properties = match properties {
            Some(value) => Some(py_to_json_value(value)?),
            None => None,
        };

        block_on_py(py, async move {
            let handle = catalog
                .clone()
                .create_table(ident, location, schema, properties)
                .await
                .map_err(catalog_error_to_py)?;
            Ok(PyTableHandle { inner: handle })
        })
    }

    fn load_table(&self, py: Python, ident: PyRef<PyTableIdent>) -> PyResult<Option<PyTableHandle>> {
        let catalog = self.inner.clone();
        let ident = ident.to_core();
        block_on_py(py, async move {
            let handle = catalog
                .clone()
                .load_table(ident)
                .await
                .map_err(catalog_error_to_py)?;
            Ok(handle.map(|inner| PyTableHandle { inner }))
        })
    }

    fn list_tables(&self, py: Python, namespace: Option<String>) -> PyResult<Vec<PyTableIdent>> {
        let catalog = self.inner.clone();
        block_on_py(py, async move {
            let tables = catalog
                .list_tables(namespace.as_deref())
                .await
                .map_err(catalog_error_to_py)?;
            Ok(tables
                .into_iter()
                .map(|ident| PyTableIdent {
                    namespace: ident.namespace,
                    name: ident.name,
                })
                .collect::<Vec<_>>())
        })
    }

    fn drop_table(&self, py: Python, ident: PyRef<PyTableIdent>) -> PyResult<()> {
        let catalog = self.inner.clone();
        let ident = ident.to_core();
        block_on_py(py, async move {
            catalog.drop_table(&ident).await.map_err(catalog_error_to_py)?;
            Ok(())
        })
    }
}

#[pyclass(name = "TableHandle")]
#[derive(Clone)]
pub struct PyTableHandle {
    inner: TableHandle,
}

#[pymethods]
impl PyTableHandle {
    fn read(&self, py: Python) -> PyResult<Py<PyAny>> {
        let handle = self.inner.clone();
        block_on_py(py, async move {
            let view = handle.read().await.map_err(catalog_error_to_py)?;
            Python::attach(|py| table_view_to_py(py, &view))
        })
    }

    fn read_at(&self, py: Python, transaction_id: String) -> PyResult<Py<PyAny>> {
        let handle = self.inner.clone();
        let txn_id = uuid::Uuid::parse_str(&transaction_id).map_err(|err| {
            PyErr::new::<PyValueError, _>(format!("Invalid transaction id: {}", err))
        })?;
        block_on_py(py, async move {
            let view = handle.read_at(txn_id).await.map_err(catalog_error_to_py)?;
            Python::attach(|py| table_view_to_py(py, &view))
        })
    }

    fn diff(
        &self,
        py: Python,
        from_transaction_id: String,
        to_transaction_id: String,
    ) -> PyResult<Py<PyAny>> {
        let handle = self.inner.clone();
        let from_txn = uuid::Uuid::parse_str(&from_transaction_id).map_err(|err| {
            PyErr::new::<PyValueError, _>(format!("Invalid transaction id: {}", err))
        })?;
        let to_txn = uuid::Uuid::parse_str(&to_transaction_id).map_err(|err| {
            PyErr::new::<PyValueError, _>(format!("Invalid transaction id: {}", err))
        })?;

        block_on_py(py, async move {
            let delta = handle
                .diff(from_txn, to_txn)
                .await
                .map_err(catalog_error_to_py)?;
            Python::attach(|py| table_delta_to_py(py, &delta))
        })
    }

    fn append_file(&self, py: Python, file: PyRef<PyFileSpec>) -> PyResult<Py<PyAny>> {
        let handle = self.inner.clone();
        let file = file.inner.clone();
        block_on_py(py, async move {
            let result = handle.append_file(file).await.map_err(catalog_error_to_py)?;
            Python::attach(|py| commit_result_to_py(py, &result))
        })
    }

    fn append_files(&self, py: Python, files: Vec<Py<PyFileSpec>>) -> PyResult<Py<PyAny>> {
        let handle = self.inner.clone();
        let mut core_files = Vec::with_capacity(files.len());
        Python::attach(|py| {
            for file in files {
                let file_ref = file.borrow(py);
                core_files.push(file_ref.inner.clone());
            }
            Ok::<(), PyErr>(())
        })?;

        block_on_py(py, async move {
            let result = handle
                .append_files(core_files)
                .await
                .map_err(catalog_error_to_py)?;
            Python::attach(|py| commit_result_to_py(py, &result))
        })
    }

    fn delete_files(&self, py: Python, file_uuids: Vec<String>) -> PyResult<Py<PyAny>> {
        let handle = self.inner.clone();
        let mut uuids = Vec::with_capacity(file_uuids.len());
        for uuid_str in file_uuids {
            uuids.push(uuid::Uuid::parse_str(&uuid_str).map_err(|err| {
                PyErr::new::<PyValueError, _>(format!("Invalid file uuid: {}", err))
            })?);
        }

        block_on_py(py, async move {
            let result = handle
                .delete_files(uuids)
                .await
                .map_err(catalog_error_to_py)?;
            Python::attach(|py| commit_result_to_py(py, &result))
        })
    }

    fn set_properties(&self, py: Python, properties: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let handle = self.inner.clone();
        let json = py_to_json_value(properties)?;
        block_on_py(py, async move {
            let result = handle
                .set_properties(json)
                .await
                .map_err(catalog_error_to_py)?;
            Python::attach(|py| commit_result_to_py(py, &result))
        })
    }
}

fn file_to_py(py: Python, file: &crate::catalog::schema::File) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("file_uuid", file.file_uuid.to_string())?;
    dict.set_item("table_uuid", file.table_uuid.to_string())?;
    dict.set_item("file_format", file.file_format.as_str())?;
    dict.set_item("file_path", file.file_path.as_str())?;
    dict.set_item("record_count", file.record_count)?;
    dict.set_item("file_size_bytes", file.file_size_bytes)?;
    dict.set_item("added_in_transaction_id", file.added_in_transaction_id.to_string())?;
    dict.set_item(
        "removed_in_transaction_id",
        file.removed_in_transaction_id.map(|id| id.to_string()),
    )?;
    match &file.partition_values {
        Some(values) => dict.set_item("partition_values", json_value_to_py(py, values)?)?,
        None => dict.set_item("partition_values", py.None())?,
    };
    match &file.format_options {
        Some(values) => dict.set_item("format_options", json_value_to_py(py, values)?)?,
        None => dict.set_item("format_options", py.None())?,
    };
    dict.into_py_any(py)
}

fn schema_to_py(py: Python, schema: &crate::catalog::schema::Schema) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("schema_uuid", schema.schema_uuid.to_string())?;
    dict.set_item("table_uuid", schema.table_uuid.to_string())?;
    dict.set_item("schema_version", schema.schema_version)?;
    dict.set_item(
        "valid_from_transaction_id",
        schema.valid_from_transaction_id.to_string(),
    )?;
    dict.set_item(
        "valid_to_transaction_id",
        schema.valid_to_transaction_id.map(|id| id.to_string()),
    )?;
    dict.set_item("created_at", schema.created_at.to_rfc3339())?;

    let columns = PyList::empty(py);
    for column in &schema.columns {
        let column_dict = PyDict::new(py);
        column_dict.set_item("column_uuid", column.column_uuid.to_string())?;
        column_dict.set_item("schema_uuid", column.schema_uuid.to_string())?;
        column_dict.set_item("column_name", column.column_name.as_str())?;
        column_dict.set_item("column_type", data_type_to_spec(py, &column.column_type)?)?;
        column_dict.set_item("ordinal_position", column.ordinal_position)?;
        column_dict.set_item("is_nullable", column.is_nullable)?;
        columns.append(column_dict)?;
    }
    dict.set_item("columns", columns)?;
    dict.into_py_any(py)
}

fn table_stats_to_py(
    py: Python,
    stats: &crate::catalog::schema::TableStats,
) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("table_uuid", stats.table_uuid.to_string())?;
    dict.set_item("transaction_id", stats.transaction_id.to_string())?;
    dict.set_item("record_count", stats.record_count)?;
    dict.set_item("file_size_bytes", stats.file_size_bytes)?;
    dict.set_item("file_count", stats.file_count)?;
    dict.set_item("last_updated", stats.last_updated.to_rfc3339())?;
    dict.into_py_any(py)
}

fn table_view_to_py(py: Python, view: &TableView) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    let ident = PyDict::new(py);
    ident.set_item("namespace", view.ident.namespace.as_str())?;
    ident.set_item("name", view.ident.name.as_str())?;
    dict.set_item("ident", ident)?;
    dict.set_item("table_uuid", view.table_uuid.to_string())?;
    dict.set_item("transaction_id", view.transaction_id.to_string())?;
    dict.set_item("schema", schema_to_py(py, &view.schema)?)?;

    let files = PyList::empty(py);
    for file in &view.files {
        files.append(file_to_py(py, file)?)?;
    }
    dict.set_item("files", files)?;

    dict.set_item("properties", json_value_to_py(py, &view.properties)?)?;
    match &view.stats {
        Some(stats) => dict.set_item("stats", table_stats_to_py(py, stats)?)?,
        None => dict.set_item("stats", py.None())?,
    };

    dict.into_py_any(py)
}

fn table_delta_to_py(py: Python, delta: &TableDelta) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("from_transaction_id", delta.from_transaction_id.to_string())?;
    dict.set_item("to_transaction_id", delta.to_transaction_id.to_string())?;

    let added_files = PyList::empty(py);
    for file in &delta.added_files {
        added_files.append(file_to_py(py, file)?)?;
    }
    dict.set_item("added_files", added_files)?;

    let removed_files = PyList::empty(py);
    for file in &delta.removed_files {
        removed_files.append(file_to_py(py, file)?)?;
    }
    dict.set_item("removed_files", removed_files)?;

    match &delta.new_schema {
        Some(schema) => dict.set_item("new_schema", schema_to_py(py, schema)?)?,
        None => dict.set_item("new_schema", py.None())?,
    };

    match &delta.new_properties {
        Some(props) => dict.set_item("new_properties", json_value_to_py(py, props)?)?,
        None => dict.set_item("new_properties", py.None())?,
    };

    dict.into_py_any(py)
}

fn commit_result_to_py(py: Python, result: &CommitResult) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("transaction_id", result.transaction_id.to_string())?;
    match &result.table_view {
        Some(view) => dict.set_item("table_view", table_view_to_py(py, view)?)?,
        None => dict.set_item("table_view", py.None())?,
    };
    dict.into_py_any(py)
}

fn batches_to_stream(batches: Vec<RecordBatch>) -> RecordBatchStream {
    Box::pin(stream::iter(batches.into_iter().map(Ok)))
}

#[pyfunction]
fn read_parquet_ipc(
    py: Python,
    path: String,
    batch_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    block_on_py(py, async move {
        let reader = ParquetReader::new();
        let options = ParquetReadOptions {
            batch_size,
            ..ParquetReadOptions::default()
        };
        let batch = reader
            .read_with_options(Path::new(&path), &options)
            .await
            .map_err(storage_error_to_py)?;
        let bytes = record_batches_to_ipc(vec![batch]).map_err(storage_error_to_py)?;
        Python::attach(|py| PyBytes::new(py, &bytes).into_py_any(py))
    })
}

#[pyfunction]
fn read_parquet_stream_ipc(
    py: Python,
    path: String,
    batch_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    block_on_py(py, async move {
        let reader = ParquetReader::new();
        let options = ParquetReadOptions {
            batch_size,
            ..ParquetReadOptions::default()
        };
        let stream = reader
            .read_stream(Path::new(&path), &options)
            .await
            .map_err(storage_error_to_py)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(storage_error_to_py)?;
        let bytes = record_batches_to_ipc(batches).map_err(storage_error_to_py)?;
        Python::attach(|py| PyBytes::new(py, &bytes).into_py_any(py))
    })
}

#[pyfunction]
fn write_parquet_ipc(
    py: Python,
    path: String,
    data: &Bound<'_, PyBytes>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let data = data.as_bytes().to_vec();
    let options_value = match options {
        Some(value) if !value.is_none() => Some(py_to_json_value(value)?),
        _ => None,
    };
    block_on_py(py, async move {
        let batches = ipc_to_record_batches(&data).map_err(storage_error_to_py)?;
        let Some(first) = batches.first() else {
            return Err(storage_error_to_py(CoreStorageError::Unsupported(
                "write_parquet requires at least one RecordBatch".to_string(),
            )));
        };
        let writer = ParquetWriter::new();
        let props = parse_parquet_write_options(options_value.as_ref()).map_err(storage_error_to_py)?;
        writer
            .write_with_options(first, Path::new(&path), &props)
            .await
            .map_err(storage_error_to_py)?;
        Ok(())
    })
}

#[pyfunction]
fn write_parquet_stream_ipc(
    py: Python,
    path: String,
    data: &Bound<'_, PyBytes>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let data = data.as_bytes().to_vec();
    let options_value = match options {
        Some(value) if !value.is_none() => Some(py_to_json_value(value)?),
        _ => None,
    };
    block_on_py(py, async move {
        let batches = ipc_to_record_batches(&data).map_err(storage_error_to_py)?;
        if batches.is_empty() {
            return Err(storage_error_to_py(CoreStorageError::Unsupported(
                "write_parquet_stream requires at least one RecordBatch".to_string(),
            )));
        }
        let stream = batches_to_stream(batches);
        let writer = ParquetWriter::new();
        let props = parse_parquet_write_options(options_value.as_ref()).map_err(storage_error_to_py)?;
        writer
            .write_stream(stream, Path::new(&path), &props)
            .await
            .map_err(storage_error_to_py)?;
        Ok(())
    })
}

#[pyfunction]
fn read_lance_ipc(
    py: Python,
    path: String,
    batch_size: Option<usize>,
    columns: Option<Vec<String>>,
    filter: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    scan_in_order: Option<bool>,
    io_buffer_size: Option<u64>,
) -> PyResult<Py<PyAny>> {
    block_on_py(py, async move {
        let reader = crate::storage::file_format::lance::LanceReader::new();
        let options = LanceReadOptions {
            batch_size,
            columns,
            filter,
            limit,
            offset,
            scan_in_order,
            io_buffer_size,
        };
        let batch = reader
            .read_with_options(Path::new(&path), &options)
            .await
            .map_err(storage_error_to_py)?;
        let bytes = record_batches_to_ipc(vec![batch]).map_err(storage_error_to_py)?;
        Python::attach(|py| PyBytes::new(py, &bytes).into_py_any(py))
    })
}

#[pyfunction]
fn read_lance_stream_ipc(
    py: Python,
    path: String,
    batch_size: Option<usize>,
    columns: Option<Vec<String>>,
    filter: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    scan_in_order: Option<bool>,
    io_buffer_size: Option<u64>,
) -> PyResult<Py<PyAny>> {
    block_on_py(py, async move {
        let reader = crate::storage::file_format::lance::LanceReader::new();
        let options = LanceReadOptions {
            batch_size,
            columns,
            filter,
            limit,
            offset,
            scan_in_order,
            io_buffer_size,
        };
        let stream = reader
            .read_stream(Path::new(&path), &options)
            .await
            .map_err(storage_error_to_py)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(storage_error_to_py)?;
        let bytes = record_batches_to_ipc(batches).map_err(storage_error_to_py)?;
        Python::attach(|py| PyBytes::new(py, &bytes).into_py_any(py))
    })
}

#[pyfunction]
fn write_lance_ipc(
    py: Python,
    path: String,
    data: &Bound<'_, PyBytes>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let data = data.as_bytes().to_vec();
    let options_value = match options {
        Some(value) if !value.is_none() => Some(py_to_json_value(value)?),
        _ => None,
    };
    block_on_py(py, async move {
        let batches = ipc_to_record_batches(&data).map_err(storage_error_to_py)?;
        let Some(first) = batches.first() else {
            return Err(storage_error_to_py(CoreStorageError::Unsupported(
                "write_lance requires at least one RecordBatch".to_string(),
            )));
        };
        let writer = crate::storage::file_format::lance::LanceWriter::new();
        let params = parse_lance_write_options(options_value.as_ref()).map_err(storage_error_to_py)?;
        writer
            .write_with_options(first, Path::new(&path), &params)
            .await
            .map_err(storage_error_to_py)?;
        Ok(())
    })
}

#[pyfunction]
fn write_lance_stream_ipc(
    py: Python,
    path: String,
    data: &Bound<'_, PyBytes>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let data = data.as_bytes().to_vec();
    let options_value = match options {
        Some(value) if !value.is_none() => Some(py_to_json_value(value)?),
        _ => None,
    };
    block_on_py(py, async move {
        let batches = ipc_to_record_batches(&data).map_err(storage_error_to_py)?;
        if batches.is_empty() {
            return Err(storage_error_to_py(CoreStorageError::Unsupported(
                "write_lance_stream requires at least one RecordBatch".to_string(),
            )));
        }
        let stream = batches_to_stream(batches);
        let writer = crate::storage::file_format::lance::LanceWriter::new();
        let params = parse_lance_write_options(options_value.as_ref()).map_err(storage_error_to_py)?;
        writer
            .write_stream(stream, Path::new(&path), &params)
            .await
            .map_err(storage_error_to_py)?;
        Ok(())
    })
}

#[pyfunction]
fn read_vortex_ipc(
    py: Python,
    path: String,
    initial_read_size: Option<usize>,
    segment_cache: Option<bool>,
) -> PyResult<Py<PyAny>> {
    block_on_py(py, async move {
        let reader = VortexReader::new();
        let options = VortexReadOptions {
            initial_read_size,
            segment_cache,
        };
        let batch = reader
            .read_with_options(Path::new(&path), &options)
            .await
            .map_err(storage_error_to_py)?;
        let bytes = record_batches_to_ipc(vec![batch]).map_err(storage_error_to_py)?;
        Python::attach(|py| PyBytes::new(py, &bytes).into_py_any(py))
    })
}

#[pyfunction]
fn read_vortex_stream_ipc(
    py: Python,
    path: String,
    initial_read_size: Option<usize>,
    segment_cache: Option<bool>,
) -> PyResult<Py<PyAny>> {
    block_on_py(py, async move {
        let reader = VortexReader::new();
        let options = VortexReadOptions {
            initial_read_size,
            segment_cache,
        };
        let stream = reader
            .read_stream(Path::new(&path), &options)
            .await
            .map_err(storage_error_to_py)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(storage_error_to_py)?;
        let bytes = record_batches_to_ipc(batches).map_err(storage_error_to_py)?;
        Python::attach(|py| PyBytes::new(py, &bytes).into_py_any(py))
    })
}

#[pyfunction]
fn write_vortex_ipc(
    py: Python,
    path: String,
    data: &Bound<'_, PyBytes>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let data = data.as_bytes().to_vec();
    let options_value = match options {
        Some(value) if !value.is_none() => Some(py_to_json_value(value)?),
        _ => None,
    };
    block_on_py(py, async move {
        let batches = ipc_to_record_batches(&data).map_err(storage_error_to_py)?;
        let Some(first) = batches.first() else {
            return Err(storage_error_to_py(CoreStorageError::Unsupported(
                "write_vortex requires at least one RecordBatch".to_string(),
            )));
        };
        let writer = VortexWriter::new();
        let opts = parse_vortex_write_options(options_value.as_ref()).map_err(storage_error_to_py)?;
        writer
            .write_with_options(first, Path::new(&path), &opts)
            .await
            .map_err(storage_error_to_py)?;
        Ok(())
    })
}

#[pyfunction]
fn write_vortex_stream_ipc(
    py: Python,
    path: String,
    data: &Bound<'_, PyBytes>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    let data = data.as_bytes().to_vec();
    let options_value = match options {
        Some(value) if !value.is_none() => Some(py_to_json_value(value)?),
        _ => None,
    };
    block_on_py(py, async move {
        let batches = ipc_to_record_batches(&data).map_err(storage_error_to_py)?;
        if batches.is_empty() {
            return Err(storage_error_to_py(CoreStorageError::Unsupported(
                "write_vortex_stream requires at least one RecordBatch".to_string(),
            )));
        }
        let stream = batches_to_stream(batches);
        let writer = VortexWriter::new();
        let opts = parse_vortex_write_options(options_value.as_ref()).map_err(storage_error_to_py)?;
        writer
            .write_stream(stream, Path::new(&path), &opts)
            .await
            .map_err(storage_error_to_py)?;
        Ok(())
    })
}

pub fn init_module(py: Python, module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = Lazy::force(&RUNTIME);
    module.add("PlanarError", py.get_type::<PlanarError>())?;
    module.add("CatalogError", py.get_type::<CatalogError>())?;
    module.add("StorageError", py.get_type::<StorageError>())?;
    module.add_class::<PyTableIdent>()?;
    module.add_class::<PyColumnSpec>()?;
    module.add_class::<PySchemaSpec>()?;
    module.add_class::<PyFileSpec>()?;
    module.add_class::<PyCatalog>()?;
    module.add_class::<PyTableHandle>()?;
    module.add_function(wrap_pyfunction!(read_parquet_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(read_parquet_stream_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(write_parquet_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(write_parquet_stream_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(read_lance_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(read_lance_stream_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(write_lance_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(write_lance_stream_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(read_vortex_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(read_vortex_stream_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(write_vortex_ipc, module)?)?;
    module.add_function(wrap_pyfunction!(write_vortex_stream_ipc, module)?)?;
    Ok(())
}
