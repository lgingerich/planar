# Planar Python API (Draft)

This document is the starting point for Planar's public Python API documentation.
It covers the current surface area and intended patterns for storage and catalog usage.

## Design Principles

- Keep the API concrete and explicit.
- Avoid JSON in core APIs except where persisted (catalog `format_options`).
- Prefer async methods for I/O and consistency across formats.
- Align read/write/stream behavior across formats.

## Installation

Install from source for now:

```bash
pip install -e .
```

## Core Types

### Catalog

The catalog is the entry point for table metadata and commits.

```python
from planar import Catalog, TableIdent, SchemaSpec, ColumnSpec
import pyarrow as pa

catalog = await Catalog.in_memory()
ident = TableIdent("default", "events")

schema = SchemaSpec().with_column(ColumnSpec("id", pa.int64()))
handle = await catalog.create_table(
    ident=ident,
    location="file:///tmp/planar/events",
    schema=schema,
)
```

### TableHandle

```python
view = await handle.read()
delta = await handle.diff(view.transaction_id, view.transaction_id)
```

### FileSpec

`FileSpec` attaches file metadata to table mutations. `format_options` are persisted
and validated, but should be used sparingly since they are stored as JSON.

```python
from planar import FileSpec

file = FileSpec(
    file_format="parquet",
    file_path="file:///tmp/planar/events/part-000.parquet",
    record_count=1000,
    file_size_bytes=1024 * 1024,
    format_options={"compression": "zstd"},
)
```

## Storage Readers/Writers (Rust Core)

The Python surface will mirror the Rust core behavior. These APIs are defined in Rust
and will be exposed through bindings.

### Common Patterns

- `read(path, options)` returns a single `RecordBatch` (materialized).
- `read_stream(path, options)` yields batches for streaming.
- `write(batch, path, options)` writes a single batch.
- `write_stream(stream, path, options)` writes a stream of batches.

`read` and `read_stream` accept the same options; the difference is only
batch vs stream delivery.

### Parquet

Parquet already provides concrete option types:

- `ArrowReaderOptions` for read behavior.
- `WriterProperties` for write behavior.

### Lance

Lance exposes a configurable `Scanner` but does not provide a dedicated
read-options struct. For Python ergonomics we will keep a concrete `LanceReadOptions`
in Planar and map those fields to `Scanner` settings.

### Vortex

Vortex exposes concrete types:

- `VortexOpenOptions` for reads.
- `VortexWriteOptions` for writes.

## Planned Python Storage API

The intended Python interface (names subject to change) aligns with the Rust core:

```python
from planar.storage import read, read_stream, write

batch = await read(
    "data.parquet",
    file_format="parquet",
    options={"batch_size": 8192},
)
stream = await read_stream(
    "data.parquet",
    file_format="parquet",
    options={"batch_size": 8192},
)
await write(
    batch,
    "data.parquet",
    file_format="parquet",
    options={"compression": "zstd"},
)
```

Format-specific options will be concrete objects where possible.
We will only accept JSON-ish dicts where the core must persist options.

## Notes

- The Python API is async-first.
- Streaming APIs will return async iterators of `RecordBatch` objects.
- File format options should be explicit and typed when possible.
