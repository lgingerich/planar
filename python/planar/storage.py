from __future__ import annotations

from typing import Iterable, Optional

import pyarrow as pa

from . import _native


def _ipc_to_record_batch(data: bytes) -> pa.RecordBatch:
    reader = pa.ipc.open_stream(data)
    batches = list(reader)
    if not batches:
        raise ValueError("No batches returned")
    if len(batches) == 1:
        return batches[0]
    table = pa.Table.from_batches(batches).combine_chunks()
    return table.to_batches()[0]


def _ipc_to_stream_reader(data: bytes) -> pa.ipc.RecordBatchStreamReader:
    return pa.ipc.open_stream(data)


def _batches_to_ipc(batches: Iterable[pa.RecordBatch]) -> bytes:
    batch_list = list(batches)
    if not batch_list:
        raise ValueError("Expected at least one RecordBatch")
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch_list[0].schema)
    for batch in batch_list:
        writer.write(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def _normalize_format(file_format: str) -> str:
    fmt = file_format.lower()
    if fmt in {"parquet", "lance", "vortex"}:
        return fmt
    raise ValueError(f"Unsupported file format: {file_format}")


async def read_parquet(path: str, *, batch_size: Optional[int] = None) -> pa.RecordBatch:
    data = await _native.read_parquet_ipc(path, batch_size)
    return _ipc_to_record_batch(data)


async def read_parquet_stream(
    path: str, *, batch_size: Optional[int] = None
) -> pa.ipc.RecordBatchStreamReader:
    data = await _native.read_parquet_stream_ipc(path, batch_size)
    return _ipc_to_stream_reader(data)


async def write_parquet(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc([batch])
    await _native.write_parquet_ipc(path, data, options)


async def write_parquet_stream(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc(batches)
    await _native.write_parquet_stream_ipc(path, data, options)


async def read_lance(
    path: str,
    *,
    batch_size: Optional[int] = None,
    columns: Optional[list[str]] = None,
    filter: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    scan_in_order: Optional[bool] = None,
    io_buffer_size: Optional[int] = None,
) -> pa.RecordBatch:
    data = await _native.read_lance_ipc(
        path,
        batch_size,
        columns,
        filter,
        limit,
        offset,
        scan_in_order,
        io_buffer_size,
    )
    return _ipc_to_record_batch(data)


async def read_lance_stream(
    path: str,
    *,
    batch_size: Optional[int] = None,
    columns: Optional[list[str]] = None,
    filter: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    scan_in_order: Optional[bool] = None,
    io_buffer_size: Optional[int] = None,
) -> pa.ipc.RecordBatchStreamReader:
    data = await _native.read_lance_stream_ipc(
        path,
        batch_size,
        columns,
        filter,
        limit,
        offset,
        scan_in_order,
        io_buffer_size,
    )
    return _ipc_to_stream_reader(data)


async def write_lance(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc([batch])
    await _native.write_lance_ipc(path, data, options)


async def write_lance_stream(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc(batches)
    await _native.write_lance_stream_ipc(path, data, options)


async def read_vortex(
    path: str,
    *,
    initial_read_size: Optional[int] = None,
    segment_cache: Optional[bool] = None,
) -> pa.RecordBatch:
    data = await _native.read_vortex_ipc(path, initial_read_size, segment_cache)
    return _ipc_to_record_batch(data)


async def read_vortex_stream(
    path: str,
    *,
    initial_read_size: Optional[int] = None,
    segment_cache: Optional[bool] = None,
) -> pa.ipc.RecordBatchStreamReader:
    data = await _native.read_vortex_stream_ipc(path, initial_read_size, segment_cache)
    return _ipc_to_stream_reader(data)


async def write_vortex(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc([batch])
    await _native.write_vortex_ipc(path, data, options)


async def write_vortex_stream(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc(batches)
    await _native.write_vortex_stream_ipc(path, data, options)


async def read(
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> pa.RecordBatch:
    fmt = _normalize_format(file_format)
    options = options or {}
    if fmt == "parquet":
        return await read_parquet(path, batch_size=options.get("batch_size"))
    if fmt == "lance":
        return await read_lance(
            path,
            batch_size=options.get("batch_size"),
            columns=options.get("columns"),
            filter=options.get("filter"),
            limit=options.get("limit"),
            offset=options.get("offset"),
            scan_in_order=options.get("scan_in_order"),
            io_buffer_size=options.get("io_buffer_size"),
        )
    return await read_vortex(
        path,
        initial_read_size=options.get("initial_read_size"),
        segment_cache=options.get("segment_cache"),
    )


async def read_stream(
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> pa.ipc.RecordBatchStreamReader:
    fmt = _normalize_format(file_format)
    options = options or {}
    if fmt == "parquet":
        return await read_parquet_stream(path, batch_size=options.get("batch_size"))
    if fmt == "lance":
        return await read_lance_stream(
            path,
            batch_size=options.get("batch_size"),
            columns=options.get("columns"),
            filter=options.get("filter"),
            limit=options.get("limit"),
            offset=options.get("offset"),
            scan_in_order=options.get("scan_in_order"),
            io_buffer_size=options.get("io_buffer_size"),
        )
    return await read_vortex_stream(
        path,
        initial_read_size=options.get("initial_read_size"),
        segment_cache=options.get("segment_cache"),
    )


async def write(
    batch: pa.RecordBatch,
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> None:
    fmt = _normalize_format(file_format)
    if fmt == "parquet":
        await write_parquet(batch, path, options=options)
        return
    if fmt == "lance":
        await write_lance(batch, path, options=options)
        return
    await write_vortex(batch, path, options=options)


async def write_stream(
    batches: Iterable[pa.RecordBatch],
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> None:
    fmt = _normalize_format(file_format)
    if fmt == "parquet":
        await write_parquet_stream(batches, path, options=options)
        return
    if fmt == "lance":
        await write_lance_stream(batches, path, options=options)
        return
    await write_vortex_stream(batches, path, options=options)


__all__ = [
    "read",
    "read_stream",
    "write",
    "write_stream",
]
