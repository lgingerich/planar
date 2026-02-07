from __future__ import annotations

import asyncio
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


def read_parquet(path: str, *, batch_size: Optional[int] = None) -> pa.RecordBatch:
    data = _native.read_parquet_ipc(path, batch_size)
    return _ipc_to_record_batch(data)


def read_parquet_stream(
    path: str, *, batch_size: Optional[int] = None
) -> pa.ipc.RecordBatchStreamReader:
    data = _native.read_parquet_stream_ipc(path, batch_size)
    return _ipc_to_stream_reader(data)


def write_parquet(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc([batch])
    _native.write_parquet_ipc(path, data, options)


def write_parquet_stream(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc(batches)
    _native.write_parquet_stream_ipc(path, data, options)


def read_lance(
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
    data = _native.read_lance_ipc(
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


def read_lance_stream(
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
    data = _native.read_lance_stream_ipc(
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


def write_lance(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc([batch])
    _native.write_lance_ipc(path, data, options)


def write_lance_stream(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc(batches)
    _native.write_lance_stream_ipc(path, data, options)


def read_vortex(
    path: str,
    *,
    initial_read_size: Optional[int] = None,
    segment_cache: Optional[bool] = None,
) -> pa.RecordBatch:
    data = _native.read_vortex_ipc(path, initial_read_size, segment_cache)
    return _ipc_to_record_batch(data)


def read_vortex_stream(
    path: str,
    *,
    initial_read_size: Optional[int] = None,
    segment_cache: Optional[bool] = None,
) -> pa.ipc.RecordBatchStreamReader:
    data = _native.read_vortex_stream_ipc(path, initial_read_size, segment_cache)
    return _ipc_to_stream_reader(data)


def write_vortex(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc([batch])
    _native.write_vortex_ipc(path, data, options)


def write_vortex_stream(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    data = _batches_to_ipc(batches)
    _native.write_vortex_stream_ipc(path, data, options)


def read(
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> pa.RecordBatch:
    fmt = _normalize_format(file_format)
    options = options or {}
    if fmt == "parquet":
        return read_parquet(path, batch_size=options.get("batch_size"))
    if fmt == "lance":
        return read_lance(
            path,
            batch_size=options.get("batch_size"),
            columns=options.get("columns"),
            filter=options.get("filter"),
            limit=options.get("limit"),
            offset=options.get("offset"),
            scan_in_order=options.get("scan_in_order"),
            io_buffer_size=options.get("io_buffer_size"),
        )
    return read_vortex(
        path,
        initial_read_size=options.get("initial_read_size"),
        segment_cache=options.get("segment_cache"),
    )


def read_stream(
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> pa.ipc.RecordBatchStreamReader:
    fmt = _normalize_format(file_format)
    options = options or {}
    if fmt == "parquet":
        return read_parquet_stream(path, batch_size=options.get("batch_size"))
    if fmt == "lance":
        return read_lance_stream(
            path,
            batch_size=options.get("batch_size"),
            columns=options.get("columns"),
            filter=options.get("filter"),
            limit=options.get("limit"),
            offset=options.get("offset"),
            scan_in_order=options.get("scan_in_order"),
            io_buffer_size=options.get("io_buffer_size"),
        )
    return read_vortex_stream(
        path,
        initial_read_size=options.get("initial_read_size"),
        segment_cache=options.get("segment_cache"),
    )


def write(
    batch: pa.RecordBatch,
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> None:
    fmt = _normalize_format(file_format)
    if fmt == "parquet":
        write_parquet(batch, path, options=options)
        return
    if fmt == "lance":
        write_lance(batch, path, options=options)
        return
    write_vortex(batch, path, options=options)


def write_stream(
    batches: Iterable[pa.RecordBatch],
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> None:
    fmt = _normalize_format(file_format)
    if fmt == "parquet":
        write_parquet_stream(batches, path, options=options)
        return
    if fmt == "lance":
        write_lance_stream(batches, path, options=options)
        return
    write_vortex_stream(batches, path, options=options)


async def read_parquet_async(path: str, *, batch_size: Optional[int] = None) -> pa.RecordBatch:
    return await asyncio.to_thread(read_parquet, path, batch_size=batch_size)


async def read_parquet_stream_async(
    path: str, *, batch_size: Optional[int] = None
) -> pa.ipc.RecordBatchStreamReader:
    return await asyncio.to_thread(read_parquet_stream, path, batch_size=batch_size)


async def write_parquet_async(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    await asyncio.to_thread(write_parquet, batch, path, options=options)


async def write_parquet_stream_async(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    await asyncio.to_thread(write_parquet_stream, batches, path, options=options)


async def read_lance_async(
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
    return await asyncio.to_thread(
        read_lance,
        path,
        batch_size=batch_size,
        columns=columns,
        filter=filter,
        limit=limit,
        offset=offset,
        scan_in_order=scan_in_order,
        io_buffer_size=io_buffer_size,
    )


async def read_lance_stream_async(
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
    return await asyncio.to_thread(
        read_lance_stream,
        path,
        batch_size=batch_size,
        columns=columns,
        filter=filter,
        limit=limit,
        offset=offset,
        scan_in_order=scan_in_order,
        io_buffer_size=io_buffer_size,
    )


async def write_lance_async(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    await asyncio.to_thread(write_lance, batch, path, options=options)


async def write_lance_stream_async(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    await asyncio.to_thread(write_lance_stream, batches, path, options=options)


async def read_vortex_async(
    path: str,
    *,
    initial_read_size: Optional[int] = None,
    segment_cache: Optional[bool] = None,
) -> pa.RecordBatch:
    return await asyncio.to_thread(
        read_vortex, path, initial_read_size=initial_read_size, segment_cache=segment_cache
    )


async def read_vortex_stream_async(
    path: str,
    *,
    initial_read_size: Optional[int] = None,
    segment_cache: Optional[bool] = None,
) -> pa.ipc.RecordBatchStreamReader:
    return await asyncio.to_thread(
        read_vortex_stream,
        path,
        initial_read_size=initial_read_size,
        segment_cache=segment_cache,
    )


async def write_vortex_async(
    batch: pa.RecordBatch, path: str, *, options: Optional[dict] = None
) -> None:
    await asyncio.to_thread(write_vortex, batch, path, options=options)


async def write_vortex_stream_async(
    batches: Iterable[pa.RecordBatch], path: str, *, options: Optional[dict] = None
) -> None:
    await asyncio.to_thread(write_vortex_stream, batches, path, options=options)


async def read_async(
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> pa.RecordBatch:
    return await asyncio.to_thread(read, path, file_format=file_format, options=options)


async def read_stream_async(
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> pa.ipc.RecordBatchStreamReader:
    return await asyncio.to_thread(
        read_stream, path, file_format=file_format, options=options
    )


async def write_async(
    batch: pa.RecordBatch,
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> None:
    await asyncio.to_thread(write, batch, path, file_format=file_format, options=options)


async def write_stream_async(
    batches: Iterable[pa.RecordBatch],
    path: str,
    *,
    file_format: str,
    options: Optional[dict] = None,
) -> None:
    await asyncio.to_thread(
        write_stream, batches, path, file_format=file_format, options=options
    )


__all__ = [
    "read",
    "read_stream",
    "write",
    "write_stream",
    "read_parquet",
    "read_parquet_stream",
    "write_parquet",
    "write_parquet_stream",
    "read_lance",
    "read_lance_stream",
    "write_lance",
    "write_lance_stream",
    "read_vortex",
    "read_vortex_stream",
    "write_vortex",
    "write_vortex_stream",
    "read_async",
    "read_stream_async",
    "write_async",
    "write_stream_async",
    "read_parquet_async",
    "read_parquet_stream_async",
    "write_parquet_async",
    "write_parquet_stream_async",
    "read_lance_async",
    "read_lance_stream_async",
    "write_lance_async",
    "write_lance_stream_async",
    "read_vortex_async",
    "read_vortex_stream_async",
    "write_vortex_async",
    "write_vortex_stream_async",
]
