# Streaming Buffer Architecture

## Purpose

This document describes a server-side write buffer for streaming workloads. The goal is to accumulate small writes in memory and flush them to data files only when a minimum size threshold is reached, avoiding the "small file problem" that plagues lakehouse systems.

## Operational Note

The streaming buffer is an **optional component** that requires additional infrastructure to operate. It is not required for basic Planar usage. Users who do not have streaming workloads or who handle batching at the application level can skip this component entirely.

When enabled, the buffer can be configured **per table**. Some tables may benefit from buffering (high-frequency small writes) while others do not need it (batch loads of large files). The configuration allows mixing buffered and unbuffered tables in the same deployment.

**Infrastructure requirements**:
- A long-running server process (the buffer server).
- Local disk for write-ahead log (or distributed log for HA).
- Memory proportional to the number of buffered tables and buffer sizes.
- Network connectivity between writers and the buffer server.

## Problem Statement

Streaming workloads produce many small writes. If each write creates a file, the table accumulates thousands or millions of tiny files. This causes:

- **Read amplification**: Readers must open many files, each with overhead.
- **Metadata bloat**: Each file requires a metadata row, growing the control plane.
- **Object storage costs**: Many small files are less efficient than fewer large files.
- **Compaction pressure**: Background compaction must constantly merge small files.

Iceberg and Delta Lake both suffer from this problem. Users must either batch writes at the application level or run frequent compaction jobs.

## Design Goal

Provide an optional server component that buffers writes in memory and flushes to files only when:
- A size threshold is reached (e.g., 128 MB).
- A time threshold is reached (e.g., 5 minutes since first buffered write).
- A manual flush is requested.

This is conceptually similar to:
- **Memtables** in LSM-tree databases (RocksDB, LevelDB).
- **Write-ahead logs** with periodic checkpointing.
- **OS write buffers** that batch small writes before flushing to disk.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Streaming Buffer Server                            │
│                                                                             │
│  ┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐   │
│  │ Table A Buffer    │    │ Table B Buffer    │    │ Table C Buffer    │   │
│  │                   │    │                   │    │                   │   │
│  │ In-memory rows    │    │ In-memory rows    │    │ In-memory rows    │   │
│  │ + WAL on disk     │    │ + WAL on disk     │    │ + WAL on disk     │   │
│  └─────────┬─────────┘    └─────────┬─────────┘    └─────────┬─────────┘   │
│            │                        │                        │             │
│            └────────────────────────┼────────────────────────┘             │
│                                     │                                       │
│                              ┌──────┴──────┐                                │
│                              │ Flush       │                                │
│                              │ Coordinator │                                │
│                              └──────┬──────┘                                │
└─────────────────────────────────────┼───────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                 │                 │
                    ▼                 ▼                 ▼
            ┌───────────┐     ┌───────────┐     ┌───────────┐
            │ Object    │     │ Control   │     │ WAL       │
            │ Storage   │     │ Plane DB  │     │ Storage   │
            │ (files)   │     │ (metadata)│     │ (local)   │
            └───────────┘     └───────────┘     └───────────┘
```

## Components

### Write Buffer (per table)

Each table has a dedicated in-memory buffer that accumulates incoming rows. The buffer is an Arrow RecordBatch or a collection of RecordBatches.

**Responsibilities**:
- Accept incoming rows from writers.
- Track buffer size (row count and byte size).
- Trigger flush when thresholds are reached.
- Serve buffered data to readers (optional).

### Write-Ahead Log (WAL)

For durability, incoming writes are appended to a local WAL before being added to the in-memory buffer. If the server crashes, the WAL can be replayed to recover buffered data.

**WAL design options**:
1. **Local file**: Simple, fast, but tied to a single server.
2. **Object storage**: Durable across failures, but higher latency.
3. **Distributed log** (Kafka, Redpanda): High durability and throughput, but adds complexity.

For initial implementation, local file WAL is sufficient. Distributed WAL can be added later for multi-server setups.

### Flush Coordinator

The flush coordinator monitors all buffers and triggers flushes based on policies.

**Flush triggers**:
- **Size threshold**: Buffer reaches target file size (e.g., 128 MB).
- **Time threshold**: Buffer has been open for too long (e.g., 5 minutes).
- **Row count threshold**: Buffer reaches target row count (e.g., 1 million rows).
- **Manual flush**: Operator or application requests immediate flush.
- **Shutdown flush**: Server is shutting down gracefully.

**Flush process**:
1. Snapshot the current buffer state.
2. Write buffer contents to a data file in object storage.
3. Commit file metadata to the control plane.
4. Truncate the WAL.
5. Clear the in-memory buffer.

## Write Path

```
┌────────┐      ┌─────────────────┐      ┌─────────────────┐
│ Writer │      │ Buffer Server   │      │ Control Plane   │
└───┬────┘      └────────┬────────┘      └────────┬────────┘
    │                    │                        │
    │  1. Write rows     │                        │
    │───────────────────>│                        │
    │                    │                        │
    │                    │  2. Append to WAL      │
    │                    │──────────────────────> │ (local disk)
    │                    │                        │
    │                    │  3. Add to buffer      │
    │                    │  (in-memory)           │
    │                    │                        │
    │  4. Ack            │                        │
    │<───────────────────│                        │
    │                    │                        │
    │                    │  ... buffer grows ...  │
    │                    │                        │
    │                    │  5. Flush trigger      │
    │                    │                        │
    │                    │  6. Write file         │
    │                    │──────────────────────> │ (object storage)
    │                    │                        │
    │                    │  7. Commit metadata    │
    │                    │──────────────────────> │
    │                    │                        │
    │                    │  8. Truncate WAL       │
    │                    │──────────────────────> │ (local disk)
    │                    │                        │
```

## Read Path

Readers can choose whether to include buffered data:

### Option A: Committed-only reads (default)

Readers query the control plane for committed files. Buffered data is not visible. This is simpler and provides snapshot isolation.

### Option B: Include buffered data

Readers query both the control plane and the buffer server. The buffer server returns currently buffered rows. The reader merges buffered rows with file data.

**Tradeoffs**:
- Pro: Lower latency for recent writes.
- Con: More complex read path.
- Con: Buffered data is not yet durable (if WAL is local).
- Con: Buffered data may be inconsistent if flush is in progress.

For initial implementation, **Option A (committed-only)** is recommended. Add Option B later if real-time reads are needed.

## Durability Guarantees

The durability guarantee depends on the WAL configuration:

| WAL Type | Durability | Latency |
|----------|------------|---------|
| None | Data lost on crash | Lowest |
| Local file | Data recovered on same server | Low |
| Object storage | Data recovered on any server | Medium |
| Distributed log | Data recovered on any server, HA | Higher |

For most use cases, **local file WAL** is sufficient. If the server crashes, data is recovered when the server restarts. If the server is destroyed (disk lost), data since the last flush is lost.

For critical workloads, **distributed log WAL** (e.g., Kafka) provides stronger guarantees.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `buffer_size_threshold` | 128 MB | Flush when buffer reaches this size |
| `buffer_time_threshold` | 5 min | Flush when buffer has been open this long |
| `buffer_row_threshold` | 1M rows | Flush when buffer reaches this row count |
| `wal_enabled` | true | Enable write-ahead log for durability |
| `wal_type` | local | WAL storage type (local, object, kafka) |
| `include_buffered_reads` | false | Include buffered data in reads |

## Failure Scenarios

### Server crash before flush

WAL is replayed on restart. Buffered data is recovered and added back to the in-memory buffer. Flush proceeds normally.

### Server crash during flush

The flush is atomic at the control plane level. Either the metadata commit succeeds (file is visible) or it doesn't (file is orphaned and cleaned up by GC). WAL is not truncated until commit succeeds, so data is not lost.

### Server crash after flush

Data is safely committed. No recovery needed.

### Object storage unavailable during flush

Flush fails and is retried. Data remains in buffer and WAL. No data loss.

### Control plane unavailable during flush

Flush fails and is retried. Data remains in buffer and WAL. No data loss.

## Relationship to Other Components

### Control Plane (db_control_plane.md)

The streaming buffer is a **writer** that uses the standard commit protocol. From the control plane's perspective, a flush is just a normal file append commit. The buffer server holds a base transaction ID and commits when flushing.

### External Access (external_access.md)

External engines that use committed-only reads are unaffected by the buffer. If include-buffered-reads is enabled, external engines would need to query the buffer server in addition to the control plane.

### Table Maintenance

Buffered data is not subject to compaction (it's in memory). Once flushed, files follow the normal file lifecycle and can be compacted.

## Implementation Phases

### Phase 1: Basic buffer server

- In-memory buffer per table.
- Local file WAL.
- Size and time-based flush triggers.
- Committed-only reads.

### Phase 2: Durability options

- Object storage WAL.
- Distributed log WAL (Kafka integration).

### Phase 3: Real-time reads

- Include-buffered-reads option.
- Buffer server query API.
- Merge logic in readers.

### Phase 4: Multi-server

- Distributed buffer coordination.
- Partition-level buffers.
- Load balancing across buffer servers.

## Open Questions

1. **Buffer isolation**: Should each table have its own buffer, or should buffers be shared across tables? Per-table is simpler but uses more memory.

2. **Partitioning**: For partitioned tables, should there be one buffer per partition? This would improve flush locality but increase memory usage.

3. **Back-pressure**: What happens when writers produce data faster than the buffer can flush? Should we block writers or drop data?

4. **Buffer size limits**: Should there be a maximum buffer size to prevent OOM? What happens when the limit is reached?

5. **Exactly-once semantics**: How do we handle duplicate writes if a writer retries after a network failure? Should the buffer deduplicate?
