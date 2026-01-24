# DB-Control-Plane, File-Data-Plane Architecture

## Purpose

This document defines Planar's control-plane architecture. The core idea is simple: a transactional database holds catalog and metadata, while all table data lives in immutable files stored in object storage. The goal is to preserve open table format behavior, keep storage and compute decoupled, and simplify commit coordination by using the database's native transaction semantics rather than reinventing them in the file layer.


## Architecture Overview

Planar splits into two planes. The **data plane** stores immutable data files in object storage. The **control plane** stores catalog, schema, and transaction metadata in a relational database. The database is authoritative for the current state of each table; readers resolve what files to scan by querying the control plane, then read those files directly from object storage.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Control Plane (DB)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   tables    │  │ transactions│  │   schemas   │  │       files         │ │
│  │             │  │             │  │             │  │                     │ │
│  │ table_uuid  │  │ txn_id      │  │ schema_uuid │  │ file_uuid           │ │
│  │ namespace   │  │ table_uuid  │  │ table_uuid  │  │ table_uuid          │ │
│  │ table_name  │  │ timestamp   │  │ version     │  │ file_path           │ │
│  │ location    │  │ parent_txn  │  │ valid_from  │  │ added_in_txn        │ │
│  │ current_txn │  │             │  │ valid_to    │  │ removed_in_txn      │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ metadata queries
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Data Plane (Object Storage)                       │
│                                                                             │
│   s3://bucket/tables/abc123/data/00001.parquet                              │
│   s3://bucket/tables/abc123/data/00002.parquet                              │
│   s3://bucket/tables/abc123/data/00003.parquet                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Current Planar Implementation

The existing Planar codebase already implements this architecture. The `src/catalog/mod.rs` module defines the `Catalog` trait with `create_table`, `read_table`, and `commit` operations. The `src/catalog/schema.rs` module defines the metadata types: `Table`, `Transaction`, `Schema`, `Column`, and `File`. The `db/migrations/001_initial_schema.sql` file creates the relational schema.

The key insight from the current implementation is that transactions are the unit of change. Each commit creates a new transaction record, and files are marked with `added_in_transaction_id` and `removed_in_transaction_id` to track their lifecycle. This is sound and aligns with the architecture described here.

What the current implementation lacks is explicit handling of several edge cases: orphan file cleanup, conflict retry strategies, and read path degradation. This document specifies solutions for those gaps.

## Invariants

The system maintains four invariants at all times:

1. **Snapshot consistency**: A committed transaction references only fully written files. A reader querying a transaction sees exactly the files that were visible at that transaction, no more and no less.

2. **Atomic pointer advance**: The `current_transaction_id` on a table advances atomically within a database transaction. Two concurrent commits cannot both succeed; one will fail the optimistic check and must retry.

3. **File visibility**: A file is visible to readers only if it has been added in a committed transaction and has not been removed in an earlier or equal transaction. The query predicate is: `added_in_transaction_id <= txn AND (removed_in_transaction_id IS NULL OR removed_in_transaction_id > txn)`.

4. **Orphan safety**: Files written to object storage but never committed are orphans. The system must eventually clean them up, but must never delete files referenced by any retained transaction.

## Commit Protocol

The commit protocol bridges a transactional database and a non-transactional object store. The writer cannot atomically write files and metadata together, so the protocol is designed to be safe under partial failure.

### Write Path

```
┌────────┐      ┌──────────────┐      ┌───────────┐
│ Writer │      │ Object Store │      │ Control DB│
└───┬────┘      └──────┬───────┘      └─────┬─────┘
    │                  │                    │
    │  1. Upload files │                    │
    │─────────────────>│                    │
    │                  │                    │
    │  2. Files ack    │                    │
    │<─────────────────│                    │
    │                  │                    │
    │  3. BEGIN TRANSACTION                 │
    │──────────────────────────────────────>│
    │                  │                    │
    │  4. SELECT ... FOR UPDATE (lock row)  │
    │──────────────────────────────────────>│
    │                  │                    │
    │  5. Validate current_txn == base_txn  │
    │──────────────────────────────────────>│
    │                  │                    │
    │  6. INSERT transaction + file rows    │
    │──────────────────────────────────────>│
    │                  │                    │
    │  7. UPDATE tables SET current_txn     │
    │──────────────────────────────────────>│
    │                  │                    │
    │  8. COMMIT                            │
    │──────────────────────────────────────>│
    │                  │                    │
```

### Step-by-Step

1. **Upload files**: The writer uploads data files to object storage. These files are not yet referenced by any transaction. If the writer crashes here, the files become orphans.

2. **Begin transaction**: The writer opens a database transaction. This establishes a consistent view of the control plane.

3. **Lock the table row**: The writer acquires a row-level lock on the table. In PostgreSQL, this is `SELECT ... FOR UPDATE`. In SQLite, begin an immediate transaction. This prevents concurrent commits from interleaving.

4. **Validate base transaction**: The writer checks that `current_transaction_id` matches the base transaction it read earlier. If not, another commit succeeded in between, and this commit fails with a conflict error.

5. **Insert metadata**: The writer inserts a new transaction row and file rows. The file rows have `added_in_transaction_id` set to the new transaction.

6. **Advance pointer**: The writer updates the table's `current_transaction_id` to the new transaction.

7. **Commit**: The database transaction commits. At this instant, the new files become visible to readers.

### Failure Handling

If the writer crashes before step 7, no metadata is committed. The uploaded files are orphans and will be cleaned up by GC. If the writer crashes after step 7, the commit succeeded and the files are visible.

The current Planar implementation in `SqlCatalog::commit` follows this protocol. The validation happens at lines 921-926 of `src/catalog/mod.rs`:

```rust
if current_transaction_id != base_transaction_id {
    return Err(CatalogError::Conflict(format!(
        "base transaction {} does not match current {}",
        base_transaction_id, current_transaction_id
    )));
}
```

## Concurrency Control

Conflicts occur when two writers attempt to commit based on the same base transaction. The system must detect conflicts and provide a path to resolution. There are several approaches to concurrency control, each with different tradeoffs.

### Option 1: Optimistic Concurrency with Retry

This is the current design. Writers read the base transaction, do their work, then attempt to commit. If the base transaction has changed, the commit fails and the writer must retry.

**Advantages**: High throughput when conflicts are rare. No lock contention during file upload. Simple to implement.

**Disadvantages**: Wasted work when conflicts occur. Writers may need to re-upload files or recompute mutations. Not suitable for high-contention workloads.

**Retry strategies**:
- **Exponential backoff**: Wait `base_delay * 2^attempt + jitter` between retries. Good for bursty contention.
- **Immediate retry with re-read**: Retry immediately after re-reading the current state. Good for append-only workloads where the mutation doesn't depend on current state.
- **Caller-managed retry**: Return the conflict to the caller and let them decide. Good when the mutation requires user input or external state.

### Option 2: Pessimistic Locking

Writers acquire an exclusive lock on the table before starting work. Other writers block until the lock is released.

**Advantages**: No wasted work. Guaranteed success once lock is acquired. Predictable latency under contention.

**Disadvantages**: Reduced throughput. Writers block even during file upload. Risk of deadlocks if multiple tables are involved. Lock holder crashes require timeout-based recovery.

**Implementation**: Use database advisory locks or a distributed lock service. The lock must have a TTL to handle crashed writers.

### Option 3: Serialized Writes via Queue

All writes for a table go through a single serialized queue. A coordinator processes commits one at a time.

**Advantages**: No conflicts by construction. Predictable ordering. Can batch multiple small commits.

**Disadvantages**: Single point of serialization. Higher latency for individual commits. Requires a coordinator service.

**Implementation**: A background worker per table (or shared across tables) that pulls from a commit queue and applies mutations sequentially.

### Option 4: Append-Only Fast Path

For pure append operations, conflicts may be resolvable without retry. If two writers both append files based on the same base transaction, the system can accept both commits by merging the file lists.

**Advantages**: No conflicts for the common case of appending data. High throughput for ingest workloads.

**Disadvantages**: Only works for appends. Deletes, schema changes, and overwrites still require conflict detection. More complex commit logic.

**Implementation**: During commit, if the only change since the base transaction is other appends, merge rather than conflict. This requires tracking the operation type in the transaction record.

### Current Recommendation

Start with **Option 1 (Optimistic)** for simplicity. Add **Option 4 (Append-Only Fast Path)** when append throughput becomes a bottleneck. Consider **Option 2 (Pessimistic)** for tables with known high contention. **Option 3 (Queue)** is likely overkill for most workloads but may be useful for very high-throughput ingest pipelines.

The retry logic is not currently implemented in Planar; callers must handle conflicts. A future API could offer pluggable conflict resolution strategies.

### Lock Granularity

The current design uses table-level locking. This is sufficient for most workloads. If high-concurrency writes to the same table become a bottleneck, partition-level locking could be introduced, but that adds complexity and is not needed initially.

## Read Protocol

Readers query the control plane to resolve the current transaction and file list, then scan files from object storage.

### Read Path

```
┌────────┐      ┌───────────┐      ┌──────────────┐
│ Reader │      │ Control DB│      │ Object Store │
└───┬────┘      └─────┬─────┘      └──────┬───────┘
    │                 │                   │
    │  1. Query current_txn + files       │
    │────────────────>│                   │
    │                 │                   │
    │  2. Return txn + file list          │
    │<────────────────│                   │
    │                 │                   │
    │  3. Scan files                      │
    │────────────────────────────────────>│
    │                 │                   │
    │  4. Return data                     │
    │<────────────────────────────────────│
    │                 │                   │
```

### Time Travel

The current Planar implementation supports time travel via the `read_at` method on `TableHandle`. Readers can query any retained transaction by passing its ID. The file visibility predicate handles this correctly.

### Metadata Caching

For read-heavy workloads, querying the control plane for every read adds latency and DB load. But what exactly can be cached, and what must always go to the DB?

**What Iceberg and Delta do**:

Iceberg and Delta have a natural caching story because their metadata is stored in files. A reader fetches the current metadata pointer from the catalog (a single small query), then reads the metadata files from object storage. Those files are immutable and can be cached indefinitely. When a new commit happens, the metadata pointer changes, and the reader fetches the new metadata files.

Delta goes further: readers can cache "checkpoint" files that summarize the transaction log, avoiding the need to replay many small log entries.

The key insight is that their metadata is *immutable once written*. The only mutable state is the pointer to the current metadata.

**What Planar can cache**:

Planar stores metadata in the database, which is mutable. However, we can still cache effectively:

1. **The current transaction ID**: This is a small, fast query. Readers can check if their cached transaction is still current before re-fetching the full file list.

2. **The file list for a transaction**: Once fetched, the file list for a specific transaction ID is immutable (that transaction will never change). The cache key is `(table_uuid, transaction_id)`, not just `(table_uuid)`.

3. **Schema and column metadata**: Rarely changes. Cache with long TTL.

**What cannot be cached**:

The control plane must always be consulted to determine *which* transaction is current. A stale cache might return an old transaction's file list, which is consistent but outdated. This is acceptable for some workloads (analytics queries that tolerate staleness) but not for others (transactional reads that need the latest data).

**Cache design**:

```
┌────────┐      ┌───────────────────────┐      ┌───────────┐
│ Reader │─────>│ Metadata Cache        │─────>│ Control DB│
└────────┘      │                       │      └───────────┘
                │ Key: (table, txn_id)  │
                │ Value: file list      │
                │                       │
                │ Key: (table)          │
                │ Value: current_txn_id │
                │ TTL: short (seconds)  │
                └───────────────────────┘
```

A reader first checks the cache for the current transaction ID. If the cached ID matches the reader's required freshness (e.g., "no older than 10 seconds"), it uses the cached file list. Otherwise, it queries the DB for the current transaction ID, then looks up the file list in cache or fetches it.

**Tradeoff**: This design still requires a DB round-trip to check freshness, unless the reader accepts potentially stale data. For truly DB-free reads, Planar would need to export metadata to files (like Iceberg does), which is a larger architectural change.

### Control Plane Unavailability

If the control plane is unavailable, commits are blocked. Reads can continue only if the reader has cached metadata and accepts staleness.

For high-availability deployments, the control plane database should be replicated. A read replica can serve metadata queries while the primary handles writes. This is standard DB operations and not specific to Planar.

## Orphan File Cleanup

Files uploaded but never committed are orphans. They consume storage and must be cleaned up.

### Orphan Detection

An orphan is a file that exists in object storage but is not referenced by any file row in the control plane. However, we cannot simply delete any unreferenced file, because:

1. A writer may have just uploaded the file and not yet committed.
2. The file may be referenced by a retained transaction for time travel.

### Solution: Staged Paths and Grace Period

Writers upload files to a staging prefix, e.g., `s3://bucket/tables/{table_uuid}/staging/{upload_id}/`. On commit, the file paths recorded in the database point to these locations. (Optionally, files can be moved to a canonical path after commit, but this is not required.)

The GC process:

1. List all files under the staging prefix.
2. For each file, check if it is referenced by any file row.
3. If not referenced and older than the grace period (e.g., 24 hours), delete it.

The grace period ensures we do not delete files from in-flight commits.

### Implementation

A GC job runs periodically (e.g., hourly). Pseudocode:

```rust
async fn gc_orphans(catalog: &SqlCatalog, storage: &ObjectStore, grace_period: Duration) {
    let cutoff = Utc::now() - grace_period;
    
    // List all files in staging
    let staged_files = storage.list("staging/").await?;
    
    for file in staged_files {
        if file.last_modified > cutoff {
            continue; // Too recent, might be in-flight
        }
        
        // Check if referenced
        let referenced = catalog.is_file_referenced(&file.path).await?;
        if !referenced {
            storage.delete(&file.path).await?;
        }
    }
}
```

The catalog query checks against all file rows, not just active ones, to preserve time travel.

## Transaction Retention and Expiry

Retaining all transactions forever grows the metadata unboundedly. A retention policy expires old transactions and their associated file rows.

### Retention Policy

1. **Retain N transactions**: Keep the most recent N transactions per table.
2. **Retain by age**: Keep transactions newer than a threshold (e.g., 30 days).
3. **Retain tagged transactions**: Never expire transactions with a `retained=true` property.

### Expiry Process

Expiring a transaction marks it as expired and makes its files eligible for deletion. The process:

1. Identify transactions older than the retention threshold.
2. For each expired transaction, check if any file is *only* referenced by expired transactions.
3. If so, mark the file's `removed_in_transaction_id` to the oldest active transaction.
4. Delete the transaction row.
5. The orphan GC will eventually delete the physical files.

This is a control-plane operation. The data-plane cleanup follows via normal orphan GC.

## File Lifecycle State Machine

A file moves through a defined lifecycle:

```
                    ┌──────────────────────────────────────────────────┐
                    │                                                  │
                    ▼                                                  │
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│ Staged  │───>│ Visible │───>│ Deleted │───>│ Expired │───>│ Removed │
└─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘
     │              │              │              │              │
     │              │              │              │              │
     ▼              │              │              │              ▼
  Orphan GC         │              │              │         Physical
  (if never         │              │              │         deletion
  committed)        │              │              │
                    │              │              │
                    └──────────────┴──────────────┘
                         Time travel queries
```

- **Staged**: File uploaded, not yet committed. Visible to no one.
- **Visible**: File committed, `removed_in_transaction_id` is NULL. Visible to current readers.
- **Deleted**: File logically deleted, `removed_in_transaction_id` is set. Visible to time-travel queries for transactions before the deletion.
- **Expired**: Transaction is past retention. File may still be on disk but is no longer queryable.
- **Removed**: Physical file deleted by GC.

## Table Maintenance

Planar requires several background maintenance operations: orphan file cleanup, transaction expiry, and file compaction. These operations share common characteristics and could be unified into a single maintenance subsystem, similar to PostgreSQL's autovacuum.

### Commonalities

All maintenance operations:
- Run in the background without blocking reads or writes.
- Operate on table-level or system-level scope.
- Can be triggered by thresholds, schedules, or manual invocation.
- Must be safe to run concurrently with normal operations.
- Should be idempotent (running twice produces the same result).

### Maintenance Operations

**Orphan GC**: Remove files from object storage that are not referenced by any transaction.
- Trigger: Scheduled (hourly) or when orphan count exceeds threshold.
- Scope: Per-table or system-wide.

**Transaction Expiry**: Remove old transactions and their metadata from the control plane.
- Trigger: When transaction count exceeds retention limit or oldest transaction exceeds age limit.
- Scope: Per-table.

**Compaction**: Rewrite many small files into fewer large files.
- Trigger: When small file count or ratio exceeds threshold.
- Scope: Per-table or per-partition.

**Statistics Update**: Recompute table and column statistics for query optimization.
- Trigger: After significant data changes or on schedule.
- Scope: Per-table.

### Unified Maintenance Daemon (Future Design)

A maintenance daemon could monitor all tables and trigger operations based on configurable policies. This is similar to PostgreSQL's autovacuum, which monitors tables and triggers vacuum and analyze operations based on thresholds.

```
┌─────────────────────────────────────────────────────────┐
│                  Maintenance Daemon                     │
│                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │ Orphan GC   │  │ Txn Expiry  │  │ Compaction      │  │
│  │ Worker      │  │ Worker      │  │ Worker          │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
│         │               │                 │             │
│         └───────────────┴─────────────────┘             │
│                         │                               │
│                  ┌──────┴──────┐                        │
│                  │ Scheduler   │                        │
│                  │ + Policies  │                        │
│                  └─────────────┘                        │
└─────────────────────────────────────────────────────────┘
```

**Policy examples**:
- Run orphan GC when `orphan_file_count > 1000` or every 6 hours.
- Run transaction expiry when `transaction_count > 100` or oldest transaction > 30 days.
- Run compaction when `small_file_ratio > 0.3` or `small_file_count > 500`.

**Design considerations**:
- Workers should acquire lightweight locks to prevent concurrent maintenance on the same table.
- Maintenance should be interruptible and resumable.
- Progress and errors should be logged and queryable.
- Manual trigger API for operators to force maintenance.

This design is not needed immediately. For now, maintenance can be triggered manually or via simple scheduled jobs. The unified daemon is a future optimization for operational simplicity.

### Compaction Protocol

Compaction is the most complex maintenance operation because it modifies table state. The protocol is straightforward because compaction is just a normal commit:

1. Read the current snapshot and identify small files.
2. Read the data from those files.
3. Write new, larger files to staging.
4. Commit a transaction that:
   - Adds the new files.
   - Deletes the old files (sets `removed_in_transaction_id`).
5. The old files remain for time travel until expired, then are removed by orphan GC.

Compaction can conflict with concurrent writes. If the compaction commit fails due to a conflict, it must re-read the current state and recompute which files to compact. This is acceptable because compaction is not latency-sensitive.

## Comparison to Iceberg and Delta Lake

### Iceberg

Iceberg uses file-based metadata: manifest files list data files, manifest lists point to manifests, and metadata files point to manifest lists. A catalog (often DB-backed) stores a pointer to the current metadata file.

Planar differs by storing the file list directly in the database rather than in manifest files. This simplifies commit coordination (no need to atomically publish a metadata file) but means the database must handle the file list size. For tables with millions of files, this may require pagination or a manifest-like indirection. The current design is suitable for tables up to hundreds of thousands of files.

### Delta Lake

Delta stores a transaction log as JSON files in the `_delta_log` directory. Commits append a new log entry. Readers reconstruct state by replaying the log.

Planar differs by using a database for the log rather than files. This avoids the complexity of log compaction checkpoints and provides stronger conflict detection via database transactions. The tradeoff is a dependency on the database for all commits.

### Summary

| Aspect | Iceberg | Delta Lake | Planar |
|--------|---------|------------|--------|
| Metadata location | Files + catalog pointer | Files (_delta_log) | Database |
| Commit coordination | Atomic file publish or catalog CAS | Log append with conflict check | DB transaction |
| Conflict detection | Catalog-level or file CAS | Log sequence number | Optimistic txn check |
| GC | Orphan file cleanup + snapshot expiry | Vacuum command | Orphan GC + retention policy |

## Practical Next Steps

The following changes are needed to complete this architecture in Planar:

1. **Conflict handling API**: Expose conflict errors to callers with enough context to retry. Consider adding optional retry helpers, but don't prescribe a single strategy.

2. **Add row-level locking for PostgreSQL**: Use `SELECT ... FOR UPDATE` on the table row during commit to prevent race conditions. The current implementation may have a race window.

3. **Add orphan GC**: Implement a simple CLI command or background task that scans for unreferenced files older than the grace period.

4. **Add retention policy**: Implement transaction expiry based on count or age. Start with a simple CLI command.

5. **Add metadata cache**: Implement an in-memory cache for `TableView` keyed by `(table_uuid, transaction_id)`. Add a short-TTL cache for current transaction ID.

6. **Add integration tests**: Test concurrent commits, conflict detection, and orphan cleanup.

7. **Document the schema**: Publish the database schema as a stable interface for direct access by external tools.

