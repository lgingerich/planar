# External Engine Access and API Design

## Purpose

This document explores how external compute engines (Spark, Trino, Flink, DuckDB, Polars, DataFusion, etc.) can read and write Planar tables. The goal is to enable broad ecosystem compatibility without sacrificing Planar's design principles.

## Problem Statement

Planar stores table metadata in a relational database. External engines need to discover tables, resolve current snapshots, and locate data files. The question is: what interface should Planar expose to enable this?

The answer affects:
- How many engines can integrate with Planar.
- How much connector code must be written and maintained.
- How tightly coupled engines become to Planar's internals.
- Performance and latency characteristics.
- Security and access control.

## Design Options

### Option 1: Direct Database Access

Engines connect directly to the Planar metadata database and query tables using SQL.

**How it works**: Publish the database schema as a stable interface. Engines issue SQL queries to resolve table metadata, then read data files from object storage.

**Advantages**:
- No additional service to run or maintain.
- Low latency (direct SQL queries).
- Engines can use existing SQL client libraries.
- Schema is already relational and queryable.
- Works with any engine that can issue SQL queries.

**Disadvantages**:
- Tight coupling to database schema. Schema changes can break integrations.
- Schema must be versioned and stability guarantees documented.
- Security and access control at the database level (requires DB user management).
- No abstraction layer to enforce invariants or add logic.
- Different databases (PostgreSQL, SQLite) may have slightly different SQL dialects.

**Implementation requirements**:
- Document the schema as a stable API with versioning.
- Define which tables/columns are public vs internal.
- Provide example queries for common operations (list tables, get current snapshot, list files).
- Add views or stored procedures if needed for complex queries.

### Option 2: REST API

Planar exposes an HTTP REST service that engines query for metadata.

**How it works**: A Planar server process exposes endpoints like `GET /tables`, `GET /tables/{name}/snapshot`, `GET /tables/{name}/files`. Engines use a Planar-specific HTTP client.

**Advantages**:
- Clean abstraction. Planar controls the interface completely.
- Can add logic, caching, and authorization at the API layer.
- Schema changes can be hidden behind API versioning.
- Language-agnostic (any HTTP client works).

**Disadvantages**:
- Requires running and operating a service.
- HTTP overhead for metadata queries (latency, serialization).
- Must build and maintain client libraries or connectors for each engine.
- Another moving part in the deployment.

**Implementation requirements**:
- Define REST API schema (OpenAPI or similar).
- Build server component.
- Build client libraries (at minimum, a reference implementation).
- Handle authentication and authorization.

### Option 3: Iceberg-Compatible Catalog

Planar implements the Apache Iceberg REST Catalog specification. Engines use existing Iceberg connectors.

**How it works**: Planar exposes the Iceberg REST Catalog API. When engines think they're talking to Iceberg, they're actually talking to Planar. Planar translates requests to its internal model.

**Advantages**:
- Leverage existing Iceberg ecosystem. Many engines already have Iceberg connectors.
- No need to build connectors for Spark, Trino, Flink, etc.
- Iceberg is a widely adopted standard.

**Disadvantages**:
- Constrained by Iceberg's data model and assumptions.
- Planar semantics may not map cleanly to Iceberg concepts.
- Must keep up with Iceberg API evolution.
- May limit Planar's ability to innovate beyond Iceberg.
- Users may confuse Planar with Iceberg.

**Implementation requirements**:
- Implement Iceberg REST Catalog specification.
- Map Planar concepts to Iceberg concepts (transactions → snapshots, etc.).
- Handle Iceberg-specific features (manifest files, partition specs, etc.).
- Decide which Iceberg features to support vs reject.

### Option 4: Native Library Integration

Engines embed the Planar Rust library directly (via FFI, WASM, or language-specific bindings).

**How it works**: Planar provides a Rust crate that engines can link. For non-Rust engines, provide C FFI bindings or compile to WebAssembly.

**Advantages**:
- Best performance (no network round-trips).
- Full access to Planar internals (within API boundaries).
- Works offline or in embedded scenarios.

**Disadvantages**:
- Must build bindings for each language (Python, Java, etc.).
- Versioning is harder (library updates require engine rebuilds).
- Not all engines support native library integration.

**Implementation requirements**:
- Stabilize the Rust API.
- Build C FFI bindings.
- Build language-specific wrappers (PyO3 for Python, JNI for Java, etc.).

## Comparison Matrix

| Aspect | Direct DB | REST API | Iceberg Compat | Native Library |
|--------|-----------|----------|----------------|----------------|
| Setup complexity | Low | Medium | Medium | High |
| Runtime overhead | Low | Medium | Medium | Lowest |
| Connector effort | Low | High | Low | High |
| Schema coupling | High | Low | Medium | Medium |
| Ecosystem reach | Medium | Medium | High | Low |
| Planar control | Low | High | Low | High |

## Current Recommendation

Start with **Option 4 (Native Library)** for Rust-native engines and direct programmatic access. This is already implemented via the `planar` crate.

Add **Option 1 (Direct Database Access)** as a documented, stable interface for engines that can issue SQL queries. This requires:
1. Schema documentation and versioning.
2. Example queries for common operations.
3. Clear public/internal boundaries.

Defer **Option 2 (REST API)** and **Option 3 (Iceberg Compat)** until there is demonstrated demand. Both add operational complexity and development effort.

## Schema Stability Commitment

If we pursue direct database access, the following tables and columns would be part of the stable API:

**Public tables**:
- `tables`: `table_uuid`, `namespace`, `table_name`, `location`, `current_transaction_id`, `properties`
- `transactions`: `transaction_id`, `table_uuid`, `transaction_timestamp`, `parent_transaction_id`
- `schemas`: `schema_uuid`, `table_uuid`, `schema_version`, `valid_from_transaction_id`, `valid_to_transaction_id`
- `columns`: `column_uuid`, `schema_uuid`, `column_name`, `column_type`, `ordinal_position`, `is_nullable`
- `files`: `file_uuid`, `table_uuid`, `file_path`, `file_format`, `record_count`, `file_size_bytes`, `added_in_transaction_id`, `removed_in_transaction_id`, `partition_values`

**Internal tables** (not part of stable API):
- `table_stats`, `file_column_stats` (may change structure)
- Any future internal bookkeeping tables

**Versioning**: Schema versions will be tracked. Breaking changes will increment the major version and require migration.

## Example Queries for External Engines

### List all tables

```sql
SELECT namespace, table_name, location
FROM tables
ORDER BY namespace, table_name;
```

### Get current snapshot for a table

```sql
SELECT t.table_uuid, t.current_transaction_id, tr.transaction_timestamp
FROM tables t
JOIN transactions tr ON t.current_transaction_id = tr.transaction_id
WHERE t.namespace = ? AND t.table_name = ?;
```

### List files for current snapshot

```sql
SELECT f.file_path, f.file_format, f.record_count, f.file_size_bytes, f.partition_values
FROM files f
JOIN tables t ON f.table_uuid = t.table_uuid
WHERE t.namespace = ? AND t.table_name = ?
  AND f.added_in_transaction_id <= t.current_transaction_id
  AND (f.removed_in_transaction_id IS NULL 
       OR f.removed_in_transaction_id > t.current_transaction_id);
```

### Get schema for current snapshot

```sql
SELECT c.column_name, c.column_type, c.ordinal_position, c.is_nullable
FROM columns c
JOIN schemas s ON c.schema_uuid = s.schema_uuid
JOIN tables t ON s.table_uuid = t.table_uuid
WHERE t.namespace = ? AND t.table_name = ?
  AND s.valid_from_transaction_id <= t.current_transaction_id
  AND (s.valid_to_transaction_id IS NULL 
       OR s.valid_to_transaction_id > t.current_transaction_id)
ORDER BY c.ordinal_position;
```

## Open Questions

1. **Security model**: How do we handle authentication and authorization for direct database access? Database-level users? Row-level security?

2. **Read replicas**: Should we recommend read replicas for external engine access to avoid impacting write performance?

3. **Connection pooling**: How many connections can external engines open? Should Planar provide connection pooling guidance?

4. **Query interface abstraction**: Should we provide views that abstract the raw tables, making schema evolution easier?
