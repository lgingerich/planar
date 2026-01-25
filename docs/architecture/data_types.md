# Data Type System

## Purpose

This document specifies Planar's data type system. Currently, column types are stored as free-form strings (e.g., `"bigint"`, `"double"`, `"timestamp"`), which creates ambiguity when translating between different file formats and query engines. A canonical type system with well-defined conversions is needed to support Planar's multi-format, engine-agnostic architecture.

## Motivation

Planar needs a robust type system for several reasons:

1. **Multi-format support**: Parquet, Lance, and Vortex each have their own type systems. A canonical representation enables lossless translation between formats.

2. **External engine access**: The roadmap includes Spark, Trino, and DuckDB integration. Each engine has its own type system that needs mapping to Planar's canonical types.

3. **Schema evolution**: Type changes must be validated for compatibility. For example, widening `Int32` → `Int64` is safe, but narrowing `Int64` → `Int32` can cause data loss.

4. **Statistics storage**: File column statistics store `min_value` and `max_value` as binary blobs. Proper type handling enables correct serialization and deserialization.

5. **Type validation**: Catching type errors at table creation time rather than at query time improves developer experience.

## Design Decision: Use Arrow DataType Directly

**Planar uses `arrow::datatypes::DataType` directly and stores it using Arrow IPC format.** This decision is based on:

1. **Arrow is canonical**: Battle-tested, stable type system widely adopted across the data ecosystem.

2. **Stable serialization**: Arrow IPC format is the official serialization mechanism with backward compatibility guarantees.

3. **Ecosystem compatibility**: Seamless integration with DataFusion, Parquet, Lance, and other Arrow-based libraries.

4. **Reduced maintenance**: No custom type definitions, parsers, or conversion code needed.

5. **Format conversions exist**: `arrow-parquet`, Lance, and Vortex all use Arrow types natively.

## Design Principles

1. **Use Arrow directly**: Planar's type system is Arrow's type system. Store types using Arrow IPC format.

2. **Official serialization**: Use Arrow IPC Schema format for stable, versioned type storage with backward compatibility guarantees.

3. **Build operations on types**: Focus on schema evolution validation, statistics encoding, and external engine mappings rather than reinventing type definitions.

4. **Schema evolution rules**: Define which type changes are safe (backward-compatible) vs breaking to enforce data integrity.

## What Planar Implements

Planar does not create a custom type system. Instead, it uses Arrow types and builds the following on top:

**Schema Evolution Validation** (`src/catalog/data_type.rs`)
- `can_evolve_to()` - Validates safe type changes (int32 → int64, etc.)
- Enforces nullability rules
- Prevents data-loss operations

**Statistics Encoding** (`src/catalog/data_type.rs`)
- `encode_scalar()` / `decode_scalar()` - Binary encoding for min/max values
- Uses Arrow's `ScalarValue` for in-memory representation
- Supports predicate pushdown and query optimization

**Type Storage** (`src/catalog/data_type.rs`)
- Stores types as binary using Arrow IPC Schema format
- Official stable serialization format with backward compatibility guarantees
- Provides helper functions for encoding/decoding individual DataTypes

**Format Integration** (`src/storage/file_format/*`)
- Leverages `arrow-parquet` for Parquet type conversions
- Validates Lance-compatible types
- Future: Vortex and external engine integrations

## Arrow Type System

Planar uses [`arrow::datatypes::DataType`](https://docs.rs/arrow/57.2.0/arrow/datatypes/enum.DataType.html) as its canonical type representation. Arrow provides a comprehensive type system including primitives (Int*, UInt*, Float*, Boolean), decimals, strings, temporal types (Date, Time, Timestamp, Duration), and complex nested types (List, Struct, Map, Union).

See the [Arrow DataType documentation](https://docs.rs/arrow/57.2.0/arrow/datatypes/enum.DataType.html) for the complete type reference.

### Usage Example

```rust
use arrow::datatypes::{DataType, TimeUnit};
use planar::catalog::{ColumnSpec, SchemaSpec, TableIdent};

// Create a table schema with Arrow types
let schema = SchemaSpec::new()
    .with_column(ColumnSpec {
        name: "id".to_string(),
        data_type: DataType::Int64,
        is_nullable: false,
    })
    .with_column(ColumnSpec {
        name: "created_at".to_string(),
        data_type: DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        is_nullable: false,
    });

// Create table
let table = catalog
    .create_table(
        TableIdent::new("public", "users"),
        "s3://bucket/users/".to_string(),
        schema,
        None,
    )
    .await?;
```

## Type Serialization

Planar uses **Arrow IPC Schema format** for storing types in the database. This is Arrow's official stable serialization format with backward compatibility guarantees.

### Why IPC Format?

- **Official stability**: Designed specifically for serialization, versioned and backward-compatible
- **Comprehensive**: Handles all Arrow types including complex nested structures
- **Battle-tested**: Used across the Arrow ecosystem for data exchange
- **Future-proof**: New Arrow types work automatically

### Implementation

Type serialization helpers in `src/catalog/data_type.rs`:

```rust
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::writer::IpcWriteOptions;

/// Encode a DataType to bytes using Arrow IPC format
pub fn encode_data_type(data_type: &DataType, field_name: &str) -> Result<Vec<u8>> {
    // Wrap the DataType in a Field and Schema for IPC encoding
    let field = Field::new(field_name, data_type.clone(), true);
    let schema = Schema::new(vec![field]);
    
    // Encode schema to IPC format
    let encoded = arrow_ipc::writer::schema_to_fb(&schema, &IpcWriteOptions::default());
    Ok(encoded.to_vec())
}

/// Decode a DataType from IPC bytes
pub fn decode_data_type(bytes: &[u8]) -> Result<DataType> {
    let schema = arrow_ipc::convert::fb_to_schema(bytes)?;
    
    // Extract the first field's type
    schema.field(0)
        .map(|f| f.data_type().clone())
        .ok_or_else(|| CatalogError::InvalidArgument("Empty schema".into()))
}
```

### Database Storage

Column types are stored as binary blobs in the `columns` table:

**Schema:**
```sql
CREATE TABLE columns (
    column_uuid BLOB NOT NULL PRIMARY KEY,
    schema_uuid BLOB NOT NULL,
    column_name TEXT NOT NULL,
    column_type BLOB NOT NULL,  -- IPC-encoded DataType
    ordinal_position INTEGER NOT NULL,
    is_nullable BOOLEAN NOT NULL,
    FOREIGN KEY (schema_uuid) REFERENCES schemas(schema_uuid)
);
```

**Usage:**
```rust
use planar::catalog::data_type::{encode_data_type, decode_data_type};

// When creating a column
let data_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
let encoded = encode_data_type(&data_type, "created_at")?;

sqlx::query("INSERT INTO columns (column_type, ...) VALUES (?1, ...)")
    .bind(&encoded)
    .execute(&pool)
    .await?;

// When reading a column
let encoded: Vec<u8> = row.get("column_type");
let data_type = decode_data_type(&encoded)?;
```

## Format Integration

Since Planar uses Arrow types natively, format conversions leverage existing libraries.

### Parquet

The `arrow-parquet` crate handles all type conversions automatically. No custom conversion logic needed.

### Lance

Lance uses Arrow types natively but has restrictions:
- No `Large*` types (LargeList, LargeUtf8, LargeBinary)
- Limited nested type support

Planar will validate schema compatibility when writing Lance files.

### Vortex

Vortex uses Arrow types with compression. Integration will use Vortex's Arrow-compatible API.

### External Engines (Future)

Engine-specific connectors will map Arrow types to SQL types (Spark, Trino, DuckDB) when those integrations are added.

## Schema Evolution Rules

Schema evolution changes the structure of a table over time. Type changes must follow compatibility rules to prevent data corruption.

### Safe Type Changes (Backward-Compatible)

These changes are **allowed** without data rewrite:

| From | To | Rationale |
|------|----|-----------| 
| `Int8` | `Int16`, `Int32`, `Int64` | Widening preserves values |
| `Int16` | `Int32`, `Int64` | Widening preserves values |
| `Int32` | `Int64` | Widening preserves values |
| `UInt8` | `UInt16`, `UInt32`, `UInt64` | Widening preserves values |
| `UInt16` | `UInt32`, `UInt64` | Widening preserves values |
| `UInt32` | `UInt64` | Widening preserves values |
| `Float32` | `Float64` | Widening preserves values |
| `Date32` | `Date64` | Date64 is more precise |
| `Timestamp(Second, tz)` | `Timestamp(Millisecond\|Microsecond\|Nanosecond, tz)` | Increased precision |
| `Timestamp(Millisecond, tz)` | `Timestamp(Microsecond\|Nanosecond, tz)` | Increased precision |
| `Timestamp(Microsecond, tz)` | `Timestamp(Nanosecond, tz)` | Increased precision |
| Non-nullable field | Nullable field | Making a column nullable is safe |
| `Decimal128(p1, s)` | `Decimal128(p2, s)` where `p2 > p1` | Increased precision |

### Unsafe Type Changes (Backward-Incompatible)

These changes are **not allowed** without explicit data rewrite:

| From | To | Rationale |
|------|----|-----------| 
| `Int64` | `Int32` | Narrowing can overflow |
| `Float64` | `Float32` | Loss of precision |
| `Utf8` | `Int32` | Type mismatch |
| Nullable field | Non-nullable field | Existing nulls would violate constraint |
| `Decimal128(p, s1)` | `Decimal128(p, s2)` where `s1 != s2` | Changing scale requires rewrite |
| `Timestamp(Nanosecond, tz)` | `Timestamp(Microsecond\|Millisecond\|Second, tz)` | Loss of precision |
| `Utf8` | `LargeUtf8` | Offset type change requires rewrite |


### Implementation

Implemented as `can_evolve_to()` in `src/catalog/data_type.rs`. Validates type widening for integers, floats, timestamps, and decimals. Called in `SqlCatalog::commit()` when processing `MutationOp::UpdateSchema`.

## Statistics Storage

File column statistics store min/max values as binary blobs. Planar uses Arrow's `ScalarValue` for in-memory representation.

**Implementation** (`src/catalog/data_type.rs`):
- `encode_scalar()` - Encode ScalarValue to bytes (primitives use little-endian, strings use UTF-8)
- `decode_scalar()` - Decode bytes to ScalarValue given a DataType
- Arrow's `ScalarValue` implements `PartialOrd` for comparisons and predicate pushdown

## Implementation Plan

### Phase 1: Integrate Arrow Types (MVP)
- Change `ColumnSpec` to use `arrow::datatypes::DataType`
- Migrate database `column_type` from TEXT to BLOB
- Implement `encode_data_type()` / `decode_data_type()` using Arrow IPC format in `src/catalog/data_type.rs`
- Update `SqlCatalog` to serialize/deserialize types using IPC

### Phase 2: Schema Evolution Validation
- Implement `can_evolve_to()` function for safe type changes
- Add validation in `SqlCatalog::commit()` for `MutationOp::UpdateSchema`
- Enforce nullability rules

### Phase 3: Statistics Encoding
- Implement `encode_scalar()` / `decode_scalar()` for ScalarValue
- Update file format readers to populate statistics
- Store in `FileColumnStats` table

### Phase 4: Format Integration
- Validate Lance-compatible types (no Large* types)
- Document Parquet/Lance/Vortex integration

### Phase 5: External Engines (Future)
- Build type mapping for Spark, Trino, DuckDB
- Implement catalog translation layers


## References

- [Arrow DataType Documentation](https://docs.rs/arrow/57.2.0/arrow/datatypes/enum.DataType.html) - Complete Arrow type reference
- [Arrow Type System Specification](https://arrow.apache.org/docs/format/Columnar.html#schema-message) - Official Arrow specification
- [arrow-parquet Type Conversions](https://docs.rs/parquet/latest/parquet/) - Parquet integration
- [Apache Iceberg Type System](https://iceberg.apache.org/spec/#schemas-and-data-types) - Similar approach using standard types
- [Delta Lake Schema Specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format) - Alternative approach with JSON schemas
