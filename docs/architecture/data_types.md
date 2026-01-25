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

## Design Principles

1. **Arrow as canonical representation**: Use Apache Arrow's type system as Planar's canonical type representation. Arrow is already used internally for `RecordBatch` operations, and it has well-defined mappings to Parquet, which simplifies implementation.

2. **Format-agnostic**: Planar types should not be tied to any single file format. Conversions to format-specific types (Parquet, Lance, Vortex) are explicit and bidirectional.

3. **Schema evolution rules**: Define which type changes are safe (backward-compatible) vs breaking. These rules enforce data integrity.

4. **Serialization stability**: The string representation of types must be stable across versions. Once a type is written to the database, its string form cannot change.

## Canonical Type System

Planar's type system is based on Arrow DataType with additional semantics:

### Primitive Types

```rust
pub enum PrimitiveType {
    /// Boolean (1 bit, 0 = false, 1 = true)
    Boolean,
    
    /// Signed 8-bit integer
    Int8,
    /// Signed 16-bit integer
    Int16,
    /// Signed 32-bit integer
    Int32,
    /// Signed 64-bit integer
    Int64,
    
    /// Unsigned 8-bit integer
    UInt8,
    /// Unsigned 16-bit integer
    UInt16,
    /// Unsigned 32-bit integer
    UInt32,
    /// Unsigned 64-bit integer
    UInt64,
    
    /// 32-bit floating point (IEEE 754)
    Float32,
    /// 64-bit floating point (IEEE 754)
    Float64,
}
```

### Decimal Types

```rust
pub struct DecimalType {
    /// Precision: total number of digits (1-38)
    pub precision: u8,
    /// Scale: number of digits after decimal point (0-precision)
    pub scale: i8,
    /// Bit width: 128 or 256
    pub bit_width: DecimalBitWidth,
}

pub enum DecimalBitWidth {
    /// 128-bit decimal (max precision 38)
    Decimal128,
    /// 256-bit decimal (max precision 76)
    Decimal256,
}
```

### String and Binary Types

```rust
pub enum StringType {
    /// UTF-8 encoded string (variable length, 32-bit offsets)
    String,
    /// UTF-8 encoded string (variable length, 64-bit offsets for >2GB strings)
    LargeString,
    /// UTF-8 encoded string (fixed length)
    FixedSizeString(i32),
}

pub enum BinaryType {
    /// Variable-length binary (32-bit offsets)
    Binary,
    /// Variable-length binary (64-bit offsets for >2GB data)
    LargeBinary,
    /// Fixed-length binary
    FixedSizeBinary(i32),
}
```

### Temporal Types

```rust
pub enum TemporalType {
    /// Date stored as days since UNIX epoch (32-bit)
    Date32,
    /// Date stored as milliseconds since UNIX epoch (64-bit)
    Date64,
    
    /// Time of day (no date component)
    Time32(TimeUnit),
    /// Time of day (no date component, 64-bit)
    Time64(TimeUnit),
    
    /// Timestamp with optional timezone
    Timestamp(TimeUnit, Option<Arc<str>>),
    
    /// Duration (time delta)
    Duration(TimeUnit),
    
    /// Calendar interval (year-month)
    IntervalYearMonth,
    /// Calendar interval (day-time)
    IntervalDayTime,
    /// Calendar interval (month-day-nano)
    IntervalMonthDayNano,
}

pub enum TimeUnit {
    /// Seconds
    Second,
    /// Milliseconds
    Millisecond,
    /// Microseconds
    Microsecond,
    /// Nanoseconds
    Nanosecond,
}
```

### Complex Types

```rust
pub enum ComplexType {
    /// List of values (variable length, 32-bit offsets)
    List(Box<Field>),
    /// List of values (variable length, 64-bit offsets)
    LargeList(Box<Field>),
    /// List of values (fixed length)
    FixedSizeList(Box<Field>, i32),
    
    /// Struct with named fields
    Struct(Vec<Field>),
    
    /// Map of key-value pairs
    Map(Box<Field>, Box<Field>, bool), // key, value, keys_sorted
    
    /// Union (tagged or dense)
    Union(Vec<Field>, UnionMode),
}

pub enum UnionMode {
    /// Sparse union (uses type_id only)
    Sparse,
    /// Dense union (uses type_id and offset)
    Dense,
}
```

### Field Definition

```rust
pub struct Field {
    /// Field name
    pub name: Arc<str>,
    /// Field data type
    pub data_type: DataType,
    /// Whether the field can contain null values
    pub nullable: bool,
    /// Optional metadata (key-value pairs)
    pub metadata: Option<HashMap<String, String>>,
}
```

### Top-Level DataType Enum

```rust
pub enum DataType {
    Primitive(PrimitiveType),
    Decimal(DecimalType),
    String(StringType),
    Binary(BinaryType),
    Temporal(TemporalType),
    Complex(ComplexType),
}
```

## String Representation

Types are serialized to strings for database storage. The format must be stable and human-readable.

### Format Specification

```
<type> ::= <primitive> | <decimal> | <string> | <binary> | <temporal> | <complex>

<primitive> ::= "boolean" | "int8" | "int16" | "int32" | "int64"
              | "uint8" | "uint16" | "uint32" | "uint64"
              | "float32" | "float64"

<decimal> ::= "decimal128(" <precision> "," <scale> ")"
            | "decimal256(" <precision> "," <scale> ")"

<string> ::= "string" | "large_string" | "fixed_string(" <size> ")"

<binary> ::= "binary" | "large_binary" | "fixed_binary(" <size> ")"

<temporal> ::= "date32" | "date64"
             | "time32(" <unit> ")" | "time64(" <unit> ")"
             | "timestamp(" <unit> ")"
             | "timestamp(" <unit> "," <timezone> ")"
             | "duration(" <unit> ")"
             | "interval_year_month" | "interval_day_time" | "interval_month_day_nano"

<unit> ::= "s" | "ms" | "us" | "ns"

<complex> ::= "list(" <field> ")"
            | "large_list(" <field> ")"
            | "fixed_list(" <field> "," <size> ")"
            | "struct(" <field> ["," <field>]* ")"
            | "map(" <key_field> "," <value_field> ")"
            | "map_sorted(" <key_field> "," <value_field> ")"
            | "union_sparse(" <field> ["," <field>]* ")"
            | "union_dense(" <field> ["," <field>]* ")"

<field> ::= <name> ":" <type> ["?"] ["{" <metadata> "}"]

<metadata> ::= <key> "=" <value> ["," <key> "=" <value>]*
```

### Examples

```
int32
decimal128(10,2)
string
timestamp(ms,UTC)
list(item:int64)
struct(id:int32,name:string?,created_at:timestamp(us))
```

### Backward Compatibility

Once a type string is written to the database, its format cannot change. If the internal representation changes, the parser must still understand old formats.

## Type Conversions

### Arrow ↔ Planar

Planar types are a subset of Arrow types. Conversion is straightforward:

```rust
impl From<arrow::datatypes::DataType> for planar::DataType {
    fn from(arrow_type: arrow::datatypes::DataType) -> Self {
        // Map Arrow types to Planar types
    }
}

impl TryFrom<planar::DataType> for arrow::datatypes::DataType {
    type Error = TypeError;
    
    fn try_from(planar_type: planar::DataType) -> Result<Self, Self::Error> {
        // Map Planar types to Arrow types
    }
}
```

### Parquet ↔ Planar

Parquet has its own type system with physical and logical types. The conversion must preserve semantics:

| Planar Type | Parquet Physical | Parquet Logical |
|-------------|------------------|-----------------|
| `int32` | `INT32` | `INT(32, signed)` |
| `int64` | `INT64` | `INT(64, signed)` |
| `decimal128(p,s)` | `FIXED_LEN_BYTE_ARRAY(16)` | `DECIMAL(p,s)` |
| `string` | `BYTE_ARRAY` | `STRING` |
| `timestamp(us,UTC)` | `INT64` | `TIMESTAMP(MICROS, true)` |
| `list(item:T)` | `list` | - |
| `struct(...)` | `group` | - |

Not all Parquet logical types have direct Planar equivalents. Unsupported types should error gracefully.

### Lance ↔ Planar

Lance uses Arrow's type system internally, so conversion is simpler. However, Lance has restrictions on certain types (e.g., no large types).

### Vortex ↔ Planar

Vortex is a compressed columnar format that also uses Arrow types. Conversion should be straightforward, but compression-specific types may need special handling.

## Schema Evolution Rules

Schema evolution changes the structure of a table over time. Type changes must follow compatibility rules to prevent data corruption.

### Safe Type Changes (Backward-Compatible)

These changes are **allowed** without data rewrite:

| From | To | Rationale |
|------|----|-----------| 
| `int8` | `int16`, `int32`, `int64` | Widening preserves values |
| `int16` | `int32`, `int64` | Widening preserves values |
| `int32` | `int64` | Widening preserves values |
| `float32` | `float64` | Widening preserves values |
| `date32` | `date64` | Date64 is more precise |
| `timestamp(s,tz)` | `timestamp(ms,tz)`, `timestamp(us,tz)`, `timestamp(ns,tz)` | Increased precision |
| `T` | `T?` (nullable) | Making a column nullable is safe |
| `decimal(p1,s)` | `decimal(p2,s)` where `p2 > p1` | Increased precision |

### Unsafe Type Changes (Backward-Incompatible)

These changes are **not allowed** without explicit data rewrite:

| From | To | Rationale |
|------|----|-----------| 
| `int64` | `int32` | Narrowing can overflow |
| `float64` | `float32` | Loss of precision |
| `string` | `int32` | Type mismatch |
| `T?` (nullable) | `T` (non-null) | Existing nulls would violate constraint |
| `decimal(p,s1)` | `decimal(p,s2)` where `s1 != s2` | Changing scale requires rewrite |
| `timestamp(ns,tz)` | `timestamp(us,tz)` | Loss of precision |

### Struct Field Changes

For struct types, these rules apply:

- **Adding a nullable field**: Safe (new field is null for existing rows)
- **Adding a non-null field**: Unsafe (no value for existing rows)
- **Removing a field**: Safe (field is ignored in reads)
- **Renaming a field**: Unsafe (breaks existing queries)
- **Reordering fields**: Unsafe (breaks positional access)

### Implementation

Schema evolution validation happens in `SqlCatalog::commit` when processing `MutationOp::UpdateSchema`:

```rust
impl SqlCatalog {
    async fn validate_schema_evolution(
        &self,
        old_schema: &Schema,
        new_schema: &SchemaSpec,
    ) -> Result<()> {
        // For each old column, check if new column exists
        for old_col in &old_schema.columns {
            if let Some(new_col) = new_schema.columns.iter().find(|c| c.name == old_col.column_name) {
                // Column exists, validate type change
                let old_type = DataType::from_str(&old_col.column_type)?;
                let new_type = DataType::from_str(&new_col.column_type)?;
                
                if !old_type.can_evolve_to(&new_type) {
                    return Err(CatalogError::InvalidSchemaEvolution(
                        format!("Cannot change {} from {} to {}", 
                            old_col.column_name, old_type, new_type)
                    ));
                }
                
                // Check nullability
                if old_col.is_nullable && !new_col.is_nullable {
                    return Err(CatalogError::InvalidSchemaEvolution(
                        format!("Cannot make nullable column {} non-nullable", old_col.column_name)
                    ));
                }
            }
            // Column removed is OK (ignored in reads)
        }
        
        // Check new columns
        for new_col in &new_schema.columns {
            if !old_schema.columns.iter().any(|c| c.column_name == new_col.name) {
                // New column added
                if !new_col.is_nullable {
                    return Err(CatalogError::InvalidSchemaEvolution(
                        format!("Cannot add non-nullable column {}", new_col.name)
                    ));
                }
            }
        }
        
        Ok(())
    }
}
```

## Statistics Storage

File column statistics store min/max values as binary blobs. With proper type handling, these can be serialized and deserialized correctly.

### Binary Encoding

Each type defines its binary encoding:

```rust
pub trait TypeEncoder {
    fn encode(&self, value: &ScalarValue) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<ScalarValue>;
}
```

For primitive types, use native byte order (little-endian):

- `int32`: 4 bytes (little-endian)
- `int64`: 8 bytes (little-endian)
- `float64`: 8 bytes (IEEE 754, little-endian)
- `string`: UTF-8 bytes
- `timestamp(us)`: 8 bytes (microseconds since epoch, little-endian)

For complex types, use a format-specific encoding (e.g., Arrow IPC format).

### Comparison Semantics

Statistics are used for query optimization (predicate pushdown). Each type defines comparison rules:

```rust
pub trait TypeComparator {
    fn compare(&self, a: &ScalarValue, b: &ScalarValue) -> Ordering;
}
```

For types with non-obvious ordering (e.g., structs, maps), comparison may not be supported.

## Implementation Plan

### Phase 1: Core Type System (MVP)

1. Define `DataType` enum with primitive, decimal, string, binary, and temporal types.
2. Implement `FromStr` and `Display` for string serialization.
3. Implement `From<arrow::datatypes::DataType>` and `TryFrom<planar::DataType>` for Arrow conversion.
4. Update `ColumnSpec` to use `DataType` instead of `String`.
5. Add migration to preserve existing string types (parse and re-serialize).

### Phase 2: Format Conversions

1. Implement Parquet ↔ Planar conversions.
2. Implement Lance ↔ Planar conversions.
3. Implement Vortex ↔ Planar conversions.
4. Add integration tests for round-trip conversions.

### Phase 3: Schema Evolution

1. Implement `DataType::can_evolve_to` method.
2. Add validation in `SqlCatalog::commit` for `UpdateSchema`.
3. Add tests for safe and unsafe schema evolution scenarios.
4. Document schema evolution rules in user-facing docs.

### Phase 4: Statistics Encoding

1. Implement `TypeEncoder` trait for all types.
2. Update `FileColumnStats` to use typed min/max values.
3. Add statistics serialization/deserialization in file format readers/writers.

### Phase 5: External Engine Types

1. Add SQL type conversions (for Spark, Trino, DuckDB).
2. Document type mapping tables for each engine.
3. Add integration tests with external engines.

## Testing Strategy

### Unit Tests

- Parse and serialize all type variants
- Validate evolution rules (safe and unsafe)
- Test Arrow conversions (bidirectional)
- Test format-specific conversions (Parquet, Lance, Vortex)

### Integration Tests

- Create tables with all type variants
- Perform schema evolution operations
- Write and read files in all formats
- Verify statistics encoding/decoding

### Compatibility Tests

- Ensure old type strings parse correctly after format changes
- Test migration from string-based types to typed `DataType`

## Open Questions

1. **Nested nullability**: Should `list(item:int32?)` allow nulls in the list items, or only null lists? Arrow supports both. Planar should decide if both are needed or if one is sufficient.

2. **Timezone handling**: Should timestamps without timezones be allowed? Some systems (e.g., Parquet) allow timezone-naive timestamps, but Arrow recommends always using UTC. Planar should document its stance.

3. **Custom types**: Should Planar support user-defined types or extensions? This adds complexity but enables use cases like UUID, JSON, or domain-specific types.

4. **Type aliases**: Should common types have aliases? For example, `bigint` as an alias for `int64`, or `varchar` for `string`. This improves usability but adds maintenance burden.

## References

- [Apache Arrow Type System](https://arrow.apache.org/docs/status.html)
- [Apache Parquet Logical Types](https://parquet.apache.org/docs/file-format/types/)
- [Apache Iceberg Type System](https://iceberg.apache.org/spec/#schemas-and-data-types)
- [Delta Lake Schema Specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format)
