//! Utilities for encoding Arrow types and validating schema evolution rules.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow_ipc::convert::{IpcSchemaEncoder, fb_to_schema};
use arrow_ipc::root_as_schema;

use crate::catalog::{CatalogError, Result};

/// Encodes an Arrow [`DataType`] into bytes using Arrow IPC schema encoding.
pub fn encode_data_type(data_type: &DataType) -> Result<Vec<u8>> {
    // Wrap in a placeholder Field+Schema for IPC encoding
    // The field name "_" indicates it's just a wrapper and will be discarded
    let field = Field::new("_", data_type.clone(), true);
    let schema = Schema::new(vec![field]);

    // Encode to IPC format using FlatBuffers
    let fbb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    Ok(fbb.finished_data().to_vec())
}

/// Decodes an Arrow [`DataType`] from bytes created by [`encode_data_type`].
pub fn decode_data_type(bytes: &[u8]) -> Result<DataType> {
    // Deserialize from FlatBuffer format
    let ipc_schema = root_as_schema(bytes)
        .map_err(|e| ArrowError::IpcError(format!("Invalid FlatBuffer schema: {}", e)))?;
    let schema = fb_to_schema(ipc_schema);

    // Extract the DataType from the first (and only) field
    let fields = schema.fields();
    match fields.len() {
        0 => Err(CatalogError::InvalidArgument(
            "Invalid encoded DataType: schema has no fields".into(),
        )),
        1 => Ok(fields[0].data_type().clone()),
        n => Err(CatalogError::InvalidArgument(format!(
            "Invalid encoded DataType: expected 1 field, found {}",
            n
        ))),
    }
}

/// Check if a DataType can safely evolve to another DataType.
///
/// This function validates schema evolution rules to prevent data loss or corruption.
/// Safe evolutions include widening numeric types, increasing timestamp precision,
/// and other backward-compatible changes.
///
/// # Examples
///
/// ```
/// use arrow::datatypes::DataType;
/// use planar::catalog::data_type::can_evolve_to;
///
/// // Safe: widening integer types
/// assert!(can_evolve_to(&DataType::Int32, &DataType::Int64));
///
/// // Unsafe: narrowing integer types
/// assert!(!can_evolve_to(&DataType::Int64, &DataType::Int32));
///
/// // Unsafe: incompatible types
/// assert!(!can_evolve_to(&DataType::Utf8, &DataType::Int64));
/// ```
pub fn can_evolve_to(from_type: &DataType, to_type: &DataType) -> bool {
    // Same type is always allowed
    if from_type == to_type {
        return true;
    }

    match (from_type, to_type) {
        // Integer widening (signed)
        (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64) => true,
        (DataType::Int16, DataType::Int32 | DataType::Int64) => true,
        (DataType::Int32, DataType::Int64) => true,

        // Integer widening (unsigned)
        (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64) => true,
        (DataType::UInt16, DataType::UInt32 | DataType::UInt64) => true,
        (DataType::UInt32, DataType::UInt64) => true,

        // Float widening
        (DataType::Float32, DataType::Float64) => true,

        // Date widening (Date32 is days, Date64 is milliseconds - more precise)
        (DataType::Date32, DataType::Date64) => true,

        // Timestamp precision increase (must have same timezone)
        (DataType::Timestamp(from_unit, from_tz), DataType::Timestamp(to_unit, to_tz)) => {
            // Timezones must match exactly
            if from_tz != to_tz {
                return false;
            }

            // Check if precision is increasing
            matches!(
                (from_unit, to_unit),
                (
                    TimeUnit::Second,
                    TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond
                ) | (
                    TimeUnit::Millisecond,
                    TimeUnit::Microsecond | TimeUnit::Nanosecond
                ) | (TimeUnit::Microsecond, TimeUnit::Nanosecond)
            )
        }

        // Decimal precision increase (scale must stay the same)
        (
            DataType::Decimal128(from_precision, from_scale),
            DataType::Decimal128(to_precision, to_scale),
        ) => from_scale == to_scale && to_precision > from_precision,

        (
            DataType::Decimal256(from_precision, from_scale),
            DataType::Decimal256(to_precision, to_scale),
        ) => from_scale == to_scale && to_precision > from_precision,

        // No other evolutions are safe
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn roundtrip(data_type: DataType) {
        let encoded = encode_data_type(&data_type).expect("encode should succeed");
        let decoded = decode_data_type(&encoded).expect("decode should succeed");
        assert_eq!(data_type, decoded, "round-trip should preserve type");
    }

    #[test]
    fn test_primitive_types() {
        roundtrip(DataType::Boolean);
        roundtrip(DataType::Int8);
        roundtrip(DataType::Int16);
        roundtrip(DataType::Int32);
        roundtrip(DataType::Int64);
        roundtrip(DataType::UInt8);
        roundtrip(DataType::UInt16);
        roundtrip(DataType::UInt32);
        roundtrip(DataType::UInt64);
        roundtrip(DataType::Float32);
        roundtrip(DataType::Float64);
    }

    #[test]
    fn test_string_types() {
        roundtrip(DataType::Utf8);
        roundtrip(DataType::LargeUtf8);
        roundtrip(DataType::Binary);
        roundtrip(DataType::LargeBinary);
        roundtrip(DataType::FixedSizeBinary(16));
    }

    #[test]
    fn test_temporal_types() {
        roundtrip(DataType::Date32);
        roundtrip(DataType::Date64);
        roundtrip(DataType::Time32(TimeUnit::Second));
        roundtrip(DataType::Time32(TimeUnit::Millisecond));
        roundtrip(DataType::Time64(TimeUnit::Microsecond));
        roundtrip(DataType::Time64(TimeUnit::Nanosecond));
        roundtrip(DataType::Duration(TimeUnit::Second));
        roundtrip(DataType::Duration(TimeUnit::Millisecond));
        roundtrip(DataType::Duration(TimeUnit::Microsecond));
        roundtrip(DataType::Duration(TimeUnit::Nanosecond));
    }

    #[test]
    fn test_timestamp_types() {
        // Without timezone
        roundtrip(DataType::Timestamp(TimeUnit::Second, None));
        roundtrip(DataType::Timestamp(TimeUnit::Millisecond, None));
        roundtrip(DataType::Timestamp(TimeUnit::Microsecond, None));
        roundtrip(DataType::Timestamp(TimeUnit::Nanosecond, None));

        // With timezone
        roundtrip(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
        roundtrip(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("America/New_York".into()),
        ));
        roundtrip(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("+00:00".into()),
        ));
    }

    #[test]
    fn test_decimal_types() {
        roundtrip(DataType::Decimal128(10, 2));
        roundtrip(DataType::Decimal128(38, 10));
        roundtrip(DataType::Decimal256(76, 20));
    }

    #[test]
    fn test_list_types() {
        roundtrip(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))));
        roundtrip(DataType::LargeList(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            false,
        ))));
        roundtrip(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float64, true)),
            5,
        ));
    }

    #[test]
    fn test_struct_type() {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ];
        roundtrip(DataType::Struct(fields.into()));
    }

    #[test]
    fn test_nested_types() {
        // List of structs
        let struct_type = DataType::Struct(
            vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
            ]
            .into(),
        );
        roundtrip(DataType::List(Arc::new(Field::new(
            "point",
            struct_type,
            true,
        ))));

        // Struct with list
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("values", list_type, true),
        ];
        roundtrip(DataType::Struct(fields.into()));
    }

    #[test]
    fn test_decode_empty_bytes() {
        let result = decode_data_type(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("arrow error"));
    }

    #[test]
    fn test_decode_invalid_bytes() {
        let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];
        let result = decode_data_type(&garbage);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("arrow error"));
    }

    #[test]
    fn test_encoded_size() {
        // Basic types should be relatively small
        let int32_encoded = encode_data_type(&DataType::Int32).unwrap();
        assert!(
            int32_encoded.len() < 200,
            "Int32 encoding should be compact"
        );

        let timestamp_encoded = encode_data_type(&DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ))
        .unwrap();
        assert!(
            timestamp_encoded.len() < 300,
            "Timestamp with timezone should be reasonably compact"
        );
    }
}

#[cfg(test)]
mod evolution_tests {
    use super::*;

    // ========================================================================
    // Same type evolutions (always allowed)
    // ========================================================================

    #[test]
    fn test_same_type_allowed() {
        assert!(can_evolve_to(&DataType::Int32, &DataType::Int32));
        assert!(can_evolve_to(&DataType::Utf8, &DataType::Utf8));
        assert!(can_evolve_to(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ));
    }

    // ========================================================================
    // Integer evolution tests
    // ========================================================================

    #[test]
    fn test_signed_integer_widening_safe() {
        // Int8 widening
        assert!(can_evolve_to(&DataType::Int8, &DataType::Int16));
        assert!(can_evolve_to(&DataType::Int8, &DataType::Int32));
        assert!(can_evolve_to(&DataType::Int8, &DataType::Int64));

        // Int16 widening
        assert!(can_evolve_to(&DataType::Int16, &DataType::Int32));
        assert!(can_evolve_to(&DataType::Int16, &DataType::Int64));

        // Int32 widening
        assert!(can_evolve_to(&DataType::Int32, &DataType::Int64));
    }

    #[test]
    fn test_signed_integer_narrowing_unsafe() {
        // Int64 narrowing
        assert!(!can_evolve_to(&DataType::Int64, &DataType::Int32));
        assert!(!can_evolve_to(&DataType::Int64, &DataType::Int16));
        assert!(!can_evolve_to(&DataType::Int64, &DataType::Int8));

        // Int32 narrowing
        assert!(!can_evolve_to(&DataType::Int32, &DataType::Int16));
        assert!(!can_evolve_to(&DataType::Int32, &DataType::Int8));

        // Int16 narrowing
        assert!(!can_evolve_to(&DataType::Int16, &DataType::Int8));
    }

    #[test]
    fn test_unsigned_integer_widening_safe() {
        // UInt8 widening
        assert!(can_evolve_to(&DataType::UInt8, &DataType::UInt16));
        assert!(can_evolve_to(&DataType::UInt8, &DataType::UInt32));
        assert!(can_evolve_to(&DataType::UInt8, &DataType::UInt64));

        // UInt16 widening
        assert!(can_evolve_to(&DataType::UInt16, &DataType::UInt32));
        assert!(can_evolve_to(&DataType::UInt16, &DataType::UInt64));

        // UInt32 widening
        assert!(can_evolve_to(&DataType::UInt32, &DataType::UInt64));
    }

    #[test]
    fn test_unsigned_integer_narrowing_unsafe() {
        // UInt64 narrowing
        assert!(!can_evolve_to(&DataType::UInt64, &DataType::UInt32));
        assert!(!can_evolve_to(&DataType::UInt64, &DataType::UInt16));
        assert!(!can_evolve_to(&DataType::UInt64, &DataType::UInt8));

        // UInt32 narrowing
        assert!(!can_evolve_to(&DataType::UInt32, &DataType::UInt16));
        assert!(!can_evolve_to(&DataType::UInt32, &DataType::UInt8));

        // UInt16 narrowing
        assert!(!can_evolve_to(&DataType::UInt16, &DataType::UInt8));
    }

    #[test]
    fn test_mixed_signedness_unsafe() {
        // Signed to unsigned
        assert!(!can_evolve_to(&DataType::Int32, &DataType::UInt32));
        assert!(!can_evolve_to(&DataType::Int64, &DataType::UInt64));

        // Unsigned to signed
        assert!(!can_evolve_to(&DataType::UInt32, &DataType::Int32));
        assert!(!can_evolve_to(&DataType::UInt64, &DataType::Int64));
    }

    // ========================================================================
    // Float evolution tests
    // ========================================================================

    #[test]
    fn test_float_widening_safe() {
        assert!(can_evolve_to(&DataType::Float32, &DataType::Float64));
    }

    #[test]
    fn test_float_narrowing_unsafe() {
        assert!(!can_evolve_to(&DataType::Float64, &DataType::Float32));
    }

    #[test]
    fn test_integer_to_float_unsafe() {
        // Even if float has more precision, this is a type change
        assert!(!can_evolve_to(&DataType::Int32, &DataType::Float32));
        assert!(!can_evolve_to(&DataType::Int64, &DataType::Float64));
        assert!(!can_evolve_to(&DataType::Float32, &DataType::Int32));
    }

    // ========================================================================
    // Date evolution tests
    // ========================================================================

    #[test]
    fn test_date_widening_safe() {
        // Date32 (days since epoch) to Date64 (milliseconds since epoch) is safe
        assert!(can_evolve_to(&DataType::Date32, &DataType::Date64));
    }

    #[test]
    fn test_date_narrowing_unsafe() {
        // Date64 to Date32 loses precision
        assert!(!can_evolve_to(&DataType::Date64, &DataType::Date32));
    }

    // ========================================================================
    // Timestamp evolution tests
    // ========================================================================

    #[test]
    fn test_timestamp_precision_increase_safe() {
        let tz = Some("UTC".into());

        // Second to higher precision
        assert!(can_evolve_to(
            &DataType::Timestamp(TimeUnit::Second, tz.clone()),
            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone())
        ));
        assert!(can_evolve_to(
            &DataType::Timestamp(TimeUnit::Second, tz.clone()),
            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone())
        ));
        assert!(can_evolve_to(
            &DataType::Timestamp(TimeUnit::Second, tz.clone()),
            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())
        ));

        // Millisecond to higher precision
        assert!(can_evolve_to(
            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone())
        ));
        assert!(can_evolve_to(
            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())
        ));

        // Microsecond to higher precision
        assert!(can_evolve_to(
            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())
        ));
    }

    #[test]
    fn test_timestamp_precision_decrease_unsafe() {
        let tz = Some("UTC".into());

        // Nanosecond to lower precision
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone())
        ));
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone())
        ));
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Second, tz.clone())
        ));

        // Microsecond to lower precision
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone())
        ));
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Second, tz.clone())
        ));

        // Millisecond to lower precision
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
            &DataType::Timestamp(TimeUnit::Second, tz.clone())
        ));
    }

    #[test]
    fn test_timestamp_timezone_change_unsafe() {
        // Different timezones not allowed
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into()))
        ));

        // None to Some not allowed
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ));

        // Some to None not allowed
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
    }

    #[test]
    fn test_timestamp_without_timezone() {
        // Precision increase without timezone is still allowed
        assert!(can_evolve_to(
            &DataType::Timestamp(TimeUnit::Second, None),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        ));
    }

    // ========================================================================
    // Decimal evolution tests
    // ========================================================================

    #[test]
    fn test_decimal_precision_increase_safe() {
        // Decimal128: increasing precision with same scale is safe
        assert!(can_evolve_to(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(20, 2)
        ));
        assert!(can_evolve_to(
            &DataType::Decimal128(10, 5),
            &DataType::Decimal128(38, 5)
        ));

        // Decimal256: increasing precision with same scale is safe
        assert!(can_evolve_to(
            &DataType::Decimal256(40, 2),
            &DataType::Decimal256(60, 2)
        ));
    }

    #[test]
    fn test_decimal_precision_decrease_unsafe() {
        // Decreasing precision can cause overflow
        assert!(!can_evolve_to(
            &DataType::Decimal128(20, 2),
            &DataType::Decimal128(10, 2)
        ));
    }

    #[test]
    fn test_decimal_scale_change_unsafe() {
        // Changing scale requires data rewrite
        assert!(!can_evolve_to(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(10, 3)
        ));
        assert!(!can_evolve_to(
            &DataType::Decimal128(10, 3),
            &DataType::Decimal128(10, 2)
        ));

        // Even with increased precision, scale change not allowed
        assert!(!can_evolve_to(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(20, 3)
        ));
    }

    #[test]
    fn test_decimal128_to_decimal256_unsafe() {
        // Different decimal types not compatible
        assert!(!can_evolve_to(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal256(10, 2)
        ));
        assert!(!can_evolve_to(
            &DataType::Decimal256(10, 2),
            &DataType::Decimal128(10, 2)
        ));
    }

    // ========================================================================
    // String and binary types
    // ========================================================================

    #[test]
    fn test_string_types_no_evolution() {
        // Utf8 to LargeUtf8 requires offset type change (not safe without rewrite)
        assert!(!can_evolve_to(&DataType::Utf8, &DataType::LargeUtf8));
        assert!(!can_evolve_to(&DataType::LargeUtf8, &DataType::Utf8));

        // Binary types similar
        assert!(!can_evolve_to(&DataType::Binary, &DataType::LargeBinary));
        assert!(!can_evolve_to(&DataType::LargeBinary, &DataType::Binary));
    }

    // ========================================================================
    // Type mismatches (always unsafe)
    // ========================================================================

    #[test]
    fn test_incompatible_types() {
        // String to numeric
        assert!(!can_evolve_to(&DataType::Utf8, &DataType::Int32));
        assert!(!can_evolve_to(&DataType::Utf8, &DataType::Float64));

        // Numeric to string
        assert!(!can_evolve_to(&DataType::Int64, &DataType::Utf8));

        // Boolean to anything else
        assert!(!can_evolve_to(&DataType::Boolean, &DataType::Int32));
        assert!(!can_evolve_to(&DataType::Int32, &DataType::Boolean));

        // Date to timestamp
        assert!(!can_evolve_to(
            &DataType::Date32,
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        ));
        assert!(!can_evolve_to(
            &DataType::Date64,
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        ));

        // Timestamp to date
        assert!(!can_evolve_to(
            &DataType::Timestamp(TimeUnit::Millisecond, None),
            &DataType::Date64
        ));
    }

    #[test]
    fn test_complex_types_no_evolution() {
        use std::sync::Arc;

        // List types
        let list_int32 = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_int64 = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        assert!(!can_evolve_to(&list_int32, &list_int64));

        // Struct types
        let struct1 = DataType::Struct(vec![Field::new("id", DataType::Int32, false)].into());
        let struct2 = DataType::Struct(vec![Field::new("id", DataType::Int64, false)].into());
        assert!(!can_evolve_to(&struct1, &struct2));
    }
}
