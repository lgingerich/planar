use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow_ipc::convert::{fb_to_schema, IpcSchemaEncoder};
use arrow_ipc::root_as_schema;

use crate::catalog::{CatalogError, Result};

/// Encode a DataType to bytes using Arrow IPC format
pub fn encode_data_type(data_type: &DataType) -> Result<Vec<u8>> {
    // Wrap in a placeholder Field+Schema for IPC encoding
    // The field name "_" indicates it's just a wrapper and will be discarded
    let field = Field::new("_", data_type.clone(), true);
    let schema = Schema::new(vec![field]);
    
    // Encode to IPC format using FlatBuffers
    let fbb = IpcSchemaEncoder::new().schema_to_fb(&schema);
    Ok(fbb.finished_data().to_vec())
}

/// Decode a DataType from bytes using Arrow IPC format
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
        n => Err(CatalogError::InvalidArgument(
            format!("Invalid encoded DataType: expected 1 field, found {}", n),
        )),
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
        roundtrip(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
        roundtrip(DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())));
        roundtrip(DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())));
    }

    #[test]
    fn test_decimal_types() {
        roundtrip(DataType::Decimal128(10, 2));
        roundtrip(DataType::Decimal128(38, 10));
        roundtrip(DataType::Decimal256(76, 20));
    }

    #[test]
    fn test_list_types() {
        roundtrip(DataType::List(Arc::new(Field::new("item", DataType::Int32, true))));
        roundtrip(DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))));
        roundtrip(DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 5));
    }

    #[test]
    fn test_struct_type() {
        let fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        ];
        roundtrip(DataType::Struct(fields.into()));
    }

    #[test]
    fn test_nested_types() {
        // List of structs
        let struct_type = DataType::Struct(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ].into());
        roundtrip(DataType::List(Arc::new(Field::new("point", struct_type, true))));
        
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
        assert!(int32_encoded.len() < 200, "Int32 encoding should be compact");
        
        let timestamp_encoded = encode_data_type(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        ).unwrap();
        assert!(timestamp_encoded.len() < 300, "Timestamp with timezone should be reasonably compact");
    }
}