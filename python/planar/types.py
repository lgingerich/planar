from __future__ import annotations

from typing import Any, Dict

import pyarrow as pa


def _field_to_spec(field: pa.Field) -> Dict[str, Any]:
    return {
        "name": field.name,
        "nullable": field.nullable,
        "dtype": data_type_to_spec(field.type),
    }


def _field_from_spec(spec: Dict[str, Any]) -> pa.Field:
    return pa.field(
        spec["name"],
        spec_to_data_type(spec["dtype"]),
        nullable=spec["nullable"],
    )


def data_type_to_spec(dtype: pa.DataType) -> Dict[str, Any]:
    if pa.types.is_null(dtype):
        return {"kind": "null"}
    if pa.types.is_boolean(dtype):
        return {"kind": "bool"}

    if pa.types.is_int8(dtype):
        return {"kind": "int8"}
    if pa.types.is_int16(dtype):
        return {"kind": "int16"}
    if pa.types.is_int32(dtype):
        return {"kind": "int32"}
    if pa.types.is_int64(dtype):
        return {"kind": "int64"}

    if pa.types.is_uint8(dtype):
        return {"kind": "uint8"}
    if pa.types.is_uint16(dtype):
        return {"kind": "uint16"}
    if pa.types.is_uint32(dtype):
        return {"kind": "uint32"}
    if pa.types.is_uint64(dtype):
        return {"kind": "uint64"}

    if pa.types.is_float32(dtype):
        return {"kind": "float32"}
    if pa.types.is_float64(dtype):
        return {"kind": "float64"}

    if pa.types.is_string(dtype):
        return {"kind": "string"}
    if pa.types.is_large_string(dtype):
        return {"kind": "large_string"}

    if pa.types.is_binary(dtype):
        return {"kind": "binary"}
    if pa.types.is_large_binary(dtype):
        return {"kind": "large_binary"}
    if pa.types.is_fixed_size_binary(dtype):
        return {"kind": "fixed_size_binary", "byte_width": dtype.byte_width}

    if pa.types.is_date32(dtype):
        return {"kind": "date32"}
    if pa.types.is_date64(dtype):
        return {"kind": "date64"}

    if pa.types.is_time32(dtype):
        return {"kind": "time32", "unit": dtype.unit}
    if pa.types.is_time64(dtype):
        return {"kind": "time64", "unit": dtype.unit}

    if pa.types.is_duration(dtype):
        return {"kind": "duration", "unit": dtype.unit}

    if pa.types.is_timestamp(dtype):
        return {"kind": "timestamp", "unit": dtype.unit, "tz": dtype.tz}

    if pa.types.is_decimal(dtype):
        kind = "decimal128" if dtype.bit_width == 128 else "decimal256"
        return {"kind": kind, "precision": dtype.precision, "scale": dtype.scale}

    if pa.types.is_list(dtype):
        return {"kind": "list", "field": _field_to_spec(dtype.value_field)}
    if pa.types.is_large_list(dtype):
        return {"kind": "large_list", "field": _field_to_spec(dtype.value_field)}
    if pa.types.is_fixed_size_list(dtype):
        return {
            "kind": "fixed_size_list",
            "field": _field_to_spec(dtype.value_field),
            "size": dtype.list_size,
        }

    if pa.types.is_struct(dtype):
        return {
            "kind": "struct",
            "fields": [_field_to_spec(field) for field in dtype],
        }

    raise ValueError(f"Unsupported PyArrow DataType: {dtype}")


def spec_to_data_type(spec: Dict[str, Any]) -> pa.DataType:
    kind = spec["kind"]
    if kind == "null":
        return pa.null()
    if kind == "bool":
        return pa.bool_()

    if kind == "int8":
        return pa.int8()
    if kind == "int16":
        return pa.int16()
    if kind == "int32":
        return pa.int32()
    if kind == "int64":
        return pa.int64()

    if kind == "uint8":
        return pa.uint8()
    if kind == "uint16":
        return pa.uint16()
    if kind == "uint32":
        return pa.uint32()
    if kind == "uint64":
        return pa.uint64()

    if kind == "float32":
        return pa.float32()
    if kind == "float64":
        return pa.float64()

    if kind == "string":
        return pa.string()
    if kind == "large_string":
        return pa.large_string()

    if kind == "binary":
        return pa.binary()
    if kind == "large_binary":
        return pa.large_binary()
    if kind == "fixed_size_binary":
        return pa.binary(spec["byte_width"])

    if kind == "date32":
        return pa.date32()
    if kind == "date64":
        return pa.date64()

    if kind == "time32":
        return pa.time32(spec["unit"])
    if kind == "time64":
        return pa.time64(spec["unit"])

    if kind == "duration":
        return pa.duration(spec["unit"])

    if kind == "timestamp":
        return pa.timestamp(spec["unit"], tz=spec.get("tz"))

    if kind == "decimal128":
        return pa.decimal128(spec["precision"], spec["scale"])
    if kind == "decimal256":
        return pa.decimal256(spec["precision"], spec["scale"])

    if kind == "list":
        return pa.list_(_field_from_spec(spec["field"]))
    if kind == "large_list":
        return pa.large_list(_field_from_spec(spec["field"]))
    if kind == "fixed_size_list":
        fixed_size_list = getattr(pa, "fixed_size_list", None)
        if fixed_size_list is not None:
            return fixed_size_list(_field_from_spec(spec["field"]), spec["size"])
        return pa.list_(_field_from_spec(spec["field"]), list_size=spec["size"])

    if kind == "struct":
        return pa.struct([_field_from_spec(field) for field in spec["fields"]])

    raise ValueError(f"Unsupported dtype spec kind: {kind}")
