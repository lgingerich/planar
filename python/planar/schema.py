from __future__ import annotations

from typing import Any, Dict

from . import _native
from .types import data_type_to_spec


class ColumnSpec:
    def __init__(self, name: str, dtype: Any) -> None:
        self._inner = _native.ColumnSpec(name, data_type_to_spec(dtype))

    def nullable(self) -> "ColumnSpec":
        self._inner.nullable()
        return self


class SchemaSpec:
    def __init__(self) -> None:
        self._inner = _native.SchemaSpec()

    def with_column(self, column: ColumnSpec) -> "SchemaSpec":
        self._inner.with_column(column._inner)
        return self


class FileSpec:
    def __init__(
        self,
        file_format: str,
        file_path: str,
        record_count: int,
        file_size_bytes: int,
        format_options: Dict[str, Any] | None = None,
    ) -> None:
        validated = _validate_format_options(file_format, format_options)
        self._inner = _native.FileSpec(
            file_format, file_path, record_count, file_size_bytes, validated
        )

    def with_partition_values(self, values: Dict[str, Any]) -> "FileSpec":
        self._inner.with_partition_values(values)
        return self


def _validate_format_options(
    file_format: str, format_options: Dict[str, Any] | None
) -> Dict[str, Any] | None:
    if format_options is None:
        return None

    if not isinstance(format_options, dict):
        raise TypeError("format_options must be a dict or None")

    format_keys = {"parquet", "lance", "vortex"}
    namespaced_keys = format_keys.intersection(format_options.keys())

    if namespaced_keys:
        if len(format_options) != 1 or file_format not in format_options:
            raise ValueError(
                "format_options must target a single file_format and match file_format"
            )
        nested = format_options[file_format]
        if not isinstance(nested, dict):
            raise TypeError("format_options for the selected format must be a dict")
        format_options = nested

    return format_options


__all__ = ["ColumnSpec", "SchemaSpec", "FileSpec"]
