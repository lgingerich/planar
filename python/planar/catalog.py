from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from . import _native
from .schema import FileSpec, SchemaSpec
from .types import spec_to_data_type


class TableIdent:
    def __init__(self, namespace: str, name: str) -> None:
        self.namespace = namespace
        self.name = name
        self._inner = _native.TableIdent(namespace, name)


@dataclass(frozen=True)
class ColumnView:
    column_uuid: str
    schema_uuid: str
    column_name: str
    column_type: Any
    ordinal_position: int
    is_nullable: bool


@dataclass(frozen=True)
class SchemaView:
    schema_uuid: str
    table_uuid: str
    schema_version: int
    valid_from_transaction_id: str
    valid_to_transaction_id: Optional[str]
    created_at: str
    columns: List[ColumnView]


@dataclass(frozen=True)
class FileView:
    file_uuid: str
    table_uuid: str
    file_format: str
    file_path: str
    record_count: int
    file_size_bytes: int
    added_in_transaction_id: str
    removed_in_transaction_id: Optional[str]
    partition_values: Optional[Dict[str, Any]]
    format_options: Optional[Dict[str, Any]]


@dataclass(frozen=True)
class TableStats:
    table_uuid: str
    transaction_id: str
    record_count: int
    file_size_bytes: int
    file_count: int
    last_updated: str


@dataclass(frozen=True)
class TableView:
    ident: TableIdent
    table_uuid: str
    transaction_id: str
    schema: SchemaView
    files: List[FileView]
    properties: Dict[str, Any]
    stats: Optional[TableStats]


@dataclass(frozen=True)
class TableDelta:
    from_transaction_id: str
    to_transaction_id: str
    added_files: List[FileView]
    removed_files: List[FileView]
    new_schema: Optional[SchemaView]
    new_properties: Optional[Dict[str, Any]]


@dataclass(frozen=True)
class CommitResult:
    transaction_id: str
    table_view: Optional[TableView]


def _column_from_dict(raw: Dict[str, Any]) -> ColumnView:
    return ColumnView(
        column_uuid=raw["column_uuid"],
        schema_uuid=raw["schema_uuid"],
        column_name=raw["column_name"],
        column_type=spec_to_data_type(raw["column_type"]),
        ordinal_position=raw["ordinal_position"],
        is_nullable=raw["is_nullable"],
    )


def _schema_from_dict(raw: Dict[str, Any]) -> SchemaView:
    return SchemaView(
        schema_uuid=raw["schema_uuid"],
        table_uuid=raw["table_uuid"],
        schema_version=raw["schema_version"],
        valid_from_transaction_id=raw["valid_from_transaction_id"],
        valid_to_transaction_id=raw["valid_to_transaction_id"],
        created_at=raw["created_at"],
        columns=[_column_from_dict(col) for col in raw["columns"]],
    )


def _file_from_dict(raw: Dict[str, Any]) -> FileView:
    return FileView(
        file_uuid=raw["file_uuid"],
        table_uuid=raw["table_uuid"],
        file_format=raw["file_format"],
        file_path=raw["file_path"],
        record_count=raw["record_count"],
        file_size_bytes=raw["file_size_bytes"],
        added_in_transaction_id=raw["added_in_transaction_id"],
        removed_in_transaction_id=raw["removed_in_transaction_id"],
        partition_values=raw["partition_values"],
        format_options=raw.get("format_options"),
    )


def _stats_from_dict(raw: Dict[str, Any]) -> TableStats:
    return TableStats(
        table_uuid=raw["table_uuid"],
        transaction_id=raw["transaction_id"],
        record_count=raw["record_count"],
        file_size_bytes=raw["file_size_bytes"],
        file_count=raw["file_count"],
        last_updated=raw["last_updated"],
    )


def _table_view_from_dict(raw: Dict[str, Any]) -> TableView:
    ident_raw = raw["ident"]
    return TableView(
        ident=TableIdent(ident_raw["namespace"], ident_raw["name"]),
        table_uuid=raw["table_uuid"],
        transaction_id=raw["transaction_id"],
        schema=_schema_from_dict(raw["schema"]),
        files=[_file_from_dict(item) for item in raw["files"]],
        properties=raw["properties"],
        stats=_stats_from_dict(raw["stats"]) if raw["stats"] else None,
    )


def _table_delta_from_dict(raw: Dict[str, Any]) -> TableDelta:
    return TableDelta(
        from_transaction_id=raw["from_transaction_id"],
        to_transaction_id=raw["to_transaction_id"],
        added_files=[_file_from_dict(item) for item in raw["added_files"]],
        removed_files=[_file_from_dict(item) for item in raw["removed_files"]],
        new_schema=_schema_from_dict(raw["new_schema"]) if raw["new_schema"] else None,
        new_properties=raw["new_properties"],
    )


def _commit_result_from_dict(raw: Dict[str, Any]) -> CommitResult:
    return CommitResult(
        transaction_id=raw["transaction_id"],
        table_view=_table_view_from_dict(raw["table_view"]) if raw["table_view"] else None,
    )


class Catalog:
    def __init__(self, inner: _native.Catalog) -> None:
        self._inner = inner

    @classmethod
    async def in_memory(cls) -> "Catalog":
        inner = await _native.Catalog.in_memory()
        return cls(inner)

    @classmethod
    async def from_connection_string(cls, connection_string: str) -> "Catalog":
        inner = await _native.Catalog.from_connection_string(connection_string)
        return cls(inner)

    async def create_table(
        self,
        ident: TableIdent,
        location: str,
        schema: SchemaSpec,
        properties: Optional[Dict[str, Any]] = None,
    ) -> "TableHandle":
        handle = await self._inner.create_table(
            ident._inner, location, schema._inner, properties
        )
        return TableHandle(handle)

    async def load_table(self, ident: TableIdent) -> Optional["TableHandle"]:
        handle = await self._inner.load_table(ident._inner)
        if handle is None:
            return None
        return TableHandle(handle)

    async def list_tables(self, namespace: Optional[str] = None) -> List[TableIdent]:
        tables = await self._inner.list_tables(namespace)
        return [TableIdent(table.namespace, table.name) for table in tables]

    async def drop_table(self, ident: TableIdent) -> None:
        await self._inner.drop_table(ident._inner)


class TableHandle:
    def __init__(self, inner: _native.TableHandle) -> None:
        self._inner = inner

    async def read(self) -> TableView:
        raw = await self._inner.read()
        return _table_view_from_dict(raw)

    async def read_at(self, transaction_id: str) -> TableView:
        raw = await self._inner.read_at(transaction_id)
        return _table_view_from_dict(raw)

    async def diff(self, from_transaction_id: str, to_transaction_id: str) -> TableDelta:
        raw = await self._inner.diff(from_transaction_id, to_transaction_id)
        return _table_delta_from_dict(raw)

    async def append_file(self, file: FileSpec) -> CommitResult:
        raw = await self._inner.append_file(file._inner)
        return _commit_result_from_dict(raw)

    async def append_files(self, files: List[FileSpec]) -> CommitResult:
        raw = await self._inner.append_files([file._inner for file in files])
        return _commit_result_from_dict(raw)

    async def delete_files(self, file_uuids: List[str]) -> CommitResult:
        raw = await self._inner.delete_files(file_uuids)
        return _commit_result_from_dict(raw)

    async def set_properties(self, properties: Dict[str, Any]) -> CommitResult:
        raw = await self._inner.set_properties(properties)
        return _commit_result_from_dict(raw)


__all__ = [
    "Catalog",
    "CommitResult",
    "FileView",
    "SchemaView",
    "TableDelta",
    "TableHandle",
    "TableIdent",
    "TableStats",
    "TableView",
]
