import pyarrow as pa
import pytest

from planar import Catalog, ColumnSpec, FileSpec, SchemaSpec, TableIdent


@pytest.mark.asyncio
async def test_catalog_table_lifecycle_smoke():
    catalog = await Catalog.in_memory()

    ident = TableIdent("sales", "transactions")
    schema = SchemaSpec().with_column(ColumnSpec("id", pa.int64()))

    table = await catalog.create_table(
        ident,
        "/data/sales/transactions",
        schema,
        {"owner": "analytics"},
    )

    view = await table.read()
    assert view.ident.namespace == "sales"
    assert view.ident.name == "transactions"
    assert view.transaction_id

    file_spec = FileSpec(
        "parquet",
        "/data/sales/transactions/part-00000.parquet",
        1,
        10,
        format_options={"compression": "zstd"},
    )
    result = await table.append_file(file_spec)
    assert result.transaction_id
