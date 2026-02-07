import pyarrow as pa
from planar import Catalog, ColumnSpec, FileSpec, SchemaSpec, TableIdent


def test_catalog_table_lifecycle_smoke():
    catalog = Catalog.in_memory()

    ident = TableIdent("sales", "transactions")
    schema = SchemaSpec().with_column(ColumnSpec("id", pa.int64()))

    table = catalog.create_table(
        ident,
        "/data/sales/transactions",
        schema,
        {"owner": "analytics"},
    )

    view = table.read()
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
    result = table.append_file(file_spec)
    assert result.transaction_id
