# Delta Export — DuckDB Extension

Export DuckLake catalog tables to Delta Lake format, enabling consumption by Delta Lake readers such as Power BI DirectLake, Apache Spark, Databricks, and other tools that understand the Delta protocol.

## Install

```sql
INSTALL delta_export FROM community;
LOAD delta_export;
```

## Usage

```sql
ATTACH 'metadata.ducklake' AS my_catalog;
USE my_catalog;
CALL delta_export();
```

To export only some tables, name them with `tables` (a bare name matches in any schema, `schema.table` only in that schema) and/or whole schemas with `schemas`. Names ignore case:

```sql
CALL delta_export(tables := ['main.orders']);                       -- just main.orders
CALL delta_export(schemas := ['sales']);                            -- every table in sales, including new ones
CALL delta_export(schemas := ['sales'], tables := ['main.orders']); -- both
```

Only the selected tables are flushed and exported. Delta logs of dropped tables are removed only in the schemas named with `schemas`; a `tables`-only export removes none. A name that matches no table or schema, or a bare table name found in several schemas, fails the call.

The function returns a summary table showing the export status of each table:

| table_name | status | data_snapshot | explanation | message |
|---|---|---|---|---|
| main.sales | needs_export | 5 | Data snapshot 5 exported to Delta | Ready for export |
| main.products | no_data_files | 3 | Table has 0 parquet files (empty) | Table has no data files (empty table) |

### Status values

- **needs_export** — data was exported to Delta format
- **no_data_files** — table has no parquet files (empty table, skipped)
- **removed_dropped** — the table was dropped in DuckLake and the Delta log delta_export had written for it was removed
- **remove_failed** — same, but removing the log failed; `message` has the error
- **needs_rewrite** — not exported: older data files no longer match the schema (rename, drop + re-add); `message` names the column and the fix
- **unsupported_column_name** — not exported: a column name Delta does not allow without column mapping

### Python

```python
import duckdb
con = duckdb.connect()
con.sql("""
    INSTALL delta_export FROM community;
    LOAD delta_export;
    ATTACH '/tmp/metadata.ducklake' AS db;
    USE db;
    CALL delta_export();
""")
```

## Features

- **Metadata-only export** — writes Delta Lake metadata on top of existing DuckLake Parquet files, no data copying
- **Column statistics** — generates Delta checkpoint files with min/max/null_count stats
- **Deletes without rewriting** — DuckLake delete files (and inlined deletes) are exported as Delta deletion vectors, so `DELETE`, `UPDATE` and `MERGE` read back correctly without `ducklake_rewrite_data_files`. Tables with deletes use Delta protocol reader 3 / writer 7 (`deletionVectors`); others stay on 1 / 2
- **Dropped tables** — after `DROP TABLE`, the next export removes that table's `_delta_log` so Delta readers stop seeing it. Only logs delta_export wrote for the same DuckLake schema are touched; the parquet files are left to DuckLake's own cleanup
- **Replaced tables** — the Delta version follows the DuckLake snapshot but always moves past an existing log, so `CREATE OR REPLACE TABLE` or a recreated catalog over the same folder is read correctly
- **Type mappings** — maps DuckLake types to Delta Lake schema types, including nested `LIST`, `STRUCT` and `MAP` columns; `NOT NULL` columns are exported as non-nullable
- **Absolute paths** — honours DuckLake's `path_is_relative` flag on schemas, tables and data files (for example files registered with `ducklake_add_data_files` from outside the data path)
- **Any storage backend** — supports any storage DuckDB supports: local filesystem, S3, Azure (abfss://), GCS
- **Any metadata backend** — finds the DuckLake metadata catalog whether it is hidden (the default on DuckDB 1.5.3+), named with `METADATA_CATALOG`, or placed in a schema with `METADATA_SCHEMA`; with several lakes attached, the one selected with `USE` is exported

### Type mapping

| DuckLake type | Delta type |
|---|---|
| int8 / int16 / int32 / int64 | byte / short / integer / long |
| uint8 / uint16 / uint32 | short / integer / long |
| float32 / float64 | float / double |
| boolean, date, varchar, blob | boolean, date, string, binary |
| decimal(P,S) | decimal(P,S) |
| timestamp, timestamptz, timestamp_s/ms/ns | timestamp |
| list, struct, map | array, struct, map (recursive) |
| json | string |
| uuid, time, timetz, interval, uint64, int128 | string (**lossy** — Delta has no equivalent, readers will not decode these columns) |

## How it works

1. Reads DuckLake internal metadata (`ducklake_table`, `ducklake_data_file`, `ducklake_column`, etc.)
2. Generates Delta Lake checkpoint files (`.checkpoint.parquet`, `.json`, `_last_checkpoint`) in each table's `_delta_log/` directory
3. Other engines (Spark, Databricks, Fabric, Trino, etc.) can then read the same Parquet files via the Delta protocol

Rows DuckLake still keeps inlined in the metadata catalog are in no parquet file, so `delta_export` flushes them first (`ducklake_flush_inlined_data`). A lake attached read-only cannot be flushed, and its inlined rows stay out of the export.

Tables whose older data files no longer match the current schema are not exported and are reported as `needs_rewrite`: a renamed column, or a column dropped and added again under the same name. The files keep the old layout, and readers that match columns by name, such as the Fabric SQL analytics endpoint, would read NULLs. Rewrite such a table, e.g. `CREATE OR REPLACE TABLE t AS SELECT * FROM t`. Widened columns (e.g. `INTEGER` to `BIGINT`) are exported with the Delta `typeWidening` feature. Column names containing a character Delta only allows with column mapping (space, `,;{}()=`) are reported as `unsupported_column_name`.

Rewriting files with deletes (`ducklake_rewrite_data_files`) is no longer required; it only lets the export drop the deletion vectors. Each export that has deletes writes a new `deletion_vector_<uuid>.bin` into the table folder and never deletes earlier ones while the table exists, because readers still on the previous version (the Fabric SQL analytics endpoint caches table metadata) read them. They are small, and they are removed together with the `_delta_log` when the table is dropped. Deletion vector files (`deletion_vector_*.bin`) and `_delta_log` live in the table folders but DuckLake does not track them, so `ducklake_delete_orphaned_files` removes them. If you run it, run `delta_export` again afterwards.

## Real-world example

For a real-world DuckLake project using delta_export with dbt on Microsoft Fabric, see https://github.com/djouallah/fabric-dbt-examples

## Requirements

- DuckDB 2.0


## License

MIT
