# Delta Export — DuckDB Extension

Export DuckLake catalog tables to Delta Lake format, enabling consumption by Delta Lake readers such as Power BI DirectLake, Apache Spark, Databricks, and other tools that understand the Delta protocol.

## Install

```sql
SET allow_unsigned_extensions = true;
INSTALL delta_export FROM 'https://github.com/djouallah/delta_export/releases/download/v1.4.4';
LOAD delta_export;
```

## Usage

```sql
ATTACH 'ducklake:metadata.ducklake' AS my_catalog (DATA_PATH '/path/to/data');
USE my_catalog;
CALL delta_export();
```

The function returns a summary table showing the export status of each table:

| table_name | status | data_snapshot | explanation | message |
|---|---|---|---|---|
| main.sales | needs_export | 5 | Data snapshot 5 needs Delta export | Ready for export |
| main.products | already_exported | 3 | Data snapshot 3 already exported | Snapshot 3 already exported |

### Status values

- **needs_export** — new or changed data was exported to Delta format
- **already_exported** — no changes since last export (idempotent)
- **no_data_files** — table is empty

### Python

```python
import duckdb
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.sql("""
         attach 'ducklake:/tmp/md.db' as db  ; use db;
         load delta_export;
         call  delta_export
        """)
```

## Features

- **Incremental exports** — only re-exports tables with new snapshots
- **Handles deletes and updates** — automatically flushes inlined data and rewrites files with deletes before exporting
- **Column statistics** — generates Delta checkpoint files with min/max/null_count stats
- **Type mappings** — maps DuckDB types (TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN, VARCHAR, BLOB, DATE, TIMESTAMP) to Delta Lake schema types
- **Cloud storage** — supports local filesystem, S3, Azure (abfss://), and GCS paths

## How it works

1. Reads DuckLake internal metadata (`ducklake_table`, `ducklake_data_file`, `ducklake_column`, etc.)
2. Identifies tables with changes since the last export
3. Flushes any inlined data and rewrites files containing deleted rows
4. Generates Delta Lake checkpoint files (`.checkpoint.parquet`, `.json`, `_last_checkpoint`) in each table's `_delta_log/` directory
5. Records the exported snapshot in `ducklake_export_log` to enable incremental exports

## Requirements

- DuckDB v1.4.x+


## License

MIT
