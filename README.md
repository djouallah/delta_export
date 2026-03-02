# Delta Export — DuckDB Extension

Export DuckLake catalog tables to Delta Lake format, enabling consumption by Delta Lake readers such as Power BI DirectLake, Apache Spark, Databricks, and other tools that understand the Delta protocol.

## Install

```sql
SET allow_unsigned_extensions = true;
INSTALL delta_export FROM 'https://djouallah.github.io/delta_export';
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
| main.sales | needs_export | 5 | Data snapshot 5 exported to Delta | Ready for export |
| main.products | no_data_files | NULL | Table has 0 parquet files (empty) | Table has no data files (empty table) |

### Status values

- **needs_export** — table has data files and was exported to Delta format
- **no_data_files** — table is empty

### Python

```python
import duckdb
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.sql("""
        INSTALL delta_export FROM 'https://djouallah.github.io/delta_export';
         attach 'ducklake:/tmp/md.db' as db  ; use db;
         load delta_export;
         call  delta_export
        """)
```

## Features

- **Read-only** — does not write to or modify the DuckLake metadata database, safe to use alongside dbt-duckdb or other connections
- **Column statistics** — generates Delta checkpoint files with min/max/null_count stats
- **Type mappings** — maps DuckDB types (TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN, VARCHAR, BLOB, DATE, TIMESTAMP) to Delta Lake schema types
- **Cloud storage** — supports local filesystem, S3, Azure (abfss://), and GCS paths

## How it works

1. Reads DuckLake internal metadata (`ducklake_table`, `ducklake_data_file`, `ducklake_column`, etc.)
2. Identifies tables that have data files
3. Generates Delta Lake checkpoint files (`.checkpoint.parquet`, `.json`, `_last_checkpoint`) in each table's `_delta_log/` directory

## Current limitations

- **Exports everything every time** — there is no incremental export; every call re-exports all tables that have data files, even if nothing changed since the last export.
- **Requires manual maintenance before export** — if you have inlined data or un-compacted deletes, run `CALL ducklake_flush_inlined_data('catalog')` and `CALL ducklake_rewrite_data_files('catalog')` before calling `delta_export()`.

## Requirements

- DuckDB v1.4.x+


## License

MIT
