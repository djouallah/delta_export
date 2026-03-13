# Delta Export Extension

DuckDB extension that exports DuckLake catalog tables to Delta Lake format.

## Architecture

- The extension registers a table function `delta_export()` that reads DuckLake internal metadata and writes Delta Lake checkpoint files (parquet, json, _last_checkpoint).
- It targets the **DuckLake internal metadata database** (`__ducklake_metadata_{catalog_name}`), NOT the DuckLake catalog itself. This is a regular DuckDB database where `ducklake_metadata`, `ducklake_table`, `ducklake_snapshot`, etc. live.
- `ducklake_export_log` is a regular DuckDB table created in this metadata DB — not a DuckLake table.
- Reference SQL implementation: `c:\lakehouse\default\Files\sql\export.sql`

## Key Constraints

- **Never use `context.Query()` inside a table function scan** — it deadlocks. Use a separate `Connection(*context.db)` instead.
- **DuckLake catalogs only support literal defaults** — no `DEFAULT CURRENT_TIMESTAMP` or expressions.
- `FileSystem::CreateDirectoriesRecursive` does not work on Windows for deep relative paths. Linux only for now.

## Build

Never build locally — always use CI. Push code and let GitHub Actions handle it.

Test extension deps in Makefile: `DEFAULT_TEST_EXTENSION_DEPS=json;parquet`.

## Testing

- **Always use real DuckLake catalogs** in tests — never create mock `ducklake_*` tables.
- SQL test pattern: `ATTACH 'ducklake:__TEST_DIR__/test.ducklake' AS lake (DATA_PATH '__TEST_DIR__/data'); USE lake;`
- Python test pattern: `INSTALL ducklake; ATTACH 'ducklake:{path}' AS test_lake (DATA_PATH '{data_path}'); USE test_lake;`
- SQL tests require: `require delta_export`, `require json`, `require parquet`, `require ducklake`
- DuckDB Python API: use `.description` for column metadata, not `.columns`.

## CI

Three workflows:

1. **LinuxBuild.yml** — builds linux_amd64 on `src/**` and build file changes, uploads artifact.
2. **IntegrationTest.yml** — runs after LinuxBuild or on `test/python/**` changes. Downloads artifact, runs pytest.
3. **MainDistributionPipeline.yml** — full multi-platform build, manual only (`workflow_dispatch`).

## DuckDB v1.5.x API

- Use `ExtensionLoader`, not `ExtensionUtil` (removed). No `#define DUCKDB_EXTENSION_MAIN`.
- `Connection::Query()` returns `unique_ptr<MaterializedQueryResult>` directly.
- Entry point macro: `DUCKDB_CPP_EXTENSION_ENTRY(delta_export, loader)`
