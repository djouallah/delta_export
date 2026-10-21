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

1. **CI.yml** — every pull request and every push to `main` (src, test, build files, submodules, workflows) runs everything: the community-extensions build for all platforms (`b15358c`), format + tidy, then LinuxBuild and IntegrationTest (Python, Spark smoke + edge cases; no fuzzing). OneLake + SQL endpoint runs only on pushes to `main` (Azure OIDC secrets never reach fork code). Green = safe to merge and release.
2. **LinuxBuild.yml** — reusable (called by CI) and dispatchable: builds the linux_amd64 test binary, uploads `delta_export-linux_amd64`.
3. **IntegrationTest.yml** — reusable (called by CI with this run's build) and dispatchable for quick reruns: `only`, `tests=` (-k), `full`, `run_id` (artifact of that run; default the latest CI build on `main`). Dispatched runs re-run only last failures (`--lf`) unless `full=true`.
4. **MainDistributionPipeline.yml** — multi-platform build plus GitHub Pages deploy, manual or tag push.
5. **Format.yml** — dispatch to apply `make format-fix` in CI and commit it.
6. **Fuzz.yml** — manual only: randomized workloads checked against Spark (`fuzz_seeds`, default 30, about 1.5 min each; `fuzz_seed` replays one; `run_id` as above). Run it before a release.

## DuckDB 2.0 API

- Targets DuckDB 2.0 only. Tests: `duckdb` submodule = the commit of the pinned Python dev wheel (`duckdb==2.0.0.dev2610162032`, f8f3f4dcd9, in IntegrationTest.yml and DuckLakeRepro.yml; LinuxBuild.yml stamps its version `v2.0.0-alpha48730` via `OVERRIDE_GIT_DESCRIBE`); an extension loads only into the exact DuckDB build it was compiled for. Release: MainDistributionPipeline builds at the commit community-extensions uses (`b15358c`, v2.0.0-alpha46057), with extension-ci-tools `v2.0-cyanoptera`. Re-pin both to `v2.0.0` once it is released.
- Catalog names (`AttachedDatabase::GetName()`, `DatabaseManager::GetDefaultDatabase()`) are `Identifier`s; convert explicitly with `static_cast<const string &>(...)`.
- DuckLake on DuckDB >= 1.5.3 attaches its metadata catalog `HIDDEN`, so it is absent from `duckdb_databases()`. Never discover it through SQL; `ResolveMetadataLocation` enumerates `DatabaseManager::GetDatabases` (includes hidden entries) from the main context. Hidden catalogs still resolve by name in queries.
- Use `ExtensionLoader`, not `ExtensionUtil` (removed). No `#define DUCKDB_EXTENSION_MAIN`.
- `Connection::Query()` returns `unique_ptr<MaterializedQueryResult>` directly.
- Entry point macro: `DUCKDB_CPP_EXTENSION_ENTRY(delta_export, loader)`
