#include "delta_export_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// SQL Statements — kept close to the original export.sql for maintainability
//===--------------------------------------------------------------------===//

static constexpr const char *SQL_CREATE_EXPORT_SUMMARY = R"(
CREATE OR REPLACE TEMP TABLE export_summary (
    table_id BIGINT,
    schema_name VARCHAR,
    table_name VARCHAR,
    table_root VARCHAR,
    status VARCHAR,
    snapshot_id BIGINT,
    message VARCHAR
);
)";

static constexpr const char *SQL_INSERT_EXPORT_SUMMARY = R"(
INSERT INTO export_summary
WITH
data_root_config AS (
    SELECT value AS data_root FROM ducklake_metadata WHERE key = 'data_path'
),
active_tables AS (
    SELECT
        t.table_id,
        t.table_name,
        s.schema_name,
        t.path AS table_path,
        s.path AS schema_path,
        rtrim((SELECT data_root FROM data_root_config), '/') || '/' ||
            CASE
                WHEN trim(s.path, '/') != '' THEN trim(s.path, '/') || '/'
                ELSE ''
            END ||
            trim(t.path, '/') AS table_root
    FROM ducklake_table t
    JOIN ducklake_schema s USING(schema_id)
    WHERE t.end_snapshot IS NULL
),
current_snapshot AS (
    SELECT MAX(snapshot_id) AS snapshot_id FROM ducklake_snapshot
),
table_last_modified AS (
    SELECT
        t.*,
        COALESCE(
            (SELECT MAX(sc.snapshot_id)
             FROM ducklake_snapshot_changes sc
             WHERE regexp_matches(sc.changes_made, '[:,]' || t.table_id || '([^0-9]|$)')
            ),
            (SELECT cs.snapshot_id
             FROM current_snapshot cs
             WHERE EXISTS (
                 SELECT 1 FROM ducklake_data_file df
                 WHERE df.table_id = t.table_id
                   AND df.end_snapshot IS NULL
             )
            )
        ) AS last_modified_snapshot,
        (SELECT COUNT(*) FROM ducklake_data_file df
         WHERE df.table_id = t.table_id
           AND df.end_snapshot IS NULL
        ) AS file_count
    FROM active_tables t
),
SELECT
    table_id,
    schema_name,
    table_name,
    table_root,
    CASE
        WHEN file_count = 0 THEN 'no_data_files'
        ELSE 'needs_export'
    END AS status,
    last_modified_snapshot AS snapshot_id,
    CASE
        WHEN file_count = 0 THEN 'Table has no data files (empty table)'
        ELSE 'Ready for export'
    END AS message
FROM table_last_modified;
)";

static constexpr const char *SQL_CREATE_CHECKPOINT_PARQUET = R"(
CREATE OR REPLACE TEMP TABLE temp_checkpoint_parquet AS
WITH
tables_to_export AS (
    SELECT es.*
    FROM export_summary es
    WHERE es.status = 'needs_export'
      AND EXISTS (
          SELECT 1 FROM ducklake_data_file df
          WHERE df.table_id = es.table_id
            AND df.end_snapshot IS NULL
      )
),
table_schemas AS (
    SELECT
        te.table_id,
        te.schema_name,
        te.table_name,
        te.snapshot_id,
        te.table_root,
        list({
            'name': c.column_name,
            'type':
                CASE
                    WHEN c.column_type IN ('int8', 'tinyint') THEN 'byte'
                    WHEN c.column_type IN ('int16', 'smallint') THEN 'short'
                    WHEN c.column_type IN ('int32', 'integer', 'int') THEN 'integer'
                    WHEN c.column_type IN ('int64', 'bigint') THEN 'long'
                    WHEN c.column_type IN ('int128', 'hugeint') THEN 'string'
                    WHEN c.column_type IN ('uint8', 'utinyint') THEN 'short'
                    WHEN c.column_type IN ('uint16', 'usmallint') THEN 'integer'
                    WHEN c.column_type IN ('uint32', 'uinteger') THEN 'long'
                    WHEN c.column_type IN ('uint64', 'ubigint') THEN 'string'
                    WHEN c.column_type IN ('float32', 'float') THEN 'float'
                    WHEN c.column_type IN ('float64', 'double') THEN 'double'
                    WHEN c.column_type = 'boolean' THEN 'boolean'
                    WHEN contains(c.column_type, 'decimal') THEN c.column_type
                    WHEN contains(c.column_type, 'timestamp') THEN 'timestamp'
                    WHEN c.column_type = 'date' THEN 'date'
                    WHEN c.column_type = 'blob' THEN 'binary'
                    WHEN c.column_type = 'varchar' THEN 'string'
                    ELSE 'string'
                END,
            'nullable': true,
            'metadata': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(name VARCHAR, type VARCHAR, nullable BOOLEAN, metadata MAP(VARCHAR, VARCHAR)) ORDER BY c.column_order) AS schema_fields
    FROM tables_to_export te
    JOIN ducklake_table t ON te.table_id = t.table_id
    JOIN ducklake_column c ON t.table_id = c.table_id
    WHERE c.end_snapshot IS NULL
    GROUP BY te.table_id, te.schema_name, te.table_name, te.snapshot_id, te.table_root
),
file_column_stats_agg AS (
    SELECT
        ts.table_id,
        df.data_file_id,
        c.column_name,
        ANY_VALUE(c.column_type) AS column_type,
        MAX(fcs.value_count) AS value_count,
        MIN(fcs.min_value) AS min_value,
        MAX(fcs.max_value) AS max_value,
        MAX(fcs.null_count) AS null_count
    FROM table_schemas ts
    JOIN ducklake_data_file df ON ts.table_id = df.table_id
    LEFT JOIN ducklake_file_column_stats fcs ON df.data_file_id = fcs.data_file_id
    LEFT JOIN ducklake_column c ON fcs.column_id = c.column_id
    WHERE df.end_snapshot IS NULL
      AND c.column_id IS NOT NULL
      AND c.end_snapshot IS NULL
    GROUP BY ts.table_id, df.data_file_id, c.column_name
),
file_column_stats_transformed AS (
    SELECT
        fca.table_id,
        fca.data_file_id,
        fca.column_name,
        fca.column_type,
        fca.value_count,
        fca.null_count,
        CASE
            WHEN fca.min_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                regexp_replace(
                    regexp_replace(replace(fca.min_value, ' ', 'T'), '[+-]\d{2}(?::\d{2})?$', ''),
                    '^([^.]+)$', '\1.000'
                ) || 'Z'
            WHEN contains(lower(fca.column_type), 'date') THEN fca.min_value
            WHEN contains(lower(fca.column_type), 'bool') THEN CAST(lower(fca.min_value) IN ('true', 't', '1', 'yes') AS VARCHAR)
            WHEN contains(lower(fca.column_type), 'int') OR contains(lower(fca.column_type), 'float')
                 OR contains(lower(fca.column_type), 'double') OR contains(lower(fca.column_type), 'decimal') THEN
                CASE WHEN contains(fca.min_value, '.') OR contains(lower(fca.min_value), 'e')
                     THEN CAST(TRY_CAST(fca.min_value AS DOUBLE) AS VARCHAR)
                     ELSE CAST(TRY_CAST(fca.min_value AS BIGINT) AS VARCHAR)
                END
            ELSE fca.min_value
        END AS transformed_min,
        CASE
            WHEN fca.max_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                regexp_replace(
                    regexp_replace(replace(fca.max_value, ' ', 'T'), '[+-]\d{2}(?::\d{2})?$', ''),
                    '^([^.]+)$', '\1.000'
                ) || 'Z'
            WHEN contains(lower(fca.column_type), 'date') THEN fca.max_value
            WHEN contains(lower(fca.column_type), 'bool') THEN CAST(lower(fca.max_value) IN ('true', 't', '1', 'yes') AS VARCHAR)
            WHEN contains(lower(fca.column_type), 'int') OR contains(lower(fca.column_type), 'float')
                 OR contains(lower(fca.column_type), 'double') OR contains(lower(fca.column_type), 'decimal') THEN
                CASE WHEN contains(fca.max_value, '.') OR contains(lower(fca.max_value), 'e')
                     THEN CAST(TRY_CAST(fca.max_value AS DOUBLE) AS VARCHAR)
                     ELSE CAST(TRY_CAST(fca.max_value AS BIGINT) AS VARCHAR)
                END
            ELSE fca.max_value
        END AS transformed_max
    FROM file_column_stats_agg fca
),
file_metadata AS (
    SELECT
        ts.table_id,
        ts.schema_name,
        ts.table_name,
        ts.snapshot_id,
        ts.table_root,
        ts.schema_fields,
        df.data_file_id,
        df.path AS file_path,
        df.file_size_bytes,
        df.record_count AS num_records,
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.transformed_min
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.transformed_min IS NOT NULL)), MAP{}::MAP(VARCHAR, VARCHAR)) AS min_values,
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.transformed_max
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.transformed_max IS NOT NULL)), MAP{}::MAP(VARCHAR, VARCHAR)) AS max_values,
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.null_count
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.null_count IS NOT NULL)), MAP{}::MAP(VARCHAR, BIGINT)) AS null_count
    FROM table_schemas ts
    JOIN ducklake_data_file df ON ts.table_id = df.table_id
    LEFT JOIN file_column_stats_transformed fct ON df.data_file_id = fct.data_file_id
    WHERE df.end_snapshot IS NULL
    GROUP BY ts.table_id, ts.schema_name, ts.table_name, ts.snapshot_id,
             ts.table_root, ts.schema_fields, df.data_file_id, df.path, df.file_size_bytes, df.record_count
),
table_aggregates AS (
    SELECT
        table_id,
        schema_name,
        table_name,
        snapshot_id,
        table_root,
        schema_fields,
        COUNT(*) AS num_files,
        SUM(num_records) AS total_rows,
        SUM(file_size_bytes) AS total_bytes,
        list({
            'path': ltrim(file_path, '/'),
            'partitionValues': MAP{}::MAP(VARCHAR, VARCHAR),
            'size': file_size_bytes,
            'modificationTime': epoch_ms(now()),
            'dataChange': true,
            'stats': COALESCE(to_json({
                'numRecords': COALESCE(num_records, 0),
                'minValues': COALESCE(min_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                'maxValues': COALESCE(max_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                'nullCount': COALESCE(null_count, MAP{}::MAP(VARCHAR, BIGINT))
            }), '{"numRecords":0}'),
            'tags': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(
            path VARCHAR,
            partitionValues MAP(VARCHAR, VARCHAR),
            size BIGINT,
            modificationTime BIGINT,
            dataChange BOOLEAN,
            stats VARCHAR,
            tags MAP(VARCHAR, VARCHAR)
        )) AS add_entries
    FROM file_metadata
    GROUP BY table_id, schema_name, table_name, snapshot_id, table_root, schema_fields
),
checkpoint_data AS (
    SELECT
        ta.*,
        now() AS now_ts,
        epoch_ms(now()) AS now_ms,
        uuid()::VARCHAR AS txn_id,
        (substring(md5(ta.table_id::VARCHAR || '-metadata'), 1, 8) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 9, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 13, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 17, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 21, 12)) AS meta_id,
        to_json({'type': 'struct', 'fields': ta.schema_fields}) AS schema_string
    FROM table_aggregates ta
),
checkpoint_parquet_data AS (
    SELECT
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        {'minReaderVersion': 1, 'minWriterVersion': 2} AS protocol,
        NULL AS metaData,
        NULL AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        1 AS row_order
    FROM checkpoint_data cd
    UNION ALL
    SELECT
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        NULL AS protocol,
        {
            'id': cd.meta_id,
            'name': cd.table_name,
            'format': {'provider': 'parquet', 'options': MAP{}::MAP(VARCHAR, VARCHAR)}::STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            'schemaString': cd.schema_string,
            'partitionColumns': []::VARCHAR[],
            'createdTime': cd.now_ms,
            'configuration': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(
            id VARCHAR,
            name VARCHAR,
            format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            schemaString VARCHAR,
            partitionColumns VARCHAR[],
            createdTime BIGINT,
            configuration MAP(VARCHAR, VARCHAR)
        ) AS metaData,
        NULL AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        2 AS row_order
    FROM checkpoint_data cd
    UNION ALL
    SELECT
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        NULL AS protocol,
        NULL AS metaData,
        unnest(cd.add_entries) AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        3 AS row_order
    FROM checkpoint_data cd
)
SELECT * FROM checkpoint_parquet_data;
)";

static constexpr const char *SQL_CREATE_CHECKPOINT_JSON = R"(
CREATE OR REPLACE TEMP TABLE temp_checkpoint_json AS
SELECT DISTINCT
    p.table_id,
    p.table_root,
    p.snapshot_id,
    p.num_files,
    to_json({
        'commitInfo': {
            'timestamp': p.now_ms,
            'operation': 'CONVERT',
            'operationParameters': {
                'convertedFrom': 'DuckLake',
                'duckLakeSnapshotId': p.snapshot_id::VARCHAR,
                'partitionBy': '[]'
            },
            'isolationLevel': 'Serializable',
            'isBlindAppend': false,
            'operationMetrics': {
                'numFiles': p.num_files::VARCHAR,
                'numOutputRows': p.total_rows::VARCHAR,
                'numOutputBytes': p.total_bytes::VARCHAR
            },
            'engineInfo': 'DuckLake-Delta-Exporter/1.0.0',
            'txnId': p.txn_id
        }
    }) || chr(10) ||
    to_json({
        'metaData': {
            'id': p.meta_id,
            'name': p.table_name,
            'format': {'provider': 'parquet', 'options': MAP{}},
            'schemaString': p.schema_string::VARCHAR,
            'partitionColumns': [],
            'createdTime': p.now_ms,
            'configuration': MAP{}
        }
    }) || chr(10) ||
    to_json({
        'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}
    }) AS content
FROM temp_checkpoint_parquet p
WHERE p.row_order = 1;
)";

static constexpr const char *SQL_CREATE_LAST_CHECKPOINT = R"(
CREATE OR REPLACE TEMP TABLE temp_last_checkpoint AS
SELECT
    table_id,
    table_root,
    snapshot_id,
    '{"version":' || snapshot_id || ',"size":' || (2 + num_files) || '}' AS content
FROM temp_checkpoint_parquet
GROUP BY table_id, table_root, snapshot_id, num_files;
)";

static constexpr const char *SQL_CREATE_EXPORT_PATHS = R"(
CREATE OR REPLACE TEMP TABLE numbered_exports AS
SELECT
    table_id,
    table_root,
    snapshot_id,
    ROW_NUMBER() OVER (ORDER BY table_id) AS export_order
FROM (SELECT DISTINCT table_id, table_root, snapshot_id FROM temp_checkpoint_parquet);

CREATE OR REPLACE TEMP TABLE export_paths AS
SELECT
    ne.export_order,
    ne.table_id,
    ne.table_root,
    ne.snapshot_id,
    paths.checkpoint_file,
    paths.json_file,
    paths.last_checkpoint_file,
    paths.delta_log_path
FROM numbered_exports ne,
LATERAL (
    SELECT
        ne.table_root || '/_delta_log/' || lpad(ne.snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' AS checkpoint_file,
        ne.table_root || '/_delta_log/' || lpad(ne.snapshot_id::VARCHAR, 20, '0') || '.json' AS json_file,
        ne.table_root || '/_delta_log/_last_checkpoint' AS last_checkpoint_file,
        ne.table_root || '/_delta_log' AS delta_log_path
) AS paths;
)";

static constexpr const char *SQL_GET_LOCAL_DIRS = R"(
SELECT DISTINCT delta_log_path
FROM export_paths
WHERE table_root NOT LIKE 'abfss://%'
  AND table_root NOT LIKE 's3://%'
  AND table_root NOT LIKE 'gs://%'
  AND table_root NOT LIKE 'az://%'
  AND table_root NOT LIKE 'http://%'
  AND table_root NOT LIKE 'https://%';
)";

static constexpr const char *SQL_GET_EXPORTS = R"(
SELECT
    export_order,
    table_id,
    table_root,
    snapshot_id,
    checkpoint_file,
    json_file,
    last_checkpoint_file
FROM export_paths
ORDER BY export_order;
)";

static constexpr const char *SQL_GET_EXPORT_SUMMARY = R"(
SELECT
    schema_name || '.' || table_name AS table_name,
    status,
    snapshot_id AS data_snapshot,
    CASE
        WHEN status = 'no_data_files' THEN 'Table has 0 parquet files (empty)'
        WHEN status = 'needs_export' THEN 'Data snapshot ' || snapshot_id || ' exported to Delta'
    END AS explanation,
    message
FROM export_summary
ORDER BY
    CASE status
        WHEN 'needs_export' THEN 1
        WHEN 'no_data_files' THEN 2
    END,
    schema_name, table_name;
)";

//===--------------------------------------------------------------------===//
// Helper: Qualify ducklake_* table names with the metadata catalog prefix
//===--------------------------------------------------------------------===//
static string QualifyMetadataTables(const string &sql, const string &metadata_catalog) {
	if (metadata_catalog.empty()) {
		return sql;
	}
	string prefix = "\"" + metadata_catalog + "\".";
	// Replace longest names first to avoid partial matches
	// e.g. ducklake_snapshot_changes before ducklake_snapshot
	string result = sql;
	vector<string> tables = {"ducklake_file_column_stats", "ducklake_snapshot_changes", "ducklake_data_file",
	                         "ducklake_metadata",         "ducklake_snapshot",         "ducklake_column",
	                         "ducklake_schema",           "ducklake_table"};
	for (auto &table : tables) {
		string qualified = prefix + table;
		// Replace all occurrences
		size_t pos = 0;
		while ((pos = result.find(table, pos)) != string::npos) {
			// Don't double-qualify if already prefixed
			if (pos >= prefix.size() && result.substr(pos - prefix.size(), prefix.size()) == prefix) {
				pos += table.size();
				continue;
			}
			result.replace(pos, table.size(), qualified);
			pos += qualified.size();
		}
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Helper: Execute SQL and throw on error
//===--------------------------------------------------------------------===//
static unique_ptr<MaterializedQueryResult> ExecuteSQL(Connection &conn, const string &sql) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("Delta export failed: %s\nSQL: %s", result->GetError(), sql.substr(0, 200));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Table Function: delta_export
//===--------------------------------------------------------------------===//

struct DeltaExportBindData : public TableFunctionData {};

struct DeltaExportGlobalState : public GlobalTableFunctionState {
	bool finished = false;
	vector<vector<Value>> rows;
	idx_t current_row = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> DeltaExportBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("data_snapshot");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("explanation");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);

	return make_uniq<DeltaExportBindData>();
}

static unique_ptr<GlobalTableFunctionState> DeltaExportGlobalInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<DeltaExportGlobalState>();
}

static void DeltaExportScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<DeltaExportGlobalState>();

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	// If we already have rows, emit them
	if (!state.rows.empty()) {
		idx_t count = 0;
		while (state.current_row < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
			auto &row = state.rows[state.current_row];
			for (idx_t col = 0; col < row.size(); col++) {
				output.data[col].SetValue(count, row[col]);
			}
			count++;
			state.current_row++;
		}
		output.SetCardinality(count);
		if (state.current_row >= state.rows.size()) {
			state.finished = true;
		}
		return;
	}

	// ── Run export on the current database ──
	// Use a separate Connection to avoid deadlocking context.Query() inside a scan
	Connection conn(*context.db);
	auto &default_db = DatabaseManager::GetDefaultDatabase(context);
	// Build the metadata catalog name for qualifying ducklake_* table references
	string metadata_catalog = default_db.empty() ? "" : "__ducklake_metadata_" + default_db;

	// Step 1: Build export summary (reads ducklake_* tables)
	ExecuteSQL(conn, SQL_CREATE_EXPORT_SUMMARY);
	ExecuteSQL(conn, QualifyMetadataTables(SQL_INSERT_EXPORT_SUMMARY, metadata_catalog));

	// Step 2: Generate checkpoint parquet data (reads ducklake_* tables)
	ExecuteSQL(conn, QualifyMetadataTables(SQL_CREATE_CHECKPOINT_PARQUET, metadata_catalog));

	// Step 4: Generate JSON checkpoint
	ExecuteSQL(conn, SQL_CREATE_CHECKPOINT_JSON);

	// Step 5: Generate last checkpoint
	ExecuteSQL(conn, SQL_CREATE_LAST_CHECKPOINT);

	// Step 6: Build export paths
	ExecuteSQL(conn, SQL_CREATE_EXPORT_PATHS);

	// Step 7: Create _delta_log directories for local paths
	auto dirs_result = ExecuteSQL(conn, SQL_GET_LOCAL_DIRS);
	auto &fs = context.db->GetFileSystem();
	for (idx_t i = 0; i < dirs_result->RowCount(); i++) {
		string dir_path = dirs_result->GetValue(0, i).ToString();
		std::replace(dir_path.begin(), dir_path.end(), '\\', '/');
		fs.CreateDirectoriesRecursive(dir_path);
	}

	// Step 8: Export loop — unlimited tables
	auto exports_result = ExecuteSQL(conn, SQL_GET_EXPORTS);

	for (idx_t i = 0; i < exports_result->RowCount(); i++) {
		int64_t table_id = exports_result->GetValue(1, i).GetValue<int64_t>();
		int64_t snapshot_id = exports_result->GetValue(3, i).GetValue<int64_t>();
		string checkpoint_file = exports_result->GetValue(4, i).ToString();
		string json_file = exports_result->GetValue(5, i).ToString();
		string last_checkpoint_file = exports_result->GetValue(6, i).ToString();

		string tid = std::to_string(table_id);
		string sid = std::to_string(snapshot_id);

		string copy_parquet = "COPY (SELECT protocol, metaData, add, remove, commitInfo "
		                      "FROM temp_checkpoint_parquet "
		                      "WHERE table_id = " +
		                      tid + " AND snapshot_id = " + sid +
		                      " ORDER BY row_order) "
		                      "TO '" +
		                      checkpoint_file + "' (FORMAT PARQUET);";
		ExecuteSQL(conn, copy_parquet);

		string copy_json = "COPY (SELECT content FROM temp_checkpoint_json "
		                   "WHERE table_id = " +
		                   tid + " AND snapshot_id = " + sid +
		                   ") "
		                   "TO '" +
		                   json_file + "' (FORMAT CSV, HEADER false, QUOTE '');";
		ExecuteSQL(conn, copy_json);

		string copy_last = "COPY (SELECT content FROM temp_last_checkpoint "
		                   "WHERE table_id = " +
		                   tid + " AND snapshot_id = " + sid +
		                   ") "
		                   "TO '" +
		                   last_checkpoint_file + "' (FORMAT CSV, HEADER false, QUOTE '');";
		ExecuteSQL(conn, copy_last);
	}

	// Step 9: Collect and return results
	auto summary_result = ExecuteSQL(conn, SQL_GET_EXPORT_SUMMARY);

	for (idx_t i = 0; i < summary_result->RowCount(); i++) {
		vector<Value> row;
		row.push_back(summary_result->GetValue(0, i));
		row.push_back(summary_result->GetValue(1, i));
		row.push_back(summary_result->GetValue(2, i));
		row.push_back(summary_result->GetValue(3, i));
		row.push_back(summary_result->GetValue(4, i));
		state.rows.push_back(std::move(row));
	}

	// Emit first batch of rows
	idx_t count = 0;
	while (state.current_row < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.current_row];
		for (idx_t col = 0; col < row.size(); col++) {
			output.data[col].SetValue(count, row[col]);
		}
		count++;
		state.current_row++;
	}
	output.SetCardinality(count);

	if (state.current_row >= state.rows.size()) {
		state.finished = true;
	}
}

//===--------------------------------------------------------------------===//
// Extension loading
//===--------------------------------------------------------------------===//

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction func("delta_export", {}, DeltaExportScan, DeltaExportBind);
	func.init_global = DeltaExportGlobalInit;
	loader.RegisterFunction(func);
}

void DeltaExportExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DeltaExportExtension::Name() {
	return "delta_export";
}

std::string DeltaExportExtension::Version() const {
#ifdef EXT_VERSION_DELTA_EXPORT
	return EXT_VERSION_DELTA_EXPORT;
#else
	return "1.0.0";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(delta_export, loader) {
	duckdb::LoadInternal(loader);
}
}
