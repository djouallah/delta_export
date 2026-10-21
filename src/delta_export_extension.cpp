#include "delta_export_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <map>
#include <set>

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
    message VARCHAR,
    table_uuid VARCHAR,
    schema_uuid VARCHAR
);
)";

// Storage roots of every schema and of every ducklake_table row, live and dropped.
static constexpr const char *SQL_CREATE_TABLE_ROOTS = R"(
CREATE OR REPLACE TEMP TABLE schema_roots AS
WITH data_root_config AS (
    SELECT value AS data_root FROM {META}ducklake_metadata WHERE key = 'data_path'
)
SELECT
    s.schema_id,
    s.schema_name,
    s.schema_uuid::VARCHAR AS schema_uuid,
    -- DuckLake spec: schema path is relative to data_path unless path_is_relative is false
    CASE
        WHEN NOT COALESCE(s.path_is_relative, true) THEN rtrim(s.path, '/')
        ELSE rtrim((SELECT data_root FROM data_root_config), '/') ||
            CASE
                WHEN trim(s.path, '/') != '' THEN '/' || trim(s.path, '/')
                ELSE ''
            END
    END AS schema_root
FROM {META}ducklake_schema s;

CREATE OR REPLACE TEMP TABLE table_roots AS
SELECT
    t.table_id,
    t.table_name,
    sr.schema_name,
    t.end_snapshot,
    t.table_uuid::VARCHAR AS table_uuid,
    sr.schema_uuid,
    -- table path is relative to the schema path unless path_is_relative is false
    CASE
        WHEN NOT COALESCE(t.path_is_relative, true) THEN rtrim(t.path, '/')
        ELSE sr.schema_root || '/' || trim(t.path, '/')
    END AS table_root
FROM {META}ducklake_table t
JOIN schema_roots sr USING (schema_id);
)";

static constexpr const char *SQL_INSERT_EXPORT_SUMMARY = R"(
INSERT INTO export_summary
WITH
active_tables AS (
    SELECT * FROM table_roots WHERE end_snapshot IS NULL
),
current_snapshot AS (
    SELECT MAX(snapshot_id) AS snapshot_id FROM {META}ducklake_snapshot
),
table_last_modified AS (
    SELECT
        t.*,
        COALESCE(
            (SELECT MAX(sc.snapshot_id)
             FROM {META}ducklake_snapshot_changes sc
             WHERE regexp_matches(sc.changes_made, '[:,]' || t.table_id || '([^0-9]|$)')
            ),
            (SELECT cs.snapshot_id
             FROM current_snapshot cs
             WHERE EXISTS (
                 SELECT 1 FROM {META}ducklake_data_file df
                 WHERE df.table_id = t.table_id
                   AND df.end_snapshot IS NULL
             )
            )
        ) AS last_modified_snapshot,
        (SELECT COUNT(*) FROM {META}ducklake_data_file df
         WHERE df.table_id = t.table_id
           AND df.end_snapshot IS NULL
        ) AS file_count
    FROM active_tables t
)
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
    END AS message,
    table_uuid,
    schema_uuid
FROM table_last_modified;
)";

// Active DuckLake delete files of the exported tables. Paths are relative to the table root like data files.
static constexpr const char *SQL_GET_DELETE_FILES = R"(
SELECT
    es.table_id,
    del.data_file_id,
    CASE
        WHEN COALESCE(del.path_is_relative, true) THEN es.table_root || '/' || ltrim(del.path, '/')
        ELSE del.path
    END AS delete_path
FROM {META}ducklake_delete_file del
JOIN export_summary es ON es.table_id = del.table_id
JOIN {META}ducklake_data_file df ON df.data_file_id = del.data_file_id AND df.end_snapshot IS NULL
WHERE es.status = 'needs_export'
  AND del.end_snapshot IS NULL
ORDER BY es.table_id, del.data_file_id;
)";

// For every live column of an exported table, the column as it was when each active data file was written.
// A different name (rename) or a different column under the same name (drop + re-add) means name-based readers
// would read NULL or the wrong data; a different type needs Delta type widening.
static constexpr const char *SQL_GET_COLUMN_HISTORY = R"(
WITH live AS (
    SELECT c.table_id, c.column_id, c.column_name, c.column_type, c.parent_column
    FROM {META}ducklake_column c
    JOIN export_summary es ON es.table_id = c.table_id
    WHERE es.status = 'needs_export' AND c.end_snapshot IS NULL
),
files AS (
    SELECT DISTINCT df.table_id, df.begin_snapshot
    FROM {META}ducklake_data_file df
    JOIN export_summary es ON es.table_id = df.table_id
    WHERE es.status = 'needs_export' AND df.end_snapshot IS NULL
),
at_write AS (
    SELECT l.table_id, l.column_id, l.column_name, l.column_type, h.column_name AS old_name, h.column_type AS old_type
    FROM live l
    JOIN files f ON f.table_id = l.table_id
    JOIN {META}ducklake_column h ON h.table_id = l.table_id AND h.column_id = l.column_id
        AND h.begin_snapshot <= f.begin_snapshot AND (h.end_snapshot IS NULL OR h.end_snapshot > f.begin_snapshot)
),
same_name AS (
    SELECT DISTINCT l.table_id, l.column_id, l.column_name, l.column_type
    FROM live l
    JOIN files f ON f.table_id = l.table_id
    JOIN {META}ducklake_column o ON o.table_id = l.table_id AND o.column_id <> l.column_id
        AND o.column_name = l.column_name AND o.parent_column IS NOT DISTINCT FROM l.parent_column
        AND o.begin_snapshot <= f.begin_snapshot AND (o.end_snapshot IS NULL OR o.end_snapshot > f.begin_snapshot)
)
SELECT DISTINCT table_id, column_id, 'rename' AS kind, old_name AS detail, column_name, column_type
FROM at_write WHERE old_name <> column_name
UNION ALL
SELECT DISTINCT table_id, column_id, 'type', old_type, column_name, column_type
FROM at_write WHERE old_type <> column_type
UNION ALL
SELECT table_id, column_id, 'same_name', column_name, column_name, column_type FROM same_name
ORDER BY 1, 2, 3, 4;
)";

// Dropped tables whose storage root no live table uses any more.
static constexpr const char *SQL_GET_DROPPED_TABLES = R"(
SELECT
    table_root,
    arg_max(table_id, end_snapshot) AS table_id,
    arg_max(schema_name, end_snapshot) AS schema_name,
    arg_max(table_name, end_snapshot) AS table_name,
    arg_max(schema_uuid, end_snapshot) AS schema_uuid
FROM table_roots tr
WHERE tr.end_snapshot IS NOT NULL
  AND tr.table_root IS NOT NULL
  AND tr.table_id NOT IN (SELECT table_id FROM export_summary)
  AND tr.table_root NOT IN (SELECT table_root FROM export_summary WHERE table_root IS NOT NULL)
GROUP BY table_root;
)";

// Current columns of every table that will be exported, including nested child columns
// (parent_column IS NOT NULL). The Delta schema is assembled from these rows in C++.
static constexpr const char *SQL_GET_EXPORT_COLUMNS = R"(
SELECT
    c.table_id,
    c.column_id,
    c.column_order,
    c.column_name,
    c.column_type,
    COALESCE(c.nulls_allowed, true) AS nulls_allowed,
    c.parent_column
FROM {META}ducklake_column c
JOIN export_summary es ON es.table_id = c.table_id
WHERE es.status = 'needs_export'
  AND c.end_snapshot IS NULL
ORDER BY c.table_id, c.column_order, c.column_id;
)";

static constexpr const char *SQL_CREATE_CHECKPOINT_PARQUET = R"(
CREATE OR REPLACE TEMP TABLE temp_checkpoint_parquet AS
WITH
-- includes emptied tables that were exported before: they get a version without files
tables_to_export AS (
    SELECT es.*
    FROM export_summary es
    WHERE es.status = 'needs_export'
),
table_schemas AS (
    SELECT
        te.table_id,
        te.schema_name,
        te.table_name,
        te.snapshot_id,
        te.table_root,
        tsj.schema_string,
        tsj.type_widening
    FROM tables_to_export te
    JOIN table_schema_json tsj USING (table_id)
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
    JOIN {META}ducklake_data_file df ON ts.table_id = df.table_id
    LEFT JOIN {META}ducklake_file_column_stats fcs ON df.data_file_id = fcs.data_file_id
    -- column_id is only unique within a table
    LEFT JOIN {META}ducklake_column c ON fcs.column_id = c.column_id AND c.table_id = df.table_id
    WHERE df.end_snapshot IS NULL
      AND c.column_id IS NOT NULL
      AND c.end_snapshot IS NULL
      AND c.parent_column IS NULL
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
        -- Readers skip files on these bounds, so a bound that may be wrong is left out: a missing bound only
        -- costs skipping, a wrong one drops rows.
        CASE
            WHEN fca.min_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                CASE
                    -- the text drops the offset below; only UTC bounds can lose it safely
                    WHEN regexp_matches(fca.min_value, '[+-]\d{2}(:\d{2})?$')
                         AND NOT regexp_matches(fca.min_value, '[+-]00(:00)?$') THEN NULL
                    ELSE regexp_replace(
                        regexp_replace(replace(fca.min_value, ' ', 'T'), '[+-]\d{2}(?::\d{2})?$', ''),
                        '^([^.]+)$', '\1.000'
                    ) || 'Z'
                END
            WHEN contains(lower(fca.column_type), 'date') THEN fca.min_value
            -- min/max are emitted as JSON strings; delta-kernel rejects a string for a boolean, and Delta writers
            -- do not collect boolean bounds anyway
            WHEN contains(lower(fca.column_type), 'bool') THEN NULL
            -- the exact decimal text: going through DOUBLE rounds past the true bound
            WHEN contains(lower(fca.column_type), 'decimal') THEN fca.min_value
            -- NaN and infinities have no JSON number; a finite lower bound is still valid with NaN rows
            WHEN contains(lower(fca.column_type), 'float') OR contains(lower(fca.column_type), 'double') THEN
                CASE WHEN isfinite(TRY_CAST(fca.min_value AS DOUBLE))
                     THEN CAST(TRY_CAST(fca.min_value AS DOUBLE) AS VARCHAR) END
            WHEN contains(lower(fca.column_type), 'int') THEN CAST(TRY_CAST(fca.min_value AS BIGINT) AS VARCHAR)
            ELSE fca.min_value
        END AS transformed_min,
        CASE
            WHEN fca.max_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                CASE
                    WHEN regexp_matches(fca.max_value, '[+-]\d{2}(:\d{2})?$')
                         AND NOT regexp_matches(fca.max_value, '[+-]00(:00)?$') THEN NULL
                    ELSE regexp_replace(
                        regexp_replace(replace(fca.max_value, ' ', 'T'), '[+-]\d{2}(?::\d{2})?$', ''),
                        '^([^.]+)$', '\1.000'
                    ) || 'Z'
                END
            WHEN contains(lower(fca.column_type), 'date') THEN fca.max_value
            WHEN contains(lower(fca.column_type), 'bool') THEN NULL
            WHEN contains(lower(fca.column_type), 'decimal') THEN fca.max_value
            -- NaN sorts above every number and may be missing from the stored bound: no float upper bound
            WHEN contains(lower(fca.column_type), 'float') OR contains(lower(fca.column_type), 'double') THEN NULL
            WHEN contains(lower(fca.column_type), 'int') THEN CAST(TRY_CAST(fca.max_value AS BIGINT) AS VARCHAR)
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
        ts.schema_string,
        ts.type_widening,
        df.data_file_id,
        df.path AS file_path,
        df.path_is_relative,
        df.file_size_bytes,
        df.record_count AS num_records,
        dv.storage_type AS dv_storage_type,
        dv.path_or_inline_dv AS dv_path,
        dv.dv_offset,
        dv.size_in_bytes AS dv_size,
        dv.cardinality AS dv_cardinality,
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
    LEFT JOIN {META}ducklake_data_file df ON ts.table_id = df.table_id AND df.end_snapshot IS NULL
    LEFT JOIN table_deletion_vectors dv ON dv.data_file_id = df.data_file_id
    LEFT JOIN file_column_stats_transformed fct ON df.data_file_id = fct.data_file_id
    GROUP BY ts.table_id, ts.schema_name, ts.table_name, ts.snapshot_id,
             ts.table_root, ts.schema_string, ts.type_widening, df.data_file_id, df.path, df.path_is_relative,
             df.file_size_bytes, df.record_count, dv.storage_type, dv.path_or_inline_dv,
             dv.dv_offset, dv.size_in_bytes, dv.cardinality
),
table_aggregates AS (
    SELECT
        table_id,
        schema_name,
        table_name,
        snapshot_id,
        table_root,
        schema_string,
        type_widening,
        COUNT(data_file_id) AS num_files,
        COALESCE(SUM(num_records), 0) - COALESCE(SUM(dv_cardinality), 0) AS total_rows,
        COALESCE(SUM(file_size_bytes), 0) AS total_bytes,
        COALESCE(bool_or(dv_storage_type IS NOT NULL), false) AS has_dv,
        -- an emptied table has one row without a file: its list is NULL and unnests to no add rows
        list({
            'path': CASE WHEN COALESCE(path_is_relative, true) THEN ltrim(file_path, '/') ELSE file_path END,
            'partitionValues': MAP{}::MAP(VARCHAR, VARCHAR),
            'size': file_size_bytes,
            'modificationTime': epoch_ms(now()),
            'dataChange': true,
            -- numRecords is the physical row count; with a deletion vector the bounds may be wider than the live rows
            'stats': COALESCE(
                CASE
                    WHEN dv_storage_type IS NULL THEN to_json({
                        'numRecords': COALESCE(num_records, 0),
                        'minValues': COALESCE(min_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                        'maxValues': COALESCE(max_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                        'nullCount': COALESCE(null_count, MAP{}::MAP(VARCHAR, BIGINT))
                    })
                    ELSE to_json({
                        'numRecords': COALESCE(num_records, 0),
                        'minValues': COALESCE(min_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                        'maxValues': COALESCE(max_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                        'nullCount': COALESCE(null_count, MAP{}::MAP(VARCHAR, BIGINT)),
                        'tightBounds': false
                    })
                END, '{"numRecords":0}'),
            'tags': MAP{}::MAP(VARCHAR, VARCHAR),
            'deletionVector': CASE
                WHEN dv_storage_type IS NULL THEN NULL
                ELSE {
                    'storageType': dv_storage_type,
                    'pathOrInlineDv': dv_path,
                    'offset': dv_offset,
                    'sizeInBytes': dv_size,
                    'cardinality': dv_cardinality
                }
            END
        }::STRUCT(
            path VARCHAR,
            partitionValues MAP(VARCHAR, VARCHAR),
            size BIGINT,
            modificationTime BIGINT,
            dataChange BOOLEAN,
            stats VARCHAR,
            tags MAP(VARCHAR, VARCHAR),
            deletionVector STRUCT(storageType VARCHAR, pathOrInlineDv VARCHAR, "offset" INTEGER, sizeInBytes INTEGER, cardinality BIGINT)
        )) FILTER (WHERE data_file_id IS NOT NULL) AS add_entries
    FROM file_metadata
    GROUP BY table_id, schema_name, table_name, snapshot_id, table_root, schema_string, type_widening
),
checkpoint_data AS (
    SELECT
        ta.*,
        -- table features in use; none keeps the widely readable protocol 1 / 2
        list_concat(
            CASE WHEN ta.has_dv THEN ['deletionVectors'] ELSE []::VARCHAR[] END,
            CASE WHEN ta.type_widening THEN ['typeWidening'] ELSE []::VARCHAR[] END
        ) AS features,
        map_concat(
            CASE WHEN ta.has_dv THEN MAP{'delta.enableDeletionVectors': 'true'} ELSE MAP{}::MAP(VARCHAR, VARCHAR) END,
            CASE WHEN ta.type_widening THEN MAP{'delta.enableTypeWidening': 'true'} ELSE MAP{}::MAP(VARCHAR, VARCHAR) END
        ) AS configuration,
        COALESCE(es.table_uuid, '') AS table_uuid,
        COALESCE(es.schema_uuid, '') AS schema_uuid,
        now() AS now_ts,
        epoch_ms(now()) AS now_ms,
        uuid()::VARCHAR AS txn_id,
        -- the DuckLake table uuid: a replaced table (CREATE OR REPLACE) gets a new Delta table id
        COALESCE(es.table_uuid,
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 1, 8) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 9, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 13, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 17, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 21, 12)) AS meta_id
    FROM table_aggregates ta
    JOIN export_summary es USING (table_id)
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
        cd.has_dv,
        cd.features,
        cd.configuration,
        cd.table_uuid,
        cd.schema_uuid,
        -- table features need reader 3 / writer 7; other tables keep the widely readable 1 / 2
        CASE
            WHEN len(cd.features) > 0 THEN {
                'minReaderVersion': 3, 'minWriterVersion': 7,
                'readerFeatures': cd.features, 'writerFeatures': cd.features
            }::STRUCT(minReaderVersion INTEGER, minWriterVersion INTEGER, readerFeatures VARCHAR[], writerFeatures VARCHAR[])
            ELSE {
                'minReaderVersion': 1, 'minWriterVersion': 2, 'readerFeatures': NULL, 'writerFeatures': NULL
            }::STRUCT(minReaderVersion INTEGER, minWriterVersion INTEGER, readerFeatures VARCHAR[], writerFeatures VARCHAR[])
        END AS protocol,
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
        cd.has_dv,
        cd.features,
        cd.configuration,
        cd.table_uuid,
        cd.schema_uuid,
        NULL AS protocol,
        {
            'id': cd.meta_id,
            'name': cd.table_name,
            'format': {'provider': 'parquet', 'options': MAP{}::MAP(VARCHAR, VARCHAR)}::STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            'schemaString': cd.schema_string,
            'partitionColumns': []::VARCHAR[],
            'createdTime': cd.now_ms,
            'configuration': cd.configuration
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
        cd.has_dv,
        cd.features,
        cd.configuration,
        cd.table_uuid,
        cd.schema_uuid,
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

// duckLakeSchemaUuid marks which DuckLake schema owns the log: dropped-table cleanup only removes logs it owns
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
                'duckLakeTableUuid': p.table_uuid,
                'duckLakeSchemaUuid': p.schema_uuid,
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
            'configuration': p.configuration
        }
    }) || chr(10) ||
    CASE
        WHEN len(p.features) > 0 THEN to_json({
            'protocol': {
                'minReaderVersion': 3, 'minWriterVersion': 7,
                'readerFeatures': p.features, 'writerFeatures': p.features
            }
        })
        ELSE to_json({
            'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}
        })
    END AS content
FROM temp_checkpoint_parquet p
WHERE p.row_order = 1;
)";

// _last_checkpoint content is assembled in C++ once the Delta version is known
static constexpr const char *SQL_CREATE_LAST_CHECKPOINT = R"(
CREATE OR REPLACE TEMP TABLE temp_last_checkpoint AS
SELECT
    table_id,
    table_root,
    snapshot_id,
    2 + num_files AS size
FROM temp_checkpoint_parquet
GROUP BY table_id, table_root, snapshot_id, num_files;
)";

static constexpr const char *SQL_CREATE_EXPORT_PATHS = R"(
CREATE OR REPLACE TEMP TABLE export_paths AS
SELECT
    ROW_NUMBER() OVER (ORDER BY table_id) AS export_order,
    table_id,
    table_root,
    snapshot_id,
    table_root || '/_delta_log' AS delta_log_path
FROM (SELECT DISTINCT table_id, table_root, snapshot_id FROM temp_checkpoint_parquet);
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
    delta_log_path
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
        WHEN status = 'removed_dropped' THEN 'Table was dropped in DuckLake; its Delta log was removed'
        WHEN status = 'remove_failed' THEN 'Table was dropped in DuckLake; removing its Delta log failed'
        WHEN status = 'needs_rewrite' THEN 'Not exported: older data files do not match the current schema'
        WHEN status = 'unsupported_column_name' THEN 'Not exported: a column name Delta does not allow'
    END AS explanation,
    message
FROM export_summary
ORDER BY
    CASE status
        WHEN 'needs_export' THEN 1
        WHEN 'needs_rewrite' THEN 2
        WHEN 'unsupported_column_name' THEN 3
        WHEN 'no_data_files' THEN 4
        WHEN 'removed_dropped' THEN 5
        WHEN 'remove_failed' THEN 6
    END,
    schema_name, table_name;
)";

//===--------------------------------------------------------------------===//
// Helper: Locate the DuckLake metadata catalog
//===--------------------------------------------------------------------===//
struct MetadataLocation {
	//! Catalog holding the ducklake_* tables; empty means "resolve through the search path"
	string catalog;
	//! Schema holding the ducklake_* tables (METADATA_SCHEMA attach option); empty means the default schema
	string schema;
	//! Name of the attached DuckLake catalog; empty when only the metadata database is attached
	string lake;
};

// DuckLake (on DuckDB >= 1.5.3) attaches its metadata catalog HIDDEN unless METADATA_CATALOG is given, so
// duckdb_databases() no longer lists it. DatabaseManager::GetDatabases includes hidden entries (it is what
// duckdb_databases() itself calls), and a hidden catalog still resolves by name in queries.
static MetadataLocation ResolveMetadataLocation(ClientContext &context) {
	auto &db_manager = DatabaseManager::Get(context);
	string default_db;
	try {
		default_db = static_cast<const string &>(DatabaseManager::GetDefaultDatabase(context));
	} catch (...) {
		// no default database set; fall back to the first attached lake
	}

	MetadataLocation default_lake, first_lake, prefixed;
	bool has_default_lake = false;
	bool has_first_lake = false;
	for (auto &db : db_manager.GetDatabases(context)) {
		if (db->IsSystem() || db->IsTemporary()) {
			continue;
		}
		// Catalog names are Identifiers, which convert to a string only explicitly
		const string &name = static_cast<const string &>(db->GetName());
		if (db->GetCatalog().GetCatalogType() == "ducklake") {
			MetadataLocation loc;
			loc.catalog = "__ducklake_metadata_" + name;
			loc.lake = name;
			// ATTACH option keys are lowercased by the parser; DuckLake leaves them in the map
			auto &options = db->GetAttachOptions();
			auto catalog_opt = options.find("metadata_catalog");
			if (catalog_opt != options.end() && !catalog_opt->second.IsNull()) {
				loc.catalog = catalog_opt->second.ToString();
			}
			auto schema_opt = options.find("metadata_schema");
			if (schema_opt != options.end() && !schema_opt->second.IsNull()) {
				loc.schema = schema_opt->second.ToString();
			}
			if (!has_first_lake) {
				first_lake = loc;
				has_first_lake = true;
			}
			if (name == default_db) {
				default_lake = loc;
				has_default_lake = true;
			}
		} else if (prefixed.catalog.empty() && StringUtil::StartsWith(name, "__ducklake_metadata_")) {
			// Older DuckLake (visible metadata catalog) or a manually attached metadata database
			prefixed.catalog = name;
		}
	}
	if (has_default_lake) {
		return default_lake; // USE lake -> that lake
	}
	if (has_first_lake) {
		return first_lake; // no USE (dbt) -> first attached lake
	}
	return prefixed; // may be empty: metadata database attached directly, unqualified names
}

//===--------------------------------------------------------------------===//
// Helper: Qualify ducklake_* table names with the metadata catalog/schema prefix
//===--------------------------------------------------------------------===//
static string QualifyMetadataTables(const string &sql, const MetadataLocation &meta) {
	string prefix;
	if (!meta.catalog.empty()) {
		prefix += "\"" + meta.catalog + "\".";
	}
	if (!meta.schema.empty()) {
		prefix += "\"" + meta.schema + "\".";
	}
	return StringUtil::Replace(sql, "{META}", prefix);
}

//===--------------------------------------------------------------------===//
// Helper: Execute SQL and throw on error
//===--------------------------------------------------------------------===//
// A completed query's rows, read by column and row. DuckDB 2.0 has no MaterializedQueryResult, and
// ColumnDataCollection::GetValue materializes every row on each call, so the rows are materialized once here.
class QueryRows {
public:
	explicit QueryRows(unique_ptr<QueryResult> result_p) : result(std::move(result_p)) {
	}
	bool HasError() const {
		return result->HasError();
	}
	const string &GetError() const {
		return result->GetError();
	}
	ExceptionType GetErrorType() const {
		return result->GetErrorType();
	}
	idx_t RowCount() {
		return result->RowCount();
	}
	Value GetValue(idx_t col, idx_t row) {
		if (!rows) {
			rows = make_uniq<ColumnDataRowCollection>(result->Collection().GetRows());
		}
		return rows->GetValue(col, row);
	}

private:
	unique_ptr<QueryResult> result;
	unique_ptr<ColumnDataRowCollection> rows;
};

// Connection::Query completes every statement before it returns, so results can be held while others run
static unique_ptr<QueryRows> RunSQL(Connection &conn, const string &sql) {
	return make_uniq<QueryRows>(conn.Query(sql));
}

static unique_ptr<QueryRows> ExecuteSQL(Connection &conn, const string &sql) {
	auto result = RunSQL(conn, sql);
	if (result->HasError()) {
		throw InvalidInputException("Delta export failed: %s\nSQL: %s", result->GetError(), sql.substr(0, 200));
	}
	return result;
}

template <class T>
static T ResultValue(QueryRows &result, idx_t col, idx_t row, const char *what) {
	auto value = result.GetValue(col, row);
	if (value.IsNull()) {
		throw InvalidInputException("Delta export failed: unexpected NULL in %s, column %d, row %d", what,
		                            NumericCast<int64_t>(col), NumericCast<int64_t>(row));
	}
	return value.GetValue<T>();
}

// Keeps a table out of this export; the status and message in the result say why. The first reason wins.
static void HoldBack(Connection &conn, int64_t table_id, const string &status, const string &message) {
	auto update = conn.Prepare(
	    "UPDATE export_summary SET status = $1, message = $2 WHERE table_id = $3 AND status = 'needs_export'");
	if (update->HasError()) {
		throw InvalidInputException("Delta export failed: %s", update->GetError());
	}
	auto result = update->Execute(Value(status), Value(message), Value::BIGINT(table_id));
	if (result->HasError()) {
		throw InvalidInputException("Delta export failed: %s", result->GetError());
	}
}

//===--------------------------------------------------------------------===//
// Helper: Build the Delta Lake schemaString of each exported table
//===--------------------------------------------------------------------===//
struct DuckLakeColumn {
	int64_t column_id = 0;
	string name;
	string type;
	bool nullable = true;
	bool has_parent = false;
	int64_t parent_id = 0;
	//! Indexes (into the table's column vector) of the nested child columns, in column_order
	vector<idx_t> children;
	//! Narrower Delta types older data files still hold for this column (type widening)
	vector<string> type_history;
};

// DuckLake stores types as internal names (int32, varchar, ...). Delta has no equivalent for uuid, time,
// timetz, interval, int128 and uint64; those are exported as 'string', which is lossy: Delta readers will
// not decode the parquet values of such columns.
static string MapPrimitiveType(const string &column_type) {
	auto t = StringUtil::Lower(column_type);
	if (t == "int8" || t == "tinyint") {
		return "byte";
	}
	if (t == "int16" || t == "smallint") {
		return "short";
	}
	if (t == "int32" || t == "integer" || t == "int") {
		return "integer";
	}
	if (t == "int64" || t == "bigint") {
		return "long";
	}
	if (t == "uint8" || t == "utinyint") {
		return "short";
	}
	if (t == "uint16" || t == "usmallint") {
		return "integer";
	}
	if (t == "uint32" || t == "uinteger") {
		return "long";
	}
	if (t == "uint64" || t == "ubigint" || t == "int128" || t == "hugeint" || t == "uint128" || t == "uhugeint") {
		return "string";
	}
	if (t == "float32" || t == "float") {
		return "float";
	}
	if (t == "float64" || t == "double") {
		return "double";
	}
	if (t == "boolean") {
		return "boolean";
	}
	if (t == "date") {
		return "date";
	}
	if (t == "blob") {
		return "binary";
	}
	if (t == "varchar") {
		return "string";
	}
	if (StringUtil::StartsWith(t, "decimal")) {
		return StringUtil::Replace(t, " ", "");
	}
	if (StringUtil::Contains(t, "timestamp")) {
		return "timestamp";
	}
	// json is stored as a string in parquet; uuid, time, timetz and interval have no Delta equivalent
	return "string";
}

static string JsonEscape(const string &input) {
	static const char *HEX = "0123456789abcdef";
	string out;
	out.reserve(input.size() + 2);
	for (auto ch : input) {
		auto c = static_cast<unsigned char>(ch);
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		case '\n':
			out += "\\n";
			break;
		case '\r':
			out += "\\r";
			break;
		case '\t':
			out += "\\t";
			break;
		default:
			if (c < 0x20) {
				out += "\\u00";
				out += HEX[c >> 4];
				out += HEX[c & 0xF];
			} else {
				out += ch;
			}
		}
	}
	return out;
}

static string BuildDeltaType(const vector<DuckLakeColumn> &columns, idx_t idx);

static bool ParseDeltaDecimal(const string &type, int &precision, int &scale) {
	if (!StringUtil::StartsWith(type, "decimal(") || type.back() != ')') {
		return false;
	}
	auto parts = StringUtil::Split(type.substr(8, type.size() - 9), ",");
	if (parts.size() != 2) {
		return false;
	}
	precision = std::stoi(parts[0]);
	scale = std::stoi(parts[1]);
	return true;
}

// Order of Delta types along the widenings Delta allows (byte < short < integer < long, float < double, decimals)
static int DeltaTypeRank(const string &type) {
	static const vector<string> order = {"byte", "short", "integer", "long", "float", "double"};
	for (idx_t i = 0; i < order.size(); i++) {
		if (order[i] == type) {
			return NumericCast<int>(i);
		}
	}
	int precision, scale;
	return ParseDeltaDecimal(type, precision, scale) ? 100 + precision * 100 + scale : 100000;
}

//! Whether Delta's typeWidening feature lets files of type `from` be read as `to`
static bool IsDeltaWidening(const string &from, const string &to) {
	static const vector<string> integral = {"byte", "short", "integer", "long"};
	static const int integral_digits[] = {3, 5, 10, 20};
	auto rank = [&](const string &t) {
		auto it = std::find(integral.begin(), integral.end(), t);
		return it == integral.end() ? -1 : NumericCast<int>(it - integral.begin());
	};
	int from_rank = rank(from), to_rank = rank(to);
	if (from_rank >= 0 && to_rank >= 0) {
		return from_rank < to_rank;
	}
	if (from == "float" && to == "double") {
		return true;
	}
	if (from_rank >= 0 && from_rank <= 2 && to == "double") {
		return true;
	}
	int p1, s1, p2, s2;
	bool from_decimal = ParseDeltaDecimal(from, p1, s1);
	bool to_decimal = ParseDeltaDecimal(to, p2, s2);
	if (from_decimal && to_decimal) {
		return s2 >= s1 && p2 - s2 >= p1 - s1;
	}
	if (from_rank >= 0 && to_decimal) {
		return p2 - s2 >= integral_digits[from_rank];
	}
	return false;
}

// delta.typeChanges: the widening steps older data files went through, oldest first
static string BuildFieldMetadata(const DuckLakeColumn &col) {
	if (col.type_history.empty()) {
		return "{}";
	}
	auto chain = col.type_history;
	std::sort(chain.begin(), chain.end(),
	          [](const string &a, const string &b) { return DeltaTypeRank(a) < DeltaTypeRank(b); });
	chain.push_back(MapPrimitiveType(col.type));
	string changes;
	for (idx_t i = 0; i + 1 < chain.size(); i++) {
		if (!changes.empty()) {
			changes += ",";
		}
		changes += "{\"fromType\":\"" + chain[i] + "\",\"toType\":\"" + chain[i + 1] + "\"}";
	}
	return "{\"delta.typeChanges\":[" + changes + "]}";
}

static string BuildDeltaField(const vector<DuckLakeColumn> &columns, idx_t idx) {
	auto &col = columns[idx];
	return "{\"name\":\"" + JsonEscape(col.name) + "\",\"type\":" + BuildDeltaType(columns, idx) +
	       ",\"nullable\":" + (col.nullable ? "true" : "false") + ",\"metadata\":" + BuildFieldMetadata(col) + "}";
}

static string BuildDeltaType(const vector<DuckLakeColumn> &columns, idx_t idx) {
	auto &col = columns[idx];
	auto t = StringUtil::Lower(col.type);
	if (t == "struct") {
		string fields;
		for (auto child : col.children) {
			if (!fields.empty()) {
				fields += ",";
			}
			fields += BuildDeltaField(columns, child);
		}
		return "{\"type\":\"struct\",\"fields\":[" + fields + "]}";
	}
	if (t == "list") {
		if (col.children.empty()) {
			throw InvalidInputException("Delta export failed: list column \"%s\" has no element column", col.name);
		}
		auto &element = columns[col.children[0]];
		return "{\"type\":\"array\",\"elementType\":" + BuildDeltaType(columns, col.children[0]) +
		       ",\"containsNull\":" + (element.nullable ? "true" : "false") + "}";
	}
	if (t == "map") {
		idx_t key_idx = DConstants::INVALID_INDEX;
		idx_t value_idx = DConstants::INVALID_INDEX;
		for (auto child : col.children) {
			if (columns[child].name == "key" && key_idx == DConstants::INVALID_INDEX) {
				key_idx = child;
			} else if (columns[child].name == "value" && value_idx == DConstants::INVALID_INDEX) {
				value_idx = child;
			}
		}
		if (key_idx == DConstants::INVALID_INDEX || value_idx == DConstants::INVALID_INDEX) {
			if (col.children.size() != 2) {
				throw InvalidInputException("Delta export failed: map column \"%s\" has no key/value columns",
				                            col.name);
			}
			key_idx = col.children[0];
			value_idx = col.children[1];
		}
		auto &value = columns[value_idx];
		return "{\"type\":\"map\",\"keyType\":" + BuildDeltaType(columns, key_idx) +
		       ",\"valueType\":" + BuildDeltaType(columns, value_idx) +
		       ",\"valueContainsNull\":" + (value.nullable ? "true" : "false") + "}";
	}
	return "\"" + MapPrimitiveType(t) + "\"";
}

static string BuildDeltaSchema(vector<DuckLakeColumn> &columns) {
	// Link nested child columns to their parents (columns arrive in column_order)
	std::map<int64_t, idx_t> index_of;
	for (idx_t i = 0; i < columns.size(); i++) {
		index_of[columns[i].column_id] = i;
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		if (!columns[i].has_parent) {
			continue;
		}
		auto parent = index_of.find(columns[i].parent_id);
		if (parent == index_of.end()) {
			throw InvalidInputException("Delta export failed: column \"%s\" references unknown parent column %s",
			                            columns[i].name, std::to_string(columns[i].parent_id));
		}
		columns[parent->second].children.push_back(i);
	}
	string fields;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (columns[i].has_parent) {
			continue;
		}
		if (!fields.empty()) {
			fields += ",";
		}
		fields += BuildDeltaField(columns, i);
	}
	return "{\"type\":\"struct\",\"fields\":[" + fields + "]}";
}

// Fills TEMP table table_schema_json(table_id, schema_string). TEMP tables are per connection, so this must
// run on the same Connection as the rest of the export.
//! (table_id, column_id) -> narrower Delta types older data files still hold
using WidenedColumns = std::map<std::pair<int64_t, int64_t>, std::set<string>>;

// Delta forbids these in column names unless column mapping is on, and the Fabric SQL analytics endpoint only
// supports column mapping by name, which cannot describe DuckLake files anyway
static constexpr const char *DELTA_INVALID_NAME_CHARS = " ,;{}()\n\t=";

static void BuildTableSchemas(Connection &conn, const MetadataLocation &meta, const WidenedColumns &widened) {
	ExecuteSQL(conn, "CREATE OR REPLACE TEMP TABLE table_schema_json (table_id BIGINT, schema_string VARCHAR, "
	                 "type_widening BOOLEAN);");
	auto columns_result = ExecuteSQL(conn, QualifyMetadataTables(SQL_GET_EXPORT_COLUMNS, meta));

	std::map<int64_t, vector<DuckLakeColumn>> table_columns;
	for (idx_t i = 0; i < columns_result->RowCount(); i++) {
		auto table_id = ResultValue<int64_t>(*columns_result, 0, i, "columns_result");
		DuckLakeColumn col;
		col.column_id = ResultValue<int64_t>(*columns_result, 1, i, "columns_result");
		col.name = columns_result->GetValue(3, i).ToString();
		col.type = columns_result->GetValue(4, i).ToString();
		col.nullable = ResultValue<bool>(*columns_result, 5, i, "columns_result");
		auto parent = columns_result->GetValue(6, i);
		if (!parent.IsNull()) {
			col.has_parent = true;
			col.parent_id = parent.GetValue<int64_t>();
		}
		auto history = widened.find(std::make_pair(table_id, col.column_id));
		if (history != widened.end()) {
			col.type_history.assign(history->second.begin(), history->second.end());
		}
		table_columns[table_id].push_back(std::move(col));
	}

	auto insert = conn.Prepare("INSERT INTO table_schema_json VALUES ($1, $2, $3)");
	if (insert->HasError()) {
		throw InvalidInputException("Delta export failed: %s", insert->GetError());
	}
	for (auto &entry : table_columns) {
		auto &columns = entry.second;
		string invalid_name;
		for (auto &col : columns) {
			if (col.name.find_first_of(DELTA_INVALID_NAME_CHARS) != string::npos) {
				invalid_name = col.name;
				break;
			}
		}
		if (!invalid_name.empty()) {
			HoldBack(conn, entry.first, "unsupported_column_name",
			         StringUtil::Format("column \"%s\" contains a character Delta only allows with column mapping "
			                            "(space , ; { } ( ) = tab or newline); rename the column",
			                            invalid_name));
			continue;
		}
		auto schema_string = BuildDeltaSchema(columns);

		std::map<int64_t, idx_t> index_of;
		for (idx_t i = 0; i < columns.size(); i++) {
			index_of[columns[i].column_id] = i;
		}
		bool type_widening = false;
		string widened_element;
		for (auto &col : columns) {
			if (col.type_history.empty()) {
				continue;
			}
			type_widening = true;
			if (col.has_parent) {
				auto parent_type = StringUtil::Lower(columns[index_of[col.parent_id]].type);
				if (parent_type == "list" || parent_type == "map") {
					widened_element = columns[index_of[col.parent_id]].name;
				}
			}
		}
		if (!widened_element.empty()) {
			HoldBack(conn, entry.first, "needs_rewrite",
			         StringUtil::Format("the element type of column \"%s\" was widened; older data files hold the "
			                            "narrower type. Rewrite the table, e.g. CREATE OR REPLACE TABLE t AS SELECT * "
			                            "FROM t",
			                            widened_element));
			continue;
		}
		auto result = insert->Execute(Value::BIGINT(entry.first), Value(schema_string), Value::BOOLEAN(type_widening));
		if (result->HasError()) {
			throw InvalidInputException("Delta export failed: %s", result->GetError());
		}
	}
}

//===--------------------------------------------------------------------===//
// Table Function: delta_export
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// Helper: paths and files
//===--------------------------------------------------------------------===//
static bool IsRemotePath(const string &path) {
	return StringUtil::Contains(path, "://");
}

// Forward slashes, no duplicate or trailing slash, no leading "./" — for comparing storage roots
static string NormalizePath(string path) {
	std::replace(path.begin(), path.end(), '\\', '/');
	string out;
	for (auto ch : path) {
		if (ch == '/' && !out.empty() && out.back() == '/' && !(out.size() >= 2 && out[out.size() - 2] == ':')) {
			continue;
		}
		out += ch;
	}
	while (out.size() > 1 && out.back() == '/') {
		out.pop_back();
	}
	if (StringUtil::StartsWith(out, "./")) {
		out = out.substr(2);
	}
	return out;
}

static string BaseName(const string &path) {
	auto pos = path.find_last_of("/\\");
	return pos == string::npos ? path : path.substr(pos + 1);
}

static string EscapeSqlString(const string &input) {
	return StringUtil::Replace(input, "'", "''");
}

//! abfs[s]://<container>@<account>.dfs.<suffix>/<path> -> az://<account>.blob.<suffix>/<container>/<path>, or ""
static string DfsToBlobUrl(const string &path) {
	for (auto prefix : {"abfss://", "abfs://"}) {
		if (!StringUtil::StartsWith(path, prefix)) {
			continue;
		}
		auto rest = path.substr(strlen(prefix));
		auto at = rest.find('@');
		auto slash = rest.find('/', at == string::npos ? 0 : at);
		if (at == string::npos || slash == string::npos) {
			return string();
		}
		auto host = rest.substr(at + 1, slash - at - 1);
		auto dfs = host.find(".dfs.");
		if (dfs == string::npos) {
			return string();
		}
		return "az://" + host.substr(0, dfs) + ".blob." + host.substr(dfs + 5) + "/" + rest.substr(0, at) +
		       rest.substr(slash);
	}
	return string();
}

static bool FileStillExists(FileSystem &fs, const string &path) {
	try {
		return fs.FileExists(path);
	} catch (std::exception &) {
		return true;
	}
}

// OneLake answers DFS deletes with a body the Azure SDK cannot parse ("[json.exception.type_error.302] type must
// be string"), even when the delete went through. Judge by whether the file is still there, and retry through
// the blob endpoint (the one delta-rs uses on OneLake) when it is.
static void RemoveFileWithFallback(FileSystem &fs, const string &path) {
	string first_error;
	try {
		fs.RemoveFile(path);
		return;
	} catch (std::exception &ex) {
		if (!FileStillExists(fs, path)) {
			return;
		}
		first_error = ErrorData(ex).Message();
	}
	auto blob_url = DfsToBlobUrl(path);
	if (blob_url.empty()) {
		throw IOException("could not delete %s: %s", path, first_error);
	}
	try {
		fs.RemoveFile(blob_url);
	} catch (std::exception &ex) {
		if (FileStillExists(fs, path)) {
			throw IOException("could not delete %s: %s; through the blob endpoint: %s", path, first_error,
			                  ErrorData(ex).Message());
		}
	}
}

//! az://<account>.blob.<suffix>/<container>/<path> -> abfss://<container>@<account>.dfs.<suffix>/<path>, or ""
static string BlobToDfsUrl(const string &path) {
	if (!StringUtil::StartsWith(path, "az://")) {
		return string();
	}
	auto rest = path.substr(5);
	auto host_end = rest.find('/');
	auto blob = rest.find(".blob.");
	if (host_end == string::npos || blob == string::npos || blob > host_end) {
		return string();
	}
	auto container_end = rest.find('/', host_end + 1);
	auto container =
	    rest.substr(host_end + 1, container_end == string::npos ? string::npos : container_end - host_end - 1);
	auto host = rest.substr(0, blob) + ".dfs." + rest.substr(blob + 6, host_end - blob - 6);
	return "abfss://" + container + "@" + host +
	       (container_end == string::npos ? string() : rest.substr(container_end));
}

// Some OneLake DFS listings come back in a shape the Azure SDK cannot parse ("type must be string, but is
// number"); list the same folder through the blob endpoint then, and name the hits as the DFS paths callers use
static vector<OpenFileInfo> GlobWithFallback(FileSystem &fs, const string &pattern) {
	try {
		return fs.Glob(pattern);
	} catch (std::exception &) {
		auto blob_pattern = DfsToBlobUrl(pattern);
		if (blob_pattern.empty()) {
			throw;
		}
		auto hits = fs.Glob(blob_pattern);
		for (auto &hit : hits) {
			auto dfs = BlobToDfsUrl(hit.path);
			if (!dfs.empty()) {
				hit.path = dfs;
			}
		}
		return hits;
	}
}

static vector<OpenFileInfo> GlobOrEmpty(FileSystem &fs, const string &pattern) {
	try {
		return GlobWithFallback(fs, pattern);
	} catch (std::exception &) {
		return {};
	}
}

//! Delta version of a _delta_log file name (00000000000000000012.json, .checkpoint.parquet, ...), or -1
static int64_t DeltaVersionOf(const string &file_name) {
	if (file_name.size() < 21 || file_name[20] != '.') {
		return -1;
	}
	for (idx_t i = 0; i < 20; i++) {
		if (!StringUtil::CharacterIsDigit(file_name[i])) {
			return -1;
		}
	}
	return std::stoll(file_name.substr(0, 20));
}

//! Highest version already present in a _delta_log, or -1 when there is none
static int64_t ExistingMaxDeltaVersion(FileSystem &fs, const string &delta_log) {
	int64_t max_version = -1;
	for (auto &file : GlobOrEmpty(fs, delta_log + "/*")) {
		max_version = MaxValue<int64_t>(max_version, DeltaVersionOf(BaseName(file.path)));
	}
	return max_version;
}

static string DeltaLogFile(const string &delta_log, int64_t version, const string &suffix) {
	auto v = std::to_string(version);
	return delta_log + "/" + string(20 - MinValue<idx_t>(v.size(), 20), '0') + v + suffix;
}

//===--------------------------------------------------------------------===//
// Helper: Delta deletion vectors
//===--------------------------------------------------------------------===//
// Delta PROTOCOL.md "Deletion Vector Format": a 64-bit RoaringTreemap in the portable format, prefixed with a magic
// number, stored in a file as [version byte 1] then per vector [size int32 BE][bitmap][CRC-32 int32 BE].
static constexpr uint32_t DV_PORTABLE_MAGIC = 1681511377;
static constexpr uint32_t ROARING_SERIAL_COOKIE_NO_RUNCONTAINER = 12346;
static constexpr idx_t ROARING_ARRAY_CONTAINER_MAX = 4096;

static void AppendLE16(string &out, uint16_t v) {
	out += static_cast<char>(v & 0xFF);
	out += static_cast<char>((v >> 8) & 0xFF);
}

static void AppendLE32(string &out, uint32_t v) {
	for (idx_t i = 0; i < 4; i++) {
		out += static_cast<char>((v >> (8 * i)) & 0xFF);
	}
}

static void AppendLE64(string &out, uint64_t v) {
	for (idx_t i = 0; i < 8; i++) {
		out += static_cast<char>((v >> (8 * i)) & 0xFF);
	}
}

static void AppendBE32(string &out, uint32_t v) {
	for (idx_t i = 0; i < 4; i++) {
		out += static_cast<char>((v >> (8 * (3 - i))) & 0xFF);
	}
}

// 32-bit RoaringBitmap portable serialization with array and bitmap containers only (no run containers)
static string SerializeRoaring32(const vector<uint32_t> &values) {
	struct Container {
		uint16_t key;
		vector<uint16_t> lows;
	};
	vector<Container> containers;
	for (auto value : values) {
		auto key = static_cast<uint16_t>(value >> 16);
		if (containers.empty() || containers.back().key != key) {
			containers.push_back(Container {key, {}});
		}
		containers.back().lows.push_back(static_cast<uint16_t>(value & 0xFFFF));
	}
	string out;
	AppendLE32(out, ROARING_SERIAL_COOKIE_NO_RUNCONTAINER);
	AppendLE32(out, static_cast<uint32_t>(containers.size()));
	for (auto &c : containers) {
		AppendLE16(out, c.key);
		AppendLE16(out, static_cast<uint16_t>(c.lows.size() - 1));
	}
	// offset header: byte offset of each container from the start of this bitmap
	uint32_t offset = static_cast<uint32_t>(8 + 8 * containers.size());
	for (auto &c : containers) {
		AppendLE32(out, offset);
		offset += c.lows.size() <= ROARING_ARRAY_CONTAINER_MAX ? static_cast<uint32_t>(2 * c.lows.size()) : 8192;
	}
	for (auto &c : containers) {
		if (c.lows.size() <= ROARING_ARRAY_CONTAINER_MAX) {
			for (auto low : c.lows) {
				AppendLE16(out, low);
			}
		} else {
			std::array<uint64_t, 1024> words {};
			for (auto low : c.lows) {
				words[low >> 6] |= uint64_t(1) << (low & 63);
			}
			for (auto word : words) {
				AppendLE64(out, word);
			}
		}
	}
	return out;
}

//! positions must be sorted and unique
static string SerializeDeletionVector(const vector<uint64_t> &positions) {
	string out;
	AppendLE32(out, DV_PORTABLE_MAGIC);
	vector<std::pair<uint32_t, vector<uint32_t>>> buckets;
	for (auto pos : positions) {
		auto high = static_cast<uint32_t>(pos >> 32);
		if (buckets.empty() || buckets.back().first != high) {
			buckets.emplace_back(high, vector<uint32_t>());
		}
		buckets.back().second.push_back(static_cast<uint32_t>(pos & 0xFFFFFFFF));
	}
	AppendLE64(out, buckets.size());
	for (auto &bucket : buckets) {
		AppendLE32(out, bucket.first);
		out += SerializeRoaring32(bucket.second);
	}
	return out;
}

static uint32_t Crc32(const string &data) {
	static const auto table = [] {
		std::array<uint32_t, 256> t {};
		for (uint32_t i = 0; i < 256; i++) {
			uint32_t c = i;
			for (idx_t k = 0; k < 8; k++) {
				c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
			}
			t[i] = c;
		}
		return t;
	}();
	uint32_t crc = 0xFFFFFFFFu;
	for (auto ch : data) {
		crc = table[(crc ^ static_cast<uint8_t>(ch)) & 0xFF] ^ (crc >> 8);
	}
	return crc ^ 0xFFFFFFFFu;
}

// Z85, the base85 variant Delta uses to encode the deletion vector file uuid in pathOrInlineDv
static string Z85Encode(const uint8_t *data, idx_t len) {
	static const char *Z85_CHARS =
	    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
	string out;
	for (idx_t i = 0; i + 4 <= len; i += 4) {
		uint32_t value = (uint32_t(data[i]) << 24) | (uint32_t(data[i + 1]) << 16) | (uint32_t(data[i + 2]) << 8) |
		                 uint32_t(data[i + 3]);
		char chunk[5];
		for (int j = 4; j >= 0; j--) {
			chunk[j] = Z85_CHARS[value % 85];
			value /= 85;
		}
		out.append(chunk, 5);
	}
	return out;
}

static string FormatUUID(const uint8_t *bytes) {
	static const char *HEX = "0123456789abcdef";
	string out;
	for (idx_t i = 0; i < 16; i++) {
		if (i == 4 || i == 6 || i == 8 || i == 10) {
			out += '-';
		}
		out += HEX[bytes[i] >> 4];
		out += HEX[bytes[i] & 0xF];
	}
	return out;
}

// Fills TEMP table table_deletion_vectors(data_file_id, ...) from the active DuckLake delete files and the inlined
// deletes, writing one deletion_vector_<uuid>.bin per table into its root. dv_files receives table_id -> file path.
static void BuildDeletionVectors(Connection &conn, FileSystem &fs, const MetadataLocation &meta,
                                 std::map<int64_t, string> &dv_files) {
	ExecuteSQL(conn, "CREATE OR REPLACE TEMP TABLE table_deletion_vectors (data_file_id BIGINT, storage_type VARCHAR, "
	                 "path_or_inline_dv VARCHAR, dv_offset INTEGER, size_in_bytes INTEGER, cardinality BIGINT);");

	std::map<int64_t, string> roots;
	auto tables = ExecuteSQL(conn, "SELECT table_id, table_root FROM export_summary WHERE status = 'needs_export'");
	for (idx_t i = 0; i < tables->RowCount(); i++) {
		roots[ResultValue<int64_t>(*tables, 0, i, "tables")] = tables->GetValue(1, i).ToString();
	}

	// table_id -> data_file_id -> deleted row positions within the data file
	std::map<int64_t, std::map<int64_t, vector<uint64_t>>> deletes;
	auto delete_files = ExecuteSQL(conn, QualifyMetadataTables(SQL_GET_DELETE_FILES, meta));
	for (idx_t i = 0; i < delete_files->RowCount(); i++) {
		auto table_id = ResultValue<int64_t>(*delete_files, 0, i, "delete_files");
		auto data_file_id = ResultValue<int64_t>(*delete_files, 1, i, "delete_files");
		auto path = delete_files->GetValue(2, i).ToString();
		auto positions = ExecuteSQL(conn, "SELECT pos FROM read_parquet('" + EscapeSqlString(path) + "')");
		auto &target = deletes[table_id][data_file_id];
		for (idx_t r = 0; r < positions->RowCount(); r++) {
			target.push_back(ResultValue<uint64_t>(*positions, 0, r, "positions"));
		}
	}
	// Deletes DuckLake keeps in the metadata catalog until ducklake_flush_inlined_data; row_id is the file position
	for (auto &root : roots) {
		auto tid = std::to_string(root.first);
		auto inlined = RunSQL(
		    conn, QualifyMetadataTables(
		              "SELECT d.file_id, d.row_id FROM {META}ducklake_inlined_delete_" + tid +
		                  " d JOIN {META}ducklake_data_file df ON df.data_file_id = d.file_id WHERE df.table_id = " +
		                  tid + " AND df.end_snapshot IS NULL",
		              meta));
		if (inlined->HasError()) {
			if (inlined->GetErrorType() == ExceptionType::CATALOG) {
				continue; // the table never had inlined deletes
			}
			throw InvalidInputException("Delta export failed: %s", inlined->GetError());
		}
		for (idx_t r = 0; r < inlined->RowCount(); r++) {
			deletes[root.first][ResultValue<int64_t>(*inlined, 0, r, "inlined")].push_back(
			    ResultValue<uint64_t>(*inlined, 1, r, "inlined"));
		}
	}
	if (deletes.empty()) {
		return;
	}

	auto insert = conn.Prepare("INSERT INTO table_deletion_vectors VALUES ($1, 'u', $2, $3, $4, $5)");
	if (insert->HasError()) {
		throw InvalidInputException("Delta export failed: %s", insert->GetError());
	}
	RandomEngine random;
	for (auto &table : deletes) {
		auto &table_root = roots[table.first];
		uint8_t uuid[16];
		for (idx_t i = 0; i < 16; i += 4) {
			auto r = random.NextRandomInteger();
			memcpy(uuid + i, &r, 4);
		}
		uuid[6] = (uuid[6] & 0x0F) | 0x40; // version 4
		uuid[8] = (uuid[8] & 0x3F) | 0x80; // RFC 4122 variant
		auto path_or_inline_dv = Z85Encode(uuid, 16);

		struct Entry {
			int64_t data_file_id;
			idx_t offset;
			idx_t size;
			idx_t cardinality;
		};
		vector<Entry> entries;
		string file(1, '\x01');
		for (auto &data_file : table.second) {
			auto &positions = data_file.second;
			std::sort(positions.begin(), positions.end());
			positions.erase(std::unique(positions.begin(), positions.end()), positions.end());
			if (positions.empty()) {
				continue;
			}
			auto bitmap = SerializeDeletionVector(positions);
			// offset points at the size prefix of this vector
			entries.push_back(Entry {data_file.first, file.size(), bitmap.size(), positions.size()});
			AppendBE32(file, static_cast<uint32_t>(bitmap.size()));
			file += bitmap;
			AppendBE32(file, Crc32(bitmap));
		}
		if (entries.empty()) {
			continue;
		}

		auto dv_path = table_root + "/deletion_vector_" + FormatUUID(uuid) + ".bin";
		if (!IsRemotePath(table_root) && !fs.DirectoryExists(table_root)) {
			fs.CreateDirectoriesRecursive(table_root);
		}
		{
			auto handle = fs.OpenFile(dv_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
			handle->Write(&file[0], file.size());
			handle->Close();
		}
		dv_files[table.first] = dv_path;

		for (auto &entry : entries) {
			auto result = insert->Execute(Value::BIGINT(entry.data_file_id), Value(path_or_inline_dv),
			                              Value::INTEGER(NumericCast<int32_t>(entry.offset)),
			                              Value::INTEGER(NumericCast<int32_t>(entry.size)),
			                              Value::BIGINT(NumericCast<int64_t>(entry.cardinality)));
			if (result->HasError()) {
				throw InvalidInputException("Delta export failed: %s", result->GetError());
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Helper: remove the Delta logs of tables dropped in DuckLake
//===--------------------------------------------------------------------===//
// A log is ours when delta_export wrote its newest commit for this DuckLake schema. Logs written before the schema
// uuid was recorded are only trusted when the catalog itself still lists the dropped table at that root.
static bool IsOwnedDeltaLog(const string &commit, const string &schema_uuid, bool listed_in_catalog) {
	if (!StringUtil::Contains(commit, "\"engineInfo\":\"DuckLake-Delta-Exporter")) {
		return false;
	}
	if (!StringUtil::Contains(commit, "\"duckLakeSchemaUuid\":")) {
		return listed_in_catalog;
	}
	return !schema_uuid.empty() && StringUtil::Contains(commit, "\"duckLakeSchemaUuid\":\"" + schema_uuid + "\"");
}

static void RemoveDeltaLog(FileSystem &fs, const string &table_root) {
	auto delta_log = table_root + "/_delta_log";
	for (auto &file : GlobWithFallback(fs, delta_log + "/*")) {
		if (!FileSystem::IsDirectory(file)) {
			RemoveFileWithFallback(fs, file.path);
		}
	}
	for (auto &file : GlobWithFallback(fs, table_root + "/deletion_vector_*.bin")) {
		RemoveFileWithFallback(fs, file.path);
	}
	if (!IsRemotePath(table_root) && fs.DirectoryExists(delta_log)) {
		fs.RemoveDirectory(delta_log);
	}
}

// Adds one export_summary row per removed (or failed) log. Candidates are dropped ducklake_table rows plus
// exported logs under this catalog's schema folders whose table is gone (its rows may be expired already).
// A non-empty only_schemas limits both to those schemas, whose live tables must all be in export_summary.
static void RemoveDroppedTableLogs(Connection &conn, FileSystem &fs, const std::set<string> &only_schemas) {
	std::set<string> live_roots;
	auto live = ExecuteSQL(conn, "SELECT table_root FROM export_summary WHERE table_root IS NOT NULL");
	for (idx_t i = 0; i < live->RowCount(); i++) {
		live_roots.insert(NormalizePath(live->GetValue(0, i).ToString()));
	}

	struct DroppedTable {
		string table_root;
		Value table_id;
		string schema_name;
		string table_name;
		string schema_uuid;
		bool listed_in_catalog;
	};
	std::map<string, DroppedTable> candidates;
	auto dropped = ExecuteSQL(conn, SQL_GET_DROPPED_TABLES);
	for (idx_t i = 0; i < dropped->RowCount(); i++) {
		auto root = dropped->GetValue(0, i).ToString();
		auto key = NormalizePath(root);
		if (live_roots.count(key) ||
		    (!only_schemas.empty() && !only_schemas.count(dropped->GetValue(2, i).ToString()))) {
			continue;
		}
		auto schema_uuid = dropped->GetValue(4, i);
		candidates[key] = DroppedTable {root,
		                                dropped->GetValue(1, i),
		                                dropped->GetValue(2, i).ToString(),
		                                dropped->GetValue(3, i).ToString(),
		                                schema_uuid.IsNull() ? string() : schema_uuid.ToString(),
		                                true};
	}
	auto schemas = ExecuteSQL(conn, "SELECT DISTINCT schema_name, schema_uuid, schema_root FROM schema_roots "
	                                "WHERE schema_root IS NOT NULL AND schema_uuid IS NOT NULL");
	const string checkpoint_suffix = "/_delta_log/_last_checkpoint";
	for (idx_t i = 0; i < schemas->RowCount(); i++) {
		if (!only_schemas.empty() && !only_schemas.count(schemas->GetValue(0, i).ToString())) {
			continue;
		}
		auto schema_root = schemas->GetValue(2, i).ToString();
		auto schema_prefix = NormalizePath(schema_root) + "/";
		for (auto &hit : GlobOrEmpty(fs, schema_root + "/*" + checkpoint_suffix)) {
			auto path = NormalizePath(hit.path);
			// a listing that fell back to the blob endpoint names paths differently from the live table roots;
			// never take those for dropped tables
			if (!StringUtil::StartsWith(path, schema_prefix) || !StringUtil::EndsWith(path, checkpoint_suffix)) {
				continue;
			}
			auto root = path.substr(0, path.size() - checkpoint_suffix.size());
			if (live_roots.count(root) || candidates.count(root)) {
				continue;
			}
			candidates[root] =
			    DroppedTable {root,           Value(LogicalType::BIGINT),         schemas->GetValue(0, i).ToString(),
			                  BaseName(root), schemas->GetValue(1, i).ToString(), false};
		}
	}
	if (candidates.empty()) {
		return;
	}

	auto insert = conn.Prepare("INSERT INTO export_summary VALUES ($1, $2, $3, $4, $5, NULL, $6, NULL, $7)");
	if (insert->HasError()) {
		throw InvalidInputException("Delta export failed: %s", insert->GetError());
	}
	for (auto &entry : candidates) {
		auto &table = entry.second;
		auto delta_log = table.table_root + "/_delta_log";
		// newest commit file decides ownership
		string newest_commit;
		int64_t newest_version = -1;
		for (auto &file : GlobOrEmpty(fs, delta_log + "/*.json")) {
			auto version = DeltaVersionOf(BaseName(file.path));
			if (version > newest_version) {
				newest_version = version;
				newest_commit = file.path;
			}
		}
		if (newest_commit.empty()) {
			continue;
		}
		auto commit = RunSQL(conn, "SELECT content FROM read_text('" + EscapeSqlString(newest_commit) + "')");
		if (commit->HasError() || commit->RowCount() == 0 ||
		    !IsOwnedDeltaLog(commit->GetValue(0, 0).ToString(), table.schema_uuid, table.listed_in_catalog)) {
			continue;
		}

		string status = "removed_dropped";
		string message = "Removed " + delta_log;
		try {
			RemoveDeltaLog(fs, table.table_root);
		} catch (std::exception &ex) {
			status = "remove_failed";
			message = ErrorData(ex).Message();
		}
		auto result = insert->Execute(table.table_id, Value(table.schema_name), Value(table.table_name),
		                              Value(table.table_root), Value(status), Value(message),
		                              table.schema_uuid.empty() ? Value() : Value(table.schema_uuid));
		if (result->HasError()) {
			throw InvalidInputException("Delta export failed: %s", result->GetError());
		}
	}
}

//===--------------------------------------------------------------------===//
// Helper: schema evolution and emptied tables
//===--------------------------------------------------------------------===//
static const string REWRITE_HINT = "Rewrite the table so every file uses the current schema, e.g. CREATE OR REPLACE "
                                   "TABLE t AS SELECT * FROM t";

// Holds back tables whose files cannot be described to name-based readers (rename, drop + re-add, a type change
// Delta cannot widen) and returns the widened columns that the typeWidening feature covers.
static WidenedColumns AnalyzeColumnHistory(Connection &conn, const MetadataLocation &meta) {
	WidenedColumns widened;
	auto history = ExecuteSQL(conn, QualifyMetadataTables(SQL_GET_COLUMN_HISTORY, meta));
	for (idx_t i = 0; i < history->RowCount(); i++) {
		auto table_id = ResultValue<int64_t>(*history, 0, i, "history");
		auto column_id = ResultValue<int64_t>(*history, 1, i, "history");
		auto kind = history->GetValue(2, i).ToString();
		auto detail = history->GetValue(3, i).ToString();
		auto name = history->GetValue(4, i).ToString();
		auto type = history->GetValue(5, i).ToString();
		if (kind == "rename") {
			HoldBack(conn, table_id, "needs_rewrite",
			         StringUtil::Format("column \"%s\" was renamed from \"%s\"; data files written before the rename "
			                            "still use the old name, which readers that match columns by name (such as "
			                            "the Fabric SQL analytics endpoint) read as NULL. %s",
			                            name, detail, REWRITE_HINT));
		} else if (kind == "same_name") {
			HoldBack(conn, table_id, "needs_rewrite",
			         StringUtil::Format("column \"%s\" was dropped and added again; older data files still hold the "
			                            "dropped column under that name. %s",
			                            name, REWRITE_HINT));
		} else {
			auto from = MapPrimitiveType(detail);
			auto to = MapPrimitiveType(type);
			if (from == to) {
				continue; // same Delta type, e.g. both exported as string
			}
			if (IsDeltaWidening(from, to)) {
				widened[std::make_pair(table_id, column_id)].insert(from);
			} else {
				HoldBack(conn, table_id, "needs_rewrite",
				         StringUtil::Format("column \"%s\" changed type from %s to %s, which Delta type widening "
				                            "cannot express for the older data files. %s",
				                            name, detail, type, REWRITE_HINT));
			}
		}
	}
	return widened;
}

// An emptied table keeps the log of its last export; export it again as a version without files so readers
// stop showing the old rows. Empty tables that were never exported stay no_data_files.
static void MarkEmptiedExportedTables(Connection &conn, FileSystem &fs, const MetadataLocation &meta) {
	auto empty = ExecuteSQL(conn, "SELECT table_id, table_root FROM export_summary "
	                              "WHERE status = 'no_data_files' AND table_root IS NOT NULL");
	for (idx_t i = 0; i < empty->RowCount(); i++) {
		auto table_root = empty->GetValue(1, i).ToString();
		if (ExistingMaxDeltaVersion(fs, table_root + "/_delta_log") < 0) {
			continue;
		}
		ExecuteSQL(conn,
		           QualifyMetadataTables(
		               "UPDATE export_summary SET status = 'needs_export', "
		               "message = 'Table has no data files; exported as an empty Delta table', "
		               "snapshot_id = COALESCE(snapshot_id, (SELECT MAX(snapshot_id) FROM {META}ducklake_snapshot)) "
		               "WHERE table_id = " +
		                   empty->GetValue(0, i).ToString(),
		               meta));
	}
}

//===--------------------------------------------------------------------===//
// Helper: the previous version, to write incremental commits
//===--------------------------------------------------------------------===//
static constexpr const char *DV_STRUCT_NULL = "NULL::STRUCT(storageType VARCHAR, pathOrInlineDv VARCHAR, \"offset\" "
                                              "INTEGER, sizeInBytes INTEGER, cardinality BIGINT)";
// A logical file: its path plus the deletion vector applied to it
static constexpr const char *ADD_KEY_SQL = "add.path || '|' || COALESCE(add.deletionVector.pathOrInlineDv || ':' || "
                                           "CAST(add.deletionVector.\"offset\" AS VARCHAR), '')";

// Fills TEMP tables prev_adds(k, path, size, dv) and prev_meta(id, schema_string, configuration,
// protocol_versions) from the checkpoint of the previous version. False when it cannot be read.
static bool LoadPreviousState(Connection &conn, const string &checkpoint) {
	auto source = "read_parquet('" + EscapeSqlString(checkpoint) + "')";
	auto adds = RunSQL(conn, "CREATE OR REPLACE TEMP TABLE prev_adds AS SELECT " + string(ADD_KEY_SQL) +
	                             " AS k, add.path AS path, add.size AS size, add.deletionVector AS dv FROM " + source +
	                             " WHERE add IS NOT NULL");
	if (adds->HasError()) {
		// checkpoints written before deletion vectors have no add.deletionVector
		adds = RunSQL(conn, "CREATE OR REPLACE TEMP TABLE prev_adds AS SELECT add.path || '|' AS k, add.path AS path, "
		                    "add.size AS size, " +
		                        string(DV_STRUCT_NULL) + " AS dv FROM " + source + " WHERE add IS NOT NULL");
		if (adds->HasError()) {
			return false;
		}
	}
	auto meta = RunSQL(
	    conn,
	    "CREATE OR REPLACE TEMP TABLE prev_meta AS SELECT "
	    "(SELECT metaData.id FROM " +
	        source + " WHERE metaData IS NOT NULL) AS id, (SELECT metaData.schemaString FROM " + source +
	        " WHERE metaData IS NOT NULL) AS schema_string, (SELECT to_json(metaData.configuration)::VARCHAR FROM " +
	        source +
	        " WHERE metaData IS NOT NULL) AS configuration, (SELECT CAST(protocol.minReaderVersion AS VARCHAR) || ',' "
	        "|| "
	        "CAST(protocol.minWriterVersion AS VARCHAR) FROM " +
	        source + " WHERE protocol IS NOT NULL) AS protocol_versions");
	return !meta->HasError();
}

struct DeltaExportBindData : public TableFunctionData {
	//! tables := [...] and schemas := [...]; both empty exports the whole catalog
	vector<string> tables;
	vector<string> schemas;

	bool Filtered() const {
		return !tables.empty() || !schemas.empty();
	}
};

// Fills TEMP table selected_tables(table_id) with the live tables named in tables := [...] ('name' in any schema,
// 'schema.name' in that one) and every live table of the schemas in schemas := [...]. Names ignore case like DuckDB
// identifiers; an unknown or ambiguous name fails the export. Returns the named schemas as DuckLake spells them.
static std::set<string> SelectTables(Connection &conn, const MetadataLocation &meta,
                                     const DeltaExportBindData &bind_data) {
	ExecuteSQL(conn, "CREATE OR REPLACE TEMP TABLE selected_tables (table_id BIGINT)");
	for (auto &name : bind_data.tables) {
		auto match = ExecuteSQL(conn, "SELECT count(DISTINCT table_id), string_agg(schema_name || '.' || table_name, "
		                              "', ' ORDER BY schema_name), any_value(table_id) FROM table_roots WHERE "
		                              "end_snapshot IS NULL AND lower('" +
		                                  EscapeSqlString(name) +
		                                  "') IN (lower(table_name), lower(schema_name || '.' || table_name))");
		auto count = ResultValue<int64_t>(*match, 0, 0, "match");
		if (count == 0) {
			throw InvalidInputException("Delta export failed: no table \"%s\" in the DuckLake catalog", name);
		}
		if (count > 1) {
			throw InvalidInputException("Delta export failed: \"%s\" matches %s; name the schema", name,
			                            match->GetValue(1, 0).ToString());
		}
		ExecuteSQL(conn, "INSERT INTO selected_tables VALUES (" + match->GetValue(2, 0).ToString() + ")");
	}
	std::set<string> schemas;
	for (auto &name : bind_data.schemas) {
		auto match = ExecuteSQL(conn, QualifyMetadataTables("SELECT schema_name FROM {META}ducklake_schema WHERE "
		                                                    "end_snapshot IS NULL AND lower(schema_name) = lower('" +
		                                                        EscapeSqlString(name) + "')",
		                                                    meta));
		if (match->RowCount() == 0) {
			throw InvalidInputException("Delta export failed: no schema \"%s\" in the DuckLake catalog", name);
		}
		auto schema = match->GetValue(0, 0).ToString();
		ExecuteSQL(conn, "INSERT INTO selected_tables SELECT table_id FROM table_roots WHERE end_snapshot IS NULL AND "
		                 "schema_name = '" +
		                     EscapeSqlString(schema) + "'");
		schemas.insert(schema);
	}
	return schemas;
}

// The names in a list option. A list given but empty fails rather than fall back to exporting everything.
static vector<string> ListOption(TableFunctionBindInput &input, const char *option) {
	vector<string> names;
	auto entry = input.named_parameters.find(option);
	if (entry == input.named_parameters.end()) {
		return names;
	}
	if (!entry->second.IsNull()) {
		for (auto &name : ListValue::GetChildren(entry->second)) {
			if (name.IsNull()) {
				throw InvalidInputException("Delta export failed: %s must not contain NULL", option);
			}
			names.push_back(name.ToString());
		}
	}
	if (names.empty()) {
		throw InvalidInputException("Delta export failed: %s must not be empty", option);
	}
	return names;
}

struct DeltaExportGlobalState : public GlobalTableFunctionState {
	bool finished = false;
	vector<vector<Value>> rows;
	idx_t current_row = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> DeltaExportBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
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

	auto result = make_uniq<DeltaExportBindData>();
	result->tables = ListOption(input, "tables");
	result->schemas = ListOption(input, "schemas");
	return std::move(result);
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
	// A new connection starts from the global settings only: carry over the caller's session-level SETs, such as
	// azure_transport_option_type = 'curl', which OneLake needs on Linux to read delete files and write the log
	conn.context->config.user_settings = context.config.user_settings;
	conn.context->config.user_variables = context.config.user_variables;
	// Locate the DuckLake metadata catalog from the main context — works with USE, dbt (no USE), a hidden
	// metadata catalog (DuckLake on DuckDB >= 1.5.3), METADATA_CATALOG / METADATA_SCHEMA, or a direct attach
	auto meta = ResolveMetadataLocation(context);
	auto &bind_data = data.bind_data->Cast<DeltaExportBindData>();

	// Step 1: Build export summary (reads ducklake_* tables)
	ExecuteSQL(conn, SQL_CREATE_EXPORT_SUMMARY);
	ExecuteSQL(conn, QualifyMetadataTables(SQL_CREATE_TABLE_ROOTS, meta));
	auto filtered = bind_data.Filtered();
	auto schemas = filtered ? SelectTables(conn, meta, bind_data) : std::set<string>();

	// Rows DuckLake still keeps inlined in the metadata catalog are in no parquet file, so a Delta reader would
	// never see them: flush them first. A read-only lake cannot flush; its inlined rows stay out of the export.
	if (!meta.lake.empty()) {
		auto flush = "CALL ducklake_flush_inlined_data('" + EscapeSqlString(meta.lake) + "'";
		if (!filtered) {
			conn.Query(flush + ")");
		} else {
			auto named = ExecuteSQL(conn, "SELECT schema_name, table_name FROM table_roots WHERE end_snapshot IS NULL "
			                              "AND table_id IN (SELECT table_id FROM selected_tables)");
			for (idx_t i = 0; i < named->RowCount(); i++) {
				conn.Query(flush + ", schema_name := '" + EscapeSqlString(named->GetValue(0, i).ToString()) +
				           "', table_name := '" + EscapeSqlString(named->GetValue(1, i).ToString()) + "')");
			}
		}
	}

	ExecuteSQL(conn, QualifyMetadataTables(SQL_INSERT_EXPORT_SUMMARY, meta));
	if (filtered) {
		ExecuteSQL(conn, "DELETE FROM export_summary WHERE table_id NOT IN (SELECT table_id FROM selected_tables)");
	}

	// The client file system carries the secrets of the session (S3, Azure, ...)
	auto &fs = FileSystem::GetFileSystem(context);
	MarkEmptiedExportedTables(conn, fs, meta);

	// Step 2: Hold back tables whose older files no longer match the schema, then build the Delta schema of
	// every exported table (nested types, nulls_allowed, type widening)
	auto widened = AnalyzeColumnHistory(conn, meta);
	BuildTableSchemas(conn, meta, widened);

	// Step 3: Turn DuckLake delete files and inlined deletes into Delta deletion vectors
	std::map<int64_t, string> dv_files;
	BuildDeletionVectors(conn, fs, meta, dv_files);

	// Step 4: Generate checkpoint parquet data (reads ducklake_* tables)
	ExecuteSQL(conn, QualifyMetadataTables(SQL_CREATE_CHECKPOINT_PARQUET, meta));

	// Step 5: Generate JSON commit and last checkpoint data
	ExecuteSQL(conn, SQL_CREATE_CHECKPOINT_JSON);
	ExecuteSQL(conn, SQL_CREATE_LAST_CHECKPOINT);

	// Step 6: Build export paths
	ExecuteSQL(conn, SQL_CREATE_EXPORT_PATHS);

	// Step 7: Create _delta_log directories for local paths
	auto dirs_result = ExecuteSQL(conn, SQL_GET_LOCAL_DIRS);
	for (idx_t i = 0; i < dirs_result->RowCount(); i++) {
		string dir_path = dirs_result->GetValue(0, i).ToString();
		std::replace(dir_path.begin(), dir_path.end(), '\\', '/');
		fs.CreateDirectoriesRecursive(dir_path);
	}

	// Step 8: Export loop — unlimited tables
	auto exports_result = ExecuteSQL(conn, SQL_GET_EXPORTS);

	for (idx_t i = 0; i < exports_result->RowCount(); i++) {
		int64_t table_id = ResultValue<int64_t>(*exports_result, 1, i, "exports_result");
		string table_root = exports_result->GetValue(2, i).ToString();
		int64_t snapshot_id = ResultValue<int64_t>(*exports_result, 3, i, "exports_result");
		string delta_log = exports_result->GetValue(4, i).ToString();

		string tid = std::to_string(table_id);
		string sid = std::to_string(snapshot_id);

		// Versions count up by one from the last one in the folder, and each commit carries the add and remove
		// actions against the previous version: readers that apply commits incrementally (the Fabric SQL
		// endpoint) see the change, readers that load the checkpoint written at every version see the same state.
		auto existing_version = ExistingMaxDeltaVersion(fs, delta_log);
		bool has_previous = existing_version >= 0 &&
		                    LoadPreviousState(conn, DeltaLogFile(delta_log, existing_version, ".checkpoint.parquet"));
		if (!has_previous) {
			ExecuteSQL(conn, "CREATE OR REPLACE TEMP TABLE prev_adds AS SELECT * FROM (SELECT NULL::VARCHAR AS k, "
			                 "NULL::VARCHAR AS path, NULL::BIGINT AS size, " +
			                     string(DV_STRUCT_NULL) + " AS dv) WHERE false;");
		}
		ExecuteSQL(conn, "CREATE OR REPLACE TEMP TABLE new_adds AS SELECT " + string(ADD_KEY_SQL) +
		                     " AS k, add FROM temp_checkpoint_parquet WHERE row_order = 3 AND table_id = " + tid +
		                     " AND snapshot_id = " + sid + ";");
		if (has_previous) {
			auto changes = ExecuteSQL(
			    conn,
			    "SELECT (SELECT count(*) FROM new_adds WHERE k NOT IN (SELECT k FROM prev_adds)) + "
			    "(SELECT count(*) FROM prev_adds WHERE k NOT IN (SELECT k FROM new_adds)) + "
			    "(SELECT count(*) FROM prev_meta pm, temp_checkpoint_parquet m, temp_checkpoint_parquet p "
			    " WHERE m.table_id = " +
			        tid + " AND m.row_order = 2 AND p.table_id = " + tid +
			        " AND p.row_order = 1 AND (pm.id IS DISTINCT FROM m.metaData.id"
			        " OR pm.schema_string IS DISTINCT FROM m.metaData.schemaString"
			        " OR pm.configuration IS DISTINCT FROM to_json(m.metaData.configuration)::VARCHAR"
			        " OR pm.protocol_versions IS DISTINCT FROM CAST(p.protocol.minReaderVersion AS VARCHAR) || ',' || "
			        "CAST(p.protocol.minWriterVersion AS VARCHAR)))");
			if (changes->GetValue(0, 0).GetValue<int64_t>() == 0) {
				continue; // nothing changed since the last export: no new version
			}
		}
		int64_t version = existing_version + 1;
		string checkpoint_file = DeltaLogFile(delta_log, version, ".checkpoint.parquet");
		string json_file = DeltaLogFile(delta_log, version, ".json");
		string last_checkpoint_file = delta_log + "/_last_checkpoint";
		string ver = std::to_string(version);

		string copy_parquet = "COPY (SELECT protocol, metaData, add, remove, commitInfo "
		                      "FROM temp_checkpoint_parquet "
		                      "WHERE table_id = " +
		                      tid + " AND snapshot_id = " + sid +
		                      " ORDER BY row_order) "
		                      "TO '" +
		                      checkpoint_file + "' (FORMAT PARQUET);";
		ExecuteSQL(conn, copy_parquet);

		// commitInfo, metaData, protocol, then the files that left (with the deletion vector they had) and the
		// files that arrived; a file whose deletion vector changed is removed and added again
		string copy_json =
		    "COPY (SELECT content FROM ("
		    "SELECT 0 AS part, 0 AS n, content FROM temp_checkpoint_json WHERE table_id = " +
		    tid + " AND snapshot_id = " + sid +
		    " UNION ALL "
		    "SELECT 1, row_number() OVER (ORDER BY path), to_json({'remove': {'path': path, "
		    "'deletionTimestamp': epoch_ms(now()), 'dataChange': true, 'extendedFileMetadata': true, "
		    "'partitionValues': MAP{}::MAP(VARCHAR, VARCHAR), 'size': size, 'deletionVector': dv}})::VARCHAR "
		    "FROM prev_adds WHERE k NOT IN (SELECT k FROM new_adds)"
		    " UNION ALL "
		    "SELECT 2, row_number() OVER (ORDER BY k), to_json({'add': add})::VARCHAR "
		    "FROM new_adds WHERE k NOT IN (SELECT k FROM prev_adds)"
		    ") ORDER BY part, n) "
		    "TO '" +
		    json_file + "' (FORMAT CSV, HEADER false, QUOTE '');";
		ExecuteSQL(conn, copy_json);

		string copy_last = "COPY (SELECT '{\"version\":" + ver +
		                   ",\"size\":' || size || '}' AS content FROM temp_last_checkpoint "
		                   "WHERE table_id = " +
		                   tid + " AND snapshot_id = " + sid +
		                   ") "
		                   "TO '" +
		                   last_checkpoint_file + "' (FORMAT CSV, HEADER false, QUOTE '');";
		ExecuteSQL(conn, copy_last);
		// Deletion vector files of earlier exports stay: readers still on the previous version (the Fabric SQL
		// endpoint caches table metadata) read them. They go with the log when the table is dropped.
	}

	// Step 9: Remove the Delta logs of tables dropped in DuckLake, only in the named schemas when filtered: an export
	// of named tables alone touches no other log
	if (!filtered || !schemas.empty()) {
		RemoveDroppedTableLogs(conn, fs, schemas);
	}

	// Step 10: Collect and return results
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
	func.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("tables", LogicalType::LIST(LogicalType::VARCHAR))
		    .Add("schemas", LogicalType::LIST(LogicalType::VARCHAR));
	});

	CreateTableFunctionInfo info(func);
	FunctionDescription desc;
	desc.description = "Exports every table of the DuckLake catalog in use (or the first attached lake) to Delta Lake "
	                   "by writing _delta_log metadata over the existing parquet files, with deletes as deletion "
	                   "vectors; also removes the Delta logs of dropped tables. With tables := ['name', "
	                   "'schema.name'] and/or schemas := ['schema'] only those tables, and every table of those "
	                   "schemas, are exported, and dropped-table logs are removed only in those schemas. Returns one "
	                   "status row per table.";
	desc.examples = {"CALL delta_export();", "CALL delta_export(tables := ['main.orders']);",
	                 "CALL delta_export(schemas := ['sales'], tables := ['main.orders']);",
	                 "SELECT table_name, status, data_snapshot FROM delta_export();"};
	desc.categories = {"ducklake", "delta", "export"};
	info.descriptions.push_back(std::move(desc));
	// what the bare RegisterFunction(TableFunction) overload does internally
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
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
