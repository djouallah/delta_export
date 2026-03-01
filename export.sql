-- verify data load
SELECT '[VERIFY] Latest data timestamp: ' || max(cutoff) AS Status FROM summary;


SELECT 'Export Delta Metadata' AS Status;

use __ducklake_metadata_dwh; 

-- Create export tracking table if it doesn't exist
CREATE TABLE IF NOT EXISTS ducklake_export_log (
    table_id BIGINT,
    snapshot_id BIGINT,
    export_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_id, snapshot_id)
);

-- 3. Main export logic
CREATE OR REPLACE TEMP TABLE export_summary (
    table_id BIGINT,
    schema_name VARCHAR,
    table_name VARCHAR,
    table_root VARCHAR,
    status VARCHAR,
    snapshot_id BIGINT,
    message VARCHAR
);

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
    -- Get the current (latest) snapshot - this always exists even after expire_snapshots
    SELECT MAX(snapshot_id) AS snapshot_id FROM ducklake_snapshot
),
table_last_modified AS (
    -- Get each table's last modification snapshot
    -- After expire_snapshots, snapshot_changes may be deleted, so we fall back to current snapshot
    -- if the table has active data files (end_snapshot IS NULL)
    SELECT 
        t.*,
        COALESCE(
            -- First try to get from snapshot_changes (works when snapshots not expired)
            (SELECT MAX(sc.snapshot_id)
             FROM ducklake_snapshot_changes sc
             WHERE regexp_matches(sc.changes_made, '[:,]' || t.table_id || '([^0-9]|$)')
            ),
            -- Fall back to current snapshot if table has active files (after expire_snapshots)
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
export_status AS (
    SELECT 
        tm.*,
        -- Get the last exported snapshot for this table
        (SELECT MAX(el.snapshot_id) 
         FROM ducklake_export_log el 
         WHERE el.table_id = tm.table_id
        ) AS last_exported_snapshot
    FROM table_last_modified tm
)
SELECT 
    table_id,
    schema_name,
    table_name,
    table_root,
    CASE 
        WHEN file_count = 0 THEN 'no_data_files'
        WHEN last_modified_snapshot IS NULL THEN 'no_changes'
        WHEN last_exported_snapshot IS NOT NULL AND last_exported_snapshot >= last_modified_snapshot THEN 'already_exported'
        ELSE 'needs_export'
    END AS status,
    last_modified_snapshot AS snapshot_id,
    CASE 
        WHEN file_count = 0 THEN 'Table has no data files (empty table)'
        WHEN last_modified_snapshot IS NULL THEN 'No changes recorded'
        WHEN last_exported_snapshot IS NOT NULL AND last_exported_snapshot >= last_modified_snapshot THEN 'Snapshot ' || last_modified_snapshot || ' already exported'
        ELSE 'Ready for export'
    END AS message
FROM export_status;

-- 4. Generate checkpoint files for tables that need export  
CREATE OR REPLACE TEMP TABLE temp_checkpoint_parquet AS
WITH 
tables_to_export AS (
    -- Only export tables that have active data files (end_snapshot IS NULL)
    -- After expire_snapshots, we only care about the current state of the table
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
                    WHEN contains(lower(c.column_type), 'int') AND contains(c.column_type, '64') THEN 'long'
                    WHEN contains(lower(c.column_type), 'int') THEN 'integer'
                    WHEN contains(lower(c.column_type), 'float') THEN 'double'
                    WHEN contains(lower(c.column_type), 'double') THEN 'double'
                    WHEN contains(lower(c.column_type), 'bool') THEN 'boolean'
                    WHEN contains(lower(c.column_type), 'timestamp') THEN 'timestamp'
                    WHEN contains(lower(c.column_type), 'date') THEN 'date'
                    WHEN contains(lower(c.column_type), 'decimal') THEN lower(c.column_type)
                    ELSE 'string'
                END,
            'nullable': true,
            'metadata': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(name VARCHAR, type VARCHAR, nullable BOOLEAN, metadata MAP(VARCHAR, VARCHAR)) ORDER BY c.column_order) AS schema_fields
    FROM tables_to_export te
    JOIN ducklake_table t ON te.table_id = t.table_id
    JOIN ducklake_column c ON t.table_id = c.table_id
    WHERE c.end_snapshot IS NULL  -- Use current columns only (survives expire_snapshots)
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
    WHERE df.end_snapshot IS NULL  -- Only active files (survives expire_snapshots)
      AND c.column_id IS NOT NULL
      AND c.end_snapshot IS NULL  -- Only active columns
    GROUP BY ts.table_id, df.data_file_id, c.column_name
),
-- Pre-transform stats and filter nulls BEFORE aggregation
-- Inline transformation logic for Delta Lake format (timestamp, date, boolean, numeric, string types)
file_column_stats_transformed AS (
    SELECT
        fca.table_id,
        fca.data_file_id,
        fca.column_name,
        fca.column_type,
        fca.value_count,
        fca.null_count,
        -- Transform min_value for Delta Lake format
        CASE
            WHEN fca.min_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                regexp_replace(
                    regexp_replace(replace(fca.min_value, ' ', 'T'), '[+-]\\d{2}(?::\\d{2})?$', ''),
                    '^([^.]+)$', '\\1.000'
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
        -- Transform max_value for Delta Lake format
        CASE
            WHEN fca.max_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN
                regexp_replace(
                    regexp_replace(replace(fca.max_value, ' ', 'T'), '[+-]\\d{2}(?::\\d{2})?$', ''),
                    '^([^.]+)$', '\\1.000'
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
        COALESCE(MAX(fct.value_count), 0) AS num_records,
        -- Note: Only include columns that have non-null transformed values
        -- Power BI DirectLake requires clean JSON without null values in stats
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
    WHERE df.end_snapshot IS NULL  -- Only active files (survives expire_snapshots)
    GROUP BY ts.table_id, ts.schema_name, ts.table_name, ts.snapshot_id, 
             ts.table_root, ts.schema_fields, df.data_file_id, df.path, df.file_size_bytes
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
            -- Ensure stats is always valid JSON - Power BI DirectLake requires non-empty JSON
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
        -- Generate stable UUID based on table_id so it doesn't change across exports
        -- Format as proper UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
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

-- Create temp_checkpoint_json by deriving from temp_checkpoint_parquet
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

-- Create last checkpoint temp table
-- Note: Power BI DirectLake requires _last_checkpoint to be valid JSON without null values
-- Only include required fields (version, size) - omit optional fields entirely
CREATE OR REPLACE TEMP TABLE temp_last_checkpoint AS 
SELECT 
    table_id,
    table_root,
    snapshot_id,
    '{"version":' || snapshot_id || ',"size":' || (2 + num_files) || '}' AS content
FROM temp_checkpoint_parquet
GROUP BY table_id, table_root, snapshot_id, num_files;

-- Export checkpoint files for tables that need export
-- Create a numbered list of all tables to export
CREATE OR REPLACE TEMP TABLE numbered_exports AS
SELECT 
    table_id, 
    table_root, 
    snapshot_id,
    ROW_NUMBER() OVER (ORDER BY table_id) AS export_order
FROM (SELECT DISTINCT table_id, table_root, snapshot_id FROM temp_checkpoint_parquet);

-- Generate export paths using LATERAL join (eliminates 7 separate temp tables)
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

-- Create _delta_log directories for local filesystem paths only
-- DuckDB COPY cannot create directories directly, but PARTITION_BY forces directory creation
CREATE OR REPLACE TEMP TABLE local_dirs_to_create AS
SELECT DISTINCT
    delta_log_path,
    ROW_NUMBER() OVER (ORDER BY delta_log_path) AS dir_order
FROM export_paths
WHERE table_root NOT LIKE 'abfss://%'
  AND table_root NOT LIKE 's3://%'
  AND table_root NOT LIKE 'gs://%'
  AND table_root NOT LIKE 'az://%'
  AND table_root NOT LIKE 'http://%'
  AND table_root NOT LIKE 'https://%';

-- Initialize directories using dynamic EXECUTE (loop simulation)
SET VARIABLE dir_count = (SELECT COUNT(*) FROM local_dirs_to_create);
SET VARIABLE dir_idx = 1;

-- Process directory 1
SET VARIABLE curr_dir = (SELECT delta_log_path FROM local_dirs_to_create WHERE dir_order = getvariable('dir_idx'));
COPY (SELECT 1 AS id, 1 AS ".duckdb_init" WHERE getvariable('dir_idx') <= getvariable('dir_count'))
  TO (COALESCE(getvariable('curr_dir'), getvariable('PATH_ROOT') || '/Files/.duckdb_skip'))
  (FORMAT CSV, PARTITION_BY (".duckdb_init"), OVERWRITE_OR_IGNORE);

SET VARIABLE dir_idx = 2;
SET VARIABLE curr_dir = (SELECT delta_log_path FROM local_dirs_to_create WHERE dir_order = getvariable('dir_idx'));
COPY (SELECT 1 AS id, 1 AS ".duckdb_init" WHERE getvariable('dir_idx') <= getvariable('dir_count'))
  TO (COALESCE(getvariable('curr_dir'), getvariable('PATH_ROOT') || '/Files/.duckdb_skip'))
  (FORMAT CSV, PARTITION_BY (".duckdb_init"), OVERWRITE_OR_IGNORE);

SET VARIABLE dir_idx = 3;
SET VARIABLE curr_dir = (SELECT delta_log_path FROM local_dirs_to_create WHERE dir_order = getvariable('dir_idx'));
COPY (SELECT 1 AS id, 1 AS ".duckdb_init" WHERE getvariable('dir_idx') <= getvariable('dir_count'))
  TO (COALESCE(getvariable('curr_dir'), getvariable('PATH_ROOT') || '/Files/.duckdb_skip'))
  (FORMAT CSV, PARTITION_BY (".duckdb_init"), OVERWRITE_OR_IGNORE);

SET VARIABLE dir_idx = 4;
SET VARIABLE curr_dir = (SELECT delta_log_path FROM local_dirs_to_create WHERE dir_order = getvariable('dir_idx'));
COPY (SELECT 1 AS id, 1 AS ".duckdb_init" WHERE getvariable('dir_idx') <= getvariable('dir_count'))
  TO (COALESCE(getvariable('curr_dir'), getvariable('PATH_ROOT') || '/Files/.duckdb_skip'))
  (FORMAT CSV, PARTITION_BY (".duckdb_init"), OVERWRITE_OR_IGNORE);

SET VARIABLE dir_idx = 5;
SET VARIABLE curr_dir = (SELECT delta_log_path FROM local_dirs_to_create WHERE dir_order = getvariable('dir_idx'));
COPY (SELECT 1 AS id, 1 AS ".duckdb_init" WHERE getvariable('dir_idx') <= getvariable('dir_count'))
  TO (COALESCE(getvariable('curr_dir'), getvariable('PATH_ROOT') || '/Files/.duckdb_skip'))
  (FORMAT CSV, PARTITION_BY (".duckdb_init"), OVERWRITE_OR_IGNORE);

SET VARIABLE dir_idx = 6;
SET VARIABLE curr_dir = (SELECT delta_log_path FROM local_dirs_to_create WHERE dir_order = getvariable('dir_idx'));
COPY (SELECT 1 AS id, 1 AS ".duckdb_init" WHERE getvariable('dir_idx') <= getvariable('dir_count'))
  TO (COALESCE(getvariable('curr_dir'), getvariable('PATH_ROOT') || '/Files/.duckdb_skip'))
  (FORMAT CSV, PARTITION_BY (".duckdb_init"), OVERWRITE_OR_IGNORE);

SET VARIABLE dir_idx = 7;
SET VARIABLE curr_dir = (SELECT delta_log_path FROM local_dirs_to_create WHERE dir_order = getvariable('dir_idx'));
COPY (SELECT 1 AS id, 1 AS ".duckdb_init" WHERE getvariable('dir_idx') <= getvariable('dir_count'))
  TO (COALESCE(getvariable('curr_dir'), getvariable('PATH_ROOT') || '/Files/.duckdb_skip'))
  (FORMAT CSV, PARTITION_BY (".duckdb_init"), OVERWRITE_OR_IGNORE);

-- Export loop: process each table using export_paths (no more 7 separate temp tables)
SET VARIABLE export_count = (SELECT COUNT(*) FROM export_paths);

-- Export 1
SET VARIABLE exp_idx = 1;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_table_id = (SELECT table_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_snapshot_id = (SELECT snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE checkpoint_file = COALESCE((SELECT checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT json_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT last_checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');

BEGIN TRANSACTION;
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id')
      ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id)
  SELECT table_id, snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx') AND getvariable('has_rows') > 0;
COMMIT;

-- Export 2
SET VARIABLE exp_idx = 2;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_table_id = (SELECT table_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_snapshot_id = (SELECT snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE checkpoint_file = COALESCE((SELECT checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT json_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT last_checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');

BEGIN TRANSACTION;
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id')
      ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id)
  SELECT table_id, snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx') AND getvariable('has_rows') > 0;
COMMIT;

-- Export 3
SET VARIABLE exp_idx = 3;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_table_id = (SELECT table_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_snapshot_id = (SELECT snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE checkpoint_file = COALESCE((SELECT checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT json_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT last_checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');

BEGIN TRANSACTION;
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id')
      ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id)
  SELECT table_id, snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx') AND getvariable('has_rows') > 0;
COMMIT;

-- Export 4
SET VARIABLE exp_idx = 4;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_table_id = (SELECT table_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_snapshot_id = (SELECT snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE checkpoint_file = COALESCE((SELECT checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT json_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT last_checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');

BEGIN TRANSACTION;
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id')
      ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id)
  SELECT table_id, snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx') AND getvariable('has_rows') > 0;
COMMIT;

-- Export 5
SET VARIABLE exp_idx = 5;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_table_id = (SELECT table_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_snapshot_id = (SELECT snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE checkpoint_file = COALESCE((SELECT checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT json_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT last_checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');

BEGIN TRANSACTION;
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id')
      ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id)
  SELECT table_id, snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx') AND getvariable('has_rows') > 0;
COMMIT;

-- Export 6
SET VARIABLE exp_idx = 6;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_table_id = (SELECT table_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_snapshot_id = (SELECT snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE checkpoint_file = COALESCE((SELECT checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT json_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT last_checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');

BEGIN TRANSACTION;
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id')
      ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id)
  SELECT table_id, snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx') AND getvariable('has_rows') > 0;
COMMIT;

-- Export 7
SET VARIABLE exp_idx = 7;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_table_id = (SELECT table_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE exp_snapshot_id = (SELECT snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx'));
SET VARIABLE checkpoint_file = COALESCE((SELECT checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT json_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT last_checkpoint_file FROM export_paths WHERE export_order = getvariable('exp_idx')), '/tmp/skip');

BEGIN TRANSACTION;
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id')
      ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint
      WHERE getvariable('has_rows') > 0 AND table_id = getvariable('exp_table_id') AND snapshot_id = getvariable('exp_snapshot_id'))
      TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id)
  SELECT table_id, snapshot_id FROM export_paths WHERE export_order = getvariable('exp_idx') AND getvariable('has_rows') > 0;
COMMIT;

-- Show detailed export status per table
SELECT 
    schema_name || '.' || table_name AS table_name,
    status,
    snapshot_id AS data_snapshot,
    CASE 
        WHEN status = 'no_data_files' THEN 'Table has 0 parquet files (empty)'
        WHEN status = 'already_exported' THEN 'Data snapshot ' || snapshot_id || ' already exported'
        WHEN status = 'needs_export' THEN 'Data snapshot ' || snapshot_id || ' needs Delta export'
    END AS explanation,
    message
FROM export_summary
ORDER BY 
    CASE status 
        WHEN 'needs_export' THEN 1 
        WHEN 'already_exported' THEN 2 
        WHEN 'no_data_files' THEN 3
    END,
    schema_name, table_name; 