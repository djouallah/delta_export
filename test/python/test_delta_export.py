"""Integration tests for the delta_export extension."""

import os
import tempfile


MOCK_DUCKLAKE_SQL = """
CREATE TABLE ducklake_metadata AS
    SELECT 'data_path' AS key, '{data_path}' AS value;

CREATE TABLE ducklake_schema AS
    SELECT 1 AS schema_id, 'main' AS schema_name, '' AS path;

CREATE TABLE ducklake_table AS
    SELECT 1 AS table_id, 'test_table' AS table_name, 'test_table' AS path,
           NULL::BIGINT AS end_snapshot, 1 AS schema_id;

CREATE TABLE ducklake_snapshot AS SELECT 1 AS snapshot_id;

CREATE TABLE ducklake_snapshot_changes AS
    SELECT 1 AS snapshot_id, 'modified:1' AS changes_made;

CREATE TABLE ducklake_data_file AS
    SELECT 1 AS data_file_id, 1 AS table_id, '/data/part-0001.parquet' AS path,
           1024::BIGINT AS file_size_bytes, NULL::BIGINT AS end_snapshot;

CREATE TABLE ducklake_column AS SELECT * FROM (VALUES
    (1, 1, 'id', 'BIGINT', 0, NULL::BIGINT),
    (2, 1, 'name', 'VARCHAR', 1, NULL::BIGINT)
) t(column_id, table_id, column_name, column_type, column_order, end_snapshot);

CREATE TABLE ducklake_file_column_stats AS SELECT * FROM (VALUES
    (1, 1, 100::BIGINT, '1', '100', 0::BIGINT),
    (1, 2, 100::BIGINT, 'Alice', 'Zoe', 5::BIGINT)
) t(data_file_id, column_id, value_count, min_value, max_value, null_count);
"""


def _setup_mock_ducklake(conn, data_path):
    for stmt in MOCK_DUCKLAKE_SQL.format(data_path=data_path).split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


def test_export_needs_export(conn):
    """First export should return needs_export status."""
    with tempfile.TemporaryDirectory() as tmp:
        _setup_mock_ducklake(conn, tmp)
        rows = conn.execute("SELECT * FROM export_delta()").fetchall()
        assert len(rows) == 1
        table_name, status, snapshot, explanation, message = rows[0]
        assert table_name == "main.test_table"
        assert status == "needs_export"
        assert snapshot == 1


def test_export_idempotent(conn):
    """Second export should return already_exported status."""
    with tempfile.TemporaryDirectory() as tmp:
        _setup_mock_ducklake(conn, tmp)
        conn.execute("SELECT * FROM export_delta()").fetchall()
        rows = conn.execute("SELECT * FROM export_delta()").fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "already_exported"


def test_export_creates_delta_files(conn):
    """Export should create checkpoint parquet, json, and _last_checkpoint."""
    with tempfile.TemporaryDirectory() as tmp:
        _setup_mock_ducklake(conn, tmp)
        conn.execute("SELECT * FROM export_delta()").fetchall()

        delta_log = os.path.join(tmp, "test_table", "_delta_log")
        assert os.path.isdir(delta_log), f"_delta_log directory not created at {delta_log}"

        files = os.listdir(delta_log)
        assert any(f.endswith(".checkpoint.parquet") for f in files), f"No checkpoint parquet in {files}"
        assert any(f.endswith(".json") for f in files), f"No json file in {files}"
        assert "_last_checkpoint" in files, f"No _last_checkpoint in {files}"


def test_checkpoint_parquet_readable(conn):
    """The checkpoint parquet should be readable and contain expected columns."""
    with tempfile.TemporaryDirectory() as tmp:
        _setup_mock_ducklake(conn, tmp)
        conn.execute("SELECT * FROM export_delta()").fetchall()

        delta_log = os.path.join(tmp, "test_table", "_delta_log")
        parquet_files = [f for f in os.listdir(delta_log) if f.endswith(".checkpoint.parquet")]
        parquet_path = os.path.join(delta_log, parquet_files[0])

        col_names = [
            d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 0").description
        ]
        assert "protocol" in col_names
        assert "metaData" in col_names
        assert "add" in col_names


def test_empty_table_no_export(conn):
    """A table with no data files should not be exported."""
    with tempfile.TemporaryDirectory() as tmp:
        _setup_mock_ducklake(conn, tmp)
        # Add a table with no data files
        conn.execute(
            "INSERT INTO ducklake_table VALUES (2, 'empty_table', 'empty_table', NULL, 1)"
        )
        rows = conn.execute("SELECT * FROM export_delta() ORDER BY status").fetchall()
        statuses = {r[0]: r[1] for r in rows}
        assert statuses["main.test_table"] == "needs_export"
        assert statuses["main.empty_table"] in ("no_data_files", "no_changes")
