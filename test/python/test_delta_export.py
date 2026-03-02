"""Integration tests for the delta_export extension using a real DuckLake catalog."""

import os


def test_export_needs_export(conn):
    """First export should return needs_export status."""
    conn.execute("CREATE TABLE test_table (id BIGINT, name VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
    rows = conn.execute("SELECT * FROM export_delta()").fetchall()
    assert len(rows) == 1
    table_name, status, snapshot, explanation, message = rows[0]
    assert table_name == "main.test_table"
    assert status == "needs_export"


def test_export_idempotent(conn):
    """Second export should return already_exported status."""
    conn.execute("CREATE TABLE test_table (id BIGINT, name VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'Alice')")
    conn.execute("SELECT * FROM export_delta()").fetchall()
    rows = conn.execute("SELECT * FROM export_delta()").fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "already_exported"


def test_export_creates_delta_files(ducklake_env):
    """Export should create checkpoint parquet, json, and _last_checkpoint."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE test_table (id BIGINT, name VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'Alice')")
    conn.execute("SELECT * FROM export_delta()").fetchall()

    delta_log = os.path.join(data_path, "test_table", "_delta_log")
    assert os.path.isdir(delta_log), f"_delta_log directory not created at {delta_log}"

    files = os.listdir(delta_log)
    assert any(f.endswith(".checkpoint.parquet") for f in files), f"No checkpoint parquet in {files}"
    assert any(f.endswith(".json") for f in files), f"No json file in {files}"
    assert "_last_checkpoint" in files, f"No _last_checkpoint in {files}"


def test_checkpoint_parquet_readable(ducklake_env):
    """The checkpoint parquet should be readable and contain expected columns."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE test_table (id BIGINT, name VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'Alice')")
    conn.execute("SELECT * FROM export_delta()").fetchall()

    delta_log = os.path.join(data_path, "test_table", "_delta_log")
    parquet_files = [f for f in os.listdir(delta_log) if f.endswith(".checkpoint.parquet")]
    parquet_path = os.path.join(delta_log, parquet_files[0])

    col_names = [
        d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 0").description
    ]
    assert "protocol" in col_names
    assert "metaData" in col_names
    assert "add" in col_names


def test_multiple_tables(conn):
    """Export should handle multiple tables."""
    conn.execute("CREATE TABLE orders (id BIGINT, amount DOUBLE)")
    conn.execute("INSERT INTO orders VALUES (1, 99.99), (2, 149.50)")
    conn.execute("CREATE TABLE customers (id BIGINT, name VARCHAR)")
    conn.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
    rows = conn.execute("SELECT * FROM export_delta() ORDER BY table_name").fetchall()
    assert len(rows) == 2
    names = [r[0] for r in rows]
    assert "main.customers" in names
    assert "main.orders" in names
    assert all(r[1] == "needs_export" for r in rows)
