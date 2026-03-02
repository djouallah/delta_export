"""Integration tests for the delta_export extension using a real DuckLake catalog."""

import os


def _find_delta_log(data_path):
    """Walk data_path to find the _delta_log directory."""
    for root, dirs, files in os.walk(data_path):
        if "_delta_log" in dirs:
            return os.path.join(root, "_delta_log")
    return None


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

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"

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

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
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


def test_roundtrip_data(ducklake_env):
    """After complex transactions, exported Delta should match DuckLake row count."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE sales (id BIGINT, amount DOUBLE, region VARCHAR)")

    # Bulk insert
    conn.execute(
        "INSERT INTO sales VALUES "
        "(1, 10.0, 'US'), (2, 20.0, 'EU'), (3, 30.0, 'US'), "
        "(4, 40.0, 'APAC'), (5, 50.0, 'EU'), (6, 60.0, 'US')"
    )
    # Update some rows
    conn.execute("UPDATE sales SET amount = amount * 1.1 WHERE region = 'EU'")
    # Delete a row
    conn.execute("DELETE FROM sales WHERE id = 4")
    # Insert more
    conn.execute("INSERT INTO sales VALUES (7, 70.0, 'APAC'), (8, 80.0, 'US')")
    # Another update
    conn.execute("UPDATE sales SET region = 'NA' WHERE region = 'US'")

    # Force checkpoint to rewrite all files with any deletes and flush inlined data
    conn.execute("CALL ducklake.set_option('rewrite_delete_threshold', 0)")
    conn.execute("CALL ducklake.set_option('inline_threshold', 0)")
    conn.execute("CHECKPOINT")
    conn.execute("SELECT * FROM export_delta()").fetchall()

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
    delta_table_root = os.path.dirname(delta_log)

    ducklake_count = conn.execute("SELECT count(*) FROM sales").fetchone()[0]
    delta_count = conn.execute(
        f"SELECT count(*) FROM delta_scan('{delta_table_root}')"
    ).fetchone()[0]
    print(f"DuckLake count: {ducklake_count}, Delta count: {delta_count}")
    assert ducklake_count == delta_count, (
        f"Row count mismatch: DuckLake={ducklake_count}, Delta={delta_count}"
    )


def test_roundtrip_with_inline_threshold(ducklake_env):
    """Export with a custom inline threshold should still produce correct Delta data."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE products (id BIGINT, name VARCHAR, price DOUBLE)")
    conn.execute(
        "INSERT INTO products VALUES "
        "(1, 'Widget', 9.99), (2, 'Gadget', 19.99), (3, 'Gizmo', 29.99), "
        "(4, 'Doohickey', 39.99), (5, 'Thingamajig', 49.99)"
    )
    conn.execute("UPDATE products SET price = price * 0.9 WHERE price > 20")
    conn.execute("DELETE FROM products WHERE id = 2")
    conn.execute("INSERT INTO products VALUES (6, 'Contraption', 59.99)")

    # Set a small inline threshold so small data gets inlined, then flush it out
    conn.execute("CALL ducklake.set_option('inline_threshold', 1024)")
    conn.execute("CALL ducklake.set_option('rewrite_delete_threshold', 0)")
    conn.execute("CHECKPOINT")
    conn.execute("SELECT * FROM export_delta()").fetchall()

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
    delta_table_root = os.path.dirname(delta_log)

    ducklake_count = conn.execute("SELECT count(*) FROM products").fetchone()[0]
    delta_count = conn.execute(
        f"SELECT count(*) FROM delta_scan('{delta_table_root}')"
    ).fetchone()[0]
    print(f"DuckLake count: {ducklake_count}, Delta count: {delta_count}")
    assert ducklake_count == delta_count, (
        f"Row count mismatch: DuckLake={ducklake_count}, Delta={delta_count}"
    )
