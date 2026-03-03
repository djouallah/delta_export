"""Integration tests for the delta_export extension using a real DuckLake catalog."""

import json
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
    rows = conn.execute("SELECT * FROM delta_export()").fetchall()
    assert len(rows) == 1
    table_name, status, snapshot, explanation, message = rows[0]
    assert table_name == "main.test_table"
    assert status == "needs_export"


def test_export_creates_delta_files(ducklake_env):
    """Export should create checkpoint parquet, json, and _last_checkpoint."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE test_table (id BIGINT, name VARCHAR)")
    conn.execute("INSERT INTO test_table VALUES (1, 'Alice')")
    conn.execute("SELECT * FROM delta_export()").fetchall()

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
    conn.execute("SELECT * FROM delta_export()").fetchall()

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
    parquet_files = [f for f in os.listdir(delta_log) if f.endswith(".checkpoint.parquet")]
    parquet_path = os.path.join(delta_log, parquet_files[0])

    col_names = [d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 0").description]
    assert "protocol" in col_names
    assert "metaData" in col_names
    assert "add" in col_names


def test_multiple_tables(conn):
    """Export should handle multiple tables."""
    conn.execute("CREATE TABLE orders (id BIGINT, amount DOUBLE)")
    conn.execute("INSERT INTO orders VALUES (1, 99.99), (2, 149.50)")
    conn.execute("CREATE TABLE customers (id BIGINT, name VARCHAR)")
    conn.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
    rows = conn.execute("SELECT * FROM delta_export() ORDER BY table_name").fetchall()
    assert len(rows) == 2
    names = [r[0] for r in rows]
    assert "main.customers" in names
    assert "main.orders" in names
    assert all(r[1] == "needs_export" for r in rows)


def test_roundtrip_heavy_mutations(ducklake_env):
    """Stress test: bulk inserts via range(), multiple updates, deletes, and re-inserts."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE events (id BIGINT, category VARCHAR, value DOUBLE, active BOOLEAN)")

    # Bulk insert 1000 rows using range()
    conn.execute(
        "INSERT INTO events "
        "SELECT i, CASE i % 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' "
        "WHEN 3 THEN 'D' ELSE 'E' END, i * 1.5, true "
        "FROM range(1, 1001) t(i)"
    )
    # Delete 20% of rows
    conn.execute("DELETE FROM events WHERE id % 5 = 0")
    # Update half the remaining rows
    conn.execute("UPDATE events SET value = value * 2.0, category = 'X' WHERE id % 2 = 1")
    # Insert another batch
    conn.execute(
        "INSERT INTO events "
        "SELECT i, 'NEW', i * 0.1, false FROM range(1001, 1501) t(i)"
    )
    # Delete some of the new rows
    conn.execute("DELETE FROM events WHERE id > 1400")
    # Update across old and new rows
    conn.execute("UPDATE events SET active = false WHERE value > 500")
    # One more insert
    conn.execute(
        "INSERT INTO events "
        "SELECT i, 'FINAL', 0.0, true FROM range(2000, 2101) t(i)"
    )

    # Flush inlined data and rewrite files with deletes before export
    conn.execute("CALL test_lake.set_option('rewrite_delete_threshold', 0)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("CALL ducklake_rewrite_data_files('test_lake')")

    conn.execute("SELECT * FROM delta_export()").fetchall()

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
    delta_table_root = os.path.dirname(delta_log)

    ducklake_count = conn.execute("SELECT count(*) FROM events").fetchone()[0]
    delta_count = conn.execute(f"SELECT count(*) FROM delta_scan('{delta_table_root}')").fetchone()[0]
    print(f"DuckLake count: {ducklake_count}, Delta count: {delta_count}")
    assert ducklake_count == delta_count, f"Row count mismatch: DuckLake={ducklake_count}, Delta={delta_count}"

    # Also verify a few aggregate values match
    ducklake_sum = conn.execute("SELECT round(sum(value), 2) FROM events").fetchone()[0]
    delta_sum = conn.execute(f"SELECT round(sum(value), 2) FROM delta_scan('{delta_table_root}')").fetchone()[0]
    print(f"DuckLake sum: {ducklake_sum}, Delta sum: {delta_sum}")
    assert ducklake_sum == delta_sum, f"Sum mismatch: DuckLake={ducklake_sum}, Delta={delta_sum}"


def test_roundtrip_multiple_tables_with_mutations(ducklake_env):
    """Multiple tables with different mutation patterns, all exported and verified."""
    conn, data_path = ducklake_env

    # Table 1: heavy inserts, no deletes
    conn.execute("CREATE TABLE logs (id BIGINT, ts TIMESTAMP, msg VARCHAR)")
    conn.execute(
        "INSERT INTO logs "
        "SELECT i, '2024-01-01'::TIMESTAMP + INTERVAL (i) MINUTE, 'log entry ' || i "
        "FROM range(1, 501) t(i)"
    )

    # Table 2: insert then delete most rows
    conn.execute("CREATE TABLE temp_data (id BIGINT, payload VARCHAR)")
    conn.execute("INSERT INTO temp_data SELECT i, repeat('x', 50) FROM range(1, 301) t(i)")
    conn.execute("DELETE FROM temp_data WHERE id > 10")

    # Table 3: insert-update-delete cycles
    conn.execute("CREATE TABLE accounts (id BIGINT, balance DOUBLE, status VARCHAR)")
    conn.execute("INSERT INTO accounts SELECT i, i * 100.0, 'active' FROM range(1, 201) t(i)")
    conn.execute("UPDATE accounts SET balance = balance - 50 WHERE id <= 100")
    conn.execute("DELETE FROM accounts WHERE balance <= 0")
    conn.execute("UPDATE accounts SET status = 'premium' WHERE balance > 10000")
    conn.execute("INSERT INTO accounts SELECT i, 5000.0, 'new' FROM range(201, 251) t(i)")

    # Flush inlined data and rewrite files with deletes before export
    conn.execute("CALL test_lake.set_option('rewrite_delete_threshold', 0)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("CALL ducklake_rewrite_data_files('test_lake')")

    conn.execute("SELECT * FROM delta_export()").fetchall()

    # Verify each table
    for table in ['logs', 'temp_data', 'accounts']:
        delta_log = None
        for root, dirs, files in os.walk(data_path):
            if "_delta_log" in dirs and table in root:
                delta_log = os.path.join(root, "_delta_log")
                break
        if delta_log is None:
            # find by walking all delta logs
            for root, dirs, files in os.walk(data_path):
                if "_delta_log" in dirs:
                    delta_table_root = root
                    # check if this is the right table by reading the checkpoint
                    parquet_files = [
                        f for f in os.listdir(os.path.join(root, "_delta_log")) if f.endswith(".checkpoint.parquet")
                    ]
                    if parquet_files:
                        meta = conn.execute(
                            f"SELECT metaData.name FROM read_parquet('{os.path.join(root, '_delta_log', parquet_files[0])}') WHERE metaData IS NOT NULL"
                        ).fetchone()
                        if meta and meta[0] == table:
                            delta_log = os.path.join(root, "_delta_log")
                            break

        assert delta_log is not None, f"_delta_log not found for {table}"
        delta_table_root = os.path.dirname(delta_log)
        ducklake_count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        delta_count = conn.execute(f"SELECT count(*) FROM delta_scan('{delta_table_root}')").fetchone()[0]
        print(f"{table}: DuckLake={ducklake_count}, Delta={delta_count}")
        assert ducklake_count == delta_count, f"{table} count mismatch: DuckLake={ducklake_count}, Delta={delta_count}"


def test_roundtrip_with_inline_threshold(ducklake_env):
    """Export with inlined data: inserts go to metadata, flush should materialize them."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE products (id BIGINT, name VARCHAR, price DOUBLE)")

    # Enable data inlining so small inserts go to metadata instead of parquet
    conn.execute("CALL test_lake.set_option('data_inlining_row_limit', 100)")

    # Multiple small inserts that will be inlined
    for i in range(10):
        conn.execute(f"INSERT INTO products SELECT i, 'product_' || i, i * 9.99 FROM range({i * 10 + 1}, {i * 10 + 11}) t(i)")
    # Some mutations on inlined data
    conn.execute("UPDATE products SET price = price * 0.5 WHERE id <= 20")
    conn.execute("DELETE FROM products WHERE id % 7 = 0")
    conn.execute("INSERT INTO products VALUES (999, 'special', 0.01)")

    # Flush inlined data and rewrite files with deletes before export
    conn.execute("CALL test_lake.set_option('rewrite_delete_threshold', 0)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("CALL ducklake_rewrite_data_files('test_lake')")

    conn.execute("SELECT * FROM delta_export()").fetchall()

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
    delta_table_root = os.path.dirname(delta_log)

    ducklake_count = conn.execute("SELECT count(*) FROM products").fetchone()[0]
    delta_count = conn.execute(f"SELECT count(*) FROM delta_scan('{delta_table_root}')").fetchone()[0]
    print(f"DuckLake count: {ducklake_count}, Delta count: {delta_count}")
    assert ducklake_count == delta_count, f"Row count mismatch: DuckLake={ducklake_count}, Delta={delta_count}"


def test_sqlite_backend_export(ducklake_sqlite_env):
    """SQLite backend: bulk inserts, updates, deletes — full roundtrip verification."""
    conn, data_path = ducklake_sqlite_env
    conn.execute("CREATE TABLE orders (id BIGINT, customer VARCHAR, amount DOUBLE, status VARCHAR)")

    # Bulk insert 500 rows
    conn.execute(
        "INSERT INTO orders "
        "SELECT i, 'customer_' || (i % 50), i * 2.5, "
        "CASE WHEN i % 3 = 0 THEN 'shipped' WHEN i % 3 = 1 THEN 'pending' ELSE 'cancelled' END "
        "FROM range(1, 501) t(i)"
    )
    # Cancel and delete some
    conn.execute("UPDATE orders SET status = 'refunded' WHERE status = 'cancelled' AND id < 100")
    conn.execute("DELETE FROM orders WHERE status = 'refunded'")
    # Bulk update amounts
    conn.execute("UPDATE orders SET amount = amount * 1.1 WHERE status = 'shipped'")
    # Insert more
    conn.execute("INSERT INTO orders SELECT i, 'new_customer', i * 0.5, 'pending' FROM range(501, 601) t(i)")
    # Delete high-value orders
    conn.execute("DELETE FROM orders WHERE amount > 1000")

    # Flush inlined data and rewrite files with deletes before export
    conn.execute("CALL test_lake.set_option('rewrite_delete_threshold', 0)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("CALL ducklake_rewrite_data_files('test_lake')")

    rows = conn.execute("SELECT * FROM delta_export()").fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "needs_export"

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"
    files = os.listdir(delta_log)
    assert any(f.endswith(".checkpoint.parquet") for f in files)
    assert any(f.endswith(".json") for f in files)
    assert "_last_checkpoint" in files

    # Verify row count matches
    delta_table_root = os.path.dirname(delta_log)
    ducklake_count = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
    delta_count = conn.execute(f"SELECT count(*) FROM delta_scan('{delta_table_root}')").fetchone()[0]
    print(f"SQLite backend: DuckLake={ducklake_count}, Delta={delta_count}")
    assert ducklake_count == delta_count, f"SQLite row count mismatch: DuckLake={ducklake_count}, Delta={delta_count}"


def test_dbt_duckdb_pattern(extension_path, tmp_path):
    """Simulate dbt-duckdb: :memory: conn, ATTACH ducklake:sqlite: AS ducklake, USE ducklake.

    This mirrors profiles.yml from github.com/djouallah/dbt:
      path: ':memory:'
      database: ducklake
      attach:
        - path: "ducklake:sqlite:{METADATA_LOCAL_PATH}"
          alias: ducklake
    """
    import duckdb

    sqlite_path = str(tmp_path / "metadata.db")
    data_path = str(tmp_path / "data")

    # Exactly how dbt-duckdb sets up the connection
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute(
        f"INSTALL '{extension_path}';"
        f"LOAD delta_export;"
        f"ATTACH 'ducklake:sqlite:{sqlite_path}' AS ducklake (DATA_PATH '{data_path}');"
        f"USE ducklake;"
    )

    # Simulate dbt models creating tables, then on-run-end: flush, rewrite
    conn.execute(
        "CREATE SCHEMA IF NOT EXISTS aemo;"
        "CREATE TABLE aemo.demand (ts TIMESTAMP, region VARCHAR, value DOUBLE);"
        "INSERT INTO aemo.demand "
        "SELECT '2024-01-01'::TIMESTAMP + INTERVAL (i) HOUR, "
        "CASE i % 3 WHEN 0 THEN 'NSW' WHEN 1 THEN 'VIC' ELSE 'QLD' END, "
        "i * 100.0 "
        "FROM range(1, 201) t(i);"
        "CREATE TABLE aemo.prices (ts TIMESTAMP, region VARCHAR, price DOUBLE);"
        "INSERT INTO aemo.prices "
        "SELECT '2024-01-01'::TIMESTAMP + INTERVAL (i) HOUR, "
        "CASE i % 3 WHEN 0 THEN 'NSW' WHEN 1 THEN 'VIC' ELSE 'QLD' END, "
        "i * 5.5 "
        "FROM range(1, 101) t(i);"
        "CALL ducklake.set_option('rewrite_delete_threshold', 0);"
        "CALL ducklake_flush_inlined_data('ducklake');"
        "CALL ducklake_rewrite_data_files('ducklake');"
    )
    conn.execute("CALL delta_export()")

    # Compare DuckLake tables vs delta_scan for each table
    tables_verified = 0
    for root, dirs, files in os.walk(data_path):
        if "_delta_log" in dirs:
            delta_log = os.path.join(root, "_delta_log")
            parquet_files = [f for f in os.listdir(delta_log) if f.endswith(".checkpoint.parquet")]
            meta = conn.execute(
                f"SELECT metaData.name FROM read_parquet('{os.path.join(delta_log, parquet_files[0])}') "
                f"WHERE metaData IS NOT NULL"
            ).fetchone()
            table_name = meta[0] if meta else "unknown"
            ducklake_count = conn.execute(f"SELECT count(*) FROM aemo.{table_name}").fetchone()[0]
            delta_count = conn.execute(f"SELECT count(*) FROM delta_scan('{root}')").fetchone()[0]
            print(f"dbt pattern - {table_name}: DuckLake={ducklake_count}, Delta={delta_count}")
            assert (
                ducklake_count == delta_count
            ), f"{table_name} mismatch: DuckLake={ducklake_count}, Delta={delta_count}"
            tables_verified += 1

    assert tables_verified == 2, f"Expected 2 tables verified, got {tables_verified}"
    conn.close()


def test_data_type_mappings(ducklake_env):
    """Verify DuckDB types are correctly mapped to Delta Lake types in the schema."""
    conn, data_path = ducklake_env
    conn.execute(
        "CREATE TABLE typed_table ("
        "  col_tinyint TINYINT,"
        "  col_smallint SMALLINT,"
        "  col_integer INTEGER,"
        "  col_bigint BIGINT,"
        "  col_float FLOAT,"
        "  col_double DOUBLE,"
        "  col_boolean BOOLEAN,"
        "  col_varchar VARCHAR,"
        "  col_blob BLOB,"
        "  col_date DATE,"
        "  col_timestamp TIMESTAMP"
        ")"
    )
    conn.execute(
        "INSERT INTO typed_table VALUES "
        "(1, 100, 1000, 100000, 1.5, 2.5, true, 'hello', '\\x0102'::BLOB, '2024-01-01', '2024-01-01 12:00:00')"
    )
    conn.execute("SELECT * FROM delta_export()").fetchall()

    delta_log = _find_delta_log(data_path)
    assert delta_log is not None, f"_delta_log not found under {data_path}"

    # Read the checkpoint parquet and extract the metaData schemaString
    parquet_files = [f for f in os.listdir(delta_log) if f.endswith(".checkpoint.parquet")]
    parquet_path = os.path.join(delta_log, parquet_files[0])
    rows = conn.execute(
        f"SELECT metaData.schemaString FROM read_parquet('{parquet_path}') WHERE metaData IS NOT NULL"
    ).fetchall()
    assert len(rows) == 1, f"Expected 1 metaData row, got {len(rows)}"

    schema = json.loads(rows[0][0])
    fields = {f["name"]: f["type"] for f in schema["fields"]}
    print(f"Schema fields: {fields}")

    # Print all mappings for diagnosis, then assert
    expected = {
        "col_tinyint": "byte",
        "col_smallint": "short",
        "col_integer": "integer",
        "col_bigint": "long",
        "col_float": "float",
        "col_double": "double",
        "col_boolean": "boolean",
        "col_varchar": "string",
        "col_blob": "binary",
        "col_date": "date",
        "col_timestamp": "timestamp",
    }
    for col, exp in expected.items():
        actual = fields.get(col, "MISSING")
        print(f"  {col}: expected={exp}, got={actual}, {'OK' if actual == exp else 'MISMATCH'}")
    for col, exp in expected.items():
        assert fields[col] == exp, f"{col}: expected {exp}, got {fields[col]}"

    # Also verify delta_scan can read the data back
    delta_table_root = os.path.dirname(delta_log)
    delta_count = conn.execute(f"SELECT count(*) FROM delta_scan('{delta_table_root}')").fetchone()[0]
    assert delta_count == 1, f"Expected 1 row from delta_scan, got {delta_count}"
