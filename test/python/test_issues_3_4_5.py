"""Issues #3 (function docs), #4 (dropped tables), #5 (deletes, replaced tables) and the stats column join."""

import json
import os
import shutil

import duckdb

from delta_readers import delta_rs_count, delta_rs_rows, find_delta_roots


def _connect(extension_path, meta_path, data_path):
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute(
        f"INSTALL '{extension_path}'; LOAD delta_export; INSTALL ducklake; INSTALL delta;"
        f"ATTACH 'ducklake:{meta_path}' AS test_lake (DATA_PATH '{data_path}'); USE test_lake;"
    )
    return conn


def _export(conn, flush=True):
    if flush:
        conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    return conn.execute("CALL delta_export()").fetchall()


def _versions(root):
    return sorted(int(f[:20]) for f in os.listdir(os.path.join(root, "_delta_log")) if f[:20].isdigit())


def _checkpoint(root):
    with open(os.path.join(root, "_delta_log", "_last_checkpoint")) as f:
        version = json.load(f)["version"]
    return os.path.join(root, "_delta_log", f"{version:020d}.checkpoint.parquet")


def _protocol(conn, root):
    return conn.execute(
        f"SELECT protocol FROM read_parquet('{_checkpoint(root)}') WHERE protocol IS NOT NULL"
    ).fetchone()[0]


def _assert_same_rows(conn, table, root, columns, order_by):
    cols = ", ".join(columns)
    expected = conn.execute(f"SELECT {cols} FROM {table} ORDER BY {order_by}").fetchall()
    assert conn.execute(f"SELECT {cols} FROM delta_scan('{root}') ORDER BY {order_by}").fetchall() == expected
    assert delta_rs_count(root) == len(expected)
    assert delta_rs_rows(root, columns, order_by) == expected


# ── #3 ──


def test_function_has_description(conn):
    description, examples = conn.execute(
        "SELECT description, examples FROM duckdb_functions() WHERE function_name = 'delta_export'"
    ).fetchone()
    assert description
    assert examples and all("delta_export(" in e for e in examples)


# ── #5a: deletes become deletion vectors ──


def test_deletes_update_merge_without_rewrite(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE events AS SELECT i AS id, i % 10 AS grp, i * 1.5 AS value FROM range(5000) t(i)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("DELETE FROM events WHERE id % 7 = 0")
    conn.execute("UPDATE events SET value = -1 WHERE id % 11 = 0")
    conn.execute(
        "MERGE INTO events USING (SELECT i AS id FROM range(4990, 5010) t(i)) src ON events.id = src.id "
        "WHEN MATCHED THEN UPDATE SET value = 0 "
        "WHEN NOT MATCHED THEN INSERT VALUES (src.id, -1, 0)"
    )
    _export(conn)  # deliberately no ducklake_rewrite_data_files

    root = find_delta_roots(data_path)["events"]
    assert any(f.startswith("deletion_vector_") for f in os.listdir(root))
    protocol = _protocol(conn, root)
    assert (protocol["minReaderVersion"], protocol["minWriterVersion"]) == (3, 7)
    assert protocol["readerFeatures"] == ["deletionVectors"]
    _assert_same_rows(conn, "events", root, ["id", "grp", "value"], "id")


def test_inlined_deletes_without_flush(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE small AS SELECT i AS id FROM range(100) t(i)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("DELETE FROM small WHERE id IN (3, 50, 99)")  # few rows: DuckLake inlines the delete
    _export(conn, flush=False)

    root = find_delta_roots(data_path)["small"]
    _assert_same_rows(conn, "small", root, ["id"], "id")


def test_partitioned_table_with_deletes(ducklake_env):
    """The issue #5 shape: partitioned, several inserts, update, delete, add column, merge."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE sales (id INTEGER, region VARCHAR, amount DOUBLE)")
    conn.execute("ALTER TABLE sales SET PARTITIONED BY (region)")
    for batch in range(3):
        conn.execute(
            f"INSERT INTO sales SELECT i, ['east', 'west', 'north'][i % 3 + 1], i * 2.0 "
            f"FROM range({batch * 2000}, {(batch + 1) * 2000}) t(i)"
        )
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("UPDATE sales SET amount = amount + 1 WHERE id % 13 = 0")
    conn.execute("DELETE FROM sales WHERE id % 4 = 0")
    conn.execute("ALTER TABLE sales ADD COLUMN note VARCHAR")
    conn.execute(
        "MERGE INTO sales USING (SELECT i AS id FROM range(5900, 6100) t(i)) src ON sales.id = src.id "
        "WHEN MATCHED THEN UPDATE SET note = 'merged' "
        "WHEN NOT MATCHED THEN INSERT VALUES (src.id, 'east', 0, 'new')"
    )
    _export(conn)

    root = find_delta_roots(data_path)["sales"]
    _assert_same_rows(conn, "sales", root, ["id", "region", "amount", "note"], "id")


def test_table_without_deletes_keeps_protocol_1_2(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE plain AS SELECT i AS id FROM range(10) t(i)")
    _export(conn)
    root = find_delta_roots(data_path)["plain"]
    protocol = _protocol(conn, root)
    assert (protocol["minReaderVersion"], protocol["minWriterVersion"]) == (1, 2)
    assert not any(f.startswith("deletion_vector_") for f in os.listdir(root))


def test_rewrite_after_export_drops_deletion_vector(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE t AS SELECT i AS id FROM range(1000) t(i)")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    conn.execute("DELETE FROM t WHERE id < 300")
    _export(conn)
    root = find_delta_roots(data_path)["t"]
    assert any(f.startswith("deletion_vector_") for f in os.listdir(root))

    conn.execute("CALL test_lake.set_option('rewrite_delete_threshold', 0)")
    conn.execute("CALL ducklake_rewrite_data_files('test_lake')")
    _export(conn)
    assert _protocol(conn, root)["minReaderVersion"] == 1
    # the previous version's deletion vector file stays for readers still on that version
    assert any(f.startswith("deletion_vector_") for f in os.listdir(root))
    _assert_same_rows(conn, "t", root, ["id"], "id")


# ── #5b: Delta version always moves forward ──


def test_reexport_same_snapshot_is_idempotent(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE t AS SELECT i AS id FROM range(10) t(i)")
    _export(conn)
    root = find_delta_roots(data_path)["t"]
    first = _versions(root)
    _export(conn)
    assert _versions(root) == first


def test_replace_table_same_folder_reused_catalog(ducklake_env):
    conn, data_path = ducklake_env
    for run, rows in enumerate([3000, 1700]):
        conn.execute(f"CREATE OR REPLACE TABLE sales AS SELECT i AS id, {run} AS run FROM range({rows}) t(i)")
        conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
        conn.execute("DELETE FROM sales WHERE id % 3 = 0")
        conn.execute("CALL ducklake_merge_adjacent_files('test_lake')")
        conn.execute("CALL ducklake_expire_snapshots('test_lake', older_than => now())")
        conn.execute("CALL ducklake_cleanup_old_files('test_lake', cleanup_all => true)")
        _export(conn)
        root = find_delta_roots(data_path)["sales"]
        _assert_same_rows(conn, "sales", root, ["id", "run"], "id")


def test_fresh_catalog_same_data_path(extension_path, tmp_path):
    """A recreated catalog restarts snapshot ids below the existing log; the new export must still win."""
    data_path = str(tmp_path / "data")
    meta = str(tmp_path / "meta.ducklake")
    conn = _connect(extension_path, meta, data_path)
    conn.execute("CREATE TABLE t (id INTEGER)")
    for batch in range(8):
        conn.execute(f"INSERT INTO t SELECT i FROM range({batch * 100}, {(batch + 1) * 100}) t(i)")
    _export(conn)
    root = find_delta_roots(data_path)["t"]
    old_max = max(_versions(root))
    conn.close()
    for leftover in (meta, meta + ".wal"):
        if os.path.exists(leftover):
            os.remove(leftover)

    conn = _connect(extension_path, meta, data_path)
    conn.execute("CREATE TABLE t AS SELECT i AS id FROM range(42) t(i)")
    _export(conn)
    assert max(_versions(root)) > old_max
    _assert_same_rows(conn, "t", root, ["id"], "id")
    conn.close()


# ── #4: dropped tables ──


def test_drop_table_removes_delta_log(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE keep AS SELECT 1 AS x")
    conn.execute("CREATE TABLE gone AS SELECT 2 AS x")
    _export(conn)
    roots = find_delta_roots(data_path)
    assert set(roots) == {"keep", "gone"}

    conn.execute("DROP TABLE gone")
    rows = _export(conn)
    assert ("main.gone", "removed_dropped") in [(r[0], r[1]) for r in rows]
    assert not os.path.exists(os.path.join(roots["gone"], "_delta_log"))
    assert set(find_delta_roots(data_path)) == {"keep"}
    assert delta_rs_count(roots["keep"]) == 1

    # nothing left to remove on the next run
    assert "removed_dropped" not in [r[1] for r in _export(conn)]


def test_drop_after_snapshot_expiry_removes_delta_log(ducklake_env):
    """Snapshot expiry may purge the dropped table's rows; the log is then found through the schema folder."""
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE gone AS SELECT 2 AS x")
    _export(conn)
    root = find_delta_roots(data_path)["gone"]
    conn.execute("DROP TABLE gone")
    conn.execute("CALL ducklake_expire_snapshots('test_lake', older_than => now())")
    conn.execute("CALL ducklake_cleanup_old_files('test_lake', cleanup_all => true)")

    rows = _export(conn)
    assert ("main.gone", "removed_dropped") in [(r[0], r[1]) for r in rows]
    assert not os.path.exists(os.path.join(root, "_delta_log"))


def test_foreign_delta_logs_untouched(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE mine AS SELECT 1 AS x")
    _export(conn)
    mine = find_delta_roots(data_path)["mine"]
    schema_dir = os.path.dirname(mine)

    # a Delta table some other engine wrote into the schema folder
    foreign_log = os.path.join(schema_dir, "foreign", "_delta_log")
    os.makedirs(foreign_log)
    with open(os.path.join(foreign_log, "00000000000000000000.json"), "w") as f:
        f.write('{"commitInfo":{"engineInfo":"Apache-Spark/3.5.3 Delta-Lake/3.2.1"}}\n')
    with open(os.path.join(foreign_log, "_last_checkpoint"), "w") as f:
        f.write('{"version":0,"size":3}')

    # a log delta_export wrote for another DuckLake catalog sharing the same data path
    other = os.path.join(schema_dir, "other_catalog_table")
    shutil.copytree(mine, other)
    for name in os.listdir(os.path.join(other, "_delta_log")):
        if name.endswith(".json"):
            path = os.path.join(other, "_delta_log", name)
            with open(path) as f:
                content = json.loads(f.readline())
            uuid = content["commitInfo"]["operationParameters"]["duckLakeSchemaUuid"]
            with open(path) as f:
                text = f.read()
            with open(path, "w") as f:
                f.write(text.replace(uuid, "00000000-0000-0000-0000-000000000000"))

    rows = _export(conn)
    assert [r[1] for r in rows] == ["needs_export"]
    assert os.path.exists(os.path.join(foreign_log, "_last_checkpoint"))
    assert os.path.exists(os.path.join(other, "_delta_log", "_last_checkpoint"))


# ── stats: column_id is only unique within a table ──


def test_stats_only_reference_own_columns(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE a AS SELECT i AS id, 'x' || i AS label FROM range(10) t(i)")
    conn.execute("CREATE TABLE b AS SELECT i * 100 AS other_id, 'y' AS other_label FROM range(10) t(i)")
    _export(conn)
    roots = find_delta_roots(data_path)
    for table, columns in {"a": {"id", "label"}, "b": {"other_id", "other_label"}}.items():
        stats = conn.execute(
            f"SELECT add.stats FROM read_parquet('{_checkpoint(roots[table])}') WHERE add IS NOT NULL"
        ).fetchall()
        for (raw,) in stats:
            parsed = json.loads(raw)
            assert set(parsed["minValues"]) <= columns, (table, parsed)
            assert set(parsed["maxValues"]) <= columns, (table, parsed)
