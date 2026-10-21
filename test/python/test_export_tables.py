"""delta_export(tables := [...], schemas := [...]) exports only those tables and leaves every other table alone."""

import os

import duckdb
import pytest

from delta_readers import delta_rs_count, find_delta_roots


def _export(conn, **lists):
    args = ", ".join(f"{option} := {names!r}" for option, names in lists.items())
    return dict(conn.execute(f"SELECT table_name, status FROM delta_export({args})").fetchall())


def _data_files(conn, table):
    return conn.execute(
        "SELECT count(df.data_file_id) FROM __ducklake_metadata_test_lake.ducklake_table t "
        "LEFT JOIN __ducklake_metadata_test_lake.ducklake_data_file df USING (table_id) "
        "WHERE t.table_name = ? AND t.end_snapshot IS NULL",
        [table],
    ).fetchone()[0]


def test_exports_only_named_tables(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CALL test_lake.set_option('data_inlining_row_limit', 10)")
    conn.execute("CREATE TABLE keep (x INT)")
    conn.execute("CREATE TABLE skip (x INT)")
    conn.execute("INSERT INTO keep VALUES (1), (2)")
    conn.execute("INSERT INTO skip VALUES (3)")

    assert _export(conn, tables=["keep"]) == {"main.keep": "needs_export"}
    roots = find_delta_roots(data_path)
    assert set(roots) == {"keep"}
    # the named table's inlined rows were flushed and exported, the other table's stay inlined
    assert delta_rs_count(roots["keep"]) == 2
    assert _data_files(conn, "skip") == 0


def test_schema_qualified_name_and_ambiguous_name(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE SCHEMA s")
    conn.execute("CREATE TABLE main.t AS SELECT 1 AS x")
    conn.execute("CREATE TABLE s.t AS SELECT 2 AS x")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")

    with pytest.raises(duckdb.Error, match="matches main.t, s.t; name the schema"):
        _export(conn, tables=["t"])
    assert find_delta_roots(data_path) == {}

    # matched ignoring case, like DuckDB identifiers
    assert _export(conn, tables=["S.T"]) == {"s.t": "needs_export"}
    assert len(find_delta_roots(data_path)) == 1


def test_schemas_export_every_table_of_the_schema(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE SCHEMA sales")
    conn.execute("CREATE SCHEMA empty")
    for table in ("sales.a", "sales.b", "main.orders", "main.other"):
        conn.execute(f"CREATE TABLE {table} AS SELECT 1 AS x")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")

    # both options together: every table of the schemas plus the named tables
    assert _export(conn, schemas=["SALES"], tables=["main.orders"]) == {
        "sales.a": "needs_export",
        "sales.b": "needs_export",
        "main.orders": "needs_export",
    }
    assert _export(conn, schemas=["empty"]) == {}

    _export(conn)
    roots = find_delta_roots(data_path)
    conn.execute("DROP TABLE sales.b")
    conn.execute("DROP TABLE main.other")
    conn.execute("CREATE TABLE sales.c AS SELECT 1 AS x")

    # new tables of the schema are picked up; only the schema's dropped tables lose their log
    assert _export(conn, schemas=["sales"]) == {
        "sales.a": "needs_export",
        "sales.b": "removed_dropped",
        "sales.c": "needs_export",
    }
    assert not os.path.exists(os.path.join(roots["b"], "_delta_log"))
    assert os.path.exists(os.path.join(roots["other"], "_delta_log"))


def test_unknown_or_empty_names_fail(conn):
    conn.execute("CREATE TABLE t AS SELECT 1 AS x")
    with pytest.raises(duckdb.Error, match='no table "nope"'):
        _export(conn, tables=["t", "nope"])
    with pytest.raises(duckdb.Error, match='no schema "nope"'):
        _export(conn, schemas=["nope"])
    with pytest.raises(duckdb.Error, match="tables must not be empty"):
        _export(conn, tables=[])
    with pytest.raises(duckdb.Error, match="schemas must not be empty"):
        _export(conn, schemas=[])
    with pytest.raises(duckdb.Error, match="tables must not contain NULL"):
        conn.execute("SELECT * FROM delta_export(tables := ['t', NULL])")


def test_named_tables_keep_dropped_table_log(ducklake_env):
    conn, data_path = ducklake_env
    conn.execute("CREATE TABLE keep AS SELECT 1 AS x")
    conn.execute("CREATE TABLE gone AS SELECT 2 AS x")
    conn.execute("CALL ducklake_flush_inlined_data('test_lake')")
    _export(conn)
    gone_log = os.path.join(find_delta_roots(data_path)["gone"], "_delta_log")

    conn.execute("DROP TABLE gone")
    assert _export(conn, tables=["keep"]) == {"main.keep": "needs_export"}
    assert os.path.exists(gone_log)

    # the whole-catalog export still cleans it up
    assert _export(conn)["main.gone"] == "removed_dropped"
    assert not os.path.exists(gone_log)
