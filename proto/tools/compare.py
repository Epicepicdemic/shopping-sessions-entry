#!/usr/bin/env python3
"""Compare every key and field with bounded-memory DuckDB aggregates."""
import argparse
from pathlib import Path
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = {
    "user_daily": (
        ("user_id", "VARCHAR"), ("day", "DATE"), ("sessions", "BIGINT"),
        ("events", "BIGINT"), ("revenue_cents", "BIGINT"), ("top_category", "VARCHAR"),
    ),
    "campaign_daily": (
        ("campaign_id", "VARCHAR"), ("day", "DATE"), ("channel", "VARCHAR"),
        ("sessions", "BIGINT"), ("purchasing_sessions", "BIGINT"),
        ("revenue_cents", "BIGINT"), ("assisted_sessions", "BIGINT"),
    ),
}


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def string_type(typ):
    if pa.types.is_dictionary(typ):
        typ = typ.value_type
    return pa.types.is_string(typ) or pa.types.is_large_string(typ) or pa.types.is_string_view(typ)


def relation(root, table, allow_csv):
    columns = SCHEMA[table]
    cast = ", ".join(f"{col}::{typ} AS {col}" for col, typ in columns)
    csv = root / f"{table}.csv"
    if allow_csv and csv.is_file():
        types = ", ".join(f"{literal(col)}: {literal(typ)}" for col, typ in columns)
        return f"SELECT {cast} FROM read_csv({literal(csv)}, header=true, types={{{types}}})"
    parts = sorted(
        path for path in (root / table).glob("*.parquet")
        if path.is_file() and not path.is_symlink() and path.name[:1] not in (".", "_")
    )
    if not parts:
        raise ValueError(f"no Parquet parts for {root / table}")
    for part in parts:
        schema = pq.ParquetFile(part).schema_arrow
        if sorted(schema.names) != sorted(col for col, _ in columns):
            raise ValueError(f"{part} has the wrong columns: {schema.names}")
        for col, typ in columns:
            actual = schema.field(col).type
            ok = {"VARCHAR": string_type, "DATE": pa.types.is_date32, "BIGINT": pa.types.is_integer}[typ]
            if not ok(actual):
                raise ValueError(f"{part} has {actual} for {col}")
    paths = "[" + ", ".join(map(literal, parts)) + "]"
    return f"SELECT {cast} FROM read_parquet({paths}, union_by_name=true)"


def compare(db, table, actual, expected):
    key, date = [column for column, _ in SCHEMA[table][:2]]
    values = ", ".join(f"{column}: {column}" for column, _ in SCHEMA[table][2:])
    actual_scan = relation(actual, table, False)
    expected_scan = relation(expected, table, True)
    db.execute(f"CREATE OR REPLACE VIEW actual AS {actual_scan}")
    db.execute(f"CREATE OR REPLACE VIEW expected AS {expected_scan}")
    stats = db.execute(f"""
        WITH both_sides AS (
            SELECT *, true AS ours FROM actual
            UNION ALL
            SELECT *, false AS ours FROM expected
        ), keyed AS (
            SELECT count(*) FILTER (WHERE ours) AS a,
                   count(*) FILTER (WHERE NOT ours) AS e,
                   first({{{values}}}) FILTER (WHERE ours) AS av,
                   first({{{values}}}) FILTER (WHERE NOT ours) AS ev
            FROM both_sides
            GROUP BY {key}, {date}
        )
        SELECT coalesce(sum(a), 0), coalesce(sum(e), 0),
               count(*) FILTER (WHERE a = 0), count(*) FILTER (WHERE e = 0),
               count(*) FILTER (WHERE a > 0 AND e > 0 AND av IS DISTINCT FROM ev),
               count(*) FILTER (WHERE a > 1), count(*) FILTER (WHERE e > 1)
        FROM keyed
    """).fetchone()
    n, wanted, missing, extra, differing, duplicates, expected_duplicates = stats
    equal = not any(stats[2:])
    print(
        f"{table}: {'MATCH' if equal else 'DIFFER'} rows {n} (expected {wanted}), "
        f"missing {missing}, extra {extra}, differing {differing}, duplicate keys {duplicates}, "
        f"expected duplicate keys {expected_duplicates}",
        flush=True,
    )

    counts = "sum(sessions), sum(revenue_cents)"
    if table == "campaign_daily":
        counts += ", sum(assisted_sessions), sum(sessions) FILTER (WHERE campaign_id <> 'organic')"
    totals = tuple(value or 0 for value in db.execute(f"SELECT {counts} FROM actual").fetchone())
    return equal, totals


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("actual", type=Path)
    parser.add_argument("expected", type=Path)
    parser.add_argument("--temp-root", type=Path, required=True)
    parser.add_argument("--memory", default="768MB")
    parser.add_argument("--threads", type=int, default=3)
    args = parser.parse_args()
    args.temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="compare-", dir=args.temp_root) as temp:
        with duckdb.connect(config={
            "memory_limit": args.memory, "threads": args.threads,
            "temp_directory": temp, "preserve_insertion_order": False,
        }) as db:
            users_ok, (sessions, revenue) = compare(db, "user_daily", args.actual, args.expected)
            campaigns_ok, (campaigns, earned, assists, credited) = compare(
                db, "campaign_daily", args.actual, args.expected
            )
    print(f"sessions user/campaign {sessions}/{campaigns}, revenue {revenue}/{earned}, "
          f"assists {assists}, credited non-organic sessions {credited}")
    return not (users_ok and campaigns_ok and sessions == campaigns and revenue == earned and assists >= credited)


if __name__ == "__main__":
    raise SystemExit(main())
