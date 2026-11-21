#!/usr/bin/env python3
"""
Tests of the harness itself: it must accept every output shape rules.md
accepts and reject every shape it does not. Run: python harness/test_check.py
"""
import contextlib
import datetime
import io
import os
import shutil
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check  # noqa: E402

DAY = datetime.date(2026, 10, 3)
USER = {"user_id": ["u1"], "day": [DAY], "sessions": [1], "events": [2],
        "revenue_cents": [0], "top_category": [None]}
CAMPAIGN = {"campaign_id": ["organic"], "channel": ["organic"], "day": [DAY], "sessions": [1],
            "purchasing_sessions": [0], "revenue_cents": [0], "assisted_sessions": [0]}
EXPECTED_CSV = {
    "user_daily.csv": "user_id,day,sessions,events,revenue_cents,top_category\nu1,2026-10-03,1,2,0,\n",
    "campaign_daily.csv": "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,"
                          "assisted_sessions\norganic,organic,2026-10-03,1,0,0,0\n",
}


def table(columns, types=None):
    types = types or {}
    arrays = {}
    for name, values in columns.items():
        if name in types:
            arrays[name] = pa.array(values, type=types[name])
        elif name == "day":
            arrays[name] = pa.array(values, type=pa.date32())
        elif isinstance(values[0], int):
            arrays[name] = pa.array(values, type=pa.int64())
        else:
            arrays[name] = pa.array(values, type=pa.string())
    return pa.table(arrays)


def write(root, name, tables):
    folder = os.path.join(root, name)
    os.makedirs(folder, exist_ok=True)
    for filename, data in tables.items():
        path = os.path.join(folder, filename)
        if isinstance(data, bytes):
            with open(path, "wb") as handle:
                handle.write(data)
        else:
            pq.write_table(data, path)


def outcome(user_files, campaign_files=None):
    root = tempfile.mkdtemp()
    expected = tempfile.mkdtemp()
    try:
        for name, content in EXPECTED_CSV.items():
            with open(os.path.join(expected, name), "w") as handle:
                handle.write(content)
        write(root, "user_daily", user_files)
        write(root, "campaign_daily", campaign_files or {"part-0.parquet": table(CAMPAIGN)})
        with contextlib.redirect_stdout(io.StringIO()) as out:
            matched = check.compare_folders(root, expected, 3)
        return matched, out.getvalue()
    finally:
        shutil.rmtree(root)
        shutil.rmtree(expected)


CASES = [
    ("plain int64 and string", True, {"part-0.parquet": table(USER)}),
    ("int8 / uint32 widths", True, {"p.parquet": table(USER, {"sessions": pa.int8(),
                                                              "events": pa.uint32()})}),
    ("large_string and dictionary strings", True,
     {"p.parquet": table(USER, {"user_id": pa.large_string()}).cast(pa.schema([
         ("user_id", pa.dictionary(pa.int32(), pa.string())), ("day", pa.date32()),
         ("sessions", pa.int64()), ("events", pa.int64()), ("revenue_cents", pa.int64()),
         ("top_category", pa.string())]))}),
    ("columns in another order", True,
     {"p.parquet": table(USER).select(["top_category", "events", "day", "user_id",
                                       "revenue_cents", "sessions"])}),
    ("rows split over two files", True,
     {"a.parquet": table(USER).slice(0, 0), "b.parquet": table(USER)}),
    ("ignored files: _SUCCESS, .hidden.parquet, _x.parquet, notes.txt", True,
     {"part-0.parquet": table(USER), "_SUCCESS": b"", ".hidden.parquet": table(USER),
      "_x.parquet": b"junk", "notes.txt": b"junk"}),
    ("a duplicate row in a second file", False,
     {"a.parquet": table(USER), "b.parquet": table(USER)}),
    ("an extra column", False, {"p.parquet": table({**USER, "extra": [1]})}),
    ("a missing column", False,
     {"p.parquet": table({k: v for k, v in USER.items() if k != "top_category"})}),
    ("a misnamed column", False,
     {"p.parquet": table({("userid" if k == "user_id" else k): v for k, v in USER.items()})}),
    ("day as a string", False, {"p.parquet": table({**USER, "day": ["2026-10-03"]}, {"day": pa.string()})}),
    ("day as a timestamp", False,
     {"p.parquet": table({**USER, "day": [datetime.datetime(2026, 10, 3)]},
                         {"day": pa.timestamp("ms")})}),
    ("sessions as a float", False, {"p.parquet": table(USER, {"sessions": pa.float64()})}),
    ("user_id as binary", False, {"p.parquet": table({**USER, "user_id": [b"u1"]},
                                                     {"user_id": pa.binary()})}),
    ("a wrong value", False, {"p.parquet": table({**USER, "events": [3]})}),
    ("empty string instead of null", False, {"p.parquet": table({**USER, "top_category": [""]},
                                                                {"top_category": pa.string()})}),
    ("no parquet files", False, {"_SUCCESS": b""}),
    ("an extra row", False, {"p.parquet": table({k: v * 2 for k, v in USER.items()}).set_column(
        0, "user_id", pa.array(["u1", "u2"]))}),
]


def main():
    failures = 0
    for name, should_match, files in CASES:
        matched, report = outcome(files)
        ok = matched == should_match
        failures += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {name}: {'match' if matched else 'differ'}")
        if not ok:
            print(report)
    print(f"\n{len(CASES) - failures}/{len(CASES)} harness tests passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
