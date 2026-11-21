#!/usr/bin/env python3
"""
The comparison harness: runs any implementation and checks its two output
tables against the reference, row for row.

An implementation is a command line. It gets DATA_DIR, OUTPUT_DIR and
SCRATCH_DIR (plus NCPU and MEM_GB) in its environment, reads DATA_DIR and
writes OUTPUT_DIR/user_daily/ and OUTPUT_DIR/campaign_daily/ as Parquet.

    # One data set, against the reference (run now, or reused from a cache):
    check.py data DATA_DIR [--expected DIR] [--reference-cache DIR]
                           [--keep-output DIR] [--show N] -- CMD ...

    # Fixture folders (each case has input/ and expected/*.csv):
    check.py fixtures ROOT [ROOT ...] [--show N] -- CMD ...

    # Two finished outputs, no running at all:
    check.py compare ACTUAL_DIR EXPECTED_DIR [--show N]

An expected folder holds either the two tables as Parquet folders (the shape
an implementation writes) or as user_daily.csv and campaign_daily.csv (the
shape of dev-answer.zip and of the fixtures' expected/ folders).

The actual output is loaded the way rules.md says it is read: every
`*.parquet` file directly in the table's folder, skipping symlinks, other
files and names starting with `.` or `_`; exactly the required columns, no
extras; any integer width; any Parquet string encoding; `day` a Parquet DATE.
The tables are then compared as sets of rows. Rows are matched on the table's
key ((user_id, day) or (campaign_id, day)) only to say which rows are
missing, extra or differing; a duplicate key is itself a failure.

Exit status: 0 match, 1 differ, 2 the implementation or the harness failed.
Requires pyarrow.
"""
import csv
import datetime
import io
import os
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import redirect_stdout

import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
REFERENCE = os.path.join(os.path.dirname(HERE), "reference", "reference.py")

# rules.md, Outputs: the columns of each table, the key that makes a row
# unique, and which columns may hold null.
TABLES = {
    "user_daily": {
        "columns": {"user_id": "string", "day": "date", "sessions": "integer",
                    "events": "integer", "revenue_cents": "integer",
                    "top_category": "string"},
        "key": ("user_id", "day"),
        "nullable": {"top_category"},
    },
    "campaign_daily": {
        "columns": {"campaign_id": "string", "channel": "string", "day": "date",
                    "sessions": "integer", "purchasing_sessions": "integer",
                    "revenue_cents": "integer", "assisted_sessions": "integer"},
        "key": ("campaign_id", "day"),
        "nullable": set(),
    },
}


class OutputError(Exception):
    """The output cannot be read as the rules require (missing folder, wrong
    columns or types, unreadable Parquet)."""


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def arrow_kind(arrow_type):
    if pa.types.is_dictionary(arrow_type):
        return arrow_kind(arrow_type.value_type)
    if pa.types.is_integer(arrow_type):
        return "integer"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type) \
            or (hasattr(pa.types, "is_string_view") and pa.types.is_string_view(arrow_type)):
        return "string"
    if pa.types.is_date(arrow_type):
        return "date"
    return str(arrow_type)


def parquet_files(folder):
    """The files rules.md says make up a table: `*.parquet` directly in the
    folder, not symlinks, not names starting with `.` or `_`."""
    if os.path.islink(folder) or not os.path.isdir(folder):
        raise OutputError(f"{folder}: not a directory")
    paths = []
    for name in sorted(os.listdir(folder)):
        path = os.path.join(folder, name)
        if not name.endswith(".parquet") or name.startswith((".", "_")):
            continue
        if os.path.islink(path) or not os.path.isfile(path):
            continue
        paths.append(path)
    if not paths:
        raise OutputError(f"{folder}: no Parquet files")
    return paths


def check_file_schema(path, spec):
    """Raise OutputError unless the file has exactly the required columns
    with acceptable types, and `day` is a Parquet DATE."""
    try:
        parquet = pq.ParquetFile(path)
    except Exception as error:  # noqa: BLE001 - any read failure is an output error
        raise OutputError(f"{path}: cannot read as Parquet: {error}") from error
    arrow_schema = parquet.schema_arrow
    names = arrow_schema.names
    wanted = spec["columns"]
    if len(names) != len(set(names)):
        raise OutputError(f"{path}: duplicate column names {names}")
    if set(names) != set(wanted):
        missing = sorted(set(wanted) - set(names))
        extra = sorted(set(names) - set(wanted))
        raise OutputError(f"{path}: wrong columns; missing {missing}, extra {extra}")
    for name, kind in wanted.items():
        actual = arrow_kind(arrow_schema.field(name).type)
        if actual != kind:
            raise OutputError(f"{path}: column {name} is {arrow_schema.field(name).type}, "
                              f"expected {kind}")
    # "`day` must be a Parquet `DATE`": check the Parquet logical type itself,
    # not only what Arrow made of it.
    parquet_schema = parquet.schema
    for index in range(len(parquet_schema)):
        column = parquet_schema.column(index)
        if column.path == "day":
            logical = str(column.logical_type)
            if "Date" not in logical and column.converted_type != "DATE":
                raise OutputError(f"{path}: day has Parquet logical type {logical}, "
                                  f"expected DATE")
    return parquet


def load_parquet_table(folder, table):
    """List of row tuples in the column order of TABLES[table]."""
    spec = TABLES[table]
    order = list(spec["columns"])
    rows = []
    for path in parquet_files(folder):
        parquet = check_file_schema(path, spec)
        data = parquet.read()
        columns = [data.column(name).to_pylist() for name in order]
        rows.extend(zip(*columns))
    return rows


def load_csv_table(path, table):
    """Rows of an expected CSV (dev-answer.zip or a fixture's expected/):
    a header row, `day` as YYYY-MM-DD, an empty top_category for null."""
    spec = TABLES[table]
    order = list(spec["columns"])
    rows = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        if set(header) != set(order):
            raise OutputError(f"{path}: header {header} does not match {order}")
        for record in reader:
            values = dict(zip(header, record))
            row = []
            for name in order:
                value = values[name]
                kind = spec["columns"][name]
                if kind == "integer":
                    row.append(int(value))
                elif kind == "date":
                    row.append(datetime.date.fromisoformat(value))
                elif value == "" and name in spec["nullable"]:
                    row.append(None)
                else:
                    row.append(value)
            rows.append(tuple(row))
    return rows


def load_output(folder, table):
    csv_path = os.path.join(folder, f"{table}.csv")
    if os.path.isfile(csv_path):
        return load_csv_table(csv_path, table)
    return load_parquet_table(os.path.join(folder, table), table)


# ---------------------------------------------------------------------------
# Comparing
# ---------------------------------------------------------------------------

def null_problems(rows, table):
    spec = TABLES[table]
    order = list(spec["columns"])
    problems = []
    for index, name in enumerate(order):
        if name in spec["nullable"]:
            continue
        nulls = sum(1 for row in rows if row[index] is None)
        if nulls:
            problems.append(f"{nulls} nulls in non-nullable column {name}")
    return problems


def compare_table(actual, expected, table):
    """A dict describing how `actual` differs from `expected`."""
    spec = TABLES[table]
    order = list(spec["columns"])
    key_index = [order.index(name) for name in spec["key"]]

    def keyed(rows):
        by_key, duplicates = {}, []
        for row in rows:
            key = tuple(row[i] for i in key_index)
            if key in by_key:
                duplicates.append(row)
            else:
                by_key[key] = row
        return by_key, duplicates

    actual_by_key, actual_duplicates = keyed(actual)
    expected_by_key, expected_duplicates = keyed(expected)
    missing = [expected_by_key[k] for k in expected_by_key if k not in actual_by_key]
    extra = [actual_by_key[k] for k in actual_by_key if k not in expected_by_key]
    differing = [(expected_by_key[k], actual_by_key[k]) for k in expected_by_key
                 if k in actual_by_key and expected_by_key[k] != actual_by_key[k]]
    # The plain set comparison the rules describe, as a cross-check of the keyed one.
    set_equal = set(actual) == set(expected) and len(actual) == len(set(actual))
    result = {
        "table": table,
        "actual_rows": len(actual),
        "expected_rows": len(expected),
        "missing": missing,
        "extra": extra,
        "differing": differing,
        "duplicates": actual_duplicates,
        "expected_duplicates": expected_duplicates,
        "nulls": null_problems(actual, table),
    }
    result["match"] = (set_equal and not missing and not extra and not differing
                       and not actual_duplicates and not result["nulls"])
    return result


def sort_key(row):
    return tuple("" if value is None else str(value) for value in row)


def describe(result, show):
    table = result["table"]
    lines = [f"{table}: {'MATCH' if result['match'] else 'DIFFER'}  "
             f"rows {result['actual_rows']} (expected {result['expected_rows']}); "
             f"missing {len(result['missing'])}, extra {len(result['extra'])}, "
             f"differing {len(result['differing'])}, duplicate keys {len(result['duplicates'])}"]
    if result["expected_duplicates"]:
        lines.append(f"  WARNING: the expected table has {len(result['expected_duplicates'])} "
                     f"duplicate keys")
    for problem in result["nulls"]:
        lines.append(f"  {problem}")
    columns = list(TABLES[table]["columns"])
    if not result["match"]:
        lines.append(f"  columns: {columns}")
    for label, word in (("missing", "missing"), ("extra", "extra"), ("duplicates", "duplicate")):
        for row in sorted(result[label], key=sort_key)[:show]:
            lines.append(f"  {word}: {row!r}")
    for expected_row, actual_row in sorted(result["differing"], key=lambda pair: sort_key(pair[0]))[:show]:
        changed = [f"{name}: expected {e!r}, got {a!r}"
                   for name, e, a in zip(columns, expected_row, actual_row) if e != a]
        key = tuple(expected_row[columns.index(name)] for name in TABLES[table]["key"])
        lines.append(f"  differing {key!r}: " + "; ".join(changed))
    return "\n".join(lines)


def self_checks(user_rows, campaign_rows):
    """rules.md's "quick self-check", on one output alone."""
    user_columns = list(TABLES["user_daily"]["columns"])
    campaign_columns = list(TABLES["campaign_daily"]["columns"])

    def total(rows, columns, name, where=lambda row: True):
        index = columns.index(name)
        return sum(row[index] or 0 for row in rows if where(row))

    notes = []
    for name in ("sessions", "revenue_cents"):
        u = total(user_rows, user_columns, name)
        c = total(campaign_rows, campaign_columns, name)
        notes.append(f"summed {name}: user_daily {u}, campaign_daily {c}"
                     + ("" if u == c else "  <-- NOT EQUAL"))
    organic = campaign_columns.index("campaign_id")
    assisted = total(campaign_rows, campaign_columns, "assisted_sessions")
    credited = total(campaign_rows, campaign_columns, "sessions",
                     where=lambda row: row[organic] != "organic")
    notes.append(f"summed assisted_sessions {assisted} >= credited non-organic sessions {credited}"
                 + ("" if assisted >= credited else "  <-- FAILS"))
    return notes


def compare_folders(actual_dir, expected_dir, show):
    """Print the comparison of two finished outputs; return True on a match."""
    try:
        expected = {table: load_output(expected_dir, table) for table in TABLES}
    except (OutputError, OSError, ValueError) as error:
        raise SystemExit(f"harness: cannot read the expected output: {error}")
    matched = True
    actual = {}
    for table in TABLES:
        try:
            actual[table] = load_output(actual_dir, table)
        except (OutputError, OSError, ValueError, pa.ArrowException) as error:
            print(f"{table}: UNREADABLE  {error}")
            matched = False
            continue
        result = compare_table(actual[table], expected[table], table)
        print(describe(result, show))
        matched = matched and result["match"]
    if len(actual) == len(TABLES):
        for note in self_checks(actual["user_daily"], actual["campaign_daily"]):
            print(f"  self-check: {note}")
    return matched


# ---------------------------------------------------------------------------
# Running
# ---------------------------------------------------------------------------

def run_implementation(command, data_dir, output_dir, quiet=False):
    """Run `command` on `data_dir`, writing `output_dir`. Returns (exit code,
    seconds, the tail of stderr)."""
    scratch = tempfile.mkdtemp(prefix="shopping-scratch-")
    env = dict(os.environ, DATA_DIR=os.path.abspath(data_dir),
               OUTPUT_DIR=os.path.abspath(output_dir), SCRATCH_DIR=scratch,
               NCPU=os.environ.get("NCPU", "8"), MEM_GB=os.environ.get("MEM_GB", "24"))
    os.makedirs(output_dir, exist_ok=True)
    started = time.monotonic()
    try:
        process = subprocess.run(command, env=env, stdout=subprocess.DEVNULL if quiet else None,
                                 stderr=subprocess.PIPE, text=True, errors="replace")
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    return process.returncode, time.monotonic() - started, process.stderr[-3000:]


def reference_output(data_dir, cache):
    """A folder holding the reference's output for `data_dir`: from the cache
    when it is there, otherwise made now."""
    if cache and all(os.path.isdir(os.path.join(cache, table)) for table in TABLES):
        print(f"reference: using cached output {cache}")
        return cache
    target = cache or tempfile.mkdtemp(prefix="shopping-reference-")
    if cache and os.path.exists(cache):
        shutil.rmtree(cache)
    print(f"reference: running on {data_dir} ...", flush=True)
    code, seconds, stderr = run_implementation([sys.executable, REFERENCE], data_dir, target,
                                               quiet=True)
    if code != 0:
        shutil.rmtree(target, ignore_errors=True)
        raise SystemExit(f"harness: the reference failed (exit {code}):\n{stderr}")
    print(f"reference: done in {seconds:.1f} s")
    return target


def split_command(argv):
    if "--" not in argv:
        raise SystemExit("usage: put the implementation's command after --")
    at = argv.index("--")
    command = argv[at + 1:]
    if not command:
        raise SystemExit("usage: no command after --")
    return argv[:at], command


def option(arguments, name, default=None):
    if name in arguments:
        at = arguments.index(name)
        value = arguments[at + 1]
        del arguments[at:at + 2]
        return value
    return default


def command_data(argv):
    arguments, command = split_command(argv)
    expected = option(arguments, "--expected")
    cache = option(arguments, "--reference-cache")
    keep = option(arguments, "--keep-output")
    show = int(option(arguments, "--show", "5"))
    if len(arguments) != 1:
        raise SystemExit("usage: check.py data DATA_DIR [options] -- CMD ...")
    data_dir = arguments[0]
    expected_dir = expected or reference_output(data_dir, cache)
    output_dir = keep or tempfile.mkdtemp(prefix="shopping-output-")
    if keep and os.path.exists(keep):
        shutil.rmtree(keep)
    print(f"implementation: running {' '.join(command)} ...", flush=True)
    code, seconds, stderr = run_implementation(command, data_dir, output_dir)
    print(f"implementation: exit {code} after {seconds:.2f} s")
    if code != 0:
        print(stderr)
        return 2
    print(f"data set: {data_dir}\nexpected: {expected_dir}")
    matched = compare_folders(output_dir, expected_dir, show)
    if not keep:
        shutil.rmtree(output_dir, ignore_errors=True)
    print("RESULT: MATCH" if matched else "RESULT: DIFFER")
    return 0 if matched else 1


def fixture_cases(roots):
    cases = []
    for root in roots:
        for name in sorted(os.listdir(root)):
            case = os.path.join(root, name)
            if os.path.isdir(os.path.join(case, "input")) and \
                    os.path.isdir(os.path.join(case, "expected")):
                cases.append(case)
    return cases


def command_fixtures(argv):
    arguments, command = split_command(argv)
    show = int(option(arguments, "--show", "5"))
    cases = fixture_cases(arguments)
    if not cases:
        raise SystemExit("harness: no fixture cases (folders with input/ and expected/)")
    failed = []
    for case in cases:
        name = os.path.relpath(case)
        note_path = os.path.join(case, "note.txt")
        note = open(note_path, encoding="utf-8").read().strip() if os.path.isfile(note_path) else ""
        output_dir = tempfile.mkdtemp(prefix="shopping-fixture-")
        code, _seconds, stderr = run_implementation(command, os.path.join(case, "input"),
                                                    output_dir, quiet=True)
        if code != 0:
            failed.append(case)
            print(f"FAIL {name}: exit {code}\n{stderr}")
        else:
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                matched = compare_folders(output_dir, os.path.join(case, "expected"), show)
            if matched:
                print(f"ok   {name}" + (f"  ({note})" if note else ""))
            else:
                failed.append(case)
                print(f"FAIL {name}" + (f"  ({note})" if note else ""))
                print("\n".join("    " + line for line in buffer.getvalue().splitlines()))
        shutil.rmtree(output_dir, ignore_errors=True)
    print(f"\n{len(cases) - len(failed)}/{len(cases)} fixtures passed")
    return 1 if failed else 0


def command_compare(argv):
    arguments = list(argv)
    show = int(option(arguments, "--show", "5"))
    if len(arguments) != 2:
        raise SystemExit("usage: check.py compare ACTUAL_DIR EXPECTED_DIR [--show N]")
    matched = compare_folders(arguments[0], arguments[1], show)
    print("RESULT: MATCH" if matched else "RESULT: DIFFER")
    return 0 if matched else 1


def main(argv):
    commands = {"data": command_data, "fixtures": command_fixtures, "compare": command_compare}
    if len(argv) < 2 or argv[1] not in commands:
        print(__doc__)
        return 2
    return commands[argv[1]](argv[2:])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
