# The reference implementation and its harness

`reference/reference.py` is a slow implementation of `rules.md` that is meant to be obviously correct. It is written in plain Python with one function per rule, and the rule is quoted above the code that implements it. Every fast pipeline is checked against it, and `harness/` does the checking.

Speed does not matter here. The dev set (1M events) takes about 14 s, and a 20M set takes about 5 minutes and 5.7 GB of memory. It reads and compares everything as bytes, never as decoded text, so "byte-wise smallest" means exactly that. Its only third-party library is pyarrow, which it uses to write Parquet. The harness also uses pyarrow, to read Parquet.

## Setup

```sh
python3 -m venv .venv && .venv/bin/pip install pyarrow   # Python 3.10 or newer
```

## Running the reference

```sh
.venv/bin/python reference/reference.py DATA_DIR OUTPUT_DIR
# or with no arguments, reading $DATA_DIR (default /data) and writing $OUTPUT_DIR (default /output)
```

It writes `OUTPUT_DIR/user_daily/part-0.parquet` and `OUTPUT_DIR/campaign_daily/part-0.parquet`, and logs counts for each stage to stderr. It exits with code 3 if the input breaks one of the guarantees in rules.md (see the ambiguities below), and it does not guess in that case.

## Running the harness

An implementation is any command line. It reads `$DATA_DIR` and writes `$OUTPUT_DIR`, and it also gets `SCRATCH_DIR`, `NCPU` and `MEM_GB`.

```sh
# One data set. The reference is run on it, or its output is reused from the cache folder.
.venv/bin/python harness/check.py data DATA_DIR --reference-cache REF_OUT_DIR -- ./my-pipeline --flag
# Against a published answer (a folder holding user_daily.csv and campaign_daily.csv, like dev-answer.zip):
.venv/bin/python harness/check.py data DATA_DIR --expected dev-answer -- ./my-pipeline
# Every fixture (the kit's 36 and our 19):
.venv/bin/python harness/check.py fixtures harness/fixtures/kit harness/fixtures/adversarial -- ./my-pipeline
# Two finished outputs, with nothing run:
.venv/bin/python harness/check.py compare MY_OUT EXPECTED_OUT
# Every generated set in a folder, each against the reference (cached per set):
PYTHON=.venv/bin/python harness/check_sets.sh SETS_DIR REF_CACHE_DIR -- ./my-pipeline
```

For each table, the harness reports MATCH or DIFFER, the counts of missing, extra and differing rows and of duplicate keys, and the first few of each (`--show N`). For a differing row it names the changed columns. It also runs the self-checks from rules.md and exits with 0 on a match, 1 on a difference and 2 on a crash. It reads output the way rules.md describes. It reads every `*.parquet` directly in the folder and skips symlinks, other files and names starting with `.` or `_`. It requires exactly the right columns and no extras. It accepts any integer width and any string encoding (plain, large, view or dictionary). `day` must carry the Parquet `DATE` logical type. The tables are compared as sets of rows. Rows are matched on their key only so that the report can say which rows differ.

To run inside Docker the way the scorer does, give the starter's `scripts/docker_run.sh` as the command. It takes the data and output folders as arguments, so wrap it: `sh -c 'scripts/docker_run.sh "$DATA_DIR" "$OUTPUT_DIR"'`.

### Other tools

- `harness/broken/*.py` are four deliberately broken copies of the reference, each breaking one rule. The harness must catch every one of them:
  - `validate_before_dedup`: Events step 2.
  - `gap_at_least_30_min`: step 5.
  - `late_at_24_hours`: step 4.
  - `click_tie_largest_id`: Clicks step 4.
- `harness/mutants.py survey harness/fixtures/kit harness/fixtures/adversarial` runs 24 one-rule mutants of the reference on the fixtures and lists which fixtures kill each one. The kit's 36 fixtures kill 19 of the 24, and adding ours kills all 24.
- `harness/test_check.py` tests the harness itself with 18 output shapes. Six are accepted: other integer widths, dictionary strings, a different column order, several files, and ignored `_SUCCESS` and dot-files. Twelve are rejected: an extra or missing column, `day` as a string or timestamp, binary strings, duplicate rows, `""` in place of null, and others.
- `harness/scramble.py DATA_DIR OUT_DIR --seed N` copies a data set with a new file layout, line order and quoting. rules.md says none of these may change the answer.
- `harness/fixtures/build_adversarial.py` writes `harness/fixtures/adversarial/`. Each case's input and its hand-written expected rows are in that file, with the reasoning for each. The table in `harness/fixtures/adversarial/README.md` lists each case and the rules.md sentence that decides it.

## Ambiguities in rules.md

The reference has to do something in each of these cases, and it does what is described here. None of them occurs in generated data. Those marked **stops** make the reference exit with code 3 instead of guessing. They are questions for the organisers if any of them ever matters.

1. **The header row.** The first line of every CSV file is skipped as the header, and the reference never checks its text. A data line equal to the header is data, as in kit fixture L3. A zero-byte file, which has no header, reads as empty.
2. **A last line without `\n`.** It is treated as a line. The rules say "`\n` line endings", but they do not say whether the final line must have one.
3. **Which files are read.** The reference reads every regular file directly in the folder whose name ends in `.csv`, including dot-files such as `.x.csv` and symlinks to files. It skips subfolders, even one named `x.csv`, and names in other cases such as `.CSV`. The rules say only "every `*.csv` file".
4. **`ingest_ts` that is not valid F1 (stops).** The rules promise that "`ingest_ts` is always valid F1", but they do not say what to do when it is not. One example is a repeated header line in an events file, whose `ingest_ts` is the text `ingest_ts`. The open question is whether the promise covers every line or only well-formed lines with an `event_id`. For that reason, no fixture puts a header line into an events file.
5. **Copies with equal `ingest_ts` that differ (stops).** The rules say "Copies with equal `ingest_ts` are identical". If two such copies differ, the rules give no tie-break.
6. **Click copies that differ (stops).** The rules say "Copies of a click have identical values, so keep any one."
7. **Duplicate keys in `products.csv` or `campaigns.csv`, the category `unknown`, or the campaign `organic` (stops).** The rules say these files are clean and unique and that the two names are never used. The reference parses both files with the same CSV dialect, so it strips quotes from them.
8. **Years in F1 and F2.** The year must be exactly four ASCII digits. Year `0000` is accepted as the proleptic Gregorian year 0, which is a leap year. Five-digit years cannot be written in either form. This only matters if a window is near year 0 or 9999.
9. **The manifest.** Its two values are parsed with the same `ts` parser (F1, F2 or F3). The rules show only F1.
10. **Bytes that are not UTF-8.** Values are compared as bytes, so comparison works on any bytes. A non-UTF-8 value that reaches an output column cannot be written as a Parquet string, and the reference raises an error. The rules say the input is UTF-8.
11. **An empty category in `products.csv`.** The reference would output `top_category` as an empty string, not as null. Its CSV fixture format cannot tell the two apart. The rules say the file is clean.
12. **Output integer types.** The harness accepts unsigned integers, as the kit's checker does ("Any integer width"). It rejects strings stored as Parquet binary without the UTF-8 annotation, which the kit's checker would read as Binary and also reject.
13. **No sessions at all.** The reference writes one zero-row Parquet file per table, with the full schema. The rules say "one or more `*.parquet` files". They do not say whether a table with no rows may be zero files.
