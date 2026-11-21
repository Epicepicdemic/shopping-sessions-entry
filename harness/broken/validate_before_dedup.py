#!/usr/bin/env python3
"""BROKEN ON PURPOSE. Breaks Events step 2, "Dedup comes before validation":
copies whose ts is bad are thrown away before dedup, so a later valid copy
rescues an event whose earliest copy was invalid."""
import sys

from _load import reference as ref


def dedup_valid_copies_only(data_dir, counts):
    kept = {}
    for path in ref.csv_files(f"{data_dir}/events"):
        for line in ref.data_lines(path):
            values = ref.parse_line(line, ref.EVENT_COLUMNS)
            if values is None or values[0] == b"":
                continue
            ts = ref.parse_ts(values[2])
            if ts is None:
                continue  # the mistake: validating before dedup
            ingest = ref.parse_ingest_ts(values[3])
            if values[0] not in kept or ingest < ref.parse_ingest_ts(
                    ref.parse_line(kept[values[0]], ref.EVENT_COLUMNS)[3]):
                kept[values[0]] = line
    return kept


ref.dedup_events = dedup_valid_copies_only
sys.exit(ref.main(sys.argv))
