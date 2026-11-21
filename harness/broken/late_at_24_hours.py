#!/usr/bin/env python3
"""BROKEN ON PURPOSE. Breaks Events step 4, "Exactly 24 hours is kept":
an event ingested exactly 24 hours after its ts is dropped as late."""
import sys

from _load import reference as ref

ref.is_late = lambda ingest, ts: ingest - ts >= ref.LATE_LIMIT_MS
sys.exit(ref.main(sys.argv))
