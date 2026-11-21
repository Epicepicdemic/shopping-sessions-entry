#!/usr/bin/env python3
"""BROKEN ON PURPOSE. Breaks Events step 5, "A new session starts when the gap
from the previous event is over 30 minutes": a gap of exactly 30:00.000 also
splits. With integer milliseconds, "> 1,799,999" is ">= 1,800,000"."""
import sys

from _load import reference as ref

ref.SESSION_GAP_MS = 1_799_999
sys.exit(ref.main(sys.argv))
