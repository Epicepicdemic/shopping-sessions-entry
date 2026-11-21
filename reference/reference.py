#!/usr/bin/env python3
"""
Shopping Sessions: the reference implementation.

Slow on purpose. Written to be read next to rules.md: one function per rule,
with the rule quoted above the code that implements it. Every fast pipeline is
compared against the output of this file, so when this file and rules.md
disagree, this file is wrong.

Usage:
    python3 reference.py [DATA_DIR OUTPUT_DIR]
With no arguments it reads $DATA_DIR (default /data) and writes $OUTPUT_DIR
(default /output). It writes OUTPUT_DIR/user_daily/part-0.parquet and
OUTPUT_DIR/campaign_daily/part-0.parquet. Progress goes to stderr.

Everything is handled as bytes, never as decoded text, so that "byte-wise
smallest" and "compare exactly" mean exactly that. The only third-party
library is pyarrow, used for nothing but writing the two Parquet files.
"""
import datetime
import json
from bisect import bisect_left, bisect_right
import os
import sys
from collections import defaultdict

MS_PER_SECOND = 1_000
MS_PER_MINUTE = 60 * MS_PER_SECOND
MS_PER_DAY = 86_400_000

# rules.md, Events step 5: "over 30 minutes (1,800,000 ms)".
SESSION_GAP_MS = 1_800_000
# rules.md, Events step 4: "drop the event if `ingest_ts - ts` is over 24 hours".
LATE_LIMIT_MS = 24 * 60 * MS_PER_MINUTE
# rules.md, Clicks step 4: "`session_start - click.ts <= 7 days` (604,800,000 ms)".
CLICK_LOOKBACK_MS = 604_800_000

EVENT_COLUMNS = 7   # event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents
CLICK_COLUMNS = 4   # click_id,user_id,campaign_id,ts
LINK_COLUMNS = 2    # user_id_a,user_id_b
LOOKUP_COLUMNS = 2  # product_id,category  /  campaign_id,channel

ORGANIC = b"organic"
UNKNOWN_CATEGORY = b"unknown"
EVENT_TYPES = (b"view", b"cart", b"purchase")


class InputBreaksAGuarantee(Exception):
    """The input breaks something rules.md promises (for example "`ingest_ts`
    is always valid F1"). The rules do not say what to do then, so the
    reference stops instead of guessing."""


def log(*parts):
    print(*parts, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Input files
# ---------------------------------------------------------------------------

# rules.md, Input files: "Read every `*.csv` file in `events/`, `clicks/` and
# `links/`." and "Every CSV file has a header row."
def csv_files(folder):
    """Every regular file directly in `folder` whose name ends in `.csv`,
    in name order (the order only affects which of two identical copies is
    kept, never the output)."""
    if not os.path.isdir(folder):
        return []
    names = sorted(os.listdir(folder))
    return [os.path.join(folder, name) for name in names
            if name.endswith(".csv") and os.path.isfile(os.path.join(folder, name))]


# rules.md, CSV dialect: "Comma-separated, UTF-8, `\n` line endings. Only `\n`
# ends a line: a `\r` is an ordinary character of the field it is in."
def data_lines(path):
    """Yield each line after the header, without its `\\n`. A binary-mode
    Python file splits on b"\\n" and nothing else, so a `\\r` stays in the
    line. A last line with no `\\n` after it is still a line."""
    with open(path, "rb") as handle:
        first = True
        for raw in handle:
            if first:
                first = False  # the header row
                continue
            yield raw[:-1] if raw.endswith(b"\n") else raw


# rules.md, CSV dialect: "Any field may be wrapped in double quotes, and the
# quotes are syntax, not part of the value: `"u1"` is `u1`, and `""` is an
# empty field. No value ever holds a comma, a quote or a newline, quoted or
# not: every comma separates fields. An empty field means null. A line with
# the wrong number of fields, or with a quote anywhere except around a whole
# field, is malformed and is dropped. An empty line is malformed too. Spaces
# are part of a value, quoted or not."
def parse_line(line, column_count):
    """The line's field values, or None if the line is malformed. An empty
    value (b"") stands for null."""
    if line == b"":
        return None
    fields = line.split(b",")
    if len(fields) != column_count:
        return None
    values = []
    for field in fields:
        if b'"' not in field:
            values.append(field)
        elif len(field) >= 2 and field.startswith(b'"') and field.endswith(b'"') \
                and b'"' not in field[1:-1]:
            values.append(field[1:-1])
        else:
            return None
    return values


def read_table(path, column_count):
    """Every well-formed data line of one CSV file, as lists of values."""
    for line in data_lines(path):
        values = parse_line(line, column_count)
        if values is not None:
            yield values


# rules.md, CSV dialect: "Wherever these rules say digits, they mean the ASCII
# characters `0` to `9`."
def is_digits(value):
    return len(value) > 0 and all(0x30 <= byte <= 0x39 for byte in value)


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------

def is_leap_year(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def days_in_month(year, month):
    if month == 2:
        return 29 if is_leap_year(year) else 28
    return 30 if month in (4, 6, 9, 11) else 31


def days_from_civil(year, month, day):
    """Days from 1970-01-01 to the given proleptic Gregorian date (Howard
    Hinnant's algorithm), using only integer arithmetic so that no library's
    year range limits it."""
    year -= month <= 2
    era = year // 400
    year_of_era = year - era * 400
    day_of_year = (153 * ((month + 9) % 12) + 2) // 5 + day - 1
    day_of_era = year_of_era * 365 + year_of_era // 4 - year_of_era // 100 + day_of_year
    return era * 146_097 + day_of_era - 719_468


def parse_calendar_part(text):
    """`YYYY-MM-DDTHH:MM:SS` (exactly 19 bytes, ASCII digits, uppercase `T`)
    as milliseconds since the epoch, or None if it is not that shape or is an
    impossible calendar value."""
    if len(text) != 19:
        return None
    if text[4:5] != b"-" or text[7:8] != b"-" or text[10:11] != b"T" \
            or text[13:14] != b":" or text[16:17] != b":":
        return None
    parts = (text[0:4], text[5:7], text[8:10], text[11:13], text[14:16], text[17:19])
    if not all(is_digits(part) for part in parts):
        return None
    year, month, day, hour, minute, second = (int(part) for part in parts)
    # "an impossible calendar value (month 13, February 30, February 29 outside
    # a leap year, hour 24, second 60)"
    if not 1 <= month <= 12:
        return None
    if not 1 <= day <= days_in_month(year, month):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        return None
    days = days_from_civil(year, month, day)
    return ((days * 24 + hour) * 60 + minute) * MS_PER_MINUTE + second * MS_PER_SECOND


# rules.md, Timestamps, F1: "`2026-10-03T23:58:10Z`: UTC, whole seconds".
def parse_f1(text):
    if len(text) != 20 or not text.endswith(b"Z"):
        return None
    return parse_calendar_part(text[:19])


# rules.md, Timestamps, F2: "`2026-10-03T19:58:10-04:00`: Offset `+HH:MM` or
# `-HH:MM` (hours 00 to 23, minutes 00 to 59); subtract it to get UTC."
# and "`-00:00` is UTC", "an offset outside its range (`+24:00`, `+05:60`) or
# without its colon (`+0200`)" is bad.
def parse_f2(text):
    if len(text) != 25:
        return None
    local = parse_calendar_part(text[:19])
    if local is None:
        return None
    sign, hours, colon, minutes = text[19:20], text[20:22], text[22:23], text[23:25]
    if sign not in (b"+", b"-") or colon != b":" or not is_digits(hours) or not is_digits(minutes):
        return None
    if not (0 <= int(hours) <= 23 and 0 <= int(minutes) <= 59):
        return None
    offset_ms = (int(hours) * 60 + int(minutes)) * MS_PER_MINUTE
    return local - offset_ms if sign == b"+" else local + offset_ms


# rules.md, Timestamps, F3: "`1791071890000`: Digits only: epoch milliseconds.
# Leading zeros are allowed" and "An F3 value is just a number, so one that is
# very short (`86400000` is 1970-01-02) or too long for a 64-bit integer is not
# malformed, only outside the window." Python integers have no size limit, so
# a 21-digit value is simply a large number that fails the window check.
def parse_f3(text):
    return int(text) if is_digits(text) else None


# rules.md, Timestamps: "`ts` in events and clicks takes three forms. Convert
# each to integer milliseconds since the Unix epoch, UTC. ... Anything else is
# a bad timestamp". Returns None for a bad timestamp.
def parse_ts(text):
    for parse in (parse_f1, parse_f2, parse_f3):
        value = parse(text)
        if value is not None:
            return value
    return None


# rules.md, Timestamps: "`ingest_ts` is always valid F1."
def parse_ingest_ts(text):
    value = parse_f1(text)
    if value is None:
        raise InputBreaksAGuarantee(f"ingest_ts {text!r} is not valid F1")
    return value


def utc_date(milliseconds):
    return datetime.date(1970, 1, 1) + datetime.timedelta(days=milliseconds // MS_PER_DAY)


# ---------------------------------------------------------------------------
# Manifest and lookups
# ---------------------------------------------------------------------------

def read_window(data_dir):
    """(window_start, window_end) in milliseconds, from manifest.json."""
    with open(os.path.join(data_dir, "manifest.json"), "rb") as handle:
        manifest = json.load(handle)
    bounds = []
    for key in ("window_start", "window_end"):
        value = parse_ts(manifest[key].encode("utf-8"))
        if value is None:
            raise InputBreaksAGuarantee(f"manifest {key} {manifest[key]!r} is not a timestamp")
        bounds.append(value)
    return bounds[0], bounds[1]


# rules.md, Input files: "`products.csv`: `product_id,category`, clean,
# `product_id` unique" and "`campaigns.csv`: `campaign_id,channel`, clean,
# `campaign_id` unique".
def read_lookup(path):
    table = {}
    for key, value in read_table(path, LOOKUP_COLUMNS):
        if key in table:
            raise InputBreaksAGuarantee(f"{path}: {key!r} appears twice")
        table[key] = value
    return table


def read_products(data_dir):
    products = read_lookup(os.path.join(data_dir, "products.csv"))
    # rules.md, Events step 7: "`unknown`, a name no category in the file uses."
    if UNKNOWN_CATEGORY in products.values():
        raise InputBreaksAGuarantee("products.csv uses the category name 'unknown'")
    return products


def read_campaigns(data_dir):
    campaigns = read_lookup(os.path.join(data_dir, "campaigns.csv"))
    # rules.md, Clicks step 5: "That ID never appears in `campaigns.csv`."
    if ORGANIC in campaigns:
        raise InputBreaksAGuarantee("campaigns.csv contains the campaign 'organic'")
    return campaigns


# ---------------------------------------------------------------------------
# Linked IDs
# ---------------------------------------------------------------------------

# rules.md, Linked IDs step 1: "Parse. Drop malformed lines (the CSV dialect
# above) and lines where either field is empty."
def read_link_pairs(data_dir):
    pairs = []
    for path in csv_files(os.path.join(data_dir, "links")):
        for id_a, id_b in read_table(path, LINK_COLUMNS):
            if id_a != b"" and id_b != b"":
                pairs.append((id_a, id_b))
    return pairs


# rules.md, Linked IDs step 2: "Join. Every remaining line joins its two IDs
# into one person, and joining is transitive ... however many links apart and
# in whichever files and order the lines come. A line may repeat another,
# either way round, and may link an ID to itself; neither changes anything."
# Step 3: "Name each person by the byte-wise smallest of their IDs. An ID that
# appears in no remaining line is a person on its own, named by itself. An ID
# can be linked without appearing in any event or click, and can still be the
# smallest, and so the name."
def person_names(pairs):
    """Map from every linked ID to its person's name. The people are the
    connected components of the graph whose edges are the link lines, found
    by a plain breadth-first search."""
    neighbours = defaultdict(set)
    for id_a, id_b in pairs:
        neighbours[id_a].add(id_b)
        neighbours[id_b].add(id_a)
    name_of = {}
    for first in neighbours:
        if first in name_of:
            continue
        component = [first]
        seen = {first}
        at = 0
        while at < len(component):
            for other in neighbours[component[at]]:
                if other not in seen:
                    seen.add(other)
                    component.append(other)
            at += 1
        name = min(component)  # bytes compare byte by byte
        for member in component:
            name_of[member] = name
    return name_of


# rules.md, Linked IDs: "an event or click is the person's as soon as it is
# parsed, whatever its own ID. Links have no time".
def person_of(user_id, name_of):
    return name_of.get(user_id, user_id)


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

# rules.md, Events step 1: "Parse. Drop malformed lines and lines whose
# `event_id` is empty."
# rules.md, Events step 2: "Dedup on `event_id`. Keep only the copy with the
# smallest `ingest_ts`. Copies with equal `ingest_ts` are identical. Dedup
# comes before validation: if the kept copy is invalid the event is dropped,
# and a later valid copy does not rescue it. A malformed copy was already
# dropped in step 1, so it is never the kept copy."
def dedup_events(data_dir, counts):
    """Map from event_id to the kept copy's raw line. Only the raw line is
    stored (and parsed again later) to keep memory down on large sets."""
    kept = {}
    for path in csv_files(os.path.join(data_dir, "events")):
        for line in data_lines(path):
            counts["event lines"] += 1
            values = parse_line(line, EVENT_COLUMNS)
            if values is None:
                counts["event lines malformed"] += 1
                continue
            event_id = values[0]
            if event_id == b"":
                counts["event lines with empty event_id"] += 1
                continue
            ingest = parse_ingest_ts(values[3])
            if event_id not in kept:
                kept[event_id] = line
                continue
            counts["event copies beyond the first"] += 1
            kept_values = parse_line(kept[event_id], EVENT_COLUMNS)
            kept_ingest = parse_ingest_ts(kept_values[3])
            if ingest < kept_ingest:
                kept[event_id] = line
            elif ingest == kept_ingest and values != kept_values:
                raise InputBreaksAGuarantee(
                    f"event {event_id!r}: two copies with equal ingest_ts differ")
    return kept


# rules.md, Events step 3: "A `purchase` also needs `price_cents` to be digits
# only with a value from 1 to 10,000,000 (`0250` is 250)."
def purchase_price(price_cents):
    if not is_digits(price_cents):
        return None
    value = int(price_cents)
    return value if 1 <= value <= 10_000_000 else None


# rules.md, Timestamps: "An event's `ts` must satisfy `window_start <= ts <
# window_end`."
def in_event_window(ts, window):
    window_start, window_end = window
    return window_start <= ts < window_end


# rules.md, Events step 3: "Validate the kept copy. Drop the event unless
# `user_id` is non-empty, `ts` is valid and in the event window, `event_type`
# is exactly `view`, `cart` or `purchase` (case-sensitive), and `product_id` is
# non-empty. ... For `view` and `cart`, `price_cents` is ignored whatever it
# holds."
def validate_event(values, window):
    """(user_id, ts, event_type, product_id, price) for a valid event, where
    price is None unless it is a purchase; None for an invalid one."""
    _event_id, user_id, ts_text, _ingest, event_type, product_id, price_cents = values
    if user_id == b"":
        return None
    ts = parse_ts(ts_text)
    if ts is None or not in_event_window(ts, window):
        return None
    if event_type not in EVENT_TYPES:
        return None
    if product_id == b"":
        return None
    price = None
    if event_type == b"purchase":
        price = purchase_price(price_cents)
        if price is None:
            return None
    return user_id, ts, event_type, product_id, price


# rules.md, Events step 4: "Drop late events: drop the event if `ingest_ts -
# ts` is over 24 hours. Exactly 24 hours is kept, and so is an `ingest_ts`
# before `ts`."
def is_late(ingest, ts):
    return ingest - ts > LATE_LIMIT_MS


# rules.md, Events step 7: "Categories come from `products.csv`. A product not
# in the file has the category `unknown`".
def category_of(product_id, products):
    return products.get(product_id, UNKNOWN_CATEGORY)


def surviving_events(kept, window, products, name_of, counts):
    """Map from person to a list of (ts, purchase_price, category) for every
    event that survives steps 3 and 4. purchase_price is None for a view or
    a cart. The kept copies are consumed as they are used."""
    by_person = defaultdict(list)
    while kept:
        _event_id, line = kept.popitem()
        values = parse_line(line, EVENT_COLUMNS)
        valid = validate_event(values, window)
        if valid is None:
            counts["events invalid"] += 1
            continue
        user_id, ts, _event_type, product_id, price = valid
        if is_late(parse_ingest_ts(values[3]), ts):
            counts["events late"] += 1
            continue
        counts["events surviving"] += 1
        by_person[person_of(user_id, name_of)].append(
            (ts, price, category_of(product_id, products)))
    return by_person


class Session:
    def __init__(self, person, start):
        self.person = person
        self.start = start
        self.events = 0
        self.purchases = []  # (category, price) per purchase

    @property
    def day(self):
        # rules.md, Events step 6: "One day per session: the UTC date of its
        # start. All its events and purchases count on that day, even those
        # after midnight."
        return utc_date(self.start)


# rules.md, Events step 5: "Sessionize each person's surviving events, all of
# their IDs together, sorted by `ts`. A new session starts when the gap from
# the previous event is over 30 minutes (1,800,000 ms). Equal timestamps share
# a session: the gap is measured between distinct timestamps ... A session
# starts at its first event's `ts`."
# After sorting by ts, the previous event's ts is the previous distinct ts or
# an equal one (gap 0), so comparing with the previous event is the same as
# comparing with the previous distinct timestamp.
def sessionize(person, events):
    sessions = []
    previous_ts = None
    for ts, price, category in sorted(events, key=lambda event: event[0]):
        if previous_ts is None or ts - previous_ts > SESSION_GAP_MS:
            sessions.append(Session(person, ts))
        session = sessions[-1]
        session.events += 1
        if price is not None:
            session.purchases.append((category, price))
        previous_ts = ts
    return sessions


# ---------------------------------------------------------------------------
# Clicks
# ---------------------------------------------------------------------------

# rules.md, Clicks step 1: "Parse. Drop malformed lines and lines whose
# `click_id` is empty."
# rules.md, Clicks step 2: "Dedup on `click_id`. Copies of a click have
# identical values, so keep any one."
def dedup_clicks(data_dir, counts):
    kept = {}
    for path in csv_files(os.path.join(data_dir, "clicks")):
        for values in read_table_counted(path, CLICK_COLUMNS, counts, "click"):
            click_id = values[0]
            if click_id == b"":
                counts["click lines with empty click_id"] += 1
                continue
            if click_id in kept:
                counts["click copies beyond the first"] += 1
                if kept[click_id] != values:
                    raise InputBreaksAGuarantee(f"click {click_id!r}: copies differ")
                continue
            kept[click_id] = values
    return kept


def read_table_counted(path, column_count, counts, kind):
    for line in data_lines(path):
        counts[f"{kind} lines"] += 1
        values = parse_line(line, column_count)
        if values is None:
            counts[f"{kind} lines malformed"] += 1
            continue
        yield values


# rules.md, Timestamps: "A click's `ts` must satisfy `window_start - 7 days <=
# ts < window_end` ... A click at exactly `window_start - 7 days` is in its
# window; one at `window_end` is not."
def in_click_window(ts, window):
    window_start, window_end = window
    return window_start - CLICK_LOOKBACK_MS <= ts < window_end


# rules.md, Clicks step 3: "Validate. Drop the click unless `user_id` is
# non-empty, `ts` is valid and in the click window, and `campaign_id` is in
# `campaigns.csv`."
def validate_click(values, window, campaigns):
    click_id, user_id, campaign_id, ts_text = values
    if user_id == b"":
        return None
    ts = parse_ts(ts_text)
    if ts is None or not in_click_window(ts, window):
        return None
    if campaign_id not in campaigns:
        return None
    return click_id, user_id, campaign_id, ts


def valid_clicks_by_person(kept, window, campaigns, name_of, counts):
    """Map from person to their valid clicks as (ts, click_id, campaign_id),
    sorted by ts and then click_id."""
    by_person = defaultdict(list)
    for values in kept.values():
        valid = validate_click(values, window, campaigns)
        if valid is None:
            counts["clicks invalid"] += 1
            continue
        counts["clicks valid"] += 1
        click_id, user_id, campaign_id, ts = valid
        by_person[person_of(user_id, name_of)].append((ts, click_id, campaign_id))
    for clicks in by_person.values():
        clicks.sort()
    return by_person


# The clicks steps 4 and 7 both choose from: "the person's valid clicks, from
# any of their IDs, with `click.ts <= session_start` and `session_start -
# click.ts <= 7 days`" (step 4) and "with `session_start - 7 days <= click.ts
# <= session_start`" (step 7), which is the same set. `clicks` is sorted by
# ts, so the set is one contiguous run; it is found by binary search and the
# run is returned whole.
def qualifying_clicks(clicks, session_start):
    low = bisect_left(clicks, session_start - CLICK_LOOKBACK_MS, key=lambda click: click[0])
    high = bisect_right(clicks, session_start, key=lambda click: click[0])
    return clicks[low:high]


# rules.md, Clicks step 4: "take the one with the latest `ts`; on a tie in
# `ts`, the smallest `click_id`, compared byte by byte."
# rules.md, Clicks step 5: "No qualifying click credits the session to the
# campaign `organic` with the channel `organic`."
def credited_campaign(qualifying):
    if not qualifying:
        return ORGANIC
    latest = max(ts for ts, _click_id, _campaign_id in qualifying)
    # click_id is unique after dedup, so the smallest pair is the smallest click_id.
    _click_id, campaign_id = min(
        (click_id, campaign_id) for ts, click_id, campaign_id in qualifying if ts == latest)
    return campaign_id


# rules.md, Clicks step 7: "Each such campaign assists the session once,
# however many of its clicks qualify. `organic` never assists."
def assisting_campaigns(qualifying):
    return {campaign_id for _ts, _click_id, campaign_id in qualifying}


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

# rules.md, Outputs, user_daily: "one row per person and day with at least
# one session", and its columns. top_category: "The category with the highest
# summed purchase `price_cents` in those sessions; a tie goes to the byte-wise
# smallest; null with no purchases. `unknown` competes like any category."
def top_category(category_totals):
    if not category_totals:
        return None
    best = max(category_totals.values())
    return min(category for category, total in category_totals.items() if total == best)


def user_daily_rows(sessions):
    groups = {}
    for session in sessions:
        key = (session.person, session.day)
        if key not in groups:
            groups[key] = {"sessions": 0, "events": 0, "revenue_cents": 0,
                           "categories": defaultdict(int)}
        group = groups[key]
        group["sessions"] += 1
        group["events"] += session.events
        for category, price in session.purchases:
            group["revenue_cents"] += price
            group["categories"][category] += price
    rows = []
    for (person, day), group in groups.items():
        rows.append({"user_id": person, "day": day, "sessions": group["sessions"],
                     "events": group["events"], "revenue_cents": group["revenue_cents"],
                     "top_category": top_category(group["categories"])})
    return rows


# rules.md, Outputs, campaign_daily: "one row per `(campaign_id, day)` with at
# least one session credited to or assisted by the campaign. A campaign that
# only assisted that day has a row with 0 in the three credited columns."
def campaign_daily_rows(sessions, credit, assists, campaigns):
    groups = {}

    def group_for(campaign_id, day):
        key = (campaign_id, day)
        if key not in groups:
            groups[key] = {"sessions": 0, "purchasing_sessions": 0, "revenue_cents": 0,
                           "assisted_sessions": 0}
        return groups[key]

    for index, session in enumerate(sessions):
        group = group_for(credit[index], session.day)
        group["sessions"] += 1
        if session.purchases:
            group["purchasing_sessions"] += 1
        group["revenue_cents"] += sum(price for _category, price in session.purchases)
        for campaign_id in assists[index]:
            group_for(campaign_id, session.day)["assisted_sessions"] += 1
    rows = []
    for (campaign_id, day), group in groups.items():
        # "`channel`: From `campaigns.csv`; `organic` for `organic`"
        channel = ORGANIC if campaign_id == ORGANIC else campaigns[campaign_id]
        rows.append({"campaign_id": campaign_id, "channel": channel, "day": day, **group})
    return rows


def text(value):
    """Parquet strings are UTF-8. rules.md says the input is UTF-8; if a value
    is not, decoding fails loudly instead of writing something else."""
    return None if value is None else value.decode("utf-8")


# rules.md, Outputs: "Exactly these columns, in any order, no extras. Any
# integer width and any Parquet string encoding is accepted; `day` must be a
# Parquet `DATE`."
def write_tables(output_dir, user_rows, campaign_rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    user_schema = pa.schema([
        ("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()),
        ("events", pa.int64()), ("revenue_cents", pa.int64()), ("top_category", pa.string()),
    ])
    campaign_schema = pa.schema([
        ("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()),
        ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()),
        ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64()),
    ])
    for name, schema, rows in (("user_daily", user_schema, user_rows),
                               ("campaign_daily", campaign_schema, campaign_rows)):
        columns = {}
        for field in schema:
            values = [row[field.name] for row in rows]
            if pa.types.is_string(field.type):
                values = [text(value) for value in values]
            columns[field.name] = pa.array(values, type=field.type)
        folder = os.path.join(output_dir, name)
        os.makedirs(folder, exist_ok=True)
        pq.write_table(pa.table(columns, schema=schema), os.path.join(folder, "part-0.parquet"))


# ---------------------------------------------------------------------------
# The whole pipeline
# ---------------------------------------------------------------------------

def run(data_dir, output_dir):
    counts = defaultdict(int)
    window = read_window(data_dir)
    products = read_products(data_dir)
    campaigns = read_campaigns(data_dir)
    log(f"window {window[0]} .. {window[1]} ms; {len(products)} products; "
        f"{len(campaigns)} campaigns")

    # rules.md, Linked IDs: "Links come before everything else".
    pairs = read_link_pairs(data_dir)
    name_of = person_names(pairs)
    log(f"{len(pairs)} usable link lines; {len(name_of)} linked IDs; "
        f"{len(set(name_of.values()))} linked people")

    kept = dedup_events(data_dir, counts)
    log(f"{len(kept)} distinct event_ids")
    events_by_person = surviving_events(kept, window, products, name_of, counts)
    del kept

    sessions = []
    for person in sorted(events_by_person):
        sessions.extend(sessionize(person, events_by_person[person]))
    del events_by_person
    counts["sessions"] = len(sessions)

    clicks_by_person = valid_clicks_by_person(
        dedup_clicks(data_dir, counts), window, campaigns, name_of, counts)

    credit = []
    assists = []
    for session in sessions:
        qualifying = qualifying_clicks(clicks_by_person.get(session.person, []), session.start)
        credit.append(credited_campaign(qualifying))
        assists.append(assisting_campaigns(qualifying))

    user_rows = user_daily_rows(sessions)
    campaign_rows = campaign_daily_rows(sessions, credit, assists, campaigns)
    write_tables(output_dir, user_rows, campaign_rows)
    for key in sorted(counts):
        log(f"  {key}: {counts[key]}")
    log(f"wrote {len(user_rows)} user_daily rows and {len(campaign_rows)} campaign_daily rows")


def main(argv):
    if len(argv) >= 3:
        data_dir, output_dir = argv[-2], argv[-1]
    else:
        data_dir = os.environ.get("DATA_DIR", "/data")
        output_dir = os.environ.get("OUTPUT_DIR", "/output")
    try:
        run(data_dir, output_dir)
    except InputBreaksAGuarantee as error:
        log(f"reference: the input breaks a guarantee of rules.md: {error}")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
