#!/usr/bin/env python3
"""
Mutation survey: does every rule have a fixture that catches it when broken?

Each mutant is the reference with one rule broken in one small way (a
monkeypatch, so reference.py is never edited). The survey runs every mutant
on the fixture folders and reports which fixtures kill it. A mutant no
fixture kills is a rule the fixtures do not pin down.

    mutants.py survey FIXTURE_ROOT [FIXTURE_ROOT ...]   # run all mutants
    mutants.py run MUTANT                               # act as an implementation
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "reference"))
import reference as ref  # noqa: E402


def mutant_offset_sign(r):
    original = r.parse_f2

    def parse_f2(text):
        value = original(text)
        if value is None:
            return None
        local = r.parse_calendar_part(text[:19])
        return local + (local - value)  # adds the offset instead of subtracting it
    r.parse_f2 = parse_f2


def mutant_price_zero_ok(r):
    r.purchase_price = lambda p: int(p) if r.is_digits(p) and int(p) <= 10_000_000 else None


def mutant_price_top_exclusive(r):
    r.purchase_price = lambda p: int(p) if r.is_digits(p) and 1 <= int(p) < 10_000_000 else None


def mutant_top_category_largest_on_tie(r):
    def top(totals):
        if not totals:
            return None
        best = max(totals.values())
        return max(c for c, t in totals.items() if t == best)
    r.top_category = top


def mutant_name_is_largest_id(r):
    original = r.person_names

    def names(pairs):
        mapping = original(pairs)
        groups = {}
        for member, name in mapping.items():
            groups.setdefault(name, []).append(member)
        return {m: max(ms) for ms in groups.values() for m in ms}
    r.person_names = names


def mutant_ignore_links(r):
    r.person_names = lambda pairs: {}


def mutant_links_not_transitive(r):
    def names(pairs):
        mapping = {}
        for a, b in pairs:
            mapping.setdefault(a, min(a, b))
            mapping.setdefault(b, min(a, b))
        return mapping
    r.person_names = names


def mutant_lenient_quotes(r):
    def parse_line(line, column_count):
        if line == b"":
            return None
        fields = line.split(b",")
        if len(fields) != column_count:
            return None
        return [f.strip(b'"') for f in fields]
    r.parse_line = parse_line


def mutant_strip_carriage_return(r):
    original = r.parse_line
    r.parse_line = lambda line, n: original(line.rstrip(b"\r"), n)


def mutant_trim_spaces(r):
    original = r.parse_line

    def parse_line(line, n):
        values = original(line, n)
        return None if values is None else [v.strip(b" ") for v in values]
    r.parse_line = parse_line


def mutant_case_insensitive_type(r):
    original = r.validate_event

    def validate(values, window):
        values = list(values)
        values[4] = values[4].lower()
        return original(values, window)
    r.validate_event = validate


def mutant_keep_latest_copy(r):
    def dedup(data_dir, counts):
        kept = {}
        for path in r.csv_files(os.path.join(data_dir, "events")):
            for line in r.data_lines(path):
                values = r.parse_line(line, r.EVENT_COLUMNS)
                if values is None or values[0] == b"":
                    continue
                ingest = r.parse_ingest_ts(values[3])
                old = kept.get(values[0])
                if old is None or ingest > r.parse_ingest_ts(r.parse_line(old, r.EVENT_COLUMNS)[3]):
                    kept[values[0]] = line
        return kept
    r.dedup_events = dedup


def mutant_unknown_product_dropped(r):
    original = r.validate_event
    holder = {}

    def validate(values, window):
        valid = original(values, window)
        if valid is not None and valid[3] not in holder["products"]:
            return None
        return valid
    original_products = r.read_products

    def read_products(data_dir):
        holder["products"] = original_products(data_dir)
        return holder["products"]
    r.read_products = read_products
    r.validate_event = validate


def mutant_day_of_last_event(r):
    original = r.sessionize

    def sessionize(person, events):
        sessions = original(person, events)
        ordered = sorted(ts for ts, _p, _c in events)
        at = 0
        for session in sessions:
            at += session.events
            session.start_for_day = ordered[at - 1]
        return sessions
    r.sessionize = sessionize
    r.Session.day = property(lambda s: r.utc_date(getattr(s, "start_for_day", s.start)))


def mutant_lookback_exclusive(r):
    def qualifying(clicks, start):
        return [c for c in clicks if c[0] <= start and start - c[0] < r.CLICK_LOOKBACK_MS]
    r.qualifying_clicks = qualifying


def mutant_click_after_start_counts(r):
    def qualifying(clicks, start):
        return [c for c in clicks if c[0] <= start + 1 and start - c[0] <= r.CLICK_LOOKBACK_MS]
    r.qualifying_clicks = qualifying


def mutant_click_window_start_exclusive(r):
    r.in_click_window = lambda ts, w: w[0] - r.CLICK_LOOKBACK_MS < ts < w[1]


def mutant_event_window_end_inclusive(r):
    r.in_event_window = lambda ts, w: w[0] <= ts <= w[1]


def mutant_assists_exclude_credited(r):
    def assisting(qualifying):
        credited = r.credited_campaign(qualifying)
        return {c for _t, _k, c in qualifying} - {credited}
    r.assisting_campaigns = assisting


def mutant_assists_count_clicks(r):
    # Count a campaign once per qualifying click instead of once per session.
    r.assisting_campaigns = lambda q: [c for _t, _k, c in q]


def mutant_f3_64_bit_wrap(r):
    r.parse_f3 = lambda t: (int(t) % (1 << 64)) if r.is_digits(t) else None


def mutant_unicode_digits(r):
    r.is_digits = lambda v: len(v) > 0 and v.decode("utf-8", "replace").isdigit()


def mutant_no_leap_years(r):
    r.is_leap_year = lambda year: False


def mutant_late_uses_local_time(r):
    original = r.validate_event

    def validate(values, window):
        valid = original(values, window)
        if valid is None:
            return None
        text = values[2]
        if len(text) == 25:  # F2: use the local clock time for the late check
            local = r.parse_calendar_part(text[:19])
            if r.parse_ingest_ts(values[3]) - local > r.LATE_LIMIT_MS:
                return None
        return valid
    r.validate_event = validate


MUTANTS = {name[len("mutant_"):]: function for name, function in globals().items()
           if name.startswith("mutant_")}


def survey(roots):
    check = os.path.join(HERE, "check.py")
    survivors = []
    for name in sorted(MUTANTS):
        process = subprocess.run([sys.executable, check, "fixtures", *roots, "--",
                                  sys.executable, os.path.abspath(__file__), "run", name],
                                 capture_output=True, text=True)
        killers = []
        for line in process.stdout.splitlines():
            if line.startswith("FAIL"):
                case = line.split()[1]
                crashed = case.endswith(":")  # "FAIL case: exit N"
                killers.append(os.path.basename(case.rstrip(":")) + (" (crash)" if crashed else ""))
        print(f"{name:34} {'killed by ' + ', '.join(killers) if killers else 'SURVIVED'}")
        if not killers:
            survivors.append(name)
    print(f"\n{len(MUTANTS) - len(survivors)}/{len(MUTANTS)} mutants killed")
    return 1 if survivors else 0


def main(argv):
    if len(argv) >= 3 and argv[1] == "run":
        MUTANTS[argv[2]](ref)
        return ref.main([argv[0]])
    if len(argv) >= 3 and argv[1] == "survey":
        return survey(argv[2:])
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
