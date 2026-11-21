#!/usr/bin/env python3
"""
Writes the adversarial fixtures into harness/fixtures/adversarial/.

Each case below holds its input lines (escapes such as \\r are written as
bytes) and its expected rows. The expected rows are written by hand from
rules.md, never computed by the reference; the reasoning is in the comment
above each case. Run this again after editing a case; the folders it writes
are committed so that no one needs to run it.

Unless a case says otherwise: window 2026-10-01 to 2026-10-15, products p1
(shoes) and p2 (books), campaigns c_1 (search) and c_2 (social), no links,
and T = 2026-10-03T10:00:00Z = 1791021600000 ms.
"""
import csv
import json
import os
import random
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "adversarial")

EVENT_HEADER = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
CLICK_HEADER = "click_id,user_id,campaign_id,ts"
LINK_HEADER = "user_id_a,user_id_b"
USER_HEADER = ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"]
CAMPAIGN_HEADER = ["campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                   "revenue_cents", "assisted_sessions"]
WINDOW = ("2026-10-01T00:00:00Z", "2026-10-15T00:00:00Z")
PRODUCTS = [("p1", "shoes"), ("p2", "books")]
CAMPAIGNS = [("c_1", "search"), ("c_2", "social")]

CASES = []


def case(name, note, events, expected_user, expected_campaign, clicks=(), links=(),
         window=WINDOW, products=PRODUCTS, campaigns=CAMPAIGNS, extra_files=None):
    """events, clicks and links are lists of files, each a list of lines.
    A plain list of strings is taken as one file."""
    CASES.append(dict(name=name, note=note, events=events, clicks=clicks, links=links,
                      window=window, products=products, campaigns=campaigns,
                      expected_user=expected_user, expected_campaign=expected_campaign,
                      extra_files=extra_files or {}))


def as_files(value):
    if not value:
        return [[]]
    if isinstance(value[0], str):
        return [list(value)]
    return [list(lines) for lines in value]


def write_csv_part(folder, index, header, lines):
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, f"part-{index:05d}.csv"), "wb") as handle:
        handle.write((header + "\n").encode("utf-8"))
        for line in lines:
            handle.write((line + "\n").encode("utf-8"))


def write_expected(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        for row in rows:
            writer.writerow(["" if value is None else value for value in row])


def build():
    if os.path.isdir(OUT):
        shutil.rmtree(OUT)
    for spec in CASES:
        root = os.path.join(OUT, spec["name"])
        data = os.path.join(root, "input")
        for folder, header, files in (("events", EVENT_HEADER, spec["events"]),
                                      ("clicks", CLICK_HEADER, spec["clicks"]),
                                      ("links", LINK_HEADER, spec["links"])):
            for index, lines in enumerate(as_files(files)):
                write_csv_part(os.path.join(data, folder), index, header, lines)
        with open(os.path.join(data, "products.csv"), "w", encoding="utf-8") as handle:
            handle.write("product_id,category\n")
            handle.writelines(f"{p},{c}\n" for p, c in spec["products"])
        with open(os.path.join(data, "campaigns.csv"), "w", encoding="utf-8") as handle:
            handle.write("campaign_id,channel\n")
            handle.writelines(f"{c},{ch}\n" for c, ch in spec["campaigns"])
        with open(os.path.join(data, "manifest.json"), "w", encoding="utf-8") as handle:
            json.dump({"window_start": spec["window"][0], "window_end": spec["window"][1]},
                      handle, indent=2)
            handle.write("\n")
        for relative, content in spec["extra_files"].items():
            with open(os.path.join(data, relative), "wb") as handle:
                handle.write(content.encode("utf-8"))
        expected = os.path.join(root, "expected")
        os.makedirs(expected)
        write_expected(os.path.join(expected, "user_daily.csv"), USER_HEADER,
                       spec["expected_user"])
        write_expected(os.path.join(expected, "campaign_daily.csv"), CAMPAIGN_HEADER,
                       spec["expected_campaign"])
        with open(os.path.join(root, "note.txt"), "w", encoding="utf-8") as handle:
            handle.write(spec["note"] + "\n")
    with open(os.path.join(OUT, "README.md"), "w", encoding="utf-8") as handle:
        handle.write("# Adversarial fixtures\n\nWritten by `../build_adversarial.py`, which holds "
                     "each case's input and hand-written expected rows with the reasoning.\n"
                     "Each `note.txt` names the rule the case tests and quotes the rules.md "
                     "sentence that decides it.\n\n| Case | Rule and deciding sentence |\n"
                     "| --- | --- |\n")
        for spec in CASES:
            handle.write(f"| {spec['name']} | {spec['note'].replace('|', '/')} |\n")
    print(f"wrote {len(CASES)} cases to {OUT}")


D = "2026-10-03"  # the usual output day

# ---------------------------------------------------------------------------
# X01. The earliest copy is invalid (bad type, bad price, outside the window,
# late, empty product) and a later copy is valid: every such event is dropped.
# The later copies are in the first file, so a reader that keeps the first
# copy it sees, or the first valid one, gets them wrong. e6's earliest copy is
# malformed (a stray quote), so the later copy is kept. e7 is a plain view.
# u1 keeps e7 (10:00) and e6 (10:10): one session, 2 events, no purchase.
# ---------------------------------------------------------------------------
case("X01_invalid_earliest_copy",
     'Events step 2: "if the kept copy is invalid the event is dropped, and a later valid copy '
     'does not rescue it. A malformed copy was already dropped in step 1, so it is never the kept copy."',
     events=[
         ["e1,u1,2026-10-03T10:01:00Z,2026-10-03T10:05:00Z,view,p1,",
          "e2,u1,2026-10-03T10:02:00Z,2026-10-03T10:05:00Z,purchase,p1,500",
          "e3,u1,2026-10-03T10:03:00Z,2026-10-03T10:05:00Z,view,p1,",
          "e4,u1,2026-10-03T10:04:00Z,2026-10-03T10:05:00Z,view,p1,",
          "e5,u1,2026-10-03T10:05:00Z,2026-10-03T10:06:00Z,view,p1,",
          "e6,u1,2026-10-03T10:10:00Z,2026-10-03T10:11:00Z,view,p1,",
          "e7,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,"],
         ["e1,u1,2026-10-03T10:01:00Z,2026-10-03T10:01:00Z,View,p1,",
          "e2,u1,2026-10-03T10:02:00Z,2026-10-03T10:01:00Z,purchase,p1,0",
          "e3,u1,2026-09-30T23:00:00Z,2026-10-03T10:01:00Z,view,p1,",
          "e4,u1,2026-10-01T09:00:00Z,2026-10-03T10:01:00Z,view,p1,",
          "e5,u1,2026-10-03T10:05:00Z,2026-10-03T10:01:00Z,view,,",
          'e6,u1,2026-10-03T10:10:00Z,2026-10-03T10:00:30Z,view,p1,5"'],
     ],
     expected_user=[("u1", D, 1, 2, 0, None)],
     expected_campaign=[("organic", "organic", D, 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X02. A later copy names a different user. Links: u2,u9. e1's earliest copy
# (ingest 10:01, in the second file) is by u9, so e1 is person u2's, not u1's.
# e3's earliest copy (ingest 11:59:59, second file) is by u3, so the purchase
# of 700 is u3's. e2 is u1's own view.
# ---------------------------------------------------------------------------
case("X02_later_copy_changes_user",
     'Events step 2: "Dedup on `event_id`. Keep only the copy with the smallest `ingest_ts`." '
     'Linked IDs: "an event or click is the person\'s as soon as it is parsed, whatever its own ID."',
     events=[
         ["e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:05:00Z,view,p1,",
          "e3,u1,2026-10-03T11:00:00Z,2026-10-03T12:00:00Z,purchase,p1,700",
          "e2,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,"],
         ["e1,u9,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
          "e3,u3,2026-10-03T11:00:00Z,2026-10-03T11:59:59Z,purchase,p1,700"],
     ],
     links=["u2,u9"],
     expected_user=[("u1", D, 1, 1, 0, None), ("u2", D, 1, 1, 0, None),
                    ("u3", D, 1, 1, 700, "shoes")],
     expected_campaign=[("organic", "organic", D, 3, 1, 700, 0)])

# ---------------------------------------------------------------------------
# X03. Stray quotes in fields the rules otherwise ignore. A view or cart's
# price is never validated, but the line is malformed when the price holds a
# quote that does not wrap the whole field: e1 (5"), e2 ("5), e5 (x"y),
# e7 ("""") and e8 (a space before the quote of product) are dropped. e3
# (price "") and e4 (price "abc") are well formed and kept. So u1 has e3
# (10:10) and e4 (10:15). Click k1 is malformed (quote after ts); k2 (quoted
# ts, 08:00) credits and assists u1's session with c_2. The link u1,u7" is
# malformed, so u7 is a person of their own with an organic session.
# ---------------------------------------------------------------------------
case("X03_stray_quotes",
     'CSV dialect: "A line with the wrong number of fields, or with a quote anywhere except around '
     'a whole field, is malformed and is dropped." Events step 3: "`price_cents` is ignored whatever it holds" '
     'applies only to lines that parsed.',
     events=['e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,5"',
             'e2,u1,2026-10-03T10:05:00Z,2026-10-03T10:06:00Z,view,p1,"5',
             'e3,u1,2026-10-03T10:10:00Z,2026-10-03T10:11:00Z,view,p1,""',
             'e4,u1,2026-10-03T10:15:00Z,2026-10-03T10:16:00Z,view,p1,"abc"',
             'e5,u1,2026-10-03T10:20:00Z,2026-10-03T10:21:00Z,cart,p1,x"y',
             'e7,u1,2026-10-03T10:30:00Z,2026-10-03T10:31:00Z,view,p1,""""',
             'e8,u1,2026-10-03T10:35:00Z,2026-10-03T10:36:00Z,view, "p1",',
             'e9,u7,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,'],
     clicks=['k1,u1,c_1,2026-10-03T09:00:00Z"',
             'k2,u1,c_2,"2026-10-03T08:00:00Z"'],
     links=['u1,u7"'],
     expected_user=[("u1", D, 1, 2, 0, None), ("u7", D, 1, 1, 0, None)],
     expected_campaign=[("c_2", "social", D, 1, 0, 0, 1), ("organic", "organic", D, 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X04. Carriage returns inside fields, not only at line ends.
#   e1: user "u1\r", a person of their own (view 10:00).
#   e2: event_type "view\r": dropped.  e4: ts "...Z\r": dropped.
#   e3: product "p1\r" is not in products.csv: a purchase of 900 in "unknown".
#   e5: a view priced "7\r" (quoted): kept, the price is ignored.
#   e6: a cart priced "\r": kept.   A line holding only "\r": malformed.
# Link "u1,u0\r" makes u1 and "u0\r" one person, named "u0\r" (0 < 1).
# Person u0\r: e3 10:05, e5 10:15, e6 10:20: one session, 3 events, 900 unknown.
# Clicks: k1's ts ends in \r (bad), k2's campaign is "c_1\r" (unknown), so
# u0\r's session is organic. k3 is by "u1\r" for c_2 at 09:00: it credits
# and assists person u1\r's session.
# ---------------------------------------------------------------------------
case("X04_carriage_returns",
     'CSV dialect: "Only `\\n` ends a line: a `\\r` is an ordinary character of the field it is in". '
     'Strings: "IDs compare exactly, case-sensitive, untrimmed."',
     events=["e1,u1\r,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e2,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view\r,p1,",
             "e3,u1,2026-10-03T10:05:00Z,2026-10-03T10:06:00Z,purchase,p1\r,900",
             "e4,u1,2026-10-03T10:10:00Z\r,2026-10-03T10:11:00Z,view,p1,",
             'e5,u1,2026-10-03T10:15:00Z,2026-10-03T10:16:00Z,view,p1,"7\r"',
             "\r",
             'e6,u1,"2026-10-03T10:20:00Z",2026-10-03T10:21:00Z,cart,p2,\r'],
     clicks=["k1,u1,c_1,2026-10-03T09:00:00Z\r",
             "k2,u1,c_1\r,2026-10-03T09:30:00Z",
             "k3,u1\r,c_2,2026-10-03T09:00:00Z"],
     links=["u1,u0\r"],
     expected_user=[("u0\r", D, 1, 3, 900, "unknown"), ("u1\r", D, 1, 1, 0, None)],
     expected_campaign=[("c_2", "social", D, 1, 0, 0, 1),
                        ("organic", "organic", D, 1, 1, 900, 0)])

# ---------------------------------------------------------------------------
# X05. Gaps one millisecond under, exactly at and one over 30 minutes, in
# mixed forms.  a: 10:00:00 (F1), 10:29:59.999 (+1,799,999), 10:59:59.999
# (+1,800,000), then 11:30:00 written as 13:30:00+02:00 (+1,800,001): two
# sessions, 3 and 1 events.  b: 10:00 (F3), 10:30:00Z (+1,800,000), then
# 11:00:00.001 (+1,800,001): two sessions, 2 and 1 events.
# ---------------------------------------------------------------------------
case("X05_thirty_minute_boundary",
     'Events step 5: "A new session starts when the gap from the previous event is over 30 minutes '
     '(1,800,000 ms)."',
     events=["e1,a,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e2,a,1791023399999,2026-10-03T10:31:00Z,view,p1,",
             "e3,a,1791025199999,2026-10-03T11:01:00Z,view,p1,",
             "e4,a,2026-10-03T13:30:00+02:00,2026-10-03T11:31:00Z,view,p1,",
             "e5,b,1791021600000,2026-10-03T10:01:00Z,view,p1,",
             "e6,b,2026-10-03T10:30:00Z,2026-10-03T10:31:00Z,view,p1,",
             "e7,b,1791025200001,2026-10-03T11:01:00Z,view,p1,"],
     expected_user=[("a", D, 2, 4, 0, None), ("b", D, 2, 3, 0, None)],
     expected_campaign=[("organic", "organic", D, 4, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X06. Sessions crossing midnight and offsets that change the date.
#   u1: 23:45 Oct 3; a purchase of p1 for 1000 at 00:10 Oct 4 (written
#       20:10-04:00); 00:40 (exactly 30 min later, same session); then 01:10:01
#       (30:01 later, a new session on Oct 4). Click k1 (c_1, Oct 3 23:00)
#       credits and assists both sessions, on their own days.
#   u2: starts at exactly Oct 4 00:00 (written 09:00+09:00), buys p2 for 300
#       at 00:05: organic, Oct 4.
#   u3: 23:50 Oct 14 kept; 00:05 Oct 15 is outside the window and dropped.
#   u4: 01:00+02:00 Oct 1 is Sep 30 23:00 UTC, dropped; 23:30-01:00 Sep 30 is
#       Oct 1 00:30 UTC, kept.
# ---------------------------------------------------------------------------
case("X06_midnight",
     'Events step 6: "One day per session: the UTC date of its start. All its events and purchases '
     'count on that day, even those after midnight." Timestamps F2: "subtract it to get UTC."',
     events=["e1,u1,2026-10-03T23:45:00Z,2026-10-03T23:46:00Z,view,p1,",
             "e2,u1,2026-10-03T20:10:00-04:00,2026-10-04T00:11:00Z,purchase,p1,1000",
             "e3,u1,2026-10-04T00:40:00Z,2026-10-04T00:41:00Z,view,p1,",
             "e4,u1,2026-10-04T01:10:01Z,2026-10-04T01:11:00Z,view,p1,",
             "e5,u2,2026-10-04T09:00:00+09:00,2026-10-04T00:01:00Z,view,p1,",
             "e6,u2,2026-10-04T00:05:00Z,2026-10-04T00:06:00Z,purchase,p2,300",
             "e7,u3,2026-10-14T23:50:00Z,2026-10-14T23:51:00Z,view,p1,",
             "e8,u3,2026-10-15T00:05:00Z,2026-10-15T00:06:00Z,view,p1,",
             "e9,u4,2026-10-01T01:00:00+02:00,2026-10-01T00:01:00Z,view,p1,",
             "e10,u4,2026-09-30T23:30:00-01:00,2026-10-01T00:31:00Z,view,p1,"],
     clicks=["k1,u1,c_1,2026-10-03T23:00:00Z"],
     expected_user=[("u1", D, 1, 3, 1000, "shoes"), ("u1", "2026-10-04", 1, 1, 0, None),
                    ("u2", "2026-10-04", 1, 2, 300, "books"), ("u3", "2026-10-14", 1, 1, 0, None),
                    ("u4", "2026-10-01", 1, 1, 0, None)],
     expected_campaign=[("c_1", "search", D, 1, 1, 1000, 1),
                        ("c_1", "search", "2026-10-04", 1, 0, 0, 1),
                        ("organic", "organic", "2026-10-04", 1, 1, 300, 0),
                        ("organic", "organic", "2026-10-14", 1, 0, 0, 0),
                        ("organic", "organic", "2026-10-01", 1, 0, 0, 0)])


# ---------------------------------------------------------------------------
# X07. Long chains, and unused IDs that become the name.
#   Chain 1: n000..n299 linked one after the next in a shuffled order (299
#     lines, shuffled across two files, some written the other way round),
#     plus "a_unused,<the 150th ID in chain order>". a_unused appears in no
#     event or click and is the byte-wise smallest, so the person is
#     "a_unused". Views by n000 (10:00), n299 (10:20), n150 (10:40): one
#     session of 3. Click by n123 for c_1 at 09:59 credits and assists it.
#   Chain 2: q000..q149 the same way, plus "Q077,q077": the upper-case ID
#     sorts before every lower-case one, so the person is "Q077". Views by
#     q000 at 10:00 and q149 at 12:00: two sessions.
#   Chain 3: r100..r149 in a chain, then r150..r199 in a chain, joined by one
#     link that is malformed (three fields), so they are two people: "r100"
#     (view by r149 at 10:00) and "r150" (view by r177 at 10:10).
# ---------------------------------------------------------------------------
def chain_links(ids, seed, extra=(), broken_after=None):
    shuffler = random.Random(seed)
    order = list(ids)
    shuffler.shuffle(order)
    lines = []
    for index in range(len(order) - 1):
        a, b = order[index], order[index + 1]
        if shuffler.random() < 0.5:
            a, b = b, a
        lines.append(f"{a},{b}")
    lines.extend(extra)
    shuffler.shuffle(lines)
    return order, lines


chain1_order, chain1 = chain_links([f"n{i:03d}" for i in range(300)], 1)
chain1.append(f"a_unused,{chain1_order[150]}")
_, chain2 = chain_links([f"q{i:03d}" for i in range(150)], 2, extra=["Q077,q077"])
r_first, r1 = chain_links([f"r{i:03d}" for i in range(100, 150)], 3)
r_second, r2 = chain_links([f"r{i:03d}" for i in range(150, 200)], 4)
chain3 = r1 + r2 + [f"{r_first[-1]},{r_second[0]},x"]
links7 = chain1 + chain2 + chain3
random.Random(5).shuffle(links7)
case("X07_long_chains",
     'Linked IDs step 2: "joining is transitive ... however many links apart and in whichever files '
     'and order the lines come." Step 3: "An ID can be linked without appearing in any event or click, '
     'and can still be the smallest, and so the name."',
     events=["e1,n000,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e2,n299,2026-10-03T10:20:00Z,2026-10-03T10:21:00Z,view,p1,",
             "e3,n150,2026-10-03T10:40:00Z,2026-10-03T10:41:00Z,view,p1,",
             "e4,q000,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e5,q149,2026-10-03T12:00:00Z,2026-10-03T12:01:00Z,view,p1,",
             "e6,r149,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e7,r177,2026-10-03T10:10:00Z,2026-10-03T10:11:00Z,view,p1,"],
     clicks=["k1,n123,c_1,2026-10-03T09:59:00Z"],
     links=[links7[:len(links7) // 2], links7[len(links7) // 2:]],
     expected_user=[("Q077", D, 2, 2, 0, None), ("a_unused", D, 1, 3, 0, None),
                    ("r100", D, 1, 1, 0, None), ("r150", D, 1, 1, 0, None)],
     expected_campaign=[("c_1", "search", D, 1, 0, 0, 1), ("organic", "organic", D, 4, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X08. Clicks at the same timestamp. All sessions start at 10:00.
#   u1: k9 (c_2) and k10 (c_1) at 09:00. Byte-wise "k10" < "k9" (numeric
#       order would pick k9): c_1 credited; both assist.
#   u2 (linked to u3): kb by u2 for c_2 at 09:30:00Z and ka by u3 for c_1 at
#       1791019800000, the same millisecond: ka wins, c_1; both assist.
#   u4 (buys p1 for 100): k5 for c_2 twice, once quoted (one click), and k6
#       for c_2 at 08:00: c_2 credited, and assists once.
#   u5: kz (c_1) at 09:00:00.001 beats ky (c_2) at 09:00:00 although
#       "ky" < "kz": the tie-break applies only to equal ts. Both assist.
# c_1: credited u1, u2, u5; assists u1, u2, u5.  c_2: credited u4 (100);
# assists u1, u2, u4, u5.
# ---------------------------------------------------------------------------
case("X08_click_ties",
     'Clicks step 4: "take the one with the latest `ts`; on a tie in `ts`, the smallest `click_id`, '
     'compared byte by byte." Step 7: "Each such campaign assists the session once".',
     events=["e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e2,u2,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e4,u4,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,100",
             "e5,u5,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,"],
     clicks=[["k9,u1,c_2,2026-10-03T09:00:00Z",
              "k10,u1,c_1,2026-10-03T09:00:00Z",
              "kb,u2,c_2,2026-10-03T09:30:00Z",
              'k5,u4,c_2,2026-10-03T09:00:00Z'],
             ["ka,u3,c_1,1791019800000",
              '"k5","u4","c_2","2026-10-03T09:00:00Z"',
              "k6,u4,c_2,2026-10-03T08:00:00Z",
              "kz,u5,c_1,1791018000001",
              "ky,u5,c_2,2026-10-03T09:00:00Z"]],
     links=["u3,u2"],
     expected_user=[("u1", D, 1, 1, 0, None), ("u2", D, 1, 1, 0, None),
                    ("u4", D, 1, 1, 100, "shoes"), ("u5", D, 1, 1, 0, None)],
     expected_campaign=[("c_1", "search", D, 3, 0, 0, 3), ("c_2", "social", D, 1, 1, 100, 4)])

# ---------------------------------------------------------------------------
# X09. A window holding February 29 (2028-02-22 to 2028-03-07).
#   u1: 2028-02-29T10:00:00Z, day Feb 29. Click k2 (c_2) at exactly
#       window_start - 7 days (Feb 15 00:00) is a valid click but 14 days
#       before the session, so u1 is organic.
#   u2: 2028-03-01T05:00:00+08:00 is Feb 29 21:00 UTC.
#   u3: Feb 28 23:45, then 1835395200000 (Feb 29 00:00): one session, Feb 28.
#   u4: 2028-02-30 and 2027-02-29 are impossible: no rows.
#   u5: buys p2 for 400 at Feb 29 23:59:59. Click k1 (c_1) at Feb 22
#       23:59:59 is exactly 7 days earlier, because Feb 29 exists: it credits.
# ---------------------------------------------------------------------------
case("X09_leap_day",
     'Timestamps: "an impossible calendar value (month 13, February 30, February 29 outside a leap '
     'year ...)" is bad, so February 29 in a leap year is valid; Clicks step 4: "`session_start - '
     'click.ts <= 7 days` (604,800,000 ms)".',
     window=("2028-02-22T00:00:00Z", "2028-03-07T00:00:00Z"),
     events=["e1,u1,2028-02-29T10:00:00Z,2028-02-29T10:01:00Z,view,p1,",
             "e2,u2,2028-03-01T05:00:00+08:00,2028-02-29T21:01:00Z,view,p1,",
             "e3,u3,2028-02-28T23:45:00Z,2028-02-28T23:46:00Z,view,p1,",
             "e4,u3,1835395200000,2028-02-29T00:01:00Z,view,p1,",
             "e5,u4,2028-02-30T10:00:00Z,2028-03-01T10:01:00Z,view,p1,",
             "e6,u4,2027-02-29T10:00:00Z,2028-03-01T10:01:00Z,view,p1,",
             "e7,u5,2028-02-29T23:59:59Z,2028-03-01T00:01:00Z,purchase,p2,400"],
     clicks=["k1,u5,c_1,2028-02-22T23:59:59Z",
             "k2,u1,c_2,2028-02-15T00:00:00Z"],
     expected_user=[("u1", "2028-02-29", 1, 1, 0, None), ("u2", "2028-02-29", 1, 1, 0, None),
                    ("u3", "2028-02-28", 1, 2, 0, None), ("u5", "2028-02-29", 1, 1, 400, "books")],
     expected_campaign=[("c_1", "search", "2028-02-29", 1, 1, 400, 1),
                        ("organic", "organic", "2028-02-29", 2, 0, 0, 0),
                        ("organic", "organic", "2028-02-28", 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X10. Very old (and very new) ingest times; any valid F1 is allowed.
#   e1: earliest copy ingested 1901-01-01T00:00:00Z has a bad ts; the copy
#       one second later (listed first) is valid. Dropped. (Ingest seconds
#       clamped to 24 or 32 bits would make the two equal.)
#   e2: earliest copy (year 0001) is u1's view at 10:05; the copy one second
#       later is a purchase. The view is kept.
#   e3: ingested 1970-01-01, long before its ts: not late, kept (10:10).
#   e4: ingested 9999-12-31T23:59:59Z: late, dropped.
#   e5: earliest copy ingested 1969-12-31T23:59:59Z (before the epoch) has
#       the type "VIEW": dropped, although the 2026 copy is valid.
# u1: e2 and e3, one session of 2 starting 10:05.
# ---------------------------------------------------------------------------
case("X10_ancient_ingest",
     'Timestamps: "`ingest_ts` is always valid F1"; Events step 2: "Keep only the copy with the '
     'smallest `ingest_ts`"; step 4: "Exactly 24 hours is kept, and so is an `ingest_ts` before `ts`."',
     events=["e1,u1,2026-10-03T10:00:00Z,1901-01-01T00:00:01Z,view,p1,",
             "e1,u1,2026-10-03 10:00:00,1901-01-01T00:00:00Z,view,p1,",
             "e2,u1,2026-10-03T10:05:00Z,0001-01-01T00:00:01Z,purchase,p1,500",
             "e2,u1,2026-10-03T10:05:00Z,0001-01-01T00:00:00Z,view,p1,",
             "e3,u1,2026-10-03T10:10:00Z,1970-01-01T00:00:00Z,view,p1,",
             "e4,u1,2026-10-03T10:15:00Z,9999-12-31T23:59:59Z,view,p1,",
             "e5,u2,2026-10-03T10:20:00Z,2026-10-03T10:20:00Z,view,p1,",
             "e5,u2,2026-10-03T10:20:00Z,1969-12-31T23:59:59Z,VIEW,p1,"],
     expected_user=[("u1", D, 1, 2, 0, None)],
     expected_campaign=[("organic", "organic", D, 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X11. F3 values beyond 64 bits, and long leading zeros.
#   u1: ts 2^64 + T: outside the window (it wraps to T in 64-bit arithmetic).
#   u3: T with 20 leading zeros (33 digits): valid, kept.
#   u4: view at T; click for c_1 at 2^64 + T - 1 h: outside the click window.
#   u5: buys p1 priced 500 with 20 leading zeros: kept.
#   u6: buys p1 priced 2^64 + 1 (wraps to 1): over 10,000,000, dropped.
# ---------------------------------------------------------------------------
TWO_64 = 18446744073709551616
case("X11_f3_beyond_64_bits",
     'Timestamps F3: "Leading zeros are allowed" and "one that is ... too long for a 64-bit integer '
     'is not malformed, only outside the window." Events step 3: price "digits only with a value '
     'from 1 to 10,000,000".',
     events=[f"e1,u1,{TWO_64 + 1791021600000},2026-10-03T10:01:00Z,view,p1,",
             "e3,u3,000000000000000000001791021600000,2026-10-03T10:01:00Z,view,p1,",
             "e4,u4,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e5,u5,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,00000000000000000000500",
             f"e6,u6,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,{TWO_64 + 1}"],
     clicks=[f"k1,u4,c_1,{TWO_64 + 1791018000000}"],
     expected_user=[("u3", D, 1, 1, 0, None), ("u4", D, 1, 1, 0, None),
                    ("u5", D, 1, 1, 500, "shoes")],
     expected_campaign=[("organic", "organic", D, 3, 1, 500, 0)])

# ---------------------------------------------------------------------------
# X12. Byte-wise order of UTF-8 and of case.
#   Link "～x,😀x" (U+FF5E is EF BD 9E, U+1F600 is F0 9F 98 80): the name is
#   "～x". UTF-16 order would pick "😀x". The view is by 😀x.
#   Link "éa,zb": byte-wise "zb" (7A) < "éa" (C3 A9); a locale collation
#   would put é first. The view is by éa.
#   u3 buys p3 (Zebra) and p4 (apple) for 250 each: "Zebra" < "apple".
#   u4 buys p9 (unknown) and p1 (shoes) for 300 each: "shoes" < "unknown".
# ---------------------------------------------------------------------------
case("X12_byte_order",
     'Linked IDs step 3: "Name each person by the byte-wise smallest of their IDs." user_daily '
     'top_category: "a tie goes to the byte-wise smallest".',
     products=PRODUCTS + [("p3", "Zebra"), ("p4", "apple")],
     events=["e1,😀x,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e2,éa,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e3,u3,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p3,250",
             "e4,u3,2026-10-03T10:01:00Z,2026-10-03T10:02:00Z,purchase,p4,250",
             "e5,u4,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p9,300",
             "e6,u4,2026-10-03T10:01:00Z,2026-10-03T10:02:00Z,purchase,p1,300"],
     links=["～x,😀x", "éa,zb"],
     expected_user=[("u3", D, 1, 2, 500, "Zebra"), ("u4", D, 1, 2, 600, "shoes"),
                    ("zb", D, 1, 1, 0, None), ("～x", D, 1, 1, 0, None)],
     expected_campaign=[("organic", "organic", D, 4, 2, 1100, 0)])

# ---------------------------------------------------------------------------
# X13. Assists across days and at the 7-day edge (campaigns c_1, c_2, c_3).
#   u1, session A Oct 3 10:00 (buys p1 for 1000), session B Oct 5 10:00.
#     k1 c_1 Oct 3 09:00, k2 c_1 Oct 3 09:10, k3 c_2 Sep 28 10:00 (exactly
#     7 days before B), k4 c_3 Oct 3 10:00:00.001 (1 ms after A starts).
#     A: qualifying k1, k2, k3; credit k2 (c_1); assists c_1, c_2.
#     B: qualifying k1..k4; credit k4 (c_3); assists c_1, c_2, c_3.
#   u2, session Oct 5 10:00; k5 c_2 at 7 days + 1 ms before: organic.
# ---------------------------------------------------------------------------
case("X13_assists",
     'Clicks step 7: "A session is assisted by every campaign with at least one of the person\'s '
     'valid clicks with `session_start - 7 days <= click.ts <= session_start`"; campaign_daily: "A '
     'campaign that only assisted that day has a row with 0 in the three credited columns."',
     campaigns=CAMPAIGNS + [("c_3", "email")],
     events=["e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,1000",
             "e2,u1,2026-10-05T10:00:00Z,2026-10-05T10:01:00Z,view,p1,",
             "e3,u2,2026-10-05T10:00:00Z,2026-10-05T10:01:00Z,view,p1,"],
     clicks=["k1,u1,c_1,2026-10-03T09:00:00Z",
             "k2,u1,c_1,2026-10-03T09:10:00Z",
             "k3,u1,c_2,1790589600000",
             "k4,u1,c_3,1791021600001",
             "k5,u2,c_2,1790589599999"],
     expected_user=[("u1", D, 1, 1, 1000, "shoes"), ("u1", "2026-10-05", 1, 1, 0, None),
                    ("u2", "2026-10-05", 1, 1, 0, None)],
     expected_campaign=[("c_1", "search", D, 1, 1, 1000, 1), ("c_2", "social", D, 0, 0, 0, 1),
                        ("c_1", "search", "2026-10-05", 0, 0, 0, 1),
                        ("c_2", "social", "2026-10-05", 0, 0, 0, 1),
                        ("c_3", "email", "2026-10-05", 1, 0, 0, 1),
                        ("organic", "organic", "2026-10-05", 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X14. The late cutoff to the millisecond, with an offset.
#   e1: ts 10:00:00.000 (F3), ingest Oct 4 10:00:00: exactly 24 h, kept.
#   e2: ts 09:59:59.999 (F3), same ingest: 24 h + 1 ms, dropped.
#   e3: ts 10:00:00Z, ingest Oct 4 10:00:01: dropped.
#   e4: ts 08:00:00-02:00 (10:00 UTC), ingest Oct 4 10:00:00: exactly 24 h
#       in UTC (26 h if the offset were forgotten), kept.
#   e5: ts 10:01, ingested Oct 1, long before: kept.
# u1: e1, e4, e5: one session of 3.
# ---------------------------------------------------------------------------
case("X14_late_boundary",
     'Events step 4: "drop the event if `ingest_ts - ts` is over 24 hours. Exactly 24 hours is kept, '
     'and so is an `ingest_ts` before `ts`."',
     events=["e1,u1,1791021600000,2026-10-04T10:00:00Z,view,p1,",
             "e2,u1,1791021599999,2026-10-04T10:00:00Z,view,p1,",
             "e3,u1,2026-10-03T10:00:00Z,2026-10-04T10:00:01Z,view,p1,",
             "e4,u1,2026-10-03T08:00:00-02:00,2026-10-04T10:00:00Z,view,p1,",
             "e5,u1,2026-10-03T10:01:00Z,2026-10-01T00:00:00Z,view,p1,"],
     expected_user=[("u1", D, 1, 3, 0, None)],
     expected_campaign=[("organic", "organic", D, 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X15. Purchase prices. Kept: 10000000 (p1) and 0000000001 (p2). Dropped:
# 10000001, +5, " 5", "5 ", 1e3, 0, -0, empty, "" (quoted empty), 5.0.
# A view priced "banana" and a cart priced -5 are kept.
# u1: 4 events, revenue 10,000,001, top category shoes.
# ---------------------------------------------------------------------------
prices15 = ["10000001", "+5", " 5", "5 ", "1e3", "0", "-0", "", '""', "5.0"]
case("X15_prices",
     'Events step 3: "A `purchase` also needs `price_cents` to be digits only with a value from 1 to '
     '10,000,000 (`0250` is 250). For `view` and `cart`, `price_cents` is ignored whatever it holds."',
     events=["e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,10000000",
             "e2,u1,2026-10-03T10:01:00Z,2026-10-03T10:02:00Z,purchase,p2,0000000001",
             "e3,u1,2026-10-03T10:02:00Z,2026-10-03T10:03:00Z,view,p1,banana",
             "e4,u1,2026-10-03T10:03:00Z,2026-10-03T10:04:00Z,cart,p1,-5"]
     + [f"x{i},u1,2026-10-03T10:1{i}:00Z,2026-10-03T10:2{i}:00Z,purchase,p2,{price}"
        for i, price in enumerate(prices15)],
     expected_user=[("u1", D, 1, 4, 10000001, "shoes")],
     expected_campaign=[("organic", "organic", D, 1, 1, 10000001, 0)])

# ---------------------------------------------------------------------------
# X16. Files: two header-only event files, two click files (one header-only,
# one whose only data line equals the header: campaign "campaign_id" is not
# in campaigns.csv, so it is dropped), and a notes.txt in events/ holding a
# valid-looking purchase, which is not a *.csv file and is not read.
# u1: e1 (10:00) and e2 (10:10), organic. (A header line repeated in an
# events file is not used: its ingest_ts would not be valid F1, which the
# rules promise; see the ambiguities in reference/README.md.)
# ---------------------------------------------------------------------------
case("X16_files_and_headers",
     'Input files: "Read every `*.csv` file in `events/`, `clicks/` and `links/`. ... some that hold '
     'only the header." "Every CSV file has a header row."',
     events=[[],
             ["e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,"],
             ["e2,u1,2026-10-03T10:10:00Z,2026-10-03T10:11:00Z,view,p1,"],
             []],
     clicks=[[], [CLICK_HEADER]],
     extra_files={"events/notes.txt": EVENT_HEADER + "\n"
                  "e3,u1,2026-10-03T10:20:00Z,2026-10-03T10:21:00Z,purchase,p1,999\n"},
     expected_user=[("u1", D, 1, 2, 0, None)],
     expected_campaign=[("organic", "organic", D, 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X17. Quoted empty fields are null. Dropped: an empty user_id, an empty
# product_id, an empty event_id (all written ""), clicks with an empty
# click_id or user_id, links with an empty side. u1 keeps e3 only: organic.
# ---------------------------------------------------------------------------
case("X17_quoted_empty_is_null",
     'CSV dialect: "`""` is an empty field. ... An empty field means null." Events step 3: "Drop '
     'the event unless `user_id` is non-empty ... and `product_id` is non-empty."',
     events=['e1,"",2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,',
             'e2,u1,2026-10-03T10:05:00Z,2026-10-03T10:06:00Z,view,"",',
             '"",u1,2026-10-03T10:10:00Z,2026-10-03T10:11:00Z,view,p1,',
             "e3,u1,2026-10-03T10:15:00Z,2026-10-03T10:16:00Z,view,p1,"],
     clicks=['"",u1,c_1,2026-10-03T09:00:00Z',
             'k2,"",c_1,2026-10-03T09:00:00Z'],
     links=['"",u1', 'u1,""'],
     expected_user=[("u1", D, 1, 1, 0, None)],
     expected_campaign=[("organic", "organic", D, 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X18. The click window's start, written with offsets.
#   u1's session starts Oct 1 00:00 UTC (written 2026-09-30T14:00:00-10:00).
#   Its click at 2026-09-24T09:00:00+09:00 is Sep 24 00:00 UTC, exactly
#   window_start - 7 days: in the click window, and exactly 7 days before.
#   u2's click at 08:59:59+09:00 is one second before the click window.
# ---------------------------------------------------------------------------
case("X18_click_window_offsets",
     'Timestamps: "A click\'s `ts` must satisfy `window_start - 7 days <= ts < window_end` ... A '
     'click at exactly `window_start - 7 days` is in its window".',
     events=["e1,u1,2026-09-30T14:00:00-10:00,2026-10-01T00:01:00Z,view,p1,",
             "e2,u2,2026-10-01T00:00:00Z,2026-10-01T00:01:00Z,view,p1,"],
     clicks=["k1,u1,c_1,2026-09-24T09:00:00+09:00",
             "k2,u2,c_1,2026-09-24T08:59:59+09:00"],
     expected_user=[("u1", "2026-10-01", 1, 1, 0, None), ("u2", "2026-10-01", 1, 1, 0, None)],
     expected_campaign=[("c_1", "search", "2026-10-01", 1, 0, 0, 1),
                        ("organic", "organic", "2026-10-01", 1, 0, 0, 0)])

# ---------------------------------------------------------------------------
# X19. A shared device ID links three people into one, named by the device
# ID "d1" (d < u), which appears in no event. Their views at 10:00 (u1),
# 10:20 (u2) and 10:45 (u3) interleave into one session; u2's click for c_1
# at 09:00 credits it.
# ---------------------------------------------------------------------------
case("X19_shared_device",
     'Linked IDs step 2: "Every remaining line joins its two IDs into one person, and joining is '
     'transitive"; Events step 5: "Sessionize each person\'s surviving events, all of their IDs together".',
     events=["e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,",
             "e2,u2,2026-10-03T10:20:00Z,2026-10-03T10:21:00Z,view,p1,",
             "e3,u3,2026-10-03T10:45:00Z,2026-10-03T10:46:00Z,view,p1,"],
     clicks=["k1,u2,c_1,2026-10-03T09:00:00Z"],
     links=["u1,d1", "d1,u2", "u3,d1"],
     expected_user=[("d1", D, 1, 3, 0, None)],
     expected_campaign=[("c_1", "search", D, 1, 0, 0, 1)])


if __name__ == "__main__":
    build()
