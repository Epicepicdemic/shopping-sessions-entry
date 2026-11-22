#!/usr/bin/env python3
"""Write regression inputs for v5 key representations and the wide fallback."""
import argparse
from pathlib import Path


def write(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as out:
        out.write(header.encode() + b"\n")
        for row in rows:
            out.write(row.encode() + b"\n")


def setup(root, wide=False):
    root.mkdir(parents=True, exist_ok=True)
    end = "2028-05-01T00:00:00Z" if wide else "2028-03-12T00:00:00Z"
    (root / "manifest.json").write_text(
        f'{{"window_start":"2028-02-27T00:00:00Z","window_end":"{end}"}}\n'
    )
    write(root / "campaigns.csv", "campaign_id,channel", ("c_1,email", "c_2,search", " paid ,social"))
    categories = (f"p{i},cat-{i:03d}" for i in range(1, 261)) if wide else ("p1,b", "p2,a")
    write(root / "products.csv", "product_id,category", categories)
    return root


def opaque(root):
    setup(root)
    short_user = "uaB071eM9"
    mid_user = "u abcdefghijklm"
    long_user = "u" + "f" * 32
    other_long = "u" + "f" * 31 + "0"
    much_longer = "e" + "x" * 600_000
    long_event = "e" + "a" * 32
    different_event = "e" + "a" * 31 + "b"
    packed_user = "u0123456789abcdef0123456789abcdef"
    uppercase_user = "u0123456789ABCDEF0123456789ABCDEF"
    header = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
    write(root / "events" / "largest.csv", header, (
        f'{long_event},{long_user},2028-02-28T23:30:00Z,2028-02-28T23:40:00Z,purchase,p1,999',
        f'"{different_event}","{mid_user}",2028-02-28T23:30:00Z,2028-02-28T23:40:00Z,purchase,p1,400',
        f'e2,{short_user},2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,purchase,p2,400',
        f'e3,{long_user},1835392200001,2028-02-29T00:35:00Z,view,p1,ignored',
        f'e4,{long_user},2028-02-29T02:00:00+01:30,2028-02-29T00:31:00Z,view,p1,',
        f'e5,{other_long},2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,purchase,missing,10',
        f'e6,{other_long},2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,purchase,p1,1000\r',
        f'{much_longer},u é,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,cart,p1,',
        'e7,"u\x00",2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,view,p1,',
        'e8,"u\x00A",2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,view,p1,',
        'e9,q"uote,2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,view,p1,',
        f'e{packed_user[1:]},{packed_user},2028-02-29T00:00:00Z,2028-02-29T00:00:01Z,purchase,p1,1234',
        f'e{uppercase_user[1:]},{uppercase_user},2028-02-29T00:00:00Z,2028-02-29T00:00:01Z,view,p1,',
    ))
    write(root / "events" / "first-copy.csv", header, (
        f'"{long_event}",unrelated,bad,2028-02-28T23:35:00Z,view,p1,',
        f'"e{packed_user[1:]}","{packed_user}",2028-02-29T00:00:00Z,2028-02-29T00:00:00Z,purchase,p2,5678',
    ))
    write(root / "links" / "a.csv", "user_id_a,user_id_b", (
        f'{short_user},"{mid_user}"', f'{long_user},{short_user}', 'u é,"u\x00A"',
    ))
    write(root / "links" / "b.csv", "user_id_a,user_id_b", (
        f'" {short_user}",{long_user}', f'{mid_user},{long_user}',
    ))
    first_click = "k" + "0" * 32
    other_click = "k" + "0" * 31 + "a"
    click_header = "click_id,user_id,campaign_id,ts"
    write(root / "clicks" / "a.csv", click_header, (
        f'{other_click},{long_user},c_2,2028-02-28T23:30:00Z',
        f'"{first_click}","{mid_user}",c_1,2028-02-28T23:30:00Z',
        f'k9,{short_user}, paid ,2028-02-21T23:29:59Z',
        f'k10,{short_user}, paid ,2028-02-28T23:30:00Z\r',
        f'k{packed_user[1:]},{packed_user},c_1,2028-02-29T00:00:00Z',
        f'k{uppercase_user[1:]},{packed_user},c_2,2028-02-29T00:00:00Z',
    ))
    write(root / "clicks" / "b.csv", click_header, (
        f'{first_click},{mid_user},"c_1","2028-02-28T23:30:00Z"',
    ))


def wide(root):
    setup(root, wide=True)
    user = " U-é" + "z" * 13
    key = "e" + "d" * 32
    header = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
    write(root / "events" / "a.csv", header, (
        f'{key},{user},2028-04-30T00:00:00Z,2028-04-30T00:01:00Z,purchase,p1,100',
        f'e2,{user},2028-04-30T00:29:59Z,2028-04-30T00:01:00Z,purchase,p259,75',
        f'e3,{user},2028-04-30T00:30:00Z,2028-04-30T00:01:00Z,purchase,p259,25',
    ))
    write(root / "events" / "b.csv", header, (
        f'{key},{user},2028-04-30T00:00:00Z,0001-01-01T00:00:00Z,purchase,p260,300',
    ))
    write(root / "links" / "one.csv", "user_id_a,user_id_b", (f'{user},U',))
    write(root / "clicks" / "a.csv", "click_id,user_id,campaign_id,ts", (
        'click-anything,U,c_2,2028-04-23T00:00:00Z',
    ))


def colliding_events(packed_probe=False):
    """Find colliding literal spellings for the old hash or the new probe."""
    mask = (1 << 64) - 1
    seen = {}
    for i in range(400_000):
        suffix = f"{i:08x}"
        value = int.from_bytes(suffix.encode(), "little")
        product = (value ^ 0x9e3779b97f4a7c15) * 0xd6e8feb86659fd93
        full = (product & mask) ^ (product >> 64)
        low = full & 0xffffffff
        if packed_probe:
            tag = (low >> 26, low & 15, ((full >> 32) ^ (low >> 13) ^ low) & 65535 or 1)
        else:
            tag = low
        if tag in seen:
            return f"e{seen[tag]}", f"e{suffix}"
        seen[tag] = suffix
    raise RuntimeError("no fingerprint collision found")


def boundaries(root):
    setup(root)
    (root / "manifest.json").write_text(
        '{"window_start":"2025-12-31T00:00:00Z","window_end":"2026-01-14T00:00:00Z"}\n'
    )
    first, second = colliding_events()
    third, fourth = colliding_events(packed_probe=True)
    header = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
    write(root / "events" / "a.csv", header, (
        f'{first},uEnd,2025-12-31T00:00:00Z,2025-12-31T00:02:50Z,purchase,p1,400',
        f'{second},"Ua",2026-01-01T00:00:00Z,2026-01-01T00:00:01Z,view,p1,""',
        f'{third},uProbeA,2026-01-01T00:00:00Z,2026-01-01T00:00:01Z,view,p1,',
        f'{fourth},uProbeB,2026-01-01T00:00:00Z,2026-01-01T00:00:01Z,view,p1,',
        *(
            f'eChain-{i},uEnd,2025-12-31T00:{i:02d}:00Z,2025-12-31T00:{i:02d}:01Z,purchase,p2,25'
            for i in range(1, 25)
        ),
        # Across the 64/128-byte classifications and the scalar long-row path.
        *(
            f'eBoundary-{i},"{"u" * i}",2026-01-01T00:00:00Z,2026-01-01T00:59:00Z,view,p1,""'
            for i in (40, 48, 63, 64, 65, 83, 127, 128, 193)
        ),
        'eBroken,uEnd,""2025-12-31T00:00:00Z"",2025-12-31T00:02:50Z,view,p1,',
        'eBroken-2,uEnd,2025-12-31T00:00:00Z,2025-12-31T00:02:50Z,view,p1,"',
        'eBroken-3,uEnd,2025-12-31T00:00:00Z,2025-12-31T00:02:50Z,view,p1,"1",extra',
    ))
    write(root / "events" / "b.csv", header, (
        f'{first},uEnd,2025-12-31T00:00:00Z,2025-12-31T00:02:49Z,purchase,p1,300',
        'eOffset,uSeparate,2025-12-30T20:00:00-04:00,2025-12-31T00:01:00Z,view,p1,',
    ))
    with (root / "events" / "b.csv").open("ab") as out:
        out.write(b'eEOF,uSeparate,"2026-01-01T00:00:00Z",2026-01-01T00:01:00Z,view,p1,""')

    names = ["!root", *(f'chain-{i:03d}' for i in range(310)), "uEnd"]
    write(root / "links" / "first.csv", "user_id_a,user_id_b", (
        f'{names[i + 1]},{names[i]}' for i in range(0, len(names) - 1, 2)
    ))
    write(root / "links" / "second.csv", "user_id_a,user_id_b", (
        *(f'{names[i]},{names[i + 1]}' for i in range(1, len(names) - 1, 2)),
        'chain-005,chain-079', 'uEnd,uEnd',
    ))
    write(root / "clicks" / "one.csv", "click_id,user_id,campaign_id,ts", (
        'kOld,uEnd,c_1,2025-12-23T20:00:00-04:00',
        'kTooOld,uEnd, paid ,2025-12-23T19:59:59-04:00',
        'kOnly,uSeparate,c_1,2025-12-23T20:00:00-04:00',
        *(f'kHistory-{i},chain-005,c_2,2025-12-30T23:59:{i % 60:02d}Z' for i in range(200)),
        'k9,uEnd,c_2,2025-12-31T00:00:00Z',
        'k2,chain-000,c_1,2025-12-31T00:00:00Z',
        '"k2","chain-000",c_1,"2025-12-31T00:00:00Z"',
    ))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    out = parser.parse_args().directory
    opaque(out / "opaque")
    wide(out / "wide")
    boundaries(out / "boundaries")


if __name__ == "__main__":
    main()
