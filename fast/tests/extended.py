#!/usr/bin/env python3
"""Generate additional exactness cases outside the repo and run the unchanged harness.
Usage: python fast/tests/extended.py WORK_DIR -- COMMAND ...
Requires the harness's pyarrow environment. WORK_DIR must be outside the repo.
"""
import csv
import datetime as dt
import json
import hashlib
from pathlib import Path
import subprocess
import sys

repo = Path(__file__).resolve().parents[2]
work = Path(sys.argv[1]).resolve()
command = sys.argv[sys.argv.index('--') + 1:]
headers = {
    'products.csv': ['product_id', 'category'], 'campaigns.csv': ['campaign_id', 'channel'],
    'events/part.csv': ['event_id','user_id','ts','ingest_ts','event_type','product_id','price_cents'],
    'clicks/part.csv': ['click_id','user_id','campaign_id','ts'],
    'links/part.csv': ['user_id_a','user_id_b'],
}
def write_case(name, products, campaigns, events, clicks=(), links=(), window=None):
    dest = work / name / 'input'
    data = [products, campaigns, events, clicks, links]
    for (file, header), rows in zip(headers.items(), data):
        path = dest / file
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', newline='') as f:
            out = csv.writer(f, lineterminator='\n', quoting=csv.QUOTE_ALL)
            out.writerow(header); out.writerows(rows)
    manifest = window or {'window_start':'2026-10-01T00:00:00Z','window_end':'2026-10-15T00:00:00Z'}
    # Escaped keys and digits exercise legal JSON beyond the generator's formatting.
    text = json.dumps(manifest).replace('window_start', r'window_\u0073tart').replace('2026', r'\u0032026')
    (dest/'manifest.json').write_text(text)
    return name

cases=[]
cases.append(write_case('catalog-over-u16',
    ((f'product-{i}',f'category-{i:06}') for i in range(65540)),
    ((f'campaign-{i}','email') for i in range(65540)),
    [('e0','u','2026-10-02T00:00:00Z','2026-10-02T00:00:00Z','purchase','product-65539','100'),
     ('e1','u','2026-10-02T01:00:00Z','2026-10-02T01:00:00Z','purchase','product-65538','200')],
    [('k','u','campaign-65539','2026-10-01T23:00:00Z')]))
# >42 days, distinct millisecond boundaries, ancient ingest order, and a huge F3.
cases.append(write_case('wide-window-and-ingest', [('p','cat')], [(f'c{i}','channel') for i in range(40)],
    [('e','u','2030-01-01T00:00:00Z','0100-01-01T00:00:00Z','purchase','p','00010000000'),
     ('e','other','2030-01-01T00:00:00Z','2029-01-01T00:00:00Z','purchase','p','1'),
     ('f','u','2119-12-31T23:59:59Z','2119-12-31T23:59:59Z','view','p','ignored'),
     ('bad','u','1844674407370955161600000000000000000','2030-01-01T00:00:00Z','purchase','p','99')],
    [('k','u','c39','2030-01-01T00:00:00Z')],
    window={'window_start':'2020-01-01T00:00:00Z','window_end':'2120-01-01T00:00:00Z'}))
start = dt.datetime(2026,10,1,23,59,59,999000,tzinfo=dt.timezone.utc)
base = int(start.timestamp()*1000)
long_id='é /'+'x'*4096
links=[(long_id, 'alias-0')]+[(f'alias-{i}',f'alias-{i+1}') for i in range(300)]
links.append((' A linked but unused name', 'alias-300'))
events=[]
for i in range(700):
    offset=(i//4)*5400001+(i%4)
    ts=base+offset
    ingest=dt.datetime.fromtimestamp(ts/1000,dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    events.append((f'event {i}',long_id if i%2 else f'alias-{i%301}',str(ts),ingest,
                   'purchase' if i%3==0 else 'view','p',str(i+1)))
# Latest timestamp tie must choose the bytewise smallest long ID across fallback/packed IDs.
clicks=[('packed','alias-0','c0',str(base)), (' A long click id '+long_id,'alias-300','c1',str(base))]
clicks += [(f'click {i}',long_id,f'c{i%3}',str(base-604800000+i*1800000)) for i in range(500)]
cases.append(write_case('bins-long-ids-and-assists',[('p','cat')],[(f'c{i}','email') for i in range(3)],events,clicks,links))

for name,window in [
    ('empty-window', {'window_start':'2026-10-01T00:00:00Z','window_end':'2026-10-01T00:00:00Z'}),
    ('reversed-window', {'window_start':'2026-10-14T00:00:00Z','window_end':'2026-10-01T00:00:00Z'}),
    ('full-i64-window', {'window_start':'0000-01-01T00:00:00Z','window_end':'9223372036854775807'}),
]:
    cases.append(write_case(name,[('p','cat')],[('c','email')],
        [('e','u','2026-10-02T00:00:00Z','2026-10-02T00:00:00Z','purchase','p','99')],
        [('k','u','c','2026-10-01T23:00:00Z')],window=window))

status=0
for case in cases:
    digest=hashlib.sha256()
    for path in sorted((work/case/'input').rglob('*')):
        if path.is_file():
            digest.update(str(path.relative_to(work/case/'input')).encode())
            digest.update(path.read_bytes())
    cache=work/case/('reference-'+digest.hexdigest()[:16])
    result=subprocess.run([sys.executable,str(repo/'harness/check.py'),'data',str(work/case/'input'),
        '--reference-cache',str(cache),'--',*command])
    print(f'EXTENDED {case}: exit {result.returncode}',flush=True)
    status |= result.returncode
raise SystemExit(status)
