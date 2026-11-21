#!/usr/bin/env python3
"""Measure a DATA_DIR/OUTPUT_DIR command. Keep logs and outputs outside git.
Example: python fast/tools/bench.py DATA WORK --label sort --repeat 3 -- /usr/bin/time -l BIN
Pass --env NAME=VALUE repeatedly for algorithm switches. No cache flushing is done.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time


def measure(data, work, label, command, environment=None):
    data, work = Path(data).resolve(), Path(work).resolve()
    run_dir = work/label
    run_dir.mkdir(parents=True, exist_ok=False)
    output, scratch = run_dir/'output', run_dir/'scratch'
    output.mkdir(); scratch.mkdir()
    env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch),
               NCPU='8', MEM_GB='24', SHOPPING_TIMING='1')
    env.update(environment or {})
    start = time.perf_counter()
    result = subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    wall = time.perf_counter()-start
    (run_dir/'stderr.log').write_text(result.stderr)
    (run_dir/'stdout.log').write_text(result.stdout)
    stages_match = re.search(r'^timing_ms: (.+)$', result.stderr, re.M)
    stages = {key: float(value) for key,value in re.findall(r'(\w+)=([\d.]+)', stages_match[1])} if stages_match else {}
    hwm = re.search(r'VmHWM:\s*(\d+)\s*kB',result.stderr)
    mac = re.search(r'^\s*(\d+)\s+maximum resident set size',result.stderr,re.M)
    footprint = re.search(r'^\s*(\d+)\s+peak memory footprint',result.stderr,re.M)
    rss = int(hwm[1])*1024 if hwm else int(mac[1]) if mac else None
    record = dict(label=label, data=str(data), command=command, environment=environment or {},
        platform=platform.platform(), wall_seconds=wall, stages_ms=stages, peak_rss_bytes=rss,
        mac_peak_footprint_bytes=int(footprint[1]) if footprint else None, exit_code=result.returncode,
        output=str(output), measured_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
    (run_dir/'result.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps(record),flush=True)
    if result.returncode: raise RuntimeError(f'{label}: exit {result.returncode}; see {run_dir}/stderr.log')
    return record


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('data',type=Path); parser.add_argument('work',type=Path)
    parser.add_argument('--label',required=True); parser.add_argument('--repeat',type=int,default=1)
    parser.add_argument('--env',action='append',default=[])
    args,command=parser.parse_known_args()
    if command[:1] == ['--']: command=command[1:]
    if not command: parser.error('a command after -- is required')
    env=dict(item.split('=',1) for item in args.env)
    for i in range(args.repeat):
        measure(args.data,args.work,f'{args.label}-r{i+1}',command,env)

if __name__=='__main__': main()
