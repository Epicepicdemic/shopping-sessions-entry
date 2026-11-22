#!/usr/bin/env python3
"""Alternate restricted linux/amd64 containers and retain one exactness pair."""

import argparse
import json
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def run(image, data, parent, env, keep, cpus):
    with tempfile.TemporaryDirectory(prefix="docker-", dir=parent) as tmp:
        output = Path(tmp) / "output"
        output.mkdir(mode=0o777)
        output.chmod(0o777)
        command = [
            "docker", "run", "--rm", "--platform", "linux/amd64", "--read-only", "--network", "none",
            "--user", "1000:1000", "--cpus", cpus, "--memory", "12g", "--memory-swap", "12g",
            "--mount", f"type=bind,src={data},dst=/data,readonly",
            "--mount", f"type=bind,src={output},dst=/output",
            "-e", "W4_TIMING=1", "-e", "W5_TIMING=1",
        ]
        for variable in env:
            command.extend(["-e", variable])
        start = time.perf_counter()
        result = subprocess.run([*command, image], capture_output=True, text=True)
        host = time.perf_counter() - start
        if result.returncode:
            raise RuntimeError(f"{image} exited {result.returncode}\n{result.stdout}{result.stderr}")
        records = result.stderr.splitlines()
        phase = next(line for line in records if line.startswith(("w4_ms ", "w5_ms ")))
        stats = next(line for line in records if " peak_rss_bytes=" in line)
        phases = dict(item.split("=", 1) for item in phase.split()[1:])
        fields = dict(item.split("=", 1) for item in stats.split()[1:])
        cgroup = dict(line.split(" ", 1)[1].split("=", 1) for line in records
                      if line.startswith(("w4_cgroup ", "w5_cgroup ")))
        if keep is not None:
            output.rename(keep)
        return {"host_seconds": host, "engine_seconds": float(phases["total"]) / 1000,
                "rss_bytes": int(fields["peak_rss_bytes"]), "cgroup_peak_bytes": int(cgroup["memory.peak"]),
                "phases": records}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--temp-root", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--control", required=True)
    parser.add_argument("--candidate-env", action="append", default=[])
    parser.add_argument("--control-env", action="append", default=[])
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--cpus", default="8")
    parser.add_argument("--keep-first", type=Path)
    args = parser.parse_args()
    parent = args.temp_root.resolve()
    parent.mkdir(parents=True, exist_ok=True)
    if args.keep_first:
        args.keep_first.mkdir(parents=True)
    engines = {"control": (args.control, args.control_env), "candidate": (args.candidate, args.candidate_env)}
    samples = {key: [] for key in engines}
    for pair in range(args.pairs + args.warmup):
        order = tuple(engines) if pair % 2 == 0 else tuple(reversed(engines))
        for kind in order:
            image, env = engines[kind]
            keep = args.keep_first / kind if args.keep_first and pair == args.warmup else None
            record = run(image, args.dataset.resolve(), parent, env, keep, args.cpus)
            warmup = pair < args.warmup
            print(json.dumps({"trial": pair, "kind": kind, "warmup": warmup, **record}), flush=True)
            if not warmup:
                samples[kind].append(record)
    summary = {kind: {
        "median_host_seconds": statistics.median(r["host_seconds"] for r in records),
        "median_engine_seconds": statistics.median(r["engine_seconds"] for r in records),
        "peak_rss_bytes": max(r["rss_bytes"] for r in records),
        "peak_cgroup_bytes": max(r["cgroup_peak_bytes"] for r in records),
    } for kind, records in samples.items()}
    summary["median_paired_speedup"] = statistics.median(
        c["engine_seconds"] / a["engine_seconds"] for c, a in zip(samples["control"], samples["candidate"]))
    print(json.dumps({"summary": summary}), flush=True)


if __name__ == "__main__":
    main()
