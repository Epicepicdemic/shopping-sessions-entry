#!/usr/bin/env python3
"""Pair fresh-output process runs and record elapsed time, CPU time and peak RSS."""
import argparse
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time


def run(binary, dataset, parent, options, keep):
    env = os.environ | {"W3_TIMING": "1", "W4_TIMING": "1"} | dict(v.split("=", 1) for v in options)
    with tempfile.TemporaryDirectory(prefix="bench-", dir=parent) as folder:
        output = Path(folder) / "output"
        log = Path(folder) / "stderr"
        with log.open("wb") as stream:
            actions = [
                (os.POSIX_SPAWN_DUP2, stream.fileno(), 1),
                (os.POSIX_SPAWN_DUP2, stream.fileno(), 2),
            ]
            start = time.monotonic_ns()
            pid = os.posix_spawn(binary, [str(binary), str(dataset), str(output)], env, file_actions=actions)
            _, status, usage = os.wait4(pid, 0)
            elapsed = (time.monotonic_ns() - start) * 1e-9
        stderr = log.read_text()
        if status:
            raise RuntimeError(f"{binary} exited {os.waitstatus_to_exitcode(status)}\n{stderr}")
        size = sum(path.stat().st_size for path in output.glob("*/*.parquet"))
        if keep is not None:
            output.rename(keep)
        return {
            "seconds": elapsed,
            "max_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
            "user_seconds": usage.ru_utime,
            "system_seconds": usage.ru_stime,
            "output_bytes": size,
            "phases": stderr.strip().splitlines(),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--temp-root", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--control", type=Path, required=True)
    parser.add_argument("--candidate-env", action="append", default=[])
    parser.add_argument("--control-env", action="append", default=[])
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--keep-first", type=Path, help="save the first measured pair's tables here")
    args = parser.parse_args()
    args.temp_root.mkdir(parents=True, exist_ok=True)
    if args.keep_first:
        args.keep_first.mkdir(parents=True, exist_ok=False)
    settings = {
        "candidate": (args.candidate.resolve(), args.candidate_env),
        "control": (args.control.resolve(), args.control_env),
    }
    records = {"candidate": [], "control": []}
    for trial in range(args.warmup + args.pairs):
        order = ("control", "candidate") if trial % 2 == 0 else ("candidate", "control")
        for kind in order:
            binary, opts = settings[kind]
            keep = args.keep_first / kind if args.keep_first and trial == args.warmup else None
            info = run(binary, args.dataset.resolve(), args.temp_root.resolve(), opts, keep)
            warmup = trial < args.warmup
            print(json.dumps({"trial": trial, "kind": kind, "warmup": warmup, **info}), flush=True)
            if not warmup:
                records[kind].append(info)
    summary = {
        kind: {
            "median_seconds": statistics.median(r["seconds"] for r in data),
            "min_seconds": min(r["seconds"] for r in data),
            "max_seconds": max(r["seconds"] for r in data),
            "peak_rss_bytes": max(r["max_rss_bytes"] for r in data),
        }
        for kind, data in records.items()
    }
    summary["median_paired_speedup"] = statistics.median(
        c["seconds"] / a["seconds"] for c, a in zip(records["control"], records["candidate"])
    )
    print(json.dumps({"summary": summary}), flush=True)


if __name__ == "__main__":
    main()
