#!/usr/bin/env python3
"""
Writes a scrambled copy of a data set that must give exactly the same output.

rules.md makes the answer independent of how the rows are laid out: the
number and size of the files, the order of the lines, and which fields are
wrapped in quotes. This copies DATA_DIR to OUT_DIR with every events/,
clicks/ and links/ line moved to a random file in a random order, a new
number of files (some holding only the header), and the quotes on every
well-formed line redrawn at random. Malformed lines are copied byte for
byte, so they stay malformed. products.csv, campaigns.csv and manifest.json
are copied unchanged.

    scramble.py DATA_DIR OUT_DIR [--seed N]

Comparing an implementation's output on OUT_DIR with its output (or the
reference's) on DATA_DIR tests order and quoting independence.
"""
import os
import random
import shutil
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "reference"))
import reference as ref  # noqa: E402

COLUMNS = {"events": ref.EVENT_COLUMNS, "clicks": ref.CLICK_COLUMNS, "links": ref.LINK_COLUMNS}


def requote(line, column_count, rng):
    values = ref.parse_line(line, column_count)
    if values is None:
        return line  # malformed: keep it exactly as it was
    return b",".join(b'"' + v + b'"' if rng.random() < 0.3 else v for v in values)


def scramble(data_dir, out_dir, seed):
    rng = random.Random(seed)
    if os.path.exists(out_dir):
        raise SystemExit(f"{out_dir} exists; remove it first")
    os.makedirs(out_dir)
    for name in ("products.csv", "campaigns.csv", "manifest.json"):
        shutil.copyfile(os.path.join(data_dir, name), os.path.join(out_dir, name))
    for folder, column_count in COLUMNS.items():
        header = None
        lines = []
        for path in ref.csv_files(os.path.join(data_dir, folder)):
            with open(path, "rb") as handle:
                first = handle.readline()
            header = header or first
            lines.extend(requote(line, column_count, rng) for line in ref.data_lines(path))
        rng.shuffle(lines)
        file_count = rng.randint(1, 40)
        files = [[] for _ in range(file_count)]
        for line in lines:
            # Skewed sizes; a few files stay header-only.
            files[min(int(rng.expovariate(1.0) * file_count / 3), file_count - 1)].append(line)
        os.makedirs(os.path.join(out_dir, folder))
        for index, part in enumerate(files):
            with open(os.path.join(out_dir, folder, f"scrambled-{index:03d}.csv"), "wb") as handle:
                handle.write(header if header.endswith(b"\n") else header + b"\n")
                for line in part:
                    handle.write(line + b"\n")
    print(f"wrote {out_dir}")


def main(argv):
    arguments = list(argv[1:])
    seed = 0
    if "--seed" in arguments:
        at = arguments.index("--seed")
        seed = int(arguments[at + 1])
        del arguments[at:at + 2]
    if len(arguments) != 2:
        print(__doc__)
        return 2
    scramble(arguments[0], arguments[1], seed)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
