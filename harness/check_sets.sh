#!/bin/sh
# Run the harness on every data set under SETS_DIR, comparing CMD with the
# reference (whose outputs are cached under CACHE_DIR/<set name>).
#   harness/check_sets.sh SETS_DIR CACHE_DIR -- CMD ...
# Prints one RESULT line per set and exits 1 if any set differs.
set -u
SETS=$1; CACHE=$2; shift 2
[ "$1" = "--" ] && shift
HERE=$(cd "$(dirname "$0")" && pwd)
PY=${PYTHON:-python3}
status=0
for set in "$SETS"/*/; do
  name=$(basename "$set")
  [ -f "$set/manifest.json" ] || continue
  result=$("$PY" "$HERE/check.py" data "$set" --reference-cache "$CACHE/$name" -- "$@" 2>&1)
  code=$?
  echo "$result" | grep -E '^(user_daily|campaign_daily|implementation: exit)' | sed "s|^|$name  |"
  echo "$name  $(echo "$result" | grep '^RESULT' || echo "RESULT: ERROR (exit $code)")"
  [ $code -eq 0 ] || status=1
done
exit $status
