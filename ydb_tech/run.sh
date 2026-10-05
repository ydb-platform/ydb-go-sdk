#!/usr/bin/env bash
set -euo pipefail
examples_root="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(dirname "$examples_root")"
cd "$repo_root"
example_count=0
for entry in "$examples_root"/*/run.sh; do
    [[ -f "$entry" ]] || continue
    example_count=$((example_count + 1))
    printf 'Running %s\n' "${entry%/run.sh}"
    python3 - "$entry" "${YDB_TECH_TIMEOUT_SECONDS:-180}" <<'PYTHON'
import subprocess
import sys

try:
    result = subprocess.run(["bash", sys.argv[1]], timeout=int(sys.argv[2]), check=False)
except subprocess.TimeoutExpired:
    print("Example exceeded its time limit: " + sys.argv[1], file=sys.stderr)
    raise SystemExit(124)
raise SystemExit(result.returncode)
PYTHON
done
if [[ "$example_count" == 0 ]]; then
    printf 'No executable examples found in %s\n' "$examples_root" >&2
    exit 1
fi
