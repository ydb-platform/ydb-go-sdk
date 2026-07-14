#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  collect-slo-coverage.sh \
    --output-dir <path> \
    --workload <name>

Collects Go coverage data from the stopped SLO workload-current container
and converts it to the legacy text format for Codecov upload.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

output_dir=""
workload=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      output_dir="${2:-}"
      shift 2
      ;;
    --workload)
      workload="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1 (use --help)"
      ;;
  esac
done

if [[ -z "$output_dir" || -z "$workload" ]]; then
  usage
  die "Incomplete argument set"
fi

mkdir -p "$output_dir"

container="ydb-workload-current"
raw_dir="${output_dir}/${workload}-raw"
profile="${output_dir}/${workload}.txt"

if ! docker inspect "$container" >/dev/null 2>&1; then
  echo "Container ${container} not found, skipping coverage collection"
  exit 0
fi

rm -rf "$raw_dir"
mkdir -p "$raw_dir"

if ! docker cp "${container}:/coverage/." "$raw_dir/" 2>/dev/null; then
  echo "Coverage data not found in ${container}, skipping"
  exit 0
fi

if ! compgen -G "${raw_dir}/covmeta.*" >/dev/null; then
  echo "Coverage directory in ${container} is empty, skipping"
  exit 0
fi

echo "Converting coverage data for ${workload}..."
if ! go tool covdata textfmt -i="$raw_dir" -o "$profile"; then
  die "Failed to convert coverage data for ${workload}"
fi

if [[ ! -s "$profile" ]]; then
  echo "Coverage profile for ${workload} is empty, skipping"
  rm -f "$profile"
  exit 0
fi

echo "Coverage profile written to ${profile}"
