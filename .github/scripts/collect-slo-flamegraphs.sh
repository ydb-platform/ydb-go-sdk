#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  collect-slo-flamegraphs.sh \
    --output-dir <path> \
    --workload <name>

Collects CPU and heap profiles from stopped SLO workload containers and
renders flamegraphs as SVG files.
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

collect_variant() {
  local variant="$1"
  local container="ydb-workload-${variant}"
  local binary_path="${output_dir}/${variant}-binary"
  local cpu_profile="${output_dir}/${variant}-cpu.prof"
  local heap_profile="${output_dir}/${variant}-heap.prof"
  local cpu_svg="${output_dir}/${workload}-${variant}-cpu.svg"
  local heap_svg="${output_dir}/${workload}-${variant}-heap.svg"

  if ! docker inspect "$container" >/dev/null 2>&1; then
    echo "Container ${container} not found, skipping ${variant} flamegraphs"
    return 0
  fi

  echo "Collecting profiles from ${container}..."

  docker cp "${container}:/slo-go-workload" "$binary_path" 2>/dev/null || {
    echo "Failed to copy binary from ${container}, skipping ${variant} flamegraphs"
    return 0
  }
  chmod +x "$binary_path"

  if docker cp "${container}:/profiles/cpu.prof" "$cpu_profile" 2>/dev/null; then
    echo "Rendering ${cpu_svg}..."
    go tool pprof -svg -output="$cpu_svg" "$binary_path" "$cpu_profile"
  else
    echo "CPU profile not found in ${container}, skipping ${variant} CPU flamegraph"
  fi

  if docker cp "${container}:/profiles/heap.prof" "$heap_profile" 2>/dev/null; then
    echo "Rendering ${heap_svg}..."
    go tool pprof -svg -output="$heap_svg" "$binary_path" "$heap_profile"
  else
    echo "Heap profile not found in ${container}, skipping ${variant} heap flamegraph"
  fi
}

collect_variant current
collect_variant baseline

if ! compgen -G "${output_dir}/*.svg" >/dev/null; then
  echo "No flamegraph SVG files were generated"
  exit 0
fi

echo "Flamegraphs written to ${output_dir}"
