#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  collect-slo-flamegraphs.sh \
    --output-dir <path> \
    --workload <name>

Collects CPU and heap profiles from stopped SLO workload containers and
renders interactive pprof flame graphs as standalone HTML files.
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

render_flamegraph_html() {
  local binary="$1"
  local profile="$2"
  local output="$3"
  local port pprof_pid

  port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')"
  go tool pprof -http="127.0.0.1:${port}" -no_browser "$binary" "$profile" >/dev/null 2>&1 &
  pprof_pid=$!

  for _ in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${port}/ui/" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done

  if ! curl -sf "http://127.0.0.1:${port}/ui/flamegraph" -o "$output"; then
    kill "$pprof_pid" 2>/dev/null || true
    wait "$pprof_pid" 2>/dev/null || true
    return 1
  fi

  kill "$pprof_pid" 2>/dev/null || true
  wait "$pprof_pid" 2>/dev/null || true
}

collect_variant() {
  local variant="$1"
  local container="ydb-workload-${variant}"
  local binary_path="${output_dir}/${workload}-${variant}-binary"
  local cpu_profile="${output_dir}/${workload}-${variant}-cpu.prof"
  local heap_profile="${output_dir}/${workload}-${variant}-heap.prof"
  local cpu_html="${output_dir}/${workload}-${variant}-cpu.html"
  local heap_html="${output_dir}/${workload}-${variant}-heap.html"

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
    echo "Rendering ${cpu_html}..."
    render_flamegraph_html "$binary_path" "$cpu_profile" "$cpu_html" || \
      echo "Failed to render ${cpu_html}"
  else
    echo "CPU profile not found in ${container}, skipping ${variant} CPU flamegraph"
  fi

  if docker cp "${container}:/profiles/heap.prof" "$heap_profile" 2>/dev/null; then
    echo "Rendering ${heap_html}..."
    render_flamegraph_html "$binary_path" "$heap_profile" "$heap_html" || \
      echo "Failed to render ${heap_html}"
  else
    echo "Heap profile not found in ${container}, skipping ${variant} heap flamegraph"
  fi

  rm -f "$binary_path" "$cpu_profile" "$heap_profile"
}

collect_variant current
collect_variant baseline

if ! compgen -G "${output_dir}/${workload}-*-cpu.html" >/dev/null && \
   ! compgen -G "${output_dir}/${workload}-*-heap.html" >/dev/null; then
  echo "No flamegraph HTML files were generated"
  exit 0
fi

echo "Flamegraphs written to ${output_dir}"
