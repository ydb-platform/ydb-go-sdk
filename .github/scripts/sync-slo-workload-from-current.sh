#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  sync-slo-workload-from-current.sh \
    --target <baseline-checkout> \
    --source <current-checkout> \
    --workload-path <path>

Copy SLO workload sources from current into a baseline checkout so the baseline
image runs current workload code against the baseline SDK (via go.mod replace).
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

target_dir=""
source_dir=""
workload_path=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      target_dir="${2:-}"
      shift 2
      ;;
    --source)
      source_dir="${2:-}"
      shift 2
      ;;
    --workload-path)
      workload_path="${2:-}"
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

if [[ -z "$target_dir" || -z "$source_dir" || -z "$workload_path" ]]; then
  usage
  die "Incomplete argument set"
fi

[[ -d "$target_dir" ]] || die "--target does not exist: $target_dir"
[[ -d "$source_dir" ]] || die "--source does not exist: $source_dir"

target_slo="$target_dir/tests/slo"
source_slo="$source_dir/tests/slo"
source_workload="$source_slo/$workload_path"

[[ -d "$source_workload" ]] || die "Workload not found: $source_workload"

echo "Overlaying SLO workload from current onto baseline checkout..."
echo "  SOURCE:  $source_dir"
echo "  TARGET:  $target_dir"
echo "  WORKLOAD: $workload_path"

rm -rf "$target_slo/internal"
cp -a "$source_slo/internal" "$target_slo/internal"

rm -rf "$target_slo/$workload_path"
cp -a "$source_workload" "$target_slo/$workload_path"

cp "$source_slo/go.mod" "$target_slo/go.mod"
cp "$source_slo/go.sum" "$target_slo/go.sum"

echo "SLO workload overlay complete"
