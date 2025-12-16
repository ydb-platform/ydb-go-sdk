#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  build-slo-image.sh \
    --context <path> \
    --tag <docker-tag> \
    --src-path <sdk-path> \
    --job-name <job-name> \
    --ref <git-ref>

Options:
  --context     Docker build context directory (e.g. $GITHUB_WORKSPACE/current).
  --tag         Docker image tag to build (e.g. ydb-app-current).
  --src-path    Value for Docker build arg SRC_PATH (e.g. native/table).
  --job-name    Value for Docker build arg JOB_NAME (e.g. native-table).
  --ref         Value for Docker build arg REF (e.g. branch name / sha).
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

context_dir=""
dockerfile="tests/slo/Dockerfile"
tag=""
ref=""
src_path=""
job_name=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --context)
      context_dir="${2:-}"
      shift 2
      ;;
    --tag)
      tag="${2:-}"
      shift 2
      ;;
    --ref)
      ref="${2:-}"
      shift 2
      ;;
    --src-path)
      src_path="${2:-}"
      shift 2
      ;;
    --job-name)
      job_name="${2:-}"
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

if [[ -z "$context_dir" || -z "$tag" || -z "$src_path" || -z "$job_name" || -z "$ref" ]]; then
  usage
  exit 2
fi

[[ -d "$context_dir" ]] || die "--context does not exist: $context_dir"
context_dir="$(cd "$context_dir" && pwd)"

[[ -f "$context_dir/$dockerfile" ]] || die "Dockerfile not found: $context_dir/$dockerfile"

echo "Building SLO image..."
echo "  TAG:        $tag"
echo "  REF:        $ref"
echo "  SRC_PATH:   $src_path"
echo "  JOB_NAME:   $job_name"

(
  cd "$context_dir"
  docker build -t "$tag" \
    --build-arg "SRC_PATH=$src_path" \
    --build-arg "JOB_NAME=$job_name" \
    --build-arg "REF=$ref" \
    -f "$dockerfile" .
)
