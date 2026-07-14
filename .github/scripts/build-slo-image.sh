#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  build-slo-image.sh \
    --context <path> \
    --tag <docker-tag> \
    --src-path <sdk-path> \
    [--enable-coverage] \
    [--fallback-image <docker-tag>]

Options:
  --context          Docker build context directory (e.g. $GITHUB_WORKSPACE/current).
  --tag              Docker image tag to build (e.g. ydb-app-current).
  --src-path         Value for Docker build arg SRC_PATH (e.g. native/table).
  --enable-coverage  Build an instrumented workload binary that emits SDK coverage.
  --fallback-image   Image tag to return if initial Docker image build fails
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

context_dir=""
dockerfile="tests/slo/Dockerfile"
tag=""
src_path=""
enable_coverage="false"
fallback_image=""

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
    --src-path)
      src_path="${2:-}"
      shift 2
      ;;
    --enable-coverage)
      enable_coverage="true"
      shift
      ;;
    --fallback-image)
      fallback_image="${2:-}"
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

if [[ -z "$context_dir" || -z "$tag" || -z "$src_path" ]]; then
  usage
  die "Incomplete argument set"
fi

[[ -d "$context_dir" ]] || die "--context does not exist: $context_dir"
context_dir="$(cd "$context_dir" && pwd)"

[[ -f "$context_dir/$dockerfile" ]] || die "Dockerfile not found: $context_dir/$dockerfile"

echo "Building SLO image..."
echo "  TAG:             $tag"
echo "  SRC_PATH:        $src_path"
echo "  ENABLE_COVERAGE: $enable_coverage"

(
  set +e
  cd "$context_dir"
  docker build -t "$tag" \
    --platform linux/amd64 \
    --build-arg "SRC_PATH=$src_path" \
    --build-arg "ENABLE_COVERAGE=$enable_coverage" \
    -f "$dockerfile" .
  exit_code=$?
  echo "Docker build exit code: $exit_code"
  if [ $exit_code -ne 0 ]; then
    if [[ -z "$fallback_image" ]]; then
      die "Docker build failed and --fallback-image is not set" >&2
    fi

    echo "Baseline build failed, using fallback image: $fallback_image"
    docker tag "$fallback_image" "$tag" || die "Fallback docker tag failed"
    exit_code=0
  fi
  exit $exit_code
)
