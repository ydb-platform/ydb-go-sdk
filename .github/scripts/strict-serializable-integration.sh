#!/usr/bin/env bash
set -euo pipefail

image="${YDB_STRICT_TEST_IMAGE:-ydbplatform/local-ydb:nightly}"
host_port="${YDB_STRICT_TEST_PORT:-2136}"
container_name="ydb-ssrw-$$"
config_dir="$(mktemp -d "$PWD/.ydb-ssrw.XXXXXXXX")"
config_file="$config_dir/config.yaml"

cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
  rm -rf "$config_dir"
}
trap cleanup EXIT

wait_for_health() {
  local health=""
  for _ in {1..45}; do
    health="$(docker inspect --format '{{.State.Health.Status}}' "$container_name" 2>/dev/null || true)"
    if [ "$health" = healthy ]; then
      return
    fi
    sleep 2
  done
  docker logs --tail 80 "$container_name"
  return 1
}

# Let local-ydb generate a config for this exact image, then start a fresh
# database with StrictSerializableIsolation enabled from the beginning.
docker run -d --rm --name "$container_name" \
  -p "$host_port:2136" \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  -e YDB_GRPC_ENABLE_TLS=false \
  "$image" >/dev/null
wait_for_health
docker cp "$container_name:/ydb_data/cluster/kikimr_configs/config.yaml" "$config_file"

docker stop "$container_name" >/dev/null

python3 - "$config_file" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
config = path.read_text()
marker = "table_service_config:\n"
if config.count(marker) != 1:
    raise SystemExit("expected one table_service_config section")
path.write_text(config.replace(marker, marker + "  enable_strict_serializable_isolation: true\n", 1))
PY

docker run -d --rm --name "$container_name" \
  -p "$host_port:2136" \
  -v "$config_file:/ssrw-config.yaml:ro" \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  -e YDB_GRPC_ENABLE_TLS=false \
  "$image" --config-path /ssrw-config.yaml >/dev/null
wait_for_health

YDB_CONNECTION_STRING="grpc://localhost:$host_port/local" \
YDB_STRICT_SERIALIZABLE_ENABLED=1 \
go test -race -tags integration -run '^TestQueryStrictSerializable' -count=1 -timeout=5m ./tests/integration
