# Running SLO Tests Locally

This guide describes how to run the full SLO test infrastructure on your machine — the same setup used in CI, but without GitHub Actions.

SLO tests exercise the SDK under load while a multi-node YDB cluster experiences failures (node stops, network black holes, rolling restarts). The infrastructure is provided by [ydb-slo-action](https://github.com/ydb-platform/ydb-slo-action); the workload binaries live in this directory.

For workload internals (table schema, metrics, environment variables), see [README.md](README.md).

## Fast development loop (no Docker image build)

During day-to-day development you do **not** need to build a workload Docker image on every change. The workloads are plain Go programs in `tests/slo/`; `go.mod` uses a `replace` directive to point at the local SDK checkout, so SDK changes are picked up immediately.

Run the real multi-node cluster, chaos monkey, and Prometheus — but execute the workload as a local binary with `go run`. Start the infra once, then iterate on code changes.

**1. Start infrastructure without the workload container** (once per session):

```bash
cd ydb-slo-action/deploy
docker compose --profile telemetry --profile chaos up -d
# omit --profile chaos for node-hints workloads
```

Wait until the cluster is ready:

```bash
docker compose ps   # database-readiness should be Exited (0)
```

**2. Run the workload from your SDK checkout:**

```bash
cd /path/to/ydb-go-sdk/tests/slo/native/table

# Prometheus is mapped to a random host port, not 9090
PROM_PORT=$(docker inspect -f '{{(index (index .NetworkSettings.Ports "9090/tcp") 0).HostPort}}' ydb-prometheus)

export YDB_ENDPOINT=grpc://$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ydb-database-1):2136
export YDB_DATABASE=/Root/testdb
export WORKLOAD_NAME=native-table
export WORKLOAD_REF=dev
export WORKLOAD_DURATION=60
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:${PROM_PORT}/api/v1/otlp
export PROMETHEUS_URL=http://127.0.0.1:${PROM_PORT}

go run . -read-rps=100 -write-rps=10
```

The database container IP is reachable from the host on Docker Desktop and most Linux setups. Use `127.0.0.1` (not `localhost`) for Prometheus to avoid IPv6 connection issues.

To run without metrics (no Prometheus required), omit `OTEL_EXPORTER_OTLP_ENDPOINT` and `PROMETHEUS_URL`.

**3. After a code change** — just re-run `go run .` (or `go run . <flags>`). The infra keeps running in the background.

**Tips:**

- Use a short `WORKLOAD_DURATION` (30–120 s) while iterating.
- Watch workload output in the terminal; metrics appear in Prometheus/Grafana without restarting infra.
- Inspect the database via [YDB console from the host](#ydb-console-from-the-host).
- To avoid re-exporting env vars, put them in a shell script or use [direnv](https://direnv.net/).

```bash
# example helper — save as tests/slo/run-dev.sh and chmod +x
#!/usr/bin/env bash
set -euo pipefail
WORKLOAD_DIR="${1:?usage: run-dev.sh <workload-dir> [go-run args...]}"
shift
PROM_PORT=$(docker inspect -f '{{(index (index .NetworkSettings.Ports "9090/tcp") 0).HostPort}}' ydb-prometheus)
YDB_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ydb-database-1)
export YDB_ENDPOINT="grpc://${YDB_IP}:2136"
export YDB_DATABASE=/Root/testdb
export WORKLOAD_NAME=dev WORKLOAD_REF=dev WORKLOAD_DURATION=60
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:${PROM_PORT}/api/v1/otlp"
export PROMETHEUS_URL="http://127.0.0.1:${PROM_PORT}"
cd "$(dirname "$0")/${WORKLOAD_DIR}"
exec go run . "$@"
```

Usage:

```bash
./tests/slo/run-dev.sh native/table -read-rps=100 -write-rps=10
```

### When to build a Docker image

Build the image (steps below) only when you need to verify the **exact CI path** — e.g. before opening a PR with the `SLO` label, or to check that the `Dockerfile` and `linux/amd64` build work correctly.

---

## Full run with Docker workload image (CI-like)

The steps below reproduce the CI pipeline: infrastructure + workload running inside Docker.

## Prerequisites

- **Docker** with **Docker Compose v2** (`docker compose`)
- Enough resources for a YDB cluster (1 storage + 5 database nodes), Prometheus, Grafana, chaos monkey, and one workload container
- **Go 1.24+** (for the fast `go run` loop; also needed to build workload images)

> **Note:** Docker images are built for `linux/amd64`. On Apple Silicon, Docker Desktop handles emulation automatically, but the first build may take a while.

## Overview

```
┌─────────────────────────────────────────────────────────┐
│  ydb-slo-action/deploy (Docker Compose)                 │
│                                                         │
│  ┌──────────┐  ┌─────────────┐  ┌──────────────────┐   │
│  │ YDB      │  │ Prometheus  │  │ Chaos monkey     │   │
│  │ cluster  │  │ + Grafana   │  │ (random faults)  │   │
│  └────┬─────┘  └──────┬──────┘  └────────┬─────────┘   │
│       │               │                   │             │
│       └───────────────┴───────────────────┘             │
│                       │                                 │
│              ┌────────▼────────┐                        │
│              │ workload-current │  (your SDK image)    │
│              └─────────────────┘                        │
└─────────────────────────────────────────────────────────┘
```

Compose uses **profiles** to control which services start:

| Profile            | Services                                      |
|--------------------|-----------------------------------------------|
| _(none)_           | YDB cluster only                              |
| `telemetry`        | Prometheus + Grafana                          |
| `chaos`            | Chaos monkey + network blackhole node         |
| `workload-current` | Your workload container                       |

## Step 1: Clone the infrastructure

```bash
git clone https://github.com/ydb-platform/ydb-slo-action.git
cd ydb-slo-action/deploy
cp env.example .env
```

## Step 2: Build a workload image

From the **root of this repository** (`ydb-go-sdk`), build the Docker image for the workload you want to test:

```bash
bash .github/scripts/build-slo-image.sh \
  --context . \
  --tag ydb-app-current \
  --src-path native/table
```

`SRC_PATH` selects which workload to build. Available workloads (matching CI):

| Workload                        | `SRC_PATH`                        |
|---------------------------------|-----------------------------------|
| database/sql table              | `database/sql/table`              |
| database/sql query              | `database/sql/query`              |
| native query                    | `native/query`                    |
| native table                    | `native/table`                    |
| native table over query service | `native/table-over-query-service` |
| native bulk upsert              | `native/bulk-upsert`              |
| native table node hints         | `native/table-node-hints`         |
| native query node hints         | `native/query-node-hints`         |

The first build downloads Go dependencies inside Docker and can take 10–15 minutes.

## Step 3: Configure `.env`

Edit `ydb-slo-action/deploy/.env`:

```bash
WORKLOAD_DURATION=600
WORKLOAD_CURRENT_IMAGE=ydb-app-current
WORKLOAD_NAME=native-table
```

| Variable                  | Default               | Description                                      |
|---------------------------|-----------------------|--------------------------------------------------|
| `WORKLOAD_DURATION`       | `600`                 | Run time in seconds (`0` = unlimited)            |
| `WORKLOAD_CURRENT_IMAGE`  | `ydb-workload-current`| Docker image tag built in Step 2                 |
| `WORKLOAD_CURRENT_COMMAND`| _(empty)_             | Extra CLI flags passed to the workload binary    |
| `WORKLOAD_NAME`           | `kv`                  | Label used in metrics and table path prefix      |

YDB and Prometheus endpoints are pre-configured in `compose.yml` and should not be overridden.

## Step 4: Start the infrastructure

Run all commands from `ydb-slo-action/deploy/`.

**Full SLO run** (YDB + telemetry + chaos + workload):

```bash
docker compose --profile telemetry --profile chaos --profile workload-current up
```

Add `-d` to run in the background.

**Without chaos** (useful for node-hints workloads, which disable chaos in CI):

```bash
docker compose --profile telemetry --profile workload-current up
```

**Infrastructure only** (no workload — useful to verify the cluster starts):

```bash
docker compose --profile telemetry --profile chaos up -d
```

The workload container receives these environment variables automatically:

| Variable                        | Value                                              |
|---------------------------------|----------------------------------------------------|
| `YDB_ENDPOINT`                  | `grpc://ydb:2136`                                  |
| `YDB_DATABASE`                  | `/Root/testdb`                                     |
| `OTEL_EXPORTER_OTLP_ENDPOINT`   | `http://ydb-prometheus:9090/api/v1/otlp`           |
| `PROMETHEUS_URL`                | `http://ydb-prometheus:9090`                       |
| `WORKLOAD_DURATION`             | from `.env`                                        |

## Workload-specific options

Pass extra flags via `WORKLOAD_CURRENT_COMMAND` in `.env` or on the command line:

```bash
# native/bulk-upsert
WORKLOAD_CURRENT_COMMAND="-batch-size=10" \
  docker compose --profile telemetry --profile chaos --profile workload-current up

# native/table-node-hints or native/query-node-hints
WORKLOAD_CURRENT_COMMAND="-min-partition-count=10" \
  docker compose --profile telemetry --profile workload-current up
```

## Step 5: Observe results

**Workload logs:**

```bash
docker compose logs -f workload-current
```

**Prometheus** — find the mapped host port (usually not `9090`):

```bash
PROM_PORT=$(docker inspect -f '{{(index (index .NetworkSettings.Ports "9090/tcp") 0).HostPort}}' ydb-prometheus)
curl "http://127.0.0.1:${PROM_PORT}/api/v1/query?query=sdk_operations_total"
```

**Grafana**:

```bash
GRAFANA_PORT=$(docker inspect -f '{{(index (index .NetworkSettings.Ports "3000/tcp") 0).HostPort}}' ydb-grafana)
open "http://127.0.0.1:${GRAFANA_PORT}"
```

Anonymous admin access is enabled by default.

A pre-built dashboard JSON is available in [slo-tests](https://github.com/ydb-platform/slo-tests/blob/main/k8s/helms/grafana.yaml#L69) if you want to import it manually.

**Useful PromQL queries:**

```promql
# Operation rate
rate(sdk_operations_total[1m])

# Read latency p99
sdk_operation_latency_p99_seconds{operation_type="read"}

# Retry attempts
rate(sdk_retry_attempts_total[1m])
```

## YDB console from the host

While the cluster is up:

```bash
MON_PORT=$(docker inspect -f '{{(index (index .NetworkSettings.Ports "8765/tcp") 0).HostPort}}' ydb-storage-1)
open "http://127.0.0.1:${MON_PORT}"
```

## Step 6: Tear down

```bash
docker compose --profile telemetry --profile chaos --profile workload-current down
```

Add `-v` to remove volumes if you need a clean cluster on the next run.

## Troubleshooting

**`failed to upload metrics: dial tcp [::1]:9090: connect: connection refused`**

Prometheus in `ydb-slo-action` compose is published on a **random host port**, not `9090`. Look up the actual port and use `127.0.0.1`:

```bash
PROM_PORT=$(docker inspect -f '{{(index (index .NetworkSettings.Ports "9090/tcp") 0).HostPort}}' ydb-prometheus)
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:${PROM_PORT}/api/v1/otlp
export PROMETHEUS_URL=http://127.0.0.1:${PROM_PORT}
```

Or drop metrics for a quick run: `unset OTEL_EXPORTER_OTLP_ENDPOINT PROMETHEUS_URL`.

**Workload exits immediately with a connection error**

Wait for the cluster to become ready. The workload depends on `database-readiness` completing successfully. Check cluster health:

```bash
docker compose ps
docker compose logs database-readiness
```

**Docker build fails on Apple Silicon**

The build script targets `linux/amd64` (same as CI). Ensure Docker Desktop is running with emulation enabled.

**High resource usage**

A full run with chaos uses several containers. Close other heavy workloads or reduce `WORKLOAD_DURATION` for a shorter smoke test:

```bash
WORKLOAD_DURATION=120 docker compose --profile telemetry --profile chaos --profile workload-current up
```

**Inspect all container logs**

```bash
docker compose logs
```

## CI reference

In GitHub Actions, the same images are built by `.github/scripts/build-slo-image.sh` and orchestrated by [`ydb-platform/ydb-slo-action/init@v2`](https://github.com/ydb-platform/ydb-slo-action). The workflow is defined in [`.github/workflows/slo.yml`](../../.github/workflows/slo.yml) and runs on pull requests labeled `SLO`.
