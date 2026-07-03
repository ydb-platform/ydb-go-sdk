# Running SLO Tests Locally

This guide describes how to run the full SLO test infrastructure on your machine — the same setup used in CI, but without GitHub Actions.

SLO tests exercise the SDK under load while a multi-node YDB cluster experiences failures (node stops, network black holes, rolling restarts). Everything runs from `tests/slo/` via Docker Compose.

For workload internals (table schema, metrics, environment variables), see [README.md](README.md).

## Prerequisites

- **Docker** with **Docker Compose v2.21+** (`docker compose`)
- Enough CPU, RAM, and disk for the compose stack

> **Note:** Docker images are built for `linux/amd64`. On Apple Silicon, Docker Desktop handles emulation automatically, but the first build may take a while.

## How it works

`compose.yaml` pulls the upstream stack from [ydb-slo-action](https://github.com/ydb-platform/ydb-slo-action) (`deploy/compose.yml` at tag `v2`). `compose.override.yaml` adds a `build:` section for `workload-current`, so the workload image is built from this SDK checkout.

```
tests/slo/
├── compose.yaml           → includes ydb-slo-action infra from GitHub
└── compose.override.yaml  → builds workload-current from tests/slo/Dockerfile
```

## Run

```bash
cd tests/slo

SRC_PATH=native/query WORKLOAD_DURATION=600 docker compose \
  --profile telemetry \
  --profile chaos \
  --profile workload-current \
  up --build
```

| Variable            | Description                                    |
|---------------------|------------------------------------------------|
| `SRC_PATH`          | Workload directory under `tests/slo/` to build (default: `native/query`) |
| `WORKLOAD_DURATION` | Run time in seconds                            |

Available `SRC_PATH` values (matching CI):

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

The first run clones `ydb-slo-action`, builds infra images from that repo, and builds the workload from this checkout. The workload build downloads Go dependencies inside Docker and can take 10–15 minutes on a cold cache.

## Observe results

Run these from `tests/slo/` while the stack is up (same profiles as in **Run**).

**Check services:**

```bash
docker compose ps
```

**Workload logs:**

```bash
docker compose logs -f workload-current
```

**Prometheus:**

```bash
curl "http://127.0.0.1:$(docker compose port prometheus 9090 | cut -d: -f2)/api/v1/query?query=sdk_operations_total"
```

**Grafana:**

```bash
open "http://127.0.0.1:$(docker compose port grafana 3000 | cut -d: -f2)"
```

Anonymous admin access is enabled by default.

**YDB console:**

```bash
open "http://127.0.0.1:$(docker compose port storage-1 8765 | cut -d: -f2)"
```

**Useful PromQL queries:**

```promql
rate(sdk_operations_total[1m])
sdk_operation_latency_p99_seconds{operation_type="read"}
rate(sdk_retry_attempts_total[1m])
```

## Tear down

```bash
docker compose \
  --profile telemetry \
  --profile chaos \
  --profile workload-current \
  down
```

Add `-v` to remove volumes for a clean cluster on the next run.

## Troubleshooting

**`No such object: ydb-grafana` (or similar) when opening Grafana/Prometheus**

The stack is not running, or telemetry was not started. From `tests/slo/`:

```bash
docker compose ps
```

`grafana` and `prometheus` must appear in the list. If not, bring the stack up with `--profile telemetry` (see **Run**). Use `docker compose port`, not `docker inspect` — it resolves ports via the compose project.

**Workload exits immediately with a connection error**

Wait for `database-readiness` to complete:

```bash
docker compose ps
docker compose logs database-readiness
```

**Docker build fails on Apple Silicon**

Ensure Docker Desktop is running with `linux/amd64` emulation enabled.

**High resource usage**

Use a smaller `WORKLOAD_DURATION` in the run command for a shorter test.
