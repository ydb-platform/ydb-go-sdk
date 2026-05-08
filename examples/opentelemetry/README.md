# ydb-go-sdk + OpenTelemetry (E2E playground)

Goal: spin up a local OpenTelemetry stack and verify that a Go application
exports traces produced by `ydb-go-sdk` via the `otel-collector` into
`Tempo` / `Prometheus`, with visualization in `Grafana`.

The infrastructure (compose file, OTel collector / Tempo / Prometheus /
Grafana configs, YDB local config and provisioned Grafana dashboards) is
mirrored 1-to-1 from the .NET SDK example
[`Ydb.Sdk.AdoNet.OpenTelemetry`](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples/Ydb.Sdk.AdoNet.OpenTelemetry)
so the two stacks can be compared side-by-side. The only Go-specific
piece is the `app` service in `compose-e2e.yaml`, which builds and runs
this directory's `main.go` from the local Dockerfile.

## How to run

```bash
# Preferred (Docker Compose v1 / legacy binary)
docker-compose -f compose-e2e.yaml up -d --build

# Alternative (Docker Compose v2 plugin, if available)
# docker compose -f compose-e2e.yaml up -d --build
```

> Note: `ydbplatform/local-ydb` is a `linux/amd64`-only image. On Apple
> Silicon / arm64 (e.g. Colima) you may need x86_64 emulation (Rosetta);
> the compose file pins `platform: linux/amd64` for the YDB service.

## Local debugging (run the Go program from the host)

If you want to iterate on the Go code without rebuilding the container,
scale `app` down and run the program directly:

```bash
docker-compose -f compose-e2e.yaml up -d --scale app=0
go mod tidy
YDB_DSN=grpc://localhost:2136/local \
  OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 \
  go run .
```

> The dockerised `app` shares `TZ` and `/etc/localtime` paths with the
> rest of the stack via the YDB container; the host run can show small
> clock skew between span timestamps from the host program and from the
> server / collector.

## Enable server-side tracing in YDB (end-to-end)

This demo already exports app traces to the collector. To also export
**YDB server traces** into the same collector:

```bash
# 1) edit ./ydb_config/ydb-config.yaml and add tracing_config
#    (see ./ydb_config/otel-tracing-snippet.yaml)
#
# 2) recreate the ydb container to pick up the updated config
docker-compose -f compose-e2e.yaml up -d --force-recreate ydb
```

## What should be running

* **YDB local UI** — `http://localhost:8765`
* **Grafana** — `http://localhost:3000` (anonymous access enabled)
* **Tempo API** — `http://localhost:3200`
* **Prometheus** — `http://localhost:9090`
* **OTel Collector**
  * OTLP gRPC: `localhost:4317`
  * OTLP HTTP: `http://localhost:4318`
  * health check: `http://localhost:13133`
  * zPages: `http://localhost:55679/debug/tracez`

## How to verify traces

1. Wait for the `app` container to finish (it executes a single
   transactional `DoTx` and exits).
2. Open Grafana → **Explore** → datasource **Tempo**.
3. Find the service **`ydb-go-sdk-otel-trace-sample`** and inspect spans
   such as `ydb.RunWithRetry`, `ydb.Try`, `ydb.ExecuteQuery`, `ydb.Commit`.

Spans are also printed in the `otel-collector` logs (the `debug`
exporter is enabled).

## Tracing reference (`spans.Adapter` implementation)

| Span                    | Kind     | When emitted                                                                                                  |
| ----------------------- | -------- | ------------------------------------------------------------------------------------------------------------- |
| `ydb.Driver.Initialize` | Internal | Driver first initialization (cluster discovery + first dial); periodic discovery refreshes do NOT emit spans  |
| `ydb.RunWithRetry`      | Internal | Wraps the entire retry loop in `retry.Retry` / `query.Client.Do` / `query.Client.DoTx`                        |
| `ydb.Try`               | Internal | One per attempt (including the first); the user operation runs inside it                                      |
| `ydb.GetSession`        | Internal | Acquire a session from the query-service pool; parents `ydb.CreateSession` on a pool miss                     |
| `ydb.CreateSession`     | Client   | QueryService `CreateSession` + first message of `AttachStream`                                                |
| `ydb.ExecuteQuery`      | Client   | Individual YQL query execution (single ExecuteQuery RPC, full stream read)                                    |
| `ydb.BeginTransaction`  | Client   | Explicit BeginTransaction RPC (eager `s.Begin` and `Tx.UnLazy`); lazy DoTx fuses begin into ExecuteQuery      |
| `ydb.Commit`            | Client   | Transaction commit                                                                                            |
| `ydb.Rollback`          | Client   | Transaction rollback                                                                                          |

The expected span tree for `db.Query().DoTx(ctx, op, query.WithIdempotent())`:

```
ydb.RunWithRetry          (Internal)
└─ ydb.Try                (Internal)
   ├─ ydb.GetSession      (Internal)
   │  └─ ydb.CreateSession (Client)   ← only on a pool miss
   ├─ ydb.ExecuteQuery    (Client)
   ├─ ydb.ExecuteQuery    (Client)
   └─ ydb.Commit          (Client)
```

### `ydb.RunWithRetry` / `ydb.Try` lifecycle

* `ydb.RunWithRetry` is the outer span for the whole retry-driven operation.
* `ydb.Try` is created for every attempt. The first attempt's span has no
  extra tags.
* Retry attempts (i.e. attempts that were preceded by an actual sleep)
  carry `ydb.retry.backoff_ms` equal to the backoff that was waited
  before them.
* On a non-retryable error the current `ydb.Try` and the surrounding
  `ydb.RunWithRetry` are both marked failed (`error.type` /
  `db.response.status_code`).
* On retries exhaustion all `ydb.Try` spans and `ydb.RunWithRetry` carry
  the terminal error.
* On context cancellation (`context.Canceled` /
  `context.DeadlineExceeded`) the current `ydb.Try` and `ydb.RunWithRetry`
  carry `error.type` set to the cancelling error's dynamic Go type.

### Common attributes

The adapter in `adapter.go` attaches the OpenTelemetry
[database semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/database-spans/)
on every span:

| Attribute              | Source                                        |
| ---------------------- | --------------------------------------------- |
| `db.system.name`       | always `"ydb"`                                |
| `db.namespace`         | `Database` field of the driver config         |
| `server.address`       | host part of the configured driver endpoint  |
| `server.port`          | port part of the configured driver endpoint  |
| `network.peer.address` | actual node address an RPC was routed to     |
| `network.peer.port`    | actual node port an RPC was routed to        |

YDB-specific attributes:

| Attribute              | Source                                        |
| ---------------------- | --------------------------------------------- |
| `ydb.node.id`          | numeric YDB node id                           |
| `ydb.node.dc`          | location DC reported by the node              |
| `ydb.retry.backoff_ms` | sleep waited before the current `ydb.Try`     |

### Error attributes (`SetException`-equivalent)

| Attribute                 | Value                                                                  |
| ------------------------- | ---------------------------------------------------------------------- |
| `error.type`              | `"transport_error"` for grpc transport errors                          |
|                           | `"ydb_error"` for any other `ydb.Error`                                |
|                           | the Go dynamic type name otherwise (e.g. `"*errors.errorString"`)      |
| `db.response.status_code` | YDB status code returned by the server (only when applicable)          |
