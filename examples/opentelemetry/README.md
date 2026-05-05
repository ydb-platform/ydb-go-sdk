# ydb-go-sdk + OpenTelemetry (E2E playground)

Goal: spin up a local OpenTelemetry stack and verify that a Go application
exports traces produced by `ydb-go-sdk` via the `otel-collector` into
`Tempo` / `Prometheus`, with visualization in `Grafana`.

The setup mirrors the .NET example
[`Ydb.Sdk.AdoNet.OpenTelemetry`](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples/Ydb.Sdk.AdoNet.OpenTelemetry)
so the two SDKs can be compared side-by-side.

## How to run (everything in Docker — recommended)

The `app` service in `compose-e2e.yaml` builds and runs the Go demo program
inside the same compose network as YDB and the OTel collector. All
containers run with `TZ=UTC` and share `/etc/localtime` from the host so
the span timeline in Tempo/Grafana is consistent across services.

```bash
docker compose -f compose-e2e.yaml up --build
```

> **Why Docker?** Running the Go program on the host while YDB and the
> collector live in containers can produce misleading span timelines in
> Tempo/Grafana, because OS-level time offsets and timezone differences
> shift span start/end timestamps relative to one another. Running every
> participant in Docker on a single shared clock removes that source of
> noise.

## How to run locally (debugging)

If you want to iterate on the Go code and run only the observability stack
in Docker, scale `app` down and start the program from the host:

```bash
docker compose -f compose-e2e.yaml up -d --scale app=0
cd examples/opentelemetry
go mod tidy
YDB_DSN=grpc://localhost:2136/local \
  OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 \
  go run .
```

Be aware of the timing caveat above when comparing the spans you see.

## What's running

* **YDB local UI** — `http://localhost:8765`
* **Grafana** — `http://localhost:3000` (anonymous access enabled)
* **Tempo API** — `http://localhost:3200`
* **Prometheus** — `http://localhost:9090`
* **OTel Collector**
  * OTLP gRPC: `localhost:4317`
  * OTLP HTTP: `http://localhost:4318`

## How to verify traces

1. Wait for `ydb-otel-sample` to finish and exit.
2. Open Grafana → **Explore** → datasource **Tempo**.
3. Find the service **`ydb-go-sdk-otel-trace-sample`** and inspect spans
   such as `ydb.RunWithRetry`, `ydb.Try`, `ydb.ExecuteQuery`, `ydb.Commit`.

## Tracing (`spans.Adapter` implementation)

| Span                | Kind     | When emitted                                                                                |
| ------------------- | -------- | ------------------------------------------------------------------------------------------- |
| `ydb.CreateSession` | Client   | QueryService `CreateSession` + first message of `AttachStream`                              |
| `ydb.RunWithRetry`  | Internal | Wraps the entire retry loop in `retry.Retry` / `query.Client.Do` / `query.Client.DoTx`      |
| `ydb.Try`           | Internal | One per attempt (including the first); the user operation runs inside it                    |
| `ydb.ExecuteQuery`  | Client   | Individual YQL query execution (single ExecuteQuery RPC, full stream read)                  |
| `ydb.Commit`        | Client   | Transaction commit                                                                          |
| `ydb.Rollback`      | Client   | Transaction rollback                                                                        |

### `ydb.RunWithRetry` / `ydb.Try` lifecycle

* `ydb.RunWithRetry` is the outer span for the whole retry-driven operation.
* `ydb.Try` is created for every attempt. The first attempt's span has no
  extra tags.
* Retry attempts (i.e. attempts that were preceded by an actual sleep) carry
  `ydb.retry.backoff_ms` equal to the backoff that was waited before them.
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
