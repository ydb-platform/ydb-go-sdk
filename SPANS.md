# Tracing spans emitted by `ydb-go-sdk`

This document describes the span tree produced by the `spans` package
(`github.com/ydb-platform/ydb-go-sdk/v3/spans`) when an adapter is installed via
`ydb.WithTraceDriver(spans.WithTraces(adapter))` etc. The naming and the
attribute keys follow [OpenTelemetry semantic conventions for database
clients][otel-db] so the spans are immediately usable from any OTel backend
(Tempo, Jaeger, Honeycomb, ...).

[otel-db]: https://opentelemetry.io/docs/specs/semconv/database/database-spans/

## Span names

### CLIENT-kind spans (one per gRPC RPC)

| Name | RPC | Notes |
| --- | --- | --- |
| `ydb.CreateSession` | `QueryService.CreateSession` + first `AttachStream` message | Emitted by the SDK whenever a new query session is created |
| `ydb.ExecuteQuery` | `QueryService.ExecuteQuery` | Emitted for every `Query` / `Exec` / `SessionQuery` / `SessionExec` / `TxQuery` / `TxExec` call and their `ResultSet` / `Row` variants. Covers reading the response stream from start to end |
| `ydb.BeginTransaction` | `QueryService.BeginTransaction` | Emitted only for the explicit `BeginTransaction` RPC (eager `s.Begin` and `Tx.UnLazy`). Lazy `DoTx` transactions fuse the begin into the first `ExecuteQuery` and **do not** produce this span |
| `ydb.Commit` | `QueryService.CommitTransaction` | |
| `ydb.Rollback` | `QueryService.RollbackTransaction` | |

### INTERNAL-kind spans

| Name | Scope |
| --- | --- |
| `ydb.Driver.Initialize` | Wraps the **first** driver initialization (cluster discovery). Periodic discovery refreshes from the repeater do **not** produce a span |
| `ydb.GetSession` | Wraps a pool `Get`. On a pool miss it parents a child `ydb.CreateSession` |
| `ydb.RunWithRetry` | Wraps an entire retry loop |
| `ydb.Try` | One per retry attempt; carries `ydb.retry.backoff_ms` (sleep waited before this attempt; `0` for the first attempt) |

### Suppressed spans

The previous `internal/...` span names are intentionally not emitted any more;
the resulting span tree only contains the user-facing `ydb.*` names above.
The suppressed names are:

- `internal/query.(*Client).Do`, `internal/query.(*Client).DoTx`
- `internal/pool.(*Pool).With`, `internal/pool.(*Pool).try`, `internal/pool.(*Pool).putItem`
- `internal/conn.(*conn).Invoke`, `internal/conn.(*conn).dial`, `internal/conn.(*conn).NewStream`
- session lifecycle delete spans

## Attributes

### Provenance overview

| Attribute | Set by |
| --- | --- |
| `db.system.name`, `db.namespace`, `server.address`, `server.port` | Adapter implementation, from driver configuration |
| `network.peer.address`, `network.peer.port`, `ydb.node.id`, `ydb.node.dc` | SDK, once the gRPC layer selects a concrete endpoint for the RPC (`spans/driver.go: annotateNetworkPeer`). Attached both to the innermost open span and to the surrounding top-level CLIENT span |
| `ydb.node.id` (additionally) | Attached at span start for every session-bound handler (`OnSession*` / `OnTx*`) using the node id already known on the session, so the value is present even before the gRPC peer is chosen and survives retries that never reach the wire |
| `error.type`, `db.response.status_code` | SDK, on every span that finishes with a non-nil error |

### Key constants

The `spans` package exports the following attribute-key constants (so consumers
do not have to hard-code OTel strings):

- `AttrDBSystemName` = `db.system.name`
- `AttrDBNamespace` = `db.namespace`
- `AttrServerAddress` = `server.address`
- `AttrServerPort` = `server.port`
- `AttrNetworkPeerAddress` = `network.peer.address`
- `AttrNetworkPeerPort` = `network.peer.port`
- `AttrYDBNodeID` = `ydb.node.id`
- `AttrYDBNodeDC` = `ydb.node.dc`
- `AttrYDBRetryBackoffMs` = `ydb.retry.backoff_ms`
- `AttrErrorType` = `error.type`
- `AttrDBResponseStatusCode` = `db.response.status_code`

### Error attributes

Whenever a span finishes with a non-nil error the SDK attaches:

- `error.type` — `"transport_error"` for gRPC transport errors,
  `"ydb_error"` for any other `ydb.Error`, dynamic Go type name otherwise.
- `db.response.status_code` — set only when the error carries a YDB status
  code.

This is the rough equivalent of OTel's `Span.RecordException` /
`Activity.SetException` from other SDKs, but exposed as plain attributes so it
works through the existing `spans.Adapter` interface without forcing
adapter authors to implement a new method.

## How retry tracing is wired

`query.Client.Do` and `query.Client.DoTx` propagate the driver-level
`trace.Retry` (e.g. the one installed by `spans.WithTraces`) into their
internal retry loop, so `ydb.RunWithRetry` / `ydb.Try` spans appear for
query-service operations as well — not just for `database/sql` retries.

The new `trace.Retry.OnRetryAttempt` callback fires once per attempt (including
the first one) with the attempt number and the backoff duration that was
waited before this attempt; the `spans` adapter consumes it to populate the
`ydb.retry.backoff_ms` attribute on each `ydb.Try`.

## Example

See [`examples/opentelemetry`](examples/opentelemetry) for an end-to-end
example that maps `spans.Adapter` onto the OpenTelemetry Go SDK via
[`ydb-go-sdk-otel`](https://github.com/ydb-platform/ydb-go-sdk-otel) and ships
an OTel Collector / Tempo / Prometheus / Grafana stack via docker-compose.
