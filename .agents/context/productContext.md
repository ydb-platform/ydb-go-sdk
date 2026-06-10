# Product Context

## Users

- Go application developers connecting to YDB for OLTP, streaming (topics), and metadata (scheme).
- Maintainers integrating Go into YDB platform services.
- Contributors extending SDK coverage to match Java, Rust, and other language SDKs.

## Problems solved

| Need | SDK surface |
|------|-------------|
| Run YQL queries and transactions | `db.Query().Do` / `DoTx`, `db.Table().Do` / `DoTx` |
| `database/sql` compatibility | `sql.Open("ydb", dsn)` via `sql.go` |
| Browse database directory / schema | `db.Scheme()` |
| Produce/consume topic messages | `db.Topic()` — reader/writer/listener APIs |
| Distributed locks / semaphores | `db.Coordination()` |
| Auth (static token, metadata, OAuth) | `credentials` options on `ydb.Open` |
| Multi-node clusters | Discovery + balancers (`RandomChoice`, `PreferNearestDC`, …) |
| Observability | `trace/` callbacks + `log/`, `metrics/`, `spans/` adapters |

## Developer experience goals

- **DSN** as primary entry: `grpc://host:port/database` or `grpcs://...`.
- **Automatic retries** in `Do`/`DoTx` on retriable errors (use `WithIdempotent()` for safe retries).
- **Examples** in `examples/` for common patterns; integration tests as living documentation.
- **Devcontainer** with local YDB pre-wired (`.devcontainer/`).

## API stability

- Published as `github.com/ydb-platform/ydb-go-sdk/v3` on pkg.go.dev.
- `// Experimental`, `// Deprecated`, `// Internals` markers per `VERSIONING.md`.
- Breaking changes tracked by `breaking.yml` (`gorelease`); label `broken changes` to skip gate.

## Related resources

- [YDB documentation](https://ydb.tech/docs) — server-side concepts, YQL
- [ydb-rs-sdk](https://github.com/ydb-platform/ydb-rs-sdk) — Rust SDK for cross-language parity
- [ydb-java-sdk](https://github.com/ydb-platform/ydb-java-sdk) — Java SDK reference
