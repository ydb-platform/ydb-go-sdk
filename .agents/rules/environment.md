# Environment — quick commands

Authoritative CI/toolchain reference: [`.agents/context/techContext.md`](../context/techContext.md).

## Devcontainer (recommended)

VS Code: **Dev Containers: Reopen in Container** (`.devcontainer/compose.yml`).

Pre-configured: `YDB_CONNECTION_STRING=grpc://ydb:2136/local`, TLS vars, YDB CLI profile.

## Lint and unit tests

```bash
golangci-lint run ./...
go test -race ./...
```

## Integration tests (host)

Requires local YDB. Env vars match CI (`tests.yml`, `helpers_test.go`):

```bash
export YDB_CONNECTION_STRING="grpc://localhost:2136/local"
export YDB_CONNECTION_STRING_SECURE="grpcs://localhost:2135/local"
export YDB_SSL_ROOT_CERTIFICATES_FILE="$(pwd)/ydb_certs/ca.pem"
# optional for session-shutdown tests:
export YDB_SESSIONS_SHUTDOWN_URLS="http://localhost:8765/actors/kqp_proxy?force_shutdown=all"

go test -race -tags integration ./tests/integration
```

See [`.agents/rules/testing.md`](testing.md) for details.
