# Testing

## Unit tests (default)

```bash
go test -race ./...
```

Integration tests in `tests/integration/` are excluded automatically (`//go:build integration`).

> Note: `CONTRIBUTING.md` documents `-tags fast` for unit-only runs, but that build tag does not exist. Default `go test ./...` is correct.

## Integration tests

```bash
go test -race -tags integration ./tests/integration
```

Requires running YDB and environment variables (see `environment.md`):

| Variable | Purpose |
|----------|---------|
| `YDB_CONNECTION_STRING` | Insecure gRPC DSN |
| `YDB_CONNECTION_STRING_SECURE` | TLS gRPC DSN |
| `YDB_SSL_ROOT_CERTIFICATES_FILE` | CA for TLS tests |
| `YDB_SESSIONS_SHUTDOWN_URLS` | Rolling-restart / session shutdown tests |

Helpers: `tests/integration/helpers_test.go`.

## CI parity

Before requesting review:

```bash
golangci-lint run ./...
go test -race ./...
```

Integration changes: run `-tags integration ./tests/integration` locally or rely on CI (skip with `no integration tests` label only when appropriate).

## Test libraries

`github.com/stretchr/testify`, `github.com/rekby/fixenv`, `go.uber.org/mock` — test-only dependencies in `go.mod`.
