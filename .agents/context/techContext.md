# Tech Context

## Toolchain

| Item | Value |
|------|-------|
| Module | `github.com/ydb-platform/ydb-go-sdk/v3` |
| Go (module) | 1.24.0 |
| CI Go matrix | 1.21.x, 1.26.x |
| gRPC | `google.golang.org/grpc` |
| Protos | `github.com/ydb-platform/ydb-go-genproto` |
| Linter (CI) | golangci-lint **v2.11.4** (`.github/workflows/lint.yml`) |

## Local development

```bash
go test -race ./...          # unit only (integration tag excluded)
golangci-lint run ./...
```

### Devcontainer

`.devcontainer/compose.yml` — Go devcontainer + `ghcr.io/ydb-platform/local-ydb:24.3`.

Env: `YDB_CONNECTION_STRING=grpc://ydb:2136/local`, `YDB_CONNECTION_STRING_SECURE=grpcs://ydb:2135/local`, `YDB_SSL_ROOT_CERTIFICATES_FILE=/ydb_certs/ca.pem`.

### Integration tests (host)

```bash
docker run -itd --name ydb -dp 2135:2135 -dp 2136:2136 -dp 8765:8765 \
  -v "$(pwd)/ydb_certs:/ydb_certs" \
  -e YDB_LOCAL_SURVIVE_RESTART=true -e YDB_USE_IN_MEMORY_PDISKS=true \
  -h localhost ydbplatform/local-ydb:latest

export YDB_CONNECTION_STRING="grpc://localhost:2136/local"
export YDB_CONNECTION_STRING_SECURE="grpcs://localhost:2135/local"
export YDB_SSL_ROOT_CERTIFICATES_FILE="$(pwd)/ydb_certs/ca.pem"
export YDB_SESSIONS_SHUTDOWN_URLS="http://localhost:8765/actors/kqp_proxy?force_shutdown=all"

go test -race -tags integration ./tests/integration
```

`YDB_CONNECTION_STRING` is the **insecure** endpoint (port 2136); TLS uses `YDB_CONNECTION_STRING_SECURE` (port 2135). Default in `tests/integration/helpers_test.go`: `grpc://localhost:2136/local`.

## CI workflows

| Workflow | Trigger | What it runs |
|----------|---------|--------------|
| `lint.yml` | push/PR to `master`, `release-*` | `golangci-lint`, gofumpt/gci format check |
| `tests.yml` | push/PR + hourly cron | unit: `go test -race ./...`; integration vs `ydbplatform/local-ydb:{24.4,latest,edge}` |
| `changelog.yml` | PR | `CHANGELOG.md` required (skip: `no changelog`) |
| `check-codegen.yml` | PR | `go generate` trace/gstack diff |
| `breaking.yml` | PR | `gorelease` (skip: `broken changes`) |
| `examples.yml` | PR | examples vs local YDB (skip: `no examples`) |
| `slo.yml` | push/PR + manual | SLO benchmarks (active in go-sdk) |
| `publish.yml` | manual | version bump + release |

## PR labels that skip CI gates

`no lint`, `no tests`, `no integration tests`, `no changelog`, `no examples`, `broken changes`

## Dependencies

Do not run `go mod tidy` or `go get` unless the task requires it.

## Codegen

```bash
go generate ./trace
```

After touching `trace/` or stack IDs — CI `check-codegen.yml` must pass.
