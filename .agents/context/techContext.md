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
# Unit tests (default — integration tag excluded)
go test -race ./...

# Lint
golangci-lint run ./...
```

### Devcontainer (recommended)

`.devcontainer/compose.yml` — Go devcontainer + `ghcr.io/ydb-platform/local-ydb:24.3`.

Pre-set env vars: `YDB_CONNECTION_STRING`, `YDB_CONNECTION_STRING_SECURE`, `YDB_SSL_ROOT_CERTIFICATES_FILE`.

### Integration tests (manual / host)

```bash
docker run -itd --name ydb -dp 2135:2135 -dp 2136:2136 -dp 8765:8765 \
  -v "$(pwd)/ydb_certs:/ydb_certs" \
  -e YDB_LOCAL_SURVIVE_RESTART=true -e YDB_USE_IN_MEMORY_PDISKS=true \
  -h localhost ydbplatform/local-ydb:latest

export YDB_CONNECTION_STRING="grpcs://localhost:2135/local"
export YDB_SSL_ROOT_CERTIFICATES_FILE="$(pwd)/ydb_certs/ca.pem"
export YDB_SESSIONS_SHUTDOWN_URLS="http://localhost:8765/actors/kqp_proxy?force_shutdown=all"

go test -race -tags integration ./tests/integration
```

## CI workflows

| Workflow | Trigger | What it runs |
|----------|---------|--------------|
| `lint.yml` | push/PR to `master`, `release-*` | `golangci-lint`, gofumpt/gci format check |
| `tests.yml` | push/PR + hourly cron | unit: `go test -race ./...`; integration vs local YDB |
| `changelog.yml` | PR | `CHANGELOG.md` required (skip: `no changelog` label) |
| `check-codegen.yml` | PR | `go generate` trace/gstack diff |
| `breaking.yml` | PR | `gorelease` API diff (skip: `broken changes`) |
| `examples.yml` | PR | examples vs local YDB (skip: `no examples`) |
| `publish.yml` | manual | version bump + release |

## PR labels that skip CI gates

`no lint`, `no tests`, `no integration tests`, `no changelog`, `no examples`, `broken changes`

## Dependencies

Shared in root `go.mod`. Do not run `go mod tidy` or `go get` unless the task requires it.

## Codegen

After touching `trace/` or stack IDs:

```bash
go generate ./trace
# gstack as needed — see check-codegen.yml
```
