# Environment and CI

## Toolchain

| Item | Value |
|------|-------|
| Module | `github.com/ydb-platform/ydb-go-sdk/v3` |
| Go | 1.24.0 (module); CI: 1.21.x, 1.26.x |
| Linter | golangci-lint v2.11.4 |

Full CI details: [`.agents/context/techContext.md`](../context/techContext.md).

## Devcontainer (recommended)

```text
Dev Containers: Reopen in Container
```

`.devcontainer/compose.yml`:

- **sdk** — Go devcontainer, `network_mode: service:ydb`
- **ydb** — `ghcr.io/ydb-platform/local-ydb:24.3`

Env pre-configured: `YDB_CONNECTION_STRING=grpc://ydb:2136/local`, TLS vars, certs volume.

`postStartCommand` runs `.devcontainer/configure.sh` (YDB CLI profile).

## Local YDB (without devcontainer)

```bash
docker run -itd --name ydb -dp 2135:2135 -dp 2136:2136 -dp 8765:8765 \
  -v "$(pwd)/ydb_certs:/ydb_certs" \
  -e YDB_LOCAL_SURVIVE_RESTART=true -e YDB_USE_IN_MEMORY_PDISKS=true \
  -h localhost ydbplatform/local-ydb:latest

export YDB_CONNECTION_STRING="grpcs://localhost:2135/local"
export YDB_SSL_ROOT_CERTIFICATES_FILE="$(pwd)/ydb_certs/ca.pem"
export YDB_SESSIONS_SHUTDOWN_URLS="http://localhost:8765/actors/kqp_proxy?force_shutdown=all"
```

## Lint install

CI uses golangci-lint **v2.11.4**. Install matching version:

```bash
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | \
  sh -s -- -b "$(go env GOPATH)/bin" v2.11.4
```

## golangci-lint

```bash
golangci-lint run ./...
```

Also linted: `examples/`, `tests/slo/` (separate configs via `.github/scripts/golangci-lint-gen-configs.sh`).
