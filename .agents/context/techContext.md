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

`.devcontainer/Dockerfile` currently installs golangci-lint v1.62.2; CI uses v2.11.4.

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

Stress a single flaky integration test: add `-count=N -run 'TestName$'` to the `go test` command above.

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

## Rolling-restart session-pool A/B

For session-lifecycle regressions, use the native Table workload with a five-node stack and isolate the rolling-restart scenario. The default chaos profile runs multiple/random scenarios and is unsuitable for comparing two SDK revisions unless the selected fault sequence is identical.

Baseline and candidate must use the same:

- `SRC_PATH=native/table` workload;
- `WithSessionPoolIdleThreshold(5 * time.Second)` option;
- five database nodes (`--profile extra-nodes`);
- workload duration, load, server images, rolling order, and observation offsets;
- clean Compose volumes between runs.

Use a temporary Compose override for `chaos-monkey` that runs only `/opt/ydb.tech/chaos/scenarios/05-rolling-restart.sh`. Keep the override out of the production diff. On Apple Silicon, build both workload images for `linux/amd64`, matching the upstream Compose service.

Measure the server metric, not only SDK gauges:

```promql
sum(table_session_active_count)
max_over_time(sum(table_session_active_count)[5m:2s])
sum by (instance) (table_session_active_count)
```

Record the peak, values at fixed offsets (for example 25 and 90 seconds after rolling completes), per-node distribution, workload exit code, and the value after `Driver.Close`.

Validated 2026-07-17 with otherwise identical rolling-only runs:

| Measurement | clean v3.137.0 | oldest-expired/LIFO fix |
|-------------|---------------:|------------------------:|
| Peak server sessions | 1029 | 1000 |
| About 25 seconds after rolling | 634 | 46 |
| About 90 seconds after rolling | 630 | 25 |
| First post-workload observation | 2 | 0 after the next scrape |

Both workloads exited successfully. Treat the exact counts as environment-specific evidence; the durable signal is that the candidate returns to the small active working set instead of retaining hundreds of sessions.

## PR labels that skip CI gates

`no lint`, `no tests`, `no integration tests`, `no changelog`, `no examples`, `broken changes`

## Changelog (unreleased)

User-facing PRs add bullets **at the very top** of `CHANGELOG.md` — **before** any `## vX.Y.Z` header. Do **not** edit an existing version section; `publish.yml` assigns the version at release.

```markdown
* Fixed …

## v3.139.7
```

Details: [`.agents/rules/changelog.md`](../rules/changelog.md).

## Dependencies

Do not run `go mod tidy` or `go get` unless the task requires it.

## Codegen

```bash
go generate ./trace
```

After touching `trace/` or stack IDs — CI `check-codegen.yml` must pass.
