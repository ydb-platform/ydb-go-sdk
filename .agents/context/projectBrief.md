# Project Brief

## What this is

**ydb-go-sdk** is the official Go SDK for [YDB](https://ydb.tech/) — a distributed SQL database. Module path: `github.com/ydb-platform/ydb-go-sdk/v3`. Provides native clients and a `database/sql` driver.

## Goals

- Idiomatic async Go client for YDB table, query, scheme, topic, coordination, discovery, and related APIs.
- Production concerns: connection pooling, session pooling, load balancing, retries, credentials, TLS.
- Stay compatible with YDB gRPC/protobuf contracts via `ydb-go-genproto`.
- Maintain semver per [`VERSIONING.md`](../../VERSIONING.md); publish releases via GitHub Actions.

## Non-goals

- `internal/` packages are not a public API for application developers.
- This repo does not host the YDB server or non-Go SDKs.
- `testutil/` is explicitly unstable (see `VERSIONING.md`).

## Key constraints

| Area | Constraint |
|------|------------|
| Language | Go (module declares `go 1.24.0`; CI tests 1.21.x and 1.26.x) |
| Public API | Facade in root + service packages; implementations in `internal/` |
| Retries | `Do`/`DoTx` loops with backoff; idempotent ops via `WithIdempotent()` |
| Tests | Unit tests co-located; integration in `tests/integration/` with `//go:build integration` |
| Changelog | User-facing PRs require `CHANGELOG.md` entry (or `no changelog` label) |
| Codegen | `trace/` changes require `go generate` — CI enforces clean diff |

## Success criteria for agent work

- Public API changes are intentional, documented, semver-aware, and have changelog entries.
- New code follows existing package boundaries and `Do`/`DoTx` patterns.
- `.agents/context/` reflects current state after significant work.

## Reference docs (by depth)

| Depth | File |
|-------|------|
| Quick start | `README.md` |
| Agent router | `AGENTS.md` → `.agents/rules/` |
| Versioning | `VERSIONING.md` |
| SQL driver | `SQL.md` |
| API reference | [pkg.go.dev/ydb-go-sdk/v3](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3) |
