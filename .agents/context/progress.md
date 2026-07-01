# Progress

> **Volatile file** — append/update as work completes.

## What works (baseline)

- **Table API**: `Do`/`DoTx`, sessions, transactions, bulk operations.
- **Query API**: streaming results, `Do`/`DoTx`, lazy tx options.
- **Scheme API**: directory listing, path operations.
- **Topics**: reader/writer/listener/multiwriter.
- **Coordination**: distributed semaphores.
- **Scripting / Ratelimiter / Operation**: exposed on `Driver` (see `systemPatterns.md`).
- **Discovery** + balancers (`RandomChoice`, `PreferNearestDC`, …); dropped endpoints eventually closed in pool (see `systemPatterns.md` → orphaned gRPC connections).
- **database/sql** driver via `sql.Open("ydb", ...)`.
- **Auth**: static tokens, metadata, OAuth credentials.
- **TLS**: custom CA via `ydb_certs/`.
- **Observability**: trace callbacks, log/metrics/spans adapters.

## CI status

- Lint: golangci-lint v2.11.4 on Go 1.26.
- Unit tests: `go test -race ./...` on ubuntu/windows/macOS × Go 1.21/1.26.
- Integration: `tests/integration` vs YDB 24.4/latest/edge images.

## Known gaps

- Check GitHub Issues for active bugs and feature requests.
- Cross-SDK parity with Java/Rust tracked issue-by-issue.

## Milestones

| Date | Milestone |
|------|-----------|
| 2026-06 | `.agents/` workspace for AI agents |
| 2026-06 | Fix orphaned gRPC reconnects when endpoints leave discovery (`closeDroppedConns`, `conn.Conn.Close`) — pending merge |
| ongoing | Trace handler hardening ([#2194](https://github.com/ydb-platform/ydb-go-sdk/pull/2194)) |

## Changelog for agents

User-facing API or behavior changes require a bullet at the **top** of `CHANGELOG.md` (past tense, **no version number**, **before** all `## v3.x.x` headers — released sections are immutable). Write for end users: observable effect only, no implementation details (debounce counts, internal field names, etc. belong in the PR description). Internal-only agent docs: label PR `no changelog`.
