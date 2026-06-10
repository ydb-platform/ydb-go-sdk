# Active Context

> **Volatile file** — update after every significant work session.

## Current focus

Introduce `.agents/` workspace for AI coding agents — aligned with [ydb-rs-sdk #430](https://github.com/ydb-platform/ydb-rs-sdk/pull/430) and [ydb-pg-extension #257](https://github.com/ydb-platform/ydb-pg-extension/pull/257).

## Recent changes

- Added `.agents/context/` for project knowledge and `.agents/rules/` for coding standards.
- Slimmed `AGENTS.md` to a lean router; migrated changelog, lint, and dependency rules to `.agents/rules/`.

## Open questions

- Whether to add nested `AGENTS.md` per major package (`table/`, `query/`, …) as the SDK grows.
- Whether to add `.agents/skills/` (e.g. PR review) shared across YDB SDK repos via `ai-dev-kit`.

## Next steps

- Keep this file and `progress.md` updated as features land.
- Add rules to `AGENTS.md` only after repeated agent mistakes (incremental, not upfront).

## Working conventions (reminder)

- Read `activeContext.md` every session; other context files only when relevant.
- Coding rules: `AGENTS.md` → `.agents/rules/` (on demand).
- Update this file and `progress.md` before closing a PR.
- Run `golangci-lint run ./...` and `go test -race ./...` before requesting review.
