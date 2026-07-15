# Agent Guidelines — ydb-go-sdk

Canonical agent entry point. Tool configs (`CLAUDE.md`, Cursor rules) start here and route to [`.agents/`](.agents/).

Keep this file **lean** (~60 lines) — it routes to detailed sources. Loading a large AGENTS.md on every session wastes context tokens; use a thin navigation file plus on-demand docs (see [agentsmd/agents.md](https://github.com/agentsmd/agents.md)).

## Project context

Project knowledge lives in [`.agents/context/`](.agents/context/). Coding rules live in [`.agents/rules/`](.agents/rules/) (below). See [`.agents/README.md`](.agents/README.md) for the full layout.

**Before coding** — read selectively:

1. One stable file as needed:
   - architecture / module layout → [`systemPatterns.md`](.agents/context/systemPatterns.md)
   - toolchain / CI / local dev → [`techContext.md`](.agents/context/techContext.md)
   - API surface / users → [`productContext.md`](.agents/context/productContext.md)
   - scope / goals → [`projectBrief.md`](.agents/context/projectBrief.md)
2. Domain file if the task is scoped (e.g. topic → [`topicContext.md`](.agents/context/topicContext.md); multiwriter → [`topicMultiwriterContext.md`](.agents/context/topicMultiwriterContext.md))
3. Quick lookup: [`README.md`](README.md), [`CONTRIBUTING.md`](CONTRIBUTING.md), [pkg.go.dev](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3)

**After significant work** — update `progress.md` and stable context files when the work itself merges. Do **not** merge changes to `activeContext.md` (branch-only scratch pad — see file header).

On **"update memory bank"** — review all core files in [`.agents/context/README.md`](.agents/context/README.md).

## Coding rules (load on demand)

| Topic | File |
|-------|------|
| Style, API boundaries, dependencies | [`.agents/rules/coding-standards.md`](.agents/rules/coding-standards.md) |
| Unit vs integration tests, local YDB | [`.agents/rules/testing.md`](.agents/rules/testing.md) |
| Changelog requirements | [`.agents/rules/changelog.md`](.agents/rules/changelog.md) |
| Issue-first workflow, user boundaries | [`.agents/rules/workflow.md`](.agents/rules/workflow.md) |
| Local dev, devcontainer, CI commands | [`.agents/rules/environment.md`](.agents/rules/environment.md) |

## Non-obvious rules (always on)

- Comments, godoc, error messages, logs: **English**.
- Match style in the touched package; do not reformat unrelated code.
- Do **not** change `go.mod` / `go.sum` unless the task requires it.
- User-facing PRs need a `CHANGELOG.md` entry at the top (or `no changelog` label) — see `changelog.md`.
- Non-trivial changes: discuss in a GitHub issue first ([`CONTRIBUTING.md`](CONTRIBUTING.md)).

## Done when

From repo root:

```bash
golangci-lint run ./...
go test -race ./...
```

Also: update `progress.md` only when the delivered work merges.

Ask the user before dependency upgrades, public API design choices, or touching `trace/` codegen.
