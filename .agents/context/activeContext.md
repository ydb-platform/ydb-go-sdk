# Active Context

> **Volatile file** — update at the end of a work session or before closing a PR.
>
> **Merge conflicts:** with many parallel PRs this file conflicts often. On conflict, keep the union of recent decisions or reset to a short generic focus — do not block the feature PR on agent housekeeping.

## Current focus

_No active task recorded._ Check open PRs and GitHub Issues for ongoing work.

## Recent changes

- `.agents/` workspace added for AI coding agents (see `systemPatterns.md` for driver architecture).

## Open questions

- Whether to add nested `AGENTS.md` per major package as the SDK grows.
- Whether to add `.agents/skills/` shared across YDB SDK repos.

## Next steps

- Update this file when starting or finishing significant work.

## Working conventions (reminder)

- Read this file every session; other context files only when relevant.
- Coding rules: `AGENTS.md` → `.agents/rules/` (on demand).
- Run `golangci-lint run ./...` and `go test -race ./...` before requesting review.
