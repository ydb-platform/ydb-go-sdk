# Coding Standards

Read when touching public API, package layout, dependencies, or error handling.

## Style

- Comments, godoc, error messages, and logs: **English**.
- Match naming and formatting in the touched package; do not reformat unrelated code.
- Run `golangci-lint run ./...` before handoff (CI also checks gofumpt + gci).

## Dependencies

- Do **not** change `go.mod` / `go.sum` unless the task explicitly requires it.
- Do not run `go mod tidy` or `go get` as a side effect of code changes.
- If a dependency change is required, document it clearly in the PR description.

## Public API

- Implementations live in `internal/` — do not leak internal types in public packages without stable wrappers.
- New service APIs follow: `internal/<service>/` → public `table/` / `query/` / … facade with `Do`/`DoTx` where applicable.
- Respect `// Experimental`, `// Deprecated`, `// Internals` markers per `VERSIONING.md`.
- `testutil/` is unstable — do not treat as semver-guaranteed API.

## Codegen

- Do not hand-edit `*_gtrace.go` or gstack-generated files.
- After changing `trace/` definitions: `go generate ./trace` and verify `check-codegen.yml` passes.

## Architecture anti-patterns

- Bypassing connection pool or session pool for production paths.
- Returning from `Do`/`DoTx` without closing streams and result sets.
- Unbounded retry loops without idempotency consideration on mutating operations.
