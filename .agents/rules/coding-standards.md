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

See [`.agents/context/systemPatterns.md`](../context/systemPatterns.md) for driver layout. In short:

- Bypassing `balancerWithMeta` / `conn.Pool` for production RPC paths.
- Returning from `Do`/`DoTx` without closing streams and result sets.
- Unbounded retry loops without idempotency consideration on mutating operations.
- Hand-editing `*_gtrace.go` instead of `go generate ./trace`.

## Mutex and critical sections

When a function holds `sync.Mutex` / `sync.RWMutex`:

- **Prefer `defer mu.Unlock()`** (or `defer mu.RUnlock()`) immediately after `Lock()` / `RLock()`. Every exit path must release the mutex; `defer` is the default safe choice in Go.
- **Do not replace `defer` with manual `mu.Unlock()` before each `return`** to run work outside the lock. That is brittle: a new early return or refactor can skip an unlock and deadlock, or leave the mutex held during blocking I/O.
- **If blocking work must run outside the lock** (gRPC `Close`, network I/O, `wg.Wait()`): extract a small helper that owns the critical section with `defer mu.Unlock()`, mutates shared state, and **returns** what to do next (e.g. `*conn` to close, `[]closer.Closer`). The caller performs slow work after the helper returns.

Example from `internal/conn/pool.go`:

```go
func (p *Pool) Put(ctx context.Context, c Conn) {
    cc, ok := c.(*conn)
    if !ok || cc == nil {
        return
    }
    if !p.tryPut(cc) {
        _ = cc.Close(ctx) // outside pool mutex
    }
}

func (p *Pool) tryPut(c *conn) bool {
    p.mu.Lock()
    defer p.mu.Unlock()
    // refcount + map update only; single unlock path via defer
    ...
    return true
}
```

Same pattern for `release()` in `Pool.RemoveRef`.

**Avoid** holding a pool-wide mutex across blocking `Close()` unless there is a documented lock-order reason (e.g. `onClose` must re-enter the same mutex). Prefer delete-from-map under lock, then close outside.

