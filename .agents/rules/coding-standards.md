# Coding Standards

Read when touching public API, package layout, dependencies, or error handling.

## Style

- Comments, godoc, error messages, and logs: **English**.
- Match naming and formatting in the touched package; do not reformat unrelated code.
- Run `golangci-lint run ./...` before handoff (CI also checks gofumpt + gci).

## Simplicity and maintainability

Simple, readable code is a design requirement, not a cleanup task for a later PR.

- Start with the smallest function or concrete type that satisfies the current behavior. Add an interface, lifecycle phase, state holder, or wrapper only when it owns a distinct invariant or removes more complexity than it introduces.
- A hypothetical future feature alone does not justify a new abstraction. Name the concrete requirement, show how the abstraction serves it, and cover that extension point with a test or benchmark.
- Keep the main execution path easy to follow from entrypoint to side effect. One decision and one piece of mutable state should have one owner; avoid duplicate state, forwarding-only layers, and parallel representations of the same policy.
- Prefer composition of small domain operations over generic framework vocabulary. Do not introduce multi-stage `Plan` / `Compile` / `Runtime`-style pipelines when a direct function or immutable value expresses the same behavior.
- In a refactor, justify every new exported or package-level entity and delete superseded scaffolding. A refactor that merely moves complexity or increases the number of concepts is not complete.
- Measure hot-path changes with a benchmark that can be compared with the previous implementation. Do not trade debuggability or obvious control flow for speculative performance.
- During review, explicitly ask: can a reader diagnose behavior and add the next known feature by changing fewer concepts than before? If not, simplify before merge.

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
