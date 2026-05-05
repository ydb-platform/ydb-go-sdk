# Query vs Table: `database/sql` Benchmarks

Two benchmarks comparing QueryService vs TableService performance over the `database/sql` interface, with saved CPU/memory profiles and execution traces.

## Benchmarks

### `BenchmarkDatabaseSQLMock`

File: `database_sql_mock_bench_test.go`.

Runs an in-process gRPC mock (Discovery + Table + Query) with fixed responses for `SELECT 42`. No real YDB cluster is needed.

Runs sub-benchmarks **`QueryService`** and **`TableService`**: the same `db.QueryContext(ctx, "SELECT 42")` call, but the connector is built with `ydb.WithQueryService(true)` or `false`. Parallelism levels: 1 and 100 goroutines. The session pool is warmed up before measurements begin.

Useful for profiling the pure client/SDK overhead without network round-trips to a real YDB cluster.

### `BenchmarkDatabaseSQL`

File: `database_sql_over_query_bench_test.go` (build tag: **`integration`**).

Same scenario (`SELECT 42` via `database/sql`) but connects to a real cluster specified by the **`YDB_CONNECTION_STRING`** environment variable.

## Artifact Naming

- **`mock-*.prof`**, **`mock-*.trace`** — captured from the mock benchmark.
- **`local-ydb-*.prof`**, **`local-ydb-*.trace`** — captured against a local/real YDB cluster.

Suffixes like `query`, `table`, `mixed` are set when recording (separate run per engine or combined).

## CPU and Memory Profiles (pprof)

Open an interactive UI in the browser:

```bash
go tool pprof -http=localhost:8080 mock-cpu-query.prof
```

For a memory profile:

```bash
go tool pprof -http=localhost:8080 mock-mem-query.prof
```

Useful views in the web UI: flame graph, top, source listing. Change the port if `8080` is in use.

Saved reports in this directory:

- [CPU flame graph (SVG)](pprof001.svg)
- [CPU profile (HTML)](integration.test%20cpu.html)

Record profiles while running the benchmark (from the module root):

```bash
go test ./tests/integration -run=^$ -bench=BenchmarkDatabaseSQLMock -benchmem \
  -cpuprofile=tests/integration/mock-cpu-mixed.prof \
  -memprofile=tests/integration/mock-mem-mixed.prof
```

For the integration benchmark, add `-tags=integration` and set `YDB_CONNECTION_STRING`.

## Execution Trace (`go tool trace`)

`.trace` files are Go runtime execution traces (goroutines, blocking, GC, CPU samples, etc.).

```bash
go tool trace tests/integration/local-ydb-mixed.trace
```

The command starts a local server and prints a URL; open it in a browser for the timeline and related views.

Record a trace while running:

```bash
go test ./tests/integration -run=^$ -bench=BenchmarkDatabaseSQLMock \
  -trace=tests/integration/mock-mixed.trace
```

---

## Benchmark Results (Apple M3 Pro, Go 1.26)

```
BenchmarkDatabaseSQLMock/QueryService-1-12     11082   106915 ns/op   28950 B/op   478 allocs/op
BenchmarkDatabaseSQLMock/QueryService-100-12   12282    99268 ns/op   31535 B/op   518 allocs/op
BenchmarkDatabaseSQLMock/TableService-1-12     14702    80738 ns/op   19746 B/op   325 allocs/op
BenchmarkDatabaseSQLMock/TableService-100-12   13524    79155 ns/op   21386 B/op   349 allocs/op
```

**QueryService overhead vs TableService:**

| Metric | QueryService | TableService | Delta |
|---|---|---|---|
| Latency (ns/op) | ~107,000 | ~81,000 | **+32%** |
| Memory (B/op) | ~29,000 | ~20,000 | **+47%** |
| Allocations | ~478 | ~325 | **+47% (+153 allocs/op)** |

---

## Profile Analysis and Optimization Opportunities

The following bottlenecks were identified by analysing `mock-mem-query.prof` vs `mock-mem-table.prof`
using `go tool pprof -top -alloc_objects`.

### #1 — `io.EOF` wrapped with `xerrors.WithStackTrace` (highest impact)

**Location:** `internal/query/result.go` — `nextPart()` and `materializedResult.NextResultSet()`

`io.EOF` is a control-flow sentinel value signalling end-of-stream, not a real error. Both functions
unconditionally wrap it with `xerrors.WithStackTrace()`:

```go
// nextPart — called on every stream.Recv() that ends the stream
func nextPart(stream ...) (...) {
    part, err = stream.Recv()
    if err != nil {
        return nil, xerrors.WithStackTrace(err) // wraps io.EOF every time
    }
    ...
}

// materializedResult.NextResultSet — called after every query to detect end-of-results
func (r *materializedResult) NextResultSet(...) (...) {
    if r.idx == len(r.resultSets) {
        return nil, xerrors.WithStackTrace(io.EOF) // always wraps io.EOF
    }
    ...
}
```

Each `WithStackTrace` call triggers: `stack.Record()` → `runtime.Caller()` → `runtime.FuncForPC()`
→ multiple string allocations. Profile evidence:

| Symbol | Flat allocs | Cumulative allocs |
|---|---|---|
| `xerrors.WithStackTrace` | 895 K | 7.7 M |
| `stack.FunctionID` | 3.0 M | 3.0 M |
| `stack.call.Record` | 830 K | 4.1 M |
| `stack.parseFunctionName` | 1.0 M | 2.0 M |
| `xerrors.(*stackError).Error` | 1.1 M | 1.1 M |

TableService has only ~32 K flat / ~598 K cumulative for `WithStackTrace` — a **28× difference**.

**Fix:** Return `io.EOF` directly without wrapping. All callers already check via `errors.Is(err, io.EOF)`.

```go
func nextPart(stream ...) (...) {
    part, err = stream.Recv()
    if err != nil {
        if err == io.EOF {
            return nil, io.EOF
        }
        return nil, xerrors.WithStackTrace(err)
    }
    return part, nil
}
```

Estimated saving: **~70–100 allocs/op** (roughly half the QueryService excess).

---

### #2 — `context.AfterFunc` in `ResultCloser.CloseOnContextCancel`

**Location:** `internal/query/result_closer.go:89`

```go
func (r *ResultCloser) CloseOnContextCancel(ctx context.Context) func() bool {
    return context.AfterFunc(ctx, func() { // allocates goroutine + 2 objects per call
        r.Close(ctx.Err())
    })
}
```

This is called from `nextPart` on every gRPC message receive, meaning it fires once per part
(≥2 times per `SELECT 42` query). `context.AfterFunc` registers an async goroutine callback and
allocates 2+ objects every time.

Profile evidence: `CloseOnContextCancel` — 376 K flat / 938 K cumulative allocs (~11.5 MB flat / 47.5 MB cumulative).

**Fix candidates:**
- Check whether `CloseOnContextCancel` is still needed now that each `nextPart` already has an
  explicit `select { case <-ctx.Done(): ... default: ... }` guard.
- If necessary, register the AfterFunc once per stream instead of once per part, and stop
  it when the stream completes.

---

### #3 — Per-row scan buffer allocation in `xquery.rows.Next`

**Location:** `internal/xsql/xquery/rows.go:145`

```go
values := xslices.Transform(make([]value.Value, len(r.allColumns)), func(v value.Value) any { return &v })
```

A new `[]value.Value` and `[]any` are allocated on every `rows.Next` call. For single-row
results this fires once per query; for multi-row result sets the cost compounds.

**Fix:** Pre-allocate `values []value.Value` and `scanDst []any` in the `rows` struct and reset
the length before each scan. These slices are safe to reuse because `rows` is never accessed
concurrently while `Next` is running.

---

### #4 — `sync.OnceFunc` closure allocation in `lastUsage.Start()`

**Location:** `internal/xsync/last_usage_guard_start.go:10`

```go
func (guard *lastUsage) Start() (stop func()) {
    guard.locks.Add(1)
    return sync.OnceFunc(func() { // allocates a closure every call
        if guard.locks.Add(-1) == 0 {
            now := guard.clock.Now()
            guard.t.Store(&now)
        }
    })
}
```

`sync.OnceFunc` creates a new heap-allocated closure each call. Called 884 K times for QueryService
vs 327 K for TableService (2.7× more). Profile: 884 K flat allocs / 13.5 MB flat.

**Fix:** Replace `sync.OnceFunc` with a simple atomic flag (e.g., `sync.Once` stored in the
returned stop-func struct, or track call count with a per-call atomic). This avoids the heap
allocation entirely when the stop function is called in the same goroutine.

---

### #5 — `updateColumns` rebuilds column metadata slices on every result set

**Location:** `internal/xsql/xquery/rows.go:43`

```go
func (r *rows) updateColumns() {
    r.columns = make([]string, 0, len(r.allColumns)) // new slice every call
    r.discarded = make([]bool, len(r.allColumns))    // new slice every call
    ...
}
```

Profile: 294 K flat / 753 K cumulative allocs.

**Fix:** Reuse existing slices by resetting their length only (`r.columns = r.columns[:0]`,
`r.discarded = r.discarded[:0]`), and grow them only when capacity is insufficient. This is safe
because `updateColumns` is only called via `sync.Once` on the first result set, or from
`NextResultSet` while the driver holds exclusive access.

---

### #6 — `executeQueryRequest` allocates a new proto struct per query

**Location:** `internal/query/execute_query.go:87`

```go
request := &Ydb_Query.ExecuteQueryRequest{...}
```

Profile: 281 K flat allocs (~20.5 MB flat). Every `SELECT 42` allocates a full
`ExecuteQueryRequest` plus a nested `QueryContent`. TableService has an equivalent cost in
`ExecuteDataQueryRequest`, so this is not a QueryService-specific regression, but it
contributes to the absolute allocation count.

**Fix:** Investigate `sync.Pool` or proto `Reset()`-based recycling for the request struct.
This is higher-risk due to protobuf ownership rules but could reduce GC pressure under high load.

---

### Summary Table

| # | Location | Flat allocs | Cumulative allocs | Difficulty | Impact |
|---|---|---|---|---|---|
| 1 | `nextPart` / `NextResultSet`: don't wrap `io.EOF` | 895 K | 7.7 M | Low | High |
| 2 | `CloseOnContextCancel`: register AfterFunc once per stream | 376 K | 938 K | Medium | Medium |
| 3 | `rows.Next`: reuse scan value buffer | — | 4.5 MB | Low | Medium |
| 4 | `lastUsage.Start`: avoid `sync.OnceFunc` closure | 884 K | — | Medium | Medium |
| 5 | `updateColumns`: reuse column slices | 294 K | 753 K | Low | Low |
| 6 | `executeQueryRequest`: pool proto request struct | 281 K | — | High | Low |

Fixing items 1–4 should close the majority of the ~47% allocation gap between QueryService and
TableService. The remaining gap is largely structural: QueryService uses one HTTP/2 stream per
query (for true server-side streaming) while TableService uses unary RPCs, which incurs less
per-query gRPC bookkeeping.
