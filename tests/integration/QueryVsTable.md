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

### Summary Table (Round 1 — initial analysis)

| # | Location | Flat allocs | Cumulative allocs | Difficulty | Impact | Tested result |
|---|---|---|---|---|---|---|
| 1 | `nextPart` / `NextResultSet`: don't wrap `io.EOF` | 895 K | 7.7 M | Low | High | ✅ **−15% allocs, −6% latency** (PR #2123) |
| 2 | `CloseOnContextCancel`: register AfterFunc once per stream | 376 K | 938 K | Medium | Medium | ❌ No improvement (PR #2125) |
| 3 | `rows.Next`: reuse scan value buffer | — | 4.5 MB | Low | Medium | ❌ No improvement (PR #2127) |
| 4 | `lastUsage.Start`: avoid `sync.OnceFunc` closure | 884 K | — | Medium | Medium | ❌ No improvement (PR #2129) |
| 5 | `updateColumns`: reuse column slices | 294 K | 753 K | Low | Low | ❌ No improvement (PR #2131) |
| 6 | `executeQueryRequest`: pool proto request struct | 281 K | — | High | Low | ❌ No improvement (PR #2133) |

---

## Round 2 — Deeper Analysis

After applying fix #1, the remaining gap (after PR #2123) is approximately:
- Latency: **+24%** vs TableService
- Memory: **+25%** vs TableService
- Allocations: **+30% (~98 allocs/op)** vs TableService

### Root cause discovery: `xcontext.cancelCtx.cancel()` triggers expensive stack recording

The key finding from a second pass over `mock-mem-query.prof` vs `mock-mem-table.prof` is a
**causal chain** that is entirely absent from the TableService profile:

```
xcontext.cancelCtx.cancel()
  └─ xcontext.errAt(context.Canceled, 1)       [349 K flat allocs]
       └─ stack.Record(skipDepth + 1)
            └─ stack.Call(depth+1)              [1,575 K flat allocs]
                 └─ runtime.Caller(...)         (syscall — not counted separately)
                 └─ stack.call.Record(...)      [830 K flat allocs]
                      └─ stack.parseFunctionName [1,046 K flat allocs]
                      └─ stack.buildRecordString  [131 K flat allocs]
                      └─ strings.Split / genSplit  [983 K flat allocs]
```

Total: **~4.9 M allocs** from this chain — **100% absent from the TableService profile**.

#### Why does it fire?

`xcontext.cancelCtx` is the custom cancel context in `internal/xcontext/context_with_cancel.go`.
Its `cancel()` method calls `errAt(context.Canceled, 1)` which captures the call site via
`stack.Record()` (= `runtime.Caller` + `runtime.FuncForPC` + string parsing). This is excellent
for debugging — when a context is cancelled you get an error like
`'context canceled' at internal/query/execute_query.go:133` — but it fires on **every normal query
completion**, not just on errors.

The QueryService path creates **two `xcontext.cancelCtx` contexts per query**:

1. **`internal/query/execute_query.go:125`**
   ```go
   executeCtx, executeCancel := xcontext.WithCancel(xcontext.ValueOnly(ctx))
   ```
   Cancelled in the `defer stop()` after every successful stream read.

2. **`internal/conn/conn.go:485`** (via `CancelsGuard.WithCancel`)
   ```go
   ctx, cancel := c.childStreams.WithCancel(ctx)
   ```
   `CancelsGuard.WithCancel` calls `xcontext.WithCancel` internally. Cancelled when the stream
   finishes (in `grpcClientStream.finish`).

TableService uses **unary `cc.Invoke`**, which never calls `xcontext.WithCancel`, so the entire
chain is absent.

Additionally, creating each `xcontext.cancelCtx` allocates the custom struct plus an inner
`context.WithCancel(parent)`:

| Symbol | Flat allocs (query) | Flat allocs (table) |
|---|---|---|
| `xcontext.WithCancel` | 557 K | 0 |
| `xcontext.CancelsGuard.WithCancel` | 262 K | 0 |
| `xcontext.errAt` (cancel-time) | 349 K | 0 |
| `stack.Call` (inside errAt) | 1,575 K | 0 |
| `stack.call.Record` | 830 K | 0 |
| `stack.parseFunctionName` | 1,046 K | 0 |
| **Total** | **~4,619 K** | **0** |

---

### #7 — Replace `xcontext.WithCancel` with `context.WithCancel` in the streaming execute path

**Location 1:** `internal/query/execute_query.go:125`

```go
// Before — allocates xcontext.cancelCtx + records stack on every cancel
executeCtx, executeCancel := xcontext.WithCancel(xcontext.ValueOnly(ctx))

// After — plain cancel context, no stack recording on cancel
executeCtx, executeCancel := context.WithCancel(xcontext.ValueOnly(ctx))
```

**Location 2:** `internal/xcontext/cancels_guard.go:23` (used by `conn.NewStream`)

```go
// Before
ctx, cancel := WithCancel(ctx)   // xcontext.WithCancel: custom cancelCtx + errAt on cancel

// After
ctx, cancel := context.WithCancel(ctx)  // standard cancel context: no errAt overhead
```

The custom stack-enriched error is only useful in the query/topic subsystem for surfacing WHERE
a context was cancelled. For the stream execution hot path the error is immediately discarded —
the caller checks `ctx.Err()` or `xerrors.IsContextError(err)` anyway.

**Expected savings: ~50–70 allocs/op**, eliminating the entire `errAt → stack.Record` chain from
the benchmark profile.

**Risk:** Low for `execute_query.go` (cancel error value is immediately thrown away). Moderate for
`CancelsGuard.WithCancel` (used across multiple subsystems — check callers before changing).

---

### #8 — `stack.FunctionID` interface boxing on every streaming recv/send

**Location:** `internal/conn/grpc_client_stream.go` — `RecvMsg`, `SendMsg`, `CloseSend`, `finish`

```go
// Called on every RecvMsg (≥2× per query):
onDone = trace.DriverOnConnStreamRecvMsg(s.parentConn.config.Trace(), &ctx,
    stack.FunctionID("github.com/.../RecvMsg"),  // boxes functionID string as Caller interface — 1 alloc
)
```

`stack.FunctionID(id string)` returns `functionID(id)` (a string type) as a `Caller` interface.
Boxing a 16-byte string into an interface value requires a heap allocation because the value does
not fit in a single pointer word.

In TableService the equivalent `Invoke` call also boxes once, but `RecvMsg` fires **2+ times per
query**, so QueryService boxes 4–5 extra interface values per query.

Profile evidence: `stack.FunctionID` — 3.08 M flat allocs (query) vs 950 K (table), a **2.1 M
extra allocations** difference.

**Fix:** Pre-allocate the `Caller` interface values as package-level variables:

```go
// In grpc_client_stream.go — package level (boxed once at init):
var (
    _callerCloseSend = stack.FunctionID("github.com/.../CloseSend")
    _callerSendMsg   = stack.FunctionID("github.com/.../SendMsg")
    _callerRecvMsg   = stack.FunctionID("github.com/.../RecvMsg")
    _callerFinish    = stack.FunctionID("github.com/.../finish")
)

// In RecvMsg:
onDone = trace.DriverOnConnStreamRecvMsg(s.parentConn.config.Trace(), &ctx, _callerRecvMsg)
```

Passing a pre-boxed interface variable is just a 2-word copy — no new heap allocation.

**Expected savings: ~5 allocs/op.** Modest but zero-risk change.

---

### #9 — `context.(*cancelCtx).propagateCancel` overhead from deep cancel chains

**Location:** Go standard library — called whenever `context.WithCancel(parent)` is used with
a cancellable parent.

Profile: 1.24 M flat allocs (query) vs **0** (table).

`propagateCancel` allocates to register the new child context in the parent's cancellation list.
This is unavoidable for correct context propagation — but the QueryService creates **more cancel
layers per request** than TableService:

| Layer | Query path | Table path |
|---|---|---|
| Session-level cancel | `xcontext.WithDone(ctx, s.Done())` | none |
| Execute-level cancel | `xcontext.WithCancel(ValueOnly(ctx))` | none |
| Stream-level cancel | `c.childStreams.WithCancel(ctx)` | none |
| AfterFunc (parent→execute) | `context.AfterFunc(ctx, executeCancel)` | none |

Each `WithCancel` with a cancellable parent runs `propagateCancel`, allocating a tree node.

**Fix direction:** Reduce the number of cancel layers by combining them where possible. For example,
`execute_query.go` already has both `xcontext.WithCancel` and an explicit `context.AfterFunc(ctx,
executeCancel)`. With standard `context.WithCancel(ValueOnly(ctx))` replacing `xcontext.WithCancel`
(see #7), the `AfterFunc` still propagates parent cancellation without the extra `cancelCtx` struct.

---

### Round 2 Summary Table

| # | Location | Flat allocs (query only) | Difficulty | Est. impact |
|---|---|---|---|---|
| 7 | Replace `xcontext.WithCancel` with `context.WithCancel` in `execute_query.go` and `CancelsGuard` | ~4,619 K | Low–Medium | **~50–70 allocs/op** |
| 8 | Pre-allocate `stack.FunctionID` callers as package-level vars in `grpc_client_stream.go` | ~2,100 K extra | Very Low | ~5 allocs/op |
| 9 | Reduce cancel-chain depth in the execute path | ~1,244 K | Medium | ~10 allocs/op |

Item #7 is the highest-priority new candidate. It eliminates the entire `errAt → stack.Record`
allocation chain that is **completely absent from the TableService profile** but fires on every
normal query completion in QueryService.
