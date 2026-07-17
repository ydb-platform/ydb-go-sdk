# System Patterns

## Repository layout

```
ydb-go-sdk/
├── driver.go, with.go, sql.go    # ydb.Open, Driver, ydb.Option, database/sql driver
├── table/, query/, scheme/, topic/ # public service client packages (+ types/options/result)
├── coordination/, discovery/, scripting/, operation/, ratelimiter/
├── config/, balancers/, credentials/, retry/, sugar/, trace/
├── log/, metrics/, spans/        # trace adapters
├── internal/                     # implementations (unstable; do not expose in public API)
├── tests/integration/            # //go:build integration
├── examples/                     # separate go.mod
└── .agents/                      # agent workspace
```

Public packages define interfaces and option types. Implementations live under `internal/<service>/` and are wired in `driver.go`.

## Driver lifecycle

### Entry points

| API | File | Role |
|-----|------|------|
| `ydb.Open(ctx, dsn, opts...)` | `driver.go` | Parse DSN, apply `ydb.Option`s, connect, return `*Driver` |
| `ydb.New(ctx, opts...)` | `driver.go` | Same without DSN in args (endpoint/database from options) |
| `sql.Open("ydb", dsn)` | `sql.go` | Registers `database/sql` driver; uses same `Driver` underneath |
| `ydb.Connector(driver)` | `internal/xsql` | Wrap native driver for `sql.OpenDB` |

`ydb.Option` functions (`with.go`) mutate `*Driver` before connect: credentials, balancer, trace, timeouts, table/query config, etc.

### `connect()` sequence (`driver.go`)

```
driverFromOptions()
  → apply env defaults (YDB_SSL_ROOT_CERTIFICATES_FILE, YDB_LOG_SEVERITY_LEVEL)
  → apply user opts
  → build config.Config

connect(ctx)
  1. conn.NewPool(ctx, config)           # gRPC connection pool (if not preset)
  2. balancer.New(ctx, config, pool)     # discovery + endpoint selection
  3. metaBalancer.meta = config.Meta()   # request metadata (database, credentials hints)
  4. xsync.OnceValue per service client  # lazy init on first Table()/Query()/...
```

Service clients are **lazy**: `d.Table()` calls `d.table.Must()` which runs the `OnceValue` factory only once.

### `balancerWithMeta` — shared RPC transport

All service clients receive the same `*balancerWithMeta` (implements `grpc.ClientConnInterface`):

- `Invoke()` / `NewStream()` inject `meta.Meta` into context, then delegate to `internal/balancer.Balancer`.
- `Balancer` picks an endpoint, gets `conn.Pool.Get(endpoint)`, executes RPC.
- On stream/call errors that indicate a bad connection, the endpoint may be **banned** (pessimization) and rediscovered.

This is the single production path for gRPC — do not dial around the balancer.

## Two pool layers

### 1. gRPC connection pool (`internal/conn/pool.go`)

- One `*conn` per `endpoint.Endpoint` (host:port + node metadata).
- `Get` / `Put` reference-count pooled wrappers; gRPC dial is lazy on first RPC.
- Refcount and map updates run under `p.mu` with `defer p.mu.Unlock()` in helpers (`tryPut`, `release`); blocking `Close()` runs **after** the helper returns. See mutex rules in [`.agents/rules/coding-standards.md`](../rules/coding-standards.md).
- Used by balancer to obtain `grpc.ClientConn` for a chosen node.

Connection state lifecycle:

```text
Created → Online ↔ Banned
             ↕
           Offline
             ↓
          Destroyed
```

- `newConn` stores `Created` directly; there is no state-change event at wrapper creation. The first successful lazy dial emits `Created → Online`.
- `Ban` moves the wrapper to `Banned`; `Unban` chooses `Online` or `Offline` from the underlying gRPC transport readiness.
- Closing the gRPC transport moves the wrapper to `Offline`; closing the wrapper finishes with `Destroyed`.
- `conn.setState` is the central transition point and emits `trace.Driver.OnConnStateChange` with the previous and new states. `Online`, `Offline`, and `Banned` are valid operational states; `Created`, `Destroyed`, and `Unknown` are lifecycle sentinels.

### Driver connection metrics

- The `metrics.Registry` API is push-based (`Gauge.Add` / `Gauge.Set`); it has no scrape-time callback or connection-pool snapshot API.
- Derive current connection state from the existing `OnConnStateChange` trace. Moving one connection between valid states decrements the old state and increments the new state, so `sum(conns)` remains invariant. Entering the first valid state increments the sum; moving to `Destroyed` decrements it.
- Do not create a `Created` series from the first `Created → Online` transition: there was no preceding creation event, so decrementing it would produce `-1`.
- A ban with `cause` is an event and belongs in a counter. Current banned connection cardinality belongs in the `state="banned"` partition of the connection gauge.
- Adding `state` changes the raw `conns` series shape, but dashboards using only `sum(conns)` retain their query and intended total. Direct selectors, grouping, joins, and alerts must account for the new label.
- The former dial/close event accounting could drift: redial could add twice after an internal transport reset, while closing a never-dialed wrapper could subtract without a prior add. State transitions avoid both cases.

### 2. YDB session pool (`internal/pool/pool.go`)

- Generic pool used by **table** and **query** clients for YDB `Session` objects.
- Configurable: limit, warm-up size, idle TTL, usage limit/TTL, create/delete timeouts.
- `MustDelete` predicate drops sessions on `xerrors.MustDeleteTableOrQuerySession(err)`.

Table and query each maintain their own session pool instance inside `internal/table.Client` / `internal/query.Client`.

Query additionally has **implicit** session pool for server-side session management (see `internal/query/client.go`).

### Session-pool reuse and expiration

- Session expiration is lazy: idle TTL, usage TTL, and usage-limit checks run while an item is acquired from the pool. There is no background idle-session reaper in `internal/pool`.
- Commit `0d2a081093671731fea36ffbbb62853b99a3133f` (`perf: internal.Pool with semaphore (#2163)`) shipped in v3.138.0 and changed idle-item reuse to a slice-backed LIFO path. This improved hot-item reuse, but a naive LIFO path can permanently hide older idle sessions underneath a frequently reused item.
- The resulting failure mode is visible during rolling restarts: the client continues creating sessions while old server sessions await cleanup, even when `WithSessionPoolIdleThreshold` is configured. Repeated connection bans or `OVERLOADED` responses may accompany the incident, but they do not explain sessions that never reach the pool's idle-expiration check.
- Preserve the LIFO fast path for valid items, but give the oldest item an expiration opportunity. On the first acquisition attempt, check the oldest item for idle TTL, usage TTL, or usage-limit expiry; close it if expired, otherwise acquire through normal LIFO/node-hint selection. Do not run the liveness predicate merely to inspect the oldest item; the selected item still goes through the normal full close check.
- The slice container supports O(1) oldest removal with a logical `head` and amortized compaction. All length, limit, clear, LIFO, and node-specific operations must account for `head`.
- Regression-test shape: create several sessions with a fake clock, refresh only the newest LIFO session, advance beyond idle TTL for the older sessions, and verify repeated operations close the old sessions while continuing to use the hot one. This test failed before the fix because the old sessions were never selected.

### Session metrics: client state vs server state

- `ydb.table.sessions{node_id}` is a client lifecycle gauge from `metrics/table.go`: it increments after successful session creation and decrements when deletion starts. It does **not** confirm that `DeleteSession` succeeded on the server.
- `ydb.table.pool.index`, `idle`, `in_use`, `createInProgress`, `concurrency`, and `wait` are pool-state snapshots derived from `TablePoolStateChange`. `pool.index` is the pool's current `Size`, so it can legitimately differ from `ydb.table.sessions`.
- Driver `request_methods{method,node_id,...}` and `request_statuses{status,node_id,...}` expose gRPC attempt traffic and outcomes, but method and status are separate counter families; they cannot reliably reconstruct an operation-by-status matrix without additional instrumentation.
- For a server-session leak investigation, use YDB's `table_session_active_count` as the authoritative active-session count and correlate it with SDK pool gauges and CreateSession/DeleteSession request counters.

## Balancer and discovery (`internal/balancer/`)

```
Balancer
  ├── clusterDiscovery()     # periodic / on-init endpoint list refresh
  ├── discover endpoints     # via Discovery gRPC (or static single endpoint)
  ├── localDCDetector        # for PreferNearestDC balancers
  ├── filter by balancerConfig (RandomChoice, PreferNearestDC, location filters)
  └── wrapCall / wrapStream  # ban endpoint on retriable connection failures
```

User-facing presets: `balancers/RandomChoice`, `SingleConn`, `PreferNearestDC`, `PreferNearestDCWithFallBack`.

Configured via `config.WithBalancer(...)` or `ydb.WithBalancer(...)`.

`Driver.Discovery()` exposes a separate discovery client (uses bootstrap connection from pool, not the main balancer loop).

## Service client map

| `Driver` accessor | Public package | Internal impl | Session pool? | Notes |
|-------------------|----------------|---------------|---------------|-------|
| `Table()` | `table/` | `internal/table/` | Yes | `Do`/`DoTx`, bulk ops |
| `Query()` | `query/` | `internal/query/` | Yes (+ implicit) | Streaming `ResultSets`, `Do`/`DoTx` |
| `Scheme()` | `scheme/` | `internal/scheme/` | No | Directory, describe path |
| `Topic()` | `topic/` | `internal/topic/` | No | Reader/writer/listener; own connection semantics |
| `Coordination()` | `coordination/` | `internal/coordination/` | No | Semaphores, distributed locks |
| `Scripting()` | `scripting/` | `internal/scripting/` | No | YQL scripts (legacy path) |
| `Ratelimiter()` | `ratelimiter/` | `internal/ratelimiter/` | No | Quota control |
| `Discovery()` | `discovery/` | `internal/discovery/` | No | Endpoint discovery API |
| `Operation()` | `operation/` | `operation/` | No | Long-running ops (experimental) |

`database/sql` path: `internal/xsql` connector acquires table sessions via the same table client stack.

## Do / DoTx pattern

Canonical pattern for **table** and **query** (public API mirrors internal):

```go
err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
    rs, err := s.Query(ctx, "SELECT 1")
    if err != nil { return err }
    defer func() { _ = rs.Close(ctx) }()
    // ...
    return nil  // success → exit retry loop
}, query.WithIdempotent())
```

Internal flow (`internal/table/client.go`, `internal/query/client.go`):

```
Do(ctx, op, opts)
  → merge retry options (label, idempotent, backoff, trace)
  → retry loop (retry.Retry / do+backoff)
      → sessionPool.Get(ctx)     # acquire Session
      → op(ctx, session)         # user callback
      → on error: classify retryable, maybe delete session, retry
      → on success: return nil
```

`DoTx`: acquire session → `BeginTransaction` → user op → `Commit` (rollback on error). Retries whole transaction when safe.

**Rules for agents:**

- Return non-nil error from callback to trigger retry; nil means success.
- Use `WithIdempotent()` for operations safe to repeat.
- Always `Close()` result sets / streams in callback.
- Unbounded `context.Context` can retry indefinitely — use deadlines.

## Retry package (`retry/`)

- `retry.Retry()` — core loop with fast/slow backoff (`internal/backoff`).
- `xerrors.Retryable(err)` — mark errors for retry.
- `retry/budget/` — optional retry budget from driver config.
- Wired into `Do`/`DoTx` and balancer discovery.

## Trace and codegen

- Callback structs in `trace/` (per service).
- `go generate ./trace` → `*_gtrace.go` wrappers in public and internal packages.
- `internal/cmd/gstack/` — `stack.FunctionID` for trace spans.
- CI `check-codegen.yml` enforces clean generated diff.

## `database/sql` integration

- Driver registered as `"ydb"` in `sql.go`.
- `internal/xsql/connector.go` — `CreateSession` uses native table client; balancing happens at session creation.
- See `SQL.md` for DSN params, balancing, and connector options.

Topic / multiwriter details (load only when needed): [`topicContext.md`](topicContext.md), [`topicMultiwriterContext.md`](topicMultiwriterContext.md).

## Adding a new RPC surface

1. Confirm protobuf in `ydb-go-genproto`.
2. Add `internal/grpcwrapper/` or service-specific raw client methods.
3. Implement `internal/<service>/client.go` using `balancerWithMeta` as `grpc.ClientConnInterface`.
4. Expose public package with stable types; add `trace/` callbacks + regenerate.
5. Wire lazy `xsync.OnceValue` factory in `driver.go` `connect()`.
6. Unit tests co-located; `tests/integration/` if server interaction needed.

Checklist and coding anti-patterns: [`.agents/rules/coding-standards.md`](../rules/coding-standards.md).
