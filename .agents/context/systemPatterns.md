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
- Created on demand via `pool.Get(endpoint)`; tracks TTL, dial options, trace.
- Used by balancer to obtain `grpc.ClientConn` for a chosen node.
- `conn.Conn` includes `Close(ctx) error` — closes gRPC transport and removes the entry from the pool via `withOnClose(p.remove)` registered at `Get` time (removal happens in `(*conn).Close` defer, not in pool API).
- `pool.Release()` still closes **all** connections on driver shutdown.
- gRPC has no dial option to disable background reconnect; only `ClientConn.Close()` stops orphaned reconnect loops.

### 2. YDB session pool (`internal/pool/pool.go`)

- Generic pool used by **table** and **query** clients for YDB `Session` objects.
- Configurable: limit, warm-up size, idle TTL, usage limit/TTL, create/delete timeouts.
- `MustDelete` predicate drops sessions on `xerrors.MustDeleteTableOrQuerySession(err)`.

Table and query each maintain their own session pool instance inside `internal/table.Client` / `internal/query.Client`.

Query additionally has **implicit** session pool for server-side session management (see `internal/query/client.go`).

## Balancer and discovery (`internal/balancer/`)

```
Balancer
  ├── clusterDiscovery()     # periodic / on-init endpoint list refresh
  ├── discover endpoints     # via Discovery gRPC (or static single endpoint)
  ├── applyDiscoveredEndpoints()
  │     ├── update connectionsState (prefer/fallback/all)
  │     └── closeDroppedConns()  # retire pool conns absent from discovery
  ├── localDCDetector        # for PreferNearestDC balancers
  ├── filter by balancerConfig (RandomChoice, PreferNearestDC, location filters)
  └── wrapCall / wrapStream  # ban endpoint on retriable connection failures
```

### Discovery update → routing state

`applyDiscoveredEndpoints` builds `connections` via `conn.EndpointsToConnections(pool, newest)`, refreshes `connectionsState`, and traces added/dropped endpoints (`OnBalancerUpdate`).

`connectionsState` (`connections_state.go`) holds `prefer`, `fallback`, `all` as `[]conn.Conn`; `All()` returns endpoint metadata for trace diffs; private `conns()` returns the previous `all` slice for drop handling.

### Orphaned gRPC connections (endpoints removed from discovery)

**Symptom:** endpoint disappears from `ListEndpoints` → balancer stops routing, but `pool.conns` still holds `grpc.ClientConn` → background reconnect logs (`addrConn.createTransport failed`, `no such host`, …).

**Fix (`closeDroppedConns`):** after each discovery update, compare previous `connectionsState.conns()` with `newest` endpoints:

- Newly absent conns enter `Balancer.dropCandidates` (`map[endpoint.Key]*dropCandidate` with `conn` + `missed` counter).
- Each subsequent discovery without the endpoint increments `missed`.
- After `droppedConnCloseAfterMissedDiscoveries` (3) misses → `conn.Close(ctx)` (debounce so in-flight RPCs on a just-dropped node are not cut immediately).
- Reappearing endpoint clears the candidate.

**Edge cases:**

- `DiscoveryInterval == 0` — discovery repeater does not tick repeatedly; `missed` never reaches the threshold → no proactive closes (static / single-shot discovery).
- `ClientConn.Close()` is synchronous in grpc-go — discovery repeater blocks during close; risk if long streams still run on a dropped conn.
- Trace `OnBalancerUpdate` `dropped` list is for observability only; it does not close connections by itself.

**Test:** `TestApplyDiscoveredEndpoints_StopsGRPCReconnectToDroppedEndpoint` (`balancer_test.go`).

**Intentionally not added:** `conn.Conns` wrapper type, `Pool.CloseConn`, close-reason in driver trace (`WithCloseCause` / `DriverConnCloseStartInfo.Cause`).

**Dead code removed:** `endpointsInfo` in `applyDiscoveredEndpoints` — leftover from removed `balancer.OnUpdate` (Nov 2024); was unused since callbacks were deleted.

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

## Adding a new RPC surface

1. Confirm protobuf in `ydb-go-genproto`.
2. Add `internal/grpcwrapper/` or service-specific raw client methods.
3. Implement `internal/<service>/client.go` using `balancerWithMeta` as `grpc.ClientConnInterface`.
4. Expose public package with stable types; add `trace/` callbacks + regenerate.
5. Wire lazy `xsync.OnceValue` factory in `driver.go` `connect()`.
6. Unit tests co-located; `tests/integration/` if server interaction needed.

Checklist and coding anti-patterns: [`.agents/rules/coding-standards.md`](../rules/coding-standards.md).
