# System Patterns

## Repository structure

```
ydb-go-sdk/
├── driver.go, sql.go, with.go     # ydb.Open, Driver, options
├── table/, query/, scheme/, topic/ # public service client packages
├── coordination/, discovery/, scripting/, operation/, ratelimiter/
├── config/, balancers/, credentials/, retry/, sugar/
├── trace/, log/, metrics/, spans/ # observability
├── internal/                      # implementations (unstable)
│   ├── table/, query/, scheme/, topic/, conn/, balancer/, pool/, ...
│   └── cmd/gtrace/, cmd/gstack/   # codegen tools
├── tests/integration/             # //go:build integration
├── examples/                      # separate go.mod
└── .agents/                       # agent workspace
```

## Layered architecture

```
ydb.Open(ctx, dsn, opts...)
    └── Driver
            ├── Table()      → table.Client      → internal/table/
            ├── Query()      → query.Client      → internal/query/
            ├── Scheme()     → scheme.Client     → internal/scheme/
            ├── Topic()      → topic.Client      → internal/topic/
            └── ...

Driver
    └── internal/balancer/     # discovery, endpoint selection, ban/pessimization
            └── internal/conn/ # gRPC connection pool per endpoint

table.Client / query.Client
    └── internal/pool/         # YDB session pool
            └── internal/grpcwrapper/
```

## Key patterns

### Do / DoTx

Canonical user API for table and query services:

```go
err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
    // return err to retry; return nil on success
}, query.WithIdempotent())
```

- `DoTx` — auto begin/commit/rollback.
- Non-nil callback error triggers retry with backoff (`retry/` package).
- Always close result sets/streams (`defer result.Close(ctx)`).

### Balancers

Configure via `config.WithBalancer(balancers.PreferNearestDC(balancers.RandomChoice()))`.

Presets in `balancers/`: `RandomChoice`, `SingleConn`, `PreferNearestDC`, location filters.

### Trace codegen

- Callback types in `trace/`; generated wrappers `*_gtrace.go` via `go generate ./trace`.
- CI `check-codegen.yml` fails on dirty generated files.

### Public vs internal

- Edit public interfaces in `table/`, `query/`, etc.
- Implement in matching `internal/<service>/` package.
- Do not expose `internal/` types in public API without stable wrappers.

## Adding a new API

1. Confirm protobuf support in `ydb-go-genproto`.
2. Add methods in `internal/<service>/` + `internal/grpcwrapper/`.
3. Expose through public `*/client.go` with `Do`/`DoTx` or direct methods + retry.
4. Add `trace/` callbacks if needed; run `go generate`.
5. Unit tests co-located; integration test in `tests/integration/` if server needed.

## Anti-patterns

- Bypassing session pool or connection pool for production RPC paths.
- Returning from `Do` callback without closing streams/results.
- Adding dependencies without explicit task requirement.
- Editing `*_gtrace.go` by hand instead of regenerating.
