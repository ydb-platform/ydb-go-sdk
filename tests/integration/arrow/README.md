# Apache Arrow tests and benchmarks

Run from this directory. The module's local `replace` builds the SDK checkout.
Arrow Go v18.8.0 is a dependency of this test module; the SDK's `go.mod` and
`go.sum` remain independent of Arrow. Go 1.25 or newer is required.

```sh
go test -race ./...
go test -race -tags integration ./...
go test -mod=readonly -race -tags integration \
  -coverpkg=github.com/ydb-platform/ydb-go-sdk/v3/... \
  -coverprofile arrow.txt -covermode atomic ./...
go test -tags integration -run '^$' -bench BenchmarkFormats -benchtime=100x -count=5 -cpu=4 -v
```

Set `YDB_CONNECTION_STRING` if it differs from `grpc://localhost:2136/local`.
Tests use anonymous credentials. The [YQL type tests](types_integration_test.go)
use self-contained `SELECT` expressions without creating tables. They compare
scalar and complex types, nested Optional values and time-zone boundaries
with protobuf and raw Arrow; see
[the compatibility notes](../../../QUERY.md#type-compatibility-tests).
The benchmark database must be `/local`.
The benchmark requires permission to create,
load and drop `/local/query_arrow_benchmark`; use a disposable local database.
For example:

```sh
docker run -d --name ydb-query-with-arrow --hostname localhost \
  --platform linux/amd64 -p 2136:2136 -p 8765:8765 \
  -e YDB_USE_IN_MEMORY_PDISKS=true ydbplatform/local-ydb:26.3.1.17
```

Wait until `ydb -e grpc://localhost:2136 -d /local sql -s 'SELECT 1;'` succeeds.
The benchmark consumes 1, 10, 100, 1,000 and 10,000 ordered table rows with six columns,
nullable values and 64-byte payloads. It compares `Session.Query` + `Scan` using the
wire decoder, direct `Session.QueryArrow` column access, and `Session.Query` +
`WithResultFormatArrow` + `Scan`. The direct gRPC `WireValue` variant
measures decoder overhead without the SDK query/session path. All variants use the same
session, SQL, checksum and 32 KiB response-part limit.
Each variant has ten warmup queries. All returned columns participate in the
checksum; the row count and checksum must match on every RPC.
The Query and WithResultFormatArrow consumers reuse Scan destinations and arguments across
rows; nullable scans still allocate each non-null destination value.

Reported `ns/op` includes server work and transport; `cpu-ns/op` is client process
user + system CPU from `getrusage`, including decoding, scans and GC. `B/op` and
`allocs/op` are Go allocations. Run benchmarks without `-race` and without other
concurrent workloads. Docker CPU architecture/emulation and resource limits
matter for elapsed time. These measurements are not serialization-only numbers
and do not establish a universal end-to-end speedup.
