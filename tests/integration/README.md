# Package `integration`

Package `integration` contains only integration tests for `ydb-go-sdk`. All test files must have build tag
```go
//go:build integration
// +build integration
```
for run this test files as integration tests int github action `integration`.

Arrow tests and benchmarks live in a [separate Go module](arrow/README.md) so
Apache Arrow Go is not a dependency of the SDK module. Run them from
`tests/integration/arrow`; `go test -tags integration ./tests/integration`
does not include nested modules.

## Query result benchmark

Run the ordinary `Session.Query` + `Scan` benchmark from the repository root:

```sh
go test -tags integration -run '^$' -bench '^BenchmarkQueryRows$' \
  -benchtime=100x -count=5 -cpu=4 -benchmem -v ./tests/integration
```

For a quick fixture and checksum check, use `-benchtime=1x -count=1` first.
Set `YDB_CONNECTION_STRING` if it differs from `grpc://localhost:2136/local`.
The benchmark uses anonymous credentials and creates, loads and drops
`/local/query_arrow_benchmark`; use a disposable local database. It runs on Linux
and macOS.

`BenchmarkQueryRows` consumes 1, 10, 100, 1,000 and 10,000 ordered rows with six
columns, nullable values and 64-byte payloads. The fixture, checksum, scan
destinations, ten warmup queries and 32 KiB response-part limit are shared with
the [Arrow benchmarks](arrow/README.md). Every RPC must return the expected row
count and checksum.

The ordinary Query benchmark is included in the main module's integration
benchmark suite. The Arrow variants still require a separate run from their
module. Compare matching row counts across `BenchmarkQueryRows/<rows>` and
`BenchmarkFormats/<rows>/QueryArrow` or `WithResultFormatArrow`. They run in
separate processes and sessions; use the same Go version, `-cpu`, benchmark
flags and database conditions, and run the commands sequentially. The benchmark
names and package names remain distinct in benchstat output.

`ns/op` includes server work and transport. `cpu-ns/op` reports client process
user + system CPU, including decoding, scans and GC. `B/op` and `allocs/op`
report Go allocations. `-cpu` controls `GOMAXPROCS`, not CPU measurement. Measure
without `-race` and other concurrent workloads; a one-iteration smoke run checks
correctness and does not establish a performance improvement.
