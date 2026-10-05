# Query results with Apache Arrow

`query.WithArrow` is experimental. It works with `Query`, `QueryRow` and
`QueryResultSet` on `query.Client`, `query.Session` and `query.TxActor`.
The server must support Arrow results and enable `EnableArrowResultSetFormat`.

The SDK module does not import Apache Arrow Go. Construct the decoder using
`query.NewArrowDecoder(ipc.NewReader)` and your own Arrow dependency. Reader,
record, array and option types are inferred automatically; the same constructor
works with v17 and v18. This independent example module uses
`github.com/apache/arrow-go/v18` v18.8.0 (Go 1.25 or newer); it does not change
the SDK's `go.mod` or `go.sum`. The decoder retains each record once and releases
the IPC reader. It clones strings and bytes when scanning or creating SDK values
so the returned data remains valid after batch release.

The example supports Bool, signed/unsigned integers, Float, Double, String,
Utf8 and a single Optional wrapper. YDB Bool may arrive as Arrow Uint8.
Unsupported types, including nested Optional and complex types, return errors;
to add support for a missing YDB type, open an
[issue](https://github.com/ydb-platform/ydb-go-sdk/issues) or submit a
[pull request](https://github.com/ydb-platform/ydb-go-sdk/pulls).

## The same row API with either format

```go
// With the usual Ydb.Value wire format:
row, err := db.Query().QueryRow(ctx, `SELECT 42 AS id, "hello"u AS name;`)
if err != nil {
    return err
}
var id int32
var name string
if err := row.Scan(&id, &name); err != nil {
    return err
}

// With Arrow IPC on the wire and the same SDK scans:
decoder := query.NewArrowDecoder(ipc.NewReader)
row, err = db.Query().QueryRow(ctx, `SELECT 42 AS id, "hello"u AS name;`,
    query.WithArrow(decoder),
)
if err != nil {
    return err
}
if err := row.ScanNamed(query.Named("id", &id), query.Named("name", &name)); err != nil {
    return err
}
```

The same option can be passed to `s.Query(...)` inside `db.Query().Do` or to
`tx.Query(...)` inside `db.Query().DoTx`. For driver-wide defaults:

```go
db, err := ydb.Open(ctx, dsn,
    ydb.WithQueryDefaultResultFormatArrow(decoder),
)
// Per-call options override driver defaults:
row, err := db.Query().QueryRow(ctx, sql, query.WithArrow(nil))
```

The decoder receives the YDB column names/types and a self-contained IPC
reader for one response part, including schema. It returns retained
column batches. Each batch preserves column order, reports its
dimensions, scans cells directly and returns owned `types.Value` objects on
demand. The
decoder validates YDB types and optionality before returning batches.
The SDK validates batch dimensions, uses the existing column mappings for
`Scan`, `ScanNamed` and `ScanStruct`, and calls `Release` when the batch is no
longer needed. Decoder errors propagate through the ordinary result error
path. The SDK does not re-execute a query to fall back to another format.

`Client.Query` and `Client.QueryResultSet` materialize their output and retain
all batches until the returned result is closed. Streaming Session and
transaction results release the current part before receiving the next one,
including at EOF, or at `Close`. Rows are valid until that transition; moving
between rows or batches within one part keeps them alive. Scanned data and
`Values()` remain valid independently. `QueryRow` returns a detached owned row.
The decoder reads every column but scans only requested cells; `Values` and
fallback conversions construct SDK values on demand. Direct `Session.QueryArrow`
can process Arrow columns without SDK rows or scans:

```go
err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
    result, err := s.QueryArrow(ctx, sql)
    if err != nil {
        return err
    }
    defer result.Close(ctx)
    for part, err := range result.Parts(ctx) {
        if err != nil {
            return err
        }
        reader, err := ipc.NewReader(part)
        if err != nil {
            return err
        }
        for reader.Next() {
            batch := reader.RecordBatch()
            // Consume batch columns here, before Next or Release.
            fmt.Println(batch.NumRows())
        }
        err = reader.Err()
        reader.Release()
        if err != nil {
            return err
        }
    }
    return nil
})
```

## Running the example

The [command](cmd/main.go) reads two result sets with `Session.Query`,
`query.NewArrowDecoder(ipc.NewReader)` and `ScanNamed`. With a local YDB running:

```sh
go run ./cmd
```

Set `YDB_CONNECTION_STRING` if it differs from `grpc://localhost:2136/local`.
The command uses anonymous authentication and prints:

```text
id=42 name="my string"
id=24 name="WOW"
```

## Checks and local benchmark

Run from this directory. The module's local `replace` builds the SDK checkout.

```sh
go test -race ./...
go test -race -tags integration ./...
go test -tags integration -run '^$' -bench BenchmarkFormats -benchtime=100x -count=5 -cpu=4 -v
```

Set `YDB_CONNECTION_STRING` if it differs from `grpc://localhost:2136/local`.
Tests use anonymous credentials. The benchmark database must be `/local`.
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
nullable values and 64-byte payloads. It compares `Session.Query` + `Scan`,
direct `Session.QueryArrow` column access, and `Session.Query` + `WithArrow` +
`Scan` using the same session, SQL, checksum and 32 KiB response-part limit.
Each variant has ten warmup queries. All returned columns participate in the
checksum; the row count and checksum must match on every RPC.
The Value and WithArrow consumers reuse Scan destinations and arguments across
rows; nullable scans still allocate each non-null destination value.

Reported `ns/op` includes server work and transport; `cpu-ns/op` is client process
user + system CPU from `getrusage`, including decoding, scans and GC. `B/op` and
`allocs/op` are Go allocations. Run benchmarks without `-race` and without other
concurrent workloads. Docker CPU architecture/emulation and resource limits
matter for elapsed time. These measurements are not serialization-only numbers
and do not establish a universal end-to-end speedup.
