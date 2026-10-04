# Query results with an application-provided Arrow decoder

`query.WithArrow` is experimental. It works with `Query`, `QueryRow` and
`QueryResultSet` on `query.Client`, `query.Session` and `query.TxActor`.
The server must support Arrow results and enable `EnableArrowResultSetFormat`.

The SDK module does not import Apache Arrow Go. Copy [decoder.go](decoder.go)
into your application and use your own Arrow dependency. This independent example
module uses `github.com/apache/arrow-go/v18` v18.8.0 (Go 1.25 or newer); it does
not change the SDK's `go.mod` or `go.sum`. `ipc.NewReader` receives an `io.Reader`.
The example uses `RecordBatch()` and releases the reader, cloning strings and
bytes so rows remain valid after the next batch and after result closure.

The example supports Bool, signed/unsigned integers, Float, Double, String,
Utf8 and a single Optional wrapper. YDB Bool may arrive as Arrow Uint8.
Unsupported types, including nested Optional and complex types, return errors;
extend the helper for the types used by your application.

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
row, err = db.Query().QueryRow(ctx, `SELECT 42 AS id, "hello"u AS name;`,
    query.WithArrow(witharrow.Decode),
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
    ydb.WithQueryExecuteOptions(query.WithArrow(witharrow.Decode)),
)
// Per-call options override driver defaults:
row, err := db.Query().QueryRow(ctx, sql, query.WithArrow(nil))
```

`ArrowDecoder` receives the YDB column names/types and a self-contained IPC
reader for one response part, including schema. It returns rows of owned
`types.Value` objects in column order. It must return non-nil values of the
corresponding YDB types, preserve optionality, and support concurrent calls.
It owns and releases Arrow buffers. The SDK validates row width and nil values,
uses the existing scanners, and does not reconstruct `Ydb.Value` objects.
Decoder errors propagate through the ordinary result error path. The SDK does
not re-execute a query to fall back to another format.

`Client.Query` still materializes the full result. Session and transaction
queries decode one response part at a time. Conversion eagerly processes all
columns in that part, including columns not subsequently scanned. Keeping the
row API requires owned values and allocations; direct `Session.QueryArrow`
can process Arrow columns without that conversion:

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
