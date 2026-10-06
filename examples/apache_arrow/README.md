# Writing and reading rows with Apache Arrow

`query.WithArrow` is experimental. It works with `Query`, `QueryRow` and
`QueryResultSet` on `query.Client`, `query.Session` and `query.TxActor`.
The server must support Arrow results and enable `EnableArrowResultSetFormat`.

The SDK module does not import Apache Arrow Go. Pass
`ipc.NewReader` and optional IPC reader options from your own Arrow dependency
to `query.WithArrow` or `ydb.WithQueryDefaultResultFormatArrow`. Reader,
record, array and option types are inferred automatically; both options
work with v17 and v18. The examples module uses
`github.com/apache/arrow-go/v18` v18.8.0 (Go 1.25 or newer); it does not change
the SDK's `go.mod` or `go.sum`. The decoder retains each record once and releases
the IPC reader. It clones strings and bytes when scanning or creating SDK values
so the returned data remains valid after batch release.

The decoder supports YDB scalars, temporal types, Decimal, Pg, Tagged, Null,
Void, lists, tuples, structs, dictionaries, sets, variants and nested Optional
values. YDB Bool may arrive as Arrow Uint8. To add support for a missing YDB type,
open an
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
row, err = db.Query().QueryRow(ctx, `SELECT 42 AS id, "hello"u AS name;`,
    query.WithArrow(ipc.NewReader),
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
    ydb.WithQueryDefaultResultFormatArrow(ipc.NewReader),
)
// Per-call options override driver defaults:
row, err := db.Query().QueryRow(ctx, sql, query.WithYdbValue())
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

The [command](main.go) creates a table, inserts two rows with Arrow
`BulkUpsert`, reads them with `Session.QueryArrow` and with
`Session.Query`, `query.WithArrow(ipc.NewReader)` and `ScanNamed`, then drops
the table. With a local YDB running, execute from the `examples` directory:

```sh
go run ./apache_arrow
```

Set `YDB_CONNECTION_STRING` if it differs from `grpc://localhost:2136/local`.
The command uses anonymous authentication and prints:

```text
QueryArrow: id=24 name="WOW"
QueryArrow: id=42 name="my string"
WithArrow: id=24 name="WOW"
WithArrow: id=42 name="my string"
```

## Tests and benchmarks

The [Arrow test module](../../tests/integration/arrow) contains unit and
integration tests, SDK coverage collection and local benchmarks.
