# Query methods and result formats

## Choosing a query method

Choose the method by the expected result shape and how much data the application
can keep in memory. The result format is independent of this choice.

| Method | When to use it | Result handling |
| --- | --- | --- |
| `Exec` | No rows are needed | Executes the query and consumes its results. |
| `QueryRow` | Exactly one row from exactly one result set is expected | Returns an owned row; reports an error for zero rows, additional rows or additional result sets. |
| `Client.QueryResultSet` | Exactly one result set fits in memory | Materializes the result set. Close the returned result set after use. |
| `Session.QueryResultSet` / `TxActor.QueryResultSet` | Exactly one result set is expected, including large results | Reads rows incrementally and checks for additional result sets as the stream is consumed. Close the result set after use. |
| `Client.Query` | All result sets fit in memory | Materializes the entire result. Close the returned result after use. |
| `Session.Query` / `TxActor.Query` | Large results or multiple result sets should be consumed incrementally | Iterate result sets and rows; close the result after use. |

Use `db.Query().Do` to obtain a session with automatic retries, or `DoTx` when
several queries must share a transaction. Consume streaming results inside the
callback and close them before returning. A retry can run the callback again;
account for this when producing application side effects. See the
[streaming query example](README.md#example) and
[transaction example](examples/transaction/query).

## Choosing a result format

| Format and API | Recommended use | Costs and ownership |
| --- | --- | --- |
| `Ydb.Value` through the ordinary query methods | Start here for small results, general type support or applications without an Arrow decoder | Default format; the SDK provides rows and scanners. |
| Arrow through `query.WithArrow(decoder)` | Evaluate for larger results when retaining `Scan`, `ScanNamed`, `ScanStruct` and `Values` is useful | Rows refer to retained column batches. The decoder scans requested cells directly; `Values` and fallback conversions create owned `types.Value` objects on demand. |
| Raw Arrow through `Session.QueryArrow` | Column processing that can consume Arrow batches directly | Avoids conversion to SDK rows. The application reads IPC, manages Arrow resources, and keeps borrowed column data within the batch lifetime. |

Arrow requires server support and the `EnableArrowResultSetFormat` feature.
`query.WithArrow` and the driver default option are experimental. Format selection
does not change materialization: `Client.Query` still keeps the entire result in
memory with Arrow enabled. Session and transaction queries decode one response
part at a time.

The SDK module does not depend on Apache Arrow Go. Supply a `query.ArrowDecoder`
using the Arrow major version already selected by your application. The
[v18 decoder example](examples/apache_arrow/with_arrow) demonstrates decoding,
resource release, scans and tests. Copy and adapt its
[decoder.go](examples/apache_arrow/with_arrow/decoder.go); the example is a
separate module, not an Arrow dependency of the SDK.

The example supports Bool, signed/unsigned integers, Float, Double, String, Utf8
and one level of Optional. Unsupported types return errors; extend your decoder
for the YDB types used by your queries. A decoder receives column names and YDB
types plus a self-contained IPC part. It returns retained `query.ArrowBatch`
objects and must preserve column order and types, validate optionality, and
support concurrent calls. Each batch provides `NumRows`, `NumCols`, direct
`Scan(row, column, dst)`, owned `Value(row, column)` and `Release` methods.
`Scan` destinations and `Value` results must remain valid after batch release.
The example retains each Arrow record once before releasing its IPC reader;
the SDK calls `Release` when the result no longer needs the batch. Decoder
errors follow the ordinary result error path; the SDK does not re-execute SQL
to fall back to another format.

For streaming `Session.Query` / `TxActor.Query` and their `QueryResultSet`
methods, rows are views of the current response part. Consume a row before
advancing to another part, including the read that reaches EOF, skipping a
result set, or closing the result. Moving between rows or batches within the
same part keeps its batches alive. `Scan` output and `Values()` are owned and
remain valid independently; save those when data must outlive the part.
`Client.Query` and `Client.QueryResultSet` retain all batches until the
returned result is closed. `QueryRow` detaches its one row before reading
ahead to validate the row and result-set counts.

### Selecting the format per query

The examples below assume `db` is an open driver, `ctx` is a context and
`myArrowDecoder` implements `query.ArrowDecoder`.

```go
row, err := db.Query().QueryRow(ctx, `SELECT 42 AS id;`,
    query.WithArrow(myArrowDecoder),
)
if err != nil {
    return err
}
var id int32
if err := row.Scan(&id); err != nil {
    return err
}
```

The same option applies to `Query`, `QueryRow` and `QueryResultSet` on Client,
Session and TxActor, including calls inside `Do` and `DoTx`.

### Selecting the driver default

```go
db, err := ydb.Open(ctx, dsn,
    ydb.WithQueryDefaultResultFormatArrow(myArrowDecoder),
)
if err != nil {
    return err
}
defer db.Close(ctx)

// This query uses myArrowDecoder and the ordinary row API.
row, err := db.Query().QueryRow(ctx, `SELECT 42 AS id;`)
if err != nil {
    return err
}
var id int32
if err := row.Scan(&id); err != nil {
    return err
}

// Override the driver default for this query only.
row, err = db.Query().QueryRow(ctx, `SELECT 42 AS id;`, query.WithArrow(nil))
if err != nil {
    return err
}
if err := row.Scan(&id); err != nil {
    return err
}
```

`query.WithArrow(otherDecoder)` selects another decoder for one query.
Subsequent queries without an override use the driver default again. Passing
`nil` to `WithQueryDefaultResultFormatArrow` selects the ordinary YDB value
format as the driver default. These defaults do not apply to `ExecuteScript` or
`FetchScriptResults`. Raw `Session.QueryArrow` always requests Arrow IPC.

For raw IPC consumption with `ipc.NewReader(part)`, see the
[example](examples/apache_arrow/with_arrow/README.md#the-same-row-api-with-either-format).

## Local benchmark

The benchmark compares full SELECT execution and consumption of all six columns:

- `Value`: `Session.Query` + `Scan`.
- `QueryArrow`: `Session.QueryArrow` + direct column access.
- `WithArrow`: `Session.Query` + `query.WithArrow` with the example v18 decoder + `Scan`.

All variants use the same session, SQL, row checksum, disabled response prefetch
and a 32 KiB response-part limit. Value and WithArrow reuse Scan destinations and
arguments between rows. Nullable scans still allocate each non-null destination.
The example decoder validates types and selects scan functions once per batch
column. It scans scalar destinations directly, copying strings and bytes
when assigning them. SDK values are created only for `Values` and fallback
conversions.
The table has 10,000 rows: Uint64 id, Optional Int32/Bool/Double/Utf8/String,
10% null in score/name, and a 64-byte payload. Queries return 1, 10, 100, 1,000
or 10,000 ordered rows; the one-row query selects a row with null score/name.
Every RPC checks both row count and checksum.

Environment: Apple M3 Pro, native darwin/arm64 Go 1.26.0, Arrow Go v18.8.0,
GOMAXPROCS=4. The server is `ydbplatform/local-ydb:26.3.1.17`, image digest
`sha256:dec57994cbc96d706aa320443c7d97c8dc6dd580320370d5482f7d61bb22c785`,
linux/amd64 under emulation in an aarch64 Colima VM with 2 vCPU and 15.6 GiB RAM,
in-memory PDisks and anonymous authentication.

Each size was measured in five separate processes, with ten warmup RPCs per
variant. Each process ran 1,000 RPCs per variant for 1/10/100 rows, or 100 RPCs
for 1,000/10,000 rows. Measurements ran without race instrumentation or concurrent
tests/builds. The table and charts contain medians; elapsed ranges in the table
are the observed min–max across the five runs, not confidence intervals.

| Rows | API | Elapsed ms/RPC (range) | Client CPU ms/RPC | Allocated MiB/RPC | Allocations/RPC |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | `Value` | 1.468 (1.391–1.617) | 0.395 | 0.021 | 378 |
| 1 | `QueryArrow` | 1.511 (1.476–1.539) | 0.425 | 0.026 | 372 |
| 1 | `WithArrow` | 1.513 (1.459–1.650) | 0.464 | 0.030 | 486 |
| 10 | `Value` | 1.525 (1.455–1.835) | 0.502 | 0.035 | 722 |
| 10 | `QueryArrow` | 1.563 (1.489–1.595) | 0.447 | 0.029 | 373 |
| 10 | `WithArrow` | 1.597 (1.512–1.622) | 0.490 | 0.033 | 559 |
| 100 | `Value` | 2.458 (2.342–2.729) | 1.011 | 0.179 | 4,112 |
| 100 | `QueryArrow` | 1.895 (1.806–2.024) | 0.557 | 0.059 | 374 |
| 100 | `WithArrow` | 1.947 (1.789–1.992) | 0.643 | 0.067 | 1,253 |
| 1,000 | `Value` | 6.530 (6.163–6.699) | 3.187 | 2.198 | 39,525 |
| 1,000 | `QueryArrow` | 4.389 (3.932–5.442) | 0.993 | 0.373 | 680 |
| 1,000 | `WithArrow` | 4.980 (4.536–5.238) | 1.595 | 0.416 | 8,582 |
| 10,000 | `Value` | 30.454 (28.878–33.341) | 25.619 | 19.211 | 394,874 |
| 10,000 | `QueryArrow` | 20.553 (19.739–23.566) | 7.677 | 4.774 | 4,721 |
| 10,000 | `WithArrow` | 21.019 (20.890–21.654) | 9.527 | 5.501 | 83,142 |

Client CPU is user + system CPU of the client process from `getrusage`, including
decoding, scanning and GC. Elapsed time includes server execution and transport.
Allocated MiB are cumulative Go allocations per RPC, not RSS or peak live memory.
Server CPU and wire payload size were not measured.

### Interpreting the results

For 1/10 rows, WithArrow does not show an elapsed-time benefit: its median
increases by 3.1% / 4.7% relative to Value, and the observed ranges overlap.
Client CPU changes by +17.5% / −2.3%. For one row, allocated bytes increase
by 39.2% and allocation count by 28.6%. At 100 rows, WithArrow reduces elapsed
time by 20.8% and client CPU by 36.4% in this workload.

For 1,000/10,000 rows, WithArrow reduces client CPU by 50.0% / 62.8%, elapsed
time by 23.7% / 31.0%, allocated bytes by 81.1% / 71.4% and allocation count
by 78.3% / 78.9% relative to Value. Direct QueryArrow reduces client CPU by
68.8% / 70.0%, but requires a different consumption API and resource ownership.

Use these measurements to select candidates for your own benchmark. They do not
establish universal row-count thresholds: types, row width, nulls, server work,
network and application processing affect the result. Functional Client/TxActor
coverage is separate; the performance measurements above use Session.

### Client CPU

```mermaid
---
config:
  themeVariables:
    xyChart:
      plotColorPalette: "#596579, #158073, #dc7127"
---
xychart-beta
    title "Client CPU"
    x-axis "Rows per response" ["1", "10", "100", "1,000", "10,000"]
    y-axis "ms/RPC" 0 --> 28
    line "Value" [0.395, 0.502, 1.011, 3.187, 25.619]
    line "QueryArrow" [0.425, 0.447, 0.557, 0.993, 7.677]
    line "WithArrow" [0.464, 0.490, 0.643, 1.595, 9.527]
```

### Allocated memory

```mermaid
---
config:
  themeVariables:
    xyChart:
      plotColorPalette: "#596579, #158073, #dc7127"
---
xychart-beta
    title "Allocated memory"
    x-axis "Rows per response" ["1", "10", "100", "1,000", "10,000"]
    y-axis "MiB/RPC" 0 --> 20
    line "Value" [0.021, 0.035, 0.179, 2.198, 19.211]
    line "QueryArrow" [0.026, 0.029, 0.059, 0.373, 4.774]
    line "WithArrow" [0.030, 0.033, 0.067, 0.416, 5.501]
```

### Allocation count

```mermaid
---
config:
  themeVariables:
    xyChart:
      plotColorPalette: "#596579, #158073, #dc7127"
---
xychart-beta
    title "Allocation count"
    x-axis "Rows per response" ["1", "10", "100", "1,000", "10,000"]
    y-axis "Allocations/RPC" 0 --> 400000
    line "Value" [378, 722, 4112, 39525, 394874]
    line "QueryArrow" [372, 373, 374, 680, 4721]
    line "WithArrow" [486, 559, 1253, 8582, 83142]
```

The x-axis lists the measured row counts at equal intervals; the y-axis is
linear. The charts show medians without error bars.

### Reproducing the benchmark

Start a disposable local YDB using the Docker command in the
[example README](examples/apache_arrow/with_arrow/README.md#checks-and-local-benchmark).
The benchmark creates, loads and drops `/local/query_arrow_benchmark`; it requires
that database and table permissions. Run from the SDK checkout:

```sh
cd examples/apache_arrow/with_arrow
for sample in 1 2 3 4 5; do
  go test -tags integration -run '^$' -bench '^BenchmarkFormats$/(1|10|100)$' -benchtime=1000x -count=1 -cpu=4 -v
  go test -tags integration -run '^$' -bench '^BenchmarkFormats$/(1000|10000)$' -benchtime=100x -count=1 -cpu=4 -v
done
```

Set `YDB_CONNECTION_STRING` if it differs from `grpc://localhost:2136/local`.
The example uses Arrow Go v18.8.0 and requires Go 1.25 or newer.
