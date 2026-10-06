package query

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
)

// ArrowExecutor is an interface for execute queries with results in `Apache Arrow` format.
type ArrowExecutor interface {
	// QueryArrow like [Executor.Query] but returns results in [Apache Arrow] format.
	// Each part of the result implements io.Reader and contains the data in Arrow IPC format.
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	//
	// [Apache Arrow]: https://arrow.apache.org/
	QueryArrow(ctx context.Context, sql string, opts ...ExecuteOption) (ArrowResult, error)
}

type ArrowResult = arrow.Result

// WithArrow requests Arrow results for one query while retaining ResultSets,
// Rows, Scan, ScanNamed, ScanStruct and Values. It applies to Query, QueryRow and
// QueryResultSet on Client, Session and TxActor, including queries inside Do and DoTx.
// Exec also requests Arrow, but discards results without invoking the reader.
// The server must support and enable Arrow results.
//
// Pass ipc.NewReader from the application's Apache Arrow Go version. Reader,
// record, array and option types are inferred from the factory; the SDK module
// has no Apache Arrow Go dependency. Optional arguments are IPC reader options
// from the same Arrow Go version. The factory and its options must support
// concurrent calls. The resulting ExecuteOption can be reused across queries.
//
// Compatible Arrow Go modules are github.com/apache/arrow/go/v6 through v17
// and github.com/apache/arrow-go/v18, using each module's arrow/ipc package.
// Apache Arrow releases 0.14.0 through 5.0.0 use the legacy IPC package
// github.com/apache/arrow/go/arrow/ipc and are also compile-compatible.
// The legacy module was also tested at v0.0.0-20211112161151-bc219186db40.
// Earlier Apache Arrow releases do not provide ipc.NewReader.
// With v6-v13 or the legacy module, google.golang.org/genproto may need an upgrade
// to avoid ambiguous imports with the SDK's googleapis/rpc dependency.
//
// For example (error handling omitted):
//
//	row, err := db.Query().QueryRow(ctx, sql, query.WithArrow(ipc.NewReader))
//	var id int32
//	err = row.Scan(&id)
//
// Reader options can be passed directly:
//
//	option := query.WithArrow(ipc.NewReader, ipc.WithAllocator(allocator))
//
// To enable Arrow by default, use ydb.WithQueryDefaultResultFormatArrow(ipc.NewReader).
// WithYdbValue overrides that driver default for one query:
//
//	row, err := db.Query().QueryRow(ctx, sql, query.WithYdbValue())
//
// The decoder supports Bool, signed/unsigned integers, Float, Double, String and
// Utf8, with one Optional wrapper. Unsupported types and mismatches with YDB
// column metadata return decode errors before scanning. YDB temporal types,
// including Date, Datetime, Timestamp and Interval, are unsupported even when
// represented by integer arrays in Arrow. To add support for a missing YDB type,
// open an issue or submit a pull request to https://github.com/ydb-platform/ydb-go-sdk.
//
// Client.Query and Client.QueryResultSet materialize the entire result and retain
// batches until Close. Session and TxActor queries decode one response part at a
// time; rows remain valid until reading another part, including at EOF, or Close.
// Scan output and Values remain valid independently. QueryRow detaches its one
// row before reading ahead and closing the internal result. Decoding includes
// every column in the part, even if the application does not scan it. Decode errors
// are returned through the ordinary result error path; no query is re-executed to
// fall back to another format.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithArrow[A arrow.Array, B arrow.Record[A], R arrow.IPCReader[B], O any](
	newReader func(io.Reader, ...O) (R, error), opts ...O,
) ExecuteOption {
	return options.WithArrow(arrow.NewDecoder(newReader, opts...))
}

// WithYdbValue selects the ordinary YDB value result format for one query,
// overriding ydb.WithQueryDefaultResultFormatArrow. It applies to Query,
// QueryRow, QueryResultSet and Exec on Client, Session and TxActor.
// Subsequent queries without an override use the driver default again.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithYdbValue() ExecuteOption {
	return options.WithArrow(nil)
}
