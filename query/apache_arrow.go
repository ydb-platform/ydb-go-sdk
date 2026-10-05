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
// Exec also requests Arrow, but discards results without invoking the decoder.
// The server must support and enable Arrow results.
//
// Create the decoder with NewArrowDecoder using the application's Apache Arrow Go
// version; the SDK module has no Apache Arrow Go dependency. The decoder converts
// each IPC response part into retained column batches.
//
// For example (error handling omitted):
//
//	decoder := query.NewArrowDecoder(ipc.NewReader)
//	row, err := db.Query().QueryRow(ctx, sql, query.WithArrow(decoder))
//	var id int32
//	err = row.Scan(&id)
//
// To enable Arrow by default, use ydb.WithQueryDefaultResultFormatArrow(decoder).
// A nil decoder overrides that driver default for one query:
//
//	row, err := db.Query().QueryRow(ctx, sql, query.WithArrow(nil))
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
func WithArrow(decoder arrow.Decoder) ExecuteOption {
	return options.WithArrow(decoder)
}

// NewArrowDecoder builds a decoder from the application's ipc.NewReader.
// Reader, record, array and option types are inferred from the factory; the SDK
// has no Apache Arrow Go dependency. It supports Bool, signed/unsigned integers,
// Float, Double, String and Utf8, with one Optional wrapper. Unsupported types
// and mismatches with YDB column metadata return decode errors before scanning.
// YDB temporal types, including Date, Datetime, Timestamp and Interval, are
// unsupported even when represented by integer arrays in Arrow. To add support
// for a missing YDB type, open an issue or submit a pull request to
// https://github.com/ydb-platform/ydb-go-sdk.
//
// For example:
//
//	decoder := query.NewArrowDecoder(ipc.NewReader)
//	row, err := db.Query().QueryRow(ctx, sql, query.WithArrow(decoder))
//
// Optional arguments are IPC reader options from the same Arrow Go version.
// The reader returns borrowed records valid until its next Read or Release;
// the decoder retains each record before reading ahead.
// The factory and its options must support concurrent calls. See WithArrow for
// result ownership and lifetime requirements.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func NewArrowDecoder[A arrow.Array, B arrow.Record[A], R arrow.IPCReader[B], O any](
	newReader func(io.Reader, ...O) (R, error), opts ...O,
) arrow.Decoder {
	return arrow.NewDecoder(newReader, opts...)
}
