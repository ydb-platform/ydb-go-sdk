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

// ArrowColumn describes a result column using its YDB type, including optionality.
type ArrowColumn = arrow.Column

// ArrowBatch owns decoded columns until the reader advances to another response
// part or closes the streaming result. Materialized results retain their batches
// until Close.
type ArrowBatch = arrow.Batch

// ArrowDecoder returns retained batches for one response part. Streaming results
// release them before reading the next part, including at EOF, or at Close.
// Rows from that part are valid only until that transition or Close; Scan output
// and Values remain valid independently. Client.Query and Client.QueryResultSet
// retain all batches until their materialized result is closed. QueryRow detaches
// its one row before reading ahead and closing the internal result.
// The decoder must support concurrent calls and preserve all rows and column order.
// Before returning batches, it must validate their YDB column types and optionality.
// The SDK checks batch dimensions; cell type validation belongs to the decoder.
type ArrowDecoder = arrow.Decoder

// WithArrow requests Arrow results for one query while retaining ResultSets,
// Rows, Scan, ScanNamed, ScanStruct and Values. It applies to Query, QueryRow and
// QueryResultSet on Client, Session and TxActor, including queries inside Do and DoTx.
// Exec also requests Arrow, but discards results without invoking the decoder.
// The server must support and enable Arrow results.
//
// The application supplies an ArrowDecoder and chooses its Apache Arrow Go version;
// the SDK module has no Apache Arrow Go dependency. The decoder converts each IPC
// response part into retained column batches. NewArrowDecoder builds a decoder
// from ipc.NewReader without specifying generic type arguments. See ArrowDecoder
// for ownership and concurrency requirements.
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
// Client.Query still materializes the entire result; Session and TxActor queries
// decode one response part at a time. Decoding includes every column in the part,
// even if the application does not scan it. Decode errors are returned through the
// ordinary result error path; no query is re-executed to fall back to another format.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithArrow(decoder ArrowDecoder) ExecuteOption {
	return options.WithArrow(decoder)
}

// ArrowArray is the column interface required by NewArrowDecoder.
type ArrowArray = arrow.Array

// ArrowRecord is the record interface required by NewArrowDecoder.
type ArrowRecord[A ArrowArray] = arrow.Record[A]

// ArrowIPCReader reads borrowed records until io.EOF and releases the IPC reader.
// A record is valid until the next Read or Release. NewArrowDecoder retains each
// record before reading ahead and transfers its ownership to the SDK result.
type ArrowIPCReader[B any] = arrow.IPCReader[B]

// NewArrowDecoder builds an ArrowDecoder from the application's ipc.NewReader.
// Reader, record, array and option types are inferred from the factory; the SDK
// has no Apache Arrow Go dependency. It supports Bool, signed/unsigned integers,
// Float, Double, String and Utf8, with one Optional wrapper. Unsupported types
// and mismatches with YDB column metadata return decode errors before scanning.
// YDB temporal types, including Date, Datetime, Timestamp and Interval, require
// a custom ArrowDecoder even when represented by integer arrays in Arrow.
//
// For example:
//
//	decoder := query.NewArrowDecoder(ipc.NewReader)
//	row, err := db.Query().QueryRow(ctx, sql, query.WithArrow(decoder))
//
// Optional arguments are IPC reader options from the same Arrow Go version.
// The factory and its options must support concurrent calls. See ArrowDecoder
// for result ownership and lifetime requirements.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func NewArrowDecoder[A ArrowArray, B ArrowRecord[A], R ArrowIPCReader[B], O any](
	newReader func(io.Reader, ...O) (R, error), opts ...O,
) ArrowDecoder {
	return arrow.NewDecoder(newReader, opts...)
}
