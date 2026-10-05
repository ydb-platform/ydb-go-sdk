package query

import (
	"context"

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

// ArrowDecoder decodes one self-contained Arrow IPC part into SDK values.
// Each returned row must contain one non-nil value per column, in column order,
// with the corresponding YDB type. Returned slices and value contents must remain
// valid after the decoder returns and after subsequent calls. The decoder owns
// and releases any Arrow resources; it must support concurrent calls.
// The SDK has no dependency on Apache Arrow Go, so applications can choose its version.
// See examples/apache_arrow/with_arrow for an Apache Arrow Go v18 decoder.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type ArrowDecoder = arrow.Decoder

// WithArrow requests Arrow results for one query while retaining ResultSets,
// Rows, Scan, ScanNamed, ScanStruct and Values. It applies to Query, QueryRow and
// QueryResultSet on Client, Session and TxActor, including queries inside Do and DoTx.
// The server must support and enable Arrow results.
//
// The application supplies an ArrowDecoder and chooses its Apache Arrow Go version;
// the SDK module has no Apache Arrow Go dependency. The decoder converts each IPC
// response part into owned SDK values. See ArrowDecoder for ownership and concurrency
// requirements, and examples/apache_arrow/with_arrow for a working decoder.
//
// For example (error handling omitted):
//
//	row, err := db.Query().QueryRow(ctx, sql, query.WithArrow(myArrowDecoder))
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
