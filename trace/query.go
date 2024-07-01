package trace

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	querySessionInfo interface {
		ID() string
		NodeID() int64
		Status() string
	}
	queryTransactionInfo interface {
		ID() string
	}

	// Query specified trace of retry call activity.
	// gtrace:gen
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	Query struct {
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnNew func(QueryNewStartInfo) func(info QueryNewDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnClose func(QueryCloseStartInfo) func(info QueryCloseDoneInfo)

		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolNew func(QueryPoolNewStartInfo) func(QueryPoolNewDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolClose func(QueryPoolCloseStartInfo) func(QueryPoolCloseDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolTry func(QueryPoolTryStartInfo) func(QueryPoolTryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolWith func(QueryPoolWithStartInfo) func(QueryPoolWithDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolPut func(QueryPoolPutStartInfo) func(QueryPoolPutDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolGet func(QueryPoolGetStartInfo) func(QueryPoolGetDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolChange func(QueryPoolChange)

		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnDo func(QueryDoStartInfo) func(QueryDoDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnDoTx func(QueryDoTxStartInfo) func(QueryDoTxDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnExecute func(QueryExecuteStartInfo) func(QueryExecuteDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnReadRow func(QueryReadRowStartInfo) func(QueryReadRowDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnReadResultSet func(QueryReadResultSetStartInfo) func(QueryReadResultSetDoneInfo)

		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionCreate func(QuerySessionCreateStartInfo) func(info QuerySessionCreateDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionAttach func(QuerySessionAttachStartInfo) func(info QuerySessionAttachDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionDelete func(QuerySessionDeleteStartInfo) func(info QuerySessionDeleteDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionExecute func(QuerySessionExecuteStartInfo) func(info QuerySessionExecuteDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionBegin        func(QuerySessionBeginStartInfo) func(info QuerySessionBeginDoneInfo)
		OnTxExecute           func(QueryTxExecuteStartInfo) func(info QueryTxExecuteDoneInfo)
		OnResultNew           func(QueryResultNewStartInfo) func(info QueryResultNewDoneInfo)
		OnResultNextPart      func(QueryResultNextPartStartInfo) func(info QueryResultNextPartDoneInfo)
		OnResultNextResultSet func(QueryResultNextResultSetStartInfo) func(info QueryResultNextResultSetDoneInfo)
		OnResultClose         func(QueryResultCloseStartInfo) func(info QueryResultCloseDoneInfo)
		OnResultSetNextRow    func(QueryResultSetNextRowStartInfo) func(info QueryResultSetNextRowDoneInfo)
		OnRowScan             func(QueryRowScanStartInfo) func(info QueryRowScanDoneInfo)
		OnRowScanNamed        func(QueryRowScanNamedStartInfo) func(info QueryRowScanNamedDoneInfo)
		OnRowScanStruct       func(QueryRowScanStructStartInfo) func(info QueryRowScanStructDoneInfo)
	}

	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryDoStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryDoDoneInfo struct {
		Attempts int
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Query string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryExecuteDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryReadRowStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Query string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryReadRowDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryReadResultSetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Query string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryReadResultSetDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionCreateStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionCreateDoneInfo struct {
		Session querySessionInfo
		Error   error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Session querySessionInfo
		Query   string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionExecuteDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryTxExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Session querySessionInfo
		Tx      queryTransactionInfo
		Query   string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryTxExecuteDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionAttachStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionAttachDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionBeginDoneInfo struct {
		Error error
		Tx    queryTransactionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultNewDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultCloseDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultNextPartStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultNextPartDoneInfo struct {
		Stats *Ydb_TableStats.QueryStats
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultNextResultSetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultNextResultSetDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultSetNextRowStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryResultSetNextRowDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryRowScanStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryRowScanDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryRowScanNamedStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryRowScanNamedDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryRowScanStructStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryRowScanStructDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionDeleteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QuerySessionDeleteDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryNewDoneInfo struct{}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryCloseDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolNewDoneInfo struct {
		Limit int
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolCloseDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolTryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolTryDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolWithStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolWithDoneInfo struct {
		Error error

		Attempts int
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolPutStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolPutDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolGetDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	QueryPoolChange struct {
		Limit int
		Index int
		Idle  int
		InUse int
	}
)
