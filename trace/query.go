package trace

import (
	"context"
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
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	Query struct {
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnNew func(QueryNewStartInfo) func(info QueryNewDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnClose func(QueryCloseStartInfo) func(info QueryCloseDoneInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnPoolNew func(QueryPoolNewStartInfo) func(QueryPoolNewDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnPoolClose func(QueryPoolCloseStartInfo) func(QueryPoolCloseDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnPoolTry func(QueryPoolTryStartInfo) func(QueryPoolTryDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnPoolWith func(QueryPoolWithStartInfo) func(QueryPoolWithDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnPoolPut func(QueryPoolPutStartInfo) func(QueryPoolPutDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnPoolGet func(QueryPoolGetStartInfo) func(QueryPoolGetDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnPoolChange func(QueryPoolChange)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnDo func(QueryDoStartInfo) func(QueryDoDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnDoTx func(QueryDoTxStartInfo) func(QueryDoTxDoneInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnSessionCreate func(QuerySessionCreateStartInfo) func(info QuerySessionCreateDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnSessionAttach func(QuerySessionAttachStartInfo) func(info QuerySessionAttachDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnSessionDelete func(QuerySessionDeleteStartInfo) func(info QuerySessionDeleteDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnSessionExecute func(QuerySessionExecuteStartInfo) func(info QuerySessionExecuteDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
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

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	QueryDoStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	QueryDoDoneInfo struct {
		Attempts int
		Error    error
	}
	QueryDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
	QuerySessionCreateStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QuerySessionCreateDoneInfo struct {
		Session querySessionInfo
		Error   error
	}
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
	QuerySessionExecuteDoneInfo struct {
		Error error
	}
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
	QueryTxExecuteDoneInfo struct {
		Error error
	}
	QuerySessionAttachStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	QuerySessionAttachDoneInfo struct {
		Error error
	}
	QuerySessionBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	QuerySessionBeginDoneInfo struct {
		Error error
		Tx    queryTransactionInfo
	}
	QueryResultNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryResultNewDoneInfo struct {
		Error error
	}
	QueryResultCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryResultCloseDoneInfo struct {
		Error error
	}
	QueryResultNextPartStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryResultNextPartDoneInfo struct {
		Error error
	}
	QueryResultNextResultSetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryResultNextResultSetDoneInfo struct {
		Error error
	}
	QueryResultSetNextRowStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryResultSetNextRowDoneInfo struct {
		Error error
	}
	QueryRowScanStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryRowScanDoneInfo struct {
		Error error
	}
	QueryRowScanNamedStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryRowScanNamedDoneInfo struct {
		Error error
	}
	QueryRowScanStructStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryRowScanStructDoneInfo struct {
		Error error
	}
	QuerySessionDeleteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session querySessionInfo
	}
	QuerySessionDeleteDoneInfo struct {
		Error error
	}
	QueryNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryNewDoneInfo    struct{}
	QueryCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryCloseDoneInfo struct {
		Error error
	}
	QueryPoolNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolNewDoneInfo struct {
		Limit int
	}
	QueryPoolCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolCloseDoneInfo struct {
		Error error
	}
	QueryPoolTryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolTryDoneInfo struct {
		Error error
	}
	QueryPoolWithStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolWithDoneInfo struct {
		Error error

		Attempts int
	}
	QueryPoolPutStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolPutDoneInfo struct {
		Error error
	}
	QueryPoolGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	QueryPoolGetDoneInfo struct {
		Error error
	}
	QueryPoolChange struct {
		Limit int
		Index int
		Idle  int
		InUse int
	}
)
