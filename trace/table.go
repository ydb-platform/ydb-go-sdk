package trace

import (
	"context"
	"time"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Table specified trace of table client activity.
	// gtrace:gen
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	Table struct {
		// Client events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnInit func(TableInitStartInfo) func(TableInitDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnClose func(TableCloseStartInfo) func(TableCloseDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnDo func(TableDoStartInfo) func(TableDoDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnDoTx func(TableDoTxStartInfo) func(TableDoTxDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnCreateSession func(TableCreateSessionStartInfo) func(TableCreateSessionDoneInfo)
		// Session events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionNew func(TableSessionNewStartInfo) func(TableSessionNewDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionDelete func(TableSessionDeleteStartInfo) func(TableSessionDeleteDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionKeepAlive func(TableKeepAliveStartInfo) func(TableKeepAliveDoneInfo)
		// Query events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionBulkUpsert func(TableBulkUpsertStartInfo) func(TableBulkUpsertDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryPrepare func(TablePrepareDataQueryStartInfo) func(TablePrepareDataQueryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryExecute func(TableExecuteDataQueryStartInfo) func(TableExecuteDataQueryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryExplain func(TableExplainQueryStartInfo) func(TableExplainQueryDoneInfo)
		// Stream events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryStreamExecute func(TableSessionQueryStreamExecuteStartInfo) func(TableSessionQueryStreamExecuteDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryStreamRead func(TableSessionQueryStreamReadStartInfo) func(TableSessionQueryStreamReadDoneInfo)
		// Transaction events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxBegin func(TableTxBeginStartInfo) func(
			TableTxBeginDoneInfo,
		)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxExecute func(TableTransactionExecuteStartInfo) func(
			TableTransactionExecuteDoneInfo,
		)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxExecuteStatement func(TableTransactionExecuteStatementStartInfo) func(
			TableTransactionExecuteStatementDoneInfo,
		)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxCommit func(TableTxCommitStartInfo) func(
			TableTxCommitDoneInfo,
		)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxRollback func(TableTxRollbackStartInfo) func(TableTxRollbackDoneInfo)
		// Pool state event
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolStateChange func(TablePoolStateChangeInfo)

		// Pool session lifecycle events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolSessionAdd func(info TablePoolSessionAddInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolSessionRemove func(info TablePoolSessionRemoveInfo)

		// Pool common API events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolPut func(TablePoolPutStartInfo) func(TablePoolPutDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolGet func(TablePoolGetStartInfo) func(TablePoolGetDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolWait func(TablePoolWaitStartInfo) func(TablePoolWaitDoneInfo)
	}
)

type (
	tableQueryParameters interface {
		String() string
	}
	tableDataQuery interface {
		String() string
		ID() string
		YQL() string
	}
	tableSessionInfo interface {
		ID() string
		NodeID() uint32
		Status() string
		LastUsage() time.Time
	}
	tableTransactionInfo interface {
		ID() string
	}
	tableResultErr interface {
		Err() error
	}
	tableResult interface {
		tableResultErr
		ResultSetCount() int
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionNewDoneInfo struct {
		Session tableSessionInfo
		Error   error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableKeepAliveStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableKeepAliveDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableBulkUpsertStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableBulkUpsertDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionDeleteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionDeleteDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePrepareDataQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
		Query   string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePrepareDataQueryDoneInfo struct {
		Result tableDataQuery
		Error  error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableExecuteDataQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context     *context.Context
		Call        call
		Session     tableSessionInfo
		Query       tableDataQuery
		Parameters  tableQueryParameters
		KeepInCache bool
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTransactionExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       call
		Session    tableSessionInfo
		Tx         tableTransactionInfo
		Query      tableDataQuery
		Parameters tableQueryParameters
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTransactionExecuteStatementStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context        *context.Context
		Call           call
		Session        tableSessionInfo
		Tx             tableTransactionInfo
		StatementQuery tableDataQuery
		Parameters     tableQueryParameters
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableExplainQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
		Query   string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableExplainQueryDoneInfo struct {
		AST   string
		Plan  string
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableExecuteDataQueryDoneInfo struct {
		Tx       tableTransactionInfo
		Prepared bool
		Result   tableResult
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTransactionExecuteDoneInfo struct {
		Result tableResult
		Error  error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTransactionExecuteStatementDoneInfo struct {
		Result tableResult
		Error  error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionQueryStreamReadStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionQueryStreamReadDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionQueryStreamExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       call
		Session    tableSessionInfo
		Query      tableDataQuery
		Parameters tableQueryParameters
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionQueryStreamExecuteDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxBeginDoneInfo struct {
		Tx    tableTransactionInfo
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxCommitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
		Tx      tableTransactionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxCommitDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxRollbackStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
		Tx      tableTransactionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxRollbackDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableInitDoneInfo struct {
		Limit int
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolStateChangeInfo struct {
		Size  int
		Event string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionNewDoneInfo struct {
		Session tableSessionInfo
		Error   error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolGetDoneInfo struct {
		Session  tableSessionInfo
		Attempts int
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolWaitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// TablePoolWaitDoneInfo means a wait iteration inside Get call is done
	// Warning: Session and Error may be nil at the same time. This means
	// that a wait iteration donned without any significant tableResultErr
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolWaitDoneInfo struct {
		Session tableSessionInfo
		Error   error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolPutStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolPutDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionCloseDoneInfo struct{}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionAddInfo struct {
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionRemoveInfo struct {
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableCloseDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableDoStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Label      string
		Idempotent bool
		NestedCall bool // flag when Retry called inside head Retry
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableDoDoneInfo struct {
		Attempts int
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Label      string
		Idempotent bool
		NestedCall bool // flag when Retry called inside head Retry
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableCreateSessionStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableCreateSessionDoneInfo struct {
		Session  tableSessionInfo
		Attempts int
		Error    error
	}
)
