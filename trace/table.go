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
	Table struct {
		// Client events
		OnInit          func(TableInitStartInfo) func(TableInitDoneInfo)
		OnClose         func(TableCloseStartInfo) func(TableCloseDoneInfo)
		OnDo            func(TableDoStartInfo) func(TableDoDoneInfo)
		OnDoTx          func(TableDoTxStartInfo) func(TableDoTxDoneInfo)
		OnCreateSession func(TableCreateSessionStartInfo) func(TableCreateSessionDoneInfo)
		// Session events
		OnSessionNew       func(TableSessionNewStartInfo) func(TableSessionNewDoneInfo)
		OnSessionDelete    func(TableSessionDeleteStartInfo) func(TableSessionDeleteDoneInfo)
		OnSessionKeepAlive func(TableKeepAliveStartInfo) func(TableKeepAliveDoneInfo)
		// Query events
		OnSessionBulkUpsert   func(TableBulkUpsertStartInfo) func(TableBulkUpsertDoneInfo)
		OnSessionQueryPrepare func(TablePrepareDataQueryStartInfo) func(TablePrepareDataQueryDoneInfo)
		OnSessionQueryExecute func(TableExecuteDataQueryStartInfo) func(TableExecuteDataQueryDoneInfo)
		OnSessionQueryExplain func(TableExplainQueryStartInfo) func(TableExplainQueryDoneInfo)
		// Stream events
		OnSessionQueryStreamExecute func(TableSessionQueryStreamExecuteStartInfo) func(TableSessionQueryStreamExecuteDoneInfo)
		OnSessionQueryStreamRead    func(TableSessionQueryStreamReadStartInfo) func(TableSessionQueryStreamReadDoneInfo)
		// Transaction events
		OnTxBegin func(TableTxBeginStartInfo) func(
			TableTxBeginDoneInfo,
		)
		OnTxExecute func(TableTransactionExecuteStartInfo) func(
			TableTransactionExecuteDoneInfo,
		)
		OnTxExecuteStatement func(TableTransactionExecuteStatementStartInfo) func(
			TableTransactionExecuteStatementDoneInfo,
		)
		OnTxCommit func(TableTxCommitStartInfo) func(
			TableTxCommitDoneInfo,
		)
		OnTxRollback func(TableTxRollbackStartInfo) func(TableTxRollbackDoneInfo)
		// Pool state event
		OnPoolStateChange func(TablePoolStateChangeInfo)

		// Pool session lifecycle events
		OnPoolSessionAdd    func(info TablePoolSessionAddInfo)
		OnPoolSessionRemove func(info TablePoolSessionRemoveInfo)

		// Pool common API events
		OnPoolPut  func(TablePoolPutStartInfo) func(TablePoolPutDoneInfo)
		OnPoolGet  func(TablePoolGetStartInfo) func(TablePoolGetDoneInfo)
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
	TableSessionNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	TableSessionNewDoneInfo struct {
		Session tableSessionInfo
		Error   error
	}
	TableKeepAliveStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	TableKeepAliveDoneInfo struct {
		Error error
	}
	TableBulkUpsertStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	TableBulkUpsertDoneInfo struct {
		Error error
	}
	TableSessionDeleteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	TableSessionDeleteDoneInfo struct {
		Error error
	}
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
	TablePrepareDataQueryDoneInfo struct {
		Result tableDataQuery
		Error  error
	}
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
	TableExplainQueryDoneInfo struct {
		AST   string
		Plan  string
		Error error
	}
	TableExecuteDataQueryDoneInfo struct {
		Tx       tableTransactionInfo
		Prepared bool
		Result   tableResult
		Error    error
	}
	TableTransactionExecuteDoneInfo struct {
		Result tableResult
		Error  error
	}
	TableTransactionExecuteStatementDoneInfo struct {
		Result tableResult
		Error  error
	}
	TableSessionQueryStreamReadStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	TableSessionQueryStreamReadDoneInfo struct {
		Error error
	}
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
	TableSessionQueryStreamExecuteDoneInfo struct {
		Error error
	}
	TableTxBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	TableTxBeginDoneInfo struct {
		Tx    tableTransactionInfo
		Error error
	}
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
	TableTxCommitDoneInfo struct {
		Error error
	}
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
	TableTxRollbackDoneInfo struct {
		Error error
	}
	TableInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	TableInitDoneInfo struct {
		Limit int
	}
	TablePoolStateChangeInfo struct {
		Size  int
		Event string
	}
	TablePoolSessionNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	TablePoolSessionNewDoneInfo struct {
		Session tableSessionInfo
		Error   error
	}
	TablePoolGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	TablePoolGetDoneInfo struct {
		Session  tableSessionInfo
		Attempts int
		Error    error
	}
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
	TablePoolWaitDoneInfo struct {
		Session tableSessionInfo
		Error   error
	}
	TablePoolPutStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	TablePoolPutDoneInfo struct {
		Error error
	}
	TablePoolSessionCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Session tableSessionInfo
	}
	TablePoolSessionCloseDoneInfo struct{}
	TablePoolSessionAddInfo       struct {
		Session tableSessionInfo
	}
	TablePoolSessionRemoveInfo struct {
		Session tableSessionInfo
	}
	TableCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	TableCloseDoneInfo struct {
		Error error
	}
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
	TableDoDoneInfo struct {
		Attempts int
		Error    error
	}
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
	TableDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
	TableCreateSessionStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	TableCreateSessionDoneInfo struct {
		Session  tableSessionInfo
		Attempts int
		Error    error
	}
)
