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
		OnDo            func(TableDoStartInfo) func(info TableDoIntermediateInfo) func(TableDoDoneInfo)
		OnDoTx          func(TableDoTxStartInfo) func(info TableDoTxIntermediateInfo) func(TableDoTxDoneInfo)
		OnCreateSession func(
			TableCreateSessionStartInfo,
		) func(
			info TableCreateSessionIntermediateInfo,
		) func(
			TableCreateSessionDoneInfo,
		)
		// Session events
		OnSessionNew       func(TableSessionNewStartInfo) func(TableSessionNewDoneInfo)
		OnSessionDelete    func(TableSessionDeleteStartInfo) func(TableSessionDeleteDoneInfo)
		OnSessionKeepAlive func(TableKeepAliveStartInfo) func(TableKeepAliveDoneInfo)
		// Query events
		OnSessionQueryPrepare func(TablePrepareDataQueryStartInfo) func(TablePrepareDataQueryDoneInfo)
		OnSessionQueryExecute func(TableExecuteDataQueryStartInfo) func(TableExecuteDataQueryDoneInfo)
		OnSessionQueryExplain func(TableExplainQueryStartInfo) func(TableExplainQueryDoneInfo)
		// Stream events
		OnSessionQueryStreamExecute func(
			TableSessionQueryStreamExecuteStartInfo,
		) func(
			TableSessionQueryStreamExecuteIntermediateInfo,
		) func(
			TableSessionQueryStreamExecuteDoneInfo,
		)
		OnSessionQueryStreamRead func(
			TableSessionQueryStreamReadStartInfo,
		) func(
			TableSessionQueryStreamReadIntermediateInfo,
		) func(
			TableSessionQueryStreamReadDoneInfo,
		)
		// Transaction events
		OnSessionTransactionBegin func(TableSessionTransactionBeginStartInfo) func(
			TableSessionTransactionBeginDoneInfo,
		)
		OnSessionTransactionExecute func(TableTransactionExecuteStartInfo) func(
			TableTransactionExecuteDoneInfo,
		)
		OnSessionTransactionExecuteStatement func(TableTransactionExecuteStatementStartInfo) func(
			TableTransactionExecuteStatementDoneInfo,
		)
		OnSessionTransactionCommit func(TableSessionTransactionCommitStartInfo) func(
			TableSessionTransactionCommitDoneInfo,
		)
		OnSessionTransactionRollback func(TableSessionTransactionRollbackStartInfo) func(
			TableSessionTransactionRollbackDoneInfo,
		)
		// Pool state event
		OnPoolStateChange func(TablePoolStateChangeInfo)

		// Pool session lifecycle events
		OnPoolSessionAdd    func(info TablePoolSessionAddInfo)
		OnPoolSessionRemove func(info TablePoolSessionRemoveInfo)

		// OnPoolSessionNew is user-defined callback for listening events about creating sessions with
		// internal session pool calls
		//
		// Deprecated: use OnPoolSessionAdd callback
		OnPoolSessionNew func(TablePoolSessionNewStartInfo) func(TablePoolSessionNewDoneInfo)

		// OnPoolSessionClose is user-defined callback for listening sessionClose calls
		//
		// Deprecated: use OnPoolSessionRemove callback
		OnPoolSessionClose func(TablePoolSessionCloseStartInfo) func(TablePoolSessionCloseDoneInfo)
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
		Session tableSessionInfo
	}
	TableKeepAliveDoneInfo struct {
		Error error
	}
	TableSessionDeleteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
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
		Session    tableSessionInfo
		Tx         tableTransactionInfo
		Query      tableDataQuery
		Parameters tableQueryParameters
		// Deprecated: has no effect (always false). See KeepInCache flag in underlying Execute query trace
		KeepInCache bool
	}
	TableTransactionExecuteStatementStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context        *context.Context
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
		Session tableSessionInfo
	}
	TableSessionQueryStreamReadIntermediateInfo struct {
		Error error
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
		Session    tableSessionInfo
		Query      tableDataQuery
		Parameters tableQueryParameters
	}
	TableSessionQueryStreamExecuteIntermediateInfo struct {
		Error error
	}
	TableSessionQueryStreamExecuteDoneInfo struct {
		Error error
	}
	TableSessionTransactionBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session tableSessionInfo
	}
	TableSessionTransactionBeginDoneInfo struct {
		Tx    tableTransactionInfo
		Error error
	}
	TableSessionTransactionCommitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session tableSessionInfo
		Tx      tableTransactionInfo
	}
	TableSessionTransactionCommitDoneInfo struct {
		Error error
	}
	TableSessionTransactionRollbackStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session tableSessionInfo
		Tx      tableTransactionInfo
	}
	TableSessionTransactionRollbackDoneInfo struct {
		Error error
	}
	TableInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
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
	}
	TableCloseDoneInfo struct {
		Error error
	}
	TableDoStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Idempotent bool
		NestedCall bool // flag when Retry called inside head Retry
	}
	TableDoIntermediateInfo struct {
		Error error
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
		Context    *context.Context
		Idempotent bool
		NestedCall bool // flag when Retry called inside head Retry
	}
	TableDoTxIntermediateInfo struct {
		Error error
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
	}
	TableCreateSessionIntermediateInfo struct {
		Error error
	}
	TableCreateSessionDoneInfo struct {
		Session  tableSessionInfo
		Attempts int
		Error    error
	}
)
