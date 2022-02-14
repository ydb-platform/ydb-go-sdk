package trace

import (
	"context"
)

// tool gtrace used from repository github.com/asmyasnikov/cmd/gtrace

//go:generate gtrace

type (
	// Table contains options for tracing table client activity.
	//gtrace:gen
	//gtrace:set Shortcut
	Table struct {
		// Session events
		OnSessionNew       func(SessionNewStartInfo) func(SessionNewDoneInfo)
		OnSessionDelete    func(SessionDeleteStartInfo) func(SessionDeleteDoneInfo)
		OnSessionKeepAlive func(KeepAliveStartInfo) func(KeepAliveDoneInfo)
		// Query events
		OnSessionQueryPrepare func(PrepareDataQueryStartInfo) func(PrepareDataQueryDoneInfo)
		OnSessionQueryExecute func(ExecuteDataQueryStartInfo) func(ExecuteDataQueryDoneInfo)
		OnSessionQueryExplain func(ExplainQueryStartInfo) func(ExplainQueryDoneInfo)
		// Stream events
		OnSessionQueryStreamExecute func(
			SessionQueryStreamExecuteStartInfo,
		) func(
			SessionQueryStreamExecuteIntermediateInfo,
		) func(
			SessionQueryStreamExecuteDoneInfo,
		)
		OnSessionQueryStreamRead func(
			SessionQueryStreamReadStartInfo,
		) func(
			SessionQueryStreamReadIntermediateInfo,
		) func(
			SessionQueryStreamReadDoneInfo,
		)
		// Transaction events
		OnSessionTransactionBegin    func(SessionTransactionBeginStartInfo) func(SessionTransactionBeginDoneInfo)
		OnSessionTransactionCommit   func(SessionTransactionCommitStartInfo) func(SessionTransactionCommitDoneInfo)
		OnSessionTransactionRollback func(SessionTransactionRollbackStartInfo) func(SessionTransactionRollbackDoneInfo)
		// Pool events
		OnPoolInit  func(PoolInitStartInfo) func(PoolInitDoneInfo)
		OnPoolClose func(PoolCloseStartInfo) func(PoolCloseDoneInfo)
		OnPoolDo    func(PoolDoStartInfo) func(info PoolDoIntermediateInfo) func(PoolDoDoneInfo)
		OnPoolDoTx  func(PoolDoTxStartInfo) func(info PoolDoTxIntermediateInfo) func(PoolDoTxDoneInfo)
		// Pool session lifecycle events
		OnPoolSessionNew   func(PoolSessionNewStartInfo) func(PoolSessionNewDoneInfo)
		OnPoolSessionClose func(PoolSessionCloseStartInfo) func(PoolSessionCloseDoneInfo)
		// Pool common API events
		OnPoolPut func(PoolPutStartInfo) func(PoolPutDoneInfo)
		// Pool native API events
		OnPoolGet  func(PoolGetStartInfo) func(PoolGetDoneInfo)
		OnPoolWait func(PoolWaitStartInfo) func(PoolWaitDoneInfo)
		// Pool ydbsql API events
		OnPoolTake func(PoolTakeStartInfo) func(PoolTakeWaitInfo) func(PoolTakeDoneInfo)
	}
)

type (
	queryParameters interface {
		String() string
	}
	dataQuery interface {
		String() string
		ID() string
		YQL() string
	}
	sessionInfo interface {
		ID() string
		Status() string
	}
	transactionInfo interface {
		ID() string
	}
	resultErr interface {
		Err() error
	}
	result interface {
		resultErr
		ResultSetCount() int
	}
	SessionNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	SessionNewDoneInfo struct {
		Session sessionInfo
		Error   error
	}
	KeepAliveStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
	}
	KeepAliveDoneInfo struct {
		Error error
	}
	SessionDeleteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
	}
	SessionDeleteDoneInfo struct {
		Error error
	}
	PrepareDataQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
		Query   string
	}
	PrepareDataQueryDoneInfo struct {
		Result dataQuery
		Error  error
	}
	ExecuteDataQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Session    sessionInfo
		Query      dataQuery
		Parameters queryParameters
	}
	ExplainQueryStartInfo struct {
		Context *context.Context
		Session sessionInfo
		Query   string
	}
	ExplainQueryDoneInfo struct {
		AST   string
		Plan  string
		Error error
	}
	ExecuteDataQueryDoneInfo struct {
		Tx       transactionInfo
		Prepared bool
		Result   result
		Error    error
	}
	SessionQueryStreamReadStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
	}
	SessionQueryStreamReadIntermediateInfo struct {
		Error error
	}
	SessionQueryStreamReadDoneInfo struct {
		Error error
	}
	SessionQueryStreamExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Session    sessionInfo
		Query      dataQuery
		Parameters queryParameters
	}
	SessionQueryStreamExecuteIntermediateInfo struct {
		Error error
	}
	SessionQueryStreamExecuteDoneInfo struct {
		Error error
	}
	SessionTransactionBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
	}
	SessionTransactionBeginDoneInfo struct {
		Tx    transactionInfo
		Error error
	}
	SessionTransactionCommitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
		Tx      transactionInfo
	}
	SessionTransactionCommitDoneInfo struct {
		Error error
	}
	SessionTransactionRollbackStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
		Tx      transactionInfo
	}
	SessionTransactionRollbackDoneInfo struct {
		Error error
	}
	PoolInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	PoolInitDoneInfo struct {
		Limit            int
		KeepAliveMinSize int
	}
	PoolSessionNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	PoolSessionNewDoneInfo struct {
		Session sessionInfo
		Error   error
	}
	PoolGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	PoolGetDoneInfo struct {
		Session  sessionInfo
		Attempts int
		Error    error
	}
	PoolWaitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	// PoolWaitDoneInfo means a wait iteration inside Get call is done
	// Warning: Session and Error may be nil at the same time. This means
	// that a wait iteration donned without any significant resultErr
	PoolWaitDoneInfo struct {
		Session sessionInfo
		Error   error
	}
	PoolTakeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
	}
	PoolTakeWaitInfo struct{}
	PoolTakeDoneInfo struct {
		Took  bool
		Error error
	}
	PoolPutStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
	}
	PoolPutDoneInfo struct {
		Error error
	}
	PoolSessionCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Session sessionInfo
	}
	PoolSessionCloseDoneInfo struct{}
	PoolCloseStartInfo       struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	PoolCloseDoneInfo struct {
		Error error
	}
	PoolDoStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Idempotent bool
	}
	PoolDoIntermediateInfo struct {
		Error error
	}
	PoolDoDoneInfo struct {
		Attempts int
		Error    error
	}
	PoolDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Idempotent bool
	}
	PoolDoTxIntermediateInfo struct {
		Error error
	}
	PoolDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
)
