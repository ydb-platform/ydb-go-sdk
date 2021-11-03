package trace

import (
	"context"
)

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
		OnSessionQueryPrepare func(SessionQueryPrepareStartInfo) func(PrepareDataQueryDoneInfo)
		OnSessionQueryExecute func(ExecuteDataQueryStartInfo) func(SessionQueryPrepareDoneInfo)
		// Stream events
		OnSessionQueryStreamExecute func(SessionQueryStreamExecuteStartInfo) func(SessionQueryStreamExecuteDoneInfo)
		OnSessionQueryStreamRead    func(SessionQueryStreamReadStartInfo) func(SessionQueryStreamReadDoneInfo)
		// Transaction events
		OnSessionTransactionBegin    func(SessionTransactionBeginStartInfo) func(SessionTransactionBeginDoneInfo)
		OnSessionTransactionCommit   func(SessionTransactionCommitStartInfo) func(SessionTransactionCommitDoneInfo)
		OnSessionTransactionRollback func(SessionTransactionRollbackStartInfo) func(SessionTransactionRollbackDoneInfo)
		// Pool events
		OnPoolInit  func(PoolInitStartInfo) func(PoolInitDoneInfo)
		OnPoolClose func(PoolCloseStartInfo) func(PoolCloseDoneInfo)
		OnPoolRetry func(PoolRetryStartInfo) func(info PoolRetryInternalInfo) func(PoolRetryDoneInfo)
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
	result interface {
		ResultSetCount() int
		TotalRowCount() int
		Err() error
	}
	streamResult interface {
		Err() error
	}
	SessionNewStartInfo struct {
		Context context.Context
	}
	SessionNewDoneInfo struct {
		Session sessionInfo
		Error   error
	}
	KeepAliveStartInfo struct {
		Context context.Context
		Session sessionInfo
	}
	KeepAliveDoneInfo struct {
		Error error
	}
	SessionDeleteStartInfo struct {
		Context context.Context
		Session sessionInfo
	}
	SessionDeleteDoneInfo struct {
		Error error
	}
	SessionQueryPrepareStartInfo struct {
		Context context.Context
		Session sessionInfo
		Query   string
	}
	PrepareDataQueryDoneInfo struct {
		Result dataQuery
		Error  error
	}
	ExecuteDataQueryStartInfo struct {
		Context    context.Context
		Session    sessionInfo
		Tx         transactionInfo
		Query      dataQuery
		Parameters queryParameters
	}
	SessionQueryPrepareDoneInfo struct {
		Prepared bool
		Result   result
		Error    error
	}
	SessionQueryStreamReadStartInfo struct {
		Context context.Context
		Session sessionInfo
	}
	SessionQueryStreamReadDoneInfo struct {
		Result streamResult
		Error  error
	}
	SessionQueryStreamExecuteStartInfo struct {
		Context    context.Context
		Session    sessionInfo
		Query      dataQuery
		Parameters queryParameters
	}
	SessionQueryStreamExecuteDoneInfo struct {
		Result streamResult
		Error  error
	}
	SessionTransactionBeginStartInfo struct {
		Context context.Context
		Session sessionInfo
	}
	SessionTransactionBeginDoneInfo struct {
		Tx    transactionInfo
		Error error
	}
	SessionTransactionCommitStartInfo struct {
		Context context.Context
		Session sessionInfo
		Tx      transactionInfo
	}
	SessionTransactionCommitDoneInfo struct {
		Error error
	}
	SessionTransactionRollbackStartInfo struct {
		Context context.Context
		Session sessionInfo
		Tx      transactionInfo
	}
	SessionTransactionRollbackDoneInfo struct {
		Error error
	}
	PoolInitStartInfo struct {
		Context context.Context
	}
	PoolInitDoneInfo struct {
		Limit            int
		KeepAliveMinSize int
	}
	PoolSessionNewStartInfo struct {
		Context context.Context
	}
	PoolSessionNewDoneInfo struct {
		Session sessionInfo
		Error   error
	}
	PoolGetStartInfo struct {
		Context context.Context
	}
	PoolGetDoneInfo struct {
		Session  sessionInfo
		Attempts int
		Error    error
	}
	PoolWaitStartInfo struct {
		Context context.Context
	}
	PoolWaitDoneInfo struct {
		Error error
	}
	PoolTakeStartInfo struct {
		Context context.Context
		Session sessionInfo
	}
	PoolTakeWaitInfo struct {
	}
	PoolTakeDoneInfo struct {
		Took  bool
		Error error
	}
	PoolPutStartInfo struct {
		Context context.Context
		Session sessionInfo
	}
	PoolPutDoneInfo struct {
		Error error
	}
	PoolSessionCloseStartInfo struct {
		Context context.Context
		Session sessionInfo
	}
	PoolSessionCloseDoneInfo struct {
	}
	PoolCloseStartInfo struct {
		Context context.Context
	}
	PoolCloseDoneInfo struct {
		Error error
	}
	PoolRetryStartInfo struct {
		Context    context.Context
		Idempotent bool
	}
	PoolRetryInternalInfo struct {
		Error error
	}
	PoolRetryDoneInfo struct {
		Attempts int
		Error    error
	}
)
