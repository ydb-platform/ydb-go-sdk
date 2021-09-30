package trace

import (
	"context"
	"time"
)

//go:generate gtrace

type (
	// Table contains options for tracing table client activity.
	//gtrace:gen
	//gtrace:set Shortcut
	Table struct {
		// Session events
		OnCreateSession func(CreateSessionStartInfo) func(CreateSessionDoneInfo)
		OnKeepAlive     func(KeepAliveStartInfo) func(KeepAliveDoneInfo)
		OnDeleteSession func(DeleteSessionStartInfo) func(DeleteSessionDoneInfo)
		// Query events
		OnPrepareDataQuery       func(PrepareDataQueryStartInfo) func(PrepareDataQueryDoneInfo)
		OnExecuteDataQuery       func(ExecuteDataQueryStartInfo) func(ExecuteDataQueryDoneInfo)
		OnStreamExecuteScanQuery func(StreamExecuteScanQueryStartInfo) func(StreamExecuteScanQueryDoneInfo)
		// StreamRead events
		OnStreamReadTable func(StreamReadTableStartInfo) func(StreamReadTableDoneInfo)
		// Transaction events
		OnBeginTransaction    func(BeginTransactionStartInfo) func(BeginTransactionDoneInfo)
		OnCommitTransaction   func(CommitTransactionStartInfo) func(CommitTransactionDoneInfo)
		OnRollbackTransaction func(RollbackTransactionStartInfo) func(RollbackTransactionDoneInfo)
		// Pool events
		OnPoolCreate func(PoolCreateStartInfo) func(PoolCreateDoneInfo)
		OnPoolClose  func(PoolCloseStartInfo) func(PoolCloseDoneInfo)
		// PoolCycle events
		OnPoolGet          func(PoolGetStartInfo) func(PoolGetDoneInfo)
		OnPoolWait         func(PoolWaitStartInfo) func(PoolWaitDoneInfo)
		OnPoolTake         func(PoolTakeStartInfo) func(PoolTakeWaitInfo) func(PoolTakeDoneInfo)
		OnPoolPut          func(PoolPutStartInfo) func(PoolPutDoneInfo)
		OnPoolCloseSession func(PoolCloseSessionStartInfo) func(PoolCloseSessionDoneInfo)
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
		Status() string
	}
	result interface {
		ResultSetCount() int
		TotalRowCount() int
		Err() error
	}
	streamResult interface {
		Err() error
	}
	CreateSessionStartInfo struct {
		Context context.Context
	}
	CreateSessionDoneInfo struct {
		SessionID string
		Endpoint  string
		Latency   time.Duration
		Error     error
	}
	KeepAliveStartInfo struct {
		Context   context.Context
		SessionID string
	}
	KeepAliveDoneInfo struct {
		SessionID   string
		SessionInfo sessionInfo
		Error       error
	}
	DeleteSessionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	DeleteSessionDoneInfo struct {
		SessionID string
		Latency   time.Duration
		Error     error
	}
	PrepareDataQueryStartInfo struct {
		Context   context.Context
		SessionID string
		Query     string
	}
	PrepareDataQueryDoneInfo struct {
		SessionID string
		Query     string
		Result    dataQuery
		Cached    bool
		Error     error
	}
	ExecuteDataQueryStartInfo struct {
		Context    context.Context
		SessionID  string
		TxID       string
		Query      dataQuery
		Parameters queryParameters
	}
	ExecuteDataQueryDoneInfo struct {
		SessionID  string
		TxID       string
		Query      dataQuery
		Parameters queryParameters
		Prepared   bool
		Result     result
		Error      error
	}
	StreamReadTableStartInfo struct {
		Context   context.Context
		SessionID string
	}
	StreamReadTableDoneInfo struct {
		SessionID string
		Result    streamResult
		Error     error
	}
	StreamExecuteScanQueryStartInfo struct {
		Context    context.Context
		SessionID  string
		Query      dataQuery
		Parameters queryParameters
	}
	StreamExecuteScanQueryDoneInfo struct {
		SessionID  string
		Query      dataQuery
		Parameters queryParameters
		Result     streamResult
		Error      error
	}
	BeginTransactionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	BeginTransactionDoneInfo struct {
		SessionID string
		TxID      string
		Error     error
	}
	CommitTransactionStartInfo struct {
		Context   context.Context
		SessionID string
		TxID      string
	}
	CommitTransactionDoneInfo struct {
		SessionID string
		TxID      string
		Error     error
	}
	RollbackTransactionStartInfo struct {
		Context   context.Context
		SessionID string
		TxID      string
	}
	RollbackTransactionDoneInfo struct {
		SessionID string
		TxID      string
		Error     error
	}
)

type (
	PoolCreateStartInfo struct {
		Context context.Context
	}
	PoolCreateDoneInfo struct {
		SessionID string
		Error     error
	}
	PoolGetStartInfo struct {
		Context context.Context
	}
	PoolGetDoneInfo struct {
		SessionID     string
		Latency       time.Duration
		RetryAttempts int
		Error         error
	}
	PoolWaitStartInfo struct {
		Context context.Context
	}
	PoolWaitDoneInfo struct {
		SessionID string
		Error     error
	}
	PoolTakeStartInfo struct {
		Context   context.Context
		SessionID string
	}
	PoolTakeWaitInfo struct {
		SessionID string
	}
	PoolTakeDoneInfo struct {
		SessionID string
		Took      bool
		Error     error
	}
	PoolPutStartInfo struct {
		Context   context.Context
		SessionID string
	}
	PoolPutDoneInfo struct {
		SessionID string
		Error     error
	}
	PoolCloseSessionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	PoolCloseSessionDoneInfo struct {
		SessionID string
		Error     error
	}
	PoolCloseStartInfo struct {
		Context context.Context
	}
	PoolCloseDoneInfo struct {
		Error error
	}
)
