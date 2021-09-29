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
		OnCreateSession          func(CreateSessionStartInfo) func(CreateSessionDoneInfo)
		OnKeepAlive              func(KeepAliveStartInfo) func(KeepAliveDoneInfo)
		OnDeleteSession          func(DeleteSessionStartInfo) func(DeleteSessionDoneInfo)
		OnPrepareDataQuery       func(PrepareDataQueryStartInfo) func(PrepareDataQueryDoneInfo)
		OnExecuteDataQuery       func(ExecuteDataQueryStartInfo) func(ExecuteDataQueryDoneInfo)
		OnStreamReadTable        func(StreamReadTableStartInfo) func(StreamReadTableDoneInfo)
		OnStreamExecuteScanQuery func(StreamExecuteScanQueryStartInfo) func(StreamExecuteScanQueryDoneInfo)
		OnBeginTransaction       func(BeginTransactionStartInfo) func(BeginTransactionDoneInfo)
		OnCommitTransaction      func(CommitTransactionStartInfo) func(CommitTransactionDoneInfo)
		OnRollbackTransaction    func(RollbackTransactionStartInfo) func(RollbackTransactionDoneInfo)

		OnPoolCreate       func(PoolCreateStartInfo) func(PoolCreateDoneInfo)
		OnPoolGet          func(PoolGetStartInfo) func(PoolGetDoneInfo)
		OnPoolWait         func(PoolWaitStartInfo) func(PoolWaitDoneInfo)
		OnPoolTake         func(PoolTakeStartInfo) func(PoolTakeWaitInfo) func(PoolTakeDoneInfo)
		OnPoolPut          func(PoolPutStartInfo) func(PoolPutDoneInfo)
		OnPoolCloseSession func(PoolCloseSessionStartInfo) func(PoolCloseSessionDoneInfo)
		OnPoolClose        func(PoolCloseStartInfo) func(PoolCloseDoneInfo)
	}
)

type (
	QueryParameters interface {
	}
	DataQuery interface {
	}
	SessionInfo interface {
		Status() string
	}
	Result interface {
	}
	CreateSessionStartInfo struct {
		Context context.Context
	}
	CreateSessionDoneInfo struct {
		Context   context.Context
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
		Context     context.Context
		SessionID   string
		SessionInfo SessionInfo
		Error       error
	}
	DeleteSessionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	DeleteSessionDoneInfo struct {
		Context   context.Context
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
		Context   context.Context
		SessionID string
		Query     string
		Result    DataQuery
		Cached    bool
		Error     error
	}
	ExecuteDataQueryStartInfo struct {
		Context    context.Context
		SessionID  string
		TxID       string
		Query      DataQuery
		Parameters QueryParameters
	}
	ExecuteDataQueryDoneInfo struct {
		Context    context.Context
		SessionID  string
		TxID       string
		Query      DataQuery
		Parameters QueryParameters
		Prepared   bool
		Result     Result
		Error      error
	}
	StreamReadTableStartInfo struct {
		Context   context.Context
		SessionID string
	}
	StreamReadTableDoneInfo struct {
		Context   context.Context
		SessionID string
		Result    Result
		Error     error
	}
	StreamExecuteScanQueryStartInfo struct {
		Context    context.Context
		SessionID  string
		Query      DataQuery
		Parameters QueryParameters
	}
	StreamExecuteScanQueryDoneInfo struct {
		Context    context.Context
		SessionID  string
		Query      DataQuery
		Parameters QueryParameters
		Result     Result
		Error      error
	}
	BeginTransactionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	BeginTransactionDoneInfo struct {
		Context   context.Context
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
		Context   context.Context
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
		Context   context.Context
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
		Context   context.Context
		SessionID string
		Error     error
	}
	PoolGetStartInfo struct {
		Context context.Context
	}
	PoolGetDoneInfo struct {
		Context       context.Context
		SessionID     string
		Latency       time.Duration
		RetryAttempts int
		Error         error
	}
	PoolWaitStartInfo struct {
		Context context.Context
	}
	PoolWaitDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	PoolTakeStartInfo struct {
		Context   context.Context
		SessionID string
	}
	PoolTakeWaitInfo struct {
		Context   context.Context
		SessionID string
	}
	PoolTakeDoneInfo struct {
		Context   context.Context
		SessionID string
		Took      bool
		Error     error
	}
	PoolPutStartInfo struct {
		Context   context.Context
		SessionID string
	}
	PoolPutDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	PoolCloseSessionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	PoolCloseSessionDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	PoolCloseStartInfo struct {
		Context context.Context
	}
	PoolCloseDoneInfo struct {
		Context context.Context
		Error   error
	}
)
