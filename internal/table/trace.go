package table

//go:generate gtrace

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/resultset"
)

// Trace contains options for tracing table client activity.
type (
	//gtrace:gen
	//gtrace:set shortcut
	Trace struct {
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
	}
)

type (
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
		SessionInfo options.SessionInfo
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
		Result    *DataQuery
		Cached    bool
		Error     error
	}
	ExecuteDataQueryStartInfo struct {
		Context    context.Context
		SessionID  string
		TxID       string
		Query      *DataQuery
		Parameters *QueryParameters
	}
	ExecuteDataQueryDoneInfo struct {
		Context    context.Context
		SessionID  string
		TxID       string
		Query      *DataQuery
		Parameters *QueryParameters
		Prepared   bool
		Result     resultset.Result
		Error      error
	}
	StreamReadTableStartInfo struct {
		Context   context.Context
		SessionID string
	}
	StreamReadTableDoneInfo struct {
		Context   context.Context
		SessionID string
		Result    resultset.Result
		Error     error
	}
	StreamExecuteScanQueryStartInfo struct {
		Context    context.Context
		SessionID  string
		Query      *DataQuery
		Parameters *QueryParameters
	}
	StreamExecuteScanQueryDoneInfo struct {
		Context    context.Context
		SessionID  string
		Query      *DataQuery
		Parameters *QueryParameters
		Result     resultset.Result
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
	RetryLoopStartInfo struct {
		Context context.Context
	}
	RetryLoopDoneInfo struct {
		Context  context.Context
		Latency  time.Duration
		Attempts int
	}
)

// SessionPoolTrace contains options for tracing session pool activity.
type (
	//gtrace:gen
	//gtrace:set shortcut
	SessionPoolTrace struct {
		OnCreate       func(SessionPoolCreateStartInfo) func(SessionPoolCreateDoneInfo)
		OnGet          func(SessionPoolGetStartInfo) func(SessionPoolGetDoneInfo)
		OnWait         func(SessionPoolWaitStartInfo) func(SessionPoolWaitDoneInfo)
		OnTake         func(SessionPoolTakeStartInfo) func(SessionPoolTakeWaitInfo) func(SessionPoolTakeDoneInfo)
		OnPut          func(SessionPoolPutStartInfo) func(SessionPoolPutDoneInfo)
		OnCloseSession func(SessionPoolCloseSessionStartInfo) func(SessionPoolCloseSessionDoneInfo)
		OnClose        func(SessionPoolCloseStartInfo) func(SessionPoolCloseDoneInfo)
	}
)

type (
	SessionPoolCreateStartInfo struct {
		Context context.Context
	}
	SessionPoolCreateDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	SessionPoolGetStartInfo struct {
		Context context.Context
	}
	SessionPoolGetDoneInfo struct {
		Context       context.Context
		SessionID     string
		Latency       time.Duration
		RetryAttempts int
		Error         error
	}
	SessionPoolWaitStartInfo struct {
		Context context.Context
	}
	SessionPoolWaitDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	SessionPoolTakeStartInfo struct {
		Context   context.Context
		SessionID string
	}
	SessionPoolTakeWaitInfo struct {
		Context   context.Context
		SessionID string
	}
	SessionPoolTakeDoneInfo struct {
		Context   context.Context
		SessionID string
		Took      bool
		Error     error
	}
	SessionPoolPutStartInfo struct {
		Context   context.Context
		SessionID string
	}
	SessionPoolPutDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	SessionPoolCloseSessionStartInfo struct {
		Context   context.Context
		SessionID string
	}
	SessionPoolCloseSessionDoneInfo struct {
		Context   context.Context
		SessionID string
		Error     error
	}
	SessionPoolCloseStartInfo struct {
		Context context.Context
	}
	SessionPoolCloseDoneInfo struct {
		Context context.Context
		Error   error
	}
)
