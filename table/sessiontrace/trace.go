package sessiontrace

//go:generate gtrace

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/options"

	table2 "github.com/ydb-platform/ydb-go-sdk/v3/table/resultset"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
)

// Trace contains options for tracing table client activity.
type (
	//gtrace:gen
	//gtrace:set shortcut
	Trace struct {
		SessionPoolTrace

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
		Context  context.Context
		Session  *table.Session
		Endpoint string
		Latency  time.Duration
		Error    error
	}
	KeepAliveStartInfo struct {
		Context context.Context
		Session *table.Session
	}
	KeepAliveDoneInfo struct {
		Context     context.Context
		Session     *table.Session
		SessionInfo options.SessionInfo
		Error       error
	}
	DeleteSessionStartInfo struct {
		Context context.Context
		Session *table.Session
	}
	DeleteSessionDoneInfo struct {
		Context context.Context
		Session *table.Session
		Latency time.Duration
		Error   error
	}
	PrepareDataQueryStartInfo struct {
		Context context.Context
		Session *table.Session
		Query   string
	}
	PrepareDataQueryDoneInfo struct {
		Context context.Context
		Session *table.Session
		Query   string
		Result  *table.DataQuery
		Cached  bool
		Error   error
	}
	ExecuteDataQueryStartInfo struct {
		Context    context.Context
		Session    *table.Session
		TxID       string
		Query      *table.DataQuery
		Parameters *table.QueryParameters
	}
	ExecuteDataQueryDoneInfo struct {
		Context    context.Context
		Session    *table.Session
		TxID       string
		Query      *table.DataQuery
		Parameters *table.QueryParameters
		Prepared   bool
		Result     *table2.Result
		Error      error
	}
	StreamReadTableStartInfo struct {
		Context context.Context
		Session *table.Session
	}
	StreamReadTableDoneInfo struct {
		Context context.Context
		Session *table.Session
		Result  *table2.Result
		Error   error
	}
	StreamExecuteScanQueryStartInfo struct {
		Context    context.Context
		Session    *table.Session
		Query      *table.DataQuery
		Parameters *table.QueryParameters
	}
	StreamExecuteScanQueryDoneInfo struct {
		Context    context.Context
		Session    *table.Session
		Query      *table.DataQuery
		Parameters *table.QueryParameters
		Result     *table2.Result
		Error      error
	}
	BeginTransactionStartInfo struct {
		Context context.Context
		Session *table.Session
	}
	BeginTransactionDoneInfo struct {
		Context context.Context
		Session *table.Session
		TxID    string
		Error   error
	}
	CommitTransactionStartInfo struct {
		Context context.Context
		Session *table.Session
		TxID    string
	}
	CommitTransactionDoneInfo struct {
		Context context.Context
		Session *table.Session
		TxID    string
		Error   error
	}
	RollbackTransactionStartInfo struct {
		Context context.Context
		Session *table.Session
		TxID    string
	}
	RollbackTransactionDoneInfo struct {
		Context context.Context
		Session *table.Session
		TxID    string
		Error   error
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
		Context context.Context
		Session *table.Session
		Error   error
	}
	SessionPoolGetStartInfo struct {
		Context context.Context
	}
	SessionPoolGetDoneInfo struct {
		Context       context.Context
		Session       *table.Session
		Latency       time.Duration
		RetryAttempts int
		Error         error
	}
	SessionPoolWaitStartInfo struct {
		Context context.Context
	}
	SessionPoolWaitDoneInfo struct {
		Context context.Context
		Session *table.Session
		Error   error
	}
	SessionPoolTakeStartInfo struct {
		Context context.Context
		Session *table.Session
	}
	SessionPoolTakeWaitInfo struct {
		Context context.Context
		Session *table.Session
	}
	SessionPoolTakeDoneInfo struct {
		Context context.Context
		Session *table.Session
		Took    bool
		Error   error
	}
	SessionPoolPutStartInfo struct {
		Context context.Context
		Session *table.Session
	}
	SessionPoolPutDoneInfo struct {
		Context context.Context
		Session *table.Session
		Error   error
	}
	SessionPoolCloseSessionStartInfo struct {
		Context context.Context
		Session *table.Session
	}
	SessionPoolCloseSessionDoneInfo struct {
		Context context.Context
		Session *table.Session
		Error   error
	}
	SessionPoolCloseStartInfo struct {
		Context context.Context
	}
	SessionPoolCloseDoneInfo struct {
		Context context.Context
		Error   error
	}
)

type (
	//gtrace:gen
	//gtrace:set shortcut
	createSessionTrace struct {
		OnCheckEnoughSpace            func(enoughSpace bool)
		OnCreateSessionGoroutineStart func() func(r createSessionResult)
		OnStartSelect                 func()
		OnReadResult                  func(r createSessionResult)
		OnContextDone                 func()
		OnPutSession                  func(session *table.Session, err error)
	}
)

type createSessionResult struct {
	s   *table.Session
	err error
}
