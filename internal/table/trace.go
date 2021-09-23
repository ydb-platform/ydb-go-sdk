package table

//go:generate gtrace

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/sessiontrace/models"
)

// Trace contains options for tracing table client activity.
type (
	//gtrace:gen
	//gtrace:set shortcut
	Trace struct {
		OnCreateSession          func(models.CreateSessionStartInfo) func(models.CreateSessionDoneInfo)
		OnKeepAlive              func(models.KeepAliveStartInfo) func(models.KeepAliveDoneInfo)
		OnDeleteSession          func(models.DeleteSessionStartInfo) func(models.DeleteSessionDoneInfo)
		OnPrepareDataQuery       func(models.PrepareDataQueryStartInfo) func(models.PrepareDataQueryDoneInfo)
		OnExecuteDataQuery       func(models.ExecuteDataQueryStartInfo) func(models.ExecuteDataQueryDoneInfo)
		OnStreamReadTable        func(models.StreamReadTableStartInfo) func(models.StreamReadTableDoneInfo)
		OnStreamExecuteScanQuery func(models.StreamExecuteScanQueryStartInfo) func(models.StreamExecuteScanQueryDoneInfo)
		OnBeginTransaction       func(models.BeginTransactionStartInfo) func(models.BeginTransactionDoneInfo)
		OnCommitTransaction      func(models.CommitTransactionStartInfo) func(models.CommitTransactionDoneInfo)
		OnRollbackTransaction    func(models.RollbackTransactionStartInfo) func(models.RollbackTransactionDoneInfo)
	}
)

// SessionPoolTrace contains options for tracing session pool activity.
type (
	//gtrace:gen
	//gtrace:set shortcut
	SessionPoolTrace struct {
		OnCreate       func(models.SessionPoolCreateStartInfo) func(models.SessionPoolCreateDoneInfo)
		OnGet          func(models.SessionPoolGetStartInfo) func(models.SessionPoolGetDoneInfo)
		OnWait         func(models.SessionPoolWaitStartInfo) func(models.SessionPoolWaitDoneInfo)
		OnTake         func(models.SessionPoolTakeStartInfo) func(models.SessionPoolTakeWaitInfo) func(models.SessionPoolTakeDoneInfo)
		OnPut          func(models.SessionPoolPutStartInfo) func(models.SessionPoolPutDoneInfo)
		OnCloseSession func(models.SessionPoolCloseSessionStartInfo) func(models.SessionPoolCloseSessionDoneInfo)
		OnClose        func(models.SessionPoolCloseStartInfo) func(models.SessionPoolCloseDoneInfo)
	}
)
