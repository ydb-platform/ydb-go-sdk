package trace

//go:generate gtrace

import (
	"context"
	"time"
)

type (
	// DatabaseSQL specified trace of `database/sql` call activity.
	// gtrace:gen
	DatabaseSQL struct {
		OnConnectorConnect func(DatabaseSQLConnectorConnectStartInfo) func(DatabaseSQLConnectorConnectDoneInfo)

		OnConnPing    func(DatabaseSQLConnPingStartInfo) func(DatabaseSQLConnPingDoneInfo)
		OnConnPrepare func(DatabaseSQLConnPrepareStartInfo) func(DatabaseSQLConnPrepareDoneInfo)
		OnConnClose   func(DatabaseSQLConnCloseStartInfo) func(DatabaseSQLConnCloseDoneInfo)
		OnConnBegin   func(DatabaseSQLConnBeginStartInfo) func(DatabaseSQLConnBeginDoneInfo)
		OnConnQuery   func(DatabaseSQLConnQueryStartInfo) func(DatabaseSQLConnQueryDoneInfo)
		OnConnExec    func(DatabaseSQLConnExecStartInfo) func(DatabaseSQLConnExecDoneInfo)

		OnTxQuery    func(DatabaseSQLTxQueryStartInfo) func(DatabaseSQLTxQueryDoneInfo)
		OnTxExec     func(DatabaseSQLTxExecStartInfo) func(DatabaseSQLTxExecDoneInfo)
		OnTxCommit   func(DatabaseSQLTxCommitStartInfo) func(DatabaseSQLTxCommitDoneInfo)
		OnTxRollback func(DatabaseSQLTxRollbackStartInfo) func(DatabaseSQLTxRollbackDoneInfo)

		OnStmtQuery func(DatabaseSQLStmtQueryStartInfo) func(DatabaseSQLStmtQueryDoneInfo)
		OnStmtExec  func(DatabaseSQLStmtExecStartInfo) func(DatabaseSQLStmtExecDoneInfo)
		OnStmtClose func(DatabaseSQLStmtCloseStartInfo) func(DatabaseSQLStmtCloseDoneInfo)

		OnDoTx func(DatabaseSQLDoTxStartInfo) func(DatabaseSQLDoTxIntermediateInfo) func(DatabaseSQLDoTxDoneInfo)
	}

	DatabaseSQLConnectorConnectStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	DatabaseSQLConnectorConnectDoneInfo struct {
		Error   error
		Session tableSessionInfo
	}
	DatabaseSQLConnPingStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	DatabaseSQLConnPingDoneInfo struct {
		Error error
	}
	DatabaseSQLConnPrepareStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Query   string
	}
	DatabaseSQLConnPrepareDoneInfo struct {
		Error error
	}
	DatabaseSQLConnCloseStartInfo struct{}
	DatabaseSQLConnCloseDoneInfo  struct {
		Error error
	}
	DatabaseSQLConnBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	DatabaseSQLConnBeginDoneInfo struct {
		Tx    tableTransactionInfo
		Error error
	}
	DatabaseSQLConnQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Query      string
		Mode       string
		Idempotent bool
		IdleTime   time.Duration
	}
	DatabaseSQLConnQueryDoneInfo struct {
		Error error
	}
	DatabaseSQLConnExecStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Query      string
		Mode       string
		Idempotent bool
		IdleTime   time.Duration
	}
	DatabaseSQLConnExecDoneInfo struct {
		Error error
	}
	DatabaseSQLTxQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		TxContext  context.Context
		Tx         tableTransactionInfo
		Query      string
		Idempotent bool
	}
	DatabaseSQLTxQueryDoneInfo struct {
		Error error
	}
	DatabaseSQLTxExecStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		TxContext  context.Context
		Tx         tableTransactionInfo
		Query      string
		Idempotent bool
	}
	DatabaseSQLTxExecDoneInfo struct {
		Error error
	}
	DatabaseSQLTxCommitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Tx      tableTransactionInfo
	}
	DatabaseSQLTxCommitDoneInfo struct {
		Error error
	}
	DatabaseSQLTxRollbackStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Tx      tableTransactionInfo
	}
	DatabaseSQLTxRollbackDoneInfo struct {
		Error error
	}
	DatabaseSQLStmtCloseStartInfo struct{}
	DatabaseSQLStmtCloseDoneInfo  struct {
		Error error
	}
	DatabaseSQLStmtQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Query   string
	}
	DatabaseSQLStmtQueryDoneInfo struct {
		Error error
	}
	DatabaseSQLStmtExecStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Query   string
	}
	DatabaseSQLStmtExecDoneInfo struct {
		Error error
	}
	DatabaseSQLDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		ID         string
		Idempotent bool
	}
	DatabaseSQLDoTxIntermediateInfo struct {
		Error error
	}
	DatabaseSQLDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
)
