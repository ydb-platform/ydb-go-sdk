package trace

//go:generate gtrace

import "context"

type (
	// SQL specified trace of `database/sql` call activity.
	// gtrace:gen
	SQL struct {
		OnConnectorConnect func(SQLConnectorConnectStartInfo) func(SQLConnectorConnectDoneInfo)

		OnConnPing         func(SQLConnPingStartInfo) func(SQLConnPingDoneInfo)
		OnConnPrepare      func(SQLConnPrepareStartInfo) func(SQLConnPrepareDoneInfo)
		OnConnClose        func(SQLConnCloseStartInfo) func(SQLConnCloseDoneInfo)
		OnConnBeginTx      func(SQLConnBeginTxStartInfo) func(SQLConnBeginTxDoneInfo)
		OnConnQueryContext func(SQLConnQueryContextStartInfo) func(SQLConnQueryContextDoneInfo)
		OnConnExecContext  func(SQLConnExecContextStartInfo) func(SQLConnExecContextDoneInfo)

		OnTxCommit   func(SQLTxCommitStartInfo) func(SQLTxCommitDoneInfo)
		OnTxRollback func(SQLTxRollbackStartInfo) func(SQLTxRollbackDoneInfo)

		OnStmtQueryContext func(SQLStmtQueryContextStartInfo) func(SQLStmtQueryContextDoneInfo)
		OnStmtExecContext  func(SQLStmtExecContextStartInfo) func(SQLStmtExecContextDoneInfo)
		OnStmtClose        func(SQLStmtCloseStartInfo) func(SQLStmtCloseDoneInfo)
	}

	SQLConnectorConnectStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	SQLConnectorConnectDoneInfo struct {
		Error error
	}
	SQLConnPingStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	SQLConnPingDoneInfo struct {
		Error error
	}
	SQLConnPrepareStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Query   string
	}
	SQLConnPrepareDoneInfo struct {
		Error error
	}
	SQLConnCloseStartInfo struct{}
	SQLConnCloseDoneInfo  struct {
		Error error
	}
	SQLConnBeginTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	SQLConnBeginTxDoneInfo struct {
		Error error
	}
	SQLConnQueryContextStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Query   string
	}
	SQLConnQueryContextDoneInfo struct {
		Error error
	}
	SQLConnExecContextStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Query   string
	}
	SQLConnExecContextDoneInfo struct {
		Error error
	}
	SQLTxCommitStartInfo struct{}
	SQLTxCommitDoneInfo  struct {
		Error error
	}
	SQLTxRollbackStartInfo struct{}
	SQLTxRollbackDoneInfo  struct {
		Error error
	}
	SQLStmtCloseStartInfo struct{}
	SQLStmtCloseDoneInfo  struct {
		Error error
	}
	SQLStmtQueryContextStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Query   string
	}
	SQLStmtQueryContextDoneInfo struct {
		Error error
	}
	SQLStmtExecContextStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Query   string
	}
	SQLStmtExecContextDoneInfo struct {
		Error error
	}
)
