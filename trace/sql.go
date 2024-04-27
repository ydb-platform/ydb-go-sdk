package trace

//go:generate gtrace

import (
	"context"
	"time"
)

type (
	// DatabaseSQL specified trace of `database/sql` call activity.
	// gtrace:gen
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQL struct {
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnConnectorConnect func(DatabaseSQLConnectorConnectStartInfo) func(DatabaseSQLConnectorConnectDoneInfo)

		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnConnPing func(DatabaseSQLConnPingStartInfo) func(DatabaseSQLConnPingDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnConnPrepare func(DatabaseSQLConnPrepareStartInfo) func(DatabaseSQLConnPrepareDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnConnClose func(DatabaseSQLConnCloseStartInfo) func(DatabaseSQLConnCloseDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnConnBegin func(DatabaseSQLConnBeginStartInfo) func(DatabaseSQLConnBeginDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnConnQuery func(DatabaseSQLConnQueryStartInfo) func(DatabaseSQLConnQueryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnConnExec func(DatabaseSQLConnExecStartInfo) func(DatabaseSQLConnExecDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnConnIsTableExists func(DatabaseSQLConnIsTableExistsStartInfo) func(DatabaseSQLConnIsTableExistsDoneInfo)

		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxQuery func(DatabaseSQLTxQueryStartInfo) func(DatabaseSQLTxQueryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxExec func(DatabaseSQLTxExecStartInfo) func(DatabaseSQLTxExecDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxPrepare func(DatabaseSQLTxPrepareStartInfo) func(DatabaseSQLTxPrepareDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxCommit func(DatabaseSQLTxCommitStartInfo) func(DatabaseSQLTxCommitDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxRollback func(DatabaseSQLTxRollbackStartInfo) func(DatabaseSQLTxRollbackDoneInfo)

		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnStmtQuery func(DatabaseSQLStmtQueryStartInfo) func(DatabaseSQLStmtQueryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnStmtExec func(DatabaseSQLStmtExecStartInfo) func(DatabaseSQLStmtExecDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnStmtClose func(DatabaseSQLStmtCloseStartInfo) func(DatabaseSQLStmtCloseDoneInfo)

		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnDoTx func(DatabaseSQLDoTxStartInfo) func(DatabaseSQLDoTxIntermediateInfo) func(DatabaseSQLDoTxDoneInfo)
	}

	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnectorConnectStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnectorConnectDoneInfo struct {
		Error   error
		Session tableSessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnPingStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnPingDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnPrepareStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Query   string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnPrepareDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxPrepareStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context   *context.Context
		Call      call
		TxContext context.Context //nolint:containedctx
		Tx        tableTransactionInfo
		Query     string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxPrepareDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnCloseDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnBeginDoneInfo struct {
		Tx    tableTransactionInfo
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       call
		Query      string
		Mode       string
		Idempotent bool
		IdleTime   time.Duration
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnQueryDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnExecStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       call
		Query      string
		Mode       string
		Idempotent bool
		IdleTime   time.Duration
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnExecDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnIsTableExistsStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context   *context.Context
		Call      call
		TableName string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLConnIsTableExistsDoneInfo struct {
		Exists bool
		Error  error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context   *context.Context
		Call      call
		TxContext context.Context //nolint:containedctx
		Tx        tableTransactionInfo
		Query     string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxQueryDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxExecStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context   *context.Context
		Call      call
		TxContext context.Context //nolint:containedctx
		Tx        tableTransactionInfo
		Query     string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxExecDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxCommitStartInfo struct {
		// TxContext make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Tx      tableTransactionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxCommitDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxRollbackStartInfo struct {
		// TxContext make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Tx      tableTransactionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLTxRollbackDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLStmtCloseStartInfo struct {
		StmtContext *context.Context
		Call        call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLStmtCloseDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLStmtQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context     *context.Context
		Call        call
		StmtContext context.Context //nolint:containedctx
		Query       string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLStmtQueryDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLStmtExecStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context     *context.Context
		Call        call
		StmtContext context.Context //nolint:containedctx
		Query       string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLStmtExecDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       call
		ID         string
		Idempotent bool
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLDoTxIntermediateInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	DatabaseSQLDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
)
