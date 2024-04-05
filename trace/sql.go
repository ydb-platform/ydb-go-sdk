package trace

//go:generate gtrace

import (
	"context"
	"time"
)

type (
	// DatabaseSQL specified trace of `database/sql` call activity.
	// gtrace:gen
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQL struct {
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnConnectorConnect func(DatabaseSQLConnectorConnectStartInfo) func(DatabaseSQLConnectorConnectDoneInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnConnPing func(DatabaseSQLConnPingStartInfo) func(DatabaseSQLConnPingDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnConnPrepare func(DatabaseSQLConnPrepareStartInfo) func(DatabaseSQLConnPrepareDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnConnClose func(DatabaseSQLConnCloseStartInfo) func(DatabaseSQLConnCloseDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnConnBegin func(DatabaseSQLConnBeginStartInfo) func(DatabaseSQLConnBeginDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnConnQuery func(DatabaseSQLConnQueryStartInfo) func(DatabaseSQLConnQueryDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnConnExec func(DatabaseSQLConnExecStartInfo) func(DatabaseSQLConnExecDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnConnIsTableExists func(DatabaseSQLConnIsTableExistsStartInfo) func(DatabaseSQLConnIsTableExistsDoneInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnTxQuery func(DatabaseSQLTxQueryStartInfo) func(DatabaseSQLTxQueryDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnTxExec func(DatabaseSQLTxExecStartInfo) func(DatabaseSQLTxExecDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnTxPrepare func(DatabaseSQLTxPrepareStartInfo) func(DatabaseSQLTxPrepareDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnTxCommit func(DatabaseSQLTxCommitStartInfo) func(DatabaseSQLTxCommitDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnTxRollback func(DatabaseSQLTxRollbackStartInfo) func(DatabaseSQLTxRollbackDoneInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnStmtQuery func(DatabaseSQLStmtQueryStartInfo) func(DatabaseSQLStmtQueryDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnStmtExec func(DatabaseSQLStmtExecStartInfo) func(DatabaseSQLStmtExecDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnStmtClose func(DatabaseSQLStmtCloseStartInfo) func(DatabaseSQLStmtCloseDoneInfo)

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
		OnDoTx func(DatabaseSQLDoTxStartInfo) func(DatabaseSQLDoTxIntermediateInfo) func(DatabaseSQLDoTxDoneInfo)
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnectorConnectStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnectorConnectDoneInfo struct {
		Error   error
		Session tableSessionInfo
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnPingStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnPingDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnPrepareStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Query   string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnPrepareDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLTxPrepareStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context   *context.Context
		Call      call
		TxContext *context.Context
		Tx        tableTransactionInfo
		Query     string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLTxPrepareDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnCloseDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnBeginDoneInfo struct {
		Tx    tableTransactionInfo
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
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
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/master/VERSIONING.md#unstable
	DatabaseSQLConnQueryDoneInfo struct {
		Error error
	}
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
	DatabaseSQLConnExecDoneInfo struct {
		Error error
	}
	DatabaseSQLConnIsTableExistsStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context   *context.Context
		Call      call
		TableName string
	}
	DatabaseSQLConnIsTableExistsDoneInfo struct {
		Exists bool
		Error  error
	}
	DatabaseSQLTxQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context   *context.Context
		Call      call
		TxContext context.Context
		Tx        tableTransactionInfo
		Query     string
	}
	DatabaseSQLTxQueryDoneInfo struct {
		Error error
	}
	DatabaseSQLTxExecStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context   *context.Context
		Call      call
		TxContext context.Context
		Tx        tableTransactionInfo
		Query     string
	}
	DatabaseSQLTxExecDoneInfo struct {
		Error error
	}
	DatabaseSQLTxCommitStartInfo struct {
		// TxContext make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Tx      tableTransactionInfo
	}
	DatabaseSQLTxCommitDoneInfo struct {
		Error error
	}
	DatabaseSQLTxRollbackStartInfo struct {
		// TxContext make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Tx      tableTransactionInfo
	}
	DatabaseSQLTxRollbackDoneInfo struct {
		Error error
	}
	DatabaseSQLStmtCloseStartInfo struct {
		StmtContext *context.Context
		Call        call
	}
	DatabaseSQLStmtCloseDoneInfo struct {
		Error error
	}
	DatabaseSQLStmtQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context     *context.Context
		Call        call
		StmtContext context.Context
		Query       string
	}
	DatabaseSQLStmtQueryDoneInfo struct {
		Error error
	}
	DatabaseSQLStmtExecStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context     *context.Context
		Call        call
		StmtContext context.Context
		Query       string
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
		Call       call
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
