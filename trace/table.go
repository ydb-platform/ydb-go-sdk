package trace

import (
	"context"
)

type (
	// Table specified trace of table client activity.
	// gtrace:gen
	// gtrace:out internal/table/gtrace
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	Table struct {
		// Client events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnInit func(TableInitStartInfo) func(TableInitDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnClose func(TableCloseStartInfo) func(TableCloseDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnDo func(TableDoStartInfo) func(TableDoDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnDoTx func(TableDoTxStartInfo) func(TableDoTxDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnBulkUpsert func(TableBulkUpsertStartInfo) func(TableBulkUpsertDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnCreateSession func(TableCreateSessionStartInfo) func(TableCreateSessionDoneInfo)
		// Session events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionNew func(TableSessionNewStartInfo) func(TableSessionNewDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionDelete func(TableSessionDeleteStartInfo) func(TableSessionDeleteDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionKeepAlive func(TableKeepAliveStartInfo) func(TableKeepAliveDoneInfo)
		// Query events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionBulkUpsert func(TableSessionBulkUpsertStartInfo) func(TableSessionBulkUpsertDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryPrepare func(TablePrepareDataQueryStartInfo) func(TablePrepareDataQueryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryExecute func(TableExecuteDataQueryStartInfo) func(TableExecuteDataQueryDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryExplain func(TableExplainQueryStartInfo) func(TableExplainQueryDoneInfo)
		// Stream events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryStreamExecute func(TableSessionQueryStreamExecuteStartInfo) func(TableSessionQueryStreamExecuteDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnSessionQueryStreamRead func(TableSessionQueryStreamReadStartInfo) func(TableSessionQueryStreamReadDoneInfo)
		// Transaction events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxBegin func(TableTxBeginStartInfo) func(
			TableTxBeginDoneInfo,
		)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxExecute func(TableTransactionExecuteStartInfo) func(
			TableTransactionExecuteDoneInfo,
		)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxExecuteStatement func(TableTransactionExecuteStatementStartInfo) func(
			TableTransactionExecuteStatementDoneInfo,
		)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxCommit func(TableTxCommitStartInfo) func(
			TableTxCommitDoneInfo,
		)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnTxRollback func(TableTxRollbackStartInfo) func(TableTxRollbackDoneInfo)

		// Pool common API events
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolPut func(TablePoolPutStartInfo) func(TablePoolPutDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolGet func(TablePoolGetStartInfo) func(TablePoolGetDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolWith func(TablePoolWithStartInfo) func(TablePoolWithDoneInfo)
		// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
		OnPoolStateChange func(TablePoolStateChangeInfo)

		// Deprecated
		// Will be removed after March 2025.
		// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
		OnPoolSessionAdd func(info TablePoolSessionAddInfo)
		// Deprecated
		// Will be removed after March 2025.
		// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
		OnPoolSessionRemove func(info TablePoolSessionRemoveInfo)
		// Deprecated
		// Will be removed after March 2025.
		// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
		OnPoolWait func(TablePoolWaitStartInfo) func(TablePoolWaitDoneInfo)
	}
)

type (
	// TableQueryParameters contains query parameters metadata for trace hooks.
	TableQueryParameters interface {
		String() string
	}
	// TableDataQuery contains data query metadata for trace hooks.
	TableDataQuery interface {
		String() string
		ID() string
		YQL() string
	}
	// TableResultErr contains result error metadata for trace hooks.
	TableResultErr interface {
		Err() error
	}
	// TableResult contains result metadata for trace hooks.
	TableResult interface {
		TableResultErr
		ResultSetCount() int
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionNewDoneInfo struct {
		Session SessionInfo
		Error   error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableKeepAliveStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableKeepAliveDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableBulkUpsertStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableBulkUpsertDoneInfo struct {
		Error    error
		Attempts int
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionBulkUpsertStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call

		Session SessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionBulkUpsertDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionDeleteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionDeleteDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePrepareDataQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
		Query   string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePrepareDataQueryDoneInfo struct {
		Result TableDataQuery
		Error  error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableExecuteDataQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context     *context.Context
		Call        Call
		Session     SessionInfo
		Query       TableDataQuery
		Parameters  TableQueryParameters
		KeepInCache bool
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTransactionExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       Call
		Session    SessionInfo
		Tx         TxInfo
		Query      TableDataQuery
		Parameters TableQueryParameters
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTransactionExecuteStatementStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context        *context.Context
		Call           Call
		Session        SessionInfo
		Tx             TxInfo
		StatementQuery TableDataQuery
		Parameters     TableQueryParameters
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableExplainQueryStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
		Query   string
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableExplainQueryDoneInfo struct {
		AST   string
		Plan  string
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableExecuteDataQueryDoneInfo struct {
		Tx       TxInfo
		Prepared bool
		Result   TableResult
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTransactionExecuteDoneInfo struct {
		Result TableResult
		Error  error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTransactionExecuteStatementDoneInfo struct {
		Result TableResult
		Error  error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionQueryStreamReadStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionQueryStreamReadDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionQueryStreamExecuteStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context    *context.Context
		Call       Call
		Session    SessionInfo
		Query      TableDataQuery
		Parameters TableQueryParameters
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableSessionQueryStreamExecuteDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxBeginStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxBeginDoneInfo struct {
		Tx    TxInfo
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxCommitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
		Tx      TxInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxCommitDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxRollbackStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
		Tx      TxInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableTxRollbackDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableInitDoneInfo struct {
		Limit int
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolStateChangeInfo struct {
		Limit            int
		Idle             int
		CreateInProgress int
		Concurrency      int
		Size             int

		// Deprecated: will be removed after November 2026.
		// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
		Index int
		// Deprecated: will be removed after November 2026.
		// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
		Wait int
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionNewDoneInfo struct {
		Session SessionInfo
		Error   error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolGetDoneInfo struct {
		Session      SessionInfo
		Attempts     int
		NodeHintInfo *NodeHintInfo
		Error        error
	}
	// Deprecated
	// Will be removed after March 2025.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
	TablePoolWaitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Deprecated
	// Will be removed after March 2025.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
	TablePoolWaitDoneInfo struct {
		Session SessionInfo
		Error   error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolWithStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolWithDoneInfo struct {
		Attempts int
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolPutStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolPutDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
		Session SessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TablePoolSessionCloseDoneInfo struct{}
	// Deprecated
	// Will be removed after March 2025.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
	TablePoolSessionAddInfo struct {
		Session SessionInfo
	}
	// Deprecated
	// Will be removed after March 2025.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
	TablePoolSessionRemoveInfo struct {
		Session SessionInfo
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableCloseDoneInfo struct {
		Error error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableDoStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call

		Label      string
		Idempotent bool
		NestedCall bool // flag when Retry called inside head Retry
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableDoDoneInfo struct {
		Attempts int
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableDoTxStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call

		Label      string
		Idempotent bool
		NestedCall bool // flag when Retry called inside head Retry
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableDoTxDoneInfo struct {
		Attempts int
		Error    error
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableCreateSessionStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    Call
	}
	// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
	TableCreateSessionDoneInfo struct {
		Session  SessionInfo
		Attempts int
		Error    error
	}
)
