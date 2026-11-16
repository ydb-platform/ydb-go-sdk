package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithTablePanicCallback,
		"OnPoolSessionAdd", "OnPoolSessionRemove", "OnPoolWait",
	)
}

func TestHooks(t *testing.T) {
	tracefuzz.TestHooks[trace.Table](t,
		TableOnInit,
		TableOnClose,
		TableOnDo,
		TableOnDoTx,
		TableOnBulkUpsert,
		TableOnCreateSession,
		TableOnSessionNew,
		TableOnSessionDelete,
		TableOnSessionKeepAlive,
		TableOnSessionBulkUpsert,
		TableOnSessionQueryPrepare,
		TableOnSessionQueryExecute,
		TableOnSessionQueryExplain,
		TableOnSessionQueryStreamExecute,
		TableOnSessionQueryStreamRead,
		TableOnTxBegin,
		TableOnTxExecute,
		TableOnTxExecuteStatement,
		TableOnTxCommit,
		TableOnTxRollback,
		TableOnPoolPut,
		TableOnPoolGet,
		TableOnPoolWith,
		TableOnPoolStateChange,
	)
}
