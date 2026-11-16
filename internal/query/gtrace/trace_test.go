package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithQueryPanicCallback)
}

func TestHooks(t *testing.T) {
	tracefuzz.TestHooks[trace.Query](t,
		QueryOnNew,
		QueryOnClose,
		QueryOnPoolNew,
		QueryOnPoolClose,
		QueryOnPoolTry,
		QueryOnPoolWith,
		QueryOnPoolPut,
		QueryOnPoolGet,
		QueryOnPoolChange,
		QueryOnDo,
		QueryOnDoTx,
		QueryOnExec,
		QueryOnQuery,
		QueryOnQueryResultSet,
		QueryOnQueryRow,
		QueryOnSessionCreate,
		QueryOnSessionAttach,
		QueryOnSessionClosed,
		QueryOnSessionDelete,
		QueryOnSessionExec,
		QueryOnSessionQuery,
		QueryOnSessionQueryResultSet,
		QueryOnSessionQueryRow,
		QueryOnSessionBegin,
		QueryOnSessionBeginTransaction,
		QueryOnTxCommit,
		QueryOnTxRollback,
		QueryOnTxExec,
		QueryOnTxQuery,
		QueryOnTxQueryResultSet,
		QueryOnTxQueryRow,
		QueryOnResultNew,
		QueryOnResultNextPart,
		QueryOnResultNextResultSet,
		QueryOnResultClose,
	)
}
