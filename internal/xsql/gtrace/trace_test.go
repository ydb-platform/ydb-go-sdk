package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithDatabaseSQLPanicCallback)
}

func TestHooks(t *testing.T) {
	tracefuzz.TestHooks[trace.DatabaseSQL](t,
		DatabaseSQLOnConnectorConnect,
		DatabaseSQLOnConnPing,
		DatabaseSQLOnConnPrepare,
		DatabaseSQLOnConnClose,
		DatabaseSQLOnConnBegin,
		DatabaseSQLOnConnBeginTx,
		DatabaseSQLOnConnCheckNamedValue,
		DatabaseSQLOnConnQuery,
		DatabaseSQLOnConnExec,
		DatabaseSQLOnConnIsTableExists,
		DatabaseSQLOnConnIsColumnExists,
		DatabaseSQLOnConnGetIndexColumns,
		DatabaseSQLOnTxQuery,
		DatabaseSQLOnTxExec,
		DatabaseSQLOnTxPrepare,
		DatabaseSQLOnTxCommit,
		DatabaseSQLOnTxRollback,
		DatabaseSQLOnStmtQuery,
		DatabaseSQLOnStmtExec,
		DatabaseSQLOnStmtClose,
		DatabaseSQLOnDoTx,
	)
}
