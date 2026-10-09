package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestStrictSerializableReadWritePublicAPI(t *testing.T) {
	settings := query.TxSettings(query.WithStrictSerializableReadWrite()).ToYdbQuerySettings()
	require.IsType(t, &Ydb_Query.TransactionSettings_StrictSerializableReadWrite{}, settings.GetTxMode())

	control := query.StrictSerializableReadWriteTxControl(query.CommitTx())
	require.IsType(t, &Ydb_Query.TransactionSettings_StrictSerializableReadWrite{},
		control.ToYdbQueryTransactionControl().GetBeginTx().GetTxMode())
	require.True(t, control.ToYdbQueryTransactionControl().GetCommitTx())

	called := false
	execute := options.ExecuteSettings(query.WithCommitTimestamp(func(*query.VirtualTimestamp) {
		called = true
	}))
	require.NotNil(t, execute.CommitTimestampCallback())
	execute.CommitTimestampCallback()(nil)
	require.True(t, called)
}
