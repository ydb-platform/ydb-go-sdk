package tx

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestStrictSerializableReadWriteSettingsAndControl(t *testing.T) {
	settings := NewSettings(WithStrictSerializableReadWrite())
	querySettings := settings.ToYdbQuerySettings()
	require.IsType(t, &Ydb_Query.TransactionSettings_StrictSerializableReadWrite{}, querySettings.GetTxMode())
	require.NotNil(t, querySettings.GetStrictSerializableReadWrite())
	field := querySettings.ProtoReflect().Descriptor().Fields().ByName("strict_serializable_read_write")
	require.Equal(t, protoreflect.FieldNumber(7), field.Number())

	require.PanicsWithValue(t, "StrictSerializableRW is supported only by Query Service", func() {
		settings.ToYdbTableSettings()
	})

	control := StrictSerializableReadWriteTxControl()
	require.True(t, control.IsBeginTxWithoutCommit())
	require.IsType(t, &Ydb_Query.TransactionSettings_StrictSerializableReadWrite{},
		control.ToYdbQueryTransactionControl().GetBeginTx().GetTxMode())
	committed := StrictSerializableReadWriteTxControl(CommitTx())
	require.False(t, committed.IsBeginTxWithoutCommit())
	require.True(t, committed.ToYdbQueryTransactionControl().GetCommitTx())
}
