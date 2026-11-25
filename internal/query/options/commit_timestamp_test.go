package options

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/querytimestamp"
)

func TestCommitTimestampCallbackOption(t *testing.T) {
	require.Nil(t, ExecuteSettings().CommitTimestampCallback())

	timestamp := querytimestamp.FromYDB(
		&Ydb.VirtualTimestamp{PlanStep: 7, TxId: 8}, querytimestamp.NewIdentity("/db"),
	)
	var got *querytimestamp.VirtualTimestamp
	settings := ExecuteSettings(WithCommitTimestamp(func(value *querytimestamp.VirtualTimestamp) {
		got = value
	}))
	require.NotNil(t, settings.CommitTimestampCallback())
	settings.CommitTimestampCallback()(timestamp)
	require.Same(t, timestamp, got)
}
