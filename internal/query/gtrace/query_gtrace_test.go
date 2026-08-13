package gtrace

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryOnSessionClosed(t *testing.T) {
	require.NotPanics(t, func() {
		QueryOnSessionClosed(&trace.Query{}, "/local", "attach_closed")
	})

	var actual trace.QuerySessionClosedInfo
	QueryOnSessionClosed(&trace.Query{
		OnSessionClosed: func(info trace.QuerySessionClosedInfo) {
			actual = info
		},
	}, "/local", "attach_closed")
	require.Equal(t, trace.QuerySessionClosedInfo{
		PoolName: "/local",
		Reason:   "attach_closed",
	}, actual)
}
