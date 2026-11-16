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

func TestComposeOnSessionClosed(t *testing.T) {
	t.Run("NilCallbacks", func(t *testing.T) {
		composed := Compose(&trace.Query{}, &trace.Query{})

		require.NotPanics(t, func() {
			composed.OnSessionClosed(trace.QuerySessionClosedInfo{})
		})
	})

	t.Run("CallsBothCallbacks", func(t *testing.T) {
		var calls []string
		composed := Compose(
			&trace.Query{OnSessionClosed: func(trace.QuerySessionClosedInfo) {
				calls = append(calls, "lhs")
			}},
			&trace.Query{OnSessionClosed: func(trace.QuerySessionClosedInfo) {
				calls = append(calls, "rhs")
			}},
		)

		composed.OnSessionClosed(trace.QuerySessionClosedInfo{})

		require.Equal(t, []string{"lhs", "rhs"}, calls)
	})

	t.Run("RecoversPanic", func(t *testing.T) {
		var recovered any
		composed := Compose(
			&trace.Query{OnSessionClosed: func(trace.QuerySessionClosedInfo) {
				panic("session closed panic")
			}},
			&trace.Query{},
			WithQueryPanicCallback(func(value any) {
				recovered = value
			}),
		)

		require.NotPanics(t, func() {
			composed.OnSessionClosed(trace.QuerySessionClosedInfo{})
		})
		require.Equal(t, "session closed panic", recovered)
	})
}
