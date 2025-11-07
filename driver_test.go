package ydb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestOpen(t *testing.T) {
	t.Run("context already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := ydb.Open(ctx, "grpc://localhost:2136/local")
		require.ErrorIs(t, err, context.Canceled)
		assert.Regexp(t, "^context canceled at", err.Error())
	})

	t.Run("context canceled at internal connect()", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		_, err := ydb.Open(ctx, "grpc://localhost:2136/local", ydb.WithTraceDriver(trace.Driver{
			OnInit: func(disi trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
				cancel()

				return func(didi trace.DriverInitDoneInfo) {}
			},
		}))
		require.ErrorIs(t, err, context.Canceled)
		assert.Regexp(t, "^context canceled at", err.Error())
	})
}
