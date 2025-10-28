package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestNew(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		cfg := New()
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.Trace())
	})
	t.Run("WithTrace", func(t *testing.T) {
		traceCoordination := &trace.Coordination{
			OnNew: func(info trace.CoordinationNewStartInfo) func(trace.CoordinationNewDoneInfo) {
				return nil
			},
		}
		cfg := New(WithTrace(traceCoordination))
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.Trace())
	})
	t.Run("WithCommonConfig", func(t *testing.T) {
		commonConfig := config.Common{}
		cfg := New(With(commonConfig))
		require.NotNil(t, cfg)
		require.Equal(t, commonConfig.OperationTimeout(), cfg.OperationTimeout())
	})
	t.Run("WithMultipleOptions", func(t *testing.T) {
		traceCoordination := &trace.Coordination{}
		commonConfig := config.Common{}
		cfg := New(
			WithTrace(traceCoordination),
			With(commonConfig),
		)
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.Trace())
		require.Equal(t, commonConfig.OperationTimeout(), cfg.OperationTimeout())
	})
	t.Run("WithNilOption", func(t *testing.T) {
		cfg := New(nil)
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.Trace())
	})
	t.Run("WithTraceCompose", func(t *testing.T) {
		trace1 := &trace.Coordination{
			OnNew: func(info trace.CoordinationNewStartInfo) func(trace.CoordinationNewDoneInfo) {
				return nil
			},
		}
		trace2 := &trace.Coordination{
			OnClose: func(info trace.CoordinationCloseStartInfo) func(trace.CoordinationCloseDoneInfo) {
				return nil
			},
		}
		cfg := New(WithTrace(trace1), WithTrace(trace2))
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.Trace())
	})
}

func TestTrace(t *testing.T) {
	t.Run("DefaultTrace", func(t *testing.T) {
		cfg := New()
		trace := cfg.Trace()
		require.NotNil(t, trace)
	})
	t.Run("CustomTrace", func(t *testing.T) {
		traceCoordination := &trace.Coordination{
			OnNew: func(info trace.CoordinationNewStartInfo) func(trace.CoordinationNewDoneInfo) {
				return nil
			},
		}
		cfg := New(WithTrace(traceCoordination))
		trace := cfg.Trace()
		require.NotNil(t, trace)
	})
}
