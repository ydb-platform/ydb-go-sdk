package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQuerySessionClosedMetrics(t *testing.T) {
	registry := newRecordingRegistry()
	tracer := query(recordingConfig{
		registry: registry,
		details:  trace.QuerySessionEvents,
	})

	require.Equal(t, "counter", registry.kinds["query.session.closed"])
	require.Equal(t, []string{"ydb.query.session.pool.name", "reason"},
		registry.labelNames["query.session.closed"])

	for _, reason := range []string{
		"pool_idle_timeout",
		"pool_graceful_shutdown",
		"client_timeout",
		"client_cancelled",
		"attach_closed",
		"transport_error",
		"node_shutdown",
		"session_shutdown",
		"bad_session",
		"session_busy",
	} {
		tracer.OnSessionClosed(trace.QuerySessionClosedInfo{
			PoolName: "/local",
			Reason:   reason,
		})
		require.Equal(t, float64(1), registry.value("query.session.closed", map[string]string{
			"ydb.query.session.pool.name": "/local",
			"reason":                      reason,
		}))
	}
}

func TestQuerySessionClosedMetricsDisabled(t *testing.T) {
	registry := newRecordingRegistry()
	tracer := query(recordingConfig{registry: registry})

	tracer.OnSessionClosed(trace.QuerySessionClosedInfo{
		PoolName: "/local",
		Reason:   "attach_closed",
	})
	require.Zero(t, registry.value("query.session.closed", map[string]string{
		"ydb.query.session.pool.name": "/local",
		"reason":                      "attach_closed",
	}))
}
