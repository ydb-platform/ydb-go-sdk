//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryPoolMetricsWhileSessionIsCheckedOut(t *testing.T) {
	// Regression: https://github.com/ydb-platform/ydb-go-sdk/issues/2295
	scope := newScope(t)
	ctx, cancel := context.WithTimeout(scope.Ctx, 30*time.Second)
	defer cancel()

	registry := &registryConfig{
		details:    trace.QueryPoolEvents,
		gauges:     newVec[gaugeVec](),
		counters:   newVec[counterVec](),
		timers:     newVec[timerVec](),
		histograms: newVec[histogramVec](),
	}
	db := scope.Driver(
		ydb.WithSessionPoolSizeLimit(1),
		metrics.WithTraces(registry),
	)

	// Prime the explicit session pool without relying on the warm-up option.
	var sessionID string
	require.NoError(t, db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		sessionID = s.ID()

		return s.Exec(ctx, "SELECT 1")
	}, query.WithIdempotent()))
	require.NotEmpty(t, sessionID)

	t.Run("IdleBeforeCheckout", func(t *testing.T) {
		assertQueryPoolOccupancy(t, registry, 0, 1)
	})

	require.NoError(t, db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		if err := s.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}

		// Do retains the session until this callback returns, even after Exec completes.
		// Inspect the actual metrics adapter output while the only pool slot is occupied.
		t.Run("CheckedOut", func(t *testing.T) {
			require.Equal(t, sessionID, s.ID(), "the primed session must be reused")
			assertQueryPoolOccupancy(t, registry, 1, 0)
		})

		return nil
	}, query.WithIdempotent()))

	t.Run("IdleAfterReturn", func(t *testing.T) {
		assertQueryPoolOccupancy(t, registry, 0, 1)
	})
}

func assertQueryPoolOccupancy(t *testing.T, registry *registryConfig, inUse, idle float64) {
	t.Helper()

	for name, want := range map[string]float64{
		"limit":              1,
		"index":              1,
		"create_in_progress": 0,
		"in_use":             inUse,
		"idle":               idle,
	} {
		metricName := "ydb.query.pool.size." + name + "{}"
		assert.Equal(t, want, queryPoolGaugeValue(t, registry, metricName), metricName)
	}
}

func queryPoolGaugeValue(t *testing.T, registry *registryConfig, name string) float64 {
	t.Helper()

	registry.gauges.mtx.RLock()
	defer registry.gauges.mtx.RUnlock()

	vec := registry.gauges.data[name]
	require.NotNil(t, vec, "metric vector %s must be registered", name)

	vec.m.RLock()
	defer vec.m.RUnlock()

	g := vec.gauges[name]
	require.NotNil(t, g, "metric %s must have been published", name)

	g.m.RLock()
	defer g.m.RUnlock()

	return g.value
}
