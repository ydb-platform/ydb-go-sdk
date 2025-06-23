package integration

import (
	"context"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestYDBConnMetrics(t *testing.T) {
	var (
		scope    = newScope(t)
		registry = &registryConfig{
			details:    trace.DriverConnEvents,
			gauges:     newVec[gaugeVec](),
			counters:   newVec[counterVec](),
			timers:     newVec[timerVec](),
			histograms: newVec[histogramVec](),
		}
	)

	db, err := ydb.Open(scope.Ctx, "grpc://localhost:2136/local", metrics.WithTraces(registry))
	scope.Require.NoError(err)
	defer db.Close(scope.Ctx) // cleanup resources

	db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		return s.Exec(ctx, "SELECT 42")
	})

	metric := "ydb.driver.conns"
	labelsNames := []string{"endpoint", "node_id"}
	labels := map[string]string{"endpoint": "localhost:2136", "node_id": "1"}

	gauges := registry.gauges.data[nameLabelNamesToString(metric, labelsNames)]
	scope.Require.NotNil(gauges)

	labeledGauges := gauges.gauges[nameLabelValuesToString(metric, labels)]
	scope.Require.NotNil(labeledGauges)

	scope.Require.EqualValues(1, labeledGauges.value)
}
