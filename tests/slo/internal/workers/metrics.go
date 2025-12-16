package workers

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// Metrics is deprecated and does nothing.
// Metrics export is now handled automatically by OpenTelemetry PeriodicReader.
// This method is kept for backward compatibility with existing workload code.
func (w *Workers) Metrics(ctx context.Context, wg *sync.WaitGroup, rl *rate.Limiter) {
	defer wg.Done()
	// No-op: PeriodicReader in metrics.New() handles automatic export
	<-ctx.Done()
}
