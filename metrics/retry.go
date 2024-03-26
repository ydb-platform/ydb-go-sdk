package metrics

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func retry(config Config) (t trace.Retry) {
	config = config.WithSystem("retry")
	errs := config.CounterVec("errors", "status", "retry_label", "final")
	attempts := config.HistogramVec("attempts", []float64{0, 1, 2, 3, 4, 5, 7, 10}, "retry_label")
	latency := config.TimerVec("latency", "retry_label")
	t.OnRetry = func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
		label := info.Label
		if label == "" {
			return nil
		}
		start := time.Now()

		return func(info trace.RetryLoopDoneInfo) {
			if config.Details()&trace.RetryEvents != 0 {
				attempts.With(map[string]string{
					"retry_label": label,
				}).Record(float64(info.Attempts))
				errs.With(map[string]string{
					"status":      errorBrief(info.Error),
					"retry_label": label,
					"final":       "true",
				}).Inc()
				latency.With(map[string]string{
					"retry_label": label,
				}).Record(time.Since(start))
			}
		}
	}

	return t
}
