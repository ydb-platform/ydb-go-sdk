package workers

import (
	"log"

	"github.com/beefsack/go-rate"

	"slo/internal/metrics"
)

func Metrics(rl *rate.RateLimiter, m *metrics.Metrics) {
	for {
		rl.Wait()
		err := m.Push()
		if err != nil {
			log.Printf("error while pushing: %v", err)
		}
	}
}
