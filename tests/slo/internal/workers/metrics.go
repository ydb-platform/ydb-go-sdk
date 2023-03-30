package workers

import (
	"log"
	"slo/internal/metrics"

	"github.com/beefsack/go-rate"
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
