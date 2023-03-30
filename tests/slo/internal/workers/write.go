package workers

import (
	"context"
	"log"
	"sync"

	"slo/internal/generator"
	"slo/internal/metrics"

	"github.com/beefsack/go-rate"
)

func Write(st Storager, rl *rate.RateLimiter, m *metrics.Metrics,
	gen generator.Generator, en generator.Entries, ids *[]generator.EntryID, mu *sync.RWMutex, endChan chan struct{},
) {
	for {
		select {
		case <-endChan:
			return
		default:
		}

		entry, err := gen.Generate()
		if err != nil {
			log.Print(err)
			continue
		}

		rl.Wait()

		metricID := m.StartJob(metrics.JobWrite)

		err = st.Write(context.Background(), entry)
		if err != nil {
			log.Printf("error while upsert entry: %v", err)
			m.StopJob(metricID, false)
			continue
		}

		mu.Lock()
		en[entry.ID] = entry
		*ids = append(*ids, entry.ID)
		mu.Unlock()

		m.StopJob(metricID, true)
	}
}
