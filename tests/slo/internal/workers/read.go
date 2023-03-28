package workers

import (
	"context"
	"log"
	"math/rand"
	"reflect"
	"sync"

	"github.com/beefsack/go-rate"

	"slo/internal/generator"
	"slo/internal/metrics"
)

func Read(st Storager, rl *rate.RateLimiter, m *metrics.Metrics,
	en generator.Entries, idsPtr *[]generator.EntryID, mu *sync.RWMutex, endChan chan struct{}) {
	for {
		select {
		case <-endChan:
			return
		default:
		}

		ids := *idsPtr

		if len(ids) == 0 {
			continue
		}

		i := rand.Intn(len(ids))
		id := ids[i]

		rl.Wait()

		metricID := m.StartJob(metrics.JobRead)

		e, err := st.Read(context.Background(), id)
		if err != nil {
			log.Printf("get entry error: %v", err)
			m.StopJob(metricID, false)
			continue
		}

		mu.RLock()
		original := en[id]
		mu.RUnlock()

		if reflect.DeepEqual(e, original) {
			m.StopJob(metricID, true)
			continue
		}
		log.Print("payload does not match")
		m.StopJob(metricID, false)
	}
}
