package sugar

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
)

type consumedUnitsCounter struct {
	consumedUnits uint64
}

func ConsumedUnitsCounter(ctx context.Context) *consumedUnitsCounter {
	return &consumedUnitsCounter{}
}

func (c *consumedUnitsCounter) Observe(ctx context.Context) context.Context {
	return meta.WithIncomingMetadataCallback(ctx, func(header string, values []string) {
		if header != meta.HeaderConsumedUnits {
			return
		}
		for _, v := range values {
			consumedUnits, err := strconv.ParseUint(v, 10, 64)
			if err == nil {
				atomic.AddUint64(&c.consumedUnits, consumedUnits)
			}
		}
	})
}

func (c *consumedUnitsCounter) Sum() uint64 {
	return atomic.LoadUint64(&c.consumedUnits)
}

func (c *consumedUnitsCounter) Clear() {
	atomic.StoreUint64(&c.consumedUnits, 0)
}
