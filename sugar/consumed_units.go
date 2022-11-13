package sugar

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"strconv"
)

type consumedUnitsCounter struct {
	consumedUnits float64
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
			consumedUnits, err := strconv.ParseFloat(v, 64)
			if err != nil {
				panic(err)
			}
			atomic.Ad
			c.consumedUnits += consumedUnits
		}
	})
}

func (c *consumedUnitsCounter) Sum() float64 {
	return c.consumedUnits
}
