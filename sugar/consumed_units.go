package sugar

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
)

type ConsumedUnitsCounter uint64

func (c ConsumedUnitsCounter) Observe(ctx context.Context) context.Context {
	return meta.WithIncomingMetadataCallback(ctx, func(header string, values []string) {
		if header != meta.HeaderConsumedUnits {
			return
		}
		for _, v := range values {
			consumedUnits, err := strconv.ParseUint(v, 10, 64)
			if err == nil {
				atomic.AddUint64((*uint64)(&c), consumedUnits)
			}
		}
	})
}

func (c ConsumedUnitsCounter) Get() uint64 {
	return atomic.LoadUint64((*uint64)(&c))
}

func (c ConsumedUnitsCounter) Reset() {
	atomic.StoreUint64((*uint64)(&c), 0)
}
