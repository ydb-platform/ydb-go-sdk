package pool

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPoolPutItemRejectsWhenIdleAtLimit(t *testing.T) {
	const limit = 2

	var closed atomic.Int32

	p := mustNewPool[*testItem, testItem](t,
		WithLimit[*testItem, testItem](limit),
		WithCreateItemFunc(func(context.Context) (*testItem, error) {
			return &testItem{
				onClose: func() error {
					closed.Add(1)

					return nil
				},
			}, nil
		}),
	)
	defer func() {
		_ = p.Close(t.Context())
	}()

	ctx := t.Context()

	infos := make([]*itemInfo[*testItem, testItem], limit)
	for i := range limit {
		infos[i] = mustGetItem(t, p)
	}
	for _, info := range infos {
		mustPutItem(t, p, info)
	}
	require.Equal(t, limit, p.idle.Len())
	requirePoolStats(t, p, poolStats(limit, func(s *Stats) {
		s.Size = limit
		s.Idle = limit
	}))

	var createBatch dynamicStats
	extraItem, err := p.createItem(ctx, &createBatch)
	require.NoError(t, err)
	p.applyBatchStats(&createBatch)

	requirePoolStats(t, p, poolStats(limit, func(s *Stats) {
		s.Size = limit + 1
		s.Idle = limit
	}))

	extraInfo := &itemInfo[*testItem, testItem]{
		item:      extraItem,
		created:   p.config.clock.Now(),
		lastUsage: p.config.clock.Now(),
	}

	var putBatch dynamicStats
	err = p.putItem(ctx, extraInfo, &putBatch)
	p.applyBatchStats(&putBatch)

	require.ErrorIs(t, err, errPoolIsOverflow)
	require.Equal(t, int32(1), closed.Load())
	require.True(t, extraItem.closed)
	require.Equal(t, limit, p.idle.Len())
	requirePoolStats(t, p, poolStats(limit, func(s *Stats) {
		s.Size = limit
		s.Idle = limit
	}))
}
