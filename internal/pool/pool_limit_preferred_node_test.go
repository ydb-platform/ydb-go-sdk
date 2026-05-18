package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

// PreferredNodeID create path evicts one idle item when Concurrency==limit or Size>=limit
// before createItem (see pool.go getItem).
func TestPreferredNodeIDConcurrencyLimitEviction(t *testing.T) {
	const (
		limit    = 2
		nodeBusy = uint32(32)
		nodeIdle = uint32(33)
	)

	newCountedPool := func(t *testing.T) (*Pool[*testItem, testItem], *atomic.Int32) {
		t.Helper()

		var created atomic.Int32

		p := mustNewPool[*testItem, testItem](t,
			WithLimit[*testItem, testItem](limit),
			WithCreateItemFunc(func(ctx context.Context) (*testItem, error) {
				id := created.Add(1)
				nodeID, has := endpoint.ContextNodeID(ctx)
				if !has {
					return nil, errNilItem
				}

				return &testItem{
					v: id,
					onNodeID: func() uint32 {
						return nodeID
					},
				}, nil
			}),
		)

		return p, &created
	}

	seedTwoNodes := func(t *testing.T, p *Pool[*testItem, testItem], created *atomic.Int32) {
		t.Helper()

		ctx := t.Context()
		for _, node := range []uint32{nodeBusy, nodeIdle} {
			info, err := getItemWithFlush(p, endpoint.WithNodeID(ctx, node))
			require.NoError(t, err)
			require.Equal(t, node, info.item.NodeID())
			mustPutItem(t, p, info)
		}
		require.Equal(t, int32(limit), created.Load())
		requirePoolStats(t, p, poolStats(limit, func(s *Stats) {
			s.Size = limit
			s.Idle = limit
		}))
	}

	t.Run("ReviewerScenarioConcurrentWithRespectsLimit", func(t *testing.T) {
		p, created := newCountedPool(t)
		defer mustClose(t, p)

		seedTwoNodes(t, p, created)

		ctx := t.Context()
		releaseFirst := make(chan struct{})
		firstHolding := make(chan struct{})

		var firstWG sync.WaitGroup
		firstWG.Add(1)
		go func() {
			defer firstWG.Done()
			_ = p.With(endpoint.WithNodeID(ctx, nodeBusy), func(_ context.Context, item *testItem) error {
				require.Equal(t, nodeBusy, item.NodeID())
				close(firstHolding)
				<-releaseFirst

				return nil
			})
		}()
		<-firstHolding

		var (
			secondErr  error
			secondNode uint32
		)
		secondDone := make(chan struct{})
		go func() {
			secondErr = p.With(endpoint.WithNodeID(ctx, nodeBusy), func(_ context.Context, item *testItem) error {
				secondNode = item.NodeID()

				return nil
			})
			close(secondDone)
		}()

		select {
		case <-secondDone:
		case <-time.After(5 * time.Second):
			t.Fatal("second With did not complete")
		}

		close(releaseFirst)
		firstWG.Wait()

		require.NoError(t, secondErr)
		require.Equal(t, nodeBusy, secondNode)
		require.Equal(t, int32(3), created.Load())
		require.LessOrEqual(t, p.Stats().Size, limit)
	})

	t.Run("SingleWithFullPoolPreferredKeepsSizeAtLimit", func(t *testing.T) {
		const preferred = uint32(99)

		p, created := newCountedPool(t)
		defer mustClose(t, p)

		ctx := t.Context()
		for _, node := range []uint32{0, 1} {
			info, err := getItemWithFlush(p, endpoint.WithNodeID(ctx, node))
			require.NoError(t, err)
			mustPutItem(t, p, info)
		}
		require.Equal(t, int32(2), created.Load())
		requirePoolStats(t, p, poolStats(limit, func(s *Stats) {
			s.Size = limit
			s.Idle = limit
		}))

		err := p.With(endpoint.WithNodeID(ctx, preferred), func(_ context.Context, item *testItem) error {
			require.Equal(t, preferred, item.NodeID())

			return nil
		})
		require.NoError(t, err)

		require.Equal(t, int32(3), created.Load())
		require.LessOrEqual(t, p.Stats().Size, limit)
	})

	t.Run("ConcurrentWithAllItemsBusyDoesNotCreateBeyondLimit", func(t *testing.T) {
		p, created := newCountedPool(t)
		defer mustClose(t, p)

		seedTwoNodes(t, p, created)

		ctx := t.Context()
		wait := make(chan struct{})
		holding := make(chan struct{}, limit)

		var holdWG sync.WaitGroup
		holdWG.Add(limit)
		for range limit {
			go func() {
				defer holdWG.Done()
				_ = p.With(t.Context(), func(context.Context, *testItem) error {
					holding <- struct{}{}
					<-wait

					return nil
				})
			}()
		}
		for range limit {
			<-holding
		}

		getCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()

		var thirdGotItem bool
		err := p.With(endpoint.WithNodeID(getCtx, nodeBusy), func(context.Context, *testItem) error {
			thirdGotItem = true

			return nil
		})
		require.Error(t, err)
		require.False(t, thirdGotItem)
		require.Equal(t, int32(limit), created.Load())
		require.LessOrEqual(t, p.Stats().Size, limit)

		close(wait)
		holdWG.Wait()
	})
}
