package conn

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func testPoolConnUseCount(p *Pool, key endpoint.Key) (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	value, ok := p.conns[key]
	if !ok || value == nil {
		return 0, false
	}

	return value.useCount.Load(), true
}

func TestPool_ReviewConcerns(t *testing.T) {
	t.Run("GetAndPutAreAtomicUnderMutex", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("atomic:2135")
		const workers = 16
		const iterations = 128

		var wg sync.WaitGroup
		wg.Add(workers)

		for range workers {
			go func() {
				defer wg.Done()
				for range iterations {
					c := pool.Get(e)
					require.NotNil(t, c)
					require.False(t, c.(*conn).isClosed())
					pool.Put(ctx, c)
				}
			}()
		}

		wg.Wait()

		useCount, hasConn := testPoolConnUseCount(pool, e.Key())
		require.False(t, hasConn)
		require.Equal(t, int64(0), useCount)
	})

	t.Run("GetReturnsNilWhenPoolClosed", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})

		e := endpoint.New("closed-pool:2135")
		require.NotNil(t, pool.Get(e))

		require.NoError(t, pool.Release(ctx))
		require.True(t, pool.isClosed())
		require.Nil(t, pool.Get(e))
	})

	t.Run("PutExtraCallsDoNotUnderflowUseCount", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("underflow:2135")
		c := pool.Get(e)

		pool.Put(ctx, c)
		require.False(t, testPoolHasConn(pool, e.Key()))

		pool.Put(ctx, c)
		pool.Put(ctx, c)
		require.False(t, testPoolHasConn(pool, e.Key()))
	})

	t.Run("GetAfterPutCloseReturnsFreshConn", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("fresh-after-close:2135")
		first := pool.Get(e)
		pool.Put(ctx, first)
		require.False(t, testPoolHasConn(pool, e.Key()))

		second := pool.Get(e)
		require.NotNil(t, second)
		require.NotSame(t, first, second)
		require.False(t, second.(*conn).isClosed())
	})

	t.Run("GetProceedsWhilePutClosesOutsidePoolMutex", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("slow-close:2135")
		closeStarted := make(chan struct{})
		closeContinue := make(chan struct{})

		cc := newConn(e, pool,
			withOnClose(func(c *conn) {
				close(closeStarted)
				<-closeContinue
			}),
		)

		pool.mu.Lock()
		value := &connValue{cc: cc}
		value.useCount.Store(1)
		pool.conns[e.Key()] = value
		pool.mu.Unlock()

		putDone := make(chan struct{})
		go func() {
			defer close(putDone)
			pool.Put(ctx, cc)
		}()

		select {
		case <-closeStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("Put did not reach conn.Close")
		}

		got := pool.Get(e)
		require.NotNil(t, got)
		require.NotSame(t, cc, got)
		require.False(t, got.(*conn).isClosed())

		close(closeContinue)

		select {
		case <-putDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Put did not finish")
		}
	})

	t.Run("ReleaseDoesNotBlockIsClosed", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})

		for i := range 8 {
			_ = pool.Get(endpoint.New("release-block:2135", endpoint.WithID(uint32(i+1))))
		}

		releaseDone := make(chan struct{})
		go func() {
			defer close(releaseDone)
			_ = pool.Release(ctx)
		}()

		deadline := time.After(2 * time.Second)
		for {
			if pool.isClosed() {
				break
			}
			select {
			case <-deadline:
				t.Fatal("isClosed stayed blocked while Release closes connections")
			case <-time.After(10 * time.Millisecond):
			}
		}

		select {
		case <-releaseDone:
		case <-time.After(5 * time.Second):
			t.Fatal("Release did not finish")
		}
	})

	t.Run("GetIncrementsUseCountButEndpointsToConnectionsDoesNot", func(t *testing.T) {
		ctx := context.Background()
		pool := NewPool(ctx, &mockConfig{})
		defer func() {
			_ = pool.Release(ctx)
		}()

		e := endpoint.New("refcount-api:2135")

		_ = endpointsToConnections(pool, []endpoint.Endpoint{e})
		useCount, ok := testPoolConnUseCount(pool, e.Key())
		require.True(t, ok)
		require.Equal(t, int64(0), useCount)

		c := pool.Get(e)
		require.NotNil(t, c)

		useCount, ok = testPoolConnUseCount(pool, e.Key())
		require.True(t, ok)
		require.Equal(t, int64(1), useCount)
	})
}

func TestPool_ConcurrentGetPutDoesNotResurrectClosedConn(t *testing.T) {
	ctx := context.Background()
	pool := NewPool(ctx, &mockConfig{})
	defer func() {
		_ = pool.Release(ctx)
	}()

	e := endpoint.New("resurrect:2135")
	var closedConns atomic.Int64

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c := pool.Get(e)
				if c == nil {
					continue
				}
				if c.(*conn).isClosed() {
					closedConns.Add(1)
				}
				pool.Put(ctx, c)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c := pool.Get(e)
				if c == nil {
					continue
				}
				if c.(*conn).isClosed() {
					closedConns.Add(1)
				}
				pool.Put(ctx, c)
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	require.Equal(t, int64(0), closedConns.Load())
}
