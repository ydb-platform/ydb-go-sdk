package query

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

func TestBorrowedSessionSettings(t *testing.T) {
	physical := newTestSession("shared")
	physical.streamResultCloseTimeout = time.Second
	p := testPool(t, func(context.Context) (*Session, error) { return physical, nil })
	shared := &SessionPool{Pool: p, closed: xsync.NewValue(&closeState{cancels: make(map[uint64]context.CancelFunc)})}
	t.Cleanup(func() { require.NoError(t, shared.Close(t.Context())) })
	var decoded atomic.Int32
	decoder := func(context.Context, []arrow.Column, io.Reader) ([]arrow.Batch, error) {
		decoded.Add(1)

		return nil, nil
	}
	a := &sessionPoolAdapter{shared: shared, cfg: config.New(
		config.WithLazyTx(true), config.WithDefaultResultFormatArrow(decoder),
		config.WithSessionDeleteTimeout(2*time.Second),
	)}
	require.NoError(t, a.With(t.Context(), func(_ context.Context, s *Session) error {
		require.Same(t, physical.Core, s.Core)
		require.NotSame(t, physical, s)
		require.True(t, s.lazyTx)
		require.Equal(t, 2*time.Second, s.streamResultCloseTimeout)
		_, err := s.defaultArrowDecoder(t.Context(), nil, nil)

		return err
	}))
	require.EqualValues(t, 1, decoded.Load())
	require.False(t, physical.lazyTx)
	require.Nil(t, physical.defaultArrowDecoder)
}

func TestSharedPoolCloseRegistry(t *testing.T) {
	p := testPool(t, func(context.Context) (*Session, error) { return newTestSession("shared"), nil })
	shared := &SessionPool{Pool: p, closed: xsync.NewValue(&closeState{cancels: make(map[uint64]context.CancelFunc)})}
	var cancelled atomic.Int32
	unregister := registerCloseCancel(shared.closed, func() { cancelled.Add(1) })
	a := &sessionPoolAdapter{shared: shared, cfg: config.New()}
	require.NoError(t, a.Close(t.Context()))
	require.EqualValues(t, 0, cancelled.Load())
	require.NoError(t, shared.Close(t.Context()))
	require.NoError(t, shared.Close(t.Context()))
	require.EqualValues(t, 1, cancelled.Load())
	unregister()
	registerCloseCancel(shared.closed, func() { cancelled.Add(1) })()
	require.EqualValues(t, 2, cancelled.Load())
}

func TestBorrowedClientCloseLeavesOwnerAlive(t *testing.T) {
	p := testPool(t, func(context.Context) (*Session, error) { return newTestSession("shared"), nil })
	shared := &SessionPool{Pool: p, closed: xsync.NewValue(&closeState{cancels: make(map[uint64]context.CancelFunc)})}
	t.Cleanup(func() { require.NoError(t, shared.Close(t.Context())) })
	client, err := newWithQueryServiceClient(t.Context(), NewMockQueryServiceClient(gomock.NewController(t)),
		nil, config.New(), WithSessionPool(shared),
	)
	require.NoError(t, err)
	require.NoError(t, client.Close(t.Context()))
	require.NoError(t, shared.With(t.Context(), func(context.Context, *Session) error { return nil }))
	_, _, err = client.enter(t.Context())
	require.ErrorIs(t, err, errClosedClient)
}

func TestNilSharedPoolClose(t *testing.T) {
	var p *SessionPool
	require.NoError(t, p.Close(t.Context()))
}
