package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

func TestSharedQueryAdapterInvalidCallbackSession(t *testing.T) {
	var created atomic.Uint64
	p := testPool(t, func(context.Context) (*Session, error) {
		return &Session{Core: &invalidCallbackCore{id: fmt.Sprintf("session-%d", created.Add(1))}}, nil
	})
	t.Cleanup(func() { require.NoError(t, p.Close(t.Context())) })
	a := &sessionPoolAdapter{shared: &SessionPool{Pool: p}, cfg: config.New()}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	cause := errors.New("invalid query callback session")
	attempts := 0
	var firstError error
	var first *Session
	require.NoError(t, a.With(ctx, func(_ context.Context, s *Session) error {
		attempts++
		if attempts == 1 {
			first = s

			return retry.RetryableError(cause, xerrors.Invalid(s))
		}
		require.NotSame(t, first.Core, s.Core)
		require.NotEqual(t, first.ID(), s.ID())

		return nil
	}, retry.WithTrace(&trace.Retry{
		OnRetryAttempt: func(info trace.RetryAttemptStartInfo) func(trace.RetryAttemptDoneInfo) {
			return func(done trace.RetryAttemptDoneInfo) {
				if info.Attempt == 1 {
					firstError = done.Error
				}
			}
		},
	})))
	require.ErrorIs(t, firstError, cause)
	require.NotNil(t, xerrors.RetryableError(firstError))
	require.Equal(t, 2, attempts)
	require.EqualValues(t, 2, created.Load())
	require.False(t, first.IsAlive())
}

func TestSharedQueryAdapterCallbackError(t *testing.T) {
	var created atomic.Uint64
	p := testPool(t, func(context.Context) (*Session, error) {
		return &Session{Core: &invalidCallbackCore{id: fmt.Sprintf("session-%d", created.Add(1))}}, nil
	})
	t.Cleanup(func() { require.NoError(t, p.Close(t.Context())) })
	a := &sessionPoolAdapter{shared: &SessionPool{Pool: p}, cfg: config.New()}
	cause := errors.New("query callback error")
	attempts := 0
	err := a.With(t.Context(), func(context.Context, *Session) error {
		attempts++

		return cause
	})
	require.ErrorIs(t, err, cause)
	require.Nil(t, xerrors.RetryableError(err))
	require.Equal(t, 1, attempts)
	require.EqualValues(t, 1, created.Load())
}

type invalidCallbackCore struct {
	id     string
	status atomic.Uint32
}

func (c *invalidCallbackCore) ID() string         { return c.id }
func (c *invalidCallbackCore) NodeID() uint32     { return 1 }
func (c *invalidCallbackCore) Status() string     { return Status(c.status.Load()).String() }
func (c *invalidCallbackCore) IsAlive() bool      { return IsAlive(Status(c.status.Load())) }
func (c *invalidCallbackCore) SetStatus(s Status) { c.status.Store(uint32(s)) }
func (c *invalidCallbackCore) Close(context.Context) error {
	c.SetStatus(StatusClosed)

	return nil
}
