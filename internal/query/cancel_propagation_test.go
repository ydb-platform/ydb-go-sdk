package query

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TestStreamResultNextResultSet_CtxErrorDoesNotCancelExecuteStream is a regression test:
// per-call ctx cancellation must not run executeCancel early or poison lastErr.
func TestStreamResultNextResultSet_CtxErrorDoesNotCancelExecuteStream(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      &Ydb.ResultSet{},
		}, nil)

		var onCloseCalls atomic.Uint64

		r, err := newResult(context.Background(), stream, withStreamResultOnClose(func() {
			onCloseCalls.Add(1)
		}))
		require.NoError(t, err)

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = r.nextResultSet(cancelledCtx)
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 0, onCloseCalls.Load(),
			"nextResultSet must not run executeCancel on per-call ctx error",
		)
		require.NoError(t, r.lastErr)
	})
}

func TestClientCloseCancelsInflightDo(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()
		opStarted := make(chan struct{})

		c := &Client{
			config: config.New(),
			explicitSessionPool: &mockSessionPool{
				withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
					return f(ctx, newTestSession("s-1"))
				},
			},
			implicitSessionPool: &mockSessionPool{
				withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
					return f(ctx, newTestSession("s-2"))
				},
			},
			closed: xsync.NewValue(&closeState{
				cancels: make(map[uint64]context.CancelFunc),
			}),
		}

		doDone := make(chan error, 1)
		go func() {
			doDone <- c.Do(ctx, func(ctx context.Context, _ query.Session) error {
				close(opStarted)
				<-ctx.Done()

				return ctx.Err()
			})
		}()

		select {
		case <-opStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("Do user op never started; setup is broken")
		}

		require.NoError(t, c.Close(context.Background()))

		select {
		case err := <-doDone:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Do did not return after Client.Close cancellation")
		}

		err := c.Do(ctx, func(context.Context, query.Session) error {
			return nil
		})
		require.ErrorIs(t, err, errClosedClient)
	})
}

func TestClientCloseCancelsInflightDoTx(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)

		queryService := NewMockQueryServiceClient(ctrl)
		queryService.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.BeginTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
				TxMeta: &Ydb_Query.TransactionMeta{Id: "tx-1"},
			}, nil,
		).AnyTimes()
		queryService.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.RollbackTransactionResponse{Status: Ydb.StatusIds_SUCCESS}, nil,
		).AnyTimes()
		queryService.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.CommitTransactionResponse{Status: Ydb.StatusIds_SUCCESS}, nil,
		).AnyTimes()

		opStarted := make(chan struct{})

		c := &Client{
			config: config.New(),
			client: queryService,
			explicitSessionPool: &mockSessionPool{
				withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
					return f(ctx, newTestSessionWithClient("s-1", queryService, false))
				},
			},
			implicitSessionPool: &mockSessionPool{
				withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
					return f(ctx, newTestSessionWithClient("s-2", queryService, false))
				},
			},
			closed: xsync.NewValue(&closeState{
				cancels: make(map[uint64]context.CancelFunc),
			}),
		}

		doDone := make(chan error, 1)
		go func() {
			doDone <- c.DoTx(ctx, func(ctx context.Context, _ query.TxActor) error {
				close(opStarted)
				<-ctx.Done()

				return ctx.Err()
			})
		}()

		select {
		case <-opStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("DoTx user op never started; setup is broken")
		}

		require.NoError(t, c.Close(context.Background()))

		select {
		case err := <-doDone:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("DoTx did not return after Client.Close cancellation")
		}
	})
}

func TestClientCallUnregistersCloseCancel(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()

		c := &Client{
			config: config.New(),
			explicitSessionPool: &mockSessionPool{
				withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
					return f(ctx, newTestSession("s-1"))
				},
			},
			implicitSessionPool: &mockSessionPool{
				withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
					return f(ctx, newTestSession("s-2"))
				},
			},
			closed: xsync.NewValue(&closeState{
				cancels: make(map[uint64]context.CancelFunc),
			}),
		}

		require.NoError(t, c.Do(ctx, func(context.Context, query.Session) error {
			return nil
		}))

		state := c.closed.Get()
		require.False(t, state.closed)
		require.Empty(t, state.cancels)
	})
}

func TestClientCloseCancelsRegisteredCloseGoroutine(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		deleteStarted := make(chan struct{})

		queryService := NewMockQueryServiceClient(ctrl)
		queryService.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, _ *Ydb_Query.DeleteSessionRequest, _ ...grpc.CallOption) (
				*Ydb_Query.DeleteSessionResponse, error,
			) {
				close(deleteStarted)
				<-ctx.Done()

				return nil, ctx.Err()
			},
		)

		c := &Client{
			config: config.New(),
			explicitSessionPool: &mockSessionPool{
				withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
					return f(ctx, newTestSession("s-1"))
				},
			},
			implicitSessionPool: &mockSessionPool{
				withFunc: func(ctx context.Context, f func(ctx context.Context, s *Session) error) error {
					return f(ctx, newTestSession("s-2"))
				},
			},
			closed: xsync.NewValue(&closeState{
				cancels: make(map[uint64]context.CancelFunc),
			}),
		}
		core := &sessionCore{
			Client:              queryService,
			Trace:               &trace.Query{},
			id:                  "s-1",
			registerCloseCancel: c.registerCloseCancel,
		}

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		require.NoError(t, core.Close(cancelledCtx))

		select {
		case <-deleteStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("registered deleteSession goroutine never started")
		}

		require.NoError(t, c.Close(context.Background()))

		require.Eventually(t, func() bool {
			return len(c.closed.Get().cancels) == 0
		}, 5*time.Second, 10*time.Millisecond)
	})
}
