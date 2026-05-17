package query

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

// TestExplicitSessionPoolSpoiledIdleSession checks that sessions invalidated by a broken
// attach stream (see session_core.listenAttachStream) are removed from the idle container
// on the next pool.With and replaced with a newly created session.
func TestExplicitSessionPoolSpoiledIdleSession(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)

		var (
			sessionSeq     atomic.Int32
			createSessions atomic.Int32
			deleteSessions atomic.Int32
		)

		breakAttach := make(chan struct{})
		attachBroken := make(chan struct{})
		var attachBrokenOnce sync.Once

		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, *Ydb_Query.CreateSessionRequest, ...grpc.CallOption) (*Ydb_Query.CreateSessionResponse, error) {
				createSessions.Add(1)
				id := sessionSeq.Add(1)

				return &Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: fmt.Sprintf("sess-%d", id),
				}, nil
			}).AnyTimes()
		var attachSessions atomic.Int32
		client.EXPECT().AttachSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ *Ydb_Query.AttachSessionRequest, _ ...grpc.CallOption) (
				Ydb_Query_V1.QueryService_AttachSessionClient, error,
			) {
				attachStream := NewMockQueryService_AttachSessionClient(ctrl)
				var firstRecv atomic.Bool
				breakableAttach := attachSessions.Add(1) == 1
				attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
					if !firstRecv.Swap(true) {
						return &Ydb_Query.SessionState{
							Status: Ydb.StatusIds_SUCCESS,
						}, nil
					}

					if !breakableAttach {
						return &Ydb_Query.SessionState{
							Status: Ydb.StatusIds_SUCCESS,
						}, nil
					}

					<-breakAttach
					attachBrokenOnce.Do(func() {
						close(attachBroken)
					})

					return nil, grpcStatus.Error(grpcCodes.Unavailable, "attach stream broken")
				}).AnyTimes()

				return attachStream, nil
			}).AnyTimes()
		client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, *Ydb_Query.DeleteSessionRequest, ...grpc.CallOption) (*Ydb_Query.DeleteSessionResponse, error) {
				deleteSessions.Add(1)

				return &Ydb_Query.DeleteSessionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil
			}).AnyTimes()

		p := testExplicitSessionPool(ctx, client)

		var firstSessionID string
		err := do(ctx, p, func(ctx context.Context, s *Session) error {
			firstSessionID = s.ID()
			require.True(t, s.IsAlive())

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, int32(1), createSessions.Load())
		require.Equal(t, int32(0), deleteSessions.Load())
		require.Equal(t, 1, p.Stats().Idle)

		close(breakAttach)

		select {
		case <-attachBroken:
		case <-time.After(time.Second):
			t.Fatal("attach stream did not break in time")
		}

		err = do(ctx, p, func(ctx context.Context, s *Session) error {
			require.NotEqual(t, firstSessionID, s.ID())
			require.True(t, s.IsAlive())

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, int32(2), createSessions.Load())
		require.Equal(t, int32(1), deleteSessions.Load(),
			"spoiled idle session must be closed when taken from the pool",
		)
	})
}

func testExplicitSessionPool(
	ctx context.Context,
	client Ydb_Query_V1.QueryServiceClient,
) *pool.Pool[*Session, Session] {
	cfg := config.New(
		config.WithPoolLimit(2),
		config.WithSessionCreateTimeout(time.Second),
		config.WithSessionDeleteTimeout(time.Second),
	)

	return pool.New[*Session, Session](ctx,
		pool.WithLimit[*Session](cfg.PoolLimit()),
		pool.WithCreateItemTimeout[*Session](cfg.SessionCreateTimeout()),
		pool.WithCloseItemTimeout[*Session](cfg.SessionDeleteTimeout()),
		pool.WithMustDeleteItemFunc(func(s *Session, err error) bool {
			if !s.IsAlive() {
				return true
			}

			return err != nil && xerrors.MustDeleteTableOrQuerySession(err)
		}),
		pool.WithCreateItemFunc(func(ctx context.Context) (*Session, error) {
			s, err := createSession(ctx, client, WithTrace(cfg.Trace()))
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			s.lazyTx = cfg.LazyTx()

			return s, nil
		}),
	)
}
