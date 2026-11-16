package ydb_test

import (
	"context"
	"fmt"
	"maps"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDriverSharedSessionPoolLimitAndReuse(t *testing.T) {
	db, server := newSharedPoolTestDriver(t, 1, 0)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	borrowed := make(chan string, 1)
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			borrowed <- s.ID()
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var tableSessionID string
	select {
	case tableSessionID = <-borrowed:
	case <-ctx.Done():
		t.Fatal("table operation did not acquire a session")
	}
	queryCtx, cancelQuery := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelQuery()
	called := false
	err := db.Query().Do(queryCtx, func(context.Context, query.Session) error {
		called = true

		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, called)
	require.Equal(t, 1, server.createdCount())
	close(release)
	require.NoError(t, <-finished)
	require.NoError(t, db.Query().Do(ctx, func(_ context.Context, s query.Session) error {
		require.Equal(t, tableSessionID, s.ID())

		return nil
	}))
	stats, err := db.SessionPoolStats()
	require.NoError(t, err)
	require.Equal(t, 1, stats.Limit)
	require.Equal(t, 1, stats.Size)
	require.NoError(t, db.Close(ctx))
	require.Equal(t, map[string]int{tableSessionID: 1}, server.deletedCounts())
}

func TestDriverSharedSessionPoolClientClose(t *testing.T) {
	for _, first := range []string{"table", "query"} {
		t.Run(first, func(t *testing.T) {
			db, server := newSharedPoolTestDriver(t, 1, 0)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tableClient, queryClient := db.Table(), db.Query()
			var sessionID string
			require.NoError(t, tableClient.Do(ctx, func(_ context.Context, s table.Session) error {
				sessionID = s.ID()

				return nil
			}))
			called := false
			if first == "table" {
				closeSharedPoolTestClient(ctx, t, tableClient)
				require.Error(t, tableClient.Do(ctx, func(context.Context, table.Session) error {
					called = true

					return nil
				}))
				require.NoError(t, queryClient.Do(ctx, func(_ context.Context, s query.Session) error {
					require.Equal(t, sessionID, s.ID())

					return nil
				}))
			} else {
				closeSharedPoolTestClient(ctx, t, queryClient)
				require.Error(t, queryClient.Do(ctx, func(context.Context, query.Session) error {
					called = true

					return nil
				}))
				require.NoError(t, tableClient.Do(ctx, func(_ context.Context, s table.Session) error {
					require.Equal(t, sessionID, s.ID())

					return nil
				}))
			}
			require.False(t, called)
			require.Empty(t, server.deletedCounts())
			require.Equal(t, 1, server.createdCount())
			require.NoError(t, db.Close(ctx))
			require.Equal(t, map[string]int{sessionID: 1}, server.deletedCounts())
		})
	}
}

func TestDriverSharedSessionPoolWarmsUpOnce(t *testing.T) {
	db, server := newSharedPoolTestDriver(t, 2, 2)
	_ = db.Table()
	require.Equal(t, 2, server.createdCount())
	_ = db.Query()
	require.Equal(t, 2, server.createdCount())
	stats, err := db.SessionPoolStats()
	require.NoError(t, err)
	require.Equal(t, 2, stats.Limit)
	require.Equal(t, 2, stats.Size)
	require.Equal(t, 2, stats.Idle)
	require.NoError(t, db.Close(context.Background()))
	require.Equal(t, map[string]int{"session-1": 1, "session-2": 1}, server.deletedCounts())
}

func TestDriverSharedSessionPoolTraces(t *testing.T) {
	var tableGet, tablePut, tableState, queryGet atomic.Int32
	db, _ := newSharedPoolTestDriver(t, 1, 0,
		ydb.WithTraceTable(trace.Table{
			OnPoolGet: func(trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
				return func(trace.TablePoolGetDoneInfo) { tableGet.Add(1) }
			},
			OnPoolPut: func(trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
				return func(trace.TablePoolPutDoneInfo) { tablePut.Add(1) }
			},
			OnPoolStateChange: func(trace.TablePoolStateChangeInfo) { tableState.Add(1) },
		}),
		ydb.WithTraceQuery(trace.Query{
			OnPoolGet: func(trace.QueryPoolGetStartInfo) func(trace.QueryPoolGetDoneInfo) {
				return func(trace.QueryPoolGetDoneInfo) { queryGet.Add(1) }
			},
		}),
	)
	require.NoError(t, db.Table().Do(t.Context(), func(context.Context, table.Session) error { return nil }))
	require.NoError(t, db.Query().Do(t.Context(), func(context.Context, query.Session) error { return nil }))
	require.EqualValues(t, 2, tableGet.Load())
	require.EqualValues(t, 2, tablePut.Load())
	require.EqualValues(t, 2, queryGet.Load())
	require.Positive(t, tableState.Load())
}

func TestDriverSharedSessionPoolStatsAfterUnusedClose(t *testing.T) {
	db, server := newSharedPoolTestDriver(t, 1, 0)
	require.NoError(t, db.Close(t.Context()))
	stats, err := db.SessionPoolStats()
	require.NoError(t, err)
	require.Equal(t, ydb.SessionPoolStats{}, stats)
	require.Zero(t, server.createdCount())
}

func TestDriverSharedSessionPoolCloseCancelsOperation(t *testing.T) {
	for _, first := range []string{"table", "query"} {
		t.Run(first, func(t *testing.T) {
			db, _ := newSharedPoolTestDriver(t, 1, 0)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tableClient, queryClient := db.Table(), db.Query()
			started := make(chan struct{})
			finished := make(chan error, 1)
			go func() {
				if first == "table" {
					finished <- tableClient.Do(ctx, func(ctx context.Context, _ table.Session) error {
						close(started)
						<-ctx.Done()

						return ctx.Err()
					})
				} else {
					finished <- queryClient.Do(ctx, func(ctx context.Context, _ query.Session) error {
						close(started)
						<-ctx.Done()

						return ctx.Err()
					})
				}
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("operation did not start")
			}
			if first == "table" {
				closeSharedPoolTestClient(ctx, t, tableClient)
			} else {
				closeSharedPoolTestClient(ctx, t, queryClient)
			}
			select {
			case err := <-finished:
				require.ErrorIs(t, err, context.Canceled)
			case <-ctx.Done():
				t.Fatal("client Close did not cancel its operation")
			}
			if first == "table" {
				require.NoError(t, queryClient.Do(ctx, func(context.Context, query.Session) error { return nil }))
			} else {
				require.NoError(t, tableClient.Do(ctx, func(context.Context, table.Session) error { return nil }))
			}
			require.NoError(t, db.Close(ctx))
		})
	}
}

type sharedPoolTestServer struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer

	mu      sync.Mutex
	created int
	deleted map[string]int
}

func (s *sharedPoolTestServer) CreateSession(context.Context, *Ydb_Query.CreateSessionRequest) (
	*Ydb_Query.CreateSessionResponse, error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created++

	return &Ydb_Query.CreateSessionResponse{
		Status: Ydb.StatusIds_SUCCESS, SessionId: fmt.Sprintf("session-%d", s.created),
	}, nil
}

func (s *sharedPoolTestServer) AttachSession(
	_ *Ydb_Query.AttachSessionRequest, stream Ydb_Query_V1.QueryService_AttachSessionServer,
) error {
	if err := stream.Send(&Ydb_Query.SessionState{Status: Ydb.StatusIds_SUCCESS}); err != nil {
		return err
	}
	<-stream.Context().Done()

	return stream.Context().Err()
}

func (s *sharedPoolTestServer) DeleteSession(_ context.Context, req *Ydb_Query.DeleteSessionRequest) (
	*Ydb_Query.DeleteSessionResponse, error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted[req.GetSessionId()]++

	return &Ydb_Query.DeleteSessionResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func (s *sharedPoolTestServer) createdCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.created
}

func (s *sharedPoolTestServer) deletedCounts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return maps.Clone(s.deleted)
}

func newSharedPoolTestDriver(t *testing.T, limit, warmUp int, opts ...ydb.Option) (*ydb.Driver, *sharedPoolTestServer) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	server := &sharedPoolTestServer{deleted: make(map[string]int)}
	Ydb_Query_V1.RegisterQueryServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	t.Cleanup(func() { _ = listener.Close() })
	opts = append([]ydb.Option{
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithSessionPoolSizeLimit(limit),
		ydb.WithSessionPoolWarmUpSessions(warmUp),
		ydb.With(config.WithGrpcOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))),
	}, opts...)
	db, err := ydb.Open(t.Context(), "grpc://bufnet:2136/local", opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })

	return db, server
}

func closeSharedPoolTestClient(ctx context.Context, t *testing.T, client any) {
	t.Helper()
	closer, ok := client.(interface {
		Close(ctx context.Context) error
	})
	require.True(t, ok)
	require.NoError(t, closer.Close(ctx))
}
