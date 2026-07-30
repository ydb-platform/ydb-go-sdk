package ydb

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	internalScheme "github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestSessionPoolWarmUpAtDriverInitialization(t *testing.T) {
	const warmUpSessions = 3

	var (
		tableCreateCalls atomic.Int32
		queryCreateCalls atomic.Int32
	)

	db, err := openWarmUpDriver(t.Context(), t, warmUpSessions,
		&warmUpTableService{
			createCalls: &tableCreateCalls,
		},
		&warmUpQueryService{
			createCalls: &queryCreateCalls,
		},
		Open,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close(t.Context()))
	})

	require.Equal(t, int32(warmUpSessions), tableCreateCalls.Load())
	require.Equal(t, int32(warmUpSessions), queryCreateCalls.Load())
}

func TestSessionPoolWarmUpRunsClientsInParallel(t *testing.T) {
	const warmUpSessions = 3

	var (
		tableCreateCalls atomic.Int32
		queryCreateCalls atomic.Int32
		tableStarted     = make(chan struct{})
		queryStarted     = make(chan struct{})
		tableStartOnce   sync.Once
		queryStartOnce   sync.Once
	)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	db, err := openWarmUpDriver(ctx, t, warmUpSessions,
		&warmUpTableService{
			createCalls: &tableCreateCalls,
			beforeCreate: func(ctx context.Context) error {
				tableStartOnce.Do(func() {
					close(tableStarted)
				})

				select {
				case <-queryStarted:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
		&warmUpQueryService{
			createCalls: &queryCreateCalls,
			beforeCreate: func(ctx context.Context) error {
				queryStartOnce.Do(func() {
					close(queryStarted)
				})

				select {
				case <-tableStarted:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
		Open,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close(t.Context()))
	})

	require.Equal(t, int32(warmUpSessions), tableCreateCalls.Load())
	require.Equal(t, int32(warmUpSessions), queryCreateCalls.Load())
}

func TestSessionPoolWarmUpCleansUpOnPartialFailure(t *testing.T) {
	const warmUpSessions = 3

	for _, test := range []struct {
		name string
		open warmUpDriverOpener
	}{
		{
			name: "Open",
			open: Open,
		},
		{
			name: "New",
			open: newWarmUpDriver,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				tableCreateCalls   atomic.Int32
				tableDeleteCalls   atomic.Int32
				queryCreateCalls   atomic.Int32
				balancerCloseCalls atomic.Int32
			)

			db, err := openWarmUpDriver(t.Context(), t, warmUpSessions,
				&warmUpTableService{
					createCalls: &tableCreateCalls,
					deleteCalls: &tableDeleteCalls,
				},
				&warmUpQueryService{
					createCalls: &queryCreateCalls,
					beforeCreate: func(context.Context) error {
						return status.Error(codes.InvalidArgument, "query warm-up failed")
					},
				},
				test.open,
				WithTraceDriver(trace.Driver{
					OnBalancerClose: func(
						trace.DriverBalancerCloseStartInfo,
					) func(trace.DriverBalancerCloseDoneInfo) {
						balancerCloseCalls.Add(1)

						return func(trace.DriverBalancerCloseDoneInfo) {}
					},
				}),
			)
			require.Nil(t, db)
			require.ErrorContains(t, err, "warm up session pools: query client")
			require.Equal(t, int32(warmUpSessions), tableCreateCalls.Load())
			require.Equal(t, int32(warmUpSessions), tableDeleteCalls.Load())
			require.Equal(t, int32(warmUpSessions), queryCreateCalls.Load())
			require.Equal(t, int32(1), balancerCloseCalls.Load())
		})
	}
}

func TestSessionPoolWarmUpCleansUpQueryOnTableFailure(t *testing.T) {
	const warmUpSessions = 3

	var (
		tableCreateCalls atomic.Int32
		queryCreateCalls atomic.Int32
		queryDeleteCalls atomic.Int32
	)

	db, err := openWarmUpDriver(t.Context(), t, warmUpSessions,
		&warmUpTableService{
			createCalls: &tableCreateCalls,
			beforeCreate: func(context.Context) error {
				return status.Error(codes.InvalidArgument, "table warm-up failed")
			},
		},
		&warmUpQueryService{
			createCalls: &queryCreateCalls,
			deleteCalls: &queryDeleteCalls,
		},
		Open,
	)
	require.Nil(t, db)
	require.ErrorContains(t, err, "warm up session pools: table client")
	require.Equal(t, int32(warmUpSessions), tableCreateCalls.Load())
	require.Equal(t, int32(warmUpSessions), queryCreateCalls.Load())
	require.Equal(t, int32(warmUpSessions), queryDeleteCalls.Load())
}

func TestSessionPoolWarmUpCleansUpWithFailure(t *testing.T) {
	const warmUpSessions = 3

	var (
		tableCreateCalls   atomic.Int32
		tableDeleteCalls   atomic.Int32
		queryCreateCalls   atomic.Int32
		balancerCloseCalls atomic.Int32
	)

	db, err := openWarmUpDriver(t.Context(), t, 0,
		&warmUpTableService{
			createCalls: &tableCreateCalls,
			deleteCalls: &tableDeleteCalls,
		},
		&warmUpQueryService{
			createCalls: &queryCreateCalls,
			beforeCreate: func(context.Context) error {
				return status.Error(codes.InvalidArgument, "query warm-up failed")
			},
		},
		Open,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close(t.Context()))
	})

	child, err := db.With(t.Context(),
		WithSessionPoolWarmUpSessions(warmUpSessions),
		WithTraceDriver(trace.Driver{
			OnBalancerClose: func(
				trace.DriverBalancerCloseStartInfo,
			) func(trace.DriverBalancerCloseDoneInfo) {
				balancerCloseCalls.Add(1)

				return func(trace.DriverBalancerCloseDoneInfo) {}
			},
		}),
	)
	require.Nil(t, child)
	require.ErrorContains(t, err, "warm up session pools: query client")
	require.Equal(t, int32(warmUpSessions), tableCreateCalls.Load())
	require.Equal(t, int32(warmUpSessions), tableDeleteCalls.Load())
	require.Equal(t, int32(warmUpSessions), queryCreateCalls.Load())
	require.Equal(t, int32(1), balancerCloseCalls.Load())
}

func TestSessionPoolWarmUpReportsBothClientFailures(t *testing.T) {
	const warmUpSessions = 3

	var (
		tableCreateCalls atomic.Int32
		queryCreateCalls atomic.Int32
	)

	db, err := openWarmUpDriver(t.Context(), t, warmUpSessions,
		&warmUpTableService{
			createCalls: &tableCreateCalls,
			beforeCreate: func(context.Context) error {
				return status.Error(codes.InvalidArgument, "table warm-up failed")
			},
		},
		&warmUpQueryService{
			createCalls: &queryCreateCalls,
			beforeCreate: func(context.Context) error {
				return status.Error(codes.InvalidArgument, "query warm-up failed")
			},
		},
		Open,
	)
	require.Nil(t, db)
	require.ErrorContains(t, err, "table client")
	require.ErrorContains(t, err, "table warm-up failed")
	require.ErrorContains(t, err, "query client")
	require.ErrorContains(t, err, "query warm-up failed")
	require.Equal(t, int32(warmUpSessions), tableCreateCalls.Load())
	require.Equal(t, int32(warmUpSessions), queryCreateCalls.Load())
}

func TestCleanupConnectFailureClosesLazyClients(t *testing.T) {
	var schemeInitCalls atomic.Int32
	d := &Driver{
		ctxCancel: func() {},
		scheme: xsync.OnceValue(func() (*internalScheme.Client, error) {
			schemeInitCalls.Add(1)

			return nil, context.Canceled
		}),
	}

	d.cleanupConnectFailure(t.Context())
	_, _ = d.scheme.Get()

	require.Zero(t, schemeInitCalls.Load())
}

type warmUpDriverOpener func(context.Context, string, ...Option) (*Driver, error)

func newWarmUpDriver(ctx context.Context, dsn string, opts ...Option) (*Driver, error) {
	return New(ctx, append([]Option{WithConnectionString(dsn)}, opts...)...)
}

func openWarmUpDriver(
	ctx context.Context,
	t *testing.T,
	warmUpSessions int,
	tableService *warmUpTableService,
	queryService *warmUpQueryService,
	open warmUpDriverOpener,
	extraOptions ...Option,
) (*Driver, error) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	Ydb_Table_V1.RegisterTableServiceServer(server, tableService)
	Ydb_Query_V1.RegisterQueryServiceServer(server, queryService)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	options := []Option{
		WithBalancer(balancers.SingleConn()),
		With(config.WithGrpcOptions(
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		)),
		WithSessionPoolWarmUpSessions(warmUpSessions),
	}
	options = append(options, extraOptions...)

	return open(ctx, "grpc://warm-up-test:2135/local", options...)
}

type warmUpTableService struct {
	Ydb_Table_V1.UnimplementedTableServiceServer

	createCalls  *atomic.Int32
	deleteCalls  *atomic.Int32
	beforeCreate func(context.Context) error
}

func (s *warmUpTableService) CreateSession(
	ctx context.Context, _ *Ydb_Table.CreateSessionRequest,
) (*Ydb_Table.CreateSessionResponse, error) {
	sessionID := s.createCalls.Add(1)
	if s.beforeCreate != nil {
		if err := s.beforeCreate(ctx); err != nil {
			return nil, err
		}
	}

	result, err := anypb.New(&Ydb_Table.CreateSessionResult{
		SessionId: fmt.Sprintf("table-session-%d", sessionID),
	})
	if err != nil {
		return nil, err
	}

	return &Ydb_Table.CreateSessionResponse{
		Operation: &Ydb_Operations.Operation{
			Ready:  true,
			Status: Ydb.StatusIds_SUCCESS,
			Result: result,
		},
	}, nil
}

func (s *warmUpTableService) DeleteSession(
	context.Context, *Ydb_Table.DeleteSessionRequest,
) (*Ydb_Table.DeleteSessionResponse, error) {
	if s.deleteCalls != nil {
		s.deleteCalls.Add(1)
	}

	return &Ydb_Table.DeleteSessionResponse{}, nil
}

type warmUpQueryService struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer

	createCalls  *atomic.Int32
	deleteCalls  *atomic.Int32
	beforeCreate func(context.Context) error
}

func (s *warmUpQueryService) CreateSession(
	ctx context.Context, _ *Ydb_Query.CreateSessionRequest,
) (*Ydb_Query.CreateSessionResponse, error) {
	sessionID := s.createCalls.Add(1)
	if s.beforeCreate != nil {
		if err := s.beforeCreate(ctx); err != nil {
			return nil, err
		}
	}

	return &Ydb_Query.CreateSessionResponse{
		Status:    Ydb.StatusIds_SUCCESS,
		SessionId: fmt.Sprintf("query-session-%d", sessionID),
	}, nil
}

func (s *warmUpQueryService) AttachSession(
	_ *Ydb_Query.AttachSessionRequest, stream Ydb_Query_V1.QueryService_AttachSessionServer,
) error {
	if err := stream.Send(&Ydb_Query.SessionState{
		Status: Ydb.StatusIds_SUCCESS,
	}); err != nil {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (s *warmUpQueryService) DeleteSession(
	context.Context, *Ydb_Query.DeleteSessionRequest,
) (*Ydb_Query.DeleteSessionResponse, error) {
	if s.deleteCalls != nil {
		s.deleteCalls.Add(1)
	}

	return &Ydb_Query.DeleteSessionResponse{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil
}
