package ydb

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
)

func TestSessionPoolWarmUpAtDriverInitialization(t *testing.T) {
	const warmUpSessions = 3

	var (
		tableCreateCalls atomic.Int32
		queryCreateCalls atomic.Int32
	)

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	Ydb_Table_V1.RegisterTableServiceServer(server, &warmUpTableService{
		createCalls: &tableCreateCalls,
	})
	Ydb_Query_V1.RegisterQueryServiceServer(server, &warmUpQueryService{
		createCalls: &queryCreateCalls,
	})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	db, err := Open(t.Context(), "grpc://warm-up-test:2135/local",
		WithBalancer(balancers.SingleConn()),
		With(config.WithGrpcOptions(
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		)),
		WithSessionPoolWarmUpSessions(warmUpSessions),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close(t.Context()))
	})

	require.Equal(t, int32(warmUpSessions), tableCreateCalls.Load())
	require.Equal(t, int32(warmUpSessions), queryCreateCalls.Load())
}

type warmUpTableService struct {
	Ydb_Table_V1.UnimplementedTableServiceServer

	createCalls *atomic.Int32
}

func (s *warmUpTableService) CreateSession(
	_ context.Context, _ *Ydb_Table.CreateSessionRequest,
) (*Ydb_Table.CreateSessionResponse, error) {
	sessionID := s.createCalls.Add(1)
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
	return &Ydb_Table.DeleteSessionResponse{}, nil
}

type warmUpQueryService struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer

	createCalls *atomic.Int32
}

func (s *warmUpQueryService) CreateSession(
	context.Context, *Ydb_Query.CreateSessionRequest,
) (*Ydb_Query.CreateSessionResponse, error) {
	sessionID := s.createCalls.Add(1)

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
	return &Ydb_Query.DeleteSessionResponse{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil
}
