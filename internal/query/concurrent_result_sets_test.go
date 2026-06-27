package query

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestClientConcurrentResultSets(t *testing.T) {
	ctx := t.Context()

	t.Run("ClientQuery", func(t *testing.T) {
		client := newMockClientCheckingConcurrentResultSets(t, implicitSessionConfig(), true)

		r, err := client.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		require.NoError(t, r.Close(ctx))
	})

	t.Run("ClientQueryResultSet", func(t *testing.T) {
		client := newMockClientCheckingConcurrentResultSets(t, implicitSessionConfig(), false)

		rs, err := client.QueryResultSet(ctx, "SELECT 1")
		require.NoError(t, err)
		require.NoError(t, rs.Close(ctx))
	})

	t.Run("ClientQueryRow", func(t *testing.T) {
		client := newMockClientCheckingConcurrentResultSets(t, implicitSessionConfig(), false)

		_, err := client.QueryRow(ctx, "SELECT 1")
		require.NoError(t, err)
	})

	t.Run("DoSessionQuery", func(t *testing.T) {
		client := newMockClientCheckingConcurrentResultSets(t, explicitSessionConfig(), false)

		err := client.Do(ctx, func(ctx context.Context, s query.Session) error {
			r, err := s.Query(ctx, "SELECT 1")
			if err != nil {
				return err
			}

			return r.Close(ctx)
		})
		require.NoError(t, err)
	})

	t.Run("WithConcurrentResultSetsNoop", func(t *testing.T) {
		require.NotNil(t, query.WithConcurrentResultSets(true))  //nolint:staticcheck
		require.NotNil(t, query.WithConcurrentResultSets(false)) //nolint:staticcheck
	})

	t.Run("ClientQueryMaterializesInterleavedResultSets", func(t *testing.T) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)

		queryService := NewMockQueryServiceClient(ctrl)
		queryService.EXPECT().
			ExecuteQuery(gomock.Any(), gomock.Any()).
			DoAndReturn(func(
				_ context.Context,
				req *Ydb_Query.ExecuteQueryRequest,
				_ ...grpc.CallOption,
			) (Ydb_Query_V1.QueryService_ExecuteQueryClient, error) {
				require.True(t, req.GetConcurrentResultSets())

				return interleavedMultiResultSetStream(ctrl), nil
			})

		client, err := newWithQueryServiceClient(ctx, queryService, nil, implicitSessionConfig())
		require.NoError(t, err)

		r, err := client.Query(ctx, "SELECT 1; SELECT 2")
		require.NoError(t, err)
		defer func() { _ = r.Close(ctx) }()

		rs0, err := r.NextResultSet(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, rs0.Index())

		for _, want := range []int64{10, 11} {
			row, err := rs0.NextRow(ctx)
			require.NoError(t, err)

			var got int64
			require.NoError(t, row.Scan(&got))
			require.Equal(t, want, got)
		}
		_, err = rs0.NextRow(ctx)
		require.ErrorIs(t, err, io.EOF)

		rs1, err := r.NextResultSet(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, rs1.Index())

		for _, want := range []int64{20, 21} {
			row, err := rs1.NextRow(ctx)
			require.NoError(t, err)

			var got int64
			require.NoError(t, row.Scan(&got))
			require.Equal(t, want, got)
		}
		_, err = rs1.NextRow(ctx)
		require.ErrorIs(t, err, io.EOF)

		_, err = r.NextResultSet(ctx)
		require.ErrorIs(t, err, io.EOF)
	})
}

func implicitSessionConfig() *config.Config {
	return config.New(config.AllowImplicitSessions())
}

func explicitSessionConfig() *config.Config {
	return config.New()
}

func newMockClientCheckingConcurrentResultSets(
	t *testing.T,
	cfg *config.Config,
	wantConcurrentResultSets bool,
) *Client {
	t.Helper()

	ctrl := gomock.NewController(t)
	queryService := NewMockQueryServiceClient(ctrl)

	if cfg.AllowImplicitSessions() {
		queryService.EXPECT().
			ExecuteQuery(gomock.Any(), gomock.Any()).
			DoAndReturn(executeQueryChecker(t, wantConcurrentResultSets, ctrl)).
			Times(1)
	} else {
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		stubAttachStreamContext(attachStream)
		attachStream.EXPECT().Recv().Return(&Ydb_Query.SessionState{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil).AnyTimes()

		queryService.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "test-session",
		}, nil)
		queryService.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
		queryService.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil).AnyTimes()
		queryService.EXPECT().
			ExecuteQuery(gomock.Any(), gomock.Any()).
			DoAndReturn(executeQueryChecker(t, wantConcurrentResultSets, ctrl)).
			Times(1)
	}

	client, err := newWithQueryServiceClient(t.Context(), queryService, nil, cfg)
	require.NoError(t, err)

	return client
}

func executeQueryChecker(
	t *testing.T,
	wantConcurrentResultSets bool,
	ctrl *gomock.Controller,
) func(
	context.Context, *Ydb_Query.ExecuteQueryRequest, ...grpc.CallOption,
) (Ydb_Query_V1.QueryService_ExecuteQueryClient, error) {
	t.Helper()

	return func(
		_ context.Context,
		req *Ydb_Query.ExecuteQueryRequest,
		_ ...grpc.CallOption,
	) (Ydb_Query_V1.QueryService_ExecuteQueryClient, error) {
		require.Equal(t, wantConcurrentResultSets, req.GetConcurrentResultSets())

		return singleRowExecuteQueryStream(ctrl), nil
	}
}

func singleRowExecuteQueryStream(ctrl *gomock.Controller) *MockQueryService_ExecuteQueryClient {
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		ResultSet: &Ydb.ResultSet{
			Rows: []*Ydb.Value{{}},
		},
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)

	return stream
}

func interleavedMultiResultSetStream(ctrl *gomock.Controller) *MockQueryService_ExecuteQueryClient {
	int64Type := &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}}
	int64Col := func(name string) *Ydb.Column {
		return &Ydb.Column{Name: name, Type: int64Type}
	}
	int64Row := func(v int64) *Ydb.Value {
		return &Ydb.Value{Items: []*Ydb.Value{{
			Value: &Ydb.Value_Int64Value{Int64Value: v},
		}}}
	}
	respPart := func(idx int64, columns []*Ydb.Column, rows []*Ydb.Value) *Ydb_Query.ExecuteQueryResponsePart {
		return &Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: idx,
			ResultSet: &Ydb.ResultSet{
				Columns: columns,
				Rows:    rows,
			},
		}
	}

	stream := newExecuteQueryStreamMock(ctrl)
	parts := []*Ydb_Query.ExecuteQueryResponsePart{
		respPart(0, []*Ydb.Column{int64Col("a")}, []*Ydb.Value{int64Row(10)}),
		respPart(1, []*Ydb.Column{int64Col("b")}, []*Ydb.Value{int64Row(20)}),
		respPart(0, nil, []*Ydb.Value{int64Row(11)}),
		respPart(1, nil, []*Ydb.Value{int64Row(21)}),
	}
	for _, part := range parts {
		stream.EXPECT().Recv().Return(part, nil)
	}
	stream.EXPECT().Recv().Return(nil, io.EOF)

	return stream
}
