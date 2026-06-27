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
	context.Context,
	*Ydb_Query.ExecuteQueryRequest,
	...grpc.CallOption,
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
