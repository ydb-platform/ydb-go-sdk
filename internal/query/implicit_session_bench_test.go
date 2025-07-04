package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func BenchmarkImplicitSessionsOnBalancerMock(b *testing.B) {
	queryAttachStream := testutil.MockClientStream()
	queryAttachStream.OnRecvMsg = func(m any) error {
		m.(*Ydb_Query.SessionState).Status = Ydb.StatusIds_SUCCESS

		return nil
	}

	ctx := context.Background()
	balancer := testutil.NewBalancer(
		testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.QueryCreateSession: func(request any) (result proto.Message, err error) {
				return &Ydb_Query.CreateSessionResponse{
					SessionId: testutil.SessionID(),
				}, nil
			},
		}),
		testutil.WithNewStreamHandlers(testutil.NewStreamHandlers{
			testutil.QueryExecuteQuery: func(desc *grpc.StreamDesc) (grpc.ClientStream, error) {
				return testutil.MockClientStream(), nil
			},
			testutil.QueryAttachSession: func(desc *grpc.StreamDesc) (grpc.ClientStream, error) {
				return queryAttachStream, nil
			},
		}),
	)
	client := New(ctx, balancer, config.New())

	b.Run("explicit", func(b *testing.B) {
		for range b.N {
			err := client.Exec(ctx, "SELECT 1")
			require.NoError(b, err)
		}
	})

	implicitSessionOpt := query.WithImplicitSession()

	b.Run("implicit", func(b *testing.B) {
		for range b.N {
			err := client.Exec(ctx, "SELECT 1", implicitSessionOpt)
			require.NoError(b, err)
		}
	})
}
