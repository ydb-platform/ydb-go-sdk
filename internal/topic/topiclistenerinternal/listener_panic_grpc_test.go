package topiclistenerinternal_test

import (
	"context"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmock"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiclistener"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestListenerSessionErrorUsesDriverPanicCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observed := make(chan any, 1)
	db, err := ydb.Open(ctx, topicmock.GrpcMockTopicConnString(fixenv.New(t), &deniedListenerServer{}),
		ydb.WithPanicCallback(func(p any) { observed <- p }),
		ydb.WithTraceTopic(trace.Topic{
			OnReaderSessionError: func(trace.TopicReaderSessionErrorInfo) { panic("session error observer") },
		}),
	)
	require.NoError(t, err)
	defer db.Close(context.Background())
	listener, err := db.Topic().StartListener("consumer", topiclistener.BaseHandler{}, topicoptions.ReadTopic("topic"))
	require.NoError(t, err)
	defer listener.Close(ctx)
	require.Equal(t, codes.PermissionDenied, status.Code(listener.WaitInit(ctx)))
	select {
	case p := <-observed:
		require.Equal(t, "session error observer", p)
	case <-ctx.Done():
		t.Fatal("driver panic callback was not called")
	}
}

type deniedListenerServer struct {
	Ydb_Topic_V1.UnimplementedTopicServiceServer
}

func (*deniedListenerServer) StreamRead(stream Ydb_Topic_V1.TopicService_StreamReadServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if req.GetInitRequest() == nil {
		return status.Error(codes.InvalidArgument, "expected init request")
	}

	return status.Error(codes.PermissionDenied, "access denied")
}
