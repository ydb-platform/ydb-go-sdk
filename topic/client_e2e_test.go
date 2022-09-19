//go:build !fast
// +build !fast

package topic_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestClient_CreateDropTopic(t *testing.T) {
	ctx := context.Background()
	db := connect(t)
	topicPath := db.Name() + "/testtopic"

	_ = db.Topic().Drop(ctx, topicPath)
	err := db.Topic().Create(ctx, topicPath,
		[]topictypes.Codec{topictypes.CodecRaw},
		topicoptions.CreateWithConsumer(
			topictypes.Consumer{
				Name: "test",
			},
		),
	)
	require.NoError(t, err)

	_, err = db.Topic().Describe(ctx, topicPath)
	require.NoError(t, err)

	err = db.Topic().Drop(ctx, topicPath)
	require.NoError(t, err)
}

func connect(t testing.TB, opts ...ydb.Option) ydb.Connection {
	connectionString := "grpc://localhost:2136/local"
	if cs := os.Getenv("YDB_CONNECTION_STRING"); cs != "" {
		connectionString = cs
	}

	var grpcOptions []grpc.DialOption
	const needLogGRPCMessages = true
	if needLogGRPCMessages {
		grpcOptions = append(grpcOptions,
			grpc.WithChainUnaryInterceptor(xtest.NewGrpcLogger(t).UnaryClientInterceptor),
			grpc.WithChainStreamInterceptor(xtest.NewGrpcLogger(t).StreamClientInterceptor),
		)
	}

	ydbOpts := []ydb.Option{
		ydb.WithDialTimeout(time.Second),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.With(config.WithGrpcOptions(grpcOptions...)),
	}
	ydbOpts = append(ydbOpts, opts...)

	db, err := ydb.Open(context.Background(), connectionString, ydbOpts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close(context.Background())
	})
	return db
}
