//go:build !fast
// +build !fast

package topic_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicclientinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestSendAsyncMessages(t *testing.T) {
	ctx := context.Background()
	db := connect(t)
	topicPath := createTopic(ctx, t, db)

	content := "hello"

	producerID := "test-producer-ang-message-group"
	writer, err := db.Topic().(*topicclientinternal.Client).StartWriter(producerID, topicPath,
		topicoptions.WithMessageGroupID(producerID),
	)
	require.NoError(t, err)
	require.NotEmpty(t, writer)
	require.NoError(t, writer.Write(ctx, topicwriter.Message{Data: strings.NewReader(content)}))

	reader, err := db.Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath))
	require.NoError(t, err)

	readCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	mess, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	readBytes, err := io.ReadAll(mess)
	require.NoError(t, err)
	require.Equal(t, content, string(readBytes))
}

func TestSendSyncMessages(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := testCtx(t)

		grpcStopper := topic.NewGrpcStopper(errors.New("stop grpc for test"))

		db := connect(t,
			grpc.WithChainUnaryInterceptor(grpcStopper.UnaryClientInterceptor),
			grpc.WithChainStreamInterceptor(grpcStopper.StreamClientInterceptor),
		)
		topicPath := createTopic(ctx, t, db)

		producerID := "test-producer"
		writer, err := db.Topic().(*topicclientinternal.Client).StartWriter(
			producerID,
			topicPath,
			topicoptions.WithWriterPartitioning(
				topicwriter.NewPartitioningWithMessageGroupID(producerID),
			),
			topicoptions.WithSyncWrite(true),
		)
		require.NoError(t, err)
		msg := topicwriter.Message{CreatedAt: time.Now(), Data: strings.NewReader("1")}
		err = writer.Write(ctx, msg)
		require.NoError(t, err)

		grpcStopper.Stop() // stop any activity through connections

		// check about connection broken
		msg = topicwriter.Message{CreatedAt: time.Now(), Data: strings.NewReader("nosent")}
		err = writer.Write(ctx, msg)
		require.Error(t, err)

		db = connect(t)
		writer, err = db.Topic().(*topicclientinternal.Client).StartWriter(producerID, topicPath,
			topicoptions.WithWriterPartitioning(
				topicwriter.NewPartitioningWithMessageGroupID(producerID),
			),
			topicoptions.WithSyncWrite(true),
		)
		require.NoError(t, err)
		msg = topicwriter.Message{CreatedAt: time.Now(), Data: strings.NewReader("2")}
		err = writer.Write(ctx, msg)
		require.NoError(t, err)

		reader, err := db.Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath))
		require.NoError(t, err)
		mess, err := reader.ReadMessage(ctx)
		require.NoError(t, err)
		require.NoError(t, topicsugar.ReadMessageDataWithCallback(mess, func(data []byte) error {
			require.Equal(t, "1", string(data))
			return nil
		}))
		mess, err = reader.ReadMessage(ctx)
		require.NoError(t, err)
		require.NoError(t, topicsugar.ReadMessageDataWithCallback(mess, func(data []byte) error {
			require.Equal(t, "2", string(data))
			return nil
		}))
	})
}

func createTopic(ctx context.Context, t testing.TB, db ydb.Connection) (topicPath string) {
	topicPath = db.Name() + "/" + t.Name() + "--test-topic"
	_ = db.Topic().Drop(ctx, topicPath)
	err := db.Topic().Create(
		ctx,
		topicPath,
		[]topictypes.Codec{topictypes.CodecRaw},
		topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumerName}),
	)
	require.NoError(t, err)

	return topicPath
}
