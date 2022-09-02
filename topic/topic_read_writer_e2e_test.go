//go:build !fast
// +build !fast

package topic_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicclientinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestSendAsyncMessages(t *testing.T) {
	ctx := context.Background()
	db := connect(t)
	topicPath := createTopic(ctx, t, db)

	content := "hello"

	writer, err := db.Topic().(*topicclientinternal.Client).StartWriter("test", topicPath)
	require.NoError(t, err)
	require.NotEmpty(t, writer)
	require.NoError(t, writer.Write(ctx, topicwriter.Message{SeqNo: 1, CreatedAt: time.Now(), Data: strings.NewReader(content)}))

	reader, err := db.Topic().StartReader(consumerName, topicoptions.ReadTopic(topicPath))
	require.NoError(t, err)
	readCtx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	mess, err := reader.ReadMessage(readCtx)
	require.NoError(t, err)

	readBytes, err := io.ReadAll(mess)
	require.NoError(t, err)
	require.Equal(t, content, string(readBytes))
}

func createTopic(ctx context.Context, t *testing.T, db ydb.Connection) (topicPath string) {
	topicPath = db.Name() + "/" + t.Name() + "--test-topic"
	_ = db.Topic().Drop(ctx, topicPath)
	err := db.Topic().Create(ctx, topicPath, []topictypes.Codec{topictypes.CodecRaw}, topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumerName}))
	require.NoError(t, err)
	return topicPath
}
