//go:build !fast
// +build !fast

package topic_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicclientinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestSendAsyncMessages(t *testing.T) {
	ctx := context.Background()
	db := connect(t)
	topicPath := createTopic(ctx, t, db)

	writer, err := db.Topic().(*topicclientinternal.Client).StartWriter("test", topicPath)
	require.NoError(t, err)
	require.NotEmpty(t, writer)
}

func createTopic(ctx context.Context, t *testing.T, db ydb.Connection) (topicPath string) {
	topicPath = db.Name() + "/" + t.Name() + "--" + time.Now().Format(time.RFC3339Nano)
	err := db.Topic().Create(ctx, topicPath, []topictypes.Codec{topictypes.CodecRaw}, topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumerName}))
	require.NoError(t, err)
	return topicPath
}
