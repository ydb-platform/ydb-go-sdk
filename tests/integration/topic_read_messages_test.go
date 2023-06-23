//go:build integration
// +build integration

package integration

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

func TestTopicReadMessages(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	for i := 0; i < 2; i++ {
		start := time.Now()
		err := scope.TopicWriter().Write(ctx, topicwriter.Message{Data: strings.NewReader("asd")})
		require.NoError(t, err)
		msg, err := scope.TopicReader().ReadMessage(ctx)
		require.NoError(t, err)
		require.Less(t, start.UnixNano(), msg.CreatedAt.UnixNano())

		require.NoError(t, err)
		err = topicsugar.ReadMessageDataWithCallback(msg, func(data []byte) error {
			require.Equal(t, "asd", string(data))
			return nil
		})
		require.NoError(t, err)
	}
}
