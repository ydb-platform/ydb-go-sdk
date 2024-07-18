//go:build integration
// +build integration

package integration

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestTopicReadInTransaction(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	require.NoError(t, scope.TopicWriter().Write(ctx, topicwriter.Message{Data: strings.NewReader("asd")}))

	require.NoError(t, scope.Driver().Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		reader := scope.TopicReaderNamed("first")
		batch, err := reader.PopBatchTx(ctx, tx)
		if err != nil {
			return err
		}
		content := string(must(io.ReadAll(batch.Messages[0])))
		require.Equal(t, "asd", content)
		return nil
	}))

	require.NoError(t, scope.TopicWriter().Write(ctx, topicwriter.Message{Data: strings.NewReader("bbb")}))
	require.NoError(t, scope.Driver().Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		reader := scope.TopicReaderNamed("second")
		batch, err := reader.PopBatchTx(ctx, tx)
		if err != nil {
			return err
		}
		content := string(must(io.ReadAll(batch.Messages[0])))
		require.Equal(t, "bbb", content)
		return nil
	}))

}
