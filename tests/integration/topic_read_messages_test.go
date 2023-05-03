//go:build integration
// +build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

func TestTopicReadMessages(t *testing.T) {
	ctx := xtest.Context(t)

	db, reader := createFeedAndReader(ctx, t)

	sendCDCMessage(ctx, t, db)
	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, msg.CreatedAt)
	t.Logf("msg: %#v", msg)

	require.NoError(t, err)
	err = topicsugar.ReadMessageDataWithCallback(msg, func(data []byte) error {
		t.Log("Content:", string(data))
		return nil
	})
	require.NoError(t, err)

	sendCDCMessage(ctx, t, db)
	batch, err := reader.ReadMessageBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Messages)
}
