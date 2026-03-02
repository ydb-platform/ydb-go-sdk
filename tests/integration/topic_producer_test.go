//go:build integration
// +build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTopicProducer_WaitInitAndClose verifies that internal topic producer
// can be initialized and closed against a real YDB topic.
func TestTopicProducer_WaitInitAndClose(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx

	topicClient := scope.Driver().Topic()
	producer, err := topicClient.CreateProducer(scope.TopicPath())
	require.NoError(t, err)

	err = producer.WaitInit(ctx)
	require.NoError(t, err)

	err = producer.Close(ctx)
	require.NoError(t, err)
}
