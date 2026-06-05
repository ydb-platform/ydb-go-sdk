package topicwriterinternal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReapplyDirectWritePartitionState(t *testing.T) {
	cfg := NewWriterReconnectorConfig(
		WithTopic("test"),
		WithDirectWrite(true),
	)
	require.EqualValues(t, -1, cfg.directWrite.resolved.Load())

	WithPartitioning(NewPartitioningWithPartitionID(7))(&cfg)
	cfg.ReapplyDirectWritePartitionState()

	require.EqualValues(t, 7, cfg.directWrite.resolved.Load())
	require.True(t, cfg.directWrite.pinnedByUser)
}
