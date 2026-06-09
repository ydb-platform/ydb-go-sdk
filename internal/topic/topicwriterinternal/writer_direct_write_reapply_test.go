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
	require.True(t, cfg.directWrite.resolved.unknown())

	WithPartitioning(NewPartitioningWithPartitionID(7))(&cfg)
	cfg.ReapplyDirectWritePartitionState()

	require.EqualValues(t, 7, cfg.directWrite.resolved.partitionIDValue())
	require.True(t, cfg.directWrite.pinnedByUser)
}
