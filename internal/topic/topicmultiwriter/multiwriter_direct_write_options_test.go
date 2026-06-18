package topicmultiwriter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

func TestWriteToManyPartitionsSetsDirectWrite(t *testing.T) {
	mwCfg := &MultiWriterConfig{}
	WithWriterPartitionByPartitionID()(mwCfg)
	WithDirectWrite(true)(mwCfg)

	cfg := topicwriterinternal.NewWriterReconnectorConfig()
	cfg.MultiWriterConfig = mwCfg

	got, ok := cfg.MultiWriterConfig.(*MultiWriterConfig)
	require.True(t, ok)
	require.True(t, got.DirectWrite)
}
