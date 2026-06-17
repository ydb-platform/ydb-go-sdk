package topicmultiwriter_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func TestWriteToManyPartitionsSetsDirectWrite(t *testing.T) {
	cfg := topicwriterinternal.NewWriterReconnectorConfig()
	topicoptions.WithWriteToManyPartitions(
		topicoptions.WithWriterPartitionByPartitionID(),
		topicoptions.WithMultiWriterDirectWrite(true),
	)(&cfg)

	mwCfg, ok := cfg.MultiWriterConfig.(*topicmultiwriter.MultiWriterConfig)
	require.True(t, ok)
	require.True(t, mwCfg.DirectWrite)
}
