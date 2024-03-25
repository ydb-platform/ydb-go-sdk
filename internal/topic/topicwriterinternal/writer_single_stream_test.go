package topicwriterinternal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestWriterImpl_CreateInitMessage(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		ctx := xtest.Context(t)
		cfg := SingleStreamWriterConfig{
			WritersCommonConfig: WritersCommonConfig{
				producerID:          "producer",
				topic:               "topic",
				writerMeta:          map[string]string{"key": "val"},
				defaultPartitioning: rawtopicwriter.NewPartitioningPartitionID(5),
				compressorCount:     1,
			},
			getLastSeqNum: true,
		}
		w := newSingleStreamWriterStopped(ctx, cfg)
		expected := rawtopicwriter.InitRequest{
			Path:             w.cfg.topic,
			ProducerID:       w.cfg.producerID,
			WriteSessionMeta: w.cfg.writerMeta,
			Partitioning:     w.cfg.defaultPartitioning,
			GetLastSeqNo:     true,
		}
		require.Equal(t, expected, w.createInitRequest())
	})

	t.Run("WithoutGetLastSeq", func(t *testing.T) {
		ctx := xtest.Context(t)
		w := newSingleStreamWriterStopped(ctx,
			SingleStreamWriterConfig{getLastSeqNum: false},
		)
		require.False(t, w.createInitRequest().GetLastSeqNo)
	})
}
