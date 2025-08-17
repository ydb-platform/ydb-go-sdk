package topicwriterinternal

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
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

func TestCutRequestBytes(t *testing.T) {
	t.Run("SingleLargeMessage", func(t *testing.T) {
		orig := &rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 1000)},
		}}
		head, rest := cutRequestBytes(orig, 10)
		require.Same(t, orig, head)
		require.Nil(t, rest)
	})
	t.Run("SingleSmallMessage", func(t *testing.T) {
		orig := &rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 1000)},
		}}
		head, rest := cutRequestBytes(orig, 1000)
		require.Same(t, orig, head)
		require.Nil(t, rest)
	})
	t.Run("TwoSmall", func(t *testing.T) {
		orig := &rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 100)},
			{Data: make([]byte, 200)},
		}}
		head, rest := cutRequestBytes(orig, 1000)
		require.Same(t, orig, head)
		require.Nil(t, rest)
	})
	t.Run("Split", func(t *testing.T) {
		orig := &rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 100)},
			{Data: make([]byte, 200)},
			{Data: make([]byte, 300)},
		}}

		head, rest := cutRequestBytes(orig, 300)

		JSON := func(a any) string {
			res, _ := json.MarshalIndent(a, "", "  ")

			return string(res)
		}

		// it will take only one message, becase it has overhead for grpc messages struct
		expectedHead := JSON(&rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 100)},
		}})
		expectedRest := JSON(&rawtopicwriter.WriteRequest{Messages: []rawtopicwriter.MessageData{
			{Data: make([]byte, 200)},
			{Data: make([]byte, 300)},
		}})

		headJSON := JSON(head)
		restJSON := JSON(rest)
		require.Equal(t, expectedHead, headJSON)
		require.Equal(t, expectedRest, restJSON)
	})
}
