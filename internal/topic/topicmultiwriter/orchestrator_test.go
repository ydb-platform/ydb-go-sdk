package topicmultiwriter

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestOrchestratorSplitInstallsActiveDescendants(t *testing.T) {
	chooser := partitionchooser.NewBoundPartitionChooser(
		partitionchooser.WithKeyHasher(func(key string) string { return key }),
	)
	description := successiveSplitsDescription()
	describeCalls := 0
	multiWriter := newTestMultiWriterWithPartitionChooser(t,
		func(context.Context, string) (topictypes.TopicDescription, error) {
			describeCalls++
			if describeCalls == 1 {
				return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{{Active: true}}}, nil
			}

			return description, nil
		}, chooser,
	)
	t.Cleanup(func() { require.NoError(t, multiWriter.Close(context.Background())) })
	require.NoError(t, multiWriter.WaitInit(context.Background()))
	require.NoError(t, multiWriter.orchestrator.onPartitionSplit(0))

	for _, tc := range []struct {
		key  string
		want int64
	}{
		{key: "a", want: 4},
		{key: "\xfe", want: 5},
		{key: "\xff", want: 3},
	} {
		got, err := chooser.ChoosePartition(topicwriterinternal.PublicMessage{Key: tc.key})
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "key %q must route to an active descendant", tc.key)
		require.False(t, multiWriter.orchestrator.partitions[got].Locked)
	}
}

func TestOrchestratorGetMaxSeqNoRefreshesAfterSplit(t *testing.T) {
	ctx := context.Background()
	multiWriter := newTestMultiWriterWithCustomWritersFactory(t,
		func(context.Context, string) (topictypes.TopicDescription, error) {
			return topictypes.TopicDescription{Partitions: []topictypes.PartitionInfo{{Active: true}}}, nil
		},
		&successiveSplitWritersFactory{
			initInfo: func(_ context.Context, _ string, pinned bool) (topicwriterinternal.InitialInfo, error) {
				if pinned {
					return topicwriterinternal.InitialInfo{LastSeqNum: 10}, nil
				}

				return topicwriterinternal.InitialInfo{LastSeqNum: 42}, nil
			},
		},
	)
	t.Cleanup(func() { require.NoError(t, multiWriter.Close(ctx)) })
	require.NoError(t, multiWriter.WaitInit(ctx))
	multiWriter.orchestrator.mu.WithLock(func() {
		multiWriter.orchestrator.partitions[0].ChildPartitionIDs = []int64{2, 3}
	})

	got, err := multiWriter.orchestrator.getMaxSeqNo([]int64{0})
	require.NoError(t, err)
	require.Equal(t, int64(42), got, "split baseline must include commits after active initialization")
}

func TestOrchestratorSaveMessageContent(t *testing.T) {
	newOrchestratorWithTracer := func(tracer *trace.Topic) *orchestrator {
		return &orchestrator{
			writerCfg: &topicwriterinternal.WriterReconnectorConfig{
				WritersCommonConfig: topicwriterinternal.WritersCommonConfig{
					Tracer:     tracer,
					LogContext: context.Background(),
				},
			},
			multiWriterCfg: &MultiWriterConfig{},
		}
	}

	t.Run("ReportsSizes", func(t *testing.T) {
		var done trace.TopicWriterCompressMessagesDoneInfo
		tracer := &trace.Topic{
			OnWriterCompressMessages: func(
				trace.TopicWriterCompressMessagesStartInfo,
			) func(trace.TopicWriterCompressMessagesDoneInfo) {
				return func(info trace.TopicWriterCompressMessagesDoneInfo) {
					done = info
				}
			},
		}

		const payload = "hello world"
		msg := message{
			MessageWithDataContent: topicwritercommon.NewMessageDataWithContent(
				topicwritercommon.PublicMessage{Data: strings.NewReader(payload)},
				topicwritercommon.NewMultiEncoder(),
			),
		}

		require.NoError(t, newOrchestratorWithTracer(tracer).saveMessageContent(&msg))

		require.NoError(t, done.Error)
		require.Equal(t, len(payload), done.UncompressedSize)
		require.Equal(t, len(payload), done.CompressedSize)
	})

	t.Run("ReportsErrorWithZeroSizes", func(t *testing.T) {
		var done trace.TopicWriterCompressMessagesDoneInfo
		tracer := &trace.Topic{
			OnWriterCompressMessages: func(
				trace.TopicWriterCompressMessagesStartInfo,
			) func(trace.TopicWriterCompressMessagesDoneInfo) {
				return func(info trace.TopicWriterCompressMessagesDoneInfo) {
					done = info
				}
			},
		}

		msg := message{
			MessageWithDataContent: topicwritercommon.NewMessageDataWithContent(
				topicwritercommon.PublicMessage{Data: strings.NewReader("hello")},
				topicwritercommon.NewMultiEncoder(),
			),
		}
		// pre-cache with a different codec so raw caching below fails,
		// the same way encoders_test.go/ReportsErrorWithZeroSizes does it.
		_, err := msg.GetEncodedBytes(rawtopiccommon.CodecGzip)
		require.NoError(t, err)

		err = newOrchestratorWithTracer(tracer).saveMessageContent(&msg)
		require.Error(t, err)

		require.Error(t, done.Error)
		require.Zero(t, done.UncompressedSize)
		require.Zero(t, done.CompressedSize)
	})
}
