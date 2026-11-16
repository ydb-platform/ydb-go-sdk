package topicmultiwriter

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

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
