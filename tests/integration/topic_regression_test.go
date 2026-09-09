//go:build integration
// +build integration

package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func TestRegressionIssue1011_WriteInitInfoLastSeqNum(t *testing.T) {
	scope := newScope(t)
	w1 := scope.TopicWriter()
	err := w1.Write(scope.Ctx, topicwriter.Message{
		Data: strings.NewReader("123"),
	})
	require.NoError(t, err)
	require.NoError(t, w1.Close(scope.Ctx))

	// Check
	w2, err := scope.Driver().Topic().StartWriter(
		scope.TopicPath(),
		topicoptions.WithWriterProducerID(scope.TopicWriterProducerID()),
		topicoptions.WithWriterSetAutoSeqNo(false),
	)
	require.NoError(t, err)

	info, err := w2.WaitInitInfo(scope.Ctx)
	require.Equal(t, int64(1), info.LastSeqNum)
}

// TestRegressionConsumerAlterPreservesSupportedCodecs guards against an unrelated
// consumer alter silently wiping the consumer's supported-codecs restriction.
//
// Before the fix, AlterConsumer.ToProto() populated set_supported_codecs
// unconditionally, so altering any other consumer property (here: Important) sent
// an empty codec list ("allow any") and reset the server-side restriction.
func TestRegressionConsumerAlterPreservesSupportedCodecs(t *testing.T) {
	scope := newScope(t)
	ctx := scope.Ctx
	client := scope.Driver().Topic()

	consumerName := "consumer-with-codecs"
	codecs := []topictypes.Codec{topictypes.CodecGzip}

	topicPath := scope.Driver().Name() + "/test-topic-" + t.Name()
	_ = client.Drop(ctx, topicPath)
	err := client.Create(ctx, topicPath,
		topicoptions.CreateWithConsumer(topictypes.Consumer{
			Name:            consumerName,
			SupportedCodecs: codecs,
		}),
	)
	require.NoError(t, err)

	// Alter an unrelated property of the consumer; codecs must be left untouched.
	err = client.Alter(ctx, topicPath,
		topicoptions.AlterConsumerWithImportant(consumerName, true),
	)
	require.NoError(t, err)

	desc, err := client.Describe(ctx, topicPath)
	require.NoError(t, err)

	require.Len(t, desc.Consumers, 1)
	require.Equal(t, consumerName, desc.Consumers[0].Name)
	require.True(t, desc.Consumers[0].Important)
	require.Equal(t, codecs, desc.Consumers[0].SupportedCodecs)
}
