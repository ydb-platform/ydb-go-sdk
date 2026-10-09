package topicclientinternal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTransactionalWriterOptionsCanShareReconnector(t *testing.T) {
	client := New(context.Background(), nil, credentials.NewAnonymousCredentials())
	opts := []topicoptions.WriterOption{
		topicoptions.WithWriterDirectWrite(false),
		topicoptions.WithWriterProducerID("producer"),
	}
	first := client.createWriterConfig("topic", opts)
	second := client.createWriterConfig("topic", opts)
	require.True(t, first.CanPool())
	require.True(t, first.PoolCompatible(second))

	otherProducer := client.createWriterConfig("topic", []topicoptions.WriterOption{
		topicoptions.WithWriterProducerID("other"),
	})
	require.False(t, first.PoolCompatible(otherProducer))

	customTrace := client.createWriterConfig("topic", []topicoptions.WriterOption{
		topicoptions.WithWriterTrace(trace.Topic{}),
	})
	require.False(t, customTrace.CanPool())
}
