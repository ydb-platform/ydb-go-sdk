package topicclientinternal

import (
	"testing"

	"github.com/stretchr/testify/require"

	internalTopic "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

func TestReaderInfoPropagatesDriverAttributes(t *testing.T) {
	const (
		configuredEndpoint = "configured:2135"
		configuredDatabase = "/local"
	)

	client := &Client{
		cfg: newTopicConfig(
			internalTopic.WithEndpoint(configuredEndpoint),
			internalTopic.WithDatabase(configuredDatabase),
		),
	}

	require.Equal(t, topicreadercommon.ReaderInfo{
		Endpoint: configuredEndpoint,
		Database: configuredDatabase,
		Consumer: "consumer",
	}, client.readerInfo("consumer", nil))

	require.Equal(t, topicreadercommon.ReaderInfo{
		Endpoint:   configuredEndpoint,
		Database:   configuredDatabase,
		Consumer:   "consumer",
		ReaderName: readerNamePointer("reader-name"),
	}, client.readerInfo("consumer", readerNamePointer("reader-name")))
}

func TestReaderInfoNormalizesConfiguredDatabase(t *testing.T) {
	for _, database := range []string{"/local", "/local//", "/local/./", "/local/child/..", "local/"} {
		t.Run(database, func(t *testing.T) {
			client := &Client{cfg: newTopicConfig(internalTopic.WithDatabase(database))}
			require.Equal(t, "/local", client.readerInfo("consumer", readerNamePointer("reader")).Database)
			require.Equal(t, database, client.cfg.Database, "connection configuration must be preserved")
		})
	}
	require.Empty(t, (&Client{}).readerInfo("", readerNamePointer("")).Database)
}

func readerNamePointer(name string) *string {
	return &name
}
