package topicclientinternal

import (
	"testing"

	"github.com/stretchr/testify/require"

	internalTopic "github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

func TestReaderInfoNormalizesConfiguredDatabase(t *testing.T) {
	for _, database := range []string{"/local", "/local//", "/local/./", "/local/child/..", "local/"} {
		t.Run(database, func(t *testing.T) {
			client := &Client{cfg: newTopicConfig(
				internalTopic.WithEndpoint("node:2135"), internalTopic.WithDatabase(database),
			)}
			require.Equal(t, topicreadercommon.ReaderInfo{
				Endpoint: "node:2135", Database: "/local", Consumer: "consumer",
			}, client.readerInfo("consumer"))
			require.Equal(t, database, client.cfg.Database, "connection configuration must be preserved")
		})
	}
	require.Empty(t, (&Client{}).readerInfo("").Database)
}
