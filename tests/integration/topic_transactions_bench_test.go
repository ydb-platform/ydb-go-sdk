//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// BenchmarkTopicTransactionalMultiWriterWrite measures the repeated short-lived
// transactional writer path, including partition writer initialization.
func BenchmarkTopicTransactionalMultiWriterWrite(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = "grpc://localhost:2136/local"
	}
	options := []ydb.Option{ydb.WithAnonymousCredentials()}
	if token := os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"); token != "" {
		options = []ydb.Option{ydb.WithAccessTokenCredentials(token)}
	}
	db, err := ydb.Open(ctx, connectionString, options...)
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, db.Close(context.Background()))
	})

	folderPath := path.Join(db.Name(), strings.ReplaceAll(b.Name(), "/", "__"))
	require.NoError(b, sugar.RemoveRecursive(ctx, db, folderPath))
	require.NoError(b, db.Scheme().MakeDirectory(ctx, folderPath))
	b.Cleanup(func() {
		require.NoError(b, sugar.RemoveRecursive(context.Background(), db, folderPath))
	})
	topicPath := path.Join(folderPath, "topic")
	require.NoError(b, db.Topic().Create(ctx, topicPath,
		topicoptions.CreateWithMinActivePartitions(1),
		topicoptions.CreateWithAutoPartitioningSettings(topictypes.AutoPartitioningSettings{
			AutoPartitioningStrategy: topictypes.AutoPartitioningStrategyDisabled,
		}),
	))

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			writer, err := db.Topic().StartTransactionalWriter(tx, topicPath,
				topicoptions.WithWriterWaitServerAck(true),
				topicoptions.WithWriteToManyPartitions(
					topicoptions.WithProducerIDPrefix("transactional-writer-benchmark"),
					topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
				),
			)
			if err != nil {
				return err
			}

			return writer.Write(ctx, topicwriter.Message{
				Key:  "key",
				Data: strings.NewReader("payload"),
			})
		}, query.WithLazyTx(false))
		require.NoError(b, err)
	}
}
