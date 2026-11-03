//go:build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

// BenchmarkTopicMultiWriterWaitInit measures the writer lifecycle, including Close.
// Topic creation is excluded from the measurement.
func BenchmarkTopicMultiWriterWaitInit(b *testing.B) {
	for _, partitions := range []int64{64, 128, 256, 512} {
		b.Run(fmt.Sprintf("p%d", partitions), func(b *testing.B) {
			scope := newScope(b)
			client := scope.Driver().Topic()
			topicPath := scope.TopicPath(topicoptions.CreateWithMinActivePartitions(partitions))
			waitKafkaHashTopicReady(scope.Ctx, b, client, topicPath, int(partitions))
			b.ReportAllocs()

			for b.Loop() {
				writer, err := client.StartWriter(topicPath, topicoptions.WithWriteToManyPartitions(
					topicoptions.WithProducerIDPrefix("benchmark-init"),
					topicoptions.WithWriterPartitionByKey(topicoptions.KafkaHashPartitionChooser()),
				))
				if err != nil {
					b.Fatal(err)
				}
				initErr := writer.WaitInit(scope.Ctx)
				closeErr := writer.Close(scope.Ctx)
				if initErr != nil {
					b.Fatal(initErr)
				}
				if closeErr != nil {
					b.Fatal(closeErr)
				}
			}
		})
	}
}
