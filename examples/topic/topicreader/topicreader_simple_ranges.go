//go:build ydb_experiment && goexperiment.rangefunc

package topicreaderexamples

import (
	"context"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// PrintMessageContent is simple example for easy start read messages
// it is not recommend way for heavy-load processing, batch processing usually will faster
func PrintMessageContentRange(ctx context.Context, reader *topicreader.Reader) {
	for msg, err := range reader.RangeMessages(ctx) {
		if err != nil {
			fmt.Printf("err: %+v\n", err)
			break
		}

		content, _ := io.ReadAll(msg)
		fmt.Println(string(content))
		_ = reader.Commit(msg.Context(), msg)
	}
}

// ReadMessagesByBatch it is recommended way for process messages
func ReadMessagesByBatchRange(ctx context.Context, reader *topicreader.Reader) {
	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch.Context(), batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}
