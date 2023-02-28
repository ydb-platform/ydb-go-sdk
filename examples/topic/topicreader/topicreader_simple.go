package topicreaderexamples

import (
	"context"
	"fmt"
	"io"

	firestore "google.golang.org/genproto/firestore/bundle"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

// PrintMessageContent is simple example for easy start read messages
// it is not recommend way for heavy-load processing, batch processing usually will faster
func PrintMessageContent(ctx context.Context, reader *topicreader.Reader) {
	for {
		msg, _ := reader.ReadMessage(ctx)
		content, _ := io.ReadAll(msg)
		fmt.Println(string(content))
		_ = reader.Commit(msg.Context(), msg)
	}
}

// ReadMessagesByBatch it is recommended way for process messages
func ReadMessagesByBatch(ctx context.Context, reader *topicreader.Reader) {
	for {
		batch, _ := reader.ReadMessageBatch(ctx)
		processBatch(batch.Context(), batch)
		_ = reader.Commit(batch.Context(), batch)
	}
}

// UnmarshalMessageContentToJSONStruct is example for effective way for unmarshal json message content to value
func UnmarshalMessageContentToJSONStruct(msg *topicreader.Message) {
	type S struct {
		MyField int `json:"my_field"`
	}

	var v S

	_ = topicsugar.JSONUnmarshal(msg, &v)
}

// UnmarshalMessageContentToProtobufStruct is example for effective way for unmarshal protobuf message content to value
func UnmarshalMessageContentToProtobufStruct(msg *topicreader.Message) {
	v := &firestore.BundledDocumentMetadata{} // protobuf type

	_ = topicsugar.ProtoUnmarshal(msg, v)
}
