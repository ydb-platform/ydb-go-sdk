//go:build go1.23

package topicreaderexamples

import (
	"context"
	"fmt"

	firestore "google.golang.org/genproto/firestore/bundle"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicsugar"
)

// IterateOverMessagesAsString is simple example for easy start read messages
// it is not recommend way for heavy-load processing, batch processing usually will faster
func IterateOverMessagesAsString(ctx context.Context, reader *topicreader.Reader) error {
	for msg, err := range topicsugar.StringIterator(ctx, reader) {
		if err != nil {
			return err
		}
		fmt.Println(msg.Data)
		_ = reader.Commit(msg.Context(), msg)
	}

	return nil
}

// IterateOverStructUnmarshalledFromJSON is example for effective way for unmarshal json message content to value
func IterateOverStructUnmarshalledFromJSON(ctx context.Context, r *topicreader.Reader) error {
	//nolint:tagliatelle
	type S struct {
		MyField int `json:"my_field"`
	}

	for msg, err := range topicsugar.JSONIterator[S](ctx, r) {
		if err != nil {
			return err
		}
		fmt.Println(msg.Data.MyField)
	}

	return nil
}

// IterateOverProtobufMessages is example for effective way for unmarshal protobuf message content to value
func IterateOverProtobufMessages(ctx context.Context, r *topicreader.Reader) error {
	for msg, err := range topicsugar.ProtobufIterator[*firestore.BundledDocumentMetadata](ctx, r) {
		if err != nil {
			return err
		}
		fmt.Println(msg.Data.GetName())
	}

	return nil
}
