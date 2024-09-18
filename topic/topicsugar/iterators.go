package topicsugar

import (
	"context"
	"encoding/json"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// MessageReader is interface for topicreader.Message
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type TopicMessageReader interface {
	ReadMessage(ctx context.Context) (*topicreader.Message, error)
}

// TopicMessagesIterator is typed representation of cdc event
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func TopicMessageIterator(ctx context.Context, r TopicMessageReader) xiter.Seq2[*topicreader.Message, error] {
	return func(yield func(*topicreader.Message, error) bool) {
		for {
			mess, err := r.ReadMessage(ctx)
			if !yield(mess, err) {
				return
			}

			if err != nil {
				return
			}
		}
	}
}

// TopicUnmarshalJSONIterator is typed representation of cdc event
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func TopicUnmarshalJSONIterator[T any](
	ctx context.Context,
	r TopicMessageReader,
) xiter.Seq2[*TypedTopicMessage[T], error] {
	var unmarshalFunc TypedUnmarshalFunc[*T] = func(data []byte, dst *T) error {
		return json.Unmarshal(data, dst)
	}

	return TopicUnmarshalJSONFunc[T](ctx, r, unmarshalFunc)
}

// TopicUnmarshalJSONIterator is typed representation of cdc event
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func TopicUnmarshalJSONFunc[T any](
	ctx context.Context,
	r TopicMessageReader,
	f TypedUnmarshalFunc[*T],
) xiter.Seq2[*TypedTopicMessage[T], error] {
	return func(yield func(*TypedTopicMessage[T], error) bool) {
		for {
			mess, err := r.ReadMessage(ctx)
			if err != nil {
				yield(nil, err)

				return
			}

			var res TypedTopicMessage[T]

			var unmarshal UnmarshalFunc = func(data []byte, _ any) error {
				return f(data, &res.Data)
			}

			err = UnmarshalMessageWith(mess, unmarshal, nil)
			if err != nil {
				yield(nil, err)

				return
			}

			res.Message = mess

			if !yield(&res, err) {
				return
			}
		}
	}
}

type TypedTopicMessage[T any] struct {
	*topicreader.Message
	Data T
}

type TypedUnmarshalFunc[T any] func(data []byte, dst T) error
