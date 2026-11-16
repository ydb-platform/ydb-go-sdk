//go:build go1.23

package topicsugar

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// TopicMessageReader is interface for topicreader.Message
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type TopicMessageReader interface {
	ReadMessage(ctx context.Context) (*topicreader.Message, error)
}

// TopicMessageIterator iterator wrapper over topic reader
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

// BytesIterator produce iterator over topic messages with Data as []byte, []byte is content of the message
func BytesIterator(
	ctx context.Context,
	r TopicMessageReader,
) xiter.Seq2[*TypedTopicMessage[[]byte], error] {
	var unmarshalFunc TypedUnmarshalFunc[*[]byte] = func(data []byte, dst *[]byte) error {
		*dst = slices.Clone(data)

		return nil
	}

	return IteratorFunc[[]byte](ctx, r, unmarshalFunc)
}

// StringIterator produce iterator over topic messages with Data is string, created from message content
func StringIterator(
	ctx context.Context,
	r TopicMessageReader,
) xiter.Seq2[*TypedTopicMessage[string], error] {
	var unmarshalFunc TypedUnmarshalFunc[*string] = func(data []byte, dst *string) error {
		*dst = string(data)

		return nil
	}

	return IteratorFunc[string](ctx, r, unmarshalFunc)
}

// JSONIterator produce iterator over topic messages with Data is T, created unmarshalled from message
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func JSONIterator[T any](
	ctx context.Context,
	r TopicMessageReader,
) xiter.Seq2[*TypedTopicMessage[T], error] {
	var unmarshalFunc TypedUnmarshalFunc[*T] = func(data []byte, dst *T) error {
		return json.Unmarshal(data, dst)
	}

	return IteratorFunc[T](ctx, r, unmarshalFunc)
}

// ProtobufIterator produce iterator over topic messages with Data is T, created unmarshalled from message
//
// T must be a concrete generated protobuf type (for example *examplepb.Message),
// not the proto.Message interface itself, otherwise iteration yields an error.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func ProtobufIterator[T proto.Message](
	ctx context.Context,
	r TopicMessageReader,
) xiter.Seq2[*TypedTopicMessage[T], error] {
	var unmarshalFunc TypedUnmarshalFunc[*T] = func(data []byte, dst *T) error {
		// *dst is the zero value of T (a nil proto.Message), so a concrete
		// message must be allocated before unmarshaling into it. Without this
		// proto.Unmarshal dereferences the nil pointer and panics.
		var zero T

		// A non-concrete instantiation (T == proto.Message) has a nil interface
		// zero value; calling ProtoReflect on it would panic, so reject it here.
		if any(zero) == nil {
			return fmt.Errorf("ydb: topicsugar: ProtobufIterator requires a concrete protobuf message type, got %T", zero)
		}

		msg, ok := zero.ProtoReflect().New().Interface().(T)
		if !ok {
			return fmt.Errorf("ydb: topicsugar: failed to allocate message of type %T", zero)
		}
		*dst = msg

		return proto.Unmarshal(data, *dst)
	}

	return IteratorFunc[T](ctx, r, unmarshalFunc)
}

// IteratorFunc produce iterator over topic messages with Data is T,
// created unmarshalled from message by custom function
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func IteratorFunc[T any](
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
