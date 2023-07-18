package topicreader

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

// Reader allow to read message from YDB topics.
// ReadMessage or ReadMessageBatch can call concurrency with Commit, other concurrency call is denied.
//
// In other words you can have one goroutine for read messages and one goroutine for commit messages.
//
// Concurrency table
// | Method           | ReadMessage | ReadMessageBatch | Commit | Close |
// | ReadMessage      |      -      |         -        |   +    | -     |
// | ReadMessageBatch |      -      |         -        |   +    | -     |
// | Commit           |      +      |         +        |   -    | -     |
// | Close            |      -      |         -        |   -    | -     |
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Reader struct {
	reader         topicreaderinternal.Reader
	readInFlyght   int32
	commitInFlyght int32
}

// NewReader
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func NewReader(internalReader topicreaderinternal.Reader) *Reader {
	return &Reader{reader: internalReader}
}

// ReadMessage read exactly one message
// exactly one of message, error is nil
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (r *Reader) ReadMessage(ctx context.Context) (*Message, error) {
	if err := r.inCall(&r.readInFlyght); err != nil {
		return nil, err
	}
	defer r.outCall(&r.readInFlyght)

	return r.reader.ReadMessage(ctx)
}

// Message
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Message = topicreaderinternal.PublicMessage

type MessageBuilder = topicreaderinternal.PublicMessageBuilder

// NewMessageBuilder create builder, which can create Message (use for tests only)
func NewMessageBuilder() *MessageBuilder {
	return topicreaderinternal.NewPublicMessageBuilder()
}

// MessageContentUnmarshaler
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type MessageContentUnmarshaler = topicreaderinternal.PublicMessageContentUnmarshaler

// Commit receive Message, Batch of single offset
// It can be fast (by default) or sync and waite response from server
// see topicoptions.CommitMode for details
//
// for topicoptions.CommitModeSync mode sync the method can return ErrCommitToExpiredSession
// it means about the message/batch was not committed because connection broken or partition routed to
// other reader by server.
// Client code should continue work normally
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (r *Reader) Commit(ctx context.Context, obj CommitRangeGetter) error {
	if err := r.inCall(&r.commitInFlyght); err != nil {
		return err
	}
	defer r.outCall(&r.commitInFlyght)

	return r.reader.Commit(ctx, obj)
}

// CommitRangeGetter
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type CommitRangeGetter = topicreaderinternal.PublicCommitRangeGetter

// ReadMessageBatch read batch of messages
// Batch is ordered message group from one partition
// exactly one of Batch, err is nil
// if Batch is not nil - reader guarantee about all Batch.Messages are not nil
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...ReadBatchOption) (*Batch, error) {
	if err := r.inCall(&r.readInFlyght); err != nil {
		return nil, err
	}
	defer r.outCall(&r.readInFlyght)

	return r.reader.ReadMessageBatch(ctx, opts...)
}

// Batch is group of ordered messages from one partition
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Batch = topicreaderinternal.PublicBatch

// ReadBatchOption
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type ReadBatchOption = topicreaderinternal.PublicReadBatchOption

// Close stop work with reader
// return when reader complete internal works, flush commit buffer, ets
// or when ctx cancelled
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (r *Reader) Close(ctx context.Context) error {
	// close must be non-concurrent with read and commit

	if err := r.inCall(&r.readInFlyght); err != nil {
		return err
	}
	defer r.outCall(&r.readInFlyght)

	if err := r.inCall(&r.commitInFlyght); err != nil {
		return err
	}
	defer r.outCall(&r.commitInFlyght)

	return r.reader.Close(ctx)
}

func (r *Reader) inCall(inFlight *int32) error {
	if atomic.CompareAndSwapInt32(inFlight, 0, 1) {
		return nil
	}

	return xerrors.WithStackTrace(ErrConcurrencyCall)
}

func (r *Reader) outCall(inFlight *int32) {
	if atomic.CompareAndSwapInt32(inFlight, 1, 0) {
		return
	}

	panic("ydb: topic reader out call without in call, must be never")
}
