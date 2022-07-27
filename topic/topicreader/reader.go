package topicreader

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
)

// Reader allow to read message from YDB topics
// reader methods must not call concurrency
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Reader struct {
	reader   topicreaderinternal.Reader
	inFlyght int64
}

// NewReader
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func NewReader(internalReader topicreaderinternal.Reader) *Reader {
	return &Reader{reader: internalReader}
}

// ReadMessage read exactly one message
// exactly one of message, error is nil
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (r *Reader) ReadMessage(ctx context.Context) (*Message, error) {
	if err := r.inCall(); err != nil {
		return nil, err
	}
	defer r.outCall()

	return r.reader.ReadMessage(ctx)
}

// Message
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Message = topicreaderinternal.PublicMessage

// MessageContentUnmarshaler
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type MessageContentUnmarshaler = topicreaderinternal.PublicMessageContentUnmarshaler

// Commit receive Message, Batch of single offset
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (r *Reader) Commit(ctx context.Context, obj CommitRangeGetter) error {
	if err := r.inCall(); err != nil {
		return err
	}
	defer r.outCall()

	return r.reader.Commit(ctx, obj)
}

// CommitRangeGetter
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type CommitRangeGetter = topicreaderinternal.PublicCommitRangeGetter

// ReadMessageBatch read batch of messages
// Batch is ordered message group from one partition
// exactly one of Batch, err is nil
// if Batch is not nil - reader guarantee about all Batch.Messages are not nil
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...ReadBatchOption) (*Batch, error) {
	if err := r.inCall(); err != nil {
		return nil, err
	}
	defer r.outCall()

	return r.reader.ReadMessageBatch(ctx, opts...)
}

// Batch is group of ordered messages from one partition
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Batch = topicreaderinternal.PublicBatch

// ReadBatchOption
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type ReadBatchOption = topicreaderinternal.PublicReadBatchOption

// Close stop work with reader
// return when reader complete internal works, flush commit buffer, ets
// or when ctx cancelled
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (r *Reader) Close(ctx context.Context) error {
	if err := r.inCall(); err != nil {
		return err
	}
	defer r.outCall()

	return r.reader.Close(ctx)
}

func (r *Reader) inCall() error {
	if atomic.CompareAndSwapInt64(&r.inFlyght, 0, 1) {
		return nil
	}

	return ErrConcurrencyCall
}

func (r *Reader) outCall() {
	if atomic.CompareAndSwapInt64(&r.inFlyght, 1, 0) {
		return
	}
	panic("ydb: reader outcall without in call, must be never")
}
