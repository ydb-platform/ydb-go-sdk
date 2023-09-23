package topicreader

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
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
type Reader struct {
	reader         topicreaderinternal.Reader
	readInFlyght   xatomic.Bool
	commitInFlyght xatomic.Bool
}

// NewReader
// create new reader, used internally only.
func NewReader(internalReader topicreaderinternal.Reader) *Reader {
	return &Reader{reader: internalReader}
}

// WaitInit waits until the reader is initialized
// or an error occurs
func (r *Reader) WaitInit(ctx context.Context) error {
	return r.reader.WaitInit(ctx)
}

// ReadMessage read exactly one message
// exactly one of message, error is nil
func (r *Reader) ReadMessage(ctx context.Context) (*Message, error) {
	if err := r.inCall(&r.readInFlyght); err != nil {
		return nil, err
	}
	defer r.outCall(&r.readInFlyght)

	return r.reader.ReadMessage(ctx)
}

// Message contains data and metadata, readed from the server
type Message = topicreaderinternal.PublicMessage

// MessageContentUnmarshaler is interface for unmarshal message content to own struct
type MessageContentUnmarshaler = topicreaderinternal.PublicMessageContentUnmarshaler

// Commit receive Message, Batch of single offset
// It can be fast (by default) or sync and waite response from server
// see topicoptions.CommitMode for details
//
// for topicoptions.CommitModeSync mode sync the method can return ErrCommitToExpiredSession
// it means about the message/batch was not committed because connection broken or partition routed to
// other reader by server.
// Client code should continue work normally
func (r *Reader) Commit(ctx context.Context, obj CommitRangeGetter) error {
	if err := r.inCall(&r.commitInFlyght); err != nil {
		return err
	}
	defer r.outCall(&r.commitInFlyght)

	return r.reader.Commit(ctx, obj)
}

// CommitRangeGetter interface for get commit offsets
type CommitRangeGetter = topicreaderinternal.PublicCommitRangeGetter

// ReadMessageBatch
// Deprecated: (was experimental) will be removed soon.
// Use ReadMessagesBatch instead.
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...ReadBatchOption) (*Batch, error) {
	if err := r.inCall(&r.readInFlyght); err != nil {
		return nil, err
	}
	defer r.outCall(&r.readInFlyght)

	return r.reader.ReadMessageBatch(ctx, opts...)
}

// ReadMessagesBatch read batch of messages
// Batch is ordered message group from one partition
// exactly one of Batch, err is nil
// if Batch is not nil - reader guarantee about all Batch.Messages are not nil
func (r *Reader) ReadMessagesBatch(ctx context.Context, opts ...ReadBatchOption) (*Batch, error) {
	if err := r.inCall(&r.readInFlyght); err != nil {
		return nil, err
	}
	defer r.outCall(&r.readInFlyght)

	return r.reader.ReadMessageBatch(ctx, opts...)
}

// Batch is ordered group of messages from one partition
type Batch = topicreaderinternal.PublicBatch

// ReadBatchOption is type for options of read batch
type ReadBatchOption = topicreaderinternal.PublicReadBatchOption

// Close stop work with reader
// return when reader complete internal works, flush commit buffer, ets
// or when ctx cancelled
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

func (r *Reader) inCall(inFlight *xatomic.Bool) error {
	if inFlight.CompareAndSwap(false, true) {
		return nil
	}

	return xerrors.WithStackTrace(ErrConcurrencyCall)
}

func (r *Reader) outCall(inFlight *xatomic.Bool) {
	if inFlight.CompareAndSwap(true, false) {
		return
	}

	panic("ydb: topic reader out call without in call, must be never")
}
