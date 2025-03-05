package topicreadernaive

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicclientinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	defaultBufferSize = 50 * 1024 * 1024 // 50MB
	reconnectPause    = time.Second
)

// NaiveReader naive implementation of topic reader, it needs for debug only and WILL be removed
// in few next releases. DENIED to use in production.
//
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
type NaiveReader struct {
	cred               credentials.Credentials
	topicSelectors     []*topicreadercommon.PublicReadSelector
	consumer           string
	commitMode         topicreadercommon.PublicCommitMode
	grpcClient         Ydb_Topic_V1.TopicServiceClient
	lifeContext        context.Context //nolint:containedctx
	cancelFunc         context.CancelCauseFunc
	decoderMap         topicreadercommon.DecoderMap
	tracer             *trace.Topic
	lastReconnectError xsync.Value[error]

	m            xsync.Mutex
	streamReader *topicNaiveStreamReader
	lastBatch    *topicreadercommon.PublicBatch
}

// NewNaiveReader create NaiveReader
//
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func NewNaiveReader(
	db *ydb.Driver,
	consumer string,
	topic topicoptions.ReadSelectors,
	opts ...topicoptions.ReaderOption,
) *NaiveReader {
	topicClient := db.Topic().(*topicclientinternal.Client) //nolint:forcetypeassert

	ctx, cancel := context.WithCancelCause(context.Background())

	defaultOpts := []topicoptions.ReaderOption{
		topicreaderinternal.WithTrace(topicclientinternal.GetTracer(topicClient)),
	}

	cfg := topicreaderinternal.TmpPublicConvertNewParamsToStreamConfig(consumer, topic, append(defaultOpts, opts...)...)

	res := &NaiveReader{
		cred:           topicclientinternal.GetCred(topicClient),
		topicSelectors: cfg.ReadSelectors,
		consumer:       consumer,
		commitMode:     cfg.CommitMode,
		grpcClient:     Ydb_Topic_V1.NewTopicServiceClient(ydb.GRPCConn(db)),
		lifeContext:    ctx,
		cancelFunc:     cancel,
		decoderMap:     cfg.Decoders,
		tracer:         cfg.Trace,
	}

	return res
}

func (r *NaiveReader) ReadMessage(ctx context.Context) (mess *topicreader.Message, resErr error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	cutMessageLocked := func() {
		if r.lastBatch == nil {
			return
		}

		head, rest := topicreadercommon.BatchCutMessages(r.lastBatch, 1)
		mess = head.Messages[0]
		r.lastBatch = rest
	}

	r.m.WithLock(func() {
		cutMessageLocked()
	})
	if mess != nil {
		return mess, nil
	}

	batch, err := r.ReadMessageBatch(ctx)
	if err != nil {
		return nil, err
	}

	r.m.WithLock(func() {
		r.lastBatch = batch
		cutMessageLocked()
	})

	return mess, nil
}

// ReadMessageBatch Read a batch of messages
//
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func (r *NaiveReader) ReadMessageBatch(ctx context.Context) (batch *topicreader.Batch, resErr error) {
	traceCtx := ctx
	onDone := trace.TopicOnReaderReadMessages(r.tracer, &traceCtx, -1, -1, -1)
	defer func() {
		if batch == nil {
			onDone(0, "", -1, -1, -1, -1, -1, resErr)
		} else {
			cr := topicreadercommon.GetCommitRange(batch)
			onDone(
				len(batch.Messages),
				batch.Topic(),
				batch.PartitionID(),
				topicreadercommon.BatchGetPartitionSession(batch).StreamPartitionSessionID.ToInt64(),
				cr.CommitOffsetStart.ToInt64(),
				cr.CommitOffsetEnd.ToInt64(),
				-1,
				resErr,
			)
		}
	}()
	ctx = traceCtx
	attempt := 0

readLoop:
	for {
		attempt++
		if attempt > 1 {
			time.Sleep(reconnectPause)
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var lastBatch *topicreadercommon.PublicBatch
		r.m.WithLock(func() {
			lastBatch = r.lastBatch
			r.lastBatch = nil
		})

		if lastBatch != nil {
			return lastBatch, nil
		}

		stream, err := r.getStream()
		if err != nil {
			continue readLoop
		}

		batch, err := stream.ReadBatch(ctx)
		if err != nil {
			_ = stream.Close(fmt.Errorf("close naive stream reader because error: %w", err))
			r.m.WithLock(func() {
				r.streamReader = nil
			})

			continue
		}

		return batch, nil
	}
}

func (r *NaiveReader) getStream() (*topicNaiveStreamReader, error) {
	r.m.Lock()
	defer r.m.Unlock()

	if r.streamReader == nil {
		onReconnectDone := trace.TopicOnReaderReconnect(r.tracer, r.lastReconnectError.Get())

		var err error
		r.streamReader, err = newTopicNaiveStreamReader(
			r.cred,
			r.grpcClient,
			r.topicSelectors,
			r.consumer,
			defaultBufferSize,
			r.decoderMap,
			r.tracer,
		)
		onReconnectDone(err)
		if err != nil {
			return nil, err
		}
	}

	return r.streamReader, nil
}

// Commit batch
//
// Internals: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#internals
func (r *NaiveReader) Commit(ctx context.Context, cr topicreadercommon.PublicCommitRangeGetter) error {
	stream, err := r.getStream()
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb naive reader error on get stream for commit: %w", err))
	}

	return stream.Commit(ctx, topicreadercommon.GetCommitRange(cr))
}

func (r *NaiveReader) Close() error {
	stream, err := r.getStream()
	if err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb naive reader error on get stream for close: %w", err))
	}

	r.cancelFunc(xerrors.WithStackTrace(errors.New("ydb naive reader cancel life context")))

	return stream.Close(fmt.Errorf("ydb customer call naive reader close"))
}
