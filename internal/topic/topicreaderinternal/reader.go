package topicreaderinternal

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errUnconnected  = xerrors.Retryable(xerrors.Wrap(errors.New("ydb: first connection attempt not finished")))
	ErrReaderClosed = xerrors.Wrap(errors.New("ydb: reader closed"))
)

const infiniteTimeout = time.Duration(math.MaxInt64)

//nolint:lll
//go:generate mockgen -destination raw_topic_reader_stream_mock_test.go -package topicreaderinternal -write_package_comment=false . RawTopicReaderStream

type RawTopicReaderStream interface {
	Recv() (rawtopicreader.ServerMessage, error)
	Send(msg rawtopicreader.ClientMessage) error
	CloseSend() error
}

// TopicSteamReaderConnect connect to grpc stream
// when connectionCtx closed stream must stop work and return errors for all methods
type TopicSteamReaderConnect func(connectionCtx context.Context) (RawTopicReaderStream, error)

type Reader struct {
	reader             batchedStreamReader
	defaultBatchConfig ReadMessageBatchOptions
	tracer             trace.Topic
}

type ReadMessageBatchOptions struct {
	batcherGetOptions
}

func (o ReadMessageBatchOptions) clone() ReadMessageBatchOptions {
	return o
}

func newReadMessageBatchOptions() ReadMessageBatchOptions {
	return ReadMessageBatchOptions{}
}

// PublicReadBatchOption для различных пожеланий к батчу вроде WithMaxMessages(int)
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicReadBatchOption interface {
	Apply(options ReadMessageBatchOptions) ReadMessageBatchOptions
}

// Apply
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (count readExplicitMessagesCount) Apply(options ReadMessageBatchOptions) ReadMessageBatchOptions {
	options.MinCount = int(count)
	options.MaxCount = int(count)
	return options
}

func NewReader(
	connector TopicSteamReaderConnect,
	consumer string,
	readSelectors []PublicReadSelector,
	opts ...PublicReaderOption,
) Reader {
	cfg := convertNewParamsToStreamConfig(consumer, readSelectors, opts...)
	readerConnector := func(ctx context.Context) (batchedStreamReader, error) {
		stream, err := connector(ctx)
		if err != nil {
			return nil, err
		}

		return newTopicStreamReader(stream, cfg.topicStreamReaderConfig)
	}

	res := Reader{
		reader:             newReaderReconnector(readerConnector, cfg.OperationTimeout(), cfg.Tracer, cfg.BaseContext),
		defaultBatchConfig: cfg.DefaultBatchConfig,
		tracer:             cfg.Tracer,
	}

	return res
}

func (r *Reader) Close(ctx context.Context) error {
	return r.reader.CloseWithError(ctx, xerrors.WithStackTrace(ErrReaderClosed))
}

type readExplicitMessagesCount int

// ReadMessage read exactly one message
func (r *Reader) ReadMessage(ctx context.Context) (*PublicMessage, error) {
	res, err := r.ReadMessageBatch(ctx, readExplicitMessagesCount(1))
	if err != nil {
		return nil, err
	}

	return res.Messages[0], nil
}

// ReadMessageBatch read batch of messages.
// Batch is collection of messages, which can be atomically committed
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...PublicReadBatchOption) (batch *PublicBatch, err error) {
	readOptions := r.defaultBatchConfig.clone()

	for _, optFunc := range opts {
		readOptions = optFunc.Apply(readOptions)
	}

forReadBatch:
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		batch, err = r.reader.ReadMessageBatch(ctx, readOptions)
		if err != nil {
			return nil, err
		}

		// if batch context is canceled - do not return it to client
		// and read next batch
		if batch.Context().Err() != nil {
			continue forReadBatch
		}
		return batch, nil
	}
}

func (r *Reader) Commit(ctx context.Context, offsets PublicCommitRangeGetter) (err error) {
	return r.reader.Commit(ctx, offsets.getCommitRange().priv)
}

func (r *Reader) CommitRanges(ctx context.Context, ranges []PublicCommitRange) error {
	commitRanges := NewCommitRangesFromPublicCommits(ranges)
	commitRanges.optimize()

	commitErrors := make(chan error, commitRanges.len())

	var wg sync.WaitGroup

	commit := func(cr commitRange) {
		defer wg.Done()
		commitErrors <- r.Commit(ctx, &cr)
	}

	wg.Add(commitRanges.len())
	for _, cr := range commitRanges.ranges {
		go commit(cr)
	}
	wg.Wait()
	close(commitErrors)

	// return first error
	for err := range commitErrors {
		if err != nil {
			return err
		}
	}

	return nil
}

type ReaderConfig struct {
	config.Common

	DefaultBatchConfig ReadMessageBatchOptions
	topicStreamReaderConfig

	reconnectionBackoff backoff.Backoff
}

// PublicReaderOption
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicReaderOption func(cfg *ReaderConfig)

func convertNewParamsToStreamConfig(
	consumer string,
	readSelectors []PublicReadSelector,
	opts ...PublicReaderOption,
) (cfg ReaderConfig) {
	cfg.topicStreamReaderConfig = newTopicStreamReaderConfig()
	cfg.Consumer = consumer
	cfg.reconnectionBackoff = backoff.Fast

	// make own copy, for prevent changing internal states if readSelectors will change outside
	cfg.ReadSelectors = make([]PublicReadSelector, len(readSelectors))
	for i := range readSelectors {
		cfg.ReadSelectors[i] = readSelectors[i].Clone()
	}

	for _, f := range opts {
		f(&cfg)
	}

	return cfg
}

// PublicReadSelector
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicReadSelector struct {
	Path       string
	Partitions []int64
	ReadFrom   time.Time     // zero value mean skip read from filter
	MaxTimeLag time.Duration // 0 mean skip time lag filter
}

// Clone create deep clone of the selector
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (s PublicReadSelector) Clone() PublicReadSelector {
	dst := s

	dst.Partitions = make([]int64, len(s.Partitions))
	copy(dst.Partitions, s.Partitions)

	return dst
}
