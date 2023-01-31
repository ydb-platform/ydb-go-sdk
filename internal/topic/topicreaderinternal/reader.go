package topicreaderinternal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/clone"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errUnconnected = xerrors.Retryable(xerrors.Wrap(
		errors.New("ydb: first connection attempt not finished"),
	))
	errReaderClosed                 = xerrors.Wrap(errors.New("ydb: reader closed"))
	errCommitSessionFromOtherReader = xerrors.Wrap(errors.New("ydb: commit with session from other reader"))
)

var globalReaderCounter int64

func nextReaderID() int64 {
	return atomic.AddInt64(&globalReaderCounter, 1)
}

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
	readerID           int64
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
	readerID := nextReaderID()

	readerConnector := func(ctx context.Context) (batchedStreamReader, error) {
		stream, err := connector(ctx)
		if err != nil {
			return nil, err
		}

		return newTopicStreamReader(readerID, stream, cfg.topicStreamReaderConfig)
	}

	res := Reader{
		reader: newReaderReconnector(
			readerID,
			readerConnector,
			cfg.OperationTimeout(),
			cfg.RetrySettings,
			cfg.Tracer,
			cfg.BaseContext,
		),
		defaultBatchConfig: cfg.DefaultBatchConfig,
		tracer:             cfg.Tracer,
		readerID:           readerID,
	}

	return res
}

func (r *Reader) Close(ctx context.Context) error {
	return r.reader.CloseWithError(ctx, xerrors.WithStackTrace(errReaderClosed))
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

	for _, opt := range opts {
		if opt != nil {
			readOptions = opt.Apply(readOptions)
		}
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
	cr := offsets.getCommitRange().priv
	if cr.partitionSession.readerID != r.readerID {
		return errCommitSessionFromOtherReader
	}

	return r.reader.Commit(ctx, cr)
}

func (r *Reader) CommitRanges(ctx context.Context, ranges []PublicCommitRange) error {
	for i := range ranges {
		if ranges[i].priv.partitionSession.readerID != r.readerID {
			return errCommitSessionFromOtherReader
		}
	}

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

	RetrySettings      topic.RetrySettings
	DefaultBatchConfig ReadMessageBatchOptions
	topicStreamReaderConfig
}

// PublicReaderOption
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicReaderOption func(cfg *ReaderConfig)

func WithCredentials(cred credentials.Credentials) PublicReaderOption {
	return func(cfg *ReaderConfig) {
		if cred == nil {
			cred = credentials.NewAnonymousCredentials()
		}
		cfg.Cred = cred
	}
}

func WithTrace(tracer trace.Topic) PublicReaderOption {
	return func(cfg *ReaderConfig) {
		cfg.Tracer = cfg.Tracer.Compose(tracer)
	}
}

func convertNewParamsToStreamConfig(
	consumer string,
	readSelectors []PublicReadSelector,
	opts ...PublicReaderOption,
) (cfg ReaderConfig) {
	cfg.topicStreamReaderConfig = newTopicStreamReaderConfig()
	cfg.Consumer = consumer

	// make own copy, for prevent changing internal states if readSelectors will change outside
	cfg.ReadSelectors = make([]PublicReadSelector, len(readSelectors))
	for i := range readSelectors {
		cfg.ReadSelectors[i] = readSelectors[i].Clone()
	}

	for _, f := range opts {
		if f != nil {
			f(&cfg)
		}
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
	dst.Partitions = clone.Int64Slice(s.Partitions)

	return dst
}
