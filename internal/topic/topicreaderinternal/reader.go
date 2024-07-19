package topicreaderinternal

import (
	"context"
	"errors"
	"fmt"
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
	errSetConsumerAndNoConsumer     = xerrors.Wrap(errors.New("ydb: reader has non empty consumer name and set option WithReaderWithoutConsumer. Only one of them must be set")) //nolint:lll
	errCommitSessionFromOtherReader = xerrors.Wrap(errors.New("ydb: commit with session from other reader"))
)

var globalReaderCounter int64

func nextReaderID() int64 {
	return atomic.AddInt64(&globalReaderCounter, 1)
}

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
	tracer             *trace.Topic
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
type PublicReadBatchOption interface {
	Apply(options ReadMessageBatchOptions) ReadMessageBatchOptions
}

type readExplicitMessagesCount int

// Apply implements PublicReadBatchOption
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
) (Reader, error) {
	cfg := convertNewParamsToStreamConfig(consumer, readSelectors, opts...)

	if errs := cfg.Validate(); len(errs) > 0 {
		return Reader{}, xerrors.WithStackTrace(fmt.Errorf(
			"ydb: failed to start topic reader, because is contains error in config: %w",
			errors.Join(errs...),
		))
	}

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
			cfg.Trace,
		),
		defaultBatchConfig: cfg.DefaultBatchConfig,
		tracer:             cfg.Trace,
		readerID:           readerID,
	}

	return res, nil
}

func (r *Reader) WaitInit(ctx context.Context) error {
	return r.reader.WaitInit(ctx)
}

func (r *Reader) ID() int64 {
	return r.readerID
}

func (r *Reader) Tracer() *trace.Topic {
	return r.tracer
}

func (r *Reader) Close(ctx context.Context) error {
	return r.reader.CloseWithError(ctx, xerrors.WithStackTrace(errReaderClosed))
}

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
		if batch.Context().Err() == nil {
			return batch, nil
		}
	}
}

func (r *Reader) Commit(ctx context.Context, offsets PublicCommitRangeGetter) (err error) {
	cr := offsets.getCommitRange().priv
	if cr.partitionSession.readerID != r.readerID {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: messages session reader id (%v) != current reader id (%v): %w",
			cr.partitionSession.readerID, r.readerID, errCommitSessionFromOtherReader,
		)))
	}

	return r.reader.Commit(ctx, cr)
}

func (r *Reader) CommitRanges(ctx context.Context, ranges []PublicCommitRange) error {
	for i := range ranges {
		if ranges[i].priv.partitionSession.readerID != r.readerID {
			return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
				"ydb: commit ranges (range item %v) "+
					"messages session reader id (%v) != current reader id (%v): %w",
				i, ranges[i].priv.partitionSession.readerID, r.readerID, errCommitSessionFromOtherReader,
			)))
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

type PublicReaderOption func(cfg *ReaderConfig)

func WithCredentials(cred credentials.Credentials) PublicReaderOption {
	return func(cfg *ReaderConfig) {
		if cred == nil {
			cred = credentials.NewAnonymousCredentials()
		}
		cfg.Cred = cred
	}
}

func WithTrace(tracer *trace.Topic) PublicReaderOption {
	return func(cfg *ReaderConfig) {
		cfg.Trace = cfg.Trace.Compose(tracer)
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
	cfg.ReadSelectors = make([]*PublicReadSelector, len(readSelectors))
	for i := range readSelectors {
		cfg.ReadSelectors[i] = readSelectors[i].Clone()
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return cfg
}

type PublicReadSelector struct {
	Path       string
	Partitions []int64
	ReadFrom   time.Time     // zero value mean skip read from filter
	MaxTimeLag time.Duration // 0 mean skip time lag filter
}

// Clone create deep clone of the selector
func (s PublicReadSelector) Clone() *PublicReadSelector { //nolint:gocritic
	s.Partitions = clone.Int64Slice(s.Partitions)

	return &s
}
