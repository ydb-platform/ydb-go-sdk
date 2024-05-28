package topicclientinternal

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Client struct {
	cfg                    topic.Config
	cred                   credentials.Credentials
	defaultOperationParams rawydb.OperationParams
	rawClient              rawtopic.Client
}

func New(
	ctx context.Context,
	conn grpc.ClientConnInterface,
	cred credentials.Credentials,
	opts ...topicoptions.TopicOption,
) *Client {
	rawClient := rawtopic.NewClient(Ydb_Topic_V1.NewTopicServiceClient(conn))

	cfg := newTopicConfig(opts...)

	var defaultOperationParams rawydb.OperationParams
	topic.OperationParamsFromConfig(&defaultOperationParams, &cfg.Common)

	return &Client{
		cfg:                    cfg,
		cred:                   cred,
		defaultOperationParams: defaultOperationParams,
		rawClient:              rawClient,
	}
}

func newTopicConfig(opts ...topicoptions.TopicOption) topic.Config {
	c := topic.Config{
		Common: config.Common{},
		Trace:  new(trace.Topic),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}

	return c
}

// Close the client
func (c *Client) Close(_ context.Context) error {
	return nil
}

// Alter topic options
func (c *Client) Alter(ctx context.Context, path string, opts ...topicoptions.AlterOption) error {
	req := new(rawtopic.AlterTopicRequest)
	req.OperationParams = c.defaultOperationParams
	req.Path = path
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyAlterOption(req)
		}
	}

	call := func(ctx context.Context) error {
		_, alterErr := c.rawClient.AlterTopic(ctx, req)

		return alterErr
	}

	if c.cfg.AutoRetry() {
		return retry.Retry(ctx, call,
			retry.WithIdempotent(true),
			retry.WithTrace(c.cfg.TraceRetry()),
			retry.WithBudget(c.cfg.RetryBudget()),
		)
	}

	return call(ctx)
}

// Create new topic
func (c *Client) Create(
	ctx context.Context,
	path string,
	opts ...topicoptions.CreateOption,
) error {
	req := new(rawtopic.CreateTopicRequest)
	req.OperationParams = c.defaultOperationParams
	req.Path = path

	for _, opt := range opts {
		if opt != nil {
			opt.ApplyCreateOption(req)
		}
	}

	call := func(ctx context.Context) error {
		_, createErr := c.rawClient.CreateTopic(ctx, req)

		return createErr
	}

	if c.cfg.AutoRetry() {
		return retry.Retry(ctx, call,
			retry.WithIdempotent(true),
			retry.WithTrace(c.cfg.TraceRetry()),
			retry.WithBudget(c.cfg.RetryBudget()),
		)
	}

	return call(ctx)
}

// Describe topic
func (c *Client) Describe(
	ctx context.Context,
	path string,
	opts ...topicoptions.DescribeOption,
) (res topictypes.TopicDescription, _ error) {
	req := rawtopic.DescribeTopicRequest{
		OperationParams: c.defaultOperationParams,
		Path:            path,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}

	var rawRes rawtopic.DescribeTopicResult

	call := func(ctx context.Context) (describeErr error) {
		rawRes, describeErr = c.rawClient.DescribeTopic(ctx, req)

		return describeErr
	}

	var err error

	if c.cfg.AutoRetry() {
		err = retry.Retry(ctx, call,
			retry.WithIdempotent(true),
			retry.WithTrace(c.cfg.TraceRetry()),
			retry.WithBudget(c.cfg.RetryBudget()),
		)
	} else {
		err = call(ctx)
	}

	if err != nil {
		return res, err
	}

	res.FromRaw(&rawRes)

	return res, nil
}

// Drop topic
func (c *Client) Drop(ctx context.Context, path string, opts ...topicoptions.DropOption) error {
	req := rawtopic.DropTopicRequest{
		OperationParams: rawydb.OperationParams{
			OperationMode:    rawydb.OperationParamsModeUnspecified,
			OperationTimeout: rawoptional.Duration{Value: time.Duration(0), HasValue: false},
			CancelAfter:      rawoptional.Duration{Value: time.Duration(0), HasValue: false},
		},
		Path: "",
	}
	req.OperationParams = c.defaultOperationParams
	req.Path = path

	for _, opt := range opts {
		if opt != nil {
			opt.ApplyDropOption(&req)
		}
	}

	call := func(ctx context.Context) error {
		_, removeErr := c.rawClient.DropTopic(ctx, req)

		return removeErr
	}

	if c.cfg.AutoRetry() {
		return retry.Retry(ctx, call,
			retry.WithIdempotent(true),
			retry.WithTrace(c.cfg.TraceRetry()),
			retry.WithBudget(c.cfg.RetryBudget()),
		)
	}

	return call(ctx)
}

// StartReader create new topic reader and start pull messages from server
// it is fast non block call, connection will start in background
func (c *Client) StartReader(
	consumer string,
	readSelectors topicoptions.ReadSelectors,
	opts ...topicoptions.ReaderOption,
) (*topicreader.Reader, error) {
	var connector topicreaderinternal.TopicSteamReaderConnect = func(ctx context.Context) (
		topicreaderinternal.RawTopicReaderStream, error,
	) {
		return c.rawClient.StreamRead(ctx)
	}

	defaultOpts := []topicoptions.ReaderOption{
		topicoptions.WithCommonConfig(c.cfg.Common),
		topicreaderinternal.WithCredentials(c.cred),
		topicreaderinternal.WithTrace(c.cfg.Trace),
		topicoptions.WithReaderStartTimeout(topic.DefaultStartTimeout),
	}
	opts = append(defaultOpts, opts...)

	internalReader, err := topicreaderinternal.NewReader(connector, consumer, readSelectors, opts...)
	if err != nil {
		return nil, err
	}
	trace.TopicOnReaderStart(internalReader.Tracer(), internalReader.ID(), consumer, err)

	return topicreader.NewReader(internalReader), nil
}

// StartWriter create new topic writer wrapper
func (c *Client) StartWriter(topicPath string, opts ...topicoptions.WriterOption) (*topicwriter.Writer, error) {
	var connector topicwriterinternal.ConnectFunc = func(ctx context.Context) (
		topicwriterinternal.RawTopicWriterStream,
		error,
	) {
		return c.rawClient.StreamWrite(ctx)
	}

	options := []topicoptions.WriterOption{
		topicwriterinternal.WithConnectFunc(connector),
		topicwriterinternal.WithTopic(topicPath),
		topicwriterinternal.WithCommonConfig(c.cfg.Common),
		topicwriterinternal.WithTrace(c.cfg.Trace),
	}

	options = append(options, opts...)

	writer, err := topicwriterinternal.NewWriter(c.cred, options)
	if err != nil {
		return nil, err
	}

	return topicwriter.NewWriter(writer), nil
}
