package topicclientinternal

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type Client struct {
	cfg                    topic.Config
	defaultOperationParams rawydb.OperationParams
	rawClient              rawtopic.Client
}

func New(conn grpc.ClientConnInterface, opts ...topicoptions.TopicOption) *Client {
	rawClient := rawtopic.NewClient(Ydb_Topic_V1.NewTopicServiceClient(conn))

	cfg := newTopicConfig(opts...)

	var defaultOperationParams rawydb.OperationParams
	topic.OperationParamsFromConfig(&defaultOperationParams, &cfg.Common)

	return &Client{
		cfg:                    cfg,
		defaultOperationParams: defaultOperationParams,
		rawClient:              rawClient,
	}
}

func newTopicConfig(opts ...topicoptions.TopicOption) topic.Config {
	c := topic.Config{}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// Close
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (c *Client) Close(_ context.Context) error {
	return nil
}

// Alter topic options
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (c *Client) Alter(ctx context.Context, path string, opts ...topicoptions.AlterOption) error {
	req := rawtopic.AlterTopicRequest{}
	req.OperationParams = c.defaultOperationParams
	req.Path = path
	for _, f := range opts {
		f(&req)
	}
	_, err := c.rawClient.AlterTopic(ctx, req)
	return err
}

// Create new topic
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (c *Client) Create(
	ctx context.Context,
	path string,
	codecs []topictypes.Codec,
	opts ...topicoptions.CreateOption,
) error {
	req := rawtopic.CreateTopicRequest{}
	req.OperationParams = c.defaultOperationParams
	req.Path = path

	req.SupportedCodecs = make(rawtopiccommon.SupportedCodecs, len(codecs))
	for i, codec := range codecs {
		req.SupportedCodecs[i] = rawtopiccommon.Codec(codec)
	}

	for _, f := range opts {
		f(&req)
	}

	_, err := c.rawClient.CreateTopic(ctx, req)
	return err
}

// Describe topic
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
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
		opt(&req)
	}

	rawRes, err := c.rawClient.DescribeTopic(ctx, req)
	if err != nil {
		return res, err
	}

	res.FromRaw(&rawRes)
	return res, nil
}

// Drop topic
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (c *Client) Drop(ctx context.Context, path string, opts ...topicoptions.DropOption) error {
	req := rawtopic.DropTopicRequest{}
	req.OperationParams = c.defaultOperationParams
	req.Path = path

	for _, f := range opts {
		f(&req)
	}
	_, err := c.rawClient.DropTopic(ctx, req)
	return err
}

// StartReader create new topic reader and start pull messages from server
// it is fast non block call, connection will start in background
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
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

	defaultOpts := []topicoptions.ReaderOption{topicoptions.WithCommonConfig(c.cfg.Common)}
	opts = append(defaultOpts, opts...)

	internalReader := topicreaderinternal.NewReader(connector, consumer, readSelectors, opts...)
	return topicreader.NewReader(internalReader), nil
}

// StartWriter create new topic writer wrapper
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (c *Client) StartWriter(producerID, path string, opts ...topicoptions.WriterOption) (*topicwriter.Writer, error) {
	// TODO: Implement handle arguments

	var connector topicwriterinternal.ConnectFunc = func(ctx context.Context) (topicwriterinternal.RawTopicWriterStream, error) {
		return c.rawClient.StreamWrite(ctx)
	}

	partitioning := rawtopicwriter.NewPartitioningMessageGroup(producerID)

	writer := topicwriterinternal.NewWriter(connector, producerID, path, nil, partitioning)
	return topicwriter.NewWriter(writer), nil
}
