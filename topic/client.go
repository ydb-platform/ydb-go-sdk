package topic

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// Client is interface for topic client
// Attention: the interface may be extended in the future.
type Client interface {
	// Alter change topic options
	Alter(ctx context.Context, path string, opts ...topicoptions.AlterOption) error

	// Create topic
	Create(ctx context.Context, path string, opts ...topicoptions.CreateOption) error

	// Describe topic
	Describe(ctx context.Context, path string, opts ...topicoptions.DescribeOption) (topictypes.TopicDescription, error)

	// Drop topic
	Drop(ctx context.Context, path string, opts ...topicoptions.DropOption) error

	// StartReader start read messages from topic
	// it is fast non block call, connection starts in background
	StartReader(
		consumer string,
		readSelectors topicoptions.ReadSelectors,
		opts ...topicoptions.ReaderOption,
	) (*topicreader.Reader, error)

	// StartWriter start write session to topic
	// it is fast non block call, connection starts in background
	StartWriter(topicPath string, opts ...topicoptions.WriterOption) (*topicwriter.Writer, error)
}
