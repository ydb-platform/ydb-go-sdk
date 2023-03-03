package topic

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// Client is interface for topic client
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Client interface {
	// Alter change topic options
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	Alter(ctx context.Context, path string, opts ...topicoptions.AlterOption) error

	// Create create topic
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	Create(ctx context.Context, path string, opts ...topicoptions.CreateOption) error

	// Describe describe topic
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	Describe(ctx context.Context, path string, opts ...topicoptions.DescribeOption) (topictypes.TopicDescription, error)

	// Drop drop topic
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	Drop(ctx context.Context, path string, opts ...topicoptions.DropOption) error

	// StartReader start read messages from topic
	// it is fast non block call, connection starts in background
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	StartReader(
		consumer string,
		readSelectors topicoptions.ReadSelectors,
		opts ...topicoptions.ReaderOption,
	) (*topicreader.Reader, error)

	// StartWriter start write session to topic
	// it is fast non block call, connection starts in background
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	StartWriter(topicPath string, opts ...topicoptions.WriterOption) (*topicwriter.Writer, error)
}
