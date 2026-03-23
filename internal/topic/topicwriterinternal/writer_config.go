package topicwriterinternal

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type WritersCommonConfig struct {
	producerID          string
	topic               string
	writerMeta          map[string]string
	defaultPartitioning rawtopicwriter.Partitioning
	compressorCount     int
	maxBytesPerMessage  int

	LogContext         context.Context //nolint:containedctx
	Tracer             *trace.Topic
	rawTopicClient     *rawtopic.Client
	cred               credentials.Credentials
	credUpdateInterval time.Duration
	clock              clockwork.Clock
	forceCodec         rawtopiccommon.Codec
}

func (c *WritersCommonConfig) Topic() string {
	return c.topic
}

func (c *WritersCommonConfig) PartitionID() (int64, bool) {
	if c.defaultPartitioning.Type == rawtopicwriter.PartitioningPartitionID {
		return c.defaultPartitioning.PartitionID, true
	}

	return 0, false
}

func (c *WritersCommonConfig) ProducerID() string {
	return c.producerID
}
