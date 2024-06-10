package topicwriterinternal

import (
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type WritersCommonConfig struct {
	producerID          string
	topic               string
	writerMeta          map[string]string
	defaultPartitioning rawtopicwriter.Partitioning
	compressorCount     int

	tracer             *trace.Topic
	cred               credentials.Credentials
	credUpdateInterval time.Duration
	clock              clockwork.Clock
}
