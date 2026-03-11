package topicwritercommon

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type PublicWriterOption func(
	writerCfg *topicwriterinternal.WriterReconnectorConfig,
	multiWriterCfg *topicmultiwriter.MultiWriterConfig,
)
