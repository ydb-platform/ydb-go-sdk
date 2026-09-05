package topicreadercommon

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TraceMessagesReceived emits the shared reader/listener message reception trace event.
func TraceMessagesReceived(
	ctx context.Context,
	tracer *trace.Topic,
	readerInfo ReaderInfo,
	topic string,
	messagesCount int,
) {
	gtrace.TopicOnReaderMessagesReceived(
		tracer,
		&ctx,
		readerInfo.Endpoint,
		readerInfo.Database,
		topic,
		readerInfo.Consumer,
		messagesCount,
	)
}
