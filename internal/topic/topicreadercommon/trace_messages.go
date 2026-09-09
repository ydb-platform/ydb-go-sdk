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
		readerInfo.ReaderName,
		readerInfo.Listener,
		messagesCount,
	)
}

// TraceMessagesDelivered emits a shared reader/listener message delivery event.
func TraceMessagesDelivered(
	ctx context.Context,
	tracer *trace.Topic,
	readerInfo ReaderInfo,
	topic string,
	messagesCount int,
) {
	gtrace.TopicOnReaderMessagesDelivered(
		tracer,
		&ctx,
		readerInfo.Endpoint,
		readerInfo.Database,
		topic,
		readerInfo.Consumer,
		readerInfo.ReaderName,
		readerInfo.Listener,
		messagesCount,
	)
}

// TraceLocalBufferChanged emits the owned-message balance delta for a topic
// reader or listener.
func TraceLocalBufferChanged(
	ctx context.Context,
	tracer *trace.Topic,
	readerInfo ReaderInfo,
	topic string,
	messagesDelta int,
) {
	gtrace.TopicOnReaderLocalBufferChanged(
		tracer,
		&ctx,
		readerInfo.Endpoint,
		readerInfo.Database,
		topic,
		readerInfo.Consumer,
		readerInfo.ReaderName,
		readerInfo.Listener,
		messagesDelta,
	)
}
