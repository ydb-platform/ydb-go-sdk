package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func topicCommitACKHook(l Logger, d trace.Detailer) func(trace.TopicReaderCommittedNotifyInfo) {
	const events = trace.TopicReaderStreamEvents
	// A non-nil ACK hook starts a worker for each reader or listener stream.
	if details, ok := d.(trace.Details); ok && details&events == 0 {
		return nil
	}

	return func(info trace.TopicReaderCommittedNotifyInfo) {
		if d.Details()&events == 0 {
			return
		}
		ctx := withFromPtr(info.Context, TRACE, "ydb", "topic", "reader", "committed", "notify")
		l.Log(ctx, "topic reader received commit ack",
			kv.String("reader_connection_id", info.ReaderConnectionID),
			kv.String("topic", info.Topic),
			kv.Int64("partition_id", info.PartitionID),
			kv.Int64("partition_session_id", info.PartitionSessionID),
			kv.Int64("committed_offset", info.CommittedOffset),
		)
	}
}
