//nolint:lll
package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Topic returns trace.Topic with logging events from details
func Topic(l Logger, d trace.Detailer, opts ...Option) (t trace.Topic) { //nolint:gocyclo
	if ll, has := l.(*logger); has {
		return internalTopic(ll.with(opts...), d)
	}
	return internalTopic(New(append(opts, withExternalLogger(l))...), d)
}

//nolint:gocyclo
func internalTopic(l *logger, d trace.Detailer) (t trace.Topic) {
	t.OnReaderReconnect = func(info trace.TopicReaderReconnectStartInfo) func(doneInfo trace.TopicReaderReconnectDoneInfo) {
		if d.Details()&trace.TopicReaderStreamLifeCycleEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "reconnect"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start")
		return func(doneInfo trace.TopicReaderReconnectDoneInfo) {
			l.Log(params.withLevel(INFO), "reconnected",
				latency(start),
			)
		}
	}
	t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
		if d.Details()&trace.TopicReaderStreamLifeCycleEvents == 0 {
			return
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "reconnect", "request"},
		}
		l.Log(params.withLevel(DEBUG), "start",
			NamedError("reason", info.Reason),
			Bool("was_sent", info.WasSent),
		)
	}
	t.OnReaderPartitionReadStartResponse = func(info trace.TopicReaderPartitionReadStartResponseStartInfo) func(stopInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
		if d.Details()&trace.TopicReaderPartitionEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "partition", "read", "start", "response"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			String("topic", info.Topic),
			String("reader_connection_id", info.ReaderConnectionID),
			Int64("partition_id", info.PartitionID),
			Int64("partition_session_id", info.PartitionSessionID),
		)
		return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(params.withLevel(INFO), "read partition response completed",
					String("topic", info.Topic),
					String("reader_connection_id", info.ReaderConnectionID),
					Int64("partition_id", info.PartitionID),
					Int64("partition_session_id", info.PartitionSessionID),
					Int64("commit_offset", *doneInfo.CommitOffset),
					Int64("read_offset", *doneInfo.ReadOffset),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(INFO), "read partition response completed",
					Error(doneInfo.Error),
					String("topic", info.Topic),
					String("reader_connection_id", info.ReaderConnectionID),
					Int64("partition_id", info.PartitionID),
					Int64("partition_session_id", info.PartitionSessionID),
					Int64("commit_offset", *doneInfo.CommitOffset),
					Int64("read_offset", *doneInfo.ReadOffset),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderPartitionReadStopResponse = func(info trace.TopicReaderPartitionReadStopResponseStartInfo) func(trace.TopicReaderPartitionReadStopResponseDoneInfo) {
		if d.Details()&trace.TopicReaderPartitionEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "partition", "read", "stop", "response"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			String("reader_connection_id", info.ReaderConnectionID),
			String("topic", info.Topic),
			Int64("partition_id", info.PartitionID),
			Int64("partition_session_id", info.PartitionSessionID),
			Int64("committed_offset", info.CommittedOffset),
			Bool("graceful", info.Graceful))
		return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(params.withLevel(INFO), "reader partition stopped",
					Error(doneInfo.Error),
					String("reader_connection_id", info.ReaderConnectionID),
					String("topic", info.Topic),
					Int64("partition_id", info.PartitionID),
					Int64("partition_session_id", info.PartitionSessionID),
					Int64("committed_offset", info.CommittedOffset),
					Bool("graceful", info.Graceful),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(INFO), "reader partition stopped",
					String("reader_connection_id", info.ReaderConnectionID),
					String("topic", info.Topic),
					Int64("partition_id", info.PartitionID),
					Int64("partition_session_id", info.PartitionSessionID),
					Int64("committed_offset", info.CommittedOffset),
					Bool("graceful", info.Graceful),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderCommit = func(info trace.TopicReaderCommitStartInfo) func(doneInfo trace.TopicReaderCommitDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "commit"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			String("topic", info.Topic),
			Int64("partition_id", info.PartitionID),
			Int64("partition_session_id", info.PartitionSessionID),
			Int64("commit_start_offset", info.StartOffset),
			Int64("commit_end_offset", info.EndOffset),
		)
		return func(doneInfo trace.TopicReaderCommitDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(params.withLevel(DEBUG), "committed",
					String("topic", info.Topic),
					Int64("partition_id", info.PartitionID),
					Int64("partition_session_id", info.PartitionSessionID),
					Int64("commit_start_offset", info.StartOffset),
					Int64("commit_end_offset", info.EndOffset),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(INFO), "committed",
					Error(doneInfo.Error),
					String("topic", info.Topic),
					Int64("partition_id", info.PartitionID),
					Int64("partition_session_id", info.PartitionSessionID),
					Int64("commit_start_offset", info.StartOffset),
					Int64("commit_end_offset", info.EndOffset),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderSendCommitMessage = func(info trace.TopicReaderSendCommitMessageStartInfo) func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "send", "commit", "message"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			Any("partitions_id", info.CommitsInfo.PartitionIDs()),
			Any("partitions_session_id", info.CommitsInfo.PartitionSessionIDs()),
		)
		return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(params.withLevel(DEBUG), "done",
					Any("partitions_id", info.CommitsInfo.PartitionIDs()),
					Any("partitions_session_id", info.CommitsInfo.PartitionSessionIDs()),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(INFO), "commit message sent",
					Error(doneInfo.Error),
					Any("partitions_id", info.CommitsInfo.PartitionIDs()),
					Any("partitions_session_id", info.CommitsInfo.PartitionSessionIDs()),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "committed", "notify"},
		}
		l.Log(params.withLevel(DEBUG), "ack",
			String("reader_connection_id", info.ReaderConnectionID),
			String("topic", info.Topic),
			Int64("partition_id", info.PartitionID),
			Int64("partition_session_id", info.PartitionSessionID),
			Int64("committed_offset", info.CommittedOffset),
		)
	}
	t.OnReaderClose = func(info trace.TopicReaderCloseStartInfo) func(doneInfo trace.TopicReaderCloseDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "close"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "done",
			String("reader_connection_id", info.ReaderConnectionID),
			NamedError("close_reason", info.CloseReason),
		)
		return func(doneInfo trace.TopicReaderCloseDoneInfo) {
			if doneInfo.CloseError != nil {
				l.Log(params.withLevel(DEBUG), "closed",
					String("reader_connection_id", info.ReaderConnectionID),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(DEBUG), "closed",
					Error(doneInfo.CloseError),
					String("reader_connection_id", info.ReaderConnectionID),
					NamedError("close_reason", info.CloseReason),
					latency(start),
					version(),
				)
			}
		}
	}

	t.OnReaderInit = func(info trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "init"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			String("pre_init_reader_connection_id", info.PreInitReaderConnectionID),
			String("consumer", info.InitRequestInfo.GetConsumer()),
			Strings("topics", info.InitRequestInfo.GetTopics()),
		)
		return func(doneInfo trace.TopicReaderInitDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(params.withLevel(DEBUG), "topic reader stream initialized",
					String("pre_init_reader_connection_id", info.PreInitReaderConnectionID),
					String("consumer", info.InitRequestInfo.GetConsumer()),
					Strings("topics", info.InitRequestInfo.GetTopics()),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(DEBUG), "topic reader stream initialized",
					Error(doneInfo.Error),
					String("pre_init_reader_connection_id", info.PreInitReaderConnectionID),
					String("consumer", info.InitRequestInfo.GetConsumer()),
					Strings("topics", info.InitRequestInfo.GetTopics()),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderError = func(info trace.TopicReaderErrorInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "error"},
		}
		l.Log(params.withLevel(INFO), "stream error",
			Error(info.Error),
			String("reader_connection_id", info.ReaderConnectionID),
			version(),
		)
	}
	t.OnReaderUpdateToken = func(info trace.OnReadUpdateTokenStartInfo) func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "update", "token"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "token updating...",
			String("reader_connection_id", info.ReaderConnectionID),
		)
		return func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			if updateTokenInfo.Error == nil {
				l.Log(params.withLevel(DEBUG), "got token",
					String("reader_connection_id", info.ReaderConnectionID),
					Int("token_len", updateTokenInfo.TokenLen),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(DEBUG), "got token",
					Error(updateTokenInfo.Error),
					String("reader_connection_id", info.ReaderConnectionID),
					Int("token_len", updateTokenInfo.TokenLen),
					latency(start),
					version(),
				)
			}
			return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
				if doneInfo.Error == nil {
					l.Log(params.withLevel(DEBUG), "token updated on stream",
						String("reader_connection_id", info.ReaderConnectionID),
						Int("token_len", updateTokenInfo.TokenLen),
						latency(start),
					)
				} else {
					l.Log(params.withLevel(DEBUG), "token updated on stream",
						Error(doneInfo.Error),
						String("reader_connection_id", info.ReaderConnectionID),
						Int("token_len", updateTokenInfo.TokenLen),
						latency(start),
						version(),
					)
				}
			}
		}
	}
	t.OnReaderSentDataRequest = func(info trace.TopicReaderSentDataRequestInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "sent", "data", "request"},
		}
		l.Log(params.withLevel(DEBUG), "sent data request",
			String("reader_connection_id", info.ReaderConnectionID),
			Int("request_bytes", info.RequestBytes),
			Int("local_capacity", info.LocalBufferSizeAfterSent),
		)
	}
	t.OnReaderReceiveDataResponse = func(startInfo trace.TopicReaderReceiveDataResponseStartInfo) func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "receive", "data", "response"},
		}
		start := time.Now()
		partitionsCount, batchesCount, messagesCount := startInfo.DataResponse.GetPartitionBatchMessagesCounts()
		l.Log(params.withLevel(DEBUG), "data response received, process starting...",
			String("reader_connection_id", startInfo.ReaderConnectionID),
			Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
			Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
			Int("partitions_count", partitionsCount),
			Int("batches_count", batchesCount),
			Int("messages_count", messagesCount),
		)
		return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(params.withLevel(DEBUG), "data response received and processed",
					String("reader_connection_id", startInfo.ReaderConnectionID),
					Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
					Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
					Int("partitions_count", partitionsCount),
					Int("batches_count", batchesCount),
					Int("messages_count", messagesCount),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(DEBUG), "data response received and processed",
					Error(doneInfo.Error),
					String("reader_connection_id", startInfo.ReaderConnectionID),
					Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
					Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
					Int("partitions_count", partitionsCount),
					Int("batches_count", batchesCount),
					Int("messages_count", messagesCount),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderReadMessages = func(info trace.TopicReaderReadMessagesStartInfo) func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "read", "messages"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "read messages called, waiting...",
			Int("min_count", info.MinCount),
			Int("max_count", info.MaxCount),
			Int("local_capacity_before", info.FreeBufferCapacity),
		)
		return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(params.withLevel(DEBUG), "read messages returned",
					Int("min_count", info.MinCount),
					Int("max_count", info.MaxCount),
					Int("local_capacity_before", info.FreeBufferCapacity),
					latency(start),
				)
			} else {
				l.Log(params.withLevel(DEBUG), "read messages returned",
					Error(doneInfo.Error),
					Int("min_count", info.MinCount),
					Int("max_count", info.MaxCount),
					Int("local_capacity_before", info.FreeBufferCapacity),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderUnknownGrpcMessage = func(info trace.OnReadUnknownGrpcMessageInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "reader", "unknown", "grpc", "message"},
		}
		l.Log(params.withLevel(INFO), "received unknown message",
			Error(info.Error),
			String("reader_connection_id", info.ReaderConnectionID),
		)
	}

	///
	/// Topic writer
	///
	t.OnWriterReconnect = func(info trace.TopicWriterReconnectStartInfo) func(doneInfo trace.TopicWriterReconnectDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "writer", "reconnect"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "connect to topic writer stream starting...",
			String("topic", info.Topic),
			String("producer_id", info.ProducerID),
			String("writer_instance_id", info.WriterInstanceID),
			Int("attempt", info.Attempt),
		)
		return func(doneInfo trace.TopicWriterReconnectDoneInfo) {
			l.Log(params.withLevel(DEBUG), "connect to topic writer stream completed",
				Error(doneInfo.Error),
				String("topic", info.Topic),
				String("producer_id", info.ProducerID),
				String("writer_instance_id", info.WriterInstanceID),
				Int("attempt", info.Attempt),
				latency(start),
			)
		}
	}
	t.OnWriterInitStream = func(info trace.TopicWriterInitStreamStartInfo) func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "writer", "stream", "init"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			String("topic", info.Topic),
			String("producer_id", info.ProducerID),
			String("writer_instance_id", info.WriterInstanceID),
		)

		return func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
			l.Log(params.withLevel(DEBUG), "init stream completed",
				Error(doneInfo.Error),
				String("topic", info.Topic),
				String("producer_id", info.ProducerID),
				String("writer_instance_id", info.WriterInstanceID),
				latency(start),
				String("session_id", doneInfo.SessionID),
			)
		}
	}
	t.OnWriterClose = func(info trace.TopicWriterCloseStartInfo) func(doneInfo trace.TopicWriterCloseDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "writer", "close"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			String("writer_instance_id", info.WriterInstanceID),
			NamedError("reason", info.Reason),
		)
		return func(doneInfo trace.TopicWriterCloseDoneInfo) {
			l.Log(params.withLevel(DEBUG), "close topic writer completed",
				Error(doneInfo.Error),
				String("writer_instance_id", info.WriterInstanceID),
				NamedError("reason", info.Reason),
				latency(start),
			)
		}
	}
	t.OnWriterCompressMessages = func(info trace.TopicWriterCompressMessagesStartInfo) func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "writer", "compress", "messages"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			String("writer_instance_id", info.WriterInstanceID),
			String("session_id", info.SessionID),
			Any("reason", info.Reason),
			Any("codec", info.Codec),
			Int("messages_count", info.MessagesCount),
			Int64("first_seqno", info.FirstSeqNo),
		)
		return func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
			l.Log(params.withLevel(DEBUG), "compress message completed",
				Error(doneInfo.Error),
				String("writer_instance_id", info.WriterInstanceID),
				String("session_id", info.SessionID),
				Any("reason", info.Reason),
				Any("codec", info.Codec),
				Int("messages_count", info.MessagesCount),
				Int64("first_seqno", info.FirstSeqNo),
				latency(start),
			)
		}
	}
	t.OnWriterSendMessages = func(info trace.TopicWriterSendMessagesStartInfo) func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return nil
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "writer", "send", "messages"},
		}
		start := time.Now()
		l.Log(params.withLevel(DEBUG), "start",
			String("writer_instance_id", info.WriterInstanceID),
			String("session_id", info.SessionID),
			Any("codec", info.Codec),
			Int("messages_count", info.MessagesCount),
			Int64("first_seqno", info.FirstSeqNo),
		)
		return func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
			l.Log(params.withLevel(DEBUG), "compress message completed",
				Error(doneInfo.Error),
				String("writer_instance_id", info.WriterInstanceID),
				String("session_id", info.SessionID),
				Any("codec", info.Codec),
				Int("messages_count", info.MessagesCount),
				Int64("first_seqno", info.FirstSeqNo),
				latency(start),
			)
		}
	}
	t.OnWriterReadUnknownGrpcMessage = func(info trace.TopicOnWriterReadUnknownGrpcMessageInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return
		}
		params := Params{
			Level:     TRACE,
			Namespace: []string{"topic", "writer", "read", "unknown", "grpc", "message"},
		}
		l.Log(params.withLevel(INFO), "topic writer receive unknown message from server",
			Error(info.Error),
			String("writer_instance_id", info.WriterInstanceID),
			String("session_id", info.SessionID),
		)
	}
	return t
}
