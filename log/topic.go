//nolint:lll
package log

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Topic returns trace.Topic with logging events from details
func Topic(l Logger, d trace.Detailer, opts ...Option) (t trace.Topic) { //nolint:gocyclo
	t.OnReaderReconnect = func(startInfo trace.TopicReaderReconnectStartInfo) func(doneInfo trace.TopicReaderReconnectDoneInfo) {
		if d.Details()&trace.TopicReaderStreamLifeCycleEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "reconnect")
		start := time.Now()
		ll.Log(DEBUG, "start")
		return func(doneInfo trace.TopicReaderReconnectDoneInfo) {
			ll.Log(INFO, "reconnected",
				latency(start),
			)
		}
	}
	t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
		if d.Details()&trace.TopicReaderStreamLifeCycleEvents == 0 {
			return
		}
		ll := l.WithNames("topic", "reader", "reconnect", "request")
		ll.Log(DEBUG, "start",
			NamedError("reason", info.Reason),
			Bool("was_sent", info.WasSent),
		)
	}
	t.OnReaderPartitionReadStartResponse = func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(stopInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
		if d.Details()&trace.TopicReaderPartitionEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "partition", "read", "start", "response")
		start := time.Now()
		ll.Log(DEBUG, "start",
			String("topic", startInfo.Topic),
			String("reader_connection_id", startInfo.ReaderConnectionID),
			Int64("partition_id", startInfo.PartitionID),
			Int64("partition_session_id", startInfo.PartitionSessionID),
		)
		return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
			if doneInfo.Error == nil {
				ll.Log(INFO, "read partition response completed",
					String("topic", startInfo.Topic),
					String("reader_connection_id", startInfo.ReaderConnectionID),
					Int64("partition_id", startInfo.PartitionID),
					Int64("partition_session_id", startInfo.PartitionSessionID),
					Int64("commit_offset", *doneInfo.CommitOffset),
					Int64("read_offset", *doneInfo.ReadOffset),
					latency(start),
				)
			} else {
				ll.Log(INFO, "read partition response completed",
					Error(doneInfo.Error),
					String("topic", startInfo.Topic),
					String("reader_connection_id", startInfo.ReaderConnectionID),
					Int64("partition_id", startInfo.PartitionID),
					Int64("partition_session_id", startInfo.PartitionSessionID),
					Int64("commit_offset", *doneInfo.CommitOffset),
					Int64("read_offset", *doneInfo.ReadOffset),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderPartitionReadStopResponse = func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(trace.TopicReaderPartitionReadStopResponseDoneInfo) {
		if d.Details()&trace.TopicReaderPartitionEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "partition", "read", "stop", "response")
		start := time.Now()
		ll.Log(DEBUG, "start",
			String("reader_connection_id", startInfo.ReaderConnectionID),
			String("topic", startInfo.Topic),
			Int64("partition_id", startInfo.PartitionID),
			Int64("partition_session_id", startInfo.PartitionSessionID),
			Int64("committed_offset", startInfo.CommittedOffset),
			Bool("graceful", startInfo.Graceful))
		return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {
			if doneInfo.Error == nil {
				ll.Log(INFO, "reader partition stopped",
					Error(doneInfo.Error),
					String("reader_connection_id", startInfo.ReaderConnectionID),
					String("topic", startInfo.Topic),
					Int64("partition_id", startInfo.PartitionID),
					Int64("partition_session_id", startInfo.PartitionSessionID),
					Int64("committed_offset", startInfo.CommittedOffset),
					Bool("graceful", startInfo.Graceful),
					latency(start),
				)
			} else {
				ll.Log(INFO, "reader partition stopped",
					String("reader_connection_id", startInfo.ReaderConnectionID),
					String("topic", startInfo.Topic),
					Int64("partition_id", startInfo.PartitionID),
					Int64("partition_session_id", startInfo.PartitionSessionID),
					Int64("committed_offset", startInfo.CommittedOffset),
					Bool("graceful", startInfo.Graceful),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderCommit = func(startInfo trace.TopicReaderCommitStartInfo) func(doneInfo trace.TopicReaderCommitDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "commit")
		start := time.Now()
		ll.Log(DEBUG, "start",
			String("topic", startInfo.Topic),
			Int64("partition_id", startInfo.PartitionID),
			Int64("partition_session_id", startInfo.PartitionSessionID),
			Int64("commit_start_offset", startInfo.StartOffset),
			Int64("commit_end_offset", startInfo.EndOffset),
		)
		return func(doneInfo trace.TopicReaderCommitDoneInfo) {
			if doneInfo.Error == nil {
				ll.Log(DEBUG, "committed",
					String("topic", startInfo.Topic),
					Int64("partition_id", startInfo.PartitionID),
					Int64("partition_session_id", startInfo.PartitionSessionID),
					Int64("commit_start_offset", startInfo.StartOffset),
					Int64("commit_end_offset", startInfo.EndOffset),
					latency(start),
				)
			} else {
				ll.Log(INFO, "committed",
					Error(doneInfo.Error),
					String("topic", startInfo.Topic),
					Int64("partition_id", startInfo.PartitionID),
					Int64("partition_session_id", startInfo.PartitionSessionID),
					Int64("commit_start_offset", startInfo.StartOffset),
					Int64("commit_end_offset", startInfo.EndOffset),
					latency(start),
					version(),
				)
			}
		}
	}
	t.OnReaderSendCommitMessage = func(startInfo trace.TopicReaderSendCommitMessageStartInfo) func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "send", "commit", "message")
		start := time.Now()
		ll.Log(DEBUG, "start",
			Any("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
			Any("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
		)
		return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
			if doneInfo.Error == nil {
				ll.Log(DEBUG, "done",
					Any("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
					Any("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
					latency(start),
				)
			} else {
				ll.Log(INFO, "commit message sent",
					Error(doneInfo.Error),
					Any("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
					Any("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
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
		ll := l.WithNames("topic", "reader", "committed", "notify")
		ll.Log(DEBUG, "ack",
			String("reader_connection_id", info.ReaderConnectionID),
			String("topic", info.Topic),
			Int64("partition_id", info.PartitionID),
			Int64("partition_session_id", info.PartitionSessionID),
			Int64("committed_offset", info.CommittedOffset),
		)
	}
	t.OnReaderClose = func(startInfo trace.TopicReaderCloseStartInfo) func(doneInfo trace.TopicReaderCloseDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "close")
		start := time.Now()
		ll.Log(DEBUG, "done",
			String("reader_connection_id", startInfo.ReaderConnectionID),
			NamedError("close_reason", startInfo.CloseReason),
		)
		return func(doneInfo trace.TopicReaderCloseDoneInfo) {
			if doneInfo.CloseError != nil {
				ll.Log(DEBUG, "closed",
					String("reader_connection_id", startInfo.ReaderConnectionID),
					latency(start),
				)
			} else {
				ll.Log(DEBUG, "closed",
					Error(doneInfo.CloseError),
					String("reader_connection_id", startInfo.ReaderConnectionID),
					NamedError("close_reason", startInfo.CloseReason),
					latency(start),
					version(),
				)
			}
		}
	}

	t.OnReaderInit = func(startInfo trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "init")
		start := time.Now()
		ll.Log(DEBUG, "start",
			String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
			String("consumer", startInfo.InitRequestInfo.GetConsumer()),
			Strings("topics", startInfo.InitRequestInfo.GetTopics()),
		)
		return func(doneInfo trace.TopicReaderInitDoneInfo) {
			if doneInfo.Error == nil {
				ll.Log(DEBUG, "topic reader stream initialized",
					String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
					String("consumer", startInfo.InitRequestInfo.GetConsumer()),
					Strings("topics", startInfo.InitRequestInfo.GetTopics()),
					latency(start),
				)
			} else {
				ll.Log(DEBUG, "topic reader stream initialized",
					Error(doneInfo.Error),
					String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
					String("consumer", startInfo.InitRequestInfo.GetConsumer()),
					Strings("topics", startInfo.InitRequestInfo.GetTopics()),
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
		ll := l.WithNames("topic", "reader", "error")
		ll.Log(INFO, "stream error",
			Error(info.Error),
			String("reader_connection_id", info.ReaderConnectionID),
			version(),
		)
	}
	t.OnReaderUpdateToken = func(startInfo trace.OnReadUpdateTokenStartInfo) func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "update", "token")
		start := time.Now()
		ll.Log(DEBUG, "token updating...",
			String("reader_connection_id", startInfo.ReaderConnectionID),
		)
		return func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			if updateTokenInfo.Error == nil {
				ll.Log(DEBUG, "got token",
					String("reader_connection_id", startInfo.ReaderConnectionID),
					Int("token_len", updateTokenInfo.TokenLen),
					latency(start),
				)
			} else {
				ll.Log(DEBUG, "got token",
					Error(updateTokenInfo.Error),
					String("reader_connection_id", startInfo.ReaderConnectionID),
					Int("token_len", updateTokenInfo.TokenLen),
					latency(start),
					version(),
				)
			}
			return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
				if doneInfo.Error == nil {
					ll.Log(DEBUG, "token updated on stream",
						String("reader_connection_id", startInfo.ReaderConnectionID),
						Int("token_len", updateTokenInfo.TokenLen),
						latency(start),
					)
				} else {
					ll.Log(DEBUG, "token updated on stream",
						Error(doneInfo.Error),
						String("reader_connection_id", startInfo.ReaderConnectionID),
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
		ll := l.WithNames("topic", "reader", "sent", "data", "request")
		ll.Log(DEBUG, "sent data request",
			String("reader_connection_id", info.ReaderConnectionID),
			Int("request_bytes", info.RequestBytes),
			Int("local_capacity", info.LocalBufferSizeAfterSent),
		)
	}
	t.OnReaderReceiveDataResponse = func(startInfo trace.TopicReaderReceiveDataResponseStartInfo) func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "receive", "data", "response")
		start := time.Now()
		partitionsCount, batchesCount, messagesCount := startInfo.DataResponse.GetPartitionBatchMessagesCounts()
		ll.Log(DEBUG, "data response received, process starting...",
			String("reader_connection_id", startInfo.ReaderConnectionID),
			Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
			Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
			Int("partitions_count", partitionsCount),
			Int("batches_count", batchesCount),
			Int("messages_count", messagesCount),
		)
		return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
			if doneInfo.Error == nil {
				ll.Log(DEBUG, "data response received and processed",
					String("reader_connection_id", startInfo.ReaderConnectionID),
					Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
					Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
					Int("partitions_count", partitionsCount),
					Int("batches_count", batchesCount),
					Int("messages_count", messagesCount),
					latency(start),
				)
			} else {
				ll.Log(DEBUG, "data response received and processed",
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
	t.OnReaderReadMessages = func(startInfo trace.TopicReaderReadMessagesStartInfo) func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "reader", "read", "messages")
		start := time.Now()
		ll.Log(DEBUG, "read messages called, waiting...",
			Int("min_count", startInfo.MinCount),
			Int("max_count", startInfo.MaxCount),
			Int("local_capacity_before", startInfo.FreeBufferCapacity),
		)
		return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
			if doneInfo.Error == nil {
				ll.Log(DEBUG, "read messages returned",
					Int("min_count", startInfo.MinCount),
					Int("max_count", startInfo.MaxCount),
					Int("local_capacity_before", startInfo.FreeBufferCapacity),
					latency(start),
				)
			} else {
				ll.Log(DEBUG, "read messages returned",
					Error(doneInfo.Error),
					Int("min_count", startInfo.MinCount),
					Int("max_count", startInfo.MaxCount),
					Int("local_capacity_before", startInfo.FreeBufferCapacity),
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
		ll := l.WithNames("topic", "reader", "unknown", "grpc", "message")
		ll.Log(INFO, "received unknown message",
			Error(info.Error),
			String("reader_connection_id", info.ReaderConnectionID),
		)
	}

	///
	/// Topic writer
	///
	t.OnWriterReconnect = func(startInfo trace.TopicWriterReconnectStartInfo) func(doneInfo trace.TopicWriterReconnectDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "writer", "reconnect")
		start := time.Now()
		ll.Log(DEBUG, "connect to topic writer stream starting...",
			String("topic", startInfo.Topic),
			String("producer_id", startInfo.ProducerID),
			String("writer_instance_id", startInfo.WriterInstanceID),
			Int("attempt", startInfo.Attempt),
		)
		return func(doneInfo trace.TopicWriterReconnectDoneInfo) {
			ll.Log(DEBUG, "connect to topic writer stream completed",
				Error(doneInfo.Error),
				String("topic", startInfo.Topic),
				String("producer_id", startInfo.ProducerID),
				String("writer_instance_id", startInfo.WriterInstanceID),
				Int("attempt", startInfo.Attempt),
				latency(start),
			)
		}
	}
	t.OnWriterInitStream = func(startInfo trace.TopicWriterInitStreamStartInfo) func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "writer", "stream", "init")
		start := time.Now()
		ll.Log(DEBUG, "start",
			String("topic", startInfo.Topic),
			String("producer_id", startInfo.ProducerID),
			String("writer_instance_id", startInfo.WriterInstanceID),
		)

		return func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
			ll.Log(DEBUG, "init stream completed",
				Error(doneInfo.Error),
				String("topic", startInfo.Topic),
				String("producer_id", startInfo.ProducerID),
				String("writer_instance_id", startInfo.WriterInstanceID),
				latency(start),
				String("session_id", doneInfo.SessionID),
			)
		}
	}
	t.OnWriterClose = func(startInfo trace.TopicWriterCloseStartInfo) func(doneInfo trace.TopicWriterCloseDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "writer", "close")
		start := time.Now()
		ll.Log(DEBUG, "start",
			String("writer_instance_id", startInfo.WriterInstanceID),
			NamedError("reason", startInfo.Reason),
		)
		return func(doneInfo trace.TopicWriterCloseDoneInfo) {
			ll.Log(DEBUG, "close topic writer completed",
				Error(doneInfo.Error),
				String("writer_instance_id", startInfo.WriterInstanceID),
				NamedError("reason", startInfo.Reason),
				latency(start),
			)
		}
	}
	t.OnWriterCompressMessages = func(startInfo trace.TopicWriterCompressMessagesStartInfo) func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "writer", "compress", "messages")
		start := time.Now()
		ll.Log(DEBUG, "start",
			String("writer_instance_id", startInfo.WriterInstanceID),
			String("session_id", startInfo.SessionID),
			Any("reason", startInfo.Reason),
			Any("codec", startInfo.Codec),
			Int("messages_count", startInfo.MessagesCount),
			Int64("first_seqno", startInfo.FirstSeqNo),
		)
		return func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
			ll.Log(DEBUG, "compress message completed",
				Error(doneInfo.Error),
				String("writer_instance_id", startInfo.WriterInstanceID),
				String("session_id", startInfo.SessionID),
				Any("reason", startInfo.Reason),
				Any("codec", startInfo.Codec),
				Int("messages_count", startInfo.MessagesCount),
				Int64("first_seqno", startInfo.FirstSeqNo),
				latency(start),
			)
		}
	}
	t.OnWriterSendMessages = func(startInfo trace.TopicWriterSendMessagesStartInfo) func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return nil
		}
		ll := l.WithNames("topic", "writer", "send", "messages")
		start := time.Now()
		ll.Log(DEBUG, "start",
			String("writer_instance_id", startInfo.WriterInstanceID),
			String("session_id", startInfo.SessionID),
			Any("codec", startInfo.Codec),
			Int("messages_count", startInfo.MessagesCount),
			Int64("first_seqno", startInfo.FirstSeqNo),
		)
		return func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
			ll.Log(DEBUG, "compress message completed",
				Error(doneInfo.Error),
				String("writer_instance_id", startInfo.WriterInstanceID),
				String("session_id", startInfo.SessionID),
				Any("codec", startInfo.Codec),
				Int("messages_count", startInfo.MessagesCount),
				Int64("first_seqno", startInfo.FirstSeqNo),
				latency(start),
			)
		}
	}
	t.OnWriterReadUnknownGrpcMessage = func(info trace.TopicOnWriterReadUnknownGrpcMessageInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return
		}
		ll := l.WithNames("topic", "writer", "read", "unknown", "grpc", "message")
		ll.Log(INFO, "topic writer receive unknown message from server",
			Error(info.Error),
			String("writer_instance_id", info.WriterInstanceID),
			String("session_id", info.SessionID),
		)
	}
	return t
}
