package log

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Topic returns trace.Topic with logging events from details
func Topic(l Logger, d trace.Detailer, opts ...Option) (t trace.Topic) {
	return internalTopic(wrapLogger(l, opts...), d)
}

//nolint:gocyclo,funlen
func internalTopic(l Logger, d trace.Detailer) (t trace.Topic) {
	t.OnReaderReconnect = func(
		info trace.TopicReaderReconnectStartInfo,
	) func(doneInfo trace.TopicReaderReconnectDoneInfo) {
		if d.Details()&trace.TopicReaderStreamLifeCycleEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "reconnect")
		start := time.Now()
		l.Log(ctx, "start")

		return func(doneInfo trace.TopicReaderReconnectDoneInfo) {
			l.Log(WithLevel(ctx, INFO), "reconnected",
				NamedError("reason", info.Reason),
				latencyField(start),
			)
		}
	}
	t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
		if d.Details()&trace.TopicReaderStreamLifeCycleEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "reconnect", "request")
		l.Log(ctx, "start",
			NamedError("reason", info.Reason),
			Bool("was_sent", info.WasSent),
		)
	}
	t.OnReaderPartitionReadStartResponse = func(
		info trace.TopicReaderPartitionReadStartResponseStartInfo,
	) func(stopInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
		if d.Details()&trace.TopicReaderPartitionEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "partition", "read", "start", "response")
		start := time.Now()
		l.Log(ctx, "start",
			String("topic", info.Topic),
			String("reader_connection_id", info.ReaderConnectionID),
			Int64("partition_id", info.PartitionID),
			Int64("partition_session_id", info.PartitionSessionID),
		)

		return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
			fields := []Field{
				String("topic", info.Topic),
				String("reader_connection_id", info.ReaderConnectionID),
				Int64("partition_id", info.PartitionID),
				Int64("partition_session_id", info.PartitionSessionID),
				latencyField(start),
			}
			if doneInfo.CommitOffset != nil {
				fields = append(fields,
					Int64("commit_offset", *doneInfo.CommitOffset),
				)
			}
			if doneInfo.ReadOffset != nil {
				fields = append(fields,
					Int64("read_offset", *doneInfo.ReadOffset),
				)
			}
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, INFO), "read partition response completed", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "read partition response completed",
					append(fields,
						Error(doneInfo.Error),
						versionField(),
					)...,
				)
			}
		}
	}
	t.OnReaderPartitionReadStopResponse = func(
		info trace.TopicReaderPartitionReadStopResponseStartInfo,
	) func(trace.TopicReaderPartitionReadStopResponseDoneInfo) {
		if d.Details()&trace.TopicReaderPartitionEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "partition", "read", "stop", "response")
		start := time.Now()
		l.Log(ctx, "start",
			String("reader_connection_id", info.ReaderConnectionID),
			String("topic", info.Topic),
			Int64("partition_id", info.PartitionID),
			Int64("partition_session_id", info.PartitionSessionID),
			Int64("committed_offset", info.CommittedOffset),
			Bool("graceful", info.Graceful))

		return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {
			fields := []Field{
				String("reader_connection_id", info.ReaderConnectionID),
				String("topic", info.Topic),
				Int64("partition_id", info.PartitionID),
				Int64("partition_session_id", info.PartitionSessionID),
				Int64("committed_offset", info.CommittedOffset),
				Bool("graceful", info.Graceful),
				latencyField(start),
			}
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, INFO), "reader partition stopped", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "reader partition stopped",
					append(fields,
						Error(doneInfo.Error),
						versionField(),
					)...,
				)
			}
		}
	}
	t.OnReaderCommit = func(info trace.TopicReaderCommitStartInfo) func(doneInfo trace.TopicReaderCommitDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "commit")
		start := time.Now()
		l.Log(ctx, "start",
			String("topic", info.Topic),
			Int64("partition_id", info.PartitionID),
			Int64("partition_session_id", info.PartitionSessionID),
			Int64("commit_start_offset", info.StartOffset),
			Int64("commit_end_offset", info.EndOffset),
		)

		return func(doneInfo trace.TopicReaderCommitDoneInfo) {
			fields := []Field{
				String("topic", info.Topic),
				Int64("partition_id", info.PartitionID),
				Int64("partition_session_id", info.PartitionSessionID),
				Int64("commit_start_offset", info.StartOffset),
				Int64("commit_end_offset", info.EndOffset),
				latencyField(start),
			}
			if doneInfo.Error == nil {
				l.Log(ctx, "committed", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "committed",
					append(fields,
						Error(doneInfo.Error),
						versionField(),
					)...,
				)
			}
		}
	}
	t.OnReaderSendCommitMessage = func(
		info trace.TopicReaderSendCommitMessageStartInfo,
	) func(trace.TopicReaderSendCommitMessageDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "send", "commit", "message")
		start := time.Now()

		commitInfo := info.CommitsInfo.GetCommitsInfo()
		for i := range commitInfo {
			l.Log(ctx, "start",
				String("topic", commitInfo[i].Topic),
				Int64("partitions_id", commitInfo[i].PartitionID),
				Int64("partitions_session_id", commitInfo[i].PartitionSessionID),
				Int64("commit_start_offset", commitInfo[i].StartOffset),
				Int64("commit_end_offset", commitInfo[i].EndOffset),
			)
		}

		return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
			for i := range commitInfo {
				fields := []Field{
					String("topic", commitInfo[i].Topic),
					Int64("partitions_id", commitInfo[i].PartitionID),
					Int64("partitions_session_id", commitInfo[i].PartitionSessionID),
					Int64("commit_start_offset", commitInfo[i].StartOffset),
					Int64("commit_end_offset", commitInfo[i].EndOffset),
					latencyField(start),
				}
				if doneInfo.Error == nil {
					l.Log(ctx, "done", fields...)
				} else {
					l.Log(WithLevel(ctx, WARN), "commit message sent",
						append(fields,
							Error(doneInfo.Error),
							versionField(),
						)...,
					)
				}
			}
		}
	}
	t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "committed", "notify")
		l.Log(ctx, "ack",
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
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "close")
		start := time.Now()
		l.Log(ctx, "done",
			String("reader_connection_id", info.ReaderConnectionID),
			NamedError("close_reason", info.CloseReason),
		)

		return func(doneInfo trace.TopicReaderCloseDoneInfo) {
			fields := []Field{
				String("reader_connection_id", info.ReaderConnectionID),
				latencyField(start),
			}
			if doneInfo.CloseError == nil {
				l.Log(ctx, "closed", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "closed",
					append(fields,
						Error(doneInfo.CloseError),
						versionField(),
					)...,
				)
			}
		}
	}

	t.OnReaderInit = func(info trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "init")
		start := time.Now()
		l.Log(ctx, "start",
			String("pre_init_reader_connection_id", info.PreInitReaderConnectionID),
			String("consumer", info.InitRequestInfo.GetConsumer()),
			Strings("topics", info.InitRequestInfo.GetTopics()),
		)

		return func(doneInfo trace.TopicReaderInitDoneInfo) {
			fields := []Field{
				String("pre_init_reader_connection_id", info.PreInitReaderConnectionID),
				String("consumer", info.InitRequestInfo.GetConsumer()),
				Strings("topics", info.InitRequestInfo.GetTopics()),
				latencyField(start),
			}
			if doneInfo.Error == nil {
				l.Log(ctx, "topic reader stream initialized", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "topic reader stream initialized",
					append(fields,
						Error(doneInfo.Error),
						versionField(),
					)...,
				)
			}
		}
	}
	t.OnReaderError = func(info trace.TopicReaderErrorInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "error")
		l.Log(WithLevel(ctx, INFO), "stream error",
			Error(info.Error),
			String("reader_connection_id", info.ReaderConnectionID),
			versionField(),
		)
	}
	t.OnReaderUpdateToken = func(
		info trace.OnReadUpdateTokenStartInfo,
	) func(
		updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo,
	) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "update", "token")
		start := time.Now()
		l.Log(ctx, "token updating...",
			String("reader_connection_id", info.ReaderConnectionID),
		)

		return func(
			updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo,
		) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			if updateTokenInfo.Error == nil {
				l.Log(ctx, "got token",
					String("reader_connection_id", info.ReaderConnectionID),
					Int("token_len", updateTokenInfo.TokenLen),
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "got token",
					Error(updateTokenInfo.Error),
					String("reader_connection_id", info.ReaderConnectionID),
					Int("token_len", updateTokenInfo.TokenLen),
					latencyField(start),
					versionField(),
				)
			}

			return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
				if doneInfo.Error == nil {
					l.Log(ctx, "token updated on stream",
						String("reader_connection_id", info.ReaderConnectionID),
						Int("token_len", updateTokenInfo.TokenLen),
						latencyField(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "token updated on stream",
						Error(doneInfo.Error),
						String("reader_connection_id", info.ReaderConnectionID),
						Int("token_len", updateTokenInfo.TokenLen),
						latencyField(start),
						versionField(),
					)
				}
			}
		}
	}
	t.OnReaderSentDataRequest = func(info trace.TopicReaderSentDataRequestInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "sent", "data", "request")
		l.Log(ctx, "sent data request",
			String("reader_connection_id", info.ReaderConnectionID),
			Int("request_bytes", info.RequestBytes),
			Int("local_capacity", info.LocalBufferSizeAfterSent),
		)
	}
	t.OnReaderReceiveDataResponse = func(
		info trace.TopicReaderReceiveDataResponseStartInfo,
	) func(trace.TopicReaderReceiveDataResponseDoneInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "receive", "data", "response")
		start := time.Now()
		partitionsCount, batchesCount, messagesCount := info.DataResponse.GetPartitionBatchMessagesCounts()
		l.Log(ctx, "data response received, process starting...",
			String("reader_connection_id", info.ReaderConnectionID),
			Int("received_bytes", info.DataResponse.GetBytesSize()),
			Int("local_capacity", info.LocalBufferSizeAfterReceive),
			Int("partitions_count", partitionsCount),
			Int("batches_count", batchesCount),
			Int("messages_count", messagesCount),
		)

		return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(ctx, "data response received and processed",
					String("reader_connection_id", info.ReaderConnectionID),
					Int("received_bytes", info.DataResponse.GetBytesSize()),
					Int("local_capacity", info.LocalBufferSizeAfterReceive),
					Int("partitions_count", partitionsCount),
					Int("batches_count", batchesCount),
					Int("messages_count", messagesCount),
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "data response received and processed",
					Error(doneInfo.Error),
					String("reader_connection_id", info.ReaderConnectionID),
					Int("received_bytes", info.DataResponse.GetBytesSize()),
					Int("local_capacity", info.LocalBufferSizeAfterReceive),
					Int("partitions_count", partitionsCount),
					Int("batches_count", batchesCount),
					Int("messages_count", messagesCount),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
	t.OnReaderReadMessages = func(
		info trace.TopicReaderReadMessagesStartInfo,
	) func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "read", "messages")
		start := time.Now()
		l.Log(ctx, "read messages called, waiting...",
			Int("min_count", info.MinCount),
			Int("max_count", info.MaxCount),
			Int("local_capacity_before", info.FreeBufferCapacity),
		)

		return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(ctx, "read messages returned",
					Int("min_count", info.MinCount),
					Int("max_count", info.MaxCount),
					Int("local_capacity_before", info.FreeBufferCapacity),
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "read messages returned",
					Error(doneInfo.Error),
					Int("min_count", info.MinCount),
					Int("max_count", info.MaxCount),
					Int("local_capacity_before", info.FreeBufferCapacity),
					latencyField(start),
					versionField(),
				)
			}
		}
	}
	t.OnReaderUnknownGrpcMessage = func(info trace.OnReadUnknownGrpcMessageInfo) {
		if d.Details()&trace.TopicReaderMessageEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "unknown", "grpc", "message")
		l.Log(WithLevel(ctx, INFO), "received unknown message",
			Error(info.Error),
			String("reader_connection_id", info.ReaderConnectionID),
		)
	}

	t.OnReaderPopBatchTx = func(
		startInfo trace.TopicReaderPopBatchTxStartInfo,
	) func(trace.TopicReaderPopBatchTxDoneInfo) {
		if d.Details()&trace.TopicReaderCustomerEvents == 0 {
			return nil
		}

		start := time.Now()
		ctx := with(*startInfo.Context, TRACE, "ydb", "topic", "reader", "customer", "popbatchtx")
		l.Log(WithLevel(ctx, TRACE), "starting pop batch tx",
			Int64("reader_id", startInfo.ReaderID),
			String("transaction_session_id", startInfo.TransactionSessionID),
			String("transaction_id", startInfo.Tx.ID()),
		)

		return func(doneInfo trace.TopicReaderPopBatchTxDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(
					WithLevel(ctx, DEBUG), "pop batch done",
					Int64("reader_id", startInfo.ReaderID),
					String("transaction_session_id", startInfo.TransactionSessionID),
					String("transaction_id", startInfo.Tx.ID()),
					Int("messaged_count", doneInfo.MessagesCount),
					Int64("start_offset", doneInfo.StartOffset),
					Int64("end_offset", doneInfo.EndOffset),
					latencyField(start),
					versionField(),
				)
			} else {
				l.Log(
					WithLevel(ctx, WARN), "pop batch failed",
					Int64("reader_id", startInfo.ReaderID),
					String("transaction_session_id", startInfo.TransactionSessionID),
					String("transaction_id", startInfo.Tx.ID()),
					Error(doneInfo.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}

	t.OnReaderStreamPopBatchTx = func(
		startInfo trace.TopicReaderStreamPopBatchTxStartInfo,
	) func(
		trace.TopicReaderStreamPopBatchTxDoneInfo,
	) {
		if d.Details()&trace.TopicReaderTransactionEvents == 0 {
			return nil
		}

		start := time.Now()
		ctx := with(*startInfo.Context, TRACE, "ydb", "topic", "reader", "transaction", "popbatchtx_on_stream")
		l.Log(WithLevel(ctx, TRACE), "starting pop batch tx",
			Int64("reader_id", startInfo.ReaderID),
			String("reader_connection_id", startInfo.ReaderConnectionID),
			String("transaction_session_id", startInfo.TransactionSessionID),
			String("transaction_id", startInfo.Tx.ID()),
			versionField(),
		)

		return func(doneInfo trace.TopicReaderStreamPopBatchTxDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(
					WithLevel(ctx, DEBUG), "pop batch on stream done",
					Int64("reader_id", startInfo.ReaderID),
					String("transaction_session_id", startInfo.TransactionSessionID),
					String("transaction_id", startInfo.Tx.ID()),
					latencyField(start),
					versionField(),
				)
			} else {
				l.Log(
					WithLevel(ctx, WARN), "pop batch on stream failed",
					Int64("reader_id", startInfo.ReaderID),
					String("transaction_session_id", startInfo.TransactionSessionID),
					String("transaction_id", startInfo.Tx.ID()),
					Error(doneInfo.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}

	t.OnReaderUpdateOffsetsInTransaction = func(
		startInfo trace.TopicReaderOnUpdateOffsetsInTransactionStartInfo,
	) func(
		trace.TopicReaderOnUpdateOffsetsInTransactionDoneInfo,
	) {
		if d.Details()&trace.TopicReaderTransactionEvents == 0 {
			return nil
		}

		start := time.Now()
		ctx := with(*startInfo.Context, TRACE, "ydb", "topic", "reader", "transaction", "update_offsets")
		l.Log(WithLevel(ctx, TRACE), "starting update offsets in transaction",
			Int64("reader_id", startInfo.ReaderID),
			String("reader_connection_id", startInfo.ReaderConnectionID),
			String("transaction_session_id", startInfo.TransactionSessionID),
			String("transaction_id", startInfo.Tx.ID()),
			versionField(),
		)

		return func(doneInfo trace.TopicReaderOnUpdateOffsetsInTransactionDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(
					WithLevel(ctx, DEBUG), "pop batch on stream done",
					Int64("reader_id", startInfo.ReaderID),
					String("transaction_session_id", startInfo.TransactionSessionID),
					String("transaction_id", startInfo.Tx.ID()),
					latencyField(start),
					versionField(),
				)
			} else {
				l.Log(
					WithLevel(ctx, WARN), "pop batch on stream failed",
					Int64("reader_id", startInfo.ReaderID),
					String("transaction_session_id", startInfo.TransactionSessionID),
					String("transaction_id", startInfo.Tx.ID()),
					Error(doneInfo.Error),
					latencyField(start),
					versionField(),
				)
			}
		}
	}

	t.OnReaderTransactionRollback = func(
		startInfo trace.TopicReaderTransactionRollbackStartInfo,
	) func(
		trace.TopicReaderTransactionRollbackDoneInfo,
	) {
		if d.Details()&trace.TopicReaderTransactionEvents == 0 {
			return nil
		}

		start := time.Now()
		ctx := with(*startInfo.Context, TRACE, "ydb", "topic", "reader", "transaction", "update_offsets")
		l.Log(WithLevel(ctx, TRACE), "starting update offsets in transaction",
			Int64("reader_id", startInfo.ReaderID),
			String("reader_connection_id", startInfo.ReaderConnectionID),
			String("transaction_session_id", startInfo.TransactionSessionID),
			String("transaction_id", startInfo.Tx.ID()),
			versionField(),
		)

		return func(doneInfo trace.TopicReaderTransactionRollbackDoneInfo) {
			if doneInfo.RollbackError == nil {
				l.Log(
					WithLevel(ctx, DEBUG), "pop batch on stream done",
					Int64("reader_id", startInfo.ReaderID),
					String("transaction_session_id", startInfo.TransactionSessionID),
					String("transaction_id", startInfo.Tx.ID()),
					latencyField(start),
					versionField(),
				)
			} else {
				l.Log(
					WithLevel(ctx, WARN), "pop batch on stream failed",
					Int64("reader_id", startInfo.ReaderID),
					String("transaction_session_id", startInfo.TransactionSessionID),
					String("transaction_id", startInfo.Tx.ID()),
					Error(doneInfo.RollbackError),
					latencyField(start),
					versionField(),
				)
			}
		}
	}

	t.OnReaderTransactionCompleted = func(
		startInfo trace.TopicReaderTransactionCompletedStartInfo,
	) func(
		trace.TopicReaderTransactionCompletedDoneInfo,
	) {
		if d.Details()&trace.TopicReaderTransactionEvents == 0 {
			return nil
		}

		// expected as very short in memory operation without errors, no need log start separately
		start := time.Now()

		return func(doneInfo trace.TopicReaderTransactionCompletedDoneInfo) {
			ctx := with(*startInfo.Context, TRACE, "ydb", "topic", "reader", "transaction", "update_offsets")
			l.Log(WithLevel(ctx, TRACE), "starting update offsets in transaction",
				Int64("reader_id", startInfo.ReaderID),
				String("reader_connection_id", startInfo.ReaderConnectionID),
				String("transaction_session_id", startInfo.TransactionSessionID),
				String("transaction_id", startInfo.Tx.ID()),
				latencyField(start),
				versionField(),
			)
		}
	}

	///
	/// Topic writer
	///
	t.OnWriterReconnect = func(
		info trace.TopicWriterReconnectStartInfo,
	) func(doneInfo trace.TopicWriterReconnectDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "writer", "reconnect")
		start := time.Now()
		l.Log(ctx, "connect to topic writer stream starting...",
			String("topic", info.Topic),
			String("producer_id", info.ProducerID),
			String("writer_instance_id", info.WriterInstanceID),
			Int("attempt", info.Attempt),
		)

		return func(doneInfo trace.TopicWriterReconnectDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "connect to topic writer stream completed",
					String("topic", info.Topic),
					String("producer_id", info.ProducerID),
					String("writer_instance_id", info.WriterInstanceID),
					Int("attempt", info.Attempt),
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "connect to topic writer stream completed",
					Error(doneInfo.Error),
					String("topic", info.Topic),
					String("producer_id", info.ProducerID),
					String("writer_instance_id", info.WriterInstanceID),
					Int("attempt", info.Attempt),
					latencyField(start),
				)
			}
		}
	}
	t.OnWriterInitStream = func(
		info trace.TopicWriterInitStreamStartInfo,
	) func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "writer", "stream", "init")
		start := time.Now()
		l.Log(ctx, "start",
			String("topic", info.Topic),
			String("producer_id", info.ProducerID),
			String("writer_instance_id", info.WriterInstanceID),
		)

		return func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "init stream completed",
					Error(doneInfo.Error),
					String("topic", info.Topic),
					String("producer_id", info.ProducerID),
					String("writer_instance_id", info.WriterInstanceID),
					latencyField(start),
					String("session_id", doneInfo.SessionID),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "init stream completed",
					Error(doneInfo.Error),
					String("topic", info.Topic),
					String("producer_id", info.ProducerID),
					String("writer_instance_id", info.WriterInstanceID),
					latencyField(start),
					String("session_id", doneInfo.SessionID),
				)
			}
		}
	}
	t.OnWriterBeforeCommitTransaction = func(info trace.TopicOnWriterBeforeCommitTransactionStartInfo) func(trace.TopicOnWriterBeforeCommitTransactionDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}

		start := time.Now()
		return func(doneInfo trace.TopicOnWriterBeforeCommitTransactionDoneInfo) {
			ctx := with(*info.Ctx, TRACE, "ydb", "topic", "writer", "beforecommit")
			l.Log(ctx, "wait of flush messages before commit transaction",
				String("kqp_session_id", info.KqpSessionID),
				String("topic_session_id_start", info.TopicSessionID),
				String("topic_session_id_finish", doneInfo.TopicSessionID),
				String("tx_id", info.TransactionID),
				latencyField(start),
			)
		}
	}
	t.OnWriterAfterFinishTransaction = func(info trace.TopicOnWriterAfterFinishTransactionStartInfo) func(trace.TopicOnWriterAfterFinishTransactionDoneInfo) {
		start := time.Now()
		return func(doneInfo trace.TopicOnWriterAfterFinishTransactionDoneInfo) {
			ctx := with(context.Background(), TRACE, "ydb", "topic", "writer", "beforecommit")
			l.Log(ctx, "close writer after transaction finished",
				String("kqp_session_id", info.SessionID),
				String("tx_id", info.TransactionID),
				latencyField(start),
			)
		}
	}
	t.OnWriterClose = func(info trace.TopicWriterCloseStartInfo) func(doneInfo trace.TopicWriterCloseDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "writer", "close")
		start := time.Now()
		l.Log(ctx, "start",
			String("writer_instance_id", info.WriterInstanceID),
			NamedError("reason", info.Reason),
		)

		return func(doneInfo trace.TopicWriterCloseDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "close topic writer completed",
					Error(doneInfo.Error),
					String("writer_instance_id", info.WriterInstanceID),
					NamedError("reason", info.Reason),
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "close topic writer completed",
					Error(doneInfo.Error),
					String("writer_instance_id", info.WriterInstanceID),
					NamedError("reason", info.Reason),
					latencyField(start),
				)
			}
		}
	}
	t.OnWriterCompressMessages = func(
		info trace.TopicWriterCompressMessagesStartInfo,
	) func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "writer", "compress", "messages")
		start := time.Now()
		l.Log(ctx, "start",
			String("writer_instance_id", info.WriterInstanceID),
			String("session_id", info.SessionID),
			Any("reason", info.Reason),
			Any("codec", info.Codec),
			Int("messages_count", info.MessagesCount),
			Int64("first_seqno", info.FirstSeqNo),
		)

		return func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(ctx, "compress message completed",
					Error(doneInfo.Error),
					String("writer_instance_id", info.WriterInstanceID),
					String("session_id", info.SessionID),
					Any("reason", info.Reason),
					Any("codec", info.Codec),
					Int("messages_count", info.MessagesCount),
					Int64("first_seqno", info.FirstSeqNo),
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "compress message completed",
					Error(doneInfo.Error),
					String("writer_instance_id", info.WriterInstanceID),
					String("session_id", info.SessionID),
					Any("reason", info.Reason),
					Any("codec", info.Codec),
					Int("messages_count", info.MessagesCount),
					Int64("first_seqno", info.FirstSeqNo),
					latencyField(start),
				)
			}
		}
	}
	t.OnWriterSendMessages = func(
		info trace.TopicWriterSendMessagesStartInfo,
	) func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "writer", "send", "messages")
		start := time.Now()
		l.Log(ctx, "start",
			String("writer_instance_id", info.WriterInstanceID),
			String("session_id", info.SessionID),
			Any("codec", info.Codec),
			Int("messages_count", info.MessagesCount),
			Int64("first_seqno", info.FirstSeqNo),
		)

		return func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(ctx, "writing messages to grpc buffer completed",
					String("writer_instance_id", info.WriterInstanceID),
					String("session_id", info.SessionID),
					Any("codec", info.Codec),
					Int("messages_count", info.MessagesCount),
					Int64("first_seqno", info.FirstSeqNo),
					latencyField(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "writing messages to grpc buffer completed",
					Error(doneInfo.Error),
					String("writer_instance_id", info.WriterInstanceID),
					String("session_id", info.SessionID),
					Any("codec", info.Codec),
					Int("messages_count", info.MessagesCount),
					Int64("first_seqno", info.FirstSeqNo),
					latencyField(start),
				)
			}
		}
	}
	t.OnWriterReceiveResult = func(info trace.TopicWriterResultMessagesInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return
		}
		acks := info.Acks.GetAcks()
		ctx := with(context.Background(), DEBUG, "ydb", "topic", "writer", "receive", "result")
		l.Log(ctx, "topic writer receive result from server",
			String("writer_instance_id", info.WriterInstanceID),
			String("session_id", info.SessionID),
			Int("acks_count", acks.AcksCount),
			Int64("seq_no_min", acks.SeqNoMin),
			Int64("seq_no_max", acks.SeqNoMax),
			Int64("written_offset_min", acks.WrittenOffsetMin),
			Int64("written_offset_max", acks.WrittenOffsetMax),
			Int("written_offset_count", acks.WrittenCount),
			Int("written_in_tx_count", acks.WrittenInTxCount),
			Int("skip_count", acks.SkipCount),
			versionField(),
		)
	}
	t.OnWriterReadUnknownGrpcMessage = func(info trace.TopicOnWriterReadUnknownGrpcMessageInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return
		}
		ctx := with(context.Background(), DEBUG, "ydb", "topic", "writer", "read", "unknown", "grpc", "message")
		l.Log(ctx, "topic writer receive unknown message from server",
			Error(info.Error),
			String("writer_instance_id", info.WriterInstanceID),
			String("session_id", info.SessionID),
		)
	}

	return t
}
