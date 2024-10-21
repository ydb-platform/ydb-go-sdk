package log

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
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
				kv.NamedError("reason", info.Reason),
				kv.Latency(start),
			)
		}
	}
	t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
		if d.Details()&trace.TopicReaderStreamLifeCycleEvents == 0 {
			return
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "reconnect", "request")
		l.Log(ctx, "start",
			kv.NamedError("reason", info.Reason),
			kv.Bool("was_sent", info.WasSent),
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
			kv.String("topic", info.Topic),
			kv.String("reader_connection_id", info.ReaderConnectionID),
			kv.Int64("partition_id", info.PartitionID),
			kv.Int64("partition_session_id", info.PartitionSessionID),
		)

		return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
			fields := []Field{
				kv.String("topic", info.Topic),
				kv.String("reader_connection_id", info.ReaderConnectionID),
				kv.Int64("partition_id", info.PartitionID),
				kv.Int64("partition_session_id", info.PartitionSessionID),
				kv.Latency(start),
			}
			if doneInfo.CommitOffset != nil {
				fields = append(fields,
					kv.Int64("commit_offset", *doneInfo.CommitOffset),
				)
			}
			if doneInfo.ReadOffset != nil {
				fields = append(fields,
					kv.Int64("read_offset", *doneInfo.ReadOffset),
				)
			}
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, INFO), "read partition response completed", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "read partition response completed",
					append(fields,
						kv.Error(doneInfo.Error),
						kv.Version(),
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
			kv.String("reader_connection_id", info.ReaderConnectionID),
			kv.String("topic", info.Topic),
			kv.Int64("partition_id", info.PartitionID),
			kv.Int64("partition_session_id", info.PartitionSessionID),
			kv.Int64("committed_offset", info.CommittedOffset),
			kv.Bool("graceful", info.Graceful))

		return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {
			fields := []Field{
				kv.String("reader_connection_id", info.ReaderConnectionID),
				kv.String("topic", info.Topic),
				kv.Int64("partition_id", info.PartitionID),
				kv.Int64("partition_session_id", info.PartitionSessionID),
				kv.Int64("committed_offset", info.CommittedOffset),
				kv.Bool("graceful", info.Graceful),
				kv.Latency(start),
			}
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, INFO), "reader partition stopped", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "reader partition stopped",
					append(fields,
						kv.Error(doneInfo.Error),
						kv.Version(),
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
			kv.String("topic", info.Topic),
			kv.Int64("partition_id", info.PartitionID),
			kv.Int64("partition_session_id", info.PartitionSessionID),
			kv.Int64("commit_start_offset", info.StartOffset),
			kv.Int64("commit_end_offset", info.EndOffset),
		)

		return func(doneInfo trace.TopicReaderCommitDoneInfo) {
			fields := []Field{
				kv.String("topic", info.Topic),
				kv.Int64("partition_id", info.PartitionID),
				kv.Int64("partition_session_id", info.PartitionSessionID),
				kv.Int64("commit_start_offset", info.StartOffset),
				kv.Int64("commit_end_offset", info.EndOffset),
				kv.Latency(start),
			}
			if doneInfo.Error == nil {
				l.Log(ctx, "committed", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "committed",
					append(fields,
						kv.Error(doneInfo.Error),
						kv.Version(),
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
				kv.String("topic", commitInfo[i].Topic),
				kv.Int64("partitions_id", commitInfo[i].PartitionID),
				kv.Int64("partitions_session_id", commitInfo[i].PartitionSessionID),
				kv.Int64("commit_start_offset", commitInfo[i].StartOffset),
				kv.Int64("commit_end_offset", commitInfo[i].EndOffset),
			)
		}

		return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
			for i := range commitInfo {
				fields := []Field{
					kv.String("topic", commitInfo[i].Topic),
					kv.Int64("partitions_id", commitInfo[i].PartitionID),
					kv.Int64("partitions_session_id", commitInfo[i].PartitionSessionID),
					kv.Int64("commit_start_offset", commitInfo[i].StartOffset),
					kv.Int64("commit_end_offset", commitInfo[i].EndOffset),
					kv.Latency(start),
				}
				if doneInfo.Error == nil {
					l.Log(ctx, "done", fields...)
				} else {
					l.Log(WithLevel(ctx, WARN), "commit message sent",
						append(fields,
							kv.Error(doneInfo.Error),
							kv.Version(),
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
			kv.String("reader_connection_id", info.ReaderConnectionID),
			kv.String("topic", info.Topic),
			kv.Int64("partition_id", info.PartitionID),
			kv.Int64("partition_session_id", info.PartitionSessionID),
			kv.Int64("committed_offset", info.CommittedOffset),
		)
	}
	t.OnReaderClose = func(info trace.TopicReaderCloseStartInfo) func(doneInfo trace.TopicReaderCloseDoneInfo) {
		if d.Details()&trace.TopicReaderStreamEvents == 0 {
			return nil
		}
		ctx := with(context.Background(), TRACE, "ydb", "topic", "reader", "close")
		start := time.Now()
		l.Log(ctx, "done",
			kv.String("reader_connection_id", info.ReaderConnectionID),
			kv.NamedError("close_reason", info.CloseReason),
		)

		return func(doneInfo trace.TopicReaderCloseDoneInfo) {
			fields := []Field{
				kv.String("reader_connection_id", info.ReaderConnectionID),
				kv.Latency(start),
			}
			if doneInfo.CloseError == nil {
				l.Log(ctx, "closed", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "closed",
					append(fields,
						kv.Error(doneInfo.CloseError),
						kv.Version(),
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
			kv.String("pre_init_reader_connection_id", info.PreInitReaderConnectionID),
			kv.String("consumer", info.InitRequestInfo.GetConsumer()),
			kv.Strings("topics", info.InitRequestInfo.GetTopics()),
		)

		return func(doneInfo trace.TopicReaderInitDoneInfo) {
			fields := []Field{
				kv.String("pre_init_reader_connection_id", info.PreInitReaderConnectionID),
				kv.String("consumer", info.InitRequestInfo.GetConsumer()),
				kv.Strings("topics", info.InitRequestInfo.GetTopics()),
				kv.Latency(start),
			}
			if doneInfo.Error == nil {
				l.Log(ctx, "topic reader stream initialized", fields...)
			} else {
				l.Log(WithLevel(ctx, WARN), "topic reader stream initialized",
					append(fields,
						kv.Error(doneInfo.Error),
						kv.Version(),
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
			kv.Error(info.Error),
			kv.String("reader_connection_id", info.ReaderConnectionID),
			kv.Version(),
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
			kv.String("reader_connection_id", info.ReaderConnectionID),
		)

		return func(
			updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo,
		) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			if updateTokenInfo.Error == nil {
				l.Log(ctx, "got token",
					kv.String("reader_connection_id", info.ReaderConnectionID),
					kv.Int("token_len", updateTokenInfo.TokenLen),
					kv.Latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "got token",
					kv.Error(updateTokenInfo.Error),
					kv.String("reader_connection_id", info.ReaderConnectionID),
					kv.Int("token_len", updateTokenInfo.TokenLen),
					kv.Latency(start),
					kv.Version(),
				)
			}

			return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
				if doneInfo.Error == nil {
					l.Log(ctx, "token updated on stream",
						kv.String("reader_connection_id", info.ReaderConnectionID),
						kv.Int("token_len", updateTokenInfo.TokenLen),
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "token updated on stream",
						kv.Error(doneInfo.Error),
						kv.String("reader_connection_id", info.ReaderConnectionID),
						kv.Int("token_len", updateTokenInfo.TokenLen),
						kv.Latency(start),
						kv.Version(),
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
			kv.String("reader_connection_id", info.ReaderConnectionID),
			kv.Int("request_bytes", info.RequestBytes),
			kv.Int("local_capacity", info.LocalBufferSizeAfterSent),
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
			kv.String("reader_connection_id", info.ReaderConnectionID),
			kv.Int("received_bytes", info.DataResponse.GetBytesSize()),
			kv.Int("local_capacity", info.LocalBufferSizeAfterReceive),
			kv.Int("partitions_count", partitionsCount),
			kv.Int("batches_count", batchesCount),
			kv.Int("messages_count", messagesCount),
		)

		return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(ctx, "data response received and processed",
					kv.String("reader_connection_id", info.ReaderConnectionID),
					kv.Int("received_bytes", info.DataResponse.GetBytesSize()),
					kv.Int("local_capacity", info.LocalBufferSizeAfterReceive),
					kv.Int("partitions_count", partitionsCount),
					kv.Int("batches_count", batchesCount),
					kv.Int("messages_count", messagesCount),
					kv.Latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "data response received and processed",
					kv.Error(doneInfo.Error),
					kv.String("reader_connection_id", info.ReaderConnectionID),
					kv.Int("received_bytes", info.DataResponse.GetBytesSize()),
					kv.Int("local_capacity", info.LocalBufferSizeAfterReceive),
					kv.Int("partitions_count", partitionsCount),
					kv.Int("batches_count", batchesCount),
					kv.Int("messages_count", messagesCount),
					kv.Latency(start),
					kv.Version(),
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
			kv.Int("min_count", info.MinCount),
			kv.Int("max_count", info.MaxCount),
			kv.Int("local_capacity_before", info.FreeBufferCapacity),
		)

		return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(ctx, "read messages returned",
					kv.Int("min_count", info.MinCount),
					kv.Int("max_count", info.MaxCount),
					kv.Int("local_capacity_before", info.FreeBufferCapacity),
					kv.Latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "read messages returned",
					kv.Error(doneInfo.Error),
					kv.Int("min_count", info.MinCount),
					kv.Int("max_count", info.MaxCount),
					kv.Int("local_capacity_before", info.FreeBufferCapacity),
					kv.Latency(start),
					kv.Version(),
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
			kv.Error(info.Error),
			kv.String("reader_connection_id", info.ReaderConnectionID),
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
			kv.Int64("reader_id", startInfo.ReaderID),
			kv.String("transaction_session_id", startInfo.TransactionSessionID),
			kv.String("transaction_id", startInfo.Tx.ID()),
		)

		return func(doneInfo trace.TopicReaderPopBatchTxDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(
					WithLevel(ctx, DEBUG), "pop batch done",
					kv.Int64("reader_id", startInfo.ReaderID),
					kv.String("transaction_session_id", startInfo.TransactionSessionID),
					kv.String("transaction_id", startInfo.Tx.ID()),
					kv.Int("messaged_count", doneInfo.MessagesCount),
					kv.Int64("start_offset", doneInfo.StartOffset),
					kv.Int64("end_offset", doneInfo.EndOffset),
					kv.Latency(start),
					kv.Version(),
				)
			} else {
				l.Log(
					WithLevel(ctx, WARN), "pop batch failed",
					kv.Int64("reader_id", startInfo.ReaderID),
					kv.String("transaction_session_id", startInfo.TransactionSessionID),
					kv.String("transaction_id", startInfo.Tx.ID()),
					kv.Error(doneInfo.Error),
					kv.Latency(start),
					kv.Version(),
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
			kv.Int64("reader_id", startInfo.ReaderID),
			kv.String("reader_connection_id", startInfo.ReaderConnectionID),
			kv.String("transaction_session_id", startInfo.TransactionSessionID),
			kv.String("transaction_id", startInfo.Tx.ID()),
			kv.Version(),
		)

		return func(doneInfo trace.TopicReaderStreamPopBatchTxDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(
					WithLevel(ctx, DEBUG), "pop batch on stream done",
					kv.Int64("reader_id", startInfo.ReaderID),
					kv.String("transaction_session_id", startInfo.TransactionSessionID),
					kv.String("transaction_id", startInfo.Tx.ID()),
					kv.Latency(start),
					kv.Version(),
				)
			} else {
				l.Log(
					WithLevel(ctx, WARN), "pop batch on stream failed",
					kv.Int64("reader_id", startInfo.ReaderID),
					kv.String("transaction_session_id", startInfo.TransactionSessionID),
					kv.String("transaction_id", startInfo.Tx.ID()),
					kv.Error(doneInfo.Error),
					kv.Latency(start),
					kv.Version(),
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
			kv.Int64("reader_id", startInfo.ReaderID),
			kv.String("reader_connection_id", startInfo.ReaderConnectionID),
			kv.String("transaction_session_id", startInfo.TransactionSessionID),
			kv.String("transaction_id", startInfo.Tx.ID()),
			kv.Version(),
		)

		return func(doneInfo trace.TopicReaderOnUpdateOffsetsInTransactionDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(
					WithLevel(ctx, DEBUG), "pop batch on stream done",
					kv.Int64("reader_id", startInfo.ReaderID),
					kv.String("transaction_session_id", startInfo.TransactionSessionID),
					kv.String("transaction_id", startInfo.Tx.ID()),
					kv.Latency(start),
					kv.Version(),
				)
			} else {
				l.Log(
					WithLevel(ctx, WARN), "pop batch on stream failed",
					kv.Int64("reader_id", startInfo.ReaderID),
					kv.String("transaction_session_id", startInfo.TransactionSessionID),
					kv.String("transaction_id", startInfo.Tx.ID()),
					kv.Error(doneInfo.Error),
					kv.Latency(start),
					kv.Version(),
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
			kv.Int64("reader_id", startInfo.ReaderID),
			kv.String("reader_connection_id", startInfo.ReaderConnectionID),
			kv.String("transaction_session_id", startInfo.TransactionSessionID),
			kv.String("transaction_id", startInfo.Tx.ID()),
			kv.Version(),
		)

		return func(doneInfo trace.TopicReaderTransactionRollbackDoneInfo) {
			if doneInfo.RollbackError == nil {
				l.Log(
					WithLevel(ctx, DEBUG), "pop batch on stream done",
					kv.Int64("reader_id", startInfo.ReaderID),
					kv.String("transaction_session_id", startInfo.TransactionSessionID),
					kv.String("transaction_id", startInfo.Tx.ID()),
					kv.Latency(start),
					kv.Version(),
				)
			} else {
				l.Log(
					WithLevel(ctx, WARN), "pop batch on stream failed",
					kv.Int64("reader_id", startInfo.ReaderID),
					kv.String("transaction_session_id", startInfo.TransactionSessionID),
					kv.String("transaction_id", startInfo.Tx.ID()),
					kv.Error(doneInfo.RollbackError),
					kv.Latency(start),
					kv.Version(),
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
				kv.Int64("reader_id", startInfo.ReaderID),
				kv.String("reader_connection_id", startInfo.ReaderConnectionID),
				kv.String("transaction_session_id", startInfo.TransactionSessionID),
				kv.String("transaction_id", startInfo.Tx.ID()),
				kv.Latency(start),
				kv.Version(),
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
			kv.String("topic", info.Topic),
			kv.String("producer_id", info.ProducerID),
			kv.String("writer_instance_id", info.WriterInstanceID),
			kv.Int("attempt", info.Attempt),
		)

		return func(doneInfo trace.TopicWriterReconnectDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "connect to topic writer stream completed",
					kv.String("topic", info.Topic),
					kv.String("producer_id", info.ProducerID),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.Int("attempt", info.Attempt),
					kv.Latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "connect to topic writer stream completed",
					kv.Error(doneInfo.Error),
					kv.String("topic", info.Topic),
					kv.String("producer_id", info.ProducerID),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.Int("attempt", info.Attempt),
					kv.Latency(start),
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
			kv.String("topic", info.Topic),
			kv.String("producer_id", info.ProducerID),
			kv.String("writer_instance_id", info.WriterInstanceID),
		)

		return func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "init stream completed",
					kv.Error(doneInfo.Error),
					kv.String("topic", info.Topic),
					kv.String("producer_id", info.ProducerID),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.Latency(start),
					kv.String("session_id", doneInfo.SessionID),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "init stream completed",
					kv.Error(doneInfo.Error),
					kv.String("topic", info.Topic),
					kv.String("producer_id", info.ProducerID),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.Latency(start),
					kv.String("session_id", doneInfo.SessionID),
				)
			}
		}
	}
	t.OnWriterBeforeCommitTransaction = func(
		info trace.TopicOnWriterBeforeCommitTransactionStartInfo,
	) func(trace.TopicOnWriterBeforeCommitTransactionDoneInfo) {
		if d.Details()&trace.TopicWriterStreamLifeCycleEvents == 0 {
			return nil
		}

		start := time.Now()

		return func(doneInfo trace.TopicOnWriterBeforeCommitTransactionDoneInfo) {
			ctx := with(*info.Ctx, TRACE, "ydb", "topic", "writer", "beforecommit")
			l.Log(ctx, "wait of flush messages before commit transaction",
				kv.String("kqp_session_id", info.KqpSessionID),
				kv.String("topic_session_id_start", info.TopicSessionID),
				kv.String("topic_session_id_finish", doneInfo.TopicSessionID),
				kv.String("tx_id", info.TransactionID),
				kv.Latency(start),
			)
		}
	}
	t.OnWriterAfterFinishTransaction = func(
		info trace.TopicOnWriterAfterFinishTransactionStartInfo,
	) func(trace.TopicOnWriterAfterFinishTransactionDoneInfo) {
		start := time.Now()

		return func(doneInfo trace.TopicOnWriterAfterFinishTransactionDoneInfo) {
			ctx := with(context.Background(), TRACE, "ydb", "topic", "writer", "beforecommit")
			l.Log(ctx, "close writer after transaction finished",
				kv.String("kqp_session_id", info.SessionID),
				kv.String("tx_id", info.TransactionID),
				kv.Latency(start),
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
			kv.String("writer_instance_id", info.WriterInstanceID),
			kv.NamedError("reason", info.Reason),
		)

		return func(doneInfo trace.TopicWriterCloseDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(WithLevel(ctx, DEBUG), "close topic writer completed",
					kv.Error(doneInfo.Error),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.NamedError("reason", info.Reason),
					kv.Latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "close topic writer completed",
					kv.Error(doneInfo.Error),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.NamedError("reason", info.Reason),
					kv.Latency(start),
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
			kv.String("writer_instance_id", info.WriterInstanceID),
			kv.String("session_id", info.SessionID),
			kv.Any("reason", info.Reason),
			kv.Any("codec", info.Codec),
			kv.Int("messages_count", info.MessagesCount),
			kv.Int64("first_seqno", info.FirstSeqNo),
		)

		return func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(ctx, "compress message completed",
					kv.Error(doneInfo.Error),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.String("session_id", info.SessionID),
					kv.Any("reason", info.Reason),
					kv.Any("codec", info.Codec),
					kv.Int("messages_count", info.MessagesCount),
					kv.Int64("first_seqno", info.FirstSeqNo),
					kv.Latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, ERROR), "compress message completed",
					kv.Error(doneInfo.Error),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.String("session_id", info.SessionID),
					kv.Any("reason", info.Reason),
					kv.Any("codec", info.Codec),
					kv.Int("messages_count", info.MessagesCount),
					kv.Int64("first_seqno", info.FirstSeqNo),
					kv.Latency(start),
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
			kv.String("writer_instance_id", info.WriterInstanceID),
			kv.String("session_id", info.SessionID),
			kv.Any("codec", info.Codec),
			kv.Int("messages_count", info.MessagesCount),
			kv.Int64("first_seqno", info.FirstSeqNo),
		)

		return func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
			if doneInfo.Error == nil {
				l.Log(ctx, "writing messages to grpc buffer completed",
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.String("session_id", info.SessionID),
					kv.Any("codec", info.Codec),
					kv.Int("messages_count", info.MessagesCount),
					kv.Int64("first_seqno", info.FirstSeqNo),
					kv.Latency(start),
				)
			} else {
				l.Log(WithLevel(ctx, WARN), "writing messages to grpc buffer completed",
					kv.Error(doneInfo.Error),
					kv.String("writer_instance_id", info.WriterInstanceID),
					kv.String("session_id", info.SessionID),
					kv.Any("codec", info.Codec),
					kv.Int("messages_count", info.MessagesCount),
					kv.Int64("first_seqno", info.FirstSeqNo),
					kv.Latency(start),
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
			kv.String("writer_instance_id", info.WriterInstanceID),
			kv.String("session_id", info.SessionID),
			kv.Int("acks_count", acks.AcksCount),
			kv.Int64("seq_no_min", acks.SeqNoMin),
			kv.Int64("seq_no_max", acks.SeqNoMax),
			kv.Int64("written_offset_min", acks.WrittenOffsetMin),
			kv.Int64("written_offset_max", acks.WrittenOffsetMax),
			kv.Int("written_offset_count", acks.WrittenCount),
			kv.Int("written_in_tx_count", acks.WrittenInTxCount),
			kv.Int("skip_count", acks.SkipCount),
			kv.Version(),
		)
	}
	t.OnWriterReadUnknownGrpcMessage = func(info trace.TopicOnWriterReadUnknownGrpcMessageInfo) {
		if d.Details()&trace.TopicWriterStreamEvents == 0 {
			return
		}
		ctx := with(context.Background(), DEBUG, "ydb", "topic", "writer", "read", "unknown", "grpc", "message")
		l.Log(ctx, "topic writer receive unknown message from server",
			kv.Error(info.Error),
			kv.String("writer_instance_id", info.WriterInstanceID),
			kv.String("session_id", info.SessionID),
		)
	}

	return t
}
