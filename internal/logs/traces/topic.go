//nolint:lll
package traces

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Topic returns trace.Topic with logging events from details
func Topic(l logs.Logger, details trace.Details, opts ...Option) trace.Topic {
	ll := newLogger(l, "ydb", "topic")
	t := trace.Topic{}

	///
	/// Topic Reader
	///

	if details&trace.TopicReaderStreamLifeCycleEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("reader", "lifecycle")

		t.OnReaderReconnect = func(startInfo trace.TopicReaderReconnectStartInfo) func(doneInfo trace.TopicReaderReconnectDoneInfo) {
			start := time.Now()

			ll.Debug("reconnecting")

			return func(doneInfo trace.TopicReaderReconnectDoneInfo) {
				ll.Info(`reconnected`,
					latency(start),
				)
			}
		}

		t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
			ll.Debug(`request reconnect`,
				logs.NamedError("reason", info.Reason),
				logs.Bool("was_sent", info.WasSent),
			)
		}
	}
	if details&trace.TopicReaderPartitionEvents != 0 {
		//nolint:govet
		ll := ll.WithSubScope("reader", "partition")
		t.OnReaderPartitionReadStartResponse = func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(stopInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
			start := time.Now()
			ll.Debug(`read partition response starting...`,
				logs.String("topic", startInfo.Topic),
				logs.String("reader_connection_id", startInfo.ReaderConnectionID),
				logs.Int64("partition_id", startInfo.PartitionID),
				logs.Int64("partition_session_id", startInfo.PartitionSessionID),
			)

			return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
				if doneInfo.Error == nil {
					ll.Info(`read partition response completed`,
						logs.String("topic", startInfo.Topic),
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.Int64("partition_id", startInfo.PartitionID),
						logs.Int64("partition_session_id", startInfo.PartitionSessionID),
						logs.Int64("commit_offset", *doneInfo.CommitOffset),
						logs.Int64("read_offset", *doneInfo.ReadOffset),
						latency(start),
						version(),
					)
				} else {
					ll.Warn(`read partition response completed`,
						logs.Error(doneInfo.Error),
						logs.String("topic", startInfo.Topic),
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.Int64("partition_id", startInfo.PartitionID),
						logs.Int64("partition_session_id", startInfo.PartitionSessionID),
						logs.Int64("commit_offset", *doneInfo.CommitOffset),
						logs.Int64("read_offset", *doneInfo.ReadOffset),
						latency(start),
						version(),
					)
				}

			}
		}

		t.OnReaderPartitionReadStopResponse = func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(trace.TopicReaderPartitionReadStopResponseDoneInfo) {
			start := time.Now()
			ll.Debug(`reader partition stopping...`,
				logs.String("reader_connection_id", startInfo.ReaderConnectionID),
				logs.String("topic", startInfo.Topic),
				logs.Int64("partition_id", startInfo.PartitionID),
				logs.Int64("partition_session_id", startInfo.PartitionSessionID),
				logs.Int64("committed_offset", startInfo.CommittedOffset),
				logs.Bool("graceful", startInfo.Graceful))

			return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {

				if doneInfo.Error != nil {
					ll.Info("reader partition stopped",
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.String("topic", startInfo.Topic),
						logs.Int64("partition_id", startInfo.PartitionID),
						logs.Int64("partition_session_id", startInfo.PartitionSessionID),
						logs.Int64("committed_offset", startInfo.CommittedOffset),
						logs.Bool("graceful", startInfo.Graceful),
						latency(start),
						version(),
					)
				} else {
					ll.Warn("reader partition stopped",
						logs.Error(doneInfo.Error),
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.String("topic", startInfo.Topic),
						logs.Int64("partition_id", startInfo.PartitionID),
						logs.Int64("partition_session_id", startInfo.PartitionSessionID),
						logs.Int64("committed_offset", startInfo.CommittedOffset),
						logs.Bool("graceful", startInfo.Graceful),
						latency(start),
						version(),
					)
				}
			}
		}
	}

	if details&trace.TopicReaderStreamEvents != 0 {
		ll := ll.WithSubScope("reader", "stream")

		t.OnReaderCommit = func(startInfo trace.TopicReaderCommitStartInfo) func(doneInfo trace.TopicReaderCommitDoneInfo) {
			start := time.Now()
			ll.Debug(`start committing...`,

				logs.String("topic", startInfo.Topic),
				logs.Int64("partition_id", startInfo.PartitionID),
				logs.Int64("partition_session_id", startInfo.PartitionSessionID),
				logs.Int64("commit_start_offset", startInfo.StartOffset),
				logs.Int64("commit_end_offset", startInfo.EndOffset),
			)
			return func(doneInfo trace.TopicReaderCommitDoneInfo) {
				if doneInfo.Error == nil {
					ll.Debug(`committed`,
						logs.String("topic", startInfo.Topic),
						logs.Int64("partition_id", startInfo.PartitionID),
						logs.Int64("partition_session_id", startInfo.PartitionSessionID),
						logs.Int64("commit_start_offset", startInfo.StartOffset),
						logs.Int64("commit_end_offset", startInfo.EndOffset),
						latency(start),
						version(),
					)
				} else {
					ll.Warn(`committed`,
						logs.Error(doneInfo.Error),
						logs.String("topic", startInfo.Topic),
						logs.Int64("partition_id", startInfo.PartitionID),
						logs.Int64("partition_session_id", startInfo.PartitionSessionID),
						logs.Int64("commit_start_offset", startInfo.StartOffset),
						logs.Int64("commit_end_offset", startInfo.EndOffset),
						latency(start),
						version(),
					)
				}

			}
		}

		t.OnReaderSendCommitMessage = func(startInfo trace.TopicReaderSendCommitMessageStartInfo) func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
			start := time.Now()
			ll.Debug(`commit message sending...`,
				logs.Any("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
				logs.Any("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
			)

			return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
				if doneInfo.Error == nil {
					ll.Debug(`commit message sent`,
						logs.Any("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
						logs.Any("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
						latency(start),
						version(),
					)
				} else {
					ll.Warn(`commit message sent`,
						logs.Error(doneInfo.Error),
						logs.Any("partitions_id", startInfo.CommitsInfo.PartitionIDs()),
						logs.Any("partitions_session_id", startInfo.CommitsInfo.PartitionSessionIDs()),
						latency(start),
						version(),
					)
				}
			}
		}

		t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
			ll.Debug(`commit ack`,
				logs.String("reader_connection_id", info.ReaderConnectionID),
				logs.String("topic", info.Topic),
				logs.Int64("partition_id", info.PartitionID),
				logs.Int64("partition_session_id", info.PartitionSessionID),
				logs.Int64("committed_offset", info.CommittedOffset),
			)
		}

		t.OnReaderClose = func(startInfo trace.TopicReaderCloseStartInfo) func(doneInfo trace.TopicReaderCloseDoneInfo) {
			start := time.Now()
			ll.Debug(`stream closing... {reader_connection_id:"%v", close_reason:"%v"}`,
				logs.String("reader_connection_id", startInfo.ReaderConnectionID),
				logs.NamedError("close_reason", startInfo.CloseReason),
			)

			return func(doneInfo trace.TopicReaderCloseDoneInfo) {
				if doneInfo.CloseError != nil {
					ll.Debug(`topic reader stream closed`,
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						latency(start),
						version(),
					)
				} else {
					ll.Warn(`topic reader stream closed`,
						logs.Error(doneInfo.CloseError),
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.NamedError("close_reason", startInfo.CloseReason),
						latency(start),
						version(),
					)
				}

			}
		}

		t.OnReaderInit = func(startInfo trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
			start := time.Now()
			ll.Debug(`stream init starting...`,
				logs.String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
				logs.String("consumer", startInfo.InitRequestInfo.GetConsumer()),
				logs.Strings("topics", startInfo.InitRequestInfo.GetTopics()),
			)

			return func(doneInfo trace.TopicReaderInitDoneInfo) {
				if doneInfo.Error == nil {
					ll.Debug(`topic reader stream initialized`,
						logs.String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
						logs.String("consumer", startInfo.InitRequestInfo.GetConsumer()),
						logs.Strings("topics", startInfo.InitRequestInfo.GetTopics()),
						latency(start),
						version(),
					)
				} else {
					ll.Warn(`topic reader stream initialized`,
						logs.Error(doneInfo.Error),
						logs.String("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID),
						logs.String("consumer", startInfo.InitRequestInfo.GetConsumer()),
						logs.Strings("topics", startInfo.InitRequestInfo.GetTopics()),
						latency(start),
						version(),
					)
				}

			}
		}

		t.OnReaderError = func(info trace.TopicReaderErrorInfo) {
			ll.Warn(`stream error`,
				logs.Error(info.Error),
				logs.String("reader_connection_id", info.ReaderConnectionID),
				version(),
			)
		}

		t.OnReaderUpdateToken = func(startInfo trace.OnReadUpdateTokenStartInfo) func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			start := time.Now()
			ll.Debug(`token updating...`,
				logs.String("reader_connection_id", startInfo.ReaderConnectionID),
			)

			return func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
				if updateTokenInfo.Error == nil {
					ll.Debug(`got token`,
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.Int("token_len", updateTokenInfo.TokenLen),
						latency(start),
						version(),
					)
				} else {
					ll.Warn(`got token`,
						logs.Error(updateTokenInfo.Error),
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.Int("token_len", updateTokenInfo.TokenLen),
						latency(start),
						version(),
					)
				}

				return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
					if doneInfo.Error == nil {
						ll.Debug(`token updated on stream`,
							logs.String("reader_connection_id", startInfo.ReaderConnectionID),
							logs.Int("token_len", updateTokenInfo.TokenLen),
							latency(start),
							version(),
						)
					} else {
						ll.Warn(`token updated on stream`,
							logs.Error(doneInfo.Error),
							logs.String("reader_connection_id", startInfo.ReaderConnectionID),
							logs.Int("token_len", updateTokenInfo.TokenLen),
							latency(start),
							version(),
						)
					}
				}
			}
		}
	}

	if details&trace.TopicReaderMessageEvents != 0 {
		ll := ll.WithSubScope("reader", "message")

		t.OnReaderSentDataRequest = func(info trace.TopicReaderSentDataRequestInfo) {
			ll.Debug(`sent data request`,
				logs.String("reader_connection_id", info.ReaderConnectionID),
				logs.Int("request_bytes", info.RequestBytes),
				logs.Int("local_capacity", info.LocalBufferSizeAfterSent),
			)
		}

		t.OnReaderReceiveDataResponse = func(startInfo trace.TopicReaderReceiveDataResponseStartInfo) func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
			start := time.Now()
			partitionsCount, batchesCount, messagesCount := startInfo.DataResponse.GetPartitionBatchMessagesCounts()
			ll.Debug(`data response received, process starting...`,
				logs.String("reader_connection_id", startInfo.ReaderConnectionID),
				logs.Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
				logs.Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
				logs.Int("partitions_count", partitionsCount),
				logs.Int("batches_count", batchesCount),
				logs.Int("messages_count", messagesCount),
			)

			return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
				if doneInfo.Error == nil {
					ll.Debug(`data response received and processed`,
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
						logs.Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
						logs.Int("partitions_count", partitionsCount),
						logs.Int("batches_count", batchesCount),
						logs.Int("messages_count", messagesCount),
						latency(start),
						version(),
					)
				} else {
					ll.Warn(`data response received and processed`,
						logs.Error(doneInfo.Error),
						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
						logs.Int("received_bytes", startInfo.DataResponse.GetBytesSize()),
						logs.Int("local_capacity", startInfo.LocalBufferSizeAfterReceive),
						logs.Int("partitions_count", partitionsCount),
						logs.Int("batches_count", batchesCount),
						logs.Int("messages_count", messagesCount),
						latency(start),
						version(),
					)
				}

			}
		}

		t.OnReaderReadMessages = func(startInfo trace.TopicReaderReadMessagesStartInfo) func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
			start := time.Now()
			ll.Debug(`read messages called, waiting...`,
				logs.Int("min_count", startInfo.MinCount),
				logs.Int("max_count", startInfo.MaxCount),
				logs.Int("local_capacity_before", startInfo.FreeBufferCapacity),
			)

			return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
				if doneInfo.Error == nil {
					ll.Debug(`read messages returned`,
						logs.Int("min_count", startInfo.MinCount),
						logs.Int("max_count", startInfo.MaxCount),
						logs.Int("local_capacity_before", startInfo.FreeBufferCapacity),
						latency(start),
						version(),
					)
				} else {
					ll.Info(`read messages returned`,
						logs.Error(doneInfo.Error),
						logs.Int("min_count", startInfo.MinCount),
						logs.Int("max_count", startInfo.MaxCount),
						logs.Int("local_capacity_before", startInfo.FreeBufferCapacity),
						latency(start),
						version(),
					)
				}
			}
		}

		t.OnReaderUnknownGrpcMessage = func(info trace.OnReadUnknownGrpcMessageInfo) {
			ll.Info(`received unknown message`,
				logs.Error(info.Error),
				logs.String("reader_connection_id", info.ReaderConnectionID),
			)

		}
	}

	///
	/// Topic writer
	///
	if details&trace.TopicWriterStreamLifeCycleEvents != 0 {
		ll := ll.WithSubScope("writer", "lifecycle")
		t.OnWriterReconnect = func(startInfo trace.TopicWriterReconnectStartInfo) func(doneInfo trace.TopicWriterReconnectDoneInfo) {
			start := time.Now()
			ll.Debug("connect to topic writer stream starting...",
				logs.String("topic", startInfo.Topic),
				logs.String("producer_id", startInfo.ProducerID),
				logs.String("writer_instance_id", startInfo.WriterInstanceID),
				logs.Int("attempt", startInfo.Attempt),
			)
			return func(doneInfo trace.TopicWriterReconnectDoneInfo) {
				ll.Debug("connect to topic writer stream completed",
					logs.Error(doneInfo.Error),
					logs.String("topic", startInfo.Topic),
					logs.String("producer_id", startInfo.ProducerID),
					logs.String("writer_instance_id", startInfo.WriterInstanceID),
					logs.Int("attempt", startInfo.Attempt),
					latency(start),
				)
			}
		}
		t.OnWriterInitStream = func(startInfo trace.TopicWriterInitStreamStartInfo) func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
			start := time.Now()
			ll.Debug("init stream starting...",
				logs.String("topic", startInfo.Topic),
				logs.String("producer_id", startInfo.ProducerID),
				logs.String("writer_instance_id", startInfo.WriterInstanceID),
			)

			return func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
				ll.Debug("init stream completed",
					logs.Error(doneInfo.Error),
					logs.String("topic", startInfo.Topic),
					logs.String("producer_id", startInfo.ProducerID),
					logs.String("writer_instance_id", startInfo.WriterInstanceID),
					latency(start),
					logs.String("session_id", doneInfo.SessionID),
				)
			}
		}
		t.OnWriterClose = func(startInfo trace.TopicWriterCloseStartInfo) func(doneInfo trace.TopicWriterCloseDoneInfo) {
			start := time.Now()
			ll.Debug("close topic writer starting...",
				logs.String("writer_instance_id", startInfo.WriterInstanceID),
				logs.NamedError("reason", startInfo.Reason),
			)

			return func(doneInfo trace.TopicWriterCloseDoneInfo) {
				ll.Debug("close topic writer completed",
					logs.Error(doneInfo.Error),
					logs.String("writer_instance_id", startInfo.WriterInstanceID),
					logs.NamedError("reason", startInfo.Reason),
					latency(start),
				)
			}
		}
	}
	if details&trace.TopicWriterStreamEvents != 0 {
		ll := ll.WithSubScope("writer", "stream")
		t.OnWriterCompressMessages = func(startInfo trace.TopicWriterCompressMessagesStartInfo) func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
			start := time.Now()
			ll.Debug("compress message starting...",
				logs.String("writer_instance_id", startInfo.WriterInstanceID),
				logs.String("session_id", startInfo.SessionID),
				logs.Any("reason", startInfo.Reason),
				logs.Any("codec", startInfo.Codec),
				logs.Int("messages_count", startInfo.MessagesCount),
				logs.Int64("first_seqno", startInfo.FirstSeqNo),
			)

			return func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
				ll.Debug("compress message completed",
					logs.Error(doneInfo.Error),
					logs.String("writer_instance_id", startInfo.WriterInstanceID),
					logs.String("session_id", startInfo.SessionID),
					logs.Any("reason", startInfo.Reason),
					logs.Any("codec", startInfo.Codec),
					logs.Int("messages_count", startInfo.MessagesCount),
					logs.Int64("first_seqno", startInfo.FirstSeqNo),
					latency(start),
				)
			}
		}
		t.OnWriterSendMessages = func(startInfo trace.TopicWriterSendMessagesStartInfo) func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
			start := time.Now()
			ll.Debug("compress message starting...",
				logs.String("writer_instance_id", startInfo.WriterInstanceID),
				logs.String("session_id", startInfo.SessionID),
				logs.Any("codec", startInfo.Codec),
				logs.Int("messages_count", startInfo.MessagesCount),
				logs.Int64("first_seqno", startInfo.FirstSeqNo),
			)

			return func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
				ll.Debug("compress message completed",
					logs.Error(doneInfo.Error),
					logs.String("writer_instance_id", startInfo.WriterInstanceID),
					logs.String("session_id", startInfo.SessionID),
					logs.Any("codec", startInfo.Codec),
					logs.Int("messages_count", startInfo.MessagesCount),
					logs.Int64("first_seqno", startInfo.FirstSeqNo),
					latency(start),
				)
			}
		}
		t.OnWriterReadUnknownGrpcMessage = func(info trace.TopicOnWriterReadUnknownGrpcMessageInfo) {
			ll.Info(
				"topic writer receive unknown message from server",
				logs.Error(info.Error),
				logs.String("writer_instance_id", info.WriterInstanceID),
				logs.String("session_id", info.SessionID),
			)
		}
	}

	return t
}
