// //nolint:lll
package traces

// import (
// 	"time"

// 	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
// 	"github.com/ydb-platform/ydb-go-sdk/v3/logs"
// 	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
// )

// // Topic returns trace.Topic with logging events from details
// func Topic(l logs.Logger, details trace.Details, opts ...Option) trace.Topic {
// 	ll := newLogger(l, "ydb", "topic")
// 	t := trace.Topic{}

// 	///
// 	/// Topic Reader
// 	///

// 	if details&trace.TopicReaderStreamLifeCycleEvents != 0 {
// 		//nolint:govet
// 		ll := ll.WithSubScope("reader", "lifecycle")

// 		t.OnReaderReconnect = func(startInfo trace.TopicReaderReconnectStartInfo) func(doneInfo trace.TopicReaderReconnectDoneInfo) {
// 			start := time.Now()

// 			ll.Debug("reconnecting")

// 			return func(doneInfo trace.TopicReaderReconnectDoneInfo) {
// 				ll.Info(`reconnected`,
// 					logs.Duration("latency", time.Since(start)),	
// 				)
// 			}
// 		}

// 		t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
// 			ll.Debug(`request reconnect`,
// 				logs.NamedError("reason", info.Reason),
// 				logs.Bool("was_sent", info.WasSent),
// 			)
// 		}
// 	}
// 	if details&trace.TopicReaderPartitionEvents != 0 {
// 		//nolint:govet
// 		ll := ll.WithSubScope("reader", "partition")
// 		t.OnReaderPartitionReadStartResponse = func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(stopInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
// 			start := time.Now()
// 			ll.Debug(`read partition response starting... {topic:"%v", reader_connection_id:"%v", partition_id:%v, partition_session_id:%v}`,
// 				logs.String("topic", startInfo.Topic),
// 				logs.String("reader_connection_id", startInfo.ReaderConnectionID),
// 				logs.Int64("partition_id", startInfo.PartitionID),
// 				logs.Int64("partition_session_id", startInfo.PartitionSessionID),
// 			)

// 			return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
// 				if doneInfo.Error == nil {
// 					ll.Info(`read partition response completed`,
// 						logs.String("topic", startInfo.Topic),
// 						logs.String("reader_connection_id", startInfo.ReaderConnectionID),
// 						logs.Int64("partition_id", startInfo.PartitionID),
// 						logs.Int64("partition_session_id", startInfo.PartitionSessionID),
// 						logs.Duration("latency", time.Since(start)),

// 					)
// 				} else {
// 					ll.Warn(`read partition response completed`,
						
// 					)
// 				}
// `topic:"%v", reader_connection_id:"%v", partition_id:%v, partition_session_id:%v,`+
// 					//
// 					`latency:%v, commit_offset:%v, read_offset:%v, version:%v}`,
// 					startInfo.Topic, startInfo.ReaderConnectionID, startInfo.PartitionID, startInfo.PartitionSessionID,
// 					//
// 					time.Since(start), doneInfo.CommitOffset, doneInfo.ReadOffset, meta.Version)
// 			}
// 		}

// 		t.OnReaderPartitionReadStopResponse = func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(trace.TopicReaderPartitionReadStopResponseDoneInfo) {
// 			start := time.Now()
// 			ll.Debug(`reader partition stopping... {reader_connection_id:"%v", topic:"%v", partition_id:%v, partition_session_id:%v, committed_offset:%v, graceful:%v}`,
// 				startInfo.ReaderConnectionID, startInfo.Topic, startInfo.PartitionID, startInfo.PartitionSessionID, startInfo.CommittedOffset, startInfo.Graceful)

// 			return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {
// 				logInfoWarn(logger, doneInfo.Error,
// 					`reader partition stopped {reader_connection_id:"%v", topic:"%v", partition_id:%v, partition_session_id:%v, committed_offset:%v, graceful:%v, `+
// 						//
// 						`latency:%v, version:%v}`,
// 					startInfo.ReaderConnectionID, startInfo.Topic, startInfo.PartitionID, startInfo.PartitionSessionID, startInfo.CommittedOffset, startInfo.Graceful,
// 					//
// 					time.Since(start), meta.Version)
// 			}
// 		}
// 	}

// 	if details&trace.TopicReaderStreamEvents != 0 {
// 		logger := topicLogger.WithName("reader").WithName("stream")

// 		t.OnReaderCommit = func(startInfo trace.TopicReaderCommitStartInfo) func(doneInfo trace.TopicReaderCommitDoneInfo) {
// 			start := time.Now()
// 			ll.Debug(`start committing... {topic:"%v", partition_id:%v, partition_session_id:%v, commit_start_offset:%v, commit_end_offset:%v}`,
// 				startInfo.Topic, startInfo.PartitionID, startInfo.PartitionSessionID, startInfo.StartOffset, startInfo.EndOffset)

// 			return func(doneInfo trace.TopicReaderCommitDoneInfo) {
// 				if doneInfo.Error == nil {
// 					ll.Debug(`committed`,
						
// 					)
// 				} else {
// 					ll.Warn(`committed`,
						
// 					)
// 				}
// `topic:"%v", partition_id:%v, partition_session_id:%v, commit_start_offset:%v, commit_end_offset:%v, `+
// 					//
// 					`latency:%v, version:%v}`,
// 					startInfo.Topic, startInfo.PartitionID, startInfo.PartitionSessionID, startInfo.StartOffset, startInfo.EndOffset,
// 					//
// 					time.Since(start), meta.Version)
// 			}
// 		}

// 		t.OnReaderSendCommitMessage = func(startInfo trace.TopicReaderSendCommitMessageStartInfo) func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
// 			start := time.Now()
// 			ll.Debug(`commit message sending... {partitions_id:%v, partitions_session_id:%v}`,
// 				startInfo.CommitsInfo.PartitionIDs(), startInfo.CommitsInfo.PartitionSessionIDs())

// 			return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
// 				if doneInfo.Error == nil {
// 					ll.Debug(`commit message sent`,
						
// 					)
// 				} else {
// 					ll.Warn(`commit message sent`,
						
// 					)
// 				}
// `partitions_id:%v, partitions_session_id:%v`+
// 					//
// 					`latency:%v, version:%v}`,
// 					startInfo.CommitsInfo.PartitionIDs(), startInfo.CommitsInfo.PartitionSessionIDs(),
// 					//
// 					time.Since(start), meta.Version)
// 			}
// 		}

// 		t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
// 			ll.Debug(`commit ack {reader_connection_id:"%v", topic:"%v", partition_id:%v, partition_session_id:%v, committed_offset:%v}`,
// 				info.ReaderConnectionID, info.Topic, info.PartitionID, info.PartitionSessionID, info.CommittedOffset)
// 		}

// 		t.OnReaderClose = func(startInfo trace.TopicReaderCloseStartInfo) func(doneInfo trace.TopicReaderCloseDoneInfo) {
// 			start := time.Now()
// 			ll.Debug(`stream closing... {reader_connection_id:"%v", close_reason:"%v"}`,
// 				startInfo.ReaderConnectionID, startInfo.CloseReason)

// 			return func(doneInfo trace.TopicReaderCloseDoneInfo) {
// 				if doneInfo.CloseError != nil {
// 					ll.Debug(`topic reader stream closed`,
						
// 					)
// 				} else {
// 					ll.Warn(`topic reader stream closed`,
						
// 					)
// 				}
// `reader_connection_id:"%v", close_reason:"%v", `+
// 					//
// 					`latency:%v, version:%v}`,
// 					startInfo.ReaderConnectionID, startInfo.CloseReason,
// 					//
// 					time.Since(start), meta.Version)
// 			}
// 		}

// 		t.OnReaderInit = func(startInfo trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
// 			start := time.Now()
// 			ll.Debug(`stream init starting... {pre_init_reader_connection_id:"%v", consumer:"%v", topics:%v}`,
// 				startInfo.PreInitReaderConnectionID, startInfo.InitRequestInfo.GetConsumer(), startInfo.InitRequestInfo.GetTopics())

// 			return func(doneInfo trace.TopicReaderInitDoneInfo) {
// 				if doneInfo.Error == nil {
// 					ll.Debug(`topic reader stream initialized`,
						
// 					)
// 				} else {
// 					ll.Warn(`topic reader stream initialized`,
						
// 					)
// 				}
// `pre_init_reader_connection_id:"%v", consumer:"%v", topics:%v, `+
// 					//
// 					`reader_connection_id:"%v", latency:%v, version:%v}`,
// 					startInfo.PreInitReaderConnectionID, startInfo.InitRequestInfo.GetConsumer(), startInfo.InitRequestInfo.GetTopics(),
// 					//
// 					doneInfo.ReaderConnectionID, time.Since(start), meta.Version)
// 			}
// 		}

// 		t.OnReaderError = func(info trace.TopicReaderErrorInfo) {
// 			ll.Warn(`stream error {reader_connection_id:"%v", error:"%v", version:%v}`,
// 				info.ReaderConnectionID, info.Error, meta.Version)
// 		}

// 		t.OnReaderUpdateToken = func(startInfo trace.OnReadUpdateTokenStartInfo) func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
// 			start := time.Now()
// 			ll.Debug(`token updating... {reader_connection_id:"%v"}`,
// 				startInfo.ReaderConnectionID)

// 			return func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
// 				if updateTokenInfo.Error == nil {
// 					ll.Debug(`got token`,
						
// 					)
// 				} else {
// 					ll.Warn(`got token`,
						
// 					)
// 				}
// `reader_connection_id:"%v"`+
// 					//
// 					`token_len:%v, latency:%v, version:%v}`,
// 					startInfo.ReaderConnectionID,
// 					//
// 					updateTokenInfo.TokenLen, time.Since(start), meta.Version)

// 				return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
// 					if doneInfo.Error == nil {
// 						ll.Debug(`token updated on stream`,
							
// 						)
// 					} else {
// 						ll.Warn(`token updated on stream`,
							
// 						)
// 					}
// `reader_connection_id:"%v"`+
// 						//
// 						`token_len:%v, `+
// 						//
// 						`latency:%v, version:%v}`,
// 						startInfo.ReaderConnectionID,
// 						//
// 						updateTokenInfo.TokenLen,
// 						//
// 						time.Since(start), meta.Version)
// 				}
// 			}
// 		}
// 	}

// 	if details&trace.TopicReaderMessageEvents != 0 {
// 		logger := topicLogger.WithName("reader").WithName("message")

// 		t.OnReaderSentDataRequest = func(info trace.TopicReaderSentDataRequestInfo) {
// 			ll.Debug(`sent data request {reader_connection_id:"%v", request_bytes:%v, local_capacity:%v} `,
// 				info.ReaderConnectionID, info.RequestBytes, info.LocalBufferSizeAfterSent)
// 		}

// 		t.OnReaderReceiveDataResponse = func(startInfo trace.TopicReaderReceiveDataResponseStartInfo) func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
// 			start := time.Now()
// 			partitionsCount, batchesCount, messagesCount := startInfo.DataResponse.GetPartitionBatchMessagesCounts()
// 			ll.Debug(`data response received, process starting... {reader_connection_id:"%v", received_bytes:%v, local_capacity:%v, partitions_count:%v, batches_count:%v, messages_count:%v}`,
// 				startInfo.ReaderConnectionID, startInfo.DataResponse.GetBytesSize(), startInfo.LocalBufferSizeAfterReceive, partitionsCount, batchesCount, messagesCount)

// 			return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
// 				if doneInfo.Error == nil {
// 					ll.Debug(`data response received and processed`,
						
// 					)
// 				} else {
// 					ll.Warn(`data response received and processed`,
						
// 					)
// 				}
// `reader_connection_id:"%v", received_bytes:%v, local_capacity:%v, partitions_count:%v, batches_count:%v, messages_count:%v, `+
// 					//
// 					`latency:%v, version:%v}`,
// 					startInfo.ReaderConnectionID, startInfo.DataResponse.GetBytesSize(), startInfo.LocalBufferSizeAfterReceive, partitionsCount, batchesCount, messagesCount,
// 					//
// 					time.Since(start), meta.Version)
// 			}
// 		}

// 		t.OnReaderReadMessages = func(startInfo trace.TopicReaderReadMessagesStartInfo) func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
// 			start := time.Now()
// 			ll.Debug(`read messages called, waiting... {min_count:%v, max_count:%v, local_capacity_before:"%v"}`,
// 				startInfo.MinCount, startInfo.MaxCount, startInfo.FreeBufferCapacity)

// 			return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
// 				if doneInfo.Error == nil {
// 					ll.Debug(`read messages returned`,
						
// 					)
// 				} else {
// 					ll.Info(`read messages returned`,
						
// 					)
// 				}
// `min_count:%v, max_count:%v, local_capacity_before:"%v", `+
// 					//
// 					`topic:"%v", partition_id:%v, messages_count:%v, local_capacity_after:%v, latency:%v, version:%v}`,
// 					startInfo.MinCount, startInfo.MaxCount, startInfo.FreeBufferCapacity,
// 					//
// 					doneInfo.Topic, doneInfo.PartitionID, doneInfo.MessagesCount, doneInfo.FreeBufferCapacity, time.Since(start), meta.Version)
// 			}
// 		}

// 		t.OnReaderUnknownGrpcMessage = func(info trace.OnReadUnknownGrpcMessageInfo) {
// 			ll.Info(`received unknown message {reader_connection_id:"%v", error:"%v"}`,
// 				info.ReaderConnectionID, info.Error)
// 		}
// 	}

// 	///
// 	/// Topic writer
// 	///
// 	if details&trace.TopicWriterStreamLifeCycleEvents != 0 {
// 		logger := topicLogger.WithName("writer").WithName("lifecycle")
// 		t.OnWriterReconnect = func(startInfo trace.TopicWriterReconnectStartInfo) func(doneInfo trace.TopicWriterReconnectDoneInfo) {
// 			start := time.Now()
// 			ll.Debug("connect to topic writer stream starting... {topic:'%v', producer_id:'%v', writer_instance_id: '%v', attempt: %v}",
// 				startInfo.Topic,
// 				startInfo.ProducerID,
// 				startInfo.WriterInstanceID,
// 				startInfo.Attempt,
// 			)
// 			return func(doneInfo trace.TopicWriterReconnectDoneInfo) {
// 				ll.Debug("connect to topic writer stream completed {topic:'%v', producer_id:'%v', writer_instance_id: '%v', attempt: %v"+
// 					//
// 					"latency: %v, error: '%v'}",
// 					startInfo.Topic,
// 					startInfo.ProducerID,
// 					startInfo.WriterInstanceID,
// 					startInfo.Attempt,
// 					//
// 					time.Since(start),
// 					doneInfo.Error,
// 				)
// 			}
// 		}
// 		t.OnWriterInitStream = func(startInfo trace.TopicWriterInitStreamStartInfo) func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
// 			start := time.Now()
// 			ll.Debug("init stream starting... {topic:'%v', producer_id:'%v', writer_instance_id: '%v'}",
// 				startInfo.Topic,
// 				startInfo.ProducerID,
// 				startInfo.WriterInstanceID,
// 			)

// 			return func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
// 				ll.Debug("init stream completed {topic:'%v', producer_id:'%v', writer_instance_id: '%v'"+
// 					//
// 					"latency: %v, session_id: '%v', error: '%v'}",
// 					startInfo.Topic,
// 					startInfo.ProducerID,
// 					startInfo.WriterInstanceID,
// 					//
// 					time.Since(start),
// 					doneInfo.SessionID,
// 					doneInfo.Error,
// 				)
// 			}
// 		}
// 		t.OnWriterClose = func(startInfo trace.TopicWriterCloseStartInfo) func(doneInfo trace.TopicWriterCloseDoneInfo) {
// 			start := time.Now()
// 			ll.Debug("close topic writer starting... {writer_instance_id: '%v', reason: '%v'}",
// 				startInfo.WriterInstanceID,
// 				startInfo.Reason,
// 			)

// 			return func(doneInfo trace.TopicWriterCloseDoneInfo) {
// 				ll.Debug("close topic writer completed {writer_instance_id: '%v', reason: '%v'",
// 					//
// 					"latency: %v, error: '%v'}",
// 					startInfo.WriterInstanceID,
// 					startInfo.Reason,
// 					//
// 					time.Since(start),
// 					doneInfo.Error,
// 				)
// 			}
// 		}
// 	}
// 	if details&trace.TopicWriterStreamEvents != 0 {
// 		logger := topicLogger.WithName("writer").WithName("stream")
// 		t.OnWriterCompressMessages = func(startInfo trace.TopicWriterCompressMessagesStartInfo) func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
// 			start := time.Now()
// 			ll.Debug("compress message starting... {writer_instance_id:'%v', session_id: '%v', reason: %v, codec: %v, messages_count: %v, first_seqno: %v}",
// 				startInfo.WriterInstanceID,
// 				startInfo.SessionID,
// 				startInfo.Reason,
// 				startInfo.Codec,
// 				startInfo.MessagesCount,
// 				startInfo.FirstSeqNo,
// 			)

// 			return func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
// 				ll.Debug("compress message completed {writer_instance_id:'%v', session_id: '%v', reason: %v, codec: %v, messages_count: %v, first_seqno: %v}"+
// 					//
// 					"latency: %v, error: '%v'}",
// 					startInfo.WriterInstanceID,
// 					startInfo.SessionID,
// 					startInfo.Reason,
// 					startInfo.Codec,
// 					startInfo.MessagesCount,
// 					startInfo.FirstSeqNo,
// 					//
// 					time.Since(start),
// 					doneInfo.Error,
// 				)
// 			}
// 		}
// 		t.OnWriterSendMessages = func(startInfo trace.TopicWriterSendMessagesStartInfo) func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
// 			start := time.Now()
// 			ll.Debug("compress message starting... {writer_instance_id:'%v', session_id: '%v', codec: %v, messages_count: %v, first_seqno: %v}",
// 				startInfo.WriterInstanceID,
// 				startInfo.SessionID,
// 				startInfo.Codec,
// 				startInfo.MessagesCount,
// 				startInfo.FirstSeqNo,
// 			)

// 			return func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
// 				ll.Debug("compress message completed {writer_instance_id:'%v', session_id: '%v', codec: %v, messages_count: %v, first_seqno: %v}"+
// 					//
// 					"latency: %v, error: '%v'}",
// 					startInfo.WriterInstanceID,
// 					startInfo.SessionID,
// 					startInfo.Codec,
// 					startInfo.MessagesCount,
// 					startInfo.FirstSeqNo,
// 					//
// 					time.Since(start),
// 					doneInfo.Error,
// 				)
// 			}
// 		}
// 		t.OnWriterReadUnknownGrpcMessage = func(info trace.TopicOnWriterReadUnknownGrpcMessageInfo) {
// 			ll.Info(
// 				"topic writer receive unknown message from server {writer_instance_id:'%v', session_id:'%v', error: '%v'}",
// 				info.WriterInstanceID,
// 				info.SessionID,
// 				info.Error,
// 			)
// 		}
// 	}

// 	return t
// }
