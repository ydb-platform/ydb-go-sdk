//nolint:tagliatelle // Benchmark reports intentionally use analysis-friendly snake_case JSON.
package main

import (
	"context"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type instrumentation struct {
	describeTopicCalls   atomic.Uint64
	streamWriteOpens     atomic.Uint64
	writerInitAttempts   atomic.Uint64
	writerInitErrors     atomic.Uint64
	writerCloseEvents    atomic.Uint64
	writerCloseErrors    atomic.Uint64
	writeRequests        atomic.Uint64
	writeRequestMessages atomic.Uint64
	writeRequestErrors   atomic.Uint64
	acknowledgedMessages atomic.Uint64
	writtenInTxMessages  atomic.Uint64
	skippedMessages      atomic.Uint64
}

type instrumentationSnapshot struct {
	DescribeTopicCalls   uint64 `json:"describe_topic_calls"`
	StreamWriteOpens     uint64 `json:"stream_write_opens"`
	WriterInitAttempts   uint64 `json:"writer_init_attempts"`
	WriterInitErrors     uint64 `json:"writer_init_errors"`
	WriterCloseEvents    uint64 `json:"writer_close_events"`
	WriterCloseErrors    uint64 `json:"writer_close_errors"`
	WriteRequests        uint64 `json:"write_requests"`
	WriteRequestMessages uint64 `json:"write_request_messages"`
	WriteRequestErrors   uint64 `json:"write_request_errors"`
	AcknowledgedMessages uint64 `json:"acknowledged_messages"`
	WrittenInTxMessages  uint64 `json:"written_in_tx_messages"`
	SkippedMessages      uint64 `json:"skipped_messages"`
}

func (m *instrumentation) snapshot() instrumentationSnapshot {
	return instrumentationSnapshot{
		DescribeTopicCalls:   m.describeTopicCalls.Load(),
		StreamWriteOpens:     m.streamWriteOpens.Load(),
		WriterInitAttempts:   m.writerInitAttempts.Load(),
		WriterInitErrors:     m.writerInitErrors.Load(),
		WriterCloseEvents:    m.writerCloseEvents.Load(),
		WriterCloseErrors:    m.writerCloseErrors.Load(),
		WriteRequests:        m.writeRequests.Load(),
		WriteRequestMessages: m.writeRequestMessages.Load(),
		WriteRequestErrors:   m.writeRequestErrors.Load(),
		AcknowledgedMessages: m.acknowledgedMessages.Load(),
		WrittenInTxMessages:  m.writtenInTxMessages.Load(),
		SkippedMessages:      m.skippedMessages.Load(),
	}
}

func (s instrumentationSnapshot) subtract(before instrumentationSnapshot) instrumentationSnapshot {
	return instrumentationSnapshot{
		DescribeTopicCalls:   s.DescribeTopicCalls - before.DescribeTopicCalls,
		StreamWriteOpens:     s.StreamWriteOpens - before.StreamWriteOpens,
		WriterInitAttempts:   s.WriterInitAttempts - before.WriterInitAttempts,
		WriterInitErrors:     s.WriterInitErrors - before.WriterInitErrors,
		WriterCloseEvents:    s.WriterCloseEvents - before.WriterCloseEvents,
		WriterCloseErrors:    s.WriterCloseErrors - before.WriterCloseErrors,
		WriteRequests:        s.WriteRequests - before.WriteRequests,
		WriteRequestMessages: s.WriteRequestMessages - before.WriteRequestMessages,
		WriteRequestErrors:   s.WriteRequestErrors - before.WriteRequestErrors,
		AcknowledgedMessages: s.AcknowledgedMessages - before.AcknowledgedMessages,
		WrittenInTxMessages:  s.WrittenInTxMessages - before.WrittenInTxMessages,
		SkippedMessages:      s.SkippedMessages - before.SkippedMessages,
	}
}

func (m *instrumentation) unaryClientInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	if strings.HasSuffix(method, "/DescribeTopic") {
		m.describeTopicCalls.Add(1)
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}

func (m *instrumentation) streamClientInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if strings.HasSuffix(method, "/StreamWrite") {
		m.streamWriteOpens.Add(1)
	}

	return streamer(ctx, desc, cc, method, opts...)
}

func (m *instrumentation) topicTrace() trace.Topic {
	return trace.Topic{
		OnWriterInitStream: func(trace.TopicWriterInitStreamStartInfo) func(trace.TopicWriterInitStreamDoneInfo) {
			m.writerInitAttempts.Add(1)

			return func(info trace.TopicWriterInitStreamDoneInfo) {
				if info.Error != nil {
					m.writerInitErrors.Add(1)
				}
			}
		},
		OnWriterClose: func(trace.TopicWriterCloseStartInfo) func(trace.TopicWriterCloseDoneInfo) {
			m.writerCloseEvents.Add(1)

			return func(info trace.TopicWriterCloseDoneInfo) {
				if info.Error != nil {
					m.writerCloseErrors.Add(1)
				}
			}
		},
		OnWriterSendMessages: func(info trace.TopicWriterSendMessagesStartInfo) func(trace.TopicWriterSendMessagesDoneInfo) {
			m.writeRequests.Add(1)
			m.writeRequestMessages.Add(uint64(info.MessagesCount))

			return func(info trace.TopicWriterSendMessagesDoneInfo) {
				if info.Error != nil {
					m.writeRequestErrors.Add(1)
				}
			}
		},
		OnWriterReceiveResult: func(info trace.TopicWriterResultMessagesInfo) {
			acks := info.Acks.GetAcks()
			m.acknowledgedMessages.Add(uint64(acks.AcksCount))
			m.writtenInTxMessages.Add(uint64(acks.WrittenInTxCount))
			m.skippedMessages.Add(uint64(acks.SkipCount))
		},
	}
}
