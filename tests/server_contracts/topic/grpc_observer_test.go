package topicresearch_test

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type grpcObserver struct {
	mu         sync.Mutex
	eventSink  func(observedGRPCEvent)
	transcript []string
}

type observedGRPCEvent struct {
	direction   string
	method      string
	messageName string
	details     string
}

type observedQueryClientStream struct {
	grpc.ClientStream

	observer *grpcObserver
}

func newGRPCObserver() *grpcObserver {
	return &grpcObserver{}
}

func (b *grpcObserver) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		conn *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		options ...grpc.CallOption,
	) error {
		if queryMethod, ok := observedQueryUnaryMethod(method); ok {
			b.recordQueryEvent("client ->", queryMethod, req)
			err := invoker(ctx, method, req, reply, conn, options...)
			if err != nil {
				b.appendGRPCEvent(observedGRPCEvent{
					direction: "server ->",
					method:    queryMethod,
					details:   "transport error: " + err.Error(),
				})
				b.appendTranscript("server ended " + queryGRPCFullMethod(queryMethod) + ": " + err.Error())

				return err
			}
			b.recordQueryResponse(queryMethod, req, reply)

			return nil
		}
		if method == Ydb_Topic_V1.TopicService_DescribeTopic_FullMethodName {
			b.recordTopicDescribeEvent("client ->", req)
			err := invoker(ctx, method, req, reply, conn, options...)
			if err != nil {
				b.appendGRPCEvent(observedGRPCEvent{
					direction: "server ->",
					method:    method,
					details:   "transport error: " + err.Error(),
				})
				b.appendTranscript("server ended " + method + ": " + err.Error())

				return err
			}
			b.recordTopicDescribeEvent("server ->", reply)

			return nil
		}

		return invoker(ctx, method, req, reply, conn, options...)
	}
}

func (b *grpcObserver) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		description *grpc.StreamDesc,
		conn *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		options ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, description, conn, method, options...)
		if err != nil {
			return stream, err
		}
		if method == Ydb_Query_V1.QueryService_AttachSession_FullMethodName {
			return &observedQueryClientStream{ClientStream: stream, observer: b}, nil
		}

		return stream, nil
	}
}

func (b *grpcObserver) Transcript() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return joinTranscript(b.transcript)
}

func (s *observedQueryClientStream) SendMsg(message any) error {
	s.observer.recordQueryEvent("client ->", "AttachSession", message)

	return s.ClientStream.SendMsg(message)
}

func (s *observedQueryClientStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	if err != nil {
		s.observer.appendGRPCEvent(observedGRPCEvent{
			direction: "server ->",
			method:    "AttachSession",
			details:   "stream ended: " + err.Error(),
		})
		s.observer.appendTranscript(
			"server ended " + Ydb_Query_V1.QueryService_AttachSession_FullMethodName + ": " + err.Error(),
		)

		return err
	}
	s.observer.recordQueryEvent("server ->", "AttachSession", message)

	return nil
}

func observedQueryUnaryMethod(method string) (string, bool) {
	switch method {
	case Ydb_Query_V1.QueryService_CreateSession_FullMethodName:
		return "CreateSession", true
	case Ydb_Query_V1.QueryService_BeginTransaction_FullMethodName:
		return "BeginTransaction", true
	case Ydb_Query_V1.QueryService_CommitTransaction_FullMethodName:
		return "CommitTransaction", true
	case Ydb_Query_V1.QueryService_RollbackTransaction_FullMethodName:
		return "RollbackTransaction", true
	default:
		return "", false
	}
}

func (b *grpcObserver) recordQueryEvent(direction, method string, message any) {
	event := observedGRPCEvent{
		direction:   direction,
		method:      method,
		messageName: protobufMessageNameFromAny(message),
		details:     summarizeQueryMessage(message),
	}
	b.appendGRPCEvent(event)
	b.record(direction+" "+queryGRPCFullMethod(method)+" / "+event.messageName, message)
}

func (b *grpcObserver) recordQueryResponse(method string, request, response any) {
	details := summarizeQueryMessage(response)
	switch request := request.(type) {
	case *Ydb_Query.BeginTransactionRequest:
		details = fmt.Sprintf("for session_id=%q, %s", request.GetSessionId(), details)
	case *Ydb_Query.CommitTransactionRequest:
		details = fmt.Sprintf("for tx_id=%q, %s", request.GetTxId(), details)
	case *Ydb_Query.RollbackTransactionRequest:
		details = fmt.Sprintf("for tx_id=%q, %s", request.GetTxId(), details)
	}
	b.appendGRPCEvent(observedGRPCEvent{
		direction:   "server ->",
		method:      method,
		messageName: protobufMessageNameFromAny(response),
		details:     details,
	})
	b.record(
		"server -> "+queryGRPCFullMethod(method)+" / "+protobufMessageNameFromAny(response),
		response,
	)
}

func (b *grpcObserver) recordTopicDescribeEvent(direction string, message any) {
	event := observedGRPCEvent{
		direction:   direction,
		method:      Ydb_Topic_V1.TopicService_DescribeTopic_FullMethodName,
		messageName: protobufMessageNameFromAny(message),
		details:     summarizeTopicDescribeMessage(message),
	}
	b.appendGRPCEvent(event)
	b.record(direction+" "+event.method+" / "+event.messageName, message)
}

func queryGRPCFullMethod(method string) string {
	switch method {
	case "CreateSession":
		return Ydb_Query_V1.QueryService_CreateSession_FullMethodName
	case "AttachSession":
		return Ydb_Query_V1.QueryService_AttachSession_FullMethodName
	case "BeginTransaction":
		return Ydb_Query_V1.QueryService_BeginTransaction_FullMethodName
	case "CommitTransaction":
		return Ydb_Query_V1.QueryService_CommitTransaction_FullMethodName
	case "RollbackTransaction":
		return Ydb_Query_V1.QueryService_RollbackTransaction_FullMethodName
	default:
		return method
	}
}

func protobufMessageNameFromAny(message any) string {
	protobuf, ok := message.(proto.Message)
	if !ok {
		return fmt.Sprintf("%T", message)
	}

	return protobufMessageName(protobuf)
}

func (b *grpcObserver) appendGRPCEvent(event observedGRPCEvent) {
	b.mu.Lock()
	sink := b.eventSink
	b.mu.Unlock()
	if sink != nil {
		sink(event)
	}
}

func (b *grpcObserver) SetEventSink(observer func(observedGRPCEvent)) {
	b.mu.Lock()
	b.eventSink = observer
	b.mu.Unlock()
}

func summarizeQueryMessage(message any) string {
	switch message := message.(type) {
	case *Ydb_Query.CreateSessionRequest:
		return "{}"
	case *Ydb_Query.CreateSessionResponse:
		return fmt.Sprintf(
			"status=%s, session_id=%q, node_id=%d%s",
			message.GetStatus(),
			message.GetSessionId(),
			message.GetNodeId(),
			summarizeQueryIssues(message.GetIssues()),
		)
	case *Ydb_Query.AttachSessionRequest:
		return fmt.Sprintf("session_id=%q", message.GetSessionId())
	case *Ydb_Query.SessionState:
		state := "attached"
		if message.GetSessionShutdown() != nil {
			state = "session_shutdown"
		} else if message.GetNodeShutdown() != nil {
			state = "node_shutdown"
		}

		return fmt.Sprintf("status=%s, state=%s", message.GetStatus(), state)
	case *Ydb_Query.BeginTransactionRequest:
		mode := "unknown"
		if message.GetTxSettings().GetSerializableReadWrite() != nil {
			mode = "serializable_read_write"
		}

		return fmt.Sprintf("session_id=%q, tx_mode=%s", message.GetSessionId(), mode)
	case *Ydb_Query.BeginTransactionResponse:
		return fmt.Sprintf(
			"status=%s, tx_id=%q%s",
			message.GetStatus(),
			message.GetTxMeta().GetId(),
			summarizeQueryIssues(message.GetIssues()),
		)
	case *Ydb_Query.CommitTransactionRequest:
		return fmt.Sprintf("session_id=%q, tx_id=%q", message.GetSessionId(), message.GetTxId())
	case *Ydb_Query.CommitTransactionResponse:
		return fmt.Sprintf("status=%s%s", message.GetStatus(), summarizeQueryIssues(message.GetIssues()))
	case *Ydb_Query.RollbackTransactionRequest:
		return fmt.Sprintf("session_id=%q, tx_id=%q", message.GetSessionId(), message.GetTxId())
	case *Ydb_Query.RollbackTransactionResponse:
		return fmt.Sprintf("status=%s%s", message.GetStatus(), summarizeQueryIssues(message.GetIssues()))
	default:
		return fmt.Sprintf("%T", message)
	}
}

func summarizeTopicDescribeMessage(message any) string {
	switch message := message.(type) {
	case *Ydb_Topic.DescribeTopicRequest:
		return fmt.Sprintf(
			"path=%q, include_stats=%t, include_location=%t",
			message.GetPath(),
			message.GetIncludeStats(),
			message.GetIncludeLocation(),
		)
	case *Ydb_Topic.DescribeTopicResponse:
		operation := message.GetOperation()

		return fmt.Sprintf(
			"operation={ready=%t, status=%s%s}",
			operation.GetReady(),
			operation.GetStatus(),
			summarizeQueryIssues(operation.GetIssues()),
		)
	default:
		return fmt.Sprintf("%T", message)
	}
}

func summarizeQueryIssues(issues []*Ydb_Issue.IssueMessage) string {
	if len(issues) == 0 {
		return ""
	}
	descriptions := make([]string, 0, len(issues))
	for _, issue := range issues {
		descriptions = append(descriptions, fmt.Sprintf(
			"#%d %q",
			issue.GetIssueCode(),
			issue.GetMessage(),
		))
	}

	return ", issues=[" + strings.Join(descriptions, "; ") + "]"
}

func (b *grpcObserver) record(direction string, message any) {
	protobuf, ok := message.(proto.Message)
	if !ok {
		b.appendTranscript(fmt.Sprintf("%s: %T", direction, message))

		return
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(protobuf)
	if err != nil {
		b.appendTranscript(fmt.Sprintf("%s: <%T marshal error: %v>", direction, message, err))

		return
	}
	b.appendTranscript(direction + ": " + string(encoded))
}

func (b *grpcObserver) appendTranscript(line string) {
	b.mu.Lock()
	b.transcript = append(b.transcript, line)
	b.mu.Unlock()
}

func joinTranscript(lines []string) string {
	if len(lines) == 0 {
		return "<empty>"
	}

	return strings.Join(lines, "\n")
}
