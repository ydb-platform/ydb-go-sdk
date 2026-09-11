package topicreadercommon

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestClassifySessionError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode string
		errorType  string
	}{
		{
			name:       "transport",
			err:        grpcStatus.Error(grpcCodes.Unavailable, "connection lost"),
			statusCode: "Unavailable",
			errorType:  "transport_error",
		},
		{
			name:       "wrapped transport",
			err:        xerrors.WithStackTrace(grpcStatus.Error(grpcCodes.DeadlineExceeded, "timeout")),
			statusCode: "DeadlineExceeded",
			errorType:  "transport_error",
		},
		{
			name:       "unknown grpc status",
			err:        grpcStatus.Error(grpcCodes.Code(99), "future transport code"),
			statusCode: "Code(99)",
			errorType:  "transport_error",
		},
		{
			name:       "ydb",
			err:        xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			statusCode: "BAD_SESSION",
			errorType:  "ydb_error",
		},
		{
			name:       "unknown ydb status",
			err:        xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_StatusCode(499999))),
			statusCode: "499999",
			errorType:  "ydb_error",
		},
		{
			name:       "unspecified ydb status",
			err:        xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED)),
			statusCode: "unknown",
			errorType:  "ydb_error",
		},
		{
			name:       "generic",
			err:        errors.New("unexpected failure"),
			statusCode: "unknown",
			errorType:  "unknown",
		},
		{
			name:       "nil",
			statusCode: "unknown",
			errorType:  "unknown",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifySessionError(test.err)
			require.Equal(t, test.statusCode, got.StatusCode)
			require.Equal(t, test.errorType, got.ErrorType)
		})
	}
}

func TestClassifySessionErrorFromRawTopicStatus(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     Ydb.StatusIds_StatusCode
		statusCode string
	}{
		{name: "unauthorized", status: Ydb.StatusIds_UNAUTHORIZED, statusCode: "UNAUTHORIZED"},
		{name: "overloaded", status: Ydb.StatusIds_OVERLOADED, statusCode: "OVERLOADED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := rawtopicreader.StreamReader{
				Tracer: &trace.Topic{},
				Stream: statusGrpcStream{response: &Ydb_Topic.StreamReadMessage_FromServer{Status: test.status}},
			}
			_, err := reader.Recv()
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("ydb: bad status from topic server: %v", test.status))

			classification := ClassifySessionError(fmt.Errorf("outer: %w", err))
			require.Equal(t, SessionErrorClassification{
				StatusCode: test.statusCode,
				ErrorType:  ydbSessionErrorType,
			}, classification)

			oldPlainError := fmt.Errorf("ydb: bad status from topic server: %v", test.status)
			wantBackoff, wantStop := topic.RetryDecision(
				fmt.Errorf("outer: %w", oldPlainError),
				topic.RetrySettings{},
				0,
			)
			gotBackoff, gotStop := topic.RetryDecision(
				fmt.Errorf("outer: %w", err),
				topic.RetrySettings{},
				0,
			)
			require.Equal(t, wantBackoff, gotBackoff)
			require.Equal(t, wantStop == nil, gotStop == nil)
		})
	}
}

func TestTraceReaderSessionError(t *testing.T) {
	var got trace.TopicReaderSessionErrorInfo
	tracer := &trace.Topic{
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			got = info
		},
	}
	readerInfo := ReaderInfo{
		Endpoint:   "endpoint",
		Database:   "/database",
		Consumer:   "consumer",
		ReaderName: readerNamePointer("reader"),
	}

	TraceReaderSessionError(
		context.Background(),
		tracer,
		readerInfo,
		"retry",
		grpcStatus.Error(grpcCodes.Unavailable, "connection lost"),
	)

	require.Equal(t, "endpoint", got.Endpoint)
	require.Equal(t, "/database", got.Database)
	require.Equal(t, "consumer", got.Consumer)
	require.Equal(t, readerNamePointer("reader"), got.ReaderName)
	require.Equal(t, "retry", got.RetryDecision)
	require.Equal(t, "Unavailable", got.StatusCode)
	require.Equal(t, "transport_error", got.ErrorType)
	require.Error(t, got.Error)
}

func TestTraceReaderSessionErrorHandlesNoOpAndUnknownTransportCode(t *testing.T) {
	readerInfo := ReaderInfo{
		Endpoint:   "endpoint",
		Database:   "database",
		Consumer:   "consumer",
		ReaderName: readerNamePointer("reader"),
	}
	unknownTransportError := grpcStatus.Error(grpcCodes.Code(99), "future transport code")

	TraceReaderSessionError(context.Background(), nil, readerInfo, "stop", unknownTransportError)
	TraceReaderSessionError(context.Background(), &trace.Topic{}, readerInfo, "stop", unknownTransportError)

	calls := 0
	tracer := &trace.Topic{
		OnReaderSessionError: func(trace.TopicReaderSessionErrorInfo) {
			calls++
		},
	}
	TraceReaderSessionError(context.Background(), tracer, readerInfo, "stop", nil)
	require.Zero(t, calls)

	var actual trace.TopicReaderSessionErrorInfo
	tracer.OnReaderSessionError = func(info trace.TopicReaderSessionErrorInfo) {
		actual = info
	}
	TraceReaderSessionError(context.Background(), tracer, readerInfo, "stop", unknownTransportError)

	require.Equal(t, "stop", actual.RetryDecision)
	require.Equal(t, "Code(99)", actual.StatusCode)
	require.Equal(t, "transport_error", actual.ErrorType)
	require.Error(t, actual.Error)
	require.Equal(t, SessionErrorClassification{
		StatusCode: "Code(99)",
		ErrorType:  "transport_error",
	}, ClassifySessionError(unknownTransportError))
	require.ErrorIs(t, actual.Error, unknownTransportError)
}

type statusGrpcStream struct {
	response *Ydb_Topic.StreamReadMessage_FromServer
}

func (s statusGrpcStream) Send(*Ydb_Topic.StreamReadMessage_FromClient) error {
	return nil
}

func (s statusGrpcStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	return s.response, nil
}

func (statusGrpcStream) CloseSend() error {
	return nil
}
