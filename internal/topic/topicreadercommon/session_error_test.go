package topicreadercommon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
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
			name:       "ydb",
			err:        xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
			statusCode: "BAD_SESSION",
			errorType:  "ydb_error",
		},
		{
			name:       "unknown ydb status",
			err:        xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_StatusCode(499999))),
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

func TestClassifySessionErrorPreservesRawTopicStatus(t *testing.T) {
	reader := rawtopicreader.StreamReader{
		Stream: &rawTopicSessionErrorStream{
			response: &Ydb_Topic.StreamReadMessage_FromServer{
				Status: Ydb.StatusIds_UNAUTHORIZED,
			},
		},
		Tracer: &trace.Topic{},
	}

	_, err := reader.Recv()
	require.Error(t, err)
	require.Equal(t, SessionErrorClassification{
		StatusCode: "UNAUTHORIZED", ErrorType: "ydb_error",
	}, ClassifySessionError(err))
}

type rawTopicSessionErrorStream struct {
	response *Ydb_Topic.StreamReadMessage_FromServer
}

func (s *rawTopicSessionErrorStream) Send(*Ydb_Topic.StreamReadMessage_FromClient) error {
	return nil
}

func (s *rawTopicSessionErrorStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	return s.response, nil
}

func (s *rawTopicSessionErrorStream) CloseSend() error {
	return nil
}
