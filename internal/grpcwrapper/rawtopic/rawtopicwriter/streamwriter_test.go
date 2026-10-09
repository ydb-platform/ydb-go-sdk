package rawtopicwriter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamWriterRecvPreservesServerIssuesInOperationError(t *testing.T) {
	stream := &stubGrpcStream{response: &Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_OVERLOADED,
		Issues: []*Ydb_Issue.IssueMessage{{
			IssueCode: xerrors.IssueCodeTopicPartitionInactive,
			Message:   "partition is inactive",
		}},
	}}
	writer := &StreamWriter{Stream: stream, Tracer: &trace.Topic{}}

	_, err := writer.Recv()

	assert.True(t, xerrors.IsOperationErrorTopicPartitionInactive(err), err)
}

type stubGrpcStream struct {
	response *Ydb_Topic.StreamWriteMessage_FromServer
}

func (s *stubGrpcStream) Send(*Ydb_Topic.StreamWriteMessage_FromClient) error {
	return nil
}

func (s *stubGrpcStream) Recv() (*Ydb_Topic.StreamWriteMessage_FromServer, error) {
	return s.response, nil
}

func (s *stubGrpcStream) CloseSend() error {
	return nil
}
