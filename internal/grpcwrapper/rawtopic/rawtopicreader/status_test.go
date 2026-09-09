package rawtopicreader

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamReaderPreservesServerStatus(t *testing.T) {
	for _, status := range []Ydb.StatusIds_StatusCode{Ydb.StatusIds_OVERLOADED, Ydb.StatusIds_UNAUTHORIZED} {
		t.Run(status.String(), func(t *testing.T) {
			stream := StreamReader{
				Stream: statusTestStream{message: &Ydb_Topic.StreamReadMessage_FromServer{
					Status: status, Issues: []*Ydb_Issue.IssueMessage{{Message: "server issue"}},
				}},
				Tracer: &trace.Topic{},
			}
			message, err := stream.Recv()
			require.Nil(t, message)
			operationErr := xerrors.OperationError(err)
			require.NotNil(t, operationErr)
			require.Equal(t, int32(status), operationErr.Code())
			require.ErrorContains(t, err, "server issue")
		})
	}
}

type statusTestStream struct {
	GrpcStream

	message *Ydb_Topic.StreamReadMessage_FromServer
}

func (s statusTestStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	return s.message, nil
}
