package rawtopicreader

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Check interface implementation
var _ TopicReaderStreamInterface = StreamReader{}

func TestStreamReaderRecvPreservesOperationStatus(t *testing.T) {
	reader := StreamReader{
		Stream: &statusGRPCStream{response: &Ydb_Topic.StreamReadMessage_FromServer{
			Status: Ydb.StatusIds_OVERLOADED,
		}},
		Tracer: &trace.Topic{},
	}

	_, err := reader.Recv()

	require.Error(t, err)
	require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED))
}

type statusGRPCStream struct {
	response *Ydb_Topic.StreamReadMessage_FromServer
}

func (*statusGRPCStream) Send(*Ydb_Topic.StreamReadMessage_FromClient) error {
	return nil
}

func (s *statusGRPCStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	return s.response, nil
}

func (*statusGRPCStream) CloseSend() error {
	return nil
}
