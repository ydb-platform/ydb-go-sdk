// nolint
package rawtopicwriter

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type GrpcStream interface {
	Send(messageNew *Ydb_PersQueue_V1.StreamingWriteClientMessage) error
	Recv() (*Ydb_PersQueue_V1.StreamingWriteServerMessage, error)
	CloseSend() error
}

type StreamWriter struct {
	Stream GrpcStream
}

func (w *StreamWriter) Recv() (ServerMessage, error) {
	panic("not implemented")
}

func (w *StreamWriter) Send(msg ClientMessage) error {
	panic("not implemented")
}

type ClientMessage interface {
	isClientMessage()
}

type clientMessageImpl struct{}

func (*clientMessageImpl) isClientMessage() {}

type ServerMessage interface {
	isServerMessage()
	StatusData() rawtopiccommon.ServerMessageMetadata
	SetStatus(status rawydb.StatusCode)
}

type serverMessageImpl struct{}

func (*serverMessageImpl) isServerMessage() {}
