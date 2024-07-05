package rawtopicreader

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var ErrUnexpectedMessageType = errors.New("unexpected message type")

type GrpcStream interface {
	Send(messageNew *Ydb_Topic.StreamReadMessage_FromClient) error
	Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error)
	CloseSend() error
}

type StreamReader struct {
	Stream GrpcStream
}

func (s StreamReader) CloseSend() error {
	return s.Stream.CloseSend()
}

//nolint:funlen
func (s StreamReader) Recv() (ServerMessage, error) {
	grpcMess, err := s.Stream.Recv()
	if xerrors.Is(err, io.EOF) {
		return nil, err
	}
	if err != nil {
		if !xerrors.IsErrorFromServer(err) {
			err = xerrors.Transport(err)
		}

		return nil, err
	}

	var meta rawtopiccommon.ServerMessageMetadata
	if err = meta.MetaFromStatusAndIssues(grpcMess); err != nil {
		return nil, err
	}
	if !meta.Status.IsSuccess() {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: bad status from topic server: %v", meta.Status))
	}

	switch m := grpcMess.GetServerMessage().(type) {
	case *Ydb_Topic.StreamReadMessage_FromServer_InitResponse:
		resp := &InitResponse{}
		resp.ServerMessageMetadata = meta
		resp.fromProto(m.InitResponse)

		return resp, nil
	case *Ydb_Topic.StreamReadMessage_FromServer_ReadResponse:
		resp := &ReadResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(m.ReadResponse); err != nil {
			return nil, err
		}

		return resp, nil
	case *Ydb_Topic.StreamReadMessage_FromServer_StartPartitionSessionRequest:
		resp := &StartPartitionSessionRequest{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(m.StartPartitionSessionRequest); err != nil {
			return nil, err
		}

		return resp, nil
	case *Ydb_Topic.StreamReadMessage_FromServer_StopPartitionSessionRequest:
		req := &StopPartitionSessionRequest{}
		req.ServerMessageMetadata = meta
		if err = req.fromProto(m.StopPartitionSessionRequest); err != nil {
			return nil, err
		}

		return req, nil
	case *Ydb_Topic.StreamReadMessage_FromServer_CommitOffsetResponse:
		resp := &CommitOffsetResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(m.CommitOffsetResponse); err != nil {
			return nil, err
		}

		return resp, nil
	case *Ydb_Topic.StreamReadMessage_FromServer_PartitionSessionStatusResponse:
		resp := &PartitionSessionStatusResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(m.PartitionSessionStatusResponse); err != nil {
			return nil, err
		}

		return resp, nil
	case *Ydb_Topic.StreamReadMessage_FromServer_UpdateTokenResponse:
		resp := &UpdateTokenResponse{}
		resp.ServerMessageMetadata = meta
		resp.MustFromProto(m.UpdateTokenResponse)

		return resp, nil
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf(
			"ydb: receive unexpected message (%v): %w",
			reflect.TypeOf(grpcMess.GetServerMessage()),
			ErrUnexpectedMessageType,
		))
	}
}

func (s StreamReader) Send(msg ClientMessage) (err error) {
	defer func() {
		err = xerrors.Transport(err)
	}()
	switch m := msg.(type) {
	case *InitRequest:
		grpcMess := &Ydb_Topic.StreamReadMessage_FromClient{
			ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_InitRequest{InitRequest: m.toProto()},
		}

		return s.Stream.Send(grpcMess)
	case *ReadRequest:
		grpcMess := &Ydb_Topic.StreamReadMessage_FromClient{
			ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_ReadRequest{ReadRequest: m.toProto()},
		}

		return s.Stream.Send(grpcMess)
	case *StartPartitionSessionResponse:
		grpcMess := &Ydb_Topic.StreamReadMessage_FromClient{
			ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_StartPartitionSessionResponse{
				StartPartitionSessionResponse: m.toProto(),
			},
		}

		return s.Stream.Send(grpcMess)
	case *StopPartitionSessionResponse:
		grpcMess := &Ydb_Topic.StreamReadMessage_FromClient{
			ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_StopPartitionSessionResponse{
				StopPartitionSessionResponse: m.toProto(),
			},
		}

		return s.Stream.Send(grpcMess)
	case *CommitOffsetRequest:
		grpcMess := &Ydb_Topic.StreamReadMessage_FromClient{
			ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_CommitOffsetRequest{
				CommitOffsetRequest: m.toProto(),
			},
		}

		return s.Stream.Send(grpcMess)
	case *PartitionSessionStatusRequest:
		grpcMess := &Ydb_Topic.StreamReadMessage_FromClient{
			ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_PartitionSessionStatusRequest{
				PartitionSessionStatusRequest: m.toProto(),
			},
		}

		return s.Stream.Send(grpcMess)
	case *UpdateTokenRequest:
		grpcMess := &Ydb_Topic.StreamReadMessage_FromClient{
			ClientMessage: &Ydb_Topic.StreamReadMessage_FromClient_UpdateTokenRequest{
				UpdateTokenRequest: m.ToProto(),
			},
		}

		return s.Stream.Send(grpcMess)
	default:
		return xerrors.WithStackTrace(fmt.Errorf("ydb: send unexpected message type: %v", reflect.TypeOf(msg)))
	}
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
