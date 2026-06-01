package rawtopicreader

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"google.golang.org/protobuf/proto"
)

var ErrUnexpectedMessageType = errors.New("unexpected message type")

type GrpcStream interface {
	Send(messageNew *Ydb_Topic.StreamReadMessage_FromClient) error
	Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error)
	CloseSend() error
}

type StreamReader struct {
	Stream   GrpcStream
	ReaderID int64

	Tracer              *trace.Topic
	sessionID           string
	sentMessageCount    int
	receiveMessageCount int
}

func (s StreamReader) CloseSend() error {
	return s.Stream.CloseSend()
}

//nolint:funlen
func (s StreamReader) Recv() (_ ServerMessage, resErr error) {
	grpcMess, err := s.Stream.Recv()

	defer func() {
		s.receiveMessageCount++

		gtrace.TopicOnReaderReceiveGRPCMessage(
			s.Tracer,
			s.ReaderID,
			s.sessionID,
			s.receiveMessageCount,
			grpcMess,
			resErr,
		)
	}()

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

	switch grpcMess.WhichServerMessage() {
	case Ydb_Topic.StreamReadMessage_FromServer_InitResponse_case:
		s.sessionID = grpcMess.GetInitResponse().GetSessionId()

		resp := &InitResponse{}
		resp.ServerMessageMetadata = meta
		resp.fromProto(grpcMess.GetInitResponse())

		return resp, nil
	case Ydb_Topic.StreamReadMessage_FromServer_ReadResponse_case:
		resp := &ReadResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(grpcMess.GetReadResponse()); err != nil {
			return nil, err
		}

		return resp, nil
	case Ydb_Topic.StreamReadMessage_FromServer_StartPartitionSessionRequest_case:
		resp := &StartPartitionSessionRequest{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(grpcMess.GetStartPartitionSessionRequest()); err != nil {
			return nil, err
		}

		return resp, nil
	case Ydb_Topic.StreamReadMessage_FromServer_StopPartitionSessionRequest_case:
		req := &StopPartitionSessionRequest{}
		req.ServerMessageMetadata = meta
		if err = req.fromProto(grpcMess.GetStopPartitionSessionRequest()); err != nil {
			return nil, err
		}

		return req, nil

	case Ydb_Topic.StreamReadMessage_FromServer_EndPartitionSession_case:
		req := &EndPartitionSession{}
		req.ServerMessageMetadata = meta
		if err = req.fromProto(grpcMess.GetEndPartitionSession()); err != nil {
			return nil, err
		}

		return req, nil

	case Ydb_Topic.StreamReadMessage_FromServer_CommitOffsetResponse_case:
		resp := &CommitOffsetResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(grpcMess.GetCommitOffsetResponse()); err != nil {
			return nil, err
		}

		return resp, nil
	case Ydb_Topic.StreamReadMessage_FromServer_PartitionSessionStatusResponse_case:
		resp := &PartitionSessionStatusResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(grpcMess.GetPartitionSessionStatusResponse()); err != nil {
			return nil, err
		}

		return resp, nil
	case Ydb_Topic.StreamReadMessage_FromServer_UpdateTokenResponse_case:
		resp := &UpdateTokenResponse{}
		resp.ServerMessageMetadata = meta
		resp.MustFromProto(grpcMess.GetUpdateTokenResponse())

		return resp, nil
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf(
			"ydb: receive unexpected message (%v): %w",
			grpcMess.WhichServerMessage(),
			ErrUnexpectedMessageType,
		))
	}
}

//nolint:funlen
func (s StreamReader) Send(msg ClientMessage) (resErr error) {
	defer func() {
		resErr = xerrors.Transport(resErr)
	}()

	var grpcMess *Ydb_Topic.StreamReadMessage_FromClient
	switch m := msg.(type) {
	case *InitRequest:
		grpcMess = Ydb_Topic.StreamReadMessage_FromClient_builder{
			InitRequest: proto.ValueOrDefault(m.toProto()),
		}.Build()

	case *ReadRequest:
		grpcMess = Ydb_Topic.StreamReadMessage_FromClient_builder{
			ReadRequest: proto.ValueOrDefault(m.toProto()),
		}.Build()
	case *StartPartitionSessionResponse:
		grpcMess = Ydb_Topic.StreamReadMessage_FromClient_builder{
			StartPartitionSessionResponse: proto.ValueOrDefault(m.toProto()),
		}.Build()
	case *StopPartitionSessionResponse:
		grpcMess = Ydb_Topic.StreamReadMessage_FromClient_builder{
			StopPartitionSessionResponse: proto.ValueOrDefault(m.toProto()),
		}.Build()

	case *CommitOffsetRequest:
		grpcMess = Ydb_Topic.StreamReadMessage_FromClient_builder{
			CommitOffsetRequest: proto.ValueOrDefault(m.toProto()),
		}.Build()

	case *PartitionSessionStatusRequest:
		grpcMess = Ydb_Topic.StreamReadMessage_FromClient_builder{
			PartitionSessionStatusRequest: proto.ValueOrDefault(m.toProto()),
		}.Build()

	case *UpdateTokenRequest:
		grpcMess = Ydb_Topic.StreamReadMessage_FromClient_builder{
			UpdateTokenRequest: proto.ValueOrDefault(m.ToProto()),
		}.Build()
	}

	if grpcMess == nil {
		resErr = xerrors.WithStackTrace(fmt.Errorf("ydb: send unexpected message type: %v", reflect.TypeOf(msg)))
	} else {
		resErr = s.Stream.Send(grpcMess)
	}

	s.sentMessageCount++
	gtrace.TopicOnReaderSentGRPCMessage(
		s.Tracer,
		s.ReaderID,
		s.sessionID,
		s.sentMessageCount,
		grpcMess,
		resErr,
	)

	return resErr
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
