package rawtopicwriter

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errConcurencyReadDenied = xerrors.Wrap(errors.New("ydb: read from rawtopicwriter in parallel"))

type GrpcStream interface {
	Send(messageNew *Ydb_Topic.StreamWriteMessage_FromClient) error
	Recv() (*Ydb_Topic.StreamWriteMessage_FromServer, error)
	CloseSend() error
}

type StreamWriter struct {
	readCounter int32

	sendCloseMtx sync.Mutex
	Stream       GrpcStream
}

func (w *StreamWriter) Recv() (ServerMessage, error) {
	readCnt := atomic.AddInt32(&w.readCounter, 1)
	defer atomic.AddInt32(&w.readCounter, -1)

	if readCnt != 1 {
		return nil, xerrors.WithStackTrace(errConcurencyReadDenied)
	}

	grpcMsg, err := w.Stream.Recv()
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: failed to read grpc message from writer stream: %w",
			err,
		)))
	}

	var meta rawtopiccommon.ServerMessageMetadata
	if err = meta.MetaFromStatusAndIssues(grpcMsg); err != nil {
		return nil, err
	}
	if !meta.Status.IsSuccess() {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: bad status from topic server: %v", meta.Status))
	}

	switch v := grpcMsg.ServerMessage.(type) {
	case *Ydb_Topic.StreamWriteMessage_FromServer_InitResponse:
		var res InitResult
		res.ServerMessageMetadata = meta
		res.mustFromProto(v.InitResponse)
		return &res, nil
	case *Ydb_Topic.StreamWriteMessage_FromServer_WriteResponse:
		var res WriteResult
		res.ServerMessageMetadata = meta
		err = res.fromProto(v.WriteResponse)
		if err != nil {
			return nil, err
		}
		return &res, nil
	case *Ydb_Topic.StreamWriteMessage_FromServer_UpdateTokenResponse:
		var res UpdateTokenResponse
		res.MustFromProto(v.UpdateTokenResponse)
		return &res, nil
	default:
		return nil, xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: unexpected message type received from raw writer stream: '%v'",
			reflect.TypeOf(grpcMsg),
		)))
	}
}

func (w *StreamWriter) Send(rawMsg ClientMessage) error {
	w.sendCloseMtx.Lock()
	defer w.sendCloseMtx.Unlock()

	var protoMsg Ydb_Topic.StreamWriteMessage_FromClient
	switch v := rawMsg.(type) {
	case *InitRequest:
		initReqProto, err := v.toProto()
		if err != nil {
			return err
		}
		protoMsg.ClientMessage = &Ydb_Topic.StreamWriteMessage_FromClient_InitRequest{
			InitRequest: initReqProto,
		}
	case *WriteRequest:
		writeReqProto, err := v.toProto()
		if err != nil {
			return err
		}
		protoMsg.ClientMessage = writeReqProto
	case *UpdateTokenRequest:
		protoMsg.ClientMessage = &Ydb_Topic.StreamWriteMessage_FromClient_UpdateTokenRequest{
			UpdateTokenRequest: v.ToProto(),
		}
	default:
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf(
			"ydb: unexpected message type for send to raw writer stream: '%v'",
			reflect.TypeOf(rawMsg),
		)))
	}

	err := w.Stream.Send(&protoMsg)
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Wrap(fmt.Errorf("ydb: failed to send grpc message to writer stream: %w", err)))
	}
	return nil
}

func (w *StreamWriter) CloseSend() error {
	w.sendCloseMtx.Lock()
	defer w.sendCloseMtx.Unlock()

	return w.Stream.CloseSend()
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
