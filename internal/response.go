package internal

import (
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Response interface {
	GetOpReady() bool
	GetOpID() string
	GetStatus() Ydb.StatusIds_StatusCode
	GetIssues() []*Ydb_Issue.IssueMessage
	GetResult() *anypb.Any
	GetResponseProto() proto.Message
}

type OpResponse interface {
	proto.Message
	GetOperation() *Ydb_Operations.Operation
}

var _ Response = &opResponseWrapper{}

type opResponseWrapper struct {
	response OpResponse
}

func WrapOpResponse(resp OpResponse) Response {
	return &opResponseWrapper{response: resp}
}

func (r *opResponseWrapper) GetOpReady() bool {
	return r.response.GetOperation().GetReady()
}

func (r *opResponseWrapper) GetOpID() string {
	return r.response.GetOperation().GetId()
}

func (r *opResponseWrapper) GetStatus() Ydb.StatusIds_StatusCode {
	return r.response.GetOperation().GetStatus()
}

func (r *opResponseWrapper) GetIssues() []*Ydb_Issue.IssueMessage {
	return r.response.GetOperation().GetIssues()
}

func (r *opResponseWrapper) GetResult() *any.Any {
	return r.response.GetOperation().GetResult()
}

func (r *opResponseWrapper) GetResponseProto() proto.Message {
	return r.response
}

type NoOpResponse interface {
	proto.Message
	GetStatus() Ydb.StatusIds_StatusCode
	GetIssues() []*Ydb_Issue.IssueMessage
}

var _ Response = &noOpResponseWrapper{}

type noOpResponseWrapper struct {
	response NoOpResponse
}

func WrapNoOpResponse(resp NoOpResponse) Response {
	return &noOpResponseWrapper{response: resp}
}

func (r *noOpResponseWrapper) GetIssues() []*Ydb_Issue.IssueMessage {
	return r.response.GetIssues()
}

func (r *noOpResponseWrapper) GetOpReady() bool {
	return true
}

func (r *noOpResponseWrapper) GetOpID() string {
	return ""
}

func (r *noOpResponseWrapper) GetResponseProto() proto.Message {
	return r.response
}

func (r *noOpResponseWrapper) GetResult() *any.Any {
	return nil
}

func (r *noOpResponseWrapper) GetStatus() Ydb.StatusIds_StatusCode {
	return r.response.GetStatus()
}
