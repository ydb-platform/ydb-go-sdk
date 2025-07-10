package testutil

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var ErrNotImplemented = xerrors.Wrap(fmt.Errorf("testutil: not implemented"))

type MethodCode uint

func (m MethodCode) String() string {
	if method, ok := codeToString[m]; ok {
		return method
	}

	return ""
}

type Method string

func (m Method) Code() MethodCode {
	if code, ok := grpcMethodToCode[m]; ok {
		return code
	}

	return UnknownMethod
}

const (
	UnknownMethod MethodCode = iota
	TableCreateSession
	TableDeleteSession
	TableKeepAlive
	TableCreateTable
	TableDropTable
	TableAlterTable
	TableCopyTable
	TableDescribeTable
	TableExplainDataQuery
	TablePrepareDataQuery
	TableExecuteDataQuery
	TableExecuteSchemeQuery
	TableBeginTransaction
	TableCommitTransaction
	TableRollbackTransaction
	TableDescribeTableOptions
	TableStreamReadTable
	TableStreamExecuteScanQuery
)

var grpcMethodToCode = map[Method]MethodCode{
	"/Ydb.Table.V1.TableService/CreateSession":          TableCreateSession,
	"/Ydb.Table.V1.TableService/DeleteSession":          TableDeleteSession,
	"/Ydb.Table.V1.TableService/KeepAlive":              TableKeepAlive,
	"/Ydb.Table.V1.TableService/CreateTable":            TableCreateTable,
	"/Ydb.Table.V1.TableService/DropTable":              TableDropTable,
	"/Ydb.Table.V1.TableService/AlterTable":             TableAlterTable,
	"/Ydb.Table.V1.TableService/CopyTable":              TableCopyTable,
	"/Ydb.Table.V1.TableService/DescribeTable":          TableDescribeTable,
	"/Ydb.Table.V1.TableService/ExplainDataQuery":       TableExplainDataQuery,
	"/Ydb.Table.V1.TableService/PrepareDataQuery":       TablePrepareDataQuery,
	"/Ydb.Table.V1.TableService/ExecuteDataQuery":       TableExecuteDataQuery,
	"/Ydb.Table.V1.TableService/ExecuteSchemeQuery":     TableExecuteSchemeQuery,
	"/Ydb.Table.V1.TableService/BeginTransaction":       TableBeginTransaction,
	"/Ydb.Table.V1.TableService/CommitTransaction":      TableCommitTransaction,
	"/Ydb.Table.V1.TableService/RollbackTransaction":    TableRollbackTransaction,
	"/Ydb.Table.V1.TableService/DescribeTableOptions":   TableDescribeTableOptions,
	"/Ydb.Table.V1.TableService/StreamReadTable":        TableStreamReadTable,
	"/Ydb.Table.V1.TableService/StreamExecuteScanQuery": TableStreamExecuteScanQuery,
}

var codeToString = map[MethodCode]string{
	TableCreateSession:          lastSegment("/Ydb.Table.V1.TableService/CreateSession"),
	TableDeleteSession:          lastSegment("/Ydb.Table.V1.TableService/DeleteSession"),
	TableKeepAlive:              lastSegment("/Ydb.Table.V1.TableService/KeepAlive"),
	TableCreateTable:            lastSegment("/Ydb.Table.V1.TableService/CreateTable"),
	TableDropTable:              lastSegment("/Ydb.Table.V1.TableService/DropTable"),
	TableAlterTable:             lastSegment("/Ydb.Table.V1.TableService/AlterTable"),
	TableCopyTable:              lastSegment("/Ydb.Table.V1.TableService/CopyTable"),
	TableDescribeTable:          lastSegment("/Ydb.Table.V1.TableService/DescribeTable"),
	TableExplainDataQuery:       lastSegment("/Ydb.Table.V1.TableService/ExplainDataQuery"),
	TablePrepareDataQuery:       lastSegment("/Ydb.Table.V1.TableService/PrepareDataQuery"),
	TableExecuteDataQuery:       lastSegment("/Ydb.Table.V1.TableService/ExecuteDataQuery"),
	TableExecuteSchemeQuery:     lastSegment("/Ydb.Table.V1.TableService/ExecuteSchemeQuery"),
	TableBeginTransaction:       lastSegment("/Ydb.Table.V1.TableService/BeginTransaction"),
	TableCommitTransaction:      lastSegment("/Ydb.Table.V1.TableService/CommitTransaction"),
	TableRollbackTransaction:    lastSegment("/Ydb.Table.V1.TableService/RollbackTransaction"),
	TableDescribeTableOptions:   lastSegment("/Ydb.Table.V1.TableService/DescribeTableOptions"),
	TableStreamReadTable:        lastSegment("/Ydb.Table.V1.TableService/StreamReadTable"),
	TableStreamExecuteScanQuery: lastSegment("/Ydb.Table.V1.TableService/StreamExecuteScanQuery"),
}

func setField(name string, dst, value interface{}) {
	x := reflect.ValueOf(dst).Elem()
	t := x.Type()
	f, ok := t.FieldByName(name)
	if !ok {
		panic(fmt.Sprintf(
			"struct %s has no field %q",
			t, name,
		))
	}
	v := reflect.ValueOf(value)
	if f.Type.Kind() != v.Type().Kind() {
		panic(fmt.Sprintf(
			"struct %s field %q is types of %s, not %s",
			t, name, f.Type, v.Type(),
		))
	}
	x.FieldByName(f.Name).Set(v)
}

type balancerStub struct {
	onInvoke func(
		ctx context.Context,
		method string,
		args interface{},
		reply interface{},
		opts ...grpc.CallOption,
	) error
	onNewStream func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		method string,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error)
}

func (b *balancerStub) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) (err error) {
	if b.onInvoke == nil {
		return fmt.Errorf("database.onInvoke() not defined")
	}

	return b.onInvoke(ctx, method, args, reply, opts...)
}

func (b *balancerStub) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	if b.onNewStream == nil {
		return nil, fmt.Errorf("database.onNewStream() not defined")
	}

	return b.onNewStream(ctx, desc, method, opts...)
}

func (b *balancerStub) Get(context.Context) (conn grpc.ClientConnInterface, err error) {
	cc := &clientConn{
		onInvoke:    b.onInvoke,
		onNewStream: b.onNewStream,
	}

	return cc, nil
}

func (b *balancerStub) Name() string {
	return "testutil.database"
}

func (b *balancerStub) Close(ctx context.Context) error {
	return nil
}

type (
	InvokeHandlers    map[MethodCode]func(request interface{}) (result proto.Message, err error)
	NewStreamHandlers map[MethodCode]func(desc *grpc.StreamDesc) (grpc.ClientStream, error)
)

type balancerOption func(c *balancerStub)

func WithInvokeHandlers(invokeHandlers InvokeHandlers) balancerOption {
	return func(r *balancerStub) {
		r.onInvoke = func(
			ctx context.Context,
			method string,
			args interface{},
			reply interface{},
			opts ...grpc.CallOption,
		) (err error) {
			if handler, ok := invokeHandlers[Method(method).Code()]; ok {
				var result proto.Message
				result, err = handler(args)
				if err != nil {
					return xerrors.WithStackTrace(err)
				}
				var anyResult *anypb.Any
				anyResult, err = anypb.New(result)
				if err != nil {
					return xerrors.WithStackTrace(err)
				}
				setField(
					"Operation",
					reply,
					&Ydb_Operations.Operation{
						Result: anyResult,
					},
				)

				return nil
			}

			return fmt.Errorf("method '%s' not implemented", method)
		}
	}
}

func WithNewStreamHandlers(newStreamHandlers NewStreamHandlers) balancerOption {
	return func(r *balancerStub) {
		r.onNewStream = func(
			ctx context.Context,
			desc *grpc.StreamDesc,
			method string,
			opts ...grpc.CallOption,
		) (_ grpc.ClientStream, err error) {
			if handler, ok := newStreamHandlers[Method(method).Code()]; ok {
				return handler(desc)
			}

			return nil, fmt.Errorf("method '%s' not implemented", method)
		}
	}
}

func NewBalancer(opts ...balancerOption) *balancerStub {
	c := &balancerStub{}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	return c
}

func (b *balancerStub) OnUpdate(func(context.Context, []endpoint.Info)) {
}

type clientConn struct {
	onInvoke func(
		ctx context.Context,
		method string,
		args interface{},
		reply interface{},
		opts ...grpc.CallOption,
	) error
	onNewStream func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		method string,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error)
	onAddress func() string
}

func (c *clientConn) Address() string {
	if c.onAddress != nil {
		return c.onAddress()
	}

	return ""
}

func (c *clientConn) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	if c.onInvoke == nil {
		return fmt.Errorf("onInvoke not implemented (method: %s, request: %v, response: %v)", method, args, reply)
	}

	return c.onInvoke(ctx, method, args, reply, opts...)
}

func (c *clientConn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if c.onNewStream == nil {
		return nil, fmt.Errorf("onNewStream not implemented (method: %s, desc: %v)", method, desc)
	}

	return c.onNewStream(ctx, desc, method, opts...)
}

type ClientStream struct {
	OnHeader    func() (metadata.MD, error)
	OnTrailer   func() metadata.MD
	OnCloseSend func() error
	OnContext   func() context.Context
	OnSendMsg   func(m interface{}) error
	OnRecvMsg   func(m interface{}) error
}

func (s *ClientStream) Header() (metadata.MD, error) {
	if s.OnHeader == nil {
		return nil, xerrors.WithStackTrace(ErrNotImplemented)
	}

	return s.OnHeader()
}

func (s *ClientStream) Trailer() metadata.MD {
	if s.OnTrailer == nil {
		return nil
	}

	return s.OnTrailer()
}

func (s *ClientStream) CloseSend() error {
	if s.OnCloseSend == nil {
		return xerrors.WithStackTrace(ErrNotImplemented)
	}

	return s.OnCloseSend()
}

func (s *ClientStream) Context() context.Context {
	if s.OnContext == nil {
		return nil
	}

	return s.OnContext()
}

func (s *ClientStream) SendMsg(m interface{}) error {
	if s.OnSendMsg == nil {
		return xerrors.WithStackTrace(ErrNotImplemented)
	}

	return s.OnSendMsg(m)
}

func (s *ClientStream) RecvMsg(m interface{}) error {
	if s.OnRecvMsg == nil {
		return xerrors.WithStackTrace(ErrNotImplemented)
	}

	return s.OnRecvMsg(m)
}

func lastSegment(m string) string {
	s := strings.Split(m, "/")

	return s[len(s)-1]
}
