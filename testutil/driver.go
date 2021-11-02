package testutil

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver"
)

var ErrNotImplemented = errors.New("testutil: not implemented")

type MethodCode uint

func (m MethodCode) String() string {
	return codeToString[m]
}

type Method string

func (m Method) Code() MethodCode {
	return grpcMethodToCode[m]
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
			"ydb/testutil: struct %s has no field %q",
			t, name,
		))
	}
	v := reflect.ValueOf(value)
	if f.Type.Kind() != v.Type().Kind() {
		panic(fmt.Sprintf(
			"ydb/testutil: struct %s field %q is types of %s, not %s",
			t, name, f.Type, v.Type(),
		))
	}
	x.FieldByName(f.Name).Set(v)
}

func getField(name string, src, dst interface{}) bool {
	var fn func(x reflect.Value, seg ...string) bool
	fn = func(x reflect.Value, seg ...string) bool {
		if x.Kind() == reflect.Ptr {
			x = x.Elem()
		}
		t := x.Type()
		f, ok := t.FieldByName(seg[0])
		if !ok {
			return false
		}
		fv := x.FieldByName(seg[0])
		if fv.Kind() == reflect.Ptr && fv.IsNil() {
			return false
		}
		if len(seg) > 1 {
			return fn(fv.Elem(), seg[1:]...)
		}

		v := reflect.ValueOf(dst)
		if v.Type().Kind() != reflect.Ptr {
			panic("ydb/testutil: destination value must be a pointer")
		}
		if v.Type().Elem().Kind() != fv.Type().Kind() {
			panic(fmt.Sprintf(
				"ydb/testutil: struct %s field %q is types of %s, not %s",
				t, name, f.Type, v.Type(),
			))
		}

		v.Elem().Set(fv)

		return true
	}
	return fn(reflect.ValueOf(src).Elem(), strings.Split(name, ".")...)
}

type db struct {
	onInvoke    func(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error
	onNewStream func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error)
	onClose     func(ctx context.Context) error
}

func (db *db) ID() uint32 {
	return 0
}

func (db *db) Address() string {
	return ""
}

func (db *db) Secure() bool {
	return true
}

func (db *db) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) (err error) {
	if db.onInvoke == nil {
		return fmt.Errorf("db.onInvoke() not implemented")
	}
	defer func() {
		if err == nil {
			if apply, ok := driver.ContextCallInfo(ctx); ok && apply != nil {
				apply(db)
			}
		}
	}()
	return db.onInvoke(ctx, method, args, reply, opts...)
}

func (db *db) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	if db.onNewStream == nil {
		return nil, fmt.Errorf("db.onNewStream() not implemented")
	}
	defer func() {
		if err == nil {
			if apply, ok := driver.ContextCallInfo(ctx); ok && apply != nil {
				apply(db)
			}
		}
	}()
	return db.onNewStream(ctx, desc, method, opts...)
}

func (db *db) Get(context.Context) (conn cluster.ClientConnInterface, err error) {
	cc := &clientConn{
		onInvoke:    db.onInvoke,
		onNewStream: db.onNewStream,
	}
	return cc, nil
}

func (db *db) Name() string {
	return "testutil.db"
}

func (db *db) Close(ctx context.Context) error {
	if db.onClose == nil {
		return fmt.Errorf("db.Close() not implemented")
	}
	return db.onClose(ctx)
}

type (
	InvokeHandlers    map[MethodCode]func(request interface{}) (result proto.Message, err error)
	NewStreamHandlers map[MethodCode]func(desc *grpc.StreamDesc) (grpc.ClientStream, error)
)

type NewClusterOption func(c *db)

func WithInvokeHandlers(invokeHandlers InvokeHandlers) NewClusterOption {
	return func(db *db) {
		db.onInvoke = func(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) (err error) {
			if handler, ok := invokeHandlers[Method(method).Code()]; ok {
				result, err := handler(args)
				if err != nil {
					return err
				}
				anyResult, err := anypb.New(result)
				if err != nil {
					return err
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
			return fmt.Errorf("testutil: method '%s' not implemented", method)
		}
	}
}

func WithNewStreamHandlers(newStreamHandlers NewStreamHandlers) NewClusterOption {
	return func(db *db) {
		db.onNewStream = func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
			if handler, ok := newStreamHandlers[Method(method).Code()]; ok {
				return handler(desc)
			}
			return nil, fmt.Errorf("testutil: method '%s' not implemented", method)
		}
	}
}

func WithClose(onClose func(ctx context.Context) error) NewClusterOption {
	return func(c *db) {
		c.onClose = onClose
	}
}

func NewCluster(opts ...NewClusterOption) cluster.Cluster {
	c := &db{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type clientConn struct {
	onInvoke    func(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error
	onNewStream func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error)
	onAddress   func() string
	onID        func() uint32
}

func (c *clientConn) ID() uint32 {
	if c.onID != nil {
		return c.onID()
	}
	return 0
}

func (c *clientConn) Address() string {
	if c.onAddress != nil {
		return c.onAddress()
	}
	return ""
}

func (c *clientConn) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	if c.onInvoke == nil {
		return fmt.Errorf("onInvoke not implemented (method: %s, request: %v, response: %v)", method, args, reply)
	}
	return c.onInvoke(ctx, method, args, reply, opts...)
}

func (c *clientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
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
		return nil, ErrNotImplemented
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
		return ErrNotImplemented
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
		return ErrNotImplemented
	}
	return s.OnSendMsg(m)

}

func (s *ClientStream) RecvMsg(m interface{}) error {
	if s.OnRecvMsg == nil {
		return ErrNotImplemented
	}
	return s.OnRecvMsg(m)
}

func lastSegment(m string) string {
	s := strings.Split(m, "/")
	return s[len(s)-1]
}
