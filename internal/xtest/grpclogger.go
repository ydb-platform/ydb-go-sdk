package xtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

// GrpcLogger use for log raw grpc messages
//
// Usage:
//
//		db, err := ydb.Open(context.Background(), connectionString,
//	     ...
//			ydb.With(config.WithGrpcOptions(grpc.WithChainUnaryInterceptor(xtest.NewGrpcLogger(t).UnaryClientInterceptor))),
//		)
type GrpcLogger struct {
	t testing.TB
}

func NewGrpcLogger(t testing.TB) GrpcLogger {
	return GrpcLogger{t: t}
}

func (l GrpcLogger) UnaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	l.t.Logf("UnaryClientInterceptor: %s - err: %v\n\nreq:\n%v\n\nresp:\n%v", method, err, protoToString(req), protoToString(reply))
	return err
}

func protoToString(v interface{}) string {
	if mess, ok := v.(proto.Message); ok {
		marshaler := jsonpb.Marshaler{}
		res, err := marshaler.MarshalToString(mess)
		if err != nil {
			return fmt.Sprintf("failed to json marshal of: '%v' with err: ", v, err)
		}
		return res
	}
	return fmt.Sprint(v)
}
