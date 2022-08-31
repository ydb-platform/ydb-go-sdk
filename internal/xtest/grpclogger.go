package xtest

import (
	"context"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
)

var globalLastStreamID = int64(0)

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

func (l GrpcLogger) UnaryClientInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	l.t.Logf(
		"UnaryClientInterceptor: %s - err: %v\n\nreq:\n%v\n\nresp:\n%v",
		method,
		err,
		req,
		reply,
	)
	return err
}

func (l GrpcLogger) StreamClientInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	stream, err := streamer(ctx, desc, cc, method, opts...)
	streamWrapper := newGrpcLoggerStream(stream, l.t)
	if stream != nil {
		stream = streamWrapper
	}
	l.t.Logf(
		"StreamStart: %v with err '%v' (streamID: %v)",
		method,
		err,
		streamWrapper.streamID,
	)
	return stream, err
}

type grpcLoggerStream struct {
	grpc.ClientStream
	streamID int64
	t        testing.TB
}

func newGrpcLoggerStream(stream grpc.ClientStream, t testing.TB) grpcLoggerStream {
	return grpcLoggerStream{stream, atomic.AddInt64(&globalLastStreamID, 1), t}
}

func (g grpcLoggerStream) CloseSend() error {
	err := g.ClientStream.CloseSend()
	g.t.Logf("CloseSend: %v (streamID: %v)", err, g.streamID)
	return err
}

func (g grpcLoggerStream) SendMsg(m interface{}) error {
	err := g.ClientStream.SendMsg(m)
	g.t.Logf("SendMsg (streamID: %v) with err '%v':\n%v ", g.streamID, err, m)
	return err
}

func (g grpcLoggerStream) RecvMsg(m interface{}) error {
	err := g.ClientStream.RecvMsg(m)
	g.t.Logf("RecvMsg (streamID: %v) with err '%v':\n%v ", g.streamID, err, m)
	return err
}
