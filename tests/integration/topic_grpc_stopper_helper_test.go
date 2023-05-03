//go:build integration
// +build integration

package integration

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

// GrpcStopper use interceptors for stop any real grpc activity on grpc channel and return error for any calls
//
// Usage:
//
//		grpcStopper := xtest.NewGrpcStopper()
//
//		db, err := ydb.Open(context.Background(), connectionString,
//	     ...
//			ydb.With(config.WithGrpcOptions(grpc.WithChainUnaryInterceptor(grpcStopper.UnaryClientInterceptor)),
//			ydb.With(config.WithGrpcOptions(grpc.WithStreamInterceptor(grpcStopper.StreamClientInterceptor)),
//		),
//
//		grpcStopper.Stop(errors.New("test error"))
//
// )
type GrpcStopper struct {
	stopChannel empty.Chan
	stopError   error
}

func NewGrpcStopper(closeError error) GrpcStopper {
	return GrpcStopper{
		stopChannel: make(empty.Chan),
		stopError:   closeError,
	}
}

func (l GrpcStopper) Stop() {
	close(l.stopChannel)
}

func (l GrpcStopper) UnaryClientInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	if isClosed(l.stopChannel) {
		return l.stopError
	}

	resChan := make(chan error, 1)
	go func() {
		resChan <- invoker(ctx, method, req, reply, cc, opts...)
	}()
	select {
	case <-l.stopChannel:
		return l.stopError
	case err := <-resChan:
		return err
	}
}

func (l GrpcStopper) StreamClientInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	select {
	case <-l.stopChannel:
		// grpc stopped
		return nil, l.stopError
	default:
	}

	stream, err := streamer(ctx, desc, cc, method, opts...)
	streamWrapper := newGrpcStopperStream(stream, l.stopChannel, l.stopError)
	if stream != nil {
		stream = streamWrapper
	}
	return stream, err
}

type GrpcStopperStream struct {
	stopChannel empty.Chan
	stopError   error
	grpc.ClientStream
}

func newGrpcStopperStream(stream grpc.ClientStream, stopChannel empty.Chan, stopError error) GrpcStopperStream {
	return GrpcStopperStream{stopChannel: stopChannel, ClientStream: stream, stopError: stopError}
}

func (g GrpcStopperStream) CloseSend() error {
	if isClosed(g.stopChannel) {
		return g.stopError
	}

	resChan := make(chan error, 1)
	go func() {
		resChan <- g.ClientStream.CloseSend()
	}()
	select {
	case <-g.stopChannel:
		return g.stopError
	case err := <-resChan:
		return err
	}
}

func (g GrpcStopperStream) SendMsg(m interface{}) error {
	if isClosed(g.stopChannel) {
		return g.stopError
	}

	resChan := make(chan error, 1)
	go func() {
		resChan <- g.ClientStream.SendMsg(m)
	}()
	select {
	case <-g.stopChannel:
		return g.stopError
	case err := <-resChan:
		return err
	}
}

func (g GrpcStopperStream) RecvMsg(m interface{}) error {
	if isClosed(g.stopChannel) {
		return g.stopError
	}

	resChan := make(chan error, 1)
	go func() {
		resChan <- g.ClientStream.RecvMsg(m)
	}()

	select {
	case <-g.stopChannel:
		return g.stopError
	case err := <-resChan:
		return err
	}
}

func isClosed(ch empty.Chan) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
