//go:build integration

package integration

import (
	"context"
	"slices"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

// GrpcStopper interrupts gRPC calls with stopError between Stop and Start.
//
// Usage:
//
//	grpcStopper := NewGrpcStopper(errors.New("test error"))
//
//	db, err := ydb.Open(context.Background(), connectionString,
//		ydb.With(config.WithGrpcOptions(
//			grpc.WithChainUnaryInterceptor(grpcStopper.UnaryClientInterceptor),
//			grpc.WithChainStreamInterceptor(grpcStopper.StreamClientInterceptor),
//		)),
//	)
//
//	grpcStopper.Stop()  // Inject the configured error into gRPC calls.
//	grpcStopper.Start() // Allow subsequent calls through again.
type GrpcStopper struct {
	mu           sync.Mutex
	stopped      bool
	stopChannels map[string]empty.Chan
	stopError    error
}

func NewGrpcStopper(closeError error) *GrpcStopper {
	return &GrpcStopper{
		stopChannels: make(map[string]empty.Chan),
		stopError:    closeError,
	}
}

// Stop interrupts the listed methods, or all methods if none are listed, until Start.
func (l *GrpcStopper) Stop(methods ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(methods) == 0 {
		l.stopped = true
	}
	for _, method := range methods {
		if l.stopChannels[method] == nil {
			l.stopChannels[method] = make(empty.Chan)
		}
	}
	for method, ch := range l.stopChannels {
		if (l.stopped || slices.Contains(methods, method)) && !isClosed(ch) {
			close(ch)
		}
	}
}

// Start allows subsequent calls through, including calls on existing streams.
// Calls interrupted by Stop are not retried; closed streams stay closed.
func (l *GrpcStopper) Start() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = false
	for method, ch := range l.stopChannels {
		if isClosed(ch) {
			l.stopChannels[method] = make(empty.Chan)
		}
	}
}

func (l *GrpcStopper) stopSignal(method string) empty.Chan {
	l.mu.Lock()
	defer l.mu.Unlock()

	ch := l.stopChannels[method]
	if ch == nil {
		ch = make(empty.Chan)
		l.stopChannels[method] = ch
		if l.stopped {
			close(ch)
		}
	}

	return ch
}

func (l *GrpcStopper) UnaryClientInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	return l.intercept(method, func() error {
		return invoker(ctx, method, req, reply, cc, opts...)
	})
}

func (l *GrpcStopper) StreamClientInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if isClosed(l.stopSignal(method)) {
		return nil, l.stopError
	}

	stream, err := streamer(ctx, desc, cc, method, opts...)
	if stream != nil {
		stream = GrpcStopperStream{ClientStream: stream, stopper: l, method: method}
	}

	return stream, err
}

func (l *GrpcStopper) intercept(method string, call func() error) error {
	// Keep this generation so Start cannot hide Stop from an in-flight call.
	stopChannel := l.stopSignal(method)
	if isClosed(stopChannel) {
		return l.stopError
	}

	resChan := make(chan error, 1)
	go func() {
		resChan <- call()
	}()
	select {
	case <-stopChannel:
		return l.stopError
	case err := <-resChan:
		return err
	}
}

type GrpcStopperStream struct {
	grpc.ClientStream

	stopper *GrpcStopper
	method  string
}

func (g GrpcStopperStream) CloseSend() error {
	return g.stopper.intercept(g.method, g.ClientStream.CloseSend)
}

func (g GrpcStopperStream) SendMsg(m any) error {
	return g.stopper.intercept(g.method, func() error { return g.ClientStream.SendMsg(m) })
}

func (g GrpcStopperStream) RecvMsg(m any) error {
	return g.stopper.intercept(g.method, func() error { return g.ClientStream.RecvMsg(m) })
}

func isClosed(ch empty.Chan) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
