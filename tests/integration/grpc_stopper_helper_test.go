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
// Pause holds unary requests and responses, and stream creation, until Start.
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
	pause        *grpcPause
	recvPause    empty.Chan
	streamStop   empty.Chan

	// onRecvMsg can hold a real server response before the SDK receives it.
	// Set it before opening streams.
	onRecvMsg func(context.Context, any) error
}

type grpcPause struct {
	reached empty.Chan
	resume  empty.Chan
	methods []string
}

func NewGrpcStopper(closeError error) *GrpcStopper {
	return &GrpcStopper{
		stopChannels: make(map[string]empty.Chan),
		stopError:    closeError,
		streamStop:   make(empty.Chan),
	}
}

// Stop interrupts the listed methods, or all methods if none are listed, until Start.
// An interrupted RPC may keep running until its caller cancels the RPC context.
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

// Start resumes paused calls and allows subsequent calls through, including calls on existing streams.
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
	if l.pause != nil {
		close(l.pause.resume)
		l.pause = nil
	}
	if l.recvPause != nil {
		close(l.recvPause)
		l.recvPause = nil
	}
}

// StopOnce interrupts in-flight calls while allowing subsequent calls through immediately.
func (l *GrpcStopper) StopOnce() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !isClosed(l.streamStop) {
		close(l.streamStop)
	}
	l.streamStop = make(empty.Chan)
}

// PauseRecv holds received stream messages until the returned function is called.
func (l *GrpcStopper) PauseRecv() (resume func()) {
	l.mu.Lock()
	defer l.mu.Unlock()

	resumeRecv := make(empty.Chan)
	l.recvPause = resumeRecv

	var once sync.Once

	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()

			if l.recvPause == resumeRecv {
				l.recvPause = nil
				close(resumeRecv)
			}
		})
	}
}

// Pause holds unary calls and stream creation until Start, Stop, or context cancellation.
// It applies to the listed methods, or all methods if none are listed.
// Repeated calls while paused keep the original methods and notification channel.
// Each paused call sends a notification unless Start, Stop, or context cancellation releases it first.
func (l *GrpcStopper) Pause(methods ...string) <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pause == nil {
		l.pause = &grpcPause{
			reached: make(empty.Chan),
			resume:  make(empty.Chan),
			methods: slices.Clone(methods),
		}
	}

	return l.pause.reached
}

func (l *GrpcStopper) pauseState(method string) *grpcPause {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pause == nil {
		return nil
	}
	if len(l.pause.methods) > 0 && !slices.Contains(l.pause.methods, method) {
		return nil
	}

	return l.pause
}

func (l *GrpcStopper) wait(ctx context.Context, method string, stopChannel empty.Chan) error {
	for {
		if isClosed(stopChannel) {
			return l.stopError
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		pause := l.pauseState(method)
		if pause == nil {
			return nil
		}
		select {
		case pause.reached <- struct{}{}:
		case <-pause.resume:
			continue
		case <-stopChannel:
			return l.stopError
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-pause.resume:
		case <-stopChannel:
			return l.stopError
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (l *GrpcStopper) waitRecv(ctx context.Context, stopChannel, streamStop empty.Chan) error {
	l.mu.Lock()
	recvPause := l.recvPause
	l.mu.Unlock()
	if recvPause == nil {
		return nil
	}

	select {
	case <-recvPause:
		return nil
	case <-stopChannel:
		return l.stopError
	case <-streamStop:
		return l.stopError
	case <-ctx.Done():
		return ctx.Err()
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

func (l *GrpcStopper) streamStopSignal() empty.Chan {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.streamStop
}

func (l *GrpcStopper) UnaryClientInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	stopChannel := l.stopSignal(method)
	if err := l.wait(ctx, method, stopChannel); err != nil {
		return err
	}

	return l.intercept(method, func() error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if l.pauseState(method) != nil {
			if pauseErr := l.wait(ctx, method, stopChannel); pauseErr != nil {
				return pauseErr
			}
		}

		return err
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
	if err := l.wait(ctx, method, l.stopSignal(method)); err != nil {
		return nil, err
	}

	stream, err := streamer(ctx, desc, cc, method, opts...)
	if stream != nil {
		stream = GrpcStopperStream{
			ClientStream: stream,
			stopper:      l,
			method:       method,
			streamStop:   l.streamStopSignal(),
		}
	}

	return stream, err
}

func (l *GrpcStopper) intercept(method string, call func() error) error {
	// Keep this generation so Start cannot hide Stop from an in-flight call.
	return l.interceptWithSignals(l.stopSignal(method), nil, call)
}

func (l *GrpcStopper) interceptWithSignals(stopChannel, streamStop empty.Chan, call func() error) error {
	if isClosed(stopChannel) || isClosed(streamStop) {
		return l.stopError
	}

	resChan := make(chan error, 1)
	go func() {
		resChan <- call()
	}()
	select {
	case <-stopChannel:
		// Preserve a completed result when both channels are ready.
		select {
		case err := <-resChan:
			return err
		default:
			return l.stopError
		}
	case <-streamStop:
		select {
		case err := <-resChan:
			return err
		default:
			return l.stopError
		}
	case err := <-resChan:
		return err
	}
}

type GrpcStopperStream struct {
	grpc.ClientStream

	stopper    *GrpcStopper
	method     string
	streamStop empty.Chan
}

func (g GrpcStopperStream) CloseSend() error {
	return g.stopper.interceptWithSignals(g.stopper.stopSignal(g.method), g.streamStop, g.ClientStream.CloseSend)
}

func (g GrpcStopperStream) SendMsg(m any) error {
	return g.stopper.interceptWithSignals(
		g.stopper.stopSignal(g.method), g.streamStop, func() error { return g.ClientStream.SendMsg(m) },
	)
}

func (g GrpcStopperStream) RecvMsg(m any) error {
	stopChannel := g.stopper.stopSignal(g.method)
	if err := g.stopper.interceptWithSignals(
		stopChannel, g.streamStop, func() error { return g.ClientStream.RecvMsg(m) },
	); err != nil {
		return err
	}
	if err := g.stopper.waitRecv(g.Context(), stopChannel, g.streamStop); err != nil {
		return err
	}
	if g.stopper.onRecvMsg != nil {
		return g.stopper.onRecvMsg(g.Context(), m)
	}

	return nil
}

func isClosed(ch empty.Chan) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
