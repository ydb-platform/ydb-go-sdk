package topicresearch_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
)

// Each session owns its transport and observation queue; only the event recorder
// and Query transaction aliases are shared across the scenario.
type streamWriteSession struct {
	*streamWriteResearch

	name         string
	number       int
	stream       Ydb_Topic_V1.TopicService_StreamWriteClient
	streamCancel context.CancelFunc
	sendQueue    chan streamWriteSend
	stopSending  chan struct{}
	sendDone     chan struct{}
	receiveDone  chan struct{}
	receiveReady chan struct{}
	closeOnce    sync.Once

	mu                    sync.Mutex
	closing               bool
	streamCloseErr        error
	pendingResponses      int
	hideNextWriteResponse bool
	received              []streamWriteReceive
	receivingEnded        bool
}

func (r *streamWriteResearch) newWriteSession(name string) (*streamWriteSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing := r.namedStreams[name]; existing != nil && !existing.isClosing() {
		return nil, fmt.Errorf("StreamWrite session %q is already active; close it or use another alias", name)
	}
	session := &streamWriteSession{streamWriteResearch: r, name: name, number: len(r.writeSessions) + 1}
	if r.namedStreams == nil {
		r.namedStreams = make(map[string]*streamWriteSession)
	}
	r.namedStreams[name] = session
	r.writeSessions = append(r.writeSessions, session)

	return session, nil
}

func writeSessionFromContext(ctx context.Context, name string) (*streamWriteSession, error) {
	research, err := researchFromContext(ctx)
	if err != nil {
		return nil, err
	}
	research.mu.Lock()
	defer research.mu.Unlock()

	session := research.namedStreams[name]
	if session == nil {
		return nil, fmt.Errorf("StreamWrite session %q is not initialized", name)
	}

	return session, nil
}

func (r *streamWriteSession) label() string {
	if r.name == "" {
		return fmt.Sprintf("#%d", r.number)
	}

	return fmt.Sprintf("#%d [%s]", r.number, r.name)
}

func (r *streamWriteSession) startStreamPumps() {
	r.sendQueue = make(chan streamWriteSend)
	r.stopSending = make(chan struct{})
	r.sendDone = make(chan struct{})
	r.receiveDone = make(chan struct{})
	r.receiveReady = make(chan struct{}, 1)

	// gRPC-Go permits one goroutine to call Send while another calls Recv on the same stream.
	go r.runStreamSender()
	go r.runStreamReceiver()
}

func (r *streamWriteSession) runStreamSender() {
	defer close(r.sendDone)
	defer func() { r.setCloseError(r.stream.CloseSend()) }()
	for {
		var operation streamWriteSend
		select {
		case <-r.stopSending:
			return
		case operation = <-r.sendQueue:
		}
		r.observeStreamWriteRequest(r.label(), operation.message)
		err := r.stream.Send(operation.message)
		if err != nil {
			r.observe(fmt.Sprintf(
				"gRPC client → server /Ydb.Topic.V1.TopicService/StreamWrite %s / Send: %v.", r.label(), err))
		}
		operation.result <- err
		close(operation.result)
	}
}

func (r *streamWriteSession) setCloseError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.streamCloseErr = err
}

func (r *streamWriteSession) runStreamReceiver() {
	defer r.finishReceiving()
	for {
		message, err := r.stream.Recv()
		if err != nil {
			if !r.isClosing() {
				r.observe(fmt.Sprintf(
					"gRPC client ← server /Ydb.Topic.V1.TopicService/StreamWrite %s / Recv: %v.", r.label(), err))
			}
			r.queueReceived(streamWriteReceive{err: err})

			return
		}
		r.observeStreamWriteResponse(r.label(), message)
		if message.GetWriteResponse() != nil && r.takeHideNextWriteResponse() {
			r.observe(fmt.Sprintf(
				"Research control flow withholds Ydb.Topic.StreamWriteMessage.WriteResponse from StreamWrite %s "+
					"and cancels the stream after recording it.", r.label()))
			r.streamCancel()
			r.queueReceived(streamWriteReceive{err: context.Canceled})

			return
		}
		r.queueReceived(streamWriteReceive{message: message})
	}
}

// Recording must continue even while the scenario is addressing another stream.
// The queue is drained by the scenario and released with the session.
func (r *streamWriteSession) queueReceived(result streamWriteReceive) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.received = append(r.received, result)
	r.notifyReceiver()
}

func (r *streamWriteSession) notifyReceiver() {
	select {
	case r.receiveReady <- struct{}{}:
	default:
	}
}

func (r *streamWriteSession) finishReceiving() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.receivingEnded = true
	close(r.receiveDone)
	r.notifyReceiver()
}

func (r *streamWriteSession) takeReceived() (streamWriteReceive, bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.received) == 0 {
		return streamWriteReceive{}, false, r.receivingEnded
	}
	result := r.received[0]
	r.received[0] = streamWriteReceive{}
	r.received = r.received[1:]
	if len(r.received) == 0 {
		r.received = nil
	}

	return result, true, r.receivingEnded
}

func (r *streamWriteSession) send(
	ctx context.Context,
	message *Ydb_Topic.StreamWriteMessage_FromClient,
) error {
	if r.isClosing() {
		return io.EOF
	}
	operation := streamWriteSend{
		message: message,
		result:  make(chan error, 1),
	}
	select {
	case r.sendQueue <- operation:
	case <-r.stopSending:
		return io.EOF
	case <-r.sendDone:
		return io.EOF
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-operation.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *streamWriteSession) receive(ctx context.Context) (*Ydb_Topic.StreamWriteMessage_FromServer, error) {
	for {
		result, ok, ended := r.takeReceived()
		if ok {
			return result.message, result.err
		}
		if ended {
			return nil, io.EOF
		}
		select {
		case <-r.receiveReady:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *streamWriteSession) observeWriteResponses(ctx context.Context, expected int) error {
	if expected == 0 {
		return nil
	}
	windowCtx, cancel := context.WithTimeout(ctx, streamResponseIdleTimeout)
	defer cancel()

	for received := range expected {
		if _, err := r.receive(windowCtx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				r.observe(fmt.Sprintf(
					"/Ydb.Topic.V1.TopicService/StreamWrite %s observation window ended after %s: "+
						"read %d StreamWriteMessage.FromServer message(s) for %d WriteRequest message(s).",
					r.label(), streamResponseIdleTimeout, received, expected))
			}

			return nil
		}
	}

	return nil
}

func (r *streamWriteSession) observeStreamEndOrIdle(ctx context.Context) error {
	windowCtx, cancel := context.WithTimeout(ctx, streamResponseIdleTimeout)
	defer cancel()
	for {
		if _, err := r.receive(windowCtx); err != nil {
			return ctx.Err()
		}
	}
}

func (r *streamWriteSession) isClosing() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.closing
}

func (r *streamWriteSession) addPendingResponse() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pendingResponses++
}

func (r *streamWriteSession) takePendingResponses() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	pending := r.pendingResponses
	r.pendingResponses = 0

	return pending
}

func (r *streamWriteSession) setHideNextWriteResponse() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.hideNextWriteResponse = true
}

func (r *streamWriteSession) takeHideNextWriteResponse() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	hide := r.hideNextWriteResponse
	r.hideNextWriteResponse = false

	return hide
}

func (r *streamWriteSession) closeStream(ctx context.Context) error {
	if r.stream == nil {
		return nil
	}

	r.closeOnce.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.closing = true
		close(r.stopSending)
	})

	select {
	case <-r.sendDone:
	case <-ctx.Done():
		if r.streamCancel != nil {
			r.streamCancel()
		}

		return ctx.Err()
	}
	if r.streamCancel != nil {
		r.streamCancel()
	}
	select {
	case <-r.receiveDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.streamCloseErr
}
