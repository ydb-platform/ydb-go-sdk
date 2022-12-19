package topicreaderinternal

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errReconnectRequestOutdated = xerrors.Wrap(errors.New("ydb: reconnect request outdated"))
	errReconnect                = xerrors.Wrap(errors.New("ydb: reconnect to topic grpc stream"))
)

type readerConnectFunc func(ctx context.Context) (batchedStreamReader, error)

type readerReconnector struct {
	clock      clockwork.Clock
	background background.Worker

	tracer      trace.Topic
	baseContext context.Context

	readerConnect readerConnectFunc

	reconnectFromBadStream chan reconnectRequest
	connectTimeout         time.Duration

	closeOnce sync.Once

	m                          xsync.RWMutex
	streamConnectionInProgress empty.Chan // opened if connection in progress, closed if connection established
	streamVal                  batchedStreamReader
	streamErr                  error
	closedErr                  error
}

//nolint:revive
func newReaderReconnector(
	connector readerConnectFunc,
	connectTimeout time.Duration,
	tracer trace.Topic,
	baseContext context.Context,
) *readerReconnector {
	res := &readerReconnector{
		clock:          clockwork.NewRealClock(),
		readerConnect:  connector,
		streamErr:      errUnconnected,
		connectTimeout: connectTimeout,
		tracer:         tracer,
		baseContext:    baseContext,
	}
	if res.connectTimeout == 0 {
		res.connectTimeout = value.InfiniteDuration
	}

	res.initChannelsAndClock()
	res.start()

	return res
}

func (r *readerReconnector) ReadMessageBatch(ctx context.Context, opts ReadMessageBatchOptions) (*PublicBatch, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	attempt := 0

	for {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-backoff.Fast.Wait(attempt):
				// pass
			}
		}

		attempt++
		stream, err := r.stream(ctx)
		switch {
		case topic.IsRetryableError(err):
			r.fireReconnectOnRetryableError(stream, err)
			runtime.Gosched()
			continue
		case err != nil:
			return nil, err
		default:
			// pass
		}

		res, err := stream.ReadMessageBatch(ctx, opts)
		if topic.IsRetryableError(err) {
			r.fireReconnectOnRetryableError(stream, err)
			runtime.Gosched()
			continue
		}
		return res, err
	}
}

func (r *readerReconnector) Commit(ctx context.Context, commitRange commitRange) error {
	stream, err := r.stream(ctx)
	if err != nil {
		return err
	}

	err = stream.Commit(ctx, commitRange)
	r.fireReconnectOnRetryableError(stream, err)
	return err
}

func (r *readerReconnector) CloseWithError(ctx context.Context, err error) error {
	var closeErr error
	r.closeOnce.Do(func() {
		r.m.WithLock(func() {
			r.closedErr = err
		})

		closeErr = r.background.Close(ctx, err)

		if r.streamVal != nil {
			streamCloseErr := r.streamVal.CloseWithError(ctx, xerrors.WithStackTrace(ErrReaderClosed))
			if closeErr == nil {
				closeErr = streamCloseErr
			}
		}
	})
	return closeErr
}

func (r *readerReconnector) start() {
	r.background.Start("reconnector-loop", r.reconnectionLoop)

	// start first connection
	r.reconnectFromBadStream <- newReconnectRequest(nil, nil)
}

func (r *readerReconnector) initChannelsAndClock() {
	if r.clock == nil {
		r.clock = clockwork.NewRealClock()
	}
	r.reconnectFromBadStream = make(chan reconnectRequest, 1)
	r.streamConnectionInProgress = make(empty.Chan)
	close(r.streamConnectionInProgress) // no progress at start
}

func (r *readerReconnector) reconnectionLoop(ctx context.Context) {
	defer r.handlePanic()

	lastTime := time.Time{}
	attempt := 0
	for {
		now := r.clock.Now()
		sinceLastTime := now.Sub(lastTime)
		lastTime = now

		const resetAttemptEmpiricalCoefficient = 10
		if sinceLastTime > r.connectTimeout*resetAttemptEmpiricalCoefficient {
			attempt = 0
		} else {
			attempt++
		}

		if attempt > 0 {
			delay := backoff.Fast.Delay(attempt)

			select {
			case <-ctx.Done():
				return
			case <-r.clock.After(delay):
				// pass
			}
		}

		select {
		case <-ctx.Done():
			return

		case request := <-r.reconnectFromBadStream:
			_ = r.reconnect(ctx, request.oldReader)
		}
	}
}

func (r *readerReconnector) reconnect(ctx context.Context, oldReader batchedStreamReader) (err error) {
	onDone := trace.TopicOnReaderReconnect(r.tracer)
	defer func() {
		onDone(err)
	}()

	if err = ctx.Err(); err != nil {
		return err
	}

	var closedErr error
	r.m.WithRLock(func() {
		closedErr = r.closedErr
	})
	if closedErr != nil {
		return err
	}

	stream, _ := r.stream(ctx)
	if oldReader != stream {
		return xerrors.WithStackTrace(errReconnectRequestOutdated)
	}

	connectionInProgress := make(empty.Chan)
	defer close(connectionInProgress)

	r.m.WithLock(func() {
		r.streamConnectionInProgress = connectionInProgress
	})

	if oldReader != nil {
		_ = oldReader.CloseWithError(ctx, xerrors.WithStackTrace(errReconnect))
	}

	newStream, err := r.connectWithTimeout()

	if topic.IsRetryableError(err) {
		go func(reason error) {
			// guarantee write reconnect signal to channel
			r.reconnectFromBadStream <- newReconnectRequest(oldReader, reason)
			trace.TopicOnReaderReconnectRequest(r.tracer, err, true)
		}(err)
	}

	r.m.WithLock(func() {
		r.streamErr = err
		if err == nil {
			r.streamVal = newStream
		}
	})
	return err
}

func (r *readerReconnector) connectWithTimeout() (_ batchedStreamReader, err error) {
	bgContext := r.background.Context()

	if err = bgContext.Err(); err != nil {
		return nil, err
	}

	connectionContext, cancel := xcontext.WithErrCancel(context.Background())

	type connectResult struct {
		stream batchedStreamReader
		err    error
	}
	result := make(chan connectResult, 1)

	go func() {
		stream, err := r.readerConnect(connectionContext)
		result <- connectResult{stream: stream, err: err}
	}()

	var res connectResult
	select {
	case <-r.clock.After(r.connectTimeout):
		// cancel connection context only if timeout exceed while connection
		// because if cancel context after connect - it will break
		cancel(xerrors.WithStackTrace(fmt.Errorf("ydb: open stream reader timeout: %w", context.DeadlineExceeded)))
		res = <-result
	case res = <-result:
		// pass
	}

	if res.err == nil {
		return res.stream, nil
	}

	return nil, res.err
}

func (r *readerReconnector) fireReconnectOnRetryableError(stream batchedStreamReader, err error) {
	if !topic.IsRetryableError(err) {
		return
	}

	select {
	case r.reconnectFromBadStream <- newReconnectRequest(stream, err):
		// send signal
		trace.TopicOnReaderReconnectRequest(r.tracer, err, true)
	default:
		// previous reconnect signal in process, no need sent signal more
		trace.TopicOnReaderReconnectRequest(r.tracer, err, false)
	}
}

func (r *readerReconnector) stream(ctx context.Context) (batchedStreamReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var err error
	var connectionChan empty.Chan
	r.m.WithRLock(func() {
		connectionChan = r.streamConnectionInProgress
		if r.closedErr != nil {
			err = r.closedErr
			return
		}
	})
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.background.Done():
		return nil, r.closedErr
	case <-connectionChan:
		var reader batchedStreamReader
		r.m.WithRLock(func() {
			reader = r.streamVal
			err = r.streamErr
		})
		r.fireReconnectOnRetryableError(reader, err)
		return reader, err
	}
}

func (r *readerReconnector) handlePanic() {
	p := recover()

	if p != nil {
		_ = r.CloseWithError(context.Background(), xerrors.WithStackTrace(fmt.Errorf("handled panic: %v", p)))
	}
}

type reconnectRequest struct {
	oldReader batchedStreamReader
	reason    error
}

func newReconnectRequest(oldReader batchedStreamReader, reason error) reconnectRequest {
	return reconnectRequest{
		oldReader: oldReader,
		reason:    reason,
	}
}
