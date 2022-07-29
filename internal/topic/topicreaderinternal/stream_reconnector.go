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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type readerConnectFunc func(ctx context.Context) (batchedStreamReader, error)

type readerReconnector struct {
	clock      clockwork.Clock
	background background.Worker

	tracer      trace.Topic
	baseContext context.Context

	readerConnect readerConnectFunc

	reconnectFromBadStream chan batchedStreamReader
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
		res.connectTimeout = infiniteTimeout
	}

	res.initChannelsAndClock()
	res.start()

	return res
}

func (r *readerReconnector) ReadMessageBatch(ctx context.Context, opts ReadMessageBatchOptions) (*PublicBatch, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	for {
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
	r.reconnectFromBadStream <- nil
}

func (r *readerReconnector) initChannelsAndClock() {
	if r.clock == nil {
		r.clock = clockwork.NewRealClock()
	}
	r.reconnectFromBadStream = make(chan batchedStreamReader, 1)
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

		case oldReader := <-r.reconnectFromBadStream:
			r.reconnect(ctx, oldReader)
		}
	}
}

func (r *readerReconnector) reconnect(ctx context.Context, oldReader batchedStreamReader) {
	if ctx.Err() != nil {
		return
	}
	var closedErr error
	r.m.WithRLock(func() {
		closedErr = r.closedErr
	})
	if closedErr != nil {
		return
	}

	stream, _ := r.stream(ctx)
	if oldReader != stream {
		return
	}

	connectionInProgress := make(empty.Chan)
	defer close(connectionInProgress)

	r.m.WithLock(func() {
		r.streamConnectionInProgress = connectionInProgress
	})

	if oldReader != nil {
		_ = oldReader.CloseWithError(ctx, xerrors.WithStackTrace(errors.New("ydb: reconnect to pq grpc stream")))
	}

	newStream, err := connectWithTimeout(r.background.Context(), r.readerConnect, r.clock, r.connectTimeout)
	trace.TopicOnReadStreamOpen(r.tracer, r.baseContext, err)

	if topic.IsRetryableError(err) {
		go func() {
			// guarantee write reconnect signal to channel
			r.reconnectFromBadStream <- oldReader
		}()
	}

	r.m.WithLock(func() {
		r.streamErr = nil
		if err == nil {
			r.streamVal = newStream
		}
	})
}

func connectWithTimeout(
	baseContext context.Context,
	connector readerConnectFunc,
	clock clockwork.Clock,
	timeout time.Duration,
) (batchedStreamReader, error) {
	if err := baseContext.Err(); err != nil {
		return nil, err
	}
	connectionContext, cancel := xcontext.WithErrCancel(baseContext)

	type connectResult struct {
		stream batchedStreamReader
		err    error
	}
	result := make(chan connectResult, 1)

	go func() {
		stream, err := connector(connectionContext)
		result <- connectResult{stream: stream, err: err}
	}()

	var res connectResult
	select {
	case <-clock.After(timeout):
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
	case r.reconnectFromBadStream <- stream:
		// send signal
	default:
		// signal was send and not handled already
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
