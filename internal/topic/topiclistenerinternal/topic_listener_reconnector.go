package topiclistenerinternal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

var (
	ErrUserCloseTopic      = errors.New("ydb: user closed topic listener")
	errTopicListenerClosed = errors.New("ydb: the topic listener already closed")
)

type TopicListenerReconnector struct {
	streamConfig *StreamListenerConfig
	client       TopicClient
	handler      EventHandler

	background background.Worker

	connectionResult    error
	connectionCompleted empty.Chan
	stopped             empty.Chan
	connectionIDCounter atomic.Int64
	closing             atomic.Bool

	m              sync.Mutex
	streamListener *streamListener
	streamCloseErr error
	stopErr        error
}

func NewTopicListenerReconnector(
	client TopicClient,
	streamConfig *StreamListenerConfig,
	handler EventHandler,
) (*TopicListenerReconnector, error) {
	res := &TopicListenerReconnector{
		streamConfig:        streamConfig,
		client:              client,
		handler:             handler,
		connectionCompleted: make(empty.Chan),
		stopped:             make(empty.Chan),
	}

	res.background.Start("connection", res.run)

	return res, nil
}

func (lr *TopicListenerReconnector) ReadSessionID() string {
	lr.m.Lock()
	sl := lr.streamListener
	lr.m.Unlock()
	if sl != nil && !sl.closing.Load() {
		return sl.ReadSessionID()
	}

	return ""
}

func (lr *TopicListenerReconnector) Close(ctx context.Context, reason error) error {
	if !lr.closing.CompareAndSwap(false, true) {
		return errTopicListenerClosed
	}
	var closeErrors []error
	err := lr.background.Close(ctx, reason)
	if !errors.Is(err, background.ErrAlreadyClosed) {
		closeErrors = append(closeErrors, err)
	}

	lr.m.Lock()
	streamCloseErr := lr.streamCloseErr
	lr.m.Unlock()
	if streamCloseErr != nil {
		closeErrors = append(closeErrors, streamCloseErr)
	}

	return errors.Join(closeErrors...)
}

func (lr *TopicListenerReconnector) run(ctx context.Context) {
	defer close(lr.stopped)

	sl, err := lr.connectStream(ctx)
	if err != nil {
		sl, err = lr.retryConnect(ctx, err)
	}
	lr.completeConnection(err)

	for err == nil {
		reconnect, reason := lr.waitAndRetireStream(ctx, sl)
		if !reconnect {
			return
		}
		sl, err = lr.retryConnect(ctx, reason)
	}

	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return
	}
	lr.stopWithError(ctx, err)
}

func (lr *TopicListenerReconnector) waitAndRetireStream(
	ctx context.Context,
	sl *streamListener,
) (bool, error) {
	select {
	case <-ctx.Done():
		lr.closeStream(sl, lr.background.CloseReason())

		return false, nil
	case <-sl.background.StopDone():
	}

	reason := sl.background.CloseReason()
	lr.closeStream(sl, reason)

	return ctx.Err() == nil, reason
}

func (lr *TopicListenerReconnector) closeStream(sl *streamListener, reason error) {
	closeErr := sl.Close(context.Background(), reason)
	lr.m.Lock()
	defer lr.m.Unlock()
	if lr.streamListener == sl {
		lr.streamListener = nil
	}
	if closeErr != nil {
		lr.streamCloseErr = errors.Join(lr.streamCloseErr, closeErr)
	}
}

func (lr *TopicListenerReconnector) stopWithError(ctx context.Context, reason error) {
	lr.m.Lock()
	lr.stopErr = errors.Join(lr.stopErr, reason)
	lr.m.Unlock()
	ctx = context.WithoutCancel(ctx)
	go func() {
		_ = lr.background.Close(ctx, reason)
	}()
	<-lr.background.Done()
}

func (lr *TopicListenerReconnector) retryConnect(ctx context.Context, reason error) (*streamListener, error) {
	firstAttempt := true
	retryOptions := append([]retry.Option{}, lr.streamConfig.retryOptions...)
	retryOptions = append(retryOptions, retry.WithIdempotent(true))

	return retry.RetryWithResult(ctx, func(ctx context.Context) (*streamListener, error) {
		if firstAttempt {
			firstAttempt = false

			return nil, lr.asRetryError(reason)
		}

		sl, err := lr.connectStream(ctx)
		if err == nil {
			return sl, nil
		}
		reason = err

		return nil, lr.asRetryError(reason)
	}, retryOptions...)
}

// asRetryError adapts the topic retry policy to the standard retryer. Transport
// errors drive classification, while the user callback still receives the full
// listener error with its surrounding context.
func (lr *TopicListenerReconnector) asRetryError(reason error) error {
	retryReason := reason
	retrySettings := topic.RetrySettings{
		StartTimeout: topic.DefaultStartTimeout,
		CheckError:   lr.streamConfig.CheckError,
	}
	hasTransportError := false
	if transportErr := xerrors.TransportError(reason); transportErr != nil {
		retryReason = transportErr
		hasTransportError = true
		if checkError := retrySettings.CheckError; checkError != nil {
			retrySettings.CheckError = func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
				args.Error = reason

				return checkError(args)
			}
		}
	}
	_, stopReason := topic.RetryDecision(retryReason, retrySettings, 0)
	if stopReason != nil {
		if !errors.Is(stopReason, reason) {
			stopReason = errors.Join(stopReason, reason)
		}

		return listenerRetryStopError{reason: stopReason}
	}

	retryError := reason
	if hasTransportError {
		retryError = errors.Join(retryReason, reason)
	}
	backoffType := retry.Check(retryReason).BackoffType()
	if backoffType != retry.TypeFastBackoff {
		backoffType = retry.TypeSlowBackoff
	}

	return retry.RetryableError(retryError, retry.WithBackoff(backoffType))
}

// listenerRetryStopError preserves the original error identity and status while
// hiding its retry metadata from retry.Check after the topic policy decides to stop.
type listenerRetryStopError struct {
	reason error
}

func (e listenerRetryStopError) Error() string {
	return e.reason.Error()
}

func (e listenerRetryStopError) Is(target error) bool {
	return errors.Is(e.reason, target)
}

func (e listenerRetryStopError) As(target any) bool {
	if _, ok := target.(*xerrors.Error); ok {
		return false
	}

	return errors.As(e.reason, target)
}

func (lr *TopicListenerReconnector) connectStream(ctx context.Context) (*streamListener, error) {
	sl, err := newStreamListener(ctx, lr.client, lr.handler, lr.streamConfig, &lr.connectionIDCounter)

	lr.m.Lock()
	defer lr.m.Unlock()

	lr.streamListener = sl

	return sl, err
}

func (lr *TopicListenerReconnector) completeConnection(err error) {
	lr.m.Lock()
	defer lr.m.Unlock()

	lr.connectionResult = err
	close(lr.connectionCompleted)
}

func (lr *TopicListenerReconnector) WaitInit(ctx context.Context) error {
	select {
	case <-ctx.Done():
		// pass
	case <-lr.connectionCompleted:
		// pass
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return lr.connectionResult
}

func (lr *TopicListenerReconnector) WaitStop(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lr.stopped:
		lr.m.Lock()
		stopErr, streamCloseErr := lr.stopErr, lr.streamCloseErr
		lr.m.Unlock()
		if stopErr != nil {
			return errors.Join(stopErr, streamCloseErr)
		}
		err := lr.background.CloseReason()
		if errors.Is(err, ErrUserCloseTopic) {
			return streamCloseErr
		}

		return errors.Join(err, streamCloseErr)
	}
}
