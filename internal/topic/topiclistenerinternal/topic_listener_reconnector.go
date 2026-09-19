package topiclistenerinternal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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

	res.background.Start("connection", res.connect)

	return res, nil
}

func (lr *TopicListenerReconnector) ReadSessionID() string {
	lr.m.Lock()
	sl := lr.streamListener
	lr.m.Unlock()
	if sl != nil {
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

func (lr *TopicListenerReconnector) connect(ctx context.Context) {
	defer close(lr.stopped)

	sl, err := lr.connectStream(ctx)
	if err != nil {
		sl, err = lr.reconnect(ctx, err)
	}
	lr.completeConnection(err)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return
		}
		lr.stopWithError(ctx, err)

		return
	}

	for {
		select {
		case <-ctx.Done():
			lr.closeStream(sl, lr.background.CloseReason())

			return
		case <-sl.background.StopDone():
		}

		reason := sl.background.CloseReason()
		lr.closeStream(sl, reason)
		if ctx.Err() != nil {
			return
		}
		sl, err = lr.reconnect(ctx, reason)
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return
			}
			lr.stopWithError(ctx, err)

			return
		}
	}
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

func (lr *TopicListenerReconnector) reconnect(ctx context.Context, reason error) (*streamListener, error) {
	clock := lr.streamConfig.clock
	started := clock.Now()

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if clock.Since(started) >= lr.streamConfig.RetrySettings.StartTimeout {
			return nil, listenerRetryTimeout(reason)
		}
		retryReason := reason
		retrySettings := lr.streamConfig.RetrySettings
		if transportErr := xerrors.TransportError(reason); transportErr != nil {
			retryReason = transportErr
			if checkError := retrySettings.CheckError; checkError != nil {
				retrySettings.CheckError = func(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
					args.Error = reason

					return checkError(args)
				}
			}
		}
		backoff, stopReason := topic.RetryDecision(retryReason, retrySettings, clock.Since(started))
		if stopReason != nil {
			if !errors.Is(stopReason, reason) {
				stopReason = errors.Join(stopReason, reason)
			}

			return nil, stopReason
		}

		delay := backoff.Delay(attempt)
		if remaining := retrySettings.StartTimeout - clock.Since(started); delay > remaining {
			delay = remaining
		}
		timer := clock.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()

			return nil, ctx.Err()
		case <-timer.Chan():
			timer.Stop()
		}
		if elapsed := clock.Since(started); elapsed >= lr.streamConfig.RetrySettings.StartTimeout {
			return nil, listenerRetryTimeout(reason)
		}

		sl, err := lr.connectStream(ctx)
		if err == nil {
			return sl, nil
		}
		reason = err
	}
}

func listenerRetryTimeout(reason error) error {
	return xerrors.WithStackTrace(fmt.Errorf(
		"ydb: topic listener reconnection timeout, last error: %w", xerrors.Unretryable(reason),
	))
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
