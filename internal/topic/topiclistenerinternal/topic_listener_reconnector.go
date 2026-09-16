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
	connectionIDCounter atomic.Int64
	closing             atomic.Bool

	m              sync.Mutex
	streamListener *streamListener
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
	sl := lr.streamListener
	lr.m.Unlock()

	if sl != nil {
		err = sl.Close(ctx, reason)
		if !errors.Is(err, context.Canceled) && !errors.Is(err, errTopicListenerClosed) {
			closeErrors = append(closeErrors, err)
		}
	}

	return errors.Join(closeErrors...)
}

func (lr *TopicListenerReconnector) connect(ctx context.Context) {
	sl, err := lr.connectStream(ctx)
	if err != nil {
		sl, err = lr.reconnect(ctx, err)
	}
	lr.completeConnection(err)
	if err != nil {
		lr.stopWithError(ctx, err)

		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-sl.background.StopDone():
		}

		reason := sl.background.CloseReason()
		_ = sl.Close(ctx, reason)
		sl, err = lr.reconnect(ctx, reason)
		if err != nil {
			lr.stopWithError(ctx, err)

			return
		}
	}
}

func (lr *TopicListenerReconnector) stopWithError(ctx context.Context, reason error) {
	ctx = context.WithoutCancel(ctx)
	go func() {
		_ = lr.background.Close(ctx, reason)
	}()
}

func (lr *TopicListenerReconnector) reconnect(ctx context.Context, reason error) (*streamListener, error) {
	clock := lr.streamConfig.clock
	started := clock.Now()

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		retryReason := reason
		if transportErr := xerrors.TransportError(reason); transportErr != nil {
			retryReason = transportErr
		}
		backoff, stopReason := topic.RetryDecision(retryReason, lr.streamConfig.RetrySettings, clock.Since(started))
		if stopReason != nil {
			if !errors.Is(stopReason, reason) {
				stopReason = errors.Join(stopReason, reason)
			}

			return nil, stopReason
		}

		timer := clock.NewTimer(backoff.Delay(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()

			return nil, ctx.Err()
		case <-timer.Chan():
			timer.Stop()
		}

		sl, err := lr.connectStream(ctx)
		if err == nil {
			return sl, nil
		}
		reason = err
	}
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
	case <-lr.background.StopDone():
		err := lr.background.CloseReason()
		if errors.Is(err, ErrUserCloseTopic) {
			return nil
		}

		return err
	}
}
