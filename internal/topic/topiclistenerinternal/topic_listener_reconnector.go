package topiclistenerinternal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

var (
	ErrUserCloseTopic      = errors.New("ydb: user closed topic listener")
	errTopicListenerClosed = errors.New("ydb: the topic listener already closed")
)

type TopicListenerReconnector struct {
	streamConfig  *StreamListenerConfig
	client        TopicClient
	handler       EventHandler
	clock         clockwork.Clock
	retrySettings topic.RetrySettings

	background background.Worker

	connectionResult    error
	connectionCompleted empty.Chan
	connectionDoneOnce  sync.Once
	stopped             empty.Chan
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
	return newTopicListenerReconnector(client, streamConfig, handler, clockwork.NewRealClock()), nil
}

func newTopicListenerReconnector(
	client TopicClient,
	streamConfig *StreamListenerConfig,
	handler EventHandler,
	clock clockwork.Clock,
) *TopicListenerReconnector {
	res := &TopicListenerReconnector{
		streamConfig: streamConfig,
		client:       client,
		handler:      handler,
		clock:        clock,
		retrySettings: topic.RetrySettings{
			StartTimeout: topic.DefaultStartTimeout,
		},
		connectionCompleted: make(empty.Chan),
		stopped:             make(empty.Chan),
	}

	res.background.Start("connection loop", res.connectionLoop)

	return res
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
	lr.completeConnection(reason)

	var closeErrors []error
	err := lr.background.Close(ctx, reason)
	if err != nil && !errors.Is(err, background.ErrAlreadyClosed) {
		closeErrors = append(closeErrors, err)
	}

	lr.m.Lock()
	sl := lr.streamListener
	lr.m.Unlock()

	if sl != nil {
		err = sl.Close(ctx, reason)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errTopicListenerClosed) {
			closeErrors = append(closeErrors, err)
		}
	}

	return errors.Join(closeErrors...)
}

func (lr *TopicListenerReconnector) completeConnection(err error) {
	lr.connectionDoneOnce.Do(func() {
		lr.m.Lock()
		lr.connectionResult = err
		lr.m.Unlock()
		close(lr.connectionCompleted)
	})
}

func (lr *TopicListenerReconnector) setStreamListener(sl *streamListener) {
	lr.m.Lock()
	lr.streamListener = sl
	lr.m.Unlock()
}

func (lr *TopicListenerReconnector) clearStreamListener(sl *streamListener) {
	lr.m.Lock()
	if lr.streamListener == sl {
		lr.streamListener = nil
	}
	lr.m.Unlock()
}

func (lr *TopicListenerReconnector) waitRetry(
	ctx context.Context,
	reason error,
	attempt int,
	retriesStarted time.Time,
) bool {
	retryBackoff, stopReason := topic.RetryDecision(
		reason,
		lr.retrySettings,
		lr.clock.Since(retriesStarted),
	)
	if stopReason != nil {
		lr.completeConnection(stopReason)
		_ = lr.background.Close(ctx, stopReason)

		return false
	}

	timer := lr.clock.NewTimer(retryBackoff.Delay(attempt))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.Chan():
		return true
	}
}

func (lr *TopicListenerReconnector) closeStreamListener(sl *streamListener, reason error) {
	closeCtx, cancel := context.WithTimeout(xcontext.ValueOnly(lr.background.Context()), time.Second)
	defer cancel()

	_ = sl.Close(closeCtx, reason)
}

func (lr *TopicListenerReconnector) connectStreamListener(ctx context.Context) (*streamListener, error) {
	return newStreamListener(
		ctx,
		lr.client,
		lr.handler,
		lr.streamConfig,
		&lr.connectionIDCounter,
	)
}

func (lr *TopicListenerReconnector) connectionLoop(ctx context.Context) {
	defer close(lr.stopped)

	var (
		attempt         int
		previousAttempt time.Time
		retriesStarted  time.Time
		reconnectReason error
	)

	for {
		if ctx.Err() != nil {
			return
		}

		now := lr.clock.Now()
		if retriesStarted.IsZero() || topic.CheckResetReconnectionCounters(
			previousAttempt,
			now,
			topic.DefaultStartTimeout,
		) {
			attempt = 0
			retriesStarted = now
		} else {
			attempt++
		}
		previousAttempt = now

		if reconnectReason != nil && !lr.waitRetry(ctx, reconnectReason, attempt, retriesStarted) {
			return
		}

		sl, err := lr.connectStreamListener(ctx)
		if err != nil {
			reconnectReason = err

			continue
		}

		lr.setStreamListener(sl)
		lr.completeConnection(nil)

		select {
		case <-ctx.Done():
			lr.closeStreamListener(sl, lr.background.CloseReason())
			lr.clearStreamListener(sl)

			return
		case <-sl.background.Done():
			reconnectReason = sl.background.CloseReason()
		}

		lr.closeStreamListener(sl, reconnectReason)
		lr.clearStreamListener(sl)

		if ctx.Err() != nil {
			return
		}

		retriesStarted = lr.clock.Now()
	}
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

	lr.m.Lock()
	err := lr.connectionResult
	lr.m.Unlock()

	return err
}

func (lr *TopicListenerReconnector) WaitStop(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lr.stopped:
		err := lr.background.CloseReason()
		if errors.Is(err, ErrUserCloseTopic) {
			return nil
		}

		return err
	}
}
