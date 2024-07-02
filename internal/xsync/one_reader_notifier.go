package xsync

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errMoreThenOneSubscriber        = xerrors.Wrap(errors.New("ydb: more then one subscriber wait the notifier"))
	errSingleReceiverNotifierClosed = xerrors.Wrap(errors.New("ydb: SingleReceiverNotifier closed"))
)

// SingleReceiverNotifier used for fire notifications about some event
// when no more than one subscriber for notifications
type SingleReceiverNotifier struct {
	empty.DoNotCopy
	eventHappened empty.Chan
	waiting       sync.Mutex

	lifeTime       context.Context
	lifeTimeDone   empty.ChanReadonly
	lifeTimeCancel context.CancelCauseFunc
}

func NewSingleReceiverNotifier() SingleReceiverNotifier {
	ctx, cancel := context.WithCancelCause(context.Background())
	return SingleReceiverNotifier{
		DoNotCopy:      empty.DoNotCopy{},
		eventHappened:  make(empty.Chan, 1),
		lifeTime:       ctx,
		lifeTimeDone:   ctx.Done(),
		lifeTimeCancel: cancel,
	}
}

func (n *SingleReceiverNotifier) Close(reason error) error {
	n.lifeTimeCancel(reason)
	if cause := context.Cause(n.lifeTime); errors.Is(cause, reason) {
		return fmt.Errorf("ydb: internal notifier already closed: %w", cause)
	}

	return nil
}

// Notify subscriber about event happened. It is fast non-blocking notification.
// It can be call simultaneously from many goroutines.
func (n *SingleReceiverNotifier) Notify() {
	select {
	case n.eventHappened <- empty.Struct{}:
		// notification send
	default:
		// channel has notification already
	}
}

// Wait a notification about event.
// If event happened before start wait - return immediately.
// Only one goroutine can listen the event same time. If listen started more than one goroutine:
// second will receive error errMoreThenOneSubscriber
func (n *SingleReceiverNotifier) Wait(ctx context.Context) error {
	if !n.waiting.TryLock() {
		return xerrors.WithStackTrace(errMoreThenOneSubscriber)
	}
	defer n.waiting.Unlock()

	_, err := xcontext.ReadWithContextCause2(ctx, n.lifeTime, n.eventHappened)
	return err
}
