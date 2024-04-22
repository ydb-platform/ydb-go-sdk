package budget

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type (
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	Budget interface {
		Acquire(ctx context.Context) error
	}
	budget struct {
		clock  clockwork.Clock
		ticker clockwork.Ticker
		quota  chan struct{}
		done   chan struct{}
	}
	option func(q *budget)
)

func withBudgetClock(clock clockwork.Clock) option {
	return func(q *budget) {
		q.clock = clock
	}
}

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func New(attemptsPerSecond int, opts ...option) *budget {
	q := &budget{
		clock: clockwork.NewRealClock(),
		done:  make(chan struct{}),
	}
	for _, opt := range opts {
		opt(q)
	}
	if attemptsPerSecond <= 0 {
		q.quota = make(chan struct{})
		close(q.quota)
	} else {
		q.quota = make(chan struct{}, attemptsPerSecond)
		for range make([]struct{}, attemptsPerSecond) {
			q.quota <- struct{}{}
		}
		q.ticker = q.clock.NewTicker(time.Second / time.Duration(attemptsPerSecond))
		go func() {
			defer close(q.quota)
			for {
				select {
				case <-q.ticker.Chan():
					select {
					case q.quota <- struct{}{}:
					case <-q.done:
						return
					}
				case <-q.done:
					return
				}
			}
		}()
	}

	return q
}

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func (q *budget) Stop() {
	if q.ticker != nil {
		q.ticker.Stop()
	}
	close(q.done)
}

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func (q *budget) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		select {
		case <-q.quota:
			return nil
		case <-ctx.Done():
			return xerrors.WithStackTrace(ctx.Err())
		}
	}
}
