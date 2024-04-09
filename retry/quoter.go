package retry

import (
	"context"
	"errors"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	ErrNoQuota = xerrors.Wrap(errors.New("no retry quota"))

	_ Limiter = (*rateLimiter)(nil)
)

type (
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	Limiter interface {
		Acquire(ctx context.Context) error
	}
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	LimiterStoper interface {
		Limiter
		Stop()
	}
	rateLimiter struct {
		clock  clockwork.Clock
		ticker clockwork.Ticker
		quota  chan struct{}
		done   chan struct{}
	}
	rateLimiterOption func(q *rateLimiter)
)

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func Quoter(attemptsPerSecond int, opts ...rateLimiterOption) *rateLimiter {
	q := &rateLimiter{
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
func (q *rateLimiter) Stop() {
	if q.ticker != nil {
		q.ticker.Stop()
	}
	close(q.done)
}

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func (q *rateLimiter) Acquire(ctx context.Context) error {
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
