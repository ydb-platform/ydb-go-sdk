package retry

import (
	"context"
	"errors"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrNoQuota = xerrors.Wrap(errors.New("no retry quota"))

	_ Limiter = (*rateLimiter)(nil)
)

type (
	Limiter interface {
		Acquire(ctx context.Context) error
	}
	rateLimiter struct {
		clock  clockwork.Clock
		ticker clockwork.Ticker
		quota  chan struct{}
		done   chan struct{}
	}
	rateLimiterOption func(q *rateLimiter)
)

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

func (q *rateLimiter) Stop() {
	if q.ticker != nil {
		q.ticker.Stop()
	}
	close(q.done)
}

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
