package retry

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"
)

type (
	Limiter interface {
		Acquire(ctx context.Context) error
	}
	rateLimiter struct {
		clock  clockwork.Clock
		ticker clockwork.Ticker
		quota  chan struct{}
	}
	rateLimiterOption func(q *rateLimiter)
	unlimitedLimiter  struct{}
)

func (unlimitedLimiter) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

var (
	_ Limiter = (*rateLimiter)(nil)
	_ Limiter = (*unlimitedLimiter)(nil)
)

func Quoter(rate uint, opts ...rateLimiterOption) *rateLimiter {
	q := &rateLimiter{
		clock: clockwork.NewRealClock(),
		quota: make(chan struct{}, rate),
	}
	for range make([]struct{}, rate) {
		q.quota <- struct{}{}
	}
	for _, opt := range opts {
		opt(q)
	}
	q.ticker = q.clock.NewTicker(time.Second / time.Duration(rate))
	go func() {
		defer close(q.quota)
		for range q.ticker.Chan() {
			select {
			case q.quota <- struct{}{}:
			default:
			}
		}
	}()

	return q
}

func (q *rateLimiter) Stop() {
	q.ticker.Stop()
}

func (q *rateLimiter) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.quota:
		return nil
	}
}
