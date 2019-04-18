package ydb

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// Default parameters used by Retry() functions within different sub packages.
var (
	DefaultMaxRetries   = 10
	DefaultRetryChecker = RetryChecker{
		RetryNotFound: true,
	}
	// DefaultBackoff is a logarithmic backoff retry strategy.
	DefaultBackoff = LogBackoff{
		SlotDuration: time.Second,
		Ceiling:      6,
	}
)

// RetryChecker contains options of checking errors returned by YDB for ability
// to retry provoked operation.
type RetryChecker struct {
	// RetryNotFound reports whether Repeater must retry ErrNotFound errors.
	RetryNotFound bool
}

// RetryMode reports whether operation is able to be retried and with which
// properties.
type RetryMode uint32

// Binary flags that used as RetryMode.
const (
	RetryUnavailable = 1 << iota >> 1
	RetryAvailable
	RetryBackoff
	RetryDeleteSession
)

func (m RetryMode) Retriable() bool         { return m != 0 }
func (m RetryMode) MustDeleteSession() bool { return m&RetryDeleteSession != 0 }
func (m RetryMode) MustBackoff() bool       { return m&RetryBackoff != 0 }

// Check returns retry mode for err.
func (r *RetryChecker) Check(err error) (m RetryMode) {
	switch e := err.(type) {
	case *TransportError:
		switch e.Reason {
		case TransportErrorResourceExhausted:
			m |= RetryBackoff
		default:
			return
		}
	case *OpError:
		switch e.Reason {
		case
			StatusUnavailable,
			StatusAborted:
			// Repeat immediately.

		case StatusOverloaded:
			m |= RetryBackoff

		case StatusBadSession:
			m |= RetryDeleteSession

		case StatusNotFound:
			if !r.RetryNotFound {
				return
			}
		default:
			return
		}
	default:
		return
	}
	return RetryAvailable | m
}

// Backoff is the interface that contains logic of delaying operation retry.
type Backoff interface {
	// Wait maps index of the retry to a channel which fulfillment means that
	// delay is over.
	//
	// Note that retry index begins from 0 and 0-th index means that it is the
	// first retry attempt after an initial error.
	Wait(n int) <-chan time.Time
}

// BackoffFunc is an adatper to allow the use of ordinary functions as Backoff.
type BackoffFunc func(n int) <-chan time.Time

// Wait implements Backoff interface.
func (f BackoffFunc) Wait(n int) <-chan time.Time {
	return f(n)
}

// WaitBackoff is a helper function that waits for i-th backoff b or ctx
// expiration.
// It returns non-nil error if and only if context expiration branch wins.
func WaitBackoff(ctx context.Context, b Backoff, i int) error {
	if b == nil {
		b = DefaultBackoff
	}
	select {
	case <-b.Wait(i):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LogBackoff contains logarithmic backoff policy.
type LogBackoff struct {
	// SlotDuration is a size of a single time slot used in backoff delay
	// calculation.
	// If SlotDuration is less or equal to zero, then the time.Second value is
	// used.
	SlotDuration time.Duration

	// Ceiling is a maximum degree of backoff delay growth.
	// If Ceiling is less or equal to zero, then the default ceiling of 1 is
	// used.
	Ceiling uint

	// JitterLimit controls fixed and random portions of backoff delay.
	// Its value can be in range [0, 1].
	// If JitterLimit is non zero, then the backoff delay will be equal to (F + R),
	// where F is a result of multiplication of this value and calculated delay
	// duration D; and R is a random sized part from [0,(D - F)].
	JitterLimit float64
}

// Wait implements Backoff interface.
func (b LogBackoff) Wait(n int) <-chan time.Time {
	return time.After(b.delay(n))
}

func (b LogBackoff) delay(i int) time.Duration {
	s := b.SlotDuration
	if s <= 0 {
		s = time.Second
	}
	n := 1 << min(uint(i), max(1, b.Ceiling))
	d := s * time.Duration(n)
	f := time.Duration(math.Min(1, math.Abs(b.JitterLimit)) * float64(d))
	if f == d {
		return f
	}
	return f + time.Duration(rand.Intn(int(d-f)+1))
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint) uint {
	if a > b {
		return a
	}
	return b
}
