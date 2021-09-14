package ydb

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// Default parameters used by Retry() functions within different sub packages.
const (
	fastSlot = 5 * time.Millisecond
	slowSlot = 1 * time.Second
)

// Default parameters used by Retry() functions within different sub packages.
var (
	FastBackoff = logBackoff{
		SlotDuration: fastSlot,
		Ceiling:      6,
	}
	SlowBackoff = logBackoff{
		SlotDuration: slowSlot,
		Ceiling:      6,
	}
)

// retryOperation is the interface that holds an operation for retry.
type retryOperation func(context.Context) (err error)

// Retry provide the best effort fo retrying operation
// Retry implements internal busy loop until one of the following conditions is met:
// - context was cancelled or deadlined
// - retry operation returned nil as error
// Warning: if context without deadline or cancellation func Retry will be worked infinite
func Retry(ctx context.Context, retryNoIdempotent bool, op retryOperation) (err error) {
	var (
		i        int
		attempts int

		code   = int32(0)
		start  = time.Now()
		onDone = OnRetry(ctx)
	)
	defer func() {
		onDone(ctx, time.Since(start), attempts)
	}()
	for {
		i++
		attempts++
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if err = op(ctx); err == nil {
				return
			}
			m := Check(err)
			if m.StatusCode() != code {
				i = 0
			}
			if !m.MustRetry(retryNoIdempotent) {
				return err
			}
			if e := Wait(ctx, FastBackoff, SlowBackoff, m, i); e != nil {
				return err
			}
			code = m.StatusCode()
		}
	}
	return err
}

// Check returns retry mode for err.
func Check(err error) (m retryMode) {
	var te *TransportError
	var oe *OpError

	switch {
	case errors.As(err, &te):
		return retryMode{
			statusCode:    int32(te.Reason),
			retry:         te.Reason.retryType(),
			backoff:       te.Reason.backoffType(),
			deleteSession: te.Reason.mustDeleteSession(),
		}
	case errors.As(err, &oe):
		return retryMode{
			statusCode:    int32(oe.Reason),
			retry:         oe.Reason.retryType(),
			backoff:       oe.Reason.backoffType(),
			deleteSession: oe.Reason.mustDeleteSession(),
		}
	default:
		return retryMode{
			statusCode:    -1,
			retry:         retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		}
	}
}

func Wait(ctx context.Context, fastBackoff Backoff, slowBackoff Backoff, m retryMode, i int) error {
	var b Backoff
	switch m.BackoffType() {
	case backoffTypeNoBackoff:
		return nil
	case backoffTypeFastBackoff:
		b = fastBackoff
	case backoffTypeSlowBackoff:
		b = slowBackoff
	}
	return waitBackoff(ctx, b, i)
}

// logBackoff contains logarithmic Backoff policy.
type logBackoff struct {
	// SlotDuration is a size of a single time slot used in Backoff delay
	// calculation.
	// If SlotDuration is less or equal to zero, then the time.Second value is
	// used.
	SlotDuration time.Duration

	// Ceiling is a maximum degree of Backoff delay growth.
	// If Ceiling is less or equal to zero, then the default ceiling of 1 is
	// used.
	Ceiling uint

	// JitterLimit controls fixed and random portions of Backoff delay.
	// Its value can be in range [0, 1].
	// If JitterLimit is non zero, then the Backoff delay will be equal to (F + R),
	// where F is a result of multiplication of this value and calculated delay
	// duration D; and R is a random sized part from [0,(D - F)].
	JitterLimit float64
}

// Wait implements Backoff interface.
func (b logBackoff) Wait(n int) <-chan time.Time {
	return time.After(b.delay(n))
}

// delay returns mapping of i to delay.
func (b logBackoff) delay(i int) time.Duration {
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

// retryMode reports whether operation is able to be retried and with which
// properties.
type retryMode struct {
	statusCode    int32
	retry         retryType
	backoff       backoffType
	deleteSession bool
}

// backoffType reports how to Backoff operation
type backoffType uint8

// Binary flags that used as backoffType
const (
	backoffTypeNoBackoff backoffType = 1 << iota >> 1

	backoffTypeFastBackoff
	backoffTypeSlowBackoff

	backoffTypeBackoffAny = backoffTypeFastBackoff | backoffTypeSlowBackoff
)

// retryType reports which operations need to retry
type retryType uint8

// Binary flags that used as retryType
const (
	retryTypeNoRetry retryType = 1 << iota >> 1
	retryTypeIdempotent
	retryTypeNoIdempotent

	retryTypeAny = retryTypeNoIdempotent | retryTypeIdempotent
)

func (m retryMode) MustRetry(retryNoIdempotent bool) bool {
	switch m.retry {
	case retryTypeNoRetry:
		return false
	case retryTypeNoIdempotent:
		return retryNoIdempotent
	default:
		return true
	}
}
func (m retryMode) StatusCode() int32        { return m.statusCode }
func (m retryMode) MustBackoff() bool        { return m.backoff&backoffTypeBackoffAny != 0 }
func (m retryMode) BackoffType() backoffType { return m.backoff }
func (m retryMode) MustDeleteSession() bool  { return m.deleteSession }

// Backoff is the interface that contains logic of delaying operation retry.
type Backoff interface {
	// Wait maps index of the retry to a channel which fulfillment means that
	// delay is over.
	//
	// Note that retry index begins from 0 and 0-th index means that it is the
	// first retry attempt after an initial error.
	Wait(n int) <-chan time.Time
}

// waitBackoff is a helper function that waits for i-th Backoff b or ctx
// expiration.
// It returns non-nil error if and only if context expiration branch wins.
func waitBackoff(ctx context.Context, b Backoff, i int) error {
	if b == nil {
		return ctx.Err()
	}
	select {
	case <-b.Wait(i):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e StatusCode) retryType() retryType {
	switch e {
	case
		StatusAborted,
		StatusUnavailable,
		StatusOverloaded,
		StatusBadSession,
		StatusSessionBusy,
		StatusNotFound:
		return retryTypeAny
	case
		StatusCancelled,
		StatusUndetermined:
		return retryTypeIdempotent
	default:
		return retryTypeNoRetry
	}
}

func (e StatusCode) backoffType() backoffType {
	switch e {
	case
		StatusOverloaded:
		return backoffTypeSlowBackoff
	case
		StatusAborted,
		StatusUnavailable,
		StatusBadSession,
		StatusCancelled,
		StatusSessionBusy,
		StatusUndetermined:
		return backoffTypeFastBackoff
	default:
		return backoffTypeNoBackoff
	}
}

func (e StatusCode) mustDeleteSession() bool {
	switch e {
	case
		StatusBadSession,
		StatusSessionExpired,
		StatusSessionBusy:
		return true
	default:
		return false
	}
}

func (t TransportErrorCode) retryType() retryType {
	switch t {
	case
		TransportErrorResourceExhausted,
		TransportErrorAborted:
		return retryTypeAny
	case
		TransportErrorInternal,
		TransportErrorUnavailable:
		return retryTypeIdempotent
	default:
		return retryTypeNoRetry
	}
}

func (t TransportErrorCode) backoffType() backoffType {
	switch t {
	case
		TransportErrorInternal,
		TransportErrorUnavailable:
		return backoffTypeFastBackoff
	case
		TransportErrorResourceExhausted:
		return backoffTypeSlowBackoff
	default:
		return backoffTypeNoBackoff
	}
}

func (t TransportErrorCode) mustDeleteSession() bool {
	switch t {
	case
		TransportErrorCanceled,
		TransportErrorResourceExhausted,
		TransportErrorOutOfRange:
		return false
	default:
		return true
	}
}
