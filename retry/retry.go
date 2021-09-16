package retry

import (
	"context"
	errors2 "github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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
		onDone = trace.OnRetry(ctx)
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
	var te *errors2.TransportError
	var oe *errors2.OpError

	switch {
	case errors2.As(err, &te):
		return retryMode{
			statusCode:    int32(te.Reason),
			retry:         te.Reason.RetryType(),
			backoff:       te.Reason.BackoffType(),
			deleteSession: te.Reason.MustDeleteSession(),
		}
	case errors2.As(err, &oe):
		return retryMode{
			statusCode:    int32(oe.Reason),
			retry:         oe.Reason.RetryType(),
			backoff:       oe.Reason.BackoffType(),
			deleteSession: oe.Reason.MustDeleteSession(),
		}
	default:
		return retryMode{
			statusCode:    -1,
			retry:         errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		}
	}
}

func Wait(ctx context.Context, fastBackoff Backoff, slowBackoff Backoff, m retryMode, i int) error {
	var b Backoff
	switch m.BackoffType() {
	case errors2.BackoffTypeNoBackoff:
		return nil
	case errors2.BackoffTypeFastBackoff:
		b = fastBackoff
	case errors2.BackoffTypeSlowBackoff:
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
	retry         errors2.RetryType
	backoff       errors2.BackoffType
	deleteSession bool
}

func (m retryMode) MustRetry(retryNoIdempotent bool) bool {
	switch m.retry {
	case errors2.RetryTypeNoRetry:
		return false
	case errors2.RetryTypeNoIdempotent:
		return retryNoIdempotent
	default:
		return true
	}
}
func (m retryMode) StatusCode() int32                { return m.statusCode }
func (m retryMode) MustBackoff() bool                { return m.backoff&errors2.BackoffTypeBackoffAny != 0 }
func (m retryMode) BackoffType() errors2.BackoffType { return m.backoff }
func (m retryMode) MustDeleteSession() bool          { return m.deleteSession }

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
