// nolint:revive
package ydb_retry

import (
	"context"
	"math"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/rand"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

type retryableErrorOption func(e *errors.RetryableError)

const (
	BackoffTypeNoBackoff   = errors.BackoffTypeNoBackoff
	BackoffTypeFastBackoff = errors.BackoffTypeFastBackoff
	BackoffTypeSlowBackoff = errors.BackoffTypeSlowBackoff
)

func WithBackoff(t errors.BackoffType) retryableErrorOption {
	return func(e *errors.RetryableError) {
		e.BackoffType = t
	}
}

func WithDeleteSession() retryableErrorOption {
	return func(e *errors.RetryableError) {
		e.MustDeleteSession = true
	}
}

func RetryableError(err error, opts ...retryableErrorOption) error {
	re := &errors.RetryableError{
		Err: err,
	}
	for _, o := range opts {
		o(re)
	}
	return re
}

// Retry provide the best effort fo retrying operation
// Retry implements internal busy loop until one of the following conditions is met:
// - deadline was canceled or deadlined
// - retry operation returned nil as error
// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
// If you need to retry your op func on some logic errors - you must returns from op func RetryableError()
func Retry(ctx context.Context, isIdempotentOperation bool, op retryOperation) (err error) {
	var (
		i        int
		attempts int

		code   = int32(0)
		start  = time.Now()
		onDone = ydb_trace.RetryOnRetry(ydb_trace.ContextRetry(ctx), ctx)
	)
	defer func() {
		onDone(ctx, time.Since(start), err)
	}()
	for {
		i++
		attempts++
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			err = op(ctx)
			if err == nil {
				return
			}
			m := Check(err)
			if m.StatusCode() != code {
				i = 0
			}
			if !m.MustRetry(isIdempotentOperation) {
				return
			}
			if e := Wait(ctx, FastBackoff, SlowBackoff, m, i); e != nil {
				return
			}
			code = m.StatusCode()
		}
	}
}

// Check returns retry mode for err.
func Check(err error) (m retryMode) {
	var te *errors.TransportError
	var oe *errors.OpError
	var re *errors.RetryableError
	switch {
	case errors.As(err, &te):
		return retryMode{
			statusCode:      int32(te.Reason),
			operationStatus: te.Reason.OperationStatus(),
			backoff:         te.Reason.BackoffType(),
			deleteSession:   te.Reason.MustDeleteSession(),
		}
	case errors.As(err, &oe):
		return retryMode{
			statusCode:      int32(oe.Reason),
			operationStatus: oe.Reason.OperationStatus(),
			backoff:         oe.Reason.BackoffType(),
			deleteSession:   oe.Reason.MustDeleteSession(),
		}
	case errors.As(err, &re):
		return retryMode{
			statusCode:      -1,
			operationStatus: errors.OperationNotFinished,
			backoff:         re.BackoffType,
			deleteSession:   re.MustDeleteSession,
		}
	default:
		return retryMode{
			statusCode:      -1,
			operationStatus: errors.OperationFinished, // it's finish, not need any retry attempts
			backoff:         errors.BackoffTypeNoBackoff,
			deleteSession:   false,
		}
	}
}

func Wait(ctx context.Context, fastBackoff Backoff, slowBackoff Backoff, m retryMode, i int) error {
	var b Backoff
	switch m.BackoffType() {
	case errors.BackoffTypeNoBackoff:
		return nil
	case errors.BackoffTypeFastBackoff:
		b = fastBackoff
	case errors.BackoffTypeSlowBackoff:
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
	return f + time.Duration(rand.Int64(int64(d-f)+1))
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

// retryMode reports whether operation is able retried and with which properties.
type retryMode struct {
	statusCode      int32
	operationStatus errors.OperationStatus
	backoff         errors.BackoffType
	deleteSession   bool
}

func (m retryMode) MustRetry(isOperationIdempotent bool) bool {
	switch m.operationStatus {
	case errors.OperationFinished:
		return false
	case errors.OperationStatusUndefined:
		return isOperationIdempotent
	default:
		return true
	}
}

func (m retryMode) StatusCode() int32 { return m.statusCode }

func (m retryMode) MustBackoff() bool { return m.backoff&errors.BackoffTypeBackoffAny != 0 }

func (m retryMode) BackoffType() errors.BackoffType { return m.backoff }

func (m retryMode) MustDeleteSession() bool { return m.deleteSession }

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
// It returns non-nil error if and only if deadline expiration branch wins.
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
