package retry

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wait"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// retryOperation is the interface that holds an operation for retry.
// if retryOperation returns not nil - operation will retry
// if retryOperation returns nil - retry loop will break
type retryOperation func(context.Context) (err error)

type retryOptions struct {
	id          string
	trace       *trace.Retry
	idempotent  bool
	stackTrace  bool
	fastBackoff backoff.Backoff
	slowBackoff backoff.Backoff

	panicCallback func(e interface{})
}

type retryOption interface {
	ApplyRetryOption(opts *retryOptions)
}

var _ retryOption = idOption("")

type idOption string

func (id idOption) ApplyRetryOption(opts *retryOptions) {
	opts.id = string(id)
}

// WithID applies id for identification call Retry in trace.Retry.OnRetry
func WithID(id string) idOption {
	return idOption(id)
}

var _ retryOption = stackTraceOption{}

type stackTraceOption struct{}

func (stackTrace stackTraceOption) ApplyRetryOption(opts *retryOptions) {
	opts.stackTrace = true
}

// WithStackTrace wraps errors with stacktrace from Retry call
func WithStackTrace() stackTraceOption {
	return stackTraceOption{}
}

var _ retryOption = traceOption{}

type traceOption struct {
	trace *trace.Retry
}

func (t traceOption) ApplyRetryOption(opts *retryOptions) {
	opts.trace = t.trace
}

// WithTrace returns trace option
func WithTrace(trace trace.Retry) traceOption {
	return traceOption{trace: &trace}
}

var _ retryOption = idempotentOption(false)

type idempotentOption bool

func (idempotent idempotentOption) ApplyRetryOption(opts *retryOptions) {
	opts.idempotent = bool(idempotent)
}

// WithIdempotent applies idempotent flag to retry operation
func WithIdempotent(idempotent bool) idempotentOption {
	return idempotentOption(idempotent)
}

var _ retryOption = fastBackoffOption{}

type fastBackoffOption struct {
	backoff backoff.Backoff
}

func (o fastBackoffOption) ApplyRetryOption(opts *retryOptions) {
	if o.backoff != nil {
		opts.fastBackoff = o.backoff
	}
}

// WithFastBackoff replaces default fast backoff
func WithFastBackoff(b backoff.Backoff) fastBackoffOption {
	return fastBackoffOption{backoff: b}
}

var _ retryOption = slowBackoffOption{}

type slowBackoffOption struct {
	backoff backoff.Backoff
}

func (o slowBackoffOption) ApplyRetryOption(opts *retryOptions) {
	if o.backoff != nil {
		opts.slowBackoff = o.backoff
	}
}

// WithSlowBackoff replaces default slow backoff
func WithSlowBackoff(b backoff.Backoff) slowBackoffOption {
	return slowBackoffOption{backoff: b}
}

var _ retryOption = panicCallbackOption{}

type panicCallbackOption struct {
	callback func(e interface{})
}

func (o panicCallbackOption) ApplyRetryOption(opts *retryOptions) {
	opts.panicCallback = o.callback
}

// WithPanicCallback returns panic callback option
// If not defined - panic would not intercept with driver
func WithPanicCallback(panicCallback func(e interface{})) panicCallbackOption {
	return panicCallbackOption{callback: panicCallback}
}

type (
	markRetryCallKey struct{}
)

func markRetryCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, markRetryCallKey{}, true)
}

func isRetryCalledAbove(ctx context.Context) bool {
	if _, has := ctx.Value(markRetryCallKey{}).(bool); has {
		return true
	}
	return false
}

// Retry provide the best effort fo retrying operation
//
// Retry implements internal busy loop until one of the following conditions is met:
//
// - context was canceled or deadlined
//
// - retry operation returned nil as error
//
// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
//
// If you need to retry your op func on some logic errors - you must return RetryableError() from retryOperation
func Retry(ctx context.Context, op retryOperation, opts ...retryOption) (err error) {
	options := &retryOptions{
		fastBackoff: backoff.Fast,
		slowBackoff: backoff.Slow,
		trace:       &trace.Retry{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyRetryOption(options)
		}
	}
	ctx = xcontext.WithIdempotent(ctx, options.idempotent)
	defer func() {
		if err != nil && options.stackTrace {
			err = xerrors.WithStackTrace(err,
				xerrors.WithSkipDepth(2), // 1 - exit from defer, 1 - exit from Retry call
			)
		}
	}()
	var (
		i        int
		attempts int

		code           = int64(0)
		onIntermediate = trace.RetryOnRetry(options.trace, &ctx, options.id, options.idempotent, isRetryCalledAbove(ctx))
	)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	for {
		i++
		attempts++
		select {
		case <-ctx.Done():
			return xerrors.WithStackTrace(
				fmt.Errorf("retry failed on attempt No.%d: %w",
					attempts, ctx.Err(),
				),
			)

		default:
			err = func() (err error) {
				if options.panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							options.panicCallback(e)
							err = xerrors.WithStackTrace(
								fmt.Errorf("panic recovered: %v", e),
							)
						}
					}()
				}
				return op(markRetryCall(ctx))
			}()

			if err == nil {
				return
			}

			if ctxErr := ctx.Err(); ctxErr != nil {
				return xerrors.WithStackTrace(
					xerrors.Join(
						fmt.Errorf("context error occurred on attempt No.%d", attempts),
						ctxErr, err,
					),
				)
			}

			m := Check(err)

			if m.StatusCode() != code {
				i = 0
			}

			if !m.MustRetry(options.idempotent) {
				return xerrors.WithStackTrace(
					xerrors.Join(
						fmt.Errorf("non-retryable error occurred on attempt No.%d (idempotent=%v)",
							attempts, options.idempotent,
						), err,
					),
				)
			}

			if e := wait.Wait(ctx, options.fastBackoff, options.slowBackoff, m.BackoffType(), i); e != nil {
				return xerrors.WithStackTrace(
					xerrors.Join(
						fmt.Errorf("wait exit on attempt No.%d",
							attempts,
						), e, err,
					),
				)
			}

			code = m.StatusCode()

			onIntermediate(err)
		}
	}
}

// Check returns retry mode for queryErr.
func Check(err error) (m retryMode) {
	code, errType, backoffType, deleteSession := xerrors.Check(err)
	return retryMode{
		code:          code,
		errType:       errType,
		backoff:       backoffType,
		deleteSession: deleteSession,
	}
}
