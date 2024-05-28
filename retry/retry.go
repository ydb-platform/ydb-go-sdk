package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// retryOperation is the interface that holds an operation for retry.
// if retryOperation returns not nil - operation will retry
// if retryOperation returns nil - retry loop will break
type retryOperation func(context.Context) (err error)

type retryOptions struct {
	label       string
	call        call
	trace       *trace.Retry
	idempotent  bool
	stackTrace  bool
	fastBackoff backoff.Backoff
	slowBackoff backoff.Backoff
	budget      budget.Budget

	panicCallback func(e interface{})
}

type Option interface {
	ApplyRetryOption(opts *retryOptions)
}

var _ Option = labelOption("")

type labelOption string

func (label labelOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, WithLabel(string(label)))
}

func (label labelOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, WithLabel(string(label)))
}

func (label labelOption) ApplyRetryOption(opts *retryOptions) {
	opts.label = string(label)
}

// WithLabel applies label for identification call Retry in trace.Retry.OnRetry
func WithLabel(label string) labelOption {
	return labelOption(label)
}

var _ Option = (*callOption)(nil)

type callOption struct {
	call
}

func (call callOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, withCaller(call))
}

func (call callOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, withCaller(call))
}

func (call callOption) ApplyRetryOption(opts *retryOptions) {
	opts.call = call
}

type call interface {
	FunctionID() string
}

func withCaller(call call) callOption {
	return callOption{call}
}

var _ Option = stackTraceOption{}

type stackTraceOption struct{}

func (stackTraceOption) ApplyRetryOption(opts *retryOptions) {
	opts.stackTrace = true
}

func (stackTraceOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, WithStackTrace())
}

func (stackTraceOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, WithStackTrace())
}

// WithStackTrace wraps errors with stacktrace from Retry call
func WithStackTrace() stackTraceOption {
	return stackTraceOption{}
}

var _ Option = traceOption{t: nil}

type traceOption struct {
	t *trace.Retry
}

func (t traceOption) ApplyRetryOption(opts *retryOptions) {
	opts.trace = opts.trace.Compose(t.t)
}

func (t traceOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, WithTrace(t.t))
}

func (t traceOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, WithTrace(t.t))
}

// WithTrace returns trace option
func WithTrace(t *trace.Retry) traceOption {
	return traceOption{t: t}
}

var _ Option = budgetOption{b: nil}

type budgetOption struct {
	b budget.Budget
}

func (b budgetOption) ApplyRetryOption(opts *retryOptions) {
	opts.budget = b.b
}

func (b budgetOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, WithBudget(b.b))
}

func (b budgetOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, WithBudget(b.b))
}

//	WithBudget returns budget option
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithBudget(b budget.Budget) budgetOption {
	return budgetOption{b: b}
}

var _ Option = idempotentOption(false)

type idempotentOption bool

func (idempotent idempotentOption) ApplyRetryOption(opts *retryOptions) {
	opts.idempotent = bool(idempotent)
}

func (idempotent idempotentOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, WithIdempotent(bool(idempotent)))
}

func (idempotent idempotentOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, WithIdempotent(bool(idempotent)))
}

// WithIdempotent applies idempotent flag to retry operation
func WithIdempotent(idempotent bool) idempotentOption {
	return idempotentOption(idempotent)
}

var _ Option = fastBackoffOption{backoff: nil}

type fastBackoffOption struct {
	backoff backoff.Backoff
}

func (o fastBackoffOption) ApplyRetryOption(opts *retryOptions) {
	if o.backoff != nil {
		opts.fastBackoff = o.backoff
	}
}

func (o fastBackoffOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, WithFastBackoff(o.backoff))
}

func (o fastBackoffOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, WithFastBackoff(o.backoff))
}

// WithFastBackoff replaces default fast backoff
func WithFastBackoff(b backoff.Backoff) fastBackoffOption {
	return fastBackoffOption{backoff: b}
}

var _ Option = slowBackoffOption{backoff: nil}

type slowBackoffOption struct {
	backoff backoff.Backoff
}

func (o slowBackoffOption) ApplyRetryOption(opts *retryOptions) {
	if o.backoff != nil {
		opts.slowBackoff = o.backoff
	}
}

func (o slowBackoffOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, WithSlowBackoff(o.backoff))
}

func (o slowBackoffOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, WithSlowBackoff(o.backoff))
}

// WithSlowBackoff replaces default slow backoff
func WithSlowBackoff(b backoff.Backoff) slowBackoffOption {
	return slowBackoffOption{backoff: b}
}

var _ Option = panicCallbackOption{callback: nil}

type panicCallbackOption struct {
	callback func(e interface{})
}

func (o panicCallbackOption) ApplyRetryOption(opts *retryOptions) {
	opts.panicCallback = o.callback
}

func (o panicCallbackOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, WithPanicCallback(o.callback))
}

func (o panicCallbackOption) ApplyDoTxOption(opts *doTxOptions) {
	opts.retryOptions = append(opts.retryOptions, WithPanicCallback(o.callback))
}

// WithPanicCallback returns panic callback option
// If not defined - panic would not intercept with driver
func WithPanicCallback(panicCallback func(e interface{})) panicCallbackOption {
	return panicCallbackOption{callback: panicCallback}
}

// Retry provide the best effort fo retrying operation
//
// Retry implements internal busy loop until one of the following conditions is met:
//
// - context was canceled or deadlined
//
// - retry operation returned nil as error
//
// Warning: if context without deadline or cancellation func was passed, Retry will work infinitely.
//
// If you need to retry your op func on some logic errors - you must return RetryableError() from retryOperation
//
//nolint:funlen
func Retry(ctx context.Context, op retryOperation, opts ...Option) (finalErr error) {
	options := &retryOptions{
		label:         "",
		call:          stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/retry.Retry"),
		trace:         new(trace.Retry),
		idempotent:    false,
		stackTrace:    false,
		fastBackoff:   backoff.Fast,
		slowBackoff:   backoff.Slow,
		budget:        budget.Limited(-1),
		panicCallback: nil,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyRetryOption(options)
		}
	}
	if options.idempotent {
		ctx = xcontext.WithIdempotent(ctx, options.idempotent)
	}

	defer func() {
		if finalErr != nil && options.stackTrace {
			//nolint:gomnd
			finalErr = xerrors.WithStackTrace(finalErr,
				xerrors.WithSkipDepth(2), // 1 - exit from defer, 1 - exit from Retry call
			)
		}
	}()
	var (
		i        int
		attempts int

		code   = int64(0)
		onDone = trace.RetryOnRetry(options.trace, &ctx,
			options.call, options.label, options.idempotent, xcontext.IsNestedCall(ctx),
		)
	)
	defer func() {
		onDone(attempts, finalErr)
	}()
	for {
		i++
		attempts++
		select {
		case <-ctx.Done():
			return xerrors.WithStackTrace(
				fmt.Errorf("retry failed on attempt No.%d: %w", attempts, ctx.Err()),
			)

		default:
			err := opWithRecover(ctx, options, op)

			if err == nil {
				return nil
			}

			m := Check(err)

			if m.StatusCode() != code {
				i = 0
			}

			code = m.StatusCode()

			if !m.MustRetry(options.idempotent) {
				return xerrors.WithStackTrace(
					fmt.Errorf("non-retryable error occurred on attempt No.%d (idempotent=%v): %w",
						attempts, options.idempotent, err),
				)
			}

			t := time.NewTimer(backoff.Delay(m.BackoffType(), i,
				backoff.WithFastBackoff(options.fastBackoff),
				backoff.WithSlowBackoff(options.slowBackoff),
			))

			select {
			case <-ctx.Done():
				t.Stop()

				return xerrors.WithStackTrace(
					xerrors.Join(
						fmt.Errorf("attempt No.%d: %w", attempts, ctx.Err()),
						err,
					),
				)
			case <-t.C:
				t.Stop()

				if acquireErr := options.budget.Acquire(ctx); acquireErr != nil {
					return xerrors.WithStackTrace(
						xerrors.Join(
							fmt.Errorf("attempt No.%d: %w", attempts, budget.ErrNoQuota),
							acquireErr,
							err,
						),
					)
				}
			}
		}
	}
}

func opWithRecover(ctx context.Context, options *retryOptions, op retryOperation) (err error) {
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

	return op(ctx)
}

// Check returns retry mode for queryErr.
func Check(err error) (m retryMode) {
	code, errType, backoffType, deleteSession := xerrors.Check(err)

	return retryMode{
		code:               code,
		errType:            errType,
		backoff:            backoffType,
		isRetryObjectValid: deleteSession,
	}
}
