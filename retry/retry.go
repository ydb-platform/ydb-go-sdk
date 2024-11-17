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
	String() string
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

var _ Option = traceOption{}

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

var _ Option = budgetOption{}

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

var _ Option = fastBackoffOption{}

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

var _ Option = slowBackoffOption{}

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

var _ Option = panicCallbackOption{}

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
// # If you need to retry your op func on some logic errors - you must return RetryableError() from retryOperation
func Retry(ctx context.Context, op retryOperation, opts ...Option) (finalErr error) {
	fmt.Println("Retry 1")
	_, err := RetryWithResult[*struct{}](ctx, func(ctx context.Context) (*struct{}, error) {
		err := op(ctx)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return nil, nil //nolint:nilnil
	}, opts...)
	fmt.Println("Retry 2")
	if err != nil {
		fmt.Println("Retry 3")
		return xerrors.WithStackTrace(err)
	}

	fmt.Println("Retry 4")
	return nil
}

// RetryWithResult provide the best effort fo retrying operation which will return value or error
//
// RetryWithResult implements internal busy loop until one of the following conditions is met:
//
// - context was canceled or deadlined
//
// - retry operation returned nil as error
//
// Warning: if context without deadline or cancellation func was passed, RetryWithResult will work infinitely.
//
// # If you need to retry your op func on some logic errors - you must return RetryableError() from retryOperation
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func RetryWithResult[T any](ctx context.Context, //nolint:revive,funlen
	op func(context.Context) (T, error), opts ...Option,
) (_ T, finalErr error) {
	var (
		zeroValue T
		options   = &retryOptions{
			call:        stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/retry.RetryWithResult"),
			trace:       &trace.Retry{},
			budget:      budget.Limited(-1),
			fastBackoff: backoff.Fast,
			slowBackoff: backoff.Slow,
		}
	)
	for _, opt := range opts {
		fmt.Println("RWR opt", opt)
		if opt != nil {
			opt.ApplyRetryOption(options)
		}
	}
	if options.idempotent {
		fmt.Println("RWR opt idempotent")
		ctx = xcontext.WithIdempotent(ctx, options.idempotent)
	}

	defer func() {
		fmt.Println("RWR defer 1 - A")
		if finalErr != nil && options.stackTrace {
			//nolint:gomnd
			fmt.Println("RWR defer 1 - B")
			finalErr = xerrors.WithStackTrace(finalErr,
				xerrors.WithSkipDepth(2), // 1 - exit from defer, 1 - exit from Retry call
			)
		}
	}()
	var (
		i        int
		attempts int
		lastErr  error

		code   = int64(0)
		onDone = trace.RetryOnRetry(options.trace, &ctx,
			options.call, options.label, options.idempotent, xcontext.IsNestedCall(ctx),
		)
	)
	defer func() {
		fmt.Println("RWR defer 2")
		onDone(attempts, finalErr)
	}()
	for {
		i++
		attempts++
		select {
		case <-ctx.Done():
			fmt.Println("RWR for loop ctx.Done")
			return zeroValue, xerrors.WithStackTrace(xerrors.Join(
				fmt.Errorf("retry failed on attempt No.%d: %w", attempts, ctx.Err()),
				lastErr,
			))

		default:
			fmt.Println("RWR for loop defer 1")
			v, err := opWithRecover(ctx, options, op)

			if err == nil {
				fmt.Println("RWR for loop defer 2")
				return v, nil
			}

			m := Check(err)

			fmt.Println("RWR for loop defer 3", m, code)
			if m.StatusCode() != code {
				i = 0
			}

			code = m.StatusCode()

			if !m.MustRetry(options.idempotent) {
				fmt.Println("RWR for loop defer 4")
				return zeroValue, xerrors.WithStackTrace(xerrors.Join(
					fmt.Errorf("non-retryable error occurred on attempt No.%d (idempotent=%v): %w",
						attempts, options.idempotent, err),
					lastErr,
				))
			}

			t := time.NewTimer(backoff.Delay(m.BackoffType(), i,
				backoff.WithFastBackoff(options.fastBackoff),
				backoff.WithSlowBackoff(options.slowBackoff),
			))

			fmt.Println("RWR for loop defer 5")
			select {
			case <-ctx.Done():
				fmt.Println("RWR for loop defer case ctx.Done")
				t.Stop()

				return zeroValue, xerrors.WithStackTrace(
					xerrors.Join(
						fmt.Errorf("attempt No.%d: %w", attempts, ctx.Err()),
						err,
						lastErr,
					),
				)
			case <-t.C:
				fmt.Println("RWR for loop defer case timer")
				t.Stop()

				if acquireErr := options.budget.Acquire(ctx); acquireErr != nil {
					return zeroValue, xerrors.WithStackTrace(
						xerrors.Join(
							fmt.Errorf("attempt No.%d: %w", attempts, budget.ErrNoQuota),
							acquireErr,
							err,
							lastErr,
						),
					)
				}
			}

			lastErr = err
		}
	}
}

func opWithRecover[T any](ctx context.Context,
	options *retryOptions, op func(context.Context) (T, error),
) (_ T, finalErr error) {
	var zeroValue T
	if options.panicCallback != nil {
		defer func() {
			if e := recover(); e != nil {
				options.panicCallback(e)
				finalErr = xerrors.WithStackTrace(
					fmt.Errorf("panic recovered: %v", e),
				)
			}
		}()
	}

	v, err := op(ctx)
	if err != nil {
		return zeroValue, xerrors.WithStackTrace(err)
	}

	return v, nil
}

// Check returns retry mode for queryErr.
func Check(err error) (m retryMode) {
	code, errType, backoffType, invalidObject := xerrors.Check(err)

	return retryMode{
		code:               code,
		errType:            errType,
		backoff:            backoffType,
		isRetryObjectValid: !invalidObject,
	}
}
