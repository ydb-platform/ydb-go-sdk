package retry

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
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
	label       string
	call        call
	trace       *trace.Retry
	idempotent  bool
	stackTrace  bool
	fastBackoff backoff.Backoff
	slowBackoff backoff.Backoff

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
// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
//
// If you need to retry your op func on some logic errors - you must return RetryableError() from retryOperation
func Retry(ctx context.Context, op retryOperation, opts ...Option) (finalErr error) {
	options := &retryOptions{
		call:        stack.FunctionID(""),
		trace:       &trace.Retry{},
		fastBackoff: backoff.Fast,
		slowBackoff: backoff.Slow,
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
			finalErr = xerrors.WithStackTrace(finalErr,
				xerrors.WithSkipDepth(2), // 1 - exit from defer, 1 - exit from Retry call
			)
		}
	}()
	var (
		i        int
		attempts int

		code           = int64(0)
		onIntermediate = trace.RetryOnRetry(options.trace, &ctx,
			options.label, options.call, options.label, options.idempotent, xcontext.IsNestedCall(ctx),
		)
	)
	defer func() {
		onIntermediate(finalErr)(attempts, finalErr)
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
			err := func() (err error) {
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
			}()

			if err == nil {
				return nil
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
					fmt.Errorf("non-retryable error occurred on attempt No.%d (idempotent=%v): %w",
						attempts, options.idempotent, err,
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
