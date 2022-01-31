package table

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/rand"
)

func TestRetryerBackoffRetryCancelation(t *testing.T) {
	for _, testErr := range []error{
		// Errors leading to Wait repeat.
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorResourceExhausted),
		),
		fmt.Errorf("wrap transport error: %w", errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorResourceExhausted),
		)),
		errors.NewOpError(errors.WithOEReason(errors.StatusOverloaded)),
		fmt.Errorf("wrap op error: %w", errors.NewOpError(errors.WithOEReason(errors.StatusOverloaded))),
	} {
		t.Run("", func(t *testing.T) {
			backoff := make(chan chan time.Time)
			p := SingleSession(
				simpleSession(t),
			)

			ctx, cancel := context.WithCancel(context.Background())
			results := make(chan error)
			go func() {
				err := do(
					ctx,
					p,
					func(ctx context.Context, _ ydb_table.Session) error {
						return testErr
					},
					withFastBackoff(ydb_testutil.BackoffFunc(func(n int) <-chan time.Time {
						ch := make(chan time.Time)
						backoff <- ch
						return ch
					})),
					withSlowBackoff(ydb_testutil.BackoffFunc(func(n int) <-chan time.Time {
						ch := make(chan time.Time)
						backoff <- ch
						return ch
					})),
				)
				results <- err
			}()

			select {
			case <-backoff:
			case res := <-results:
				t.Fatalf("unexpected result: %v", res)
			}

			cancel()
		})
	}
}

func TestRetryerBadSession(t *testing.T) {
	closed := make(map[ydb_table.Session]bool)
	p := SessionProviderFunc{
		OnGet: func(ctx context.Context) (Session, error) {
			s := simpleSession(t)
			s.OnClose(func(context.Context) {
				closed[s] = true
			})
			return s, nil
		},
	}

	var (
		maxRetryes = 100
		i          int
		sessions   []ydb_table.Session
	)
	ctx, cancel := context.WithCancel(context.Background())
	err := do(
		ctx,
		p,
		func(ctx context.Context, s ydb_table.Session) error {
			sessions = append(sessions, s)
			i++
			if i > maxRetryes {
				cancel()
			}
			return errors.NewOpError(errors.WithOEReason(errors.StatusBadSession))
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("unexpected error: %v", err)
	}
	seen := make(map[ydb_table.Session]bool, len(sessions))
	for _, s := range sessions {
		if seen[s] {
			t.Errorf("session used twice")
		} else {
			seen[s] = true
		}
		if !closed[s] {
			t.Errorf("bad session was not closed")
		}
	}
}

func TestRetryerImmediateReturn(t *testing.T) {
	for _, testErr := range []error{
		errors.NewOpError(
			errors.WithOEReason(errors.StatusGenericError),
		),
		fmt.Errorf("wrap op error: %w", errors.NewOpError(
			errors.WithOEReason(errors.StatusGenericError),
		)),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorPermissionDenied),
		),
		fmt.Errorf("wrap transport error: %w", errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorPermissionDenied),
		)),
		errors.New("whoa"),
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if e := recover(); e != nil {
					t.Fatalf("unexpected panic: %v", e)
				}
			}()
			p := SingleSession(
				simpleSession(t),
			)
			err := do(
				context.Background(),
				p,
				func(ctx context.Context, _ ydb_table.Session) error {
					return testErr
				},
				withFastBackoff(ydb_testutil.BackoffFunc(func(n int) <-chan time.Time {
					panic("this code will not be called")
				})),
				withSlowBackoff(ydb_testutil.BackoffFunc(func(n int) <-chan time.Time {
					panic("this code will not be called")
				})),
			)
			if !errors.Is(err, testErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// We are testing all suspentions of custom operation func against to all deadline
// timeouts - all sub-tests must have latency less than timeouts (+tolerance)
func TestRetryContextDeadline(t *testing.T) {
	tolerance := 10 * time.Millisecond
	timeouts := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
	}
	sleeps := []time.Duration{
		time.Nanosecond,
		time.Microsecond,
		time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		5 * time.Second,
	}
	errs := []error{
		io.EOF,
		context.DeadlineExceeded,
		fmt.Errorf("test error"),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorUnknownCode),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorCanceled),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorUnknown),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorInvalidArgument),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorDeadlineExceeded),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorNotFound),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorAlreadyExists),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorPermissionDenied),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorResourceExhausted),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorFailedPrecondition),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorAborted),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorOutOfRange),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorUnimplemented),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorInternal),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorUnavailable),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorDataLoss),
		),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorUnauthenticated),
		),
		errors.NewOpError(errors.WithOEReason(errors.StatusUnknownStatus)),
		errors.NewOpError(errors.WithOEReason(errors.StatusBadRequest)),
		errors.NewOpError(errors.WithOEReason(errors.StatusUnauthorized)),
		errors.NewOpError(errors.WithOEReason(errors.StatusInternalError)),
		errors.NewOpError(errors.WithOEReason(errors.StatusAborted)),
		errors.NewOpError(errors.WithOEReason(errors.StatusUnavailable)),
		errors.NewOpError(errors.WithOEReason(errors.StatusOverloaded)),
		errors.NewOpError(errors.WithOEReason(errors.StatusSchemeError)),
		errors.NewOpError(errors.WithOEReason(errors.StatusGenericError)),
		errors.NewOpError(errors.WithOEReason(errors.StatusTimeout)),
		errors.NewOpError(errors.WithOEReason(errors.StatusBadSession)),
		errors.NewOpError(errors.WithOEReason(errors.StatusPreconditionFailed)),
		errors.NewOpError(errors.WithOEReason(errors.StatusAlreadyExists)),
		errors.NewOpError(errors.WithOEReason(errors.StatusNotFound)),
		errors.NewOpError(errors.WithOEReason(errors.StatusSessionExpired)),
		errors.NewOpError(errors.WithOEReason(errors.StatusCancelled)),
		errors.NewOpError(errors.WithOEReason(errors.StatusUndetermined)),
		errors.NewOpError(errors.WithOEReason(errors.StatusUnsupported)),
		errors.NewOpError(errors.WithOEReason(errors.StatusSessionBusy)),
	}
	client := &client{
		cc: ydb_testutil.NewDB(ydb_testutil.WithInvokeHandlers(ydb_testutil.InvokeHandlers{})),
	}
	p := SessionProviderFunc{
		OnGet: client.createSession,
	}
	for i := range timeouts {
		for j := range sleeps {
			timeout := timeouts[i]
			sleep := sleeps[j]
			t.Run(fmt.Sprintf("Timeout=%v,Sleep=%v", timeout, sleep), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_ = do(
					ydb_trace.WithRetry(
						ctx,
						ydb_trace.Retry{
							OnRetry: func(info ydb_trace.RetryLoopStartInfo) func(ydb_trace.RetryLoopDoneInfo) {
								return func(info ydb_trace.RetryLoopDoneInfo) {
									if info.Latency-timeouts[i] > tolerance {
										t.Errorf("unexpected latency: %v", info.Latency)
									}
								}
							},
						},
					),
					p,
					func(ctx context.Context, _ ydb_table.Session) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(sleep):
							return errs[rand.Int(len(errs))]
						}
					},
				)
			})
		}
	}
}
