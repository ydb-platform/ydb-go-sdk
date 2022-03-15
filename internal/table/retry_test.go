package table

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/rand"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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
					func(ctx context.Context, _ table.Session) error {
						return testErr
					},
					table.Options{
						FastBackoff: testutil.BackoffFunc(func(n int) <-chan time.Time {
							ch := make(chan time.Time)
							backoff <- ch
							return ch
						}),
						SlowBackoff: testutil.BackoffFunc(func(n int) <-chan time.Time {
							ch := make(chan time.Time)
							backoff <- ch
							return ch
						}),
					},
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
	closed := make(map[table.Session]bool)
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
		i          int
		maxRetryes = 100
		sessions   []table.Session
	)
	ctx, cancel := context.WithCancel(context.Background())
	err := do(
		ctx,
		p,
		func(ctx context.Context, s table.Session) error {
			sessions = append(sessions, s)
			i++
			if i > maxRetryes {
				cancel()
			}
			return errors.NewOpError(
				errors.WithOEReason(errors.StatusBadSession),
			)
		},
		table.Options{},
	)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("unexpected error: %v", err)
	}
	seen := make(map[table.Session]bool, len(sessions))
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

func TestRetryerSessionClosing(t *testing.T) {
	closed := make(map[table.Session]bool)
	p := SessionProviderFunc{
		OnGet: func(ctx context.Context) (Session, error) {
			s := simpleSession(t)
			s.OnClose(func(context.Context) {
				closed[s] = true
			})
			return s, nil
		},
	}
	var sessions []table.Session
	for i := 0; i < 1000; i++ {
		err := do(
			context.Background(),
			p,
			func(ctx context.Context, s table.Session) error {
				sessions = append(sessions, s)
				s.(*session).SetStatus(options.SessionClosing)
				return nil
			},
			table.Options{},
		)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	seen := make(map[table.Session]bool, len(sessions))
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
				func(ctx context.Context, _ table.Session) error {
					return testErr
				},
				table.Options{
					FastBackoff: testutil.BackoffFunc(func(n int) <-chan time.Time {
						panic("this code will not be called")
					}),
					SlowBackoff: testutil.BackoffFunc(func(n int) <-chan time.Time {
						panic("this code will not be called")
					}),
				},
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
		cc: testutil.NewDB(testutil.WithInvokeHandlers(testutil.InvokeHandlers{})),
	}
	p := SessionProviderFunc{
		OnGet: client.createSession,
	}
	r := rand.New(rand.WithLock())
	for i := range timeouts {
		for j := range sleeps {
			timeout := timeouts[i]
			sleep := sleeps[j]
			t.Run(fmt.Sprintf("Timeout=%v,Sleep=%v", timeout, sleep), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				start := time.Now()
				_ = do(
					trace.WithRetry(
						ctx,
						trace.Retry{
							OnRetry: func(
								info trace.RetryLoopStartInfo,
							) func(
								intermediateInfo trace.RetryLoopIntermediateInfo,
							) func(
								trace.RetryLoopDoneInfo,
							) {
								return func(
									info trace.RetryLoopIntermediateInfo,
								) func(trace.RetryLoopDoneInfo) {
									return func(info trace.RetryLoopDoneInfo) {
										latency := time.Since(start)
										if latency-timeouts[i] > tolerance {
											t.Errorf("unexpected latency: %v", latency)
										}
									}
								}
							},
						},
					),
					p,
					func(ctx context.Context, _ table.Session) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(sleep):
							return errs[r.Int(len(errs))]
						}
					},
					table.Options{},
				)
			})
		}
	}
}

type CustomError struct {
	Err error
}

func (e *CustomError) Error() string {
	return fmt.Sprintf("custom error: %v", e.Err)
}

func (e *CustomError) Unwrap() error {
	return e.Err
}

func TestRetryWithCustomErrors(t *testing.T) {
	var (
		limit = 10
		ctx   = context.Background()
		p     = SessionProviderFunc{
			OnGet: func(ctx context.Context) (Session, error) {
				return simpleSession(t), nil
			},
		}
	)
	for _, test := range []struct {
		error         error
		retriable     bool
		deleteSession bool
	}{
		{
			error: &CustomError{
				Err: retry.RetryableError(
					fmt.Errorf("custom error"),
					retry.WithDeleteSession(),
				),
			},
			retriable:     true,
			deleteSession: true,
		},
		{
			error: &CustomError{
				Err: errors.NewOpError(
					errors.WithOEReason(
						errors.StatusBadSession,
					),
				),
			},
			retriable:     true,
			deleteSession: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					errors.NewOpError(
						errors.WithOEReason(
							errors.StatusBadSession,
						),
					),
				),
			},
			retriable:     true,
			deleteSession: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					errors.NewOpError(
						errors.WithOEReason(
							errors.StatusUnauthorized,
						),
					),
				),
			},
			retriable:     false,
			deleteSession: false,
		},
	} {
		t.Run(test.error.Error(), func(t *testing.T) {
			var (
				i        = 0
				sessions = make(map[table.Session]int)
			)
			err := do(ctx, p, func(ctx context.Context, s table.Session) (err error) {
				sessions[s]++
				i++
				if i < limit {
					return test.error
				}
				return nil
			}, table.Options{})
			// nolint:nestif
			if test.retriable {
				if i != limit {
					t.Fatalf("unexpected i: %d, err: %v", i, err)
				}
				if test.deleteSession {
					if len(sessions) != limit {
						t.Fatalf("unexpected len(sessions): %d, err: %v", len(sessions), err)
					}
					for s, n := range sessions {
						if n != 1 {
							t.Fatalf("unexpected session usage: %d, session: %v", n, s.ID())
						}
					}
				}
			} else {
				if i != 1 {
					t.Fatalf("unexpected i: %d, err: %v", i, err)
				}
				if len(sessions) != 1 {
					t.Fatalf("unexpected len(sessions): %d, err: %v", len(sessions), err)
				}
			}
		})
	}
}
