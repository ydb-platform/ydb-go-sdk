package table

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
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
				testutil.BackoffFunc(func(n int) <-chan time.Time {
					ch := make(chan time.Time)
					backoff <- ch
					return ch
				}),
			)

			ctx, cancel := context.WithCancel(context.Background())
			type result struct {
				err    error
				issues []error
			}
			results := make(chan result)
			go func() {
				err, issues := p.Retry(ctx, false, func(ctx context.Context, _ table.Session) error {
					return testErr
				})
				results <- result{err, issues}
			}()

			select {
			case <-backoff:
			case res := <-results:
				t.Fatalf("unexpected result: %v", res)
			}

			cancel()
			if res := <-results; !errors.Contains(res.issues, testErr) {
				t.Errorf("unexpected result: %v", res)
			}
		})
	}
}

func _newSession(t *testing.T, cl cluster.DB) table.Session {
	s, err := newSession(context.Background(), cl, trace.Table{})
	if err != nil {
		t.Fatalf("newSession unexpected error: %v", err)
	}
	return s
}

func TestRetryerImmediateRetry(t *testing.T) {
	for testErr, session := range map[error]table.Session{
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorResourceExhausted),
		): _newSession(t, simpleCluster),
		errors.NewTransportError(
			errors.WithTEReason(errors.TransportErrorAborted),
		): _newSession(t, simpleCluster),
		errors.NewOpError(
			errors.WithOEReason(errors.StatusUnavailable),
		): _newSession(t, simpleCluster),
		errors.NewOpError(
			errors.WithOEReason(errors.StatusOverloaded),
		): _newSession(t, simpleCluster),
		errors.NewOpError(
			errors.WithOEReason(errors.StatusAborted),
		): _newSession(t, simpleCluster),
		errors.NewOpError(
			errors.WithOEReason(errors.StatusNotFound),
		): _newSession(t, simpleCluster),
		fmt.Errorf("wrap op error: %w", errors.NewOpError(
			errors.WithOEReason(errors.StatusAborted),
		)): _newSession(t, simpleCluster),
	} {
		t.Run(fmt.Sprintf("err: %v, session: %v", testErr, session != nil), func(t *testing.T) {
			p := SingleSession(
				simpleSession(t),
				testutil.BackoffFunc(func(n int) <-chan time.Time {
					ch := make(chan time.Time, 1)
					ch <- time.Now()
					return ch
				}),
			)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err, issues := p.Retry(
				ctx,
				false,
				func(ctx context.Context, _ table.Session) error {
					return testErr
				},
			)
			if !errors.Contains(issues, testErr) {
				t.Fatalf("unexpected error: %v; want: %v", err, testErr)
			}
		})
	}
}

func TestRetryerBadSession(t *testing.T) {
	p := SessionProviderFunc{
		OnGet: func(ctx context.Context) (table.Session, error) {
			return simpleSession(t), nil
		},
	}

	var (
		maxRetryes = 100
		i          int
		sessions   []table.Session
	)
	ctx, cancel := context.WithCancel(context.Background())
	err, issues := p.Retry(
		ctx,
		false,
		func(ctx context.Context, s table.Session) error {
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
	if !errors.ContainsOpError(issues, errors.StatusBadSession) {
		t.Errorf("unexpected error: %v", err)
	}
	seen := make(map[table.Session]bool, len(sessions))
	for _, s := range sessions {
		if seen[s] {
			t.Errorf("session used twice")
		} else {
			seen[s] = true
		}
		if !s.IsClosed() {
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
			pool := SingleSession(
				simpleSession(t),
				testutil.BackoffFunc(func(n int) <-chan time.Time {
					panic("this code will not be called")
				}),
			)
			err, _ := pool.Retry(
				context.Background(),
				false,
				func(ctx context.Context, _ table.Session) error {
					return testErr
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
		cluster: testutil.NewDB(testutil.WithInvokeHandlers(testutil.InvokeHandlers{})),
	}
	pool := SessionProviderFunc{
		OnGet: client.CreateSession,
	}
	for i := range timeouts {
		for j := range sleeps {
			timeout := timeouts[i]
			sleep := sleeps[j]
			t.Run(fmt.Sprintf("timeout %v, sleep %v", timeout, sleep), func(t *testing.T) {
				random := rand.New(rand.NewSource(time.Now().Unix()))
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_, _ = pool.Retry(
					trace.WithRetry(
						ctx,
						trace.Retry{
							OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
								return func(info trace.RetryLoopDoneInfo) {
									if info.Latency-timeouts[i] > tolerance {
										t.Errorf("unexpected latency: %v (issues %d)", info.Latency, info.Issues)
									}
								}
							},
						},
					),
					false,
					func(ctx context.Context, _ table.Session) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(sleep):
							return errs[random.Intn(len(errs))]
						}
					},
				)
			})
		}
	}
}
