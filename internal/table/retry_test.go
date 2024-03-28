package table

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestRetryerBackoffRetryCancelation(t *testing.T) {
	for _, testErr := range []error{
		// Errors leading to Wait repeat.
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.ResourceExhausted, ""),
		),
		fmt.Errorf("wrap transport error: %w", xerrors.Transport(
			grpcStatus.Error(grpcCodes.ResourceExhausted, ""),
		)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
		fmt.Errorf("wrap op error: %w", xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))),
	} {
		t.Run("", func(t *testing.T) {
			backoff := make(chan chan time.Time)
			p := SingleSession(
				simpleSession(t),
			)

			ctx, cancel := xcontext.WithCancel(context.Background())
			results := make(chan error)
			go func() {
				err := do(ctx, p,
					config.New(),
					func(ctx context.Context, _ table.Session) error {
						return testErr
					},
					nil,
					retry.WithFastBackoff(
						testutil.BackoffFunc(func(n int) <-chan time.Time {
							ch := make(chan time.Time)
							backoff <- ch

							return ch
						}),
					),
					retry.WithSlowBackoff(
						testutil.BackoffFunc(func(n int) <-chan time.Time {
							ch := make(chan time.Time)
							backoff <- ch

							return ch
						}),
					),
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
		OnGet: func(ctx context.Context) (*session, error) {
			s := simpleSession(t)
			s.onClose = append(s.onClose, func(s *session) {
				closed[s] = true
			})

			return s, nil
		},
		OnPut: func(ctx context.Context, s *session) error {
			if s.isClosing() {
				return s.Close(ctx)
			}

			return nil
		},
	}
	var (
		i          int
		maxRetryes = 100
		sessions   []table.Session
	)
	ctx, cancel := xcontext.WithCancel(context.Background())
	err := do(ctx, p, config.New(),
		func(ctx context.Context, s table.Session) error {
			sessions = append(sessions, s)
			i++
			if i > maxRetryes {
				cancel()
			}

			return xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
			)
		},
		func(err error) {},
	)
	if !xerrors.Is(err, context.Canceled) {
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
		OnGet: func(ctx context.Context) (*session, error) {
			s := simpleSession(t)
			s.onClose = append(s.onClose, func(s *session) {
				closed[s] = true
			})

			return s, nil
		},
		OnPut: func(ctx context.Context, s *session) error {
			if s.isClosing() {
				return s.Close(ctx)
			}

			return nil
		},
	}
	var sessions []table.Session
	for i := 0; i < 1000; i++ {
		err := do(
			context.Background(),
			p,
			config.New(),
			func(ctx context.Context, s table.Session) error {
				sessions = append(sessions, s)
				s.(*session).SetStatus(table.SessionClosing)

				return nil
			},
			nil,
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
		xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
		),
		fmt.Errorf("wrap op error: %w", xerrors.Operation(
			xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
		)),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.PermissionDenied, ""),
		),
		fmt.Errorf("wrap transport error: %w", xerrors.Transport(
			grpcStatus.Error(grpcCodes.PermissionDenied, ""),
		)),
		fmt.Errorf("whoa"),
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
				config.New(),
				func(ctx context.Context, _ table.Session) error {
					return testErr
				},
				nil,
				retry.WithFastBackoff(
					testutil.BackoffFunc(func(n int) <-chan time.Time {
						panic("this code will not be called")
					}),
				),
				retry.WithSlowBackoff(
					testutil.BackoffFunc(func(n int) <-chan time.Time {
						panic("this code will not be called")
					}),
				),
			)
			if !xerrors.Is(err, testErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// We are testing all suspentions of custom operation func against to all deadline
// timeouts - all sub-tests must have latency less than timeouts (+tolerance)
func TestRetryContextDeadline(t *testing.T) {
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
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.Canceled, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.Unknown, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.InvalidArgument, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.DeadlineExceeded, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.NotFound, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.AlreadyExists, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.PermissionDenied, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.ResourceExhausted, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.FailedPrecondition, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.Aborted, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.OutOfRange, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.Unimplemented, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.Internal, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.Unavailable, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.DataLoss, ""),
		),
		xerrors.Transport(
			grpcStatus.Error(grpcCodes.Unauthenticated, ""),
		),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_INTERNAL_ERROR)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ABORTED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SCHEME_ERROR)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_TIMEOUT)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_PRECONDITION_FAILED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_ALREADY_EXISTS)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_CANCELLED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNDETERMINED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNSUPPORTED)),
		xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY)),
	}
	client := &Client{
		cc: testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{})),
	}
	p := SessionProviderFunc{
		OnGet: client.internalPoolCreateSession,
	}
	r := xrand.New(xrand.WithLock())
	for i := range timeouts {
		for j := range sleeps {
			timeout := timeouts[i]
			sleep := sleeps[j]
			t.Run(fmt.Sprintf("Timeout=%v,Sleep=%v", timeout, sleep), func(t *testing.T) {
				ctx, cancel := xcontext.WithTimeout(context.Background(), timeout)
				defer cancel()
				_ = do(
					ctx,
					p,
					config.New(),
					func(ctx context.Context, _ table.Session) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(sleep):
							return errs[r.Int(len(errs))]
						}
					},
					nil,
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
			OnGet: func(ctx context.Context) (*session, error) {
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
				Err: xerrors.Operation(
					xerrors.WithStatusCode(
						Ydb.StatusIds_BAD_SESSION,
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
					xerrors.Operation(
						xerrors.WithStatusCode(
							Ydb.StatusIds_BAD_SESSION,
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
					xerrors.Operation(
						xerrors.WithStatusCode(
							Ydb.StatusIds_UNAUTHORIZED,
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
			err := do(
				ctx,
				p,
				config.New(),
				func(ctx context.Context, s table.Session) (err error) {
					sessions[s]++
					i++
					if i < limit {
						return test.error
					}

					return nil
				},
				nil,
			)
			//nolint:nestif
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

type SessionProviderFunc struct {
	OnGet func(context.Context) (*session, error)
	OnPut func(context.Context, *session) error
}

var _ SessionProvider = SessionProviderFunc{}

func (f SessionProviderFunc) Get(ctx context.Context) (*session, error) {
	if f.OnGet == nil {
		return nil, xerrors.WithStackTrace(errNoSession)
	}

	return f.OnGet(ctx)
}

func (f SessionProviderFunc) Put(ctx context.Context, s *session) error {
	if f.OnPut == nil {
		return xerrors.WithStackTrace(testutil.ErrNotImplemented)
	}

	return f.OnPut(ctx, s)
}

// SingleSession returns SessionProvider that uses only given session during
// retries.
func SingleSession(s *session) SessionProvider {
	return &singleSession{s: s}
}

type singleSession struct {
	s     *session
	empty bool
}

func (s *singleSession) Get(context.Context) (*session, error) {
	if s.empty {
		return nil, xerrors.WithStackTrace(errNoSession)
	}
	s.empty = true

	return s.s, nil
}

func (s *singleSession) Put(_ context.Context, x *session) error {
	if x != s.s {
		return xerrors.WithStackTrace(errUnexpectedSession)
	}
	if !s.empty {
		return xerrors.WithStackTrace(errSessionOverflow)
	}
	s.empty = false

	return nil
}

var (
	errNoSession         = xerrors.Wrap(fmt.Errorf("no session"))
	errUnexpectedSession = xerrors.Wrap(fmt.Errorf("unexpected session"))
	errSessionOverflow   = xerrors.Wrap(fmt.Errorf("session overflow"))
)
