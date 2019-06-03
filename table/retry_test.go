package table

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	ydb "github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/testutil"
)

func TestRetryerBackoffRetryCancelation(t *testing.T) {
	for _, testErr := range []error{
		// Errors leading to backoff repeat.
		&ydb.TransportError{
			Reason: ydb.TransportErrorResourceExhausted,
		},
		&ydb.OpError{
			Reason: ydb.StatusOverloaded,
		},
	} {
		t.Run("", func(t *testing.T) {
			backoff := make(chan chan time.Time)
			r := Retryer{
				MaxRetries: 1,
				Backoff: ydb.BackoffFunc(func(n int) <-chan time.Time {
					ch := make(chan time.Time)
					backoff <- ch
					return ch
				}),
				SessionProvider: SingleSession(simpleSession()),
			}

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error)
			go func() {
				result <- r.Do(ctx, OperationFunc(func(ctx context.Context, _ *Session) error {
					return testErr
				}))
			}()

			select {
			case <-backoff:
			case err := <-result:
				t.Fatalf("unexpected result: %v", err)
			}

			cancel()
			if err := <-result; err != testErr {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestRetryerImmediateiRetry(t *testing.T) {
	for _, testErr := range []error{
		&ydb.OpError{
			Reason: ydb.StatusUnavailable,
		},
		&ydb.OpError{
			Reason: ydb.StatusAborted,
		},
		&ydb.OpError{
			Reason: ydb.StatusNotFound,
		},
	} {
		t.Run("", func(t *testing.T) {
			var count int
			r := Retryer{
				MaxRetries:      1,
				RetryChecker:    ydb.DefaultRetryChecker,
				SessionProvider: SingleSession(new(Session)),
			}
			err := r.Do(
				context.Background(),
				OperationFunc(func(ctx context.Context, _ *Session) error {
					count++
					return testErr
				}),
			)
			if act, exp := count, r.MaxRetries+1; act != exp {
				t.Errorf("unexpected operation calls: %v; want %v", act, exp)
			}
			if err != testErr {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestRetryerBadSession(t *testing.T) {
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				return nil
			},
		},
	}
	r := Retryer{
		MaxRetries: 3,
		SessionProvider: SessionProviderFunc{
			OnGet: client.CreateSession,
		},
	}

	var sessions []*Session
	err := r.Do(
		context.Background(),
		OperationFunc(func(ctx context.Context, s *Session) error {
			sessions = append(sessions, s)
			return &ydb.OpError{
				Reason: ydb.StatusBadSession,
			}
		}),
	)
	if !ydb.IsOpError(err, ydb.StatusBadSession) {
		t.Errorf("unexpected error: %v", err)
	}
	if act, exp := len(sessions), r.MaxRetries+1; act != exp {
		t.Errorf("unexpected operation calls: %v; want %v", act, exp)
	}
	seen := make(map[*Session]bool, len(sessions))
	for _, s := range sessions {
		if seen[s] {
			t.Errorf("session used twice")
		} else {
			seen[s] = true
		}
		if !s.closed {
			t.Errorf("bad session was not closed")
		}
	}
}

func TestRetryerBadSessionReuse(t *testing.T) {
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				return nil
			},
		},
	}
	var (
		sessions = make([]*Session, 10)
		bad      = make(map[*Session]bool)
		reused   = make(map[*Session]bool)
	)
	for i := range sessions {
		s, err := client.CreateSession(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		sessions[i] = s
		bad[s] = i < len(sessions)-1 // All bad but last.
	}
	var i int
	r := Retryer{
		MaxRetries: len(sessions),
		SessionProvider: SessionProviderFunc{
			OnGet: func(_ context.Context) (*Session, error) {
				defer func() { i++ }()
				return sessions[i], nil
			},
			OnPut: func(_ context.Context, s *Session) error {
				reused[s] = true
				return nil
			},
		},
	}
	r.Do(
		context.Background(),
		OperationFunc(func(ctx context.Context, s *Session) error {
			if bad[s] {
				return &ydb.OpError{
					Reason: ydb.StatusBadSession,
				}
			}
			return nil
		}),
	)
	for _, s := range sessions {
		if bad[s] && reused[s] {
			t.Errorf("reused bad session")
		}
		if !bad[s] && !reused[s] {
			t.Errorf("missed good session")
		}
	}
}

func TestRetryerImmediateReturn(t *testing.T) {
	for _, testErr := range []error{
		&ydb.OpError{
			Reason: ydb.StatusGenericError,
		},
		&ydb.TransportError{
			Reason: ydb.TransportErrorPermissionDenied,
		},
		errors.New("whoa"),
	} {
		t.Run("", func(t *testing.T) {
			var count int32
			r := Retryer{
				MaxRetries:      1e6,
				RetryChecker:    ydb.DefaultRetryChecker,
				SessionProvider: SingleSession(simpleSession()),
			}
			err := r.Do(
				context.Background(),
				OperationFunc(func(ctx context.Context, _ *Session) error {
					if !atomic.CompareAndSwapInt32(&count, 0, 1) {
						t.Fatalf("unexpected repeat")
					}
					return testErr
				}),
			)
			if err != testErr {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
