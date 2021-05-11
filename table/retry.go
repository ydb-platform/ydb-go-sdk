package table

import (
	"context"
	"errors"

	"github.com/yandex-cloud/ydb-go-sdk"
)

// SessionProvider is the interface that holds session lifecycle logic.
type SessionProvider interface {
	// Get returns alive idle session or creates new one.
	Get(context.Context) (*Session, error)

	// Put takes no longer needed session for reuse or deletion depending on
	// implementation.
	Put(context.Context, *Session) (err error)

	// PutBusy takes session with not yet completed operation inside.
	// It gives full ownership of s to session provider.
	PutBusy(context.Context, *Session) (err error)
}

type SessionProviderFunc struct {
	OnGet     func(context.Context) (*Session, error)
	OnPut     func(context.Context, *Session) error
	OnPutBusy func(context.Context, *Session) error
}

func (f SessionProviderFunc) Get(ctx context.Context) (*Session, error) {
	if f.OnGet == nil {
		return nil, errNoSession
	}
	return f.OnGet(ctx)
}

func (f SessionProviderFunc) Put(ctx context.Context, s *Session) error {
	if f.OnPut == nil {
		return errSessionOverflow
	}
	return f.OnPut(ctx, s)
}

func (f SessionProviderFunc) PutBusy(ctx context.Context, s *Session) error {
	if f.OnPutBusy == nil {
		return s.Close(ctx)
	}
	return f.OnPutBusy(ctx, s)
}

// SingleSession returns SessionProvider that uses only given session durting
// retries.
func SingleSession(s *Session) SessionProvider {
	return &singleSession{s: s}
}

// Operation is the interface that holds an operation for retry.
type Operation interface {
	// Do prepares actions need to be done for this operation.
	//
	// Implementations MUST NOT wrap ydb/table errors in order to leave the
	// ability to distinguish error type and make a decision about the next
	// retry attempt.
	Do(context.Context, *Session) error
}

// OperationFunc is an adapter to allow the use of ordinary functions as
// Operation.
type OperationFunc func(context.Context, *Session) error

// Do implements Operation interface.
func (f OperationFunc) Do(ctx context.Context, s *Session) error {
	return f(ctx, s)
}

// Retryer contains logic of retrying operations failed with retriable errors.
type Retryer struct {
	// SessionProvider is an interface capable for management of ydb sessions.
	// SessionProvider must not be nil.
	SessionProvider SessionProvider

	// MaxRetries is a number of maximum attempts to retry a failed operation.
	// If MaxRetries is zero then no attempts will be made.
	MaxRetries int

	// RetryChecker contains options of mapping errors to retry mode.
	//
	// Note that if RetryChecker's RetryNotFound field is set to true, creation
	// of prepared statements must always be included in the Operation logic.
	// Otherwise when prepared statement become removed by any reason from the
	// server, Retryer will just repeat MaxRetries times reception of statement
	// not found error.
	RetryChecker ydb.RetryChecker

	// Backoff is a selected backoff policy.
	// If backoff is nil, then the DefaultBackoff is used.
	Backoff ydb.Backoff
}

// Retry calls Retryer.Do() configured with default values.
func Retry(ctx context.Context, s SessionProvider, op Operation) error {
	return (Retryer{
		SessionProvider: s,
		MaxRetries:      ydb.DefaultMaxRetries,
		RetryChecker:    ydb.DefaultRetryChecker,
		Backoff:         ydb.DefaultBackoff,
	}).Do(ctx, op)
}

// Do calls op.Do until it return nil or not retriable error.
func (r Retryer) Do(ctx context.Context, op Operation) (err error) {
	var (
		e error
		s *Session
		m ydb.RetryMode
	)
	defer func() {
		if s != nil {
			_ = r.SessionProvider.Put(context.Background(), s)
		}
	}()
	for i := 0; i <= r.MaxRetries; i++ {
		s, e = func() (*Session, error) {
			s, e := r.SessionProvider.Get(ctx)
			if e != nil {
				return s, e
			}
			return s, op.Do(ctx, s)
		}()
		if e == nil {
			return nil
		}
		if err == nil {
			err = e
		}
		m = r.RetryChecker.Check(e)
		switch {
		case m.MustDeleteSession():
			if s != nil {
				_ = s.Close(ctx)
				s = nil
			}
		case m.MustCheckSession():
			if s != nil {
				_ = r.SessionProvider.PutBusy(ctx, s)
				s = nil
			}
		}
		if !m.Retriable() {
			// Return original error to make it possible to lay on for the client.
			return err
		}
		if m.MustBackoff() {
			if e := ydb.WaitBackoff(ctx, r.Backoff, i); e != nil {
				// Return original error to make it possible to lay on for the client.
				return err
			}
		}
	}
	return err
}

var (
	errNoSession         = errors.New("no session")
	errUnexpectedSession = errors.New("unexpected session")
	errSessionOverflow   = errors.New("session overflow")
)

type singleSession struct {
	s     *Session
	empty bool
}

func (s *singleSession) Get(context.Context) (*Session, error) {
	if s.empty {
		return nil, errNoSession
	}
	s.empty = true
	return s.s, nil
}

func (s *singleSession) Put(_ context.Context, x *Session) error {
	if x != s.s {
		return errUnexpectedSession
	}
	if !s.empty {
		return errSessionOverflow
	}
	s.empty = false
	return nil
}

func (s *singleSession) PutBusy(ctx context.Context, x *Session) error {
	if x != s.s {
		return errUnexpectedSession
	}
	if !s.empty {
		return errSessionOverflow
	}
	return x.Close(ctx)
}
