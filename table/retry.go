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
}

type SessionProviderFunc struct {
	OnGet func(context.Context) (*Session, error)
	OnPut func(context.Context, *Session) error
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
		s *Session
		m ydb.RetryMode
	)
	defer func() {
		if s != nil {
			r.SessionProvider.Put(context.Background(), s)
		}
	}()
	for i := 0; i <= r.MaxRetries; i++ {
		if s == nil {
			var e error
			s, e = r.SessionProvider.Get(ctx)
			if e != nil {
				if err == nil {
					// It is initial attempt to get a Session.
					// Otherwise s could be nil only when status bad session
					// received – that is, we must return bad session error to
					// make it possible to lay on for the client.
					err = e
				}
				return
			}
		}
		if err = op.Do(ctx, s); err == nil {
			return nil
		}
		if m = r.RetryChecker.Check(err); !m.Retriable() {
			return err
		}
		if m.MustDeleteSession() {
			defer s.Close(ctx)
			s = nil
		}
		if m.MustBackoff() {
			if e := ydb.WaitBackoff(ctx, r.Backoff, i); e != nil {
				// Return original error to make it possible to lay on for the
				// client.
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
