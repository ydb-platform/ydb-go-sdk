package table

import (
	"context"
	"errors"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// SessionProvider is the interface that holds session lifecycle logic.
type SessionProvider interface {
	// Get returns alive idle session or creates new one.
	Get(context.Context) (*Session, error)

	// Put takes no longer needed session for reuse or deletion depending
	// on implementation.
	// Put must be fast, if necessary must be async
	Put(context.Context, *Session) (err error)

	// CloseSession provides the most effective way of session closing
	// instead of plain session.Close.
	// CloseSession must be fast. If necessary, can be async.
	CloseSession(ctx context.Context, s *Session) error

	// Retry provide the best effort fo retrying operation
	// Retry implements internal busy loop until one of the following conditions is met:
	// - context was cancelled or deadlined
	// - retry operation returned nil as error
	// If context without deadline used session pool RetryTimeout
	Retry(ctx context.Context, retryNoIdempotent bool, op RetryOperation) (err error)

	// Close provide cleanup sessions
	Close(ctx context.Context) error
}

type SessionProviderFunc struct {
	OnGet   func(context.Context) (*Session, error)
	OnPut   func(context.Context, *Session) error
	OnRetry func(context.Context, RetryOperation) error
	OnClose func(context.Context) error
}

func (f SessionProviderFunc) Close(ctx context.Context) error {
	if f.OnClose == nil {
		return nil
	}
	return f.OnClose(ctx)
}

func (f SessionProviderFunc) Get(ctx context.Context) (*Session, error) {
	if f.OnGet == nil {
		return nil, errNoSession
	}
	return f.OnGet(ctx)
}

func (f SessionProviderFunc) Put(ctx context.Context, s *Session) error {
	if f.OnPut == nil {
		return testutil.ErrNotImplemented
	}
	return f.OnPut(ctx, s)
}

func (f SessionProviderFunc) Retry(ctx context.Context, _ bool, op RetryOperation) error {
	if f.OnRetry == nil {
		return retryBackoff(ctx, f, nil, nil, false, op)
	}
	return f.OnRetry(ctx, op)
}

func (f SessionProviderFunc) CloseSession(ctx context.Context, s *Session) error {
	return s.Close(ctx)
}

// SingleSession returns SessionProvider that uses only given session during
// retries.
func SingleSession(s *Session, b retry.Backoff) SessionProvider {
	return &singleSession{s: s, b: b}
}

var (
	errNoSession         = errors.New("no session")
	errUnexpectedSession = errors.New("unexpected session")
	errSessionOverflow   = errors.New("session overflow")
)

type singleSession struct {
	s     *Session
	b     retry.Backoff
	empty bool
}

func (s *singleSession) Close(ctx context.Context) error {
	return s.CloseSession(ctx, s.s)
}

func (s *singleSession) Retry(ctx context.Context, _ bool, op RetryOperation) (err error) {
	return retryBackoff(ctx, s, s.b, s.b, false, op)
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

func (s *singleSession) CloseSession(ctx context.Context, x *Session) error {
	if x != s.s {
		return errUnexpectedSession
	}
	if !s.empty {
		return errSessionOverflow
	}
	s.empty = true
	return x.Close(ctx)
}

func retryBackoff(
	ctx context.Context,
	p SessionProvider,
	fastBackoff retry.Backoff,
	slowBackoff retry.Backoff,
	retryNoIdempotent bool,
	op RetryOperation,
) (err error) {
	var (
		s        *Session
		i        int
		attempts int

		code   = int32(0)
		start  = time.Now()
		onDone = trace.OnRetry(ctx)
	)
	defer func() {
		onDone(ctx, time.Since(start), attempts)
		if s != nil {
			_ = p.Put(ctx, s)
		}
	}()
	for {
		i++
		attempts++
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if s == nil {
				var e error
				s, e = p.Get(ctx)
				if s == nil && e == nil {
					panic("only one of pair <session, error> must be not nil")
				}
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

			if err = op(ctx, s); err == nil {
				return
			}
			m := retry.Check(err)
			if m.StatusCode() != code {
				i = 0
			}
			if m.MustDeleteSession() {
				_ = p.CloseSession(ctx, s)
				s = nil
			}
			if !m.MustRetry(retryNoIdempotent) {
				return err
			}
			if e := retry.Wait(ctx, fastBackoff, slowBackoff, m, i); e != nil {
				return err
			}
			code = m.StatusCode()
		}
	}
	return err
}
