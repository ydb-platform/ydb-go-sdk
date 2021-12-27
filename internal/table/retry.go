package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// SessionProvider is the interface that holds session lifecycle logic.
type SessionProvider interface {
	// Get returns alive idle session or creates new one.
	Get(context.Context) (Session, error)

	// Put takes no longer needed session for reuse or deletion depending
	// on implementation.
	// Put must be fast, if necessary must be async
	Put(context.Context, Session) (err error)

	// CloseSession provides the most effective way of session closing
	// instead of plain session.Close().
	// CloseSession must be fast. If necessary, can be async.
	CloseSession(ctx context.Context, s Session) error

	// Close provide cleanup sessions
	Close(ctx context.Context) error

	// Do provide the best effort for execute operation
	// Do implements internal busy loop until one of the following conditions is met:
	// - deadline was canceled or deadlined
	// - retry operation returned nil as error
	// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
	Do(ctx context.Context, op table.Operation, opts ...table.Option) (err error)
}

type SessionProviderFunc struct {
	OnGet   func(context.Context) (Session, error)
	OnPut   func(context.Context, Session) error
	OnDo    func(context.Context, table.Operation, ...table.Option) error
	OnClose func(context.Context) error
}

var _ SessionProvider = SessionProviderFunc{}

func (f SessionProviderFunc) Close(ctx context.Context) error {
	if f.OnClose == nil {
		return nil
	}
	return f.OnClose(ctx)
}

func (f SessionProviderFunc) Get(ctx context.Context) (Session, error) {
	if f.OnGet == nil {
		return nil, errNoSession
	}
	return f.OnGet(ctx)
}

func (f SessionProviderFunc) Put(ctx context.Context, s Session) error {
	if f.OnPut == nil {
		return testutil.ErrNotImplemented
	}
	return f.OnPut(ctx, s)
}

func (f SessionProviderFunc) Do(ctx context.Context, op table.Operation, opts ...table.Option) (err error) {
	if f.OnDo == nil {
		return retryBackoff(ctx, f, nil, nil, false, op, trace.ContextTable(ctx))
	}
	return f.OnDo(ctx, op)
}

func (f SessionProviderFunc) CloseSession(ctx context.Context, s Session) error {
	return s.Close(ctx)
}

// SingleSession returns SessionProvider that uses only given session during
// retries.
func SingleSession(s Session, b retry.Backoff) SessionProvider {
	return &singleSession{s: s, b: b}
}

var (
	errNoSession         = errors.New("no session")
	errUnexpectedSession = errors.New("unexpected session")
	errSessionOverflow   = errors.New("session overflow")
)

type singleSession struct {
	s     Session
	b     retry.Backoff
	empty bool
}

func (s *singleSession) Do(ctx context.Context, op table.Operation, opts ...table.Option) (err error) {
	options := table.Options{}
	for _, o := range opts {
		o(&options)
	}
	return retryBackoff(ctx, s, s.b, s.b, options.Idempotent, op, trace.ContextTable(ctx))
}

func (s *singleSession) Close(ctx context.Context) error {
	return s.CloseSession(ctx, s.s)
}

func (s *singleSession) Get(context.Context) (Session, error) {
	if s.empty {
		return nil, errNoSession
	}
	s.empty = true
	return s.s, nil
}

func (s *singleSession) Put(_ context.Context, x Session) error {
	if x != s.s {
		return errUnexpectedSession
	}
	if !s.empty {
		return errSessionOverflow
	}
	s.empty = false
	return nil
}

func (s *singleSession) CloseSession(ctx context.Context, x Session) error {
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
	isOperationIdempotent bool,
	op table.Operation,
	t trace.Table,
) (err error) {
	var (
		s              Session
		i              int
		attempts       int
		code           = int32(0)
		onIntermediate = trace.TableOnPoolRetry(t, &ctx, isOperationIdempotent)
	)
	defer func() {
		if s != nil {
			_ = p.Put(ctx, s)
		}
		onIntermediate(err)(attempts, err)
	}()
	for ; ; i++ {
		attempts++
		if i > 0 {
			onIntermediate(err)
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		default:
			if s == nil {
				s, err = p.Get(ctx)
				if s == nil && err == nil {
					panic("only one of pair <session, error> must be not nil")
				}
				if err != nil {
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
			if !m.MustRetry(isOperationIdempotent) {
				return
			}
			if err = retry.Wait(ctx, fastBackoff, slowBackoff, m, i); err != nil {
				return
			}
			code = m.StatusCode()
		}
	}
}
