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
}

type retryOptions struct {
	options     ydb_table.Options
	fastBackoff ydb_retry.Backoff
	slowBackoff ydb_retry.Backoff
	trace       ydb_trace.Table
}

type retryOption func(o *retryOptions)

func withOptions(opts ...ydb_table.Option) retryOption {
	return func(options *retryOptions) {
		for _, o := range opts {
			o(&options.options)
		}
	}
}

func withTrace(t ydb_trace.Table) retryOption {
	return func(options *retryOptions) {
		options.trace = options.trace.Compose(t)
	}
}

func withFastBackoff(fastBackoff ydb_retry.Backoff) retryOption {
	return func(options *retryOptions) {
		options.fastBackoff = fastBackoff
	}
}

func withSlowBackoff(slowBackoff ydb_retry.Backoff) retryOption {
	return func(options *retryOptions) {
		options.slowBackoff = slowBackoff
	}
}

func do(ctx context.Context, c SessionProvider, op ydb_table.Operation, opts ...retryOption) (err error) {
	options := retryOptions{
		options: ydb_table.Options{
			Idempotent: ydb_table.ContextIdempotentOperation(ctx),
		},
		fastBackoff: ydb_retry.FastBackoff,
		slowBackoff: ydb_retry.SlowBackoff,
		trace:       ydb_trace.Table{},
	}
	for _, o := range opts {
		o(&options)
	}
	return retryBackoff(
		ctx,
		c,
		options.fastBackoff,
		options.slowBackoff,
		options.options.Idempotent,
		op,
		options.trace,
	)
}

type SessionProviderFunc struct {
	OnGet func(context.Context) (Session, error)
	OnPut func(context.Context, Session) error
}

var _ SessionProvider = SessionProviderFunc{}

func (f SessionProviderFunc) Get(ctx context.Context) (Session, error) {
	if f.OnGet == nil {
		return nil, errNoSession
	}
	return f.OnGet(ctx)
}

func (f SessionProviderFunc) Put(ctx context.Context, s Session) error {
	if f.OnPut == nil {
		return ydb_testutil.ErrNotImplemented
	}
	return f.OnPut(ctx, s)
}

func (f SessionProviderFunc) CloseSession(ctx context.Context, s Session) error {
	return s.Close(ctx)
}

// SingleSession returns SessionProvider that uses only given session during
// retries.
func SingleSession(s Session) SessionProvider {
	return &singleSession{s: s}
}

var (
	errNoSession         = errors.New("no session")
	errUnexpectedSession = errors.New("unexpected session")
	errSessionOverflow   = errors.New("session overflow")
)

type singleSession struct {
	s     Session
	empty bool
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
	fastBackoff ydb_retry.Backoff,
	slowBackoff ydb_retry.Backoff,
	isOperationIdempotent bool,
	op ydb_table.Operation,
	t ydb_trace.Table,
) (err error) {
	var (
		s              Session
		i              int
		attempts       int
		code           = int32(0)
		onIntermediate = ydb_trace.TableOnPoolRetry(t, &ctx, isOperationIdempotent)
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
					panic("both of session and error are nil")
				}
				if err != nil {
					return
				}
			}

			if err = op(ctx, s); err == nil {
				return
			}
			m := ydb_retry.Check(err)
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
			if err = ydb_retry.Wait(ctx, fastBackoff, slowBackoff, m, i); err != nil {
				return
			}
			code = m.StatusCode()
		}
	}
}
