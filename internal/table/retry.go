package table

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// SessionProvider is the interface that holds session lifecycle logic.
type SessionProvider interface {
	// Get returns alive idle session or creates new one.
	Get(context.Context) (*session, error)

	// Put takes no longer needed session for reuse or deletion depending
	// on implementation.
	// Put must be fast, if necessary must be async
	Put(context.Context, *session) (err error)
}

func doTx(
	ctx context.Context,
	c SessionProvider,
	config config.Config,
	op table.TxOperation,
	opts table.Options,
) (err error) {
	attempts, onIntermediate := 0, trace.TableOnDoTx(
		opts.Trace,
		&ctx,
		opts.Idempotent,
	)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	return retryBackoff(
		ctx,
		c,
		opts.FastBackoff,
		opts.SlowBackoff,
		opts.Idempotent,
		func(ctx context.Context, s table.Session) (err error) {
			defer func() {
				onIntermediate(err)
				attempts++
			}()

			tx, err := s.BeginTransaction(ctx, opts.TxSettings)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			defer func() {
				if err != nil {
					_ = tx.Rollback(ctx)
				}
			}()

			err = func() error {
				if panicCallback := config.PanicCallback(); panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							panicCallback(e)
						}
					}()
				}
				return op(ctx, tx)
			}()

			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			_, err = tx.CommitTx(ctx, opts.TxCommitOptions...)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
	)
}

func do(
	ctx context.Context,
	c SessionProvider,
	config config.Config,
	op table.Operation,
	opts table.Options,
) (err error) {
	attempts, onIntermediate := 0, trace.TableOnDo(
		opts.Trace,
		&ctx,
		opts.Idempotent,
	)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	return retryBackoff(
		ctx,
		c,
		opts.FastBackoff,
		opts.SlowBackoff,
		opts.Idempotent,
		func(ctx context.Context, s table.Session) (err error) {
			defer func() {
				onIntermediate(err)
				attempts++
			}()

			err = func() error {
				if panicCallback := config.PanicCallback(); panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							panicCallback(e)
						}
					}()
				}
				return op(ctx, s)
			}()

			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
	)
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

var (
	errNoSession         = xerrors.Wrap(fmt.Errorf("no session"))
	errUnexpectedSession = xerrors.Wrap(fmt.Errorf("unexpected session"))
	errSessionOverflow   = xerrors.Wrap(fmt.Errorf("session overflow"))
)

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

func retryBackoff(
	ctx context.Context,
	p SessionProvider,
	fastBackoff backoff.Backoff,
	slowBackoff backoff.Backoff,
	isOperationIdempotent bool,
	op table.Operation,
) error {
	return retry.Retry(
		ctx,
		func(ctx context.Context) error {
			s, err := p.Get(ctx)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}
			defer func() {
				_ = p.Put(ctx, s)
			}()

			err = op(ctx, s)
			if err != nil {
				s.checkError(err)
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
		retry.WithFastBackoff(fastBackoff),
		retry.WithSlowBackoff(slowBackoff),
		retry.WithIdempotent(isOperationIdempotent),
	)
}
