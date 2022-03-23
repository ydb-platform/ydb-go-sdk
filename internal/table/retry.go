package table

import (
	"context"
	"fmt"

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

func doTx(ctx context.Context, c SessionProvider, op table.TxOperation, opts table.Options) (err error) {
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
				return errors.WithStackTrace(err)
			}

			defer func() {
				if err != nil {
					_ = tx.Rollback(ctx)
				}
			}()

			err = op(ctx, tx)

			if err != nil {
				return err
			}

			_, err = tx.CommitTx(ctx, opts.TxCommitOptions...)
			if err != nil {
				return errors.WithStackTrace(err)
			}

			return nil
		},
	)
}

func do(ctx context.Context, c SessionProvider, op table.Operation, opts table.Options) (err error) {
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

			return op(ctx, s)
		},
	)
}

type SessionProviderFunc struct {
	OnGet func(context.Context) (Session, error)
	OnPut func(context.Context, Session) error
}

var _ SessionProvider = SessionProviderFunc{}

func (f SessionProviderFunc) Get(ctx context.Context) (Session, error) {
	if f.OnGet == nil {
		return nil, errors.WithStackTrace(errNoSession)
	}
	return f.OnGet(ctx)
}

func (f SessionProviderFunc) Put(ctx context.Context, s Session) error {
	if f.OnPut == nil {
		return errors.WithStackTrace(testutil.ErrNotImplemented)
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
	errNoSession         = fmt.Errorf("no session")
	errUnexpectedSession = fmt.Errorf("unexpected session")
	errSessionOverflow   = fmt.Errorf("session overflow")
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
		return nil, errors.WithStackTrace(errNoSession)
	}
	s.empty = true
	return s.s, nil
}

func (s *singleSession) Put(_ context.Context, x Session) error {
	if x != s.s {
		return errors.WithStackTrace(errUnexpectedSession)
	}
	if !s.empty {
		return errors.WithStackTrace(errSessionOverflow)
	}
	s.empty = false
	return nil
}

func (s *singleSession) CloseSession(ctx context.Context, x Session) error {
	if x != s.s {
		return errors.WithStackTrace(errUnexpectedSession)
	}
	if !s.empty {
		return errors.WithStackTrace(errSessionOverflow)
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
) (err error) {
	var (
		s        Session
		i        int
		attempts int
		code     = int64(0)
	)
	defer func() {
		if s != nil {
			_ = p.Put(ctx, s)
		}
	}()
	for ; ; i++ {
		attempts++
		select {
		case <-ctx.Done():
			return errors.WithStackTrace(ctx.Err())

		default:
			if s == nil {
				s, err = p.Get(ctx)
				if s == nil && err == nil {
					panic("both of session and error are nil")
				}
				if err != nil {
					return errors.WithStackTrace(err)
				}
			}

			err = op(ctx, s)

			if s.isClosing() {
				_ = p.CloseSession(ctx, s)
				s = nil
			}

			if err == nil {
				return
			}

			m := retry.Check(err)

			if m.StatusCode() != code {
				i = 0
			}

			if m.MustDeleteSession() && s != nil {
				_ = p.CloseSession(ctx, s)
				s = nil
			}

			if !m.MustRetry(isOperationIdempotent) {
				return
			}

			if retry.Wait(ctx, fastBackoff, slowBackoff, m, i) != nil {
				return
			}

			code = m.StatusCode()
		}
	}
}
