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
	options     table.Options
	fastBackoff retry.Backoff
	slowBackoff retry.Backoff
	trace       trace.Table
}

type retryOption func(o *retryOptions)

func withOptions(opts ...table.Option) retryOption {
	return func(options *retryOptions) {
		for _, o := range opts {
			o(&options.options)
		}
	}
}

func withTrace(t trace.Table) retryOption {
	return func(options *retryOptions) {
		options.trace = options.trace.Compose(t)
	}
}

func withFastBackoff(fastBackoff retry.Backoff) retryOption {
	return func(options *retryOptions) {
		options.fastBackoff = fastBackoff
	}
}

func withSlowBackoff(slowBackoff retry.Backoff) retryOption {
	return func(options *retryOptions) {
		options.slowBackoff = slowBackoff
	}
}

func parseOptions(ctx context.Context, opts ...retryOption) retryOptions {
	options := retryOptions{
		options: table.Options{
			Idempotent: table.ContextIdempotentOperation(ctx),
		},
		fastBackoff: retry.FastBackoff,
		slowBackoff: retry.SlowBackoff,
		trace:       trace.Table{},
	}
	for _, o := range opts {
		o(&options)
	}
	return options
}

func doTx(ctx context.Context, c SessionProvider, op table.TxOperation, opts ...retryOption) (err error) {
	options := parseOptions(ctx, opts...)
	attempts, onIntermediate := 0, trace.TableOnPoolDoTx(options.trace, &ctx, options.options.Idempotent)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	err = retryBackoff(
		ctx,
		c,
		retry.FastBackoff,
		retry.SlowBackoff,
		options.options.Idempotent,
		func(ctx context.Context, s table.Session) (err error) {
			tx, err := s.BeginTransaction(ctx, options.options.TxSettings)
			if err != nil {
				err = errors.Errorf(0, "begin transaction failed: %w", err)
			}

			defer func() {
				if err != nil {
					_ = tx.Rollback(ctx)
				}
			}()

			err = op(ctx, tx)
			if err != nil {
				err = errors.Errorf(0, "retry operation failed: %w", err)
			}

			if attempts > 0 {
				onIntermediate(err)
			}

			attempts++

			if err != nil {
				return err
			}

			_, err = tx.CommitTx(ctx, options.options.TxCommitOptions...)
			if err != nil {
				err = errors.Errorf(0, "commit failed: %w", err)
			}

			return err
		},
		options.trace,
	)
	if err != nil {
		err = errors.Errorf(0, "doTx failed: %w", err)
	}
	return err
}

func do(ctx context.Context, c SessionProvider, op table.Operation, opts ...retryOption) (err error) {
	options := parseOptions(ctx, opts...)
	attempts, onIntermediate := 0, trace.TableOnPoolDo(options.trace, &ctx, options.options.Idempotent)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	err = retryBackoff(
		ctx,
		c,
		options.fastBackoff,
		options.slowBackoff,
		options.options.Idempotent,
		func(ctx context.Context, s table.Session) error {
			err = op(ctx, s)
			if err != nil {
				err = errors.Errorf(0, "retry operation failed: %w", err)
			}

			if attempts > 0 {
				onIntermediate(err)
			}

			attempts++

			return err
		},
		options.trace,
	)
	if err != nil {
		err = errors.Errorf(0, "do failed: %w", err)
	}
	return err
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
		return testutil.ErrNotImplemented
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
		onIntermediate = trace.TableOnPoolDo(t, &ctx, isOperationIdempotent)
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
			return errors.Errorf(0, "context done: %w", ctx.Err())

		default:
			if s == nil {
				s, err = p.Get(ctx)
				if s == nil && err == nil {
					panic("both of session and error are nil")
				}
				if err != nil {
					return errors.Errorf(0, "get session from pool failed: %w", err)
				}
			}

			err = op(ctx, s)
			if err != nil {
				err = errors.Errorf(0, "retry operation failed: %w", err)
			}

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

			if m.MustDeleteSession() {
				_ = p.CloseSession(ctx, s)
				s = nil
			}

			if !m.MustRetry(isOperationIdempotent) {
				return
			}

			if retry.Wait(ctx, fastBackoff, slowBackoff, m, i) != nil {
				return errors.Errorf(0, "wait failed, last operation error: %w", err)
			}

			code = m.StatusCode()
		}
	}
}
