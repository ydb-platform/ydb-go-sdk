package query

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//go:generate mockgen -destination grpc_client_mock_test.go --typed -package query -write_package_comment=false github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1 QueryServiceClient,QueryService_AttachSessionClient,QueryService_ExecuteQueryClient

var (
	_ query.Client = (*Client)(nil)
	_ sessionPool  = (*poolStub)(nil)
	_ sessionPool  = (*pool.Pool[*Session, Session])(nil)
)

type (
	sessionPool interface {
		closer.Closer

		Stats() pool.Stats
		With(ctx context.Context, f func(ctx context.Context, s *Session) error, opts ...retry.Option) error
	}
	poolStub struct {
		createSession func(ctx context.Context) (*Session, error)
		InUse         atomic.Int32
	}
	Client struct {
		config *config.Config
		client Ydb_Query_V1.QueryServiceClient
		pool   sessionPool

		done chan struct{}
	}
)

func (p *poolStub) Close(ctx context.Context) error {
	return nil
}

func (p *poolStub) Stats() pool.Stats {
	return pool.Stats{
		Limit: -1,
		Index: 0,
		Idle:  0,
		InUse: int(p.InUse.Load()),
	}
}

func (p *poolStub) With(
	ctx context.Context, f func(ctx context.Context, s *Session) error, opts ...retry.Option,
) error {
	p.InUse.Add(1)
	defer func() {
		p.InUse.Add(-1)
	}()

	err := retry.Retry(ctx, func(ctx context.Context) (err error) {
		s, err := p.createSession(ctx)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = s.Close(ctx)
		}()

		err = f(ctx, s)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) Close(ctx context.Context) error {
	close(c.done)

	if err := c.pool.Close(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func do(
	ctx context.Context,
	pool sessionPool,
	op query.Operation,
	opts ...retry.Option,
) (finalErr error) {
	err := pool.With(ctx, func(ctx context.Context, s *Session) error {
		s.setStatus(statusInUse)

		err := op(ctx, s)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		s.setStatus(statusIdle)

		return nil
	}, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) Do(ctx context.Context, op query.Operation, opts ...options.DoOption) (finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	var (
		onDone = trace.QueryOnDo(c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).Do"),
		)
		attempts = 0
	)
	defer func() {
		onDone(attempts, finalErr)
	}()

	err := do(ctx, c.pool, func(ctx context.Context, s query.Session) error {
		attempts++

		return op(ctx, s)
	}, options.ParseDoOpts(c.config.Trace(), opts...).RetryOpts()...)

	return err
}

func doTx(
	ctx context.Context,
	pool sessionPool,
	op query.TxOperation,
	t *trace.Query,
	opts ...options.DoTxOption,
) (finalErr error) {
	doTxOpts := options.ParseDoTxOpts(t, opts...)

	err := do(ctx, pool, func(ctx context.Context, s query.Session) (err error) {
		tx, err := s.Begin(ctx, doTxOpts.TxSettings())
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		err = op(ctx, tx)
		if err != nil {
			errRollback := tx.Rollback(ctx)
			if errRollback != nil {
				return xerrors.WithStackTrace(xerrors.Join(err, errRollback))
			}

			return xerrors.WithStackTrace(err)
		}
		err = tx.CommitTx(ctx)
		if err != nil {
			errRollback := tx.Rollback(ctx)
			if errRollback != nil {
				return xerrors.WithStackTrace(xerrors.Join(err, errRollback))
			}

			return xerrors.WithStackTrace(err)
		}

		return nil
	}, doTxOpts.RetryOpts()...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

// QueryRow is a helper which read only one row from first result set in result
func (c *Client) QueryRow(ctx context.Context, q string, opts ...options.QueryOption) (row query.Row, finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	onDone := trace.QueryOnQueryRow(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).QueryRow"),
		q,
	)
	defer func() {
		onDone(finalErr)
	}()

	err := do(ctx, c.pool, func(ctx context.Context, s query.Session) error {
		_, r, err := s.Query(ctx, q, opts...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = r.Close(ctx)
		}()

		rs, err := r.NextResultSet(ctx)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		row, err = rs.NextRow(ctx)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		_, err = rs.NextRow(ctx)
		if err == nil {
			return xerrors.WithStackTrace(errMoreThanOneRow)
		}
		if !xerrors.Is(err, io.EOF) {
			return xerrors.WithStackTrace(err)
		}

		_, err = r.NextResultSet(ctx)
		if err == nil {
			return xerrors.WithStackTrace(errMoreThanOneResultSet)
		}
		if !xerrors.Is(err, io.EOF) {
			return xerrors.WithStackTrace(err)
		}

		return nil
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return row, nil
}

func clientExec(ctx context.Context, pool sessionPool, q string, opts ...options.ExecOption) (finalErr error) {
	err := do(ctx, pool, func(ctx context.Context, s query.Session) (err error) {
		_, streamResult, err := s.Query(ctx, q, opts...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = streamResult.Close(ctx)
		}()

		for {
			rs, err := streamResult.NextResultSet(ctx)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					return nil
				}

				return xerrors.WithStackTrace(err)
			}
			for {
				_, err = rs.NextRow(ctx)
				if err != nil {
					if xerrors.Is(err, io.EOF) {
						break
					}

					return xerrors.WithStackTrace(err)
				}
			}
		}
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) Exec(ctx context.Context, q string, opts ...options.ExecOption) (finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	onDone := trace.QueryOnExec(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).Exec"),
		q,
	)
	defer func() {
		onDone(finalErr)
	}()

	err := clientExec(ctx, c.pool, q, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func clientQuery(ctx context.Context, pool sessionPool, q string, opts ...options.QueryOption) (
	r query.Result, err error,
) {
	err = do(ctx, pool, func(ctx context.Context, s query.Session) (err error) {
		_, streamResult, err := s.Query(ctx, q, opts...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = streamResult.Close(ctx)
		}()

		r, err = resultToMaterializedResult(ctx, streamResult)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (c *Client) Query(ctx context.Context, q string, opts ...options.QueryOption) (r query.Result, err error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	onDone := trace.QueryOnQuery(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).Query"),
		q,
	)
	defer func() {
		onDone(err)
	}()

	r, err = clientQuery(ctx, c.pool, q, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

// QueryResultSet is a helper which read all rows from first result set in result
func (c *Client) QueryResultSet(
	ctx context.Context, q string, opts ...options.ExecOption,
) (rs query.ResultSet, finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	onDone := trace.QueryOnQueryResultSet(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).QueryResultSet"),
		q,
	)
	defer func() {
		onDone(finalErr)
	}()

	err := do(ctx, c.pool, func(ctx context.Context, s query.Session) error {
		_, r, err := s.Query(ctx, q, opts...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = r.Close(ctx)
		}()

		rs, err = r.NextResultSet(ctx)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		var rows []query.Row
		for {
			row, err := rs.NextRow(ctx) //nolint:govet
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					rs = NewMaterializedResultSet(rs.Index(), rs.Columns(), rs.ColumnTypes(), rows)

					break
				}

				return xerrors.WithStackTrace(err)
			}

			rows = append(rows, row)
		}

		_, err = r.NextResultSet(ctx)
		if err == nil {
			return xerrors.WithStackTrace(errMoreThanOneResultSet)
		}
		if !xerrors.Is(err, io.EOF) {
			return xerrors.WithStackTrace(err)
		}

		return nil
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return rs, nil
}

func (c *Client) DoTx(ctx context.Context, op query.TxOperation, opts ...options.DoTxOption) (finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	var (
		onDone = trace.QueryOnDoTx(c.config.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).DoTx"),
		)
		attempts = 0
	)
	defer func() {
		onDone(attempts, finalErr)
	}()

	err := doTx(ctx, c.pool, func(ctx context.Context, tx query.TxActor) error {
		attempts++

		return op(ctx, tx)
	}, c.config.Trace(), opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func newPool(
	ctx context.Context, cfg *config.Config, createSession func(ctx context.Context) (*Session, error),
) sessionPool {
	if cfg.UseSessionPool() {
		return pool.New(ctx,
			pool.WithLimit[*Session, Session](cfg.PoolLimit()),
			pool.WithTrace[*Session, Session](poolTrace(cfg.Trace())),
			pool.WithCreateItemTimeout[*Session, Session](cfg.SessionCreateTimeout()),
			pool.WithCloseItemTimeout[*Session, Session](cfg.SessionDeleteTimeout()),
			pool.WithCreateFunc(createSession),
		)
	}

	return &poolStub{
		createSession: createSession,
	}
}

func New(ctx context.Context, balancer grpc.ClientConnInterface, cfg *config.Config) *Client {
	onDone := trace.QueryOnNew(cfg.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.New"),
	)
	defer onDone()

	grpcClient := Ydb_Query_V1.NewQueryServiceClient(balancer)

	client := &Client{
		config: cfg,
		client: grpcClient,
		done:   make(chan struct{}),
		pool: newPool(ctx, cfg, func(ctx context.Context) (_ *Session, err error) {
			var (
				createCtx    context.Context
				cancelCreate context.CancelFunc
			)
			if d := cfg.SessionCreateTimeout(); d > 0 {
				createCtx, cancelCreate = xcontext.WithTimeout(ctx, d)
			} else {
				createCtx, cancelCreate = xcontext.WithCancel(ctx)
			}
			defer cancelCreate()

			s, err := createSession(createCtx, grpcClient, cfg)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return s, nil
		}),
	}

	return client
}

func poolTrace(t *trace.Query) *pool.Trace {
	return &pool.Trace{
		OnNew: func(info *pool.NewStartInfo) func(*pool.NewDoneInfo) {
			onDone := trace.QueryOnPoolNew(t, info.Context, info.Call)

			return func(info *pool.NewDoneInfo) {
				onDone(info.Limit)
			}
		},
		OnClose: func(info *pool.CloseStartInfo) func(*pool.CloseDoneInfo) {
			onDone := trace.QueryOnClose(t, info.Context, info.Call)

			return func(info *pool.CloseDoneInfo) {
				onDone(info.Error)
			}
		},
		OnTry: func(info *pool.TryStartInfo) func(*pool.TryDoneInfo) {
			onDone := trace.QueryOnPoolTry(t, info.Context, info.Call)

			return func(info *pool.TryDoneInfo) {
				onDone(info.Error)
			}
		},
		OnWith: func(info *pool.WithStartInfo) func(*pool.WithDoneInfo) {
			onDone := trace.QueryOnPoolWith(t, info.Context, info.Call)

			return func(info *pool.WithDoneInfo) {
				onDone(info.Error, info.Attempts)
			}
		},
		OnPut: func(info *pool.PutStartInfo) func(*pool.PutDoneInfo) {
			onDone := trace.QueryOnPoolPut(t, info.Context, info.Call)

			return func(info *pool.PutDoneInfo) {
				onDone(info.Error)
			}
		},
		OnGet: func(info *pool.GetStartInfo) func(*pool.GetDoneInfo) {
			onDone := trace.QueryOnPoolGet(t, info.Context, info.Call)

			return func(info *pool.GetDoneInfo) {
				onDone(info.Error)
			}
		},
		OnChange: func(info pool.ChangeInfo) {
			trace.QueryOnPoolChange(t, info.Limit, info.Index, info.Idle, info.InUse)
		},
	}
}
