package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"google.golang.org/grpc"

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

//go:generate mockgen -destination grpc_client_mock_test.go -package query -write_package_comment=false github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1 QueryServiceClient,QueryService_AttachSessionClient,QueryService_ExecuteQueryClient

type nodeChecker interface {
	HasNode(id uint32) bool
}

type balancer interface {
	grpc.ClientConnInterface
	nodeChecker
}

var _ query.Client = (*Client)(nil)

type Client struct {
	config     *config.Config
	grpcClient Ydb_Query_V1.QueryServiceClient
	pool       *pool.Pool[*Session, Session]
}

func (c *Client) Close(ctx context.Context) error {
	err := c.pool.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func do(
	ctx context.Context,
	pool *pool.Pool[*Session, Session],
	op query.Operation,
	t *trace.Query,
	opts ...options.DoOption,
) (attempts int, finalErr error) {
	doOpts := options.ParseDoOpts(t, opts...)

	err := pool.With(ctx, func(ctx context.Context, s *Session) error {
		s.setStatus(statusInUse)

		err := op(ctx, s)
		if err != nil {
			if xerrors.MustDeleteSession(err) {
				s.setStatus(statusError)
			}

			return xerrors.WithStackTrace(err)
		}

		s.setStatus(statusIdle)

		return nil
	}, append(doOpts.RetryOpts(), retry.WithTrace(&trace.Retry{
		OnRetry: func(
			info trace.RetryLoopStartInfo,
		) func(
			trace.RetryLoopIntermediateInfo,
		) func(
			trace.RetryLoopDoneInfo,
		) {
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				return func(info trace.RetryLoopDoneInfo) {
					attempts = info.Attempts
				}
			}
		},
	}))...)
	if err != nil {
		return attempts, xerrors.WithStackTrace(err)
	}

	return attempts, nil
}

func (c *Client) Do(ctx context.Context, op query.Operation, opts ...options.DoOption) error {
	onDone := trace.QueryOnDo(c.config.Trace(), &ctx, stack.FunctionID(""))
	attempts, err := do(ctx, c.pool, op, c.config.Trace(), opts...)
	onDone(attempts, err)

	return err
}

func doTx(
	ctx context.Context,
	pool *pool.Pool[*Session, Session],
	op query.TxOperation,
	t *trace.Query,
	opts ...options.DoTxOption,
) (attempts int, err error) {
	doTxOpts := options.ParseDoTxOpts(t, opts...)

	attempts, err = do(ctx, pool, func(ctx context.Context, s query.Session) (err error) {
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
	}, t, doTxOpts.DoOpts()...)
	if err != nil {
		return attempts, xerrors.WithStackTrace(err)
	}

	return attempts, nil
}

func (c *Client) DoTx(ctx context.Context, op query.TxOperation, opts ...options.DoTxOption) error {
	onDone := trace.QueryOnDoTx(c.config.Trace(), &ctx, stack.FunctionID(""))
	attempts, err := doTx(ctx, c.pool, op, c.config.Trace(), opts...)
	onDone(attempts, err)

	return err
}

func New(ctx context.Context, balancer balancer, cfg *config.Config) (_ *Client, err error) {
	onDone := trace.QueryOnNew(cfg.Trace(), &ctx, stack.FunctionID(""))
	defer func() {
		onDone(err)
	}()

	client := &Client{
		config:     cfg,
		grpcClient: Ydb_Query_V1.NewQueryServiceClient(balancer),
	}

	client.pool, err = pool.New(ctx,
		pool.WithMaxSize[*Session, Session](cfg.PoolMaxSize()),
		pool.WithProducersCount[*Session, Session](cfg.PoolProducersCount()),
		pool.WithTrace[*Session, Session](poolTrace(cfg.Trace())),
		pool.WithCreateFunc(func(ctx context.Context) (_ *Session, err error) {
			var cancel context.CancelFunc
			if d := cfg.SessionCreateTimeout(); d > 0 {
				ctx, cancel = xcontext.WithTimeout(ctx, d)
			} else {
				ctx, cancel = xcontext.WithCancel(ctx)
			}
			defer cancel()

			s, err := createSession(ctx,
				client.grpcClient,
				withSessionTrace(cfg.Trace()),
				withSessionCheck(func(s *Session) bool {
					return balancer.HasNode(uint32(s.nodeID))
				}),
			)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return s, nil
		}),
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return client, xerrors.WithStackTrace(ctx.Err())
}

func poolTrace(t *trace.Query) *pool.Trace {
	return &pool.Trace{
		OnNew: func(info *pool.NewStartInfo) func(*pool.NewDoneInfo) {
			onDone := trace.QueryOnPoolNew(t, info.Context, info.Call, info.MinSize, info.MaxSize, info.ProducersCount)

			return func(info *pool.NewDoneInfo) {
				onDone(info.Error, info.MinSize, info.MaxSize, info.ProducersCount)
			}
		},
		OnClose: func(info *pool.CloseStartInfo) func(*pool.CloseDoneInfo) {
			onDone := trace.QueryOnClose(t, info.Context, info.Call)

			return func(info *pool.CloseDoneInfo) {
				onDone(info.Error)
			}
		},
		OnProduce: func(info *pool.ProduceStartInfo) func(*pool.ProduceDoneInfo) {
			onDone := trace.QueryOnPoolProduce(t, info.Context, info.Call, info.Concurrency)

			return func(info *pool.ProduceDoneInfo) {
				onDone()
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
		OnSpawn: func(info *pool.SpawnStartInfo) func(*pool.SpawnDoneInfo) {
			onDone := trace.QueryOnPoolSpawn(t, info.Context, info.Call)

			return func(info *pool.SpawnDoneInfo) {
				onDone(info.Error)
			}
		},
		OnWant: func(info *pool.WantStartInfo) func(*pool.WantDoneInfo) {
			onDone := trace.QueryOnPoolWant(t, info.Context, info.Call)

			return func(info *pool.WantDoneInfo) {
				onDone(info.Error)
			}
		},
	}
}
