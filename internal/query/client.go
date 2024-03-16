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
) (finalErr error) {
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
			onIntermediate := trace.QueryOnDo(doOpts.Trace(), &ctx, stack.FunctionID(""))

			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				onDone := onIntermediate(info.Error)

				return func(info trace.RetryLoopDoneInfo) {
					onDone(info.Attempts, info.Error)
				}
			}
		},
	}))...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) Do(ctx context.Context, op query.Operation, opts ...options.DoOption) error {
	return do(ctx, c.pool, op, c.config.Trace(), opts...)
}

func doTx(
	ctx context.Context,
	pool *pool.Pool[*Session, Session],
	op query.TxOperation,
	t *trace.Query,
	opts ...options.DoTxOption,
) error {
	doTxOpts := options.ParseDoTxOpts(t, opts...)

	err := do(ctx, pool, func(ctx context.Context, s query.Session) error {
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
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) DoTx(ctx context.Context, op query.TxOperation, opts ...options.DoTxOption) error {
	return doTx(ctx, c.pool, op, c.config.Trace(), opts...)
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

	t := sessionTrace(cfg.Trace())

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
				withSessionTrace(t),
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

func sessionTrace(t *trace.Query) *traceSession {
	return &traceSession{
		onCreate: func(ctx context.Context, functionID stack.Caller) func(*Session, error) {
			onDone := trace.QueryOnSessionCreate(t, &ctx, functionID)

			return func(s *Session, err error) {
				onDone(s, err)
			}
		},
		onClose: func(ctx context.Context, functionID stack.Caller, s *Session) func(err error) {
			onDone := trace.QueryOnSessionDelete(t, &ctx, functionID, s)

			return func(err error) {
				onDone(err)
			}
		},
		onAttach: func(ctx context.Context, functionID stack.Caller, s *Session) func(err error) {
			onDone := trace.QueryOnSessionAttach(t, &ctx, functionID, s)

			return func(err error) {
				onDone(err)
			}
		},
	}
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
