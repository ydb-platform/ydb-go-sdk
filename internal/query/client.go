package query

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//go:generate mockgen -destination grpc_client_mock_test.go -package query -write_package_comment=false github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1 QueryServiceClient,QueryService_AttachSessionClient,QueryService_ExecuteQueryClient

type balancer interface {
	grpc.ClientConnInterface
}

var _ query.Client = (*Client)(nil)

type Client struct {
	config     *config.Config
	grpcClient Ydb_Query_V1.QueryServiceClient
	pool       *pool.Pool[Session]
}

func (c Client) Close(ctx context.Context) error {
	err := c.pool.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func do(
	ctx context.Context,
	pool *pool.Pool[Session],
	op query.Operation,
	t *trace.Query,
	opts ...query.DoOption,
) (finalErr error) {
	doOpts := query.ParseDoOpts(t, opts...)

	err := pool.With(ctx, func(ctx context.Context, s *Session) error {
		err := op(ctx, s)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

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

func (c Client) Do(ctx context.Context, op query.Operation, opts ...query.DoOption) error {
	return do(ctx, c.pool, op, c.config.Trace(), opts...)
}

func doTx(
	ctx context.Context,
	pool *pool.Pool[Session],
	op query.TxOperation,
	t *trace.Query,
	opts ...query.DoTxOption,
) error {
	doTxOpts := query.ParseDoTxOpts(t, opts...)

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

func (c Client) DoTx(ctx context.Context, op query.TxOperation, opts ...query.DoTxOption) error {
	return doTx(ctx, c.pool, op, c.config.Trace(), opts...)
}

func deleteSession(ctx context.Context, client Ydb_Query_V1.QueryServiceClient, sessionID string) error {
	response, err := client.DeleteSession(ctx,
		&Ydb_Query.DeleteSessionRequest{
			SessionId: sessionID,
		},
	)
	if err != nil {
		return xerrors.WithStackTrace(xerrors.Transport(err))
	}
	if response.GetStatus() != Ydb.StatusIds_SUCCESS {
		return xerrors.WithStackTrace(xerrors.FromOperation(response))
	}

	return nil
}

type createSessionConfig struct {
	onAttach func(s *Session)
	onClose  func(s *Session)
}

func createSession(
	ctx context.Context, client Ydb_Query_V1.QueryServiceClient, cfg createSessionConfig,
) (_ *Session, finalErr error) {
	s, err := client.CreateSession(ctx, &Ydb_Query.CreateSessionRequest{})
	if err != nil {
		return nil, xerrors.WithStackTrace(
			xerrors.Transport(err),
		)
	}

	if s.GetStatus() != Ydb.StatusIds_SUCCESS {
		return nil, xerrors.WithStackTrace(
			xerrors.FromOperation(s),
		)
	}

	defer func() {
		if finalErr != nil {
			_ = deleteSession(ctx, client, s.GetSessionId())
		}
	}()

	attachCtx, cancelAttach := xcontext.WithCancel(context.Background())
	defer func() {
		if finalErr != nil {
			cancelAttach()
		}
	}()

	attach, err := client.AttachSession(attachCtx, &Ydb_Query.AttachSessionRequest{
		SessionId: s.GetSessionId(),
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(
			xerrors.Transport(err),
		)
	}

	defer func() {
		if finalErr != nil {
			_ = attach.CloseSend()
		}
	}()

	state, err := attach.Recv()
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Transport(err))
	}

	if state.GetStatus() != Ydb.StatusIds_SUCCESS {
		return nil, xerrors.WithStackTrace(xerrors.FromOperation(state))
	}

	session := &Session{
		id:          s.GetSessionId(),
		nodeID:      s.GetNodeId(),
		queryClient: client,
		status:      query.SessionStatusReady,
	}

	if cfg.onAttach != nil {
		cfg.onAttach(session)
	}

	session.close = sync.OnceFunc(func() {
		if cfg.onClose != nil {
			cfg.onClose(session)
		}

		_ = attach.CloseSend()

		cancelAttach()

		atomic.StoreUint32(
			(*uint32)(&session.status),
			uint32(query.SessionStatusClosed),
		)
	})

	go func() {
		defer session.close()
		for {
			switch session.Status() {
			case query.SessionStatusReady, query.SessionStatusInUse:
				sessionState, recvErr := attach.Recv()
				if recvErr != nil || sessionState.GetStatus() != Ydb.StatusIds_SUCCESS {
					return
				}
			default:
				return
			}
		}
	}()

	return session, nil
}

func New(ctx context.Context, balancer balancer, config *config.Config) (*Client, error) {
	client := &Client{
		config:     config,
		grpcClient: Ydb_Query_V1.NewQueryServiceClient(balancer),
	}

	client.pool = pool.New(
		config.PoolMaxSize(),
		func(ctx context.Context, onClose func(s *Session)) (*Session, error) {
			var cancel context.CancelFunc
			if d := config.CreateSessionTimeout(); d > 0 {
				ctx, cancel = xcontext.WithTimeout(ctx, d)
			} else {
				ctx, cancel = xcontext.WithCancel(ctx)
			}
			defer cancel()

			s, err := createSession(ctx, client.grpcClient, createSessionConfig{
				onClose: onClose,
			})
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return s, nil
		},
		func(ctx context.Context, s *Session) error {
			var cancel context.CancelFunc
			if d := config.CreateSessionTimeout(); d > 0 {
				ctx, cancel = xcontext.WithTimeout(ctx, d)
			} else {
				ctx, cancel = xcontext.WithCancel(ctx)
			}
			defer cancel()

			err := deleteSession(ctx, client.grpcClient, s.id)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
		xerrors.MustDeleteSession,
	)

	return client, ctx.Err()
}
