package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

type balancer interface {
	grpc.ClientConnInterface
}

var _ query.Client = (*Client)(nil)

type Client struct {
	grpcClient Ydb_Query_V1.QueryServiceClient
	pool       SessionPool
}

func (c Client) Close(ctx context.Context) error {
	err := c.pool.Close(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func (c Client) do(ctx context.Context, op query.Operation, opts query.DoOptions) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		err := c.pool.With(ctx, func(ctx context.Context, s *session) error {
			err := op(ctx, s)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}
			return nil
		})
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		return nil
	}, opts.RetryOptions...)
}

func (c Client) Do(ctx context.Context, op query.Operation, opts ...query.DoOption) error {
	doOptions := query.NewDoOptions(opts...)
	if doOptions.Label != "" {
		doOptions.RetryOptions = append(doOptions.RetryOptions, retry.WithLabel(doOptions.Label))
	}
	if doOptions.Idempotent {
		doOptions.RetryOptions = append(doOptions.RetryOptions, retry.WithIdempotent(doOptions.Idempotent))
	}
	return c.do(ctx, op, doOptions)
}

func (c Client) DoTx(ctx context.Context, op query.TxOperation, opts ...query.DoTxOption) error {
	doTxOptions := query.NewDoTxOptions(opts...)
	return c.do(ctx, func(ctx context.Context, s query.Session) error {
		tx, err := s.Begin(ctx, doTxOptions.TxSettings)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		err = op(ctx, tx)
		if err != nil {
			errRollback := tx.Rollback(ctx)
			if errRollback != nil {
				return xerrors.WithStackTrace(xerrors.Join(err, errRollback))
			}
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
	}, doTxOptions.DoOptions)
}

func New(ctx context.Context, balancer balancer, config *config.Config) (*Client, error) {
	grpcClient := Ydb_Query_V1.NewQueryServiceClient(balancer)
	return &Client{
		grpcClient: grpcClient,
		pool: &poolMock{
			create: func(ctx context.Context) (*session, error) {
				s, err := grpcClient.CreateSession(ctx, &Ydb_Query.CreateSessionRequest{})
				if err != nil {
					return nil, xerrors.WithStackTrace(
						xerrors.Transport(err),
					)
				}
				if s.GetStatus() != Ydb.StatusIds_SUCCESS {
					return nil, xerrors.WithStackTrace(
						xerrors.Operation(xerrors.FromOperation(s)),
					)
				}
				return &session{
					id:           s.GetSessionId(),
					nodeID:       s.GetNodeId(),
					queryService: grpcClient,
					status:       query.SessionStatusReady,
				}, nil
			},
			close: func(ctx context.Context, s *session) error {
				_, err := grpcClient.DeleteSession(ctx,
					&Ydb_Query.DeleteSessionRequest{
						SessionId: s.id,
					},
				)
				if err != nil {
					return xerrors.WithStackTrace(err)
				}
				return nil
			},
		},
	}, nil
}
