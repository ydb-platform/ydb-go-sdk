package query

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ query.Result = (*result)(nil)

type result struct {
	stream         Ydb_Query_V1.QueryService_ExecuteQueryClient
	closeOnce      func()
	lastPart       *Ydb_Query.ExecuteQueryResponsePart
	resultSetIndex int64
	errs           []error
	closed         chan struct{}
}

func newResult(
	ctx context.Context,
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	streamCancel func(),
) (_ *result, txID string, err error) {
	select {
	case <-ctx.Done():
		return nil, txID, xerrors.WithStackTrace(ctx.Err())
	default:
		part, err := nextPart(stream)
		if err != nil {
			streamCancel()

			return nil, txID, xerrors.WithStackTrace(err)
		}
		var (
			interrupted = make(chan struct{})
			closed      = make(chan struct{})
			closeOnce   = sync.OnceFunc(func() {
				close(interrupted)
				close(closed)
				streamCancel()
			})
		)

		return &result{
			stream:         stream,
			resultSetIndex: -1,
			lastPart:       part,
			closed:         closed,
			closeOnce:      closeOnce,
		}, part.GetTxMeta().GetId(), nil
	}
}

func nextPart(
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
) (_ *Ydb_Query.ExecuteQueryResponsePart, finalErr error) {
	part, err := stream.Recv()
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return part, nil
}

func (r *result) Close(ctx context.Context) error {
	r.closeOnce()

	return nil
}

func (r *result) nextResultSet(ctx context.Context) (_ *resultSet, err error) {
	defer func() {
		if err != nil && !xerrors.Is(err,
			io.EOF, errClosedResult, context.Canceled,
		) {
			r.errs = append(r.errs, err)
		}
	}()
	nextResultSetIndex := r.resultSetIndex + 1
	for {
		select {
		case <-r.closed:
			return nil, xerrors.WithStackTrace(errClosedResult)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		default:
			if resultSetIndex := r.lastPart.GetResultSetIndex(); resultSetIndex >= nextResultSetIndex { //nolint:nestif
				r.resultSetIndex = resultSetIndex

				return newResultSet(func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
					defer func() {
						if err != nil && !xerrors.Is(err,
							io.EOF, context.Canceled,
						) {
							r.errs = append(r.errs, err)
						}
					}()
					select {
					case <-r.closed:
						return nil, errClosedResult
					default:
						part, err := nextPart(r.stream)
						if err != nil {
							if xerrors.Is(err, io.EOF) {
								r.closeOnce()
							}

							return nil, xerrors.WithStackTrace(err)
						}
						r.lastPart = part
						if part.GetResultSetIndex() > nextResultSetIndex {
							return nil, xerrors.WithStackTrace(fmt.Errorf(
								"result set (index=%d) receive part (index=%d) for next result set: %w",
								nextResultSetIndex, part.GetResultSetIndex(), io.EOF,
							))
						}

						return part, nil
					}
				}, r.lastPart), nil
			}
			part, err := nextPart(r.stream)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			if part.GetResultSetIndex() < r.resultSetIndex {
				return nil, xerrors.WithStackTrace(fmt.Errorf(
					"next result set index %d less than last result set index %d: %w",
					part.GetResultSetIndex(), r.resultSetIndex, errWrongNextResultSetIndex,
				))
			}
			r.lastPart = part
			r.resultSetIndex = part.GetResultSetIndex()
		}
	}
}

func (r *result) NextResultSet(ctx context.Context) (query.ResultSet, error) {
	return r.nextResultSet(ctx)
}

func (r *result) Err() error {
	switch {
	case len(r.errs) == 0:
		return nil
	case len(r.errs) == 1:
		return r.errs[0]
	default:
		return xerrors.WithStackTrace(xerrors.Join(r.errs...))
	}
}
