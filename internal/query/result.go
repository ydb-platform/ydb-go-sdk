package query

import (
	"context"
	"fmt"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ query.Result = (*result)(nil)

type result struct {
	stream         Ydb_Query_V1.QueryService_ExecuteQueryClient
	closeOnce      func(ctx context.Context) error
	lastPart       *Ydb_Query.ExecuteQueryResponsePart
	resultSetIndex int64
	errs           []error
	closed         chan struct{}
	trace          *trace.Query
}

func newResult(
	ctx context.Context,
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	t *trace.Query,
	closeResult context.CancelFunc,
) (_ *result, txID string, err error) {
	if t == nil {
		t = new(trace.Query)
	}
	if closeResult == nil {
		closeResult = func() {}
	}

	onDone := trace.QueryOnResultNew(t, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.newResult"),
	)
	defer func() {
		onDone(err)
	}()

	select {
	case <-ctx.Done():
		return nil, txID, xerrors.WithStackTrace(ctx.Err())
	default:
		part, err := nextPart(ctx, stream, t)
		if err != nil {
			return nil, txID, xerrors.WithStackTrace(err)
		}
		var (
			interrupted = make(chan struct{})
			closed      = make(chan struct{})
			closeOnce   = xsync.OnceFunc(func(ctx context.Context) error {
				closeResult()

				close(interrupted)
				close(closed)

				return nil
			})
		)

		return &result{
			stream:         stream,
			resultSetIndex: -1,
			lastPart:       part,
			closed:         closed,
			closeOnce:      closeOnce,
			errs:           nil,
			trace:          t,
		}, part.GetTxMeta().GetId(), nil
	}
}

func nextPart(
	ctx context.Context,
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	t *trace.Query,
) (_ *Ydb_Query.ExecuteQueryResponsePart, finalErr error) {
	if t == nil {
		t = new(trace.Query)
	}

	onDone := trace.QueryOnResultNextPart(t, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.nextPart"),
	)
	defer func() {
		onDone(finalErr)
	}()

	part, err := stream.Recv()
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return part, nil
}

func (r *result) Close(ctx context.Context) (err error) {
	onDone := trace.QueryOnResultClose(r.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*result).Close"),
	)
	defer func() {
		onDone(err)
	}()

	return r.closeOnce(ctx)
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
			if resultSetIndex := r.lastPart.GetResultSetIndex(); resultSetIndex >= nextResultSetIndex {
				r.resultSetIndex = resultSetIndex

				return newResultSet(r.getNextResultSetPart(ctx, nextResultSetIndex), r.lastPart, r.trace), nil
			}
			part, err := nextPart(ctx, r.stream, r.trace)
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

func (r *result) getNextResultSetPart(
	ctx context.Context,
	nextResultSetIndex int64,
) func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
	return func() (_ *Ydb_Query.ExecuteQueryResponsePart, err error) {
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
			part, err := nextPart(ctx, r.stream, r.trace)
			if err != nil {
				if xerrors.Is(err, io.EOF) {
					_ = r.closeOnce(ctx)
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
	}
}

func (r *result) NextResultSet(ctx context.Context) (_ query.ResultSet, err error) {
	onDone := trace.QueryOnResultNextResultSet(r.trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/query.(*result).NextResultSet"),
	)
	defer func() {
		onDone(err)
	}()

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
