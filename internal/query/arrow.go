package query

import (
	"bytes"
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	arrowResult struct {
		stream         Ydb_Query_V1.QueryService_ExecuteQueryClient
		resultSetIndex int64
	}

	arrowPart struct {
		part *Ydb_Query.ExecuteQueryResponsePart
	}
)

func (c *Client) QueryArrow(ctx context.Context, q string, opts ...options.Execute) (r arrow.Result, err error) {
	ctx, cancel := xcontext.WithDone(ctx, c.done)
	defer cancel()

	opts = append(opts, options.WithResultSetType(options.ResultSetTypeArrow))

	settings := options.ExecuteSettings(opts...)

	onDone := trace.QueryOnQuery(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Client).Query"),
		q, settings.Label(),
	)
	defer func() {
		onDone(err)
	}()

	r, err = arrowQuery(ctx, c.pool, q, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func arrowQuery(ctx context.Context, pool sessionPool, q string, opts ...options.Execute) (
	r arrow.Result, err error,
) {
	settings := options.ExecuteSettings(opts...)
	err = do(ctx, pool, func(ctx context.Context, s *Session) (err error) {
		r, err = s.executeArrow(ctx, q, options.ExecuteSettings(opts...), withTrace(s.trace))
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, settings.RetryOpts()...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (s *Session) executeArrow(
	ctx context.Context, q string, settings executeSettings, opts ...resultOption,
) (_ arrow.Result, finalErr error) {
	ctx, cancel := xcontext.WithDone(ctx, s.Done())
	defer func() {
		if finalErr != nil {
			cancel()
			applyStatusByError(s, finalErr)
		}
	}()

	request, callOptions, err := executeQueryRequest(s.ID(), q, settings)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	executeCtx, executeCancel := xcontext.WithCancel(xcontext.ValueOnly(ctx))
	defer func() {
		if finalErr != nil {
			executeCancel()
		}
	}()

	stream, err := s.client.ExecuteQuery(executeCtx, request, callOptions...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return &arrowResult{stream: stream}, nil
}

func (r *arrowResult) NextPart(ctx context.Context) (arrow.Part, error) {
	if ctx.Err() != nil {
		return nil, xerrors.WithStackTrace(ctx.Err())
	}

	part, err := r.stream.Recv()
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if part.GetResultSetIndex() <= 0 && r.resultSetIndex > 0 {
		return nil, xerrors.WithStackTrace(io.EOF)
	}
	r.resultSetIndex = part.GetResultSetIndex()

	return &arrowPart{part}, nil
}

func (p *arrowPart) Schema() io.Reader {
	return bytes.NewReader(p.part.GetResultSet().GetArrowBatchSettings().GetSchema())
}

func (p *arrowPart) Data() io.Reader {
	return bytes.NewReader(p.part.GetResultSet().GetData())
}

func (p *arrowPart) GetResultSetIndex() int64 {
	return p.part.GetResultSetIndex()
}
