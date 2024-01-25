package scripting

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scripting_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scripting"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scripting/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:gofumpt
//nolint:nolintlint
var (
	errNilClient = xerrors.Wrap(errors.New("scripting client is not initialized"))
)

type Client struct {
	config  config.Config
	service Ydb_Scripting_V1.ScriptingServiceClient
}

func (c *Client) Execute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (r result.Result, err error) {
	if c == nil {
		return r, xerrors.WithStackTrace(errNilClient)
	}
	call := func(ctx context.Context) error {
		r, err = c.execute(ctx, query, params)
		return xerrors.WithStackTrace(err)
	}
	if !c.config.AutoRetry() {
		err = call(ctx)
		return
	}
	err = retry.Retry(ctx, call,
		retry.WithStackTrace(),
		retry.WithTrace(c.config.TraceRetry()),
	)
	return r, xerrors.WithStackTrace(err)
}

func (c *Client) execute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (r result.Result, err error) {
	var (
		onDone = trace.ScriptingOnExecute(c.config.Trace(), &ctx,
			stack.FunctionID(""),
			query, params,
		)
		a       = allocator.New()
		request = &Ydb_Scripting.ExecuteYqlRequest{
			Script:     query,
			Parameters: params.Params().ToYDB(a),
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		}
		result   = Ydb_Scripting.ExecuteYqlResult{}
		response *Ydb_Scripting.ExecuteYqlResponse
	)
	defer func() {
		a.Free()
		onDone(r, err)
	}()
	response, err = c.service.ExecuteYql(ctx, request)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return scanner.NewUnary(result.GetResultSets(), result.GetQueryStats()), nil
}

func mode2mode(mode scripting.ExplainMode) Ydb_Scripting.ExplainYqlRequest_Mode {
	switch mode {
	case scripting.ExplainModePlan:
		return Ydb_Scripting.ExplainYqlRequest_PLAN
	case scripting.ExplainModeValidate:
		return Ydb_Scripting.ExplainYqlRequest_VALIDATE
	default:
		return Ydb_Scripting.ExplainYqlRequest_MODE_UNSPECIFIED
	}
}

func (c *Client) Explain(
	ctx context.Context,
	query string,
	mode scripting.ExplainMode,
) (e table.ScriptingYQLExplanation, err error) {
	if c == nil {
		return e, xerrors.WithStackTrace(errNilClient)
	}
	call := func(ctx context.Context) error {
		e, err = c.explain(ctx, query, mode)
		return xerrors.WithStackTrace(err)
	}
	if !c.config.AutoRetry() {
		err = call(ctx)
		return
	}
	err = retry.Retry(ctx, call,
		retry.WithStackTrace(),
		retry.WithIdempotent(true),
		retry.WithTrace(c.config.TraceRetry()),
	)
	return e, xerrors.WithStackTrace(err)
}

func (c *Client) explain(
	ctx context.Context,
	query string,
	mode scripting.ExplainMode,
) (e table.ScriptingYQLExplanation, err error) {
	var (
		onDone = trace.ScriptingOnExplain(c.config.Trace(), &ctx,
			stack.FunctionID(""),
			query,
		)
		request = &Ydb_Scripting.ExplainYqlRequest{
			Script: query,
			Mode:   mode2mode(mode),
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		}
		response *Ydb_Scripting.ExplainYqlResponse
		result   = Ydb_Scripting.ExplainYqlResult{}
	)
	defer func() {
		onDone(e.Explanation.Plan, err)
	}()
	response, err = c.service.ExplainYql(ctx, request)
	if err != nil {
		return e, err
	}
	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return e, err
	}
	result.GetParametersTypes()
	e = table.ScriptingYQLExplanation{
		Explanation: table.Explanation{
			Plan: result.GetPlan(),
		},
		ParameterTypes: make(map[string]types.Type, len(result.GetParametersTypes())),
	}
	for k, v := range result.GetParametersTypes() {
		e.ParameterTypes[k] = value.TypeFromYDB(v)
	}
	return e, nil
}

func (c *Client) StreamExecute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (r result.StreamResult, err error) {
	if c == nil {
		return r, xerrors.WithStackTrace(errNilClient)
	}
	call := func(ctx context.Context) error {
		r, err = c.streamExecute(ctx, query, params)
		return xerrors.WithStackTrace(err)
	}
	if !c.config.AutoRetry() {
		err = call(ctx)
		return
	}
	err = retry.Retry(ctx, call,
		retry.WithStackTrace(),
		retry.WithTrace(c.config.TraceRetry()),
	)
	return r, xerrors.WithStackTrace(err)
}

func (c *Client) streamExecute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (r result.StreamResult, err error) {
	var (
		onIntermediate = trace.ScriptingOnStreamExecute(c.config.Trace(), &ctx,
			stack.FunctionID(""),
			query, params,
		)
		a       = allocator.New()
		request = &Ydb_Scripting.ExecuteYqlRequest{
			Script:     query,
			Parameters: params.Params().ToYDB(a),
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		}
	)
	defer func() {
		a.Free()
		if err != nil {
			onIntermediate(err)(err)
		}
	}()

	ctx, cancel := xcontext.WithCancel(ctx)

	stream, err := c.service.StreamExecuteYql(ctx, request)
	if err != nil {
		cancel()
		return nil, xerrors.WithStackTrace(err)
	}

	return scanner.NewStream(ctx,
		func(ctx context.Context) (
			set *Ydb.ResultSet,
			stats *Ydb_TableStats.QueryStats,
			err error,
		) {
			defer func() {
				onIntermediate(xerrors.HideEOF(err))
			}()
			select {
			case <-ctx.Done():
				return nil, nil, xerrors.WithStackTrace(ctx.Err())
			default:
				var response *Ydb_Scripting.ExecuteYqlPartialResponse
				response, err = stream.Recv()
				result := response.GetResult()
				if result == nil || err != nil {
					return nil, nil, xerrors.WithStackTrace(err)
				}
				return result.GetResultSet(), result.GetQueryStats(), nil
			}
		},
		func(err error) error {
			cancel()
			onIntermediate(xerrors.HideEOF(err))(xerrors.HideEOF(err))
			return err
		},
	)
}

func (c *Client) Close(ctx context.Context) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	onDone := trace.ScriptingOnClose(c.config.Trace(), &ctx, stack.FunctionID(""))
	defer func() {
		onDone(err)
	}()
	return nil
}

func New(ctx context.Context, cc grpc.ClientConnInterface, config config.Config) (*Client, error) {
	return &Client{
		config:  config,
		service: Ydb_Scripting_V1.NewScriptingServiceClient(cc),
	}, nil
}
