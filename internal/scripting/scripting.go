package scripting

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scripting_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scripting"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type client struct {
	config  config.Config
	service Ydb_Scripting_V1.ScriptingServiceClient
}

func (c *client) Execute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (r result.Result, err error) {
	var (
		onDone  = trace.ScriptingOnExecute(c.config.Trace(), &ctx, query, params)
		request = &Ydb_Scripting.ExecuteYqlRequest{
			Script:     query,
			Parameters: params.Params(),
			OperationParams: operation.Params(
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		}
		result   = Ydb_Scripting.ExecuteYqlResult{}
		response *Ydb_Scripting.ExecuteYqlResponse
	)
	defer func() {
		onDone(r, err)
	}()
	response, err = c.service.ExecuteYql(ctx, request)
	if err != nil {
		return nil, err
	}

	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, err
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

func (c *client) Explain(
	ctx context.Context,
	query string,
	mode scripting.ExplainMode,
) (e table.ScriptingYQLExplanation, err error) {
	var (
		onDone  = trace.ScriptingOnExplain(c.config.Trace(), &ctx, query)
		request = &Ydb_Scripting.ExplainYqlRequest{
			Script: query,
			Mode:   mode2mode(mode),
			OperationParams: operation.Params(
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
		return
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
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

func (c *client) StreamExecute(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
) (r result.StreamResult, err error) {
	var (
		onIntermediate = trace.ScriptingOnStreamExecute(c.config.Trace(), &ctx, query, params)
		request        = &Ydb_Scripting.ExecuteYqlRequest{
			Script:     query,
			Parameters: params.Params(),
			OperationParams: operation.Params(
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		}
	)
	defer func() {
		if err != nil {
			onIntermediate(err)(err)
		}
	}()

	ctx, cancel := context.WithCancel(ctx)

	stream, err := c.service.StreamExecuteYql(ctx, request)
	if err != nil {
		cancel()
		return nil, err
	}

	return scanner.NewStream(
		func(ctx context.Context) (
			set *Ydb.ResultSet,
			stats *Ydb_TableStats.QueryStats,
			err error,
		) {
			defer func() {
				onIntermediate(err)
			}()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			default:
				response, err := stream.Recv()
				result := response.GetResult()
				if result == nil || err != nil {
					return nil, nil, err
				}
				return result.GetResultSet(), result.GetQueryStats(), nil
			}
		},
		func(err error) error {
			cancel()
			onIntermediate(err)(err)
			return err
		},
	), nil
}

func (c *client) Close(ctx context.Context) (err error) {
	onDone := trace.ScriptingOnClose(c.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()
	return nil
}

func New(cc grpc.ClientConnInterface, options []config.Option) scripting.Client {
	return &client{
		config:  config.New(options...),
		service: Ydb_Scripting_V1.NewScriptingServiceClient(cc),
	}
}
