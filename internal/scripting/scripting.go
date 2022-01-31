package scripting

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scripting_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scripting"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type client struct {
	service Ydb_Scripting_V1.ScriptingServiceClient
}

func (c *client) Execute(
	ctx context.Context,
	query string,
	params *ydb_table.QueryParameters,
) (ydb_table_result.Result, error) {
	request := &Ydb_Scripting.ExecuteYqlRequest{
		Script:     query,
		Parameters: params.Params(),
	}
	response, err := c.service.ExecuteYql(ctx, request)
	if err != nil {
		return nil, err
	}
	result := Ydb_Scripting.ExecuteYqlResult{}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, err
	}
	return scanner.NewUnary(result.GetResultSets(), result.GetQueryStats()), nil
}

func mode2mode(mode ydb_scripting.ExplainMode) Ydb_Scripting.ExplainYqlRequest_Mode {
	switch mode {
	case ydb_scripting.ExplainModePlan:
		return Ydb_Scripting.ExplainYqlRequest_PLAN
	case ydb_scripting.ExplainModeValidate:
		return Ydb_Scripting.ExplainYqlRequest_VALIDATE
	default:
		return Ydb_Scripting.ExplainYqlRequest_MODE_UNSPECIFIED
	}
}

func (c *client) Explain(
	ctx context.Context,
	query string,
	mode ydb_scripting.ExplainMode,
) (e ydb_table.ScriptingYQLExplanation, err error) {
	var (
		request = &Ydb_Scripting.ExplainYqlRequest{
			Script: query,
			Mode:   mode2mode(mode),
		}
		response *Ydb_Scripting.ExplainYqlResponse
		result   = Ydb_Scripting.ExplainYqlResult{}
	)
	response, err = c.service.ExplainYql(ctx, request)
	if err != nil {
		return
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return
	}
	result.GetParametersTypes()
	e = ydb_table.ScriptingYQLExplanation{
		Explanation: ydb_table.Explanation{
			Plan: result.GetPlan(),
		},
		ParameterTypes: make(map[string]ydb_table_types.Type, len(result.GetParametersTypes())),
	}
	for k, v := range result.GetParametersTypes() {
		e.ParameterTypes[k] = value.TypeFromYDB(v)
	}
	return e, nil
}

func (c *client) StreamExecute(
	ctx context.Context,
	query string,
	params *ydb_table.QueryParameters,
) (ydb_table_result.StreamResult, error) {
	request := &Ydb_Scripting.ExecuteYqlRequest{
		Script:     query,
		Parameters: params.Params(),
	}

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
			return err
		},
	), nil
}

func (c *client) Close(context.Context) error {
	return nil
}

func New(cc grpc.ClientConnInterface) ydb_scripting.Client {
	return &client{
		service: Ydb_Scripting_V1.NewScriptingServiceClient(cc),
	}
}
