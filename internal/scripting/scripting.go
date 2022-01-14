package scheme

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scripting_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scripting"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
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
	params *table.QueryParameters,
) (result.Result, error) {
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
) (result.StreamResult, error) {
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

	r := scanner.NewStream()
	go func() {
		var (
			response *Ydb_Scripting.ExecuteYqlPartialResponse
			err      error
		)
		defer func() {
			cancel()
			r.Close()
		}()
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				r.SetErr(err)
				return
			default:
				if response, err = stream.Recv(); err != nil {
					if !errors.Is(err, io.EOF) {
						r.SetErr(err)
						// nolint: ineffassign
						err = nil
					}
					return
				}
				if result := response.GetResult(); result != nil {
					if resultSet := result.GetResultSet(); resultSet != nil {
						r.Append(resultSet)
					}
					if stats := result.GetQueryStats(); stats != nil {
						r.UpdateStats(stats)
					}
				}
			}
		}
	}()
	return r, nil
}

func (c *client) Close(context.Context) error {
	return nil
}

func New(cc grpc.ClientConnInterface) scripting.Client {
	return &client{
		service: Ydb_Scripting_V1.NewScriptingServiceClient(cc),
	}
}
