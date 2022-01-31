package coordination

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Coordination_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type client struct {
	service Ydb_Coordination_V1.CoordinationServiceClient
}

func New(cc grpc.ClientConnInterface) ydb_coordination.Client {
	return &client{
		service: Ydb_Coordination_V1.NewCoordinationServiceClient(cc),
	}
}

func (c *client) CreateNode(ctx context.Context, path string, config ydb_coordination.Config) (err error) {
	_, err = c.service.CreateNode(ctx, &Ydb_Coordination.CreateNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.To(),
			AttachConsistencyMode:    config.AttachConsistencyMode.To(),
			RateLimiterCountersMode:  config.RatelimiterCountersMode.To(),
		},
	})
	return
}

func (c *client) AlterNode(ctx context.Context, path string, config ydb_coordination.Config) (err error) {
	_, err = c.service.AlterNode(ctx, &Ydb_Coordination.AlterNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.To(),
			AttachConsistencyMode:    config.AttachConsistencyMode.To(),
			RateLimiterCountersMode:  config.RatelimiterCountersMode.To(),
		},
	})
	return
}

func (c *client) DropNode(ctx context.Context, path string) (err error) {
	_, err = c.service.DropNode(ctx, &Ydb_Coordination.DropNodeRequest{
		Path: path,
	})
	return
}

// DescribeNode describes a coordination node
func (c *client) DescribeNode(
	ctx context.Context,
	path string,
) (
	_ *ydb_scheme.Entry,
	_ *ydb_coordination.Config,
	err error,
) {
	var (
		response *Ydb_Coordination.DescribeNodeResponse
		result   Ydb_Coordination.DescribeNodeResult
	)
	response, err = c.service.DescribeNode(ctx, &Ydb_Coordination.DescribeNodeRequest{
		Path: path,
	})
	if err != nil {
		return nil, nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, nil, err
	}
	return ydb_scheme.InnerConvertEntry(result.GetSelf()), &ydb_coordination.Config{
		Path:                     result.GetConfig().GetPath(),
		SelfCheckPeriodMillis:    result.GetConfig().GetSelfCheckPeriodMillis(),
		SessionGracePeriodMillis: result.GetConfig().GetSessionGracePeriodMillis(),
		ReadConsistencyMode:      consistencyMode(result.GetConfig().GetReadConsistencyMode()),
		AttachConsistencyMode:    consistencyMode(result.GetConfig().GetAttachConsistencyMode()),
		RatelimiterCountersMode:  rateLimiterCountersMode(result.GetConfig().GetRateLimiterCountersMode()),
	}, nil
}

func (c *client) Close(context.Context) error {
	return nil
}

func consistencyMode(t Ydb_Coordination.ConsistencyMode) ydb_coordination.ConsistencyMode {
	switch t {
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT:
		return ydb_coordination.ConsistencyModeStrict
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED:
		return ydb_coordination.ConsistencyModeRelaxed
	default:
		return ydb_coordination.ConsistencyModeUnset
	}
}

func rateLimiterCountersMode(t Ydb_Coordination.RateLimiterCountersMode) ydb_coordination.RatelimiterCountersMode {
	switch t {
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED:
		return ydb_coordination.RatelimiterCountersModeAggregated
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED:
		return ydb_coordination.RatelimiterCountersModeDetailed
	default:
		return ydb_coordination.RatelimiterCountersModeUnset
	}
}
