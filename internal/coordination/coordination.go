package coordination

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Coordination_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
)

type Client interface {
	CreateNode(ctx context.Context, path string, config coordination.Config) (err error)
	AlterNode(ctx context.Context, path string, config coordination.Config) (err error)
	DropNode(ctx context.Context, path string) (err error)
	DescribeNode(ctx context.Context, path string) (_ *scheme.Entry, _ *coordination.Config, err error)
	Close(ctx context.Context) error
}

type client struct {
	service Ydb_Coordination_V1.CoordinationServiceClient
}

func New(db cluster.DB) Client {
	return &client{
		service: Ydb_Coordination_V1.NewCoordinationServiceClient(db),
	}
}

func (c *client) isNil() bool {
	return c == nil
}

func (c *client) CreateNode(ctx context.Context, path string, config coordination.Config) (err error) {
	_, err = c.service.CreateNode(ctx, &Ydb_Coordination.CreateNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.To(),
			AttachConsistencyMode:    config.AttachConsistencyMode.To(),
			RateLimiterCountersMode:  config.RateLimiterCountersMode.To(),
		},
	})
	return
}

func (c *client) AlterNode(ctx context.Context, path string, config coordination.Config) (err error) {
	_, err = c.service.AlterNode(ctx, &Ydb_Coordination.AlterNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.To(),
			AttachConsistencyMode:    config.AttachConsistencyMode.To(),
			RateLimiterCountersMode:  config.RateLimiterCountersMode.To(),
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
func (c *client) DescribeNode(ctx context.Context, path string) (_ *scheme.Entry, _ *coordination.Config, err error) {
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
	return scheme.InnerConvertEntry(result.GetSelf()), &coordination.Config{
		Path:                     result.GetConfig().GetPath(),
		SelfCheckPeriodMillis:    result.GetConfig().GetSelfCheckPeriodMillis(),
		SessionGracePeriodMillis: result.GetConfig().GetSessionGracePeriodMillis(),
		ReadConsistencyMode:      consistencyMode(result.GetConfig().GetReadConsistencyMode()),
		AttachConsistencyMode:    consistencyMode(result.GetConfig().GetAttachConsistencyMode()),
		RateLimiterCountersMode:  rateLimiterCountersMode(result.GetConfig().GetRateLimiterCountersMode()),
	}, nil

}

func (c *client) Close(ctx context.Context) error {
	return nil
}

func consistencyMode(t Ydb_Coordination.ConsistencyMode) coordination.ConsistencyMode {
	switch t {
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT:
		return coordination.ConsistencyModeStrict
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED:
		return coordination.ConsistencyModeRelaxed
	default:
		return coordination.ConsistencyModeUnset
	}
}

func rateLimiterCountersMode(t Ydb_Coordination.RateLimiterCountersMode) coordination.RateLimiterCountersMode {
	switch t {
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED:
		return coordination.RateLimiterCountersModeAggregated
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED:
		return coordination.RateLimiterCountersModeDetailed
	default:
		return coordination.RateLimiterCountersModeUnset
	}
}
