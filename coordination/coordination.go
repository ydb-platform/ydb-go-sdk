package coordination

import (
	"context"
	"github.com/YandexDatabase/ydb-go-genproto/Ydb_Coordination_V1"
	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Coordination"
	"github.com/YandexDatabase/ydb-go-sdk/v3"
	"github.com/YandexDatabase/ydb-go-sdk/v3/scheme"
	"google.golang.org/protobuf/proto"
)

type ConsistencyMode uint

const (
	ConsistencyModeUnset ConsistencyMode = iota
	ConsistencyModeStrict
	ConsistencyModeRelaxed
)

func (t ConsistencyMode) String() string {
	switch t {
	default:
		return "Unknown"
	case ConsistencyModeUnset:
		return "Unset"
	case ConsistencyModeStrict:
		return "Strict"
	case ConsistencyModeRelaxed:
		return "Relaxed"
	}
}

type RateLimiterCountersMode uint

const (
	RateLimiterCountersModeUnset RateLimiterCountersMode = iota
	RateLimiterCountersModeAggregated
	RateLimiterCountersModeDetailed
)

func (t RateLimiterCountersMode) String() string {
	switch t {
	default:
		return "Unknown"
	case RateLimiterCountersModeUnset:
		return "Unset"
	case RateLimiterCountersModeAggregated:
		return "Aggregated"
	case RateLimiterCountersModeDetailed:
		return "Detailed"
	}
}

type Config struct {
	Path                     string
	SelfCheckPeriodMillis    uint32
	SessionGracePeriodMillis uint32
	ReadConsistencyMode      ConsistencyMode
	AttachConsistencyMode    ConsistencyMode
	RateLimiterCountersMode  RateLimiterCountersMode
}

type Client struct {
	coordinationService Ydb_Coordination_V1.CoordinationServiceClient
}

func NewClient(cluster ydb.Cluster) *Client {
	return &Client{
		coordinationService: Ydb_Coordination_V1.NewCoordinationServiceClient(cluster),
	}
}

func (c *Client) CreateNode(ctx context.Context, path string, config Config) (err error) {
	_, err = c.coordinationService.CreateNode(ctx, &Ydb_Coordination.CreateNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.to(),
			AttachConsistencyMode:    config.AttachConsistencyMode.to(),
			RateLimiterCountersMode:  config.RateLimiterCountersMode.to(),
		},
	})
	return
}

func (c *Client) AlterNode(ctx context.Context, path string, config Config) (err error) {
	_, err = c.coordinationService.AlterNode(ctx, &Ydb_Coordination.AlterNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.to(),
			AttachConsistencyMode:    config.AttachConsistencyMode.to(),
			RateLimiterCountersMode:  config.RateLimiterCountersMode.to(),
		},
	})
	return
}

func (c *Client) DropNode(ctx context.Context, path string) (err error) {
	_, err = c.coordinationService.DropNode(ctx, &Ydb_Coordination.DropNodeRequest{
		Path: path,
	})
	return
}

// Describes a coordination node
func (c *Client) DescribeNode(ctx context.Context, path string) (_ *scheme.Entry, _ *Config, err error) {
	var (
		response *Ydb_Coordination.DescribeNodeResponse
		result   Ydb_Coordination.DescribeNodeResult
	)
	response, err = c.coordinationService.DescribeNode(ctx, &Ydb_Coordination.DescribeNodeRequest{
		Path: path,
	})
	if err != nil {
		return nil, nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &result)
	if err != nil {
		return nil, nil, err
	}
	return scheme.InnerConvertEntry(result.GetSelf()), &Config{
		Path:                     result.GetConfig().GetPath(),
		SelfCheckPeriodMillis:    result.GetConfig().GetSelfCheckPeriodMillis(),
		SessionGracePeriodMillis: result.GetConfig().GetSessionGracePeriodMillis(),
		ReadConsistencyMode:      consistencyMode(result.GetConfig().GetReadConsistencyMode()),
		AttachConsistencyMode:    consistencyMode(result.GetConfig().GetAttachConsistencyMode()),
		RateLimiterCountersMode:  rateLimiterCountersMode(result.GetConfig().GetRateLimiterCountersMode()),
	}, nil

}

func (t ConsistencyMode) to() Ydb_Coordination.ConsistencyMode {
	switch t {
	case ConsistencyModeStrict:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT
	case ConsistencyModeRelaxed:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED
	default:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_UNSET
	}
}

func consistencyMode(t Ydb_Coordination.ConsistencyMode) ConsistencyMode {
	switch t {
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT:
		return ConsistencyModeStrict
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED:
		return ConsistencyModeRelaxed
	default:
		return ConsistencyModeUnset
	}
}

func (t RateLimiterCountersMode) to() Ydb_Coordination.RateLimiterCountersMode {
	switch t {
	case RateLimiterCountersModeAggregated:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED
	case RateLimiterCountersModeDetailed:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED
	default:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_UNSET
	}
}

func rateLimiterCountersMode(t Ydb_Coordination.RateLimiterCountersMode) RateLimiterCountersMode {
	switch t {
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED:
		return RateLimiterCountersModeAggregated
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED:
		return RateLimiterCountersModeDetailed
	default:
		return RateLimiterCountersModeUnset
	}
}
