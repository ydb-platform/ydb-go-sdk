package coordination

import (
	"context"
	"github.com/YandexDatabase/ydb-go-genproto/Ydb_Coordination_V1"
	"google.golang.org/protobuf/proto"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Coordination"
	ydb "github.com/YandexDatabase/ydb-go-sdk/v2"
	"github.com/YandexDatabase/ydb-go-sdk/v2/scheme"
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

type client struct {
	cluster ydb.Cluster
}

func NewClient(cluster ydb.Cluster) *client {
	return &client{cluster: cluster}
}

func (c *client) CreateNode(ctx context.Context, path string, config Config) (err error) {
	request := Ydb_Coordination.CreateNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.to(),
			AttachConsistencyMode:    config.AttachConsistencyMode.to(),
			RateLimiterCountersMode:  config.RateLimiterCountersMode.to(),
		},
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return err
	}
	_, err = Ydb_Coordination_V1.NewCoordinationServiceClient(conn).CreateNode(ctx, &request)
	return err
}

func (c *client) AlterNode(ctx context.Context, path string, config Config) (err error) {
	request := Ydb_Coordination.AlterNodeRequest{
		Path: path,
		Config: &Ydb_Coordination.Config{
			Path:                     config.Path,
			SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
			SessionGracePeriodMillis: config.SessionGracePeriodMillis,
			ReadConsistencyMode:      config.ReadConsistencyMode.to(),
			AttachConsistencyMode:    config.AttachConsistencyMode.to(),
			RateLimiterCountersMode:  config.RateLimiterCountersMode.to(),
		},
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return err
	}
	_, err = Ydb_Coordination_V1.NewCoordinationServiceClient(conn).AlterNode(ctx, &request)
	return err
}

func (c *client) DropNode(ctx context.Context, path string) (err error) {
	request := Ydb_Coordination.DropNodeRequest{
		Path: path,
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return err
	}
	_, err = Ydb_Coordination_V1.NewCoordinationServiceClient(conn).DropNode(ctx, &request)
	return err
}

// Describes a coordination node
func (c *client) DescribeNode(ctx context.Context, path string) (*scheme.Entry, *Config, error) {
	var describeNodeResult Ydb_Coordination.DescribeNodeResult
	request := Ydb_Coordination.DescribeNodeRequest{
		Path: path,
	}
	conn, err := c.cluster.Get(ctx)
	if err != nil {
		return nil, nil, err
	}
	response, err := Ydb_Coordination_V1.NewCoordinationServiceClient(conn).DescribeNode(ctx, &request)
	if err != nil {
		return nil, nil, err
	}
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &describeNodeResult)
	if err != nil {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, err
	}
	return scheme.InnerConvertEntry(describeNodeResult.GetSelf()), &Config{
		Path:                     describeNodeResult.GetConfig().GetPath(),
		SelfCheckPeriodMillis:    describeNodeResult.GetConfig().GetSelfCheckPeriodMillis(),
		SessionGracePeriodMillis: describeNodeResult.GetConfig().GetSessionGracePeriodMillis(),
		ReadConsistencyMode:      consistencyMode(describeNodeResult.GetConfig().GetReadConsistencyMode()),
		AttachConsistencyMode:    consistencyMode(describeNodeResult.GetConfig().GetAttachConsistencyMode()),
		RateLimiterCountersMode:  rateLimiterCountersMode(describeNodeResult.GetConfig().GetRateLimiterCountersMode()),
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
