package coordination

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"

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

func (t ConsistencyMode) To() Ydb_Coordination.ConsistencyMode {
	switch t {
	case ConsistencyModeStrict:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT
	case ConsistencyModeRelaxed:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED
	default:
		return Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_UNSET
	}
}

func (t RateLimiterCountersMode) To() Ydb_Coordination.RateLimiterCountersMode {
	switch t {
	case RateLimiterCountersModeAggregated:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED
	case RateLimiterCountersModeDetailed:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED
	default:
		return Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_UNSET
	}
}
