package config

type Algorithm uint8

const (
	BalancingAlgorithmRandomChoice = iota
	BalancingAlgorithmRoundRobin

	DefaultBalancingAlgorithm = BalancingAlgorithmRandomChoice
)

type BalancerConfig struct {
	// Algorithm define balancing algorithm
	Algorithm Algorithm

	// PreferLocal adds endpoint selection logic when local endpoints
	// are always used first.
	// When no alive local endpoints left other endpoints will be used.
	//
	// NOTE: some balancing methods (such as p2c) also may use knowledge of
	// endpoint's locality. Difference is that with PreferLocal local
	// endpoints selected separately from others. That is, if there at least
	// one local endpoint it will be used regardless of its performance
	// indicators.
	//
	// NOTE: currently driver (and even ydb itself) does not track load factor
	// of each endpoint properly. Enabling this Option may lead to the
	// situation, when all but one nodes in local datacenter become inactive
	// and all clients will overload this single instance very quickly. That
	// is, currently this Option may be called as experimental.
	// You have been warned.
	PreferLocal bool
}

var DefaultBalancer = BalancerConfig{Algorithm: DefaultBalancingAlgorithm, PreferLocal: true}
