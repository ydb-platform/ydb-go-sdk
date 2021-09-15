package balancer

import "time"

type Algorithm uint8

const (
	RandomChoice = iota
	RoundRobin
	P2C

	DefaultAlgorithm = RandomChoice
)

type Config struct {
	// Algorithm define balancing algorithm
	Algorithm Algorithm

	// PreferLocal reports whether p2c Balancer should prefer local endpoint
	// when all other runtime indicators are the same (such as error rate or
	// average response time).
	PreferLocal bool

	// OpTimeThreshold specifies such difference between endpoint average
	// operation time when it becomes significant to be used during comparison.
	OpTimeThreshold time.Duration
}
