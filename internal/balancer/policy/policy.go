package policy

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

// EndpointPriority describes endpoint selection priority independent of connection state.
type EndpointPriority struct {
	Key endpoint.Key
	// Priority is ordered from the most preferred endpoint at zero to the least preferred endpoint.
	Priority uint64
}

// Info contains immutable data shared by a policy during one discovery refresh.
type Info struct {
	SelfLocation string
}

type preference struct {
	name  string
	match func(info Info, candidate endpoint.Info) bool
}

// Policy is an immutable endpoint priority pipeline.
type Policy struct {
	preferences      []preference
	singleConnection bool
	detectNearestDC  bool
}

func SingleConn() Policy {
	return Policy{singleConnection: true}
}

func Prefer(
	policy Policy,
	name string,
	match func(info Info, candidate endpoint.Info) bool,
) Policy {
	policy.preferences = append(slices.Clone(policy.preferences), preference{
		name:  name,
		match: match,
	})

	return policy
}

func PreferNearestDC(
	policy Policy,
	name string,
	match func(info Info, candidate endpoint.Info) bool,
) Policy {
	policy = Prefer(policy, name, match)
	policy.detectNearestDC = true

	return policy
}

// Prioritize applies preferences from the outermost constructor to the innermost constructor.
func (p Policy) Prioritize(info Info, endpoints []endpoint.Endpoint) []EndpointPriority {
	priorities := make([]EndpointPriority, len(endpoints))
	for i, candidate := range endpoints {
		priorities[i] = EndpointPriority{Key: candidate.Key()}
	}
	for i := len(p.preferences) - 1; i >= 0; i-- {
		applyPreference(info, endpoints, priorities, p.preferences[i])
	}

	return priorities
}

func applyPreference(
	info Info,
	endpoints []endpoint.Endpoint,
	priorities []EndpointPriority,
	preference preference,
) {
	for i, candidate := range endpoints {
		if priorities[i].Priority > math.MaxUint64>>1 {
			priorities[i].Priority = math.MaxUint64

			continue
		}
		priorities[i].Priority <<= 1
		if !preference.match(info, candidate) {
			priorities[i].Priority++
		}
	}
}

// SingleConnection reports whether the balancer must use only the configured entrypoint.
func (p Policy) SingleConnection() bool {
	return p.singleConnection
}

// DetectsNearestDC reports whether the balancer needs client-side nearest DC detection.
func (p Policy) DetectsNearestDC() bool {
	return p.detectNearestDC
}

func (p Policy) String() string {
	mode := "Priority"
	if p.singleConnection {
		mode = "SingleConn"
	}
	if len(p.preferences) == 0 {
		return mode
	}

	names := make([]string, len(p.preferences))
	for i := range p.preferences {
		names[len(p.preferences)-1-i] = p.preferences[i].name
	}

	return fmt.Sprintf("%s{Preferences=[%s]}", mode, strings.Join(names, ","))
}
