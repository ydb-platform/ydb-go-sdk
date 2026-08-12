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
	// Excluded prevents the endpoint from participating in policy-based selection.
	Excluded bool
}

// Info contains immutable data shared by a policy during one discovery refresh.
type Info struct {
	SelfLocation string
}

type preference struct {
	name          string
	match         func(info Info, candidate endpoint.Info) bool
	allowFallback bool
}

type preferenceDecision uint8

const (
	preferencePreferred preferenceDecision = iota
	preferenceFallback
	preferenceExcluded
)

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
	return addPreference(policy, name, match, false)
}

func PreferWithFallback(
	policy Policy,
	name string,
	match func(info Info, candidate endpoint.Info) bool,
) Policy {
	return addPreference(policy, name, match, true)
}

func addPreference(
	policy Policy,
	name string,
	match func(info Info, candidate endpoint.Info) bool,
	allowFallback bool,
) Policy {
	policy.preferences = append(slices.Clone(policy.preferences), preference{
		name:          name,
		match:         match,
		allowFallback: allowFallback,
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

func PreferNearestDCWithFallback(
	policy Policy,
	name string,
	match func(info Info, candidate endpoint.Info) bool,
) Policy {
	policy = PreferWithFallback(policy, name, match)
	policy.detectNearestDC = true

	return policy
}

// Prioritize assigns one priority bit per preference in pipeline construction order.
// Outer constructors are appended later and therefore receive more significant bits.
func (p Policy) Prioritize(info Info, endpoints []endpoint.Endpoint) []EndpointPriority {
	priorities := make([]EndpointPriority, len(endpoints))
	for i, candidate := range endpoints {
		priorities[i] = EndpointPriority{Key: candidate.Key()}
	}
	for index, preference := range p.preferences {
		applyPreference(info, endpoints, priorities, preference, index)
	}

	return priorities
}

func applyPreference(
	info Info,
	endpoints []endpoint.Endpoint,
	priorities []EndpointPriority,
	preference preference,
	index int,
) {
	for i, candidate := range endpoints {
		if priorities[i].Excluded {
			continue
		}

		switch preference.decide(info, candidate) {
		case preferencePreferred:
		case preferenceFallback:
			if index >= 64 {
				priorities[i].Priority = math.MaxUint64
			} else {
				priorities[i].Priority |= uint64(1) << index
			}
		case preferenceExcluded:
			priorities[i].Excluded = true
		}
	}
}

func (p preference) decide(info Info, candidate endpoint.Info) preferenceDecision {
	if p.match(info, candidate) {
		return preferencePreferred
	}
	if p.allowFallback {
		return preferenceFallback
	}

	return preferenceExcluded
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
