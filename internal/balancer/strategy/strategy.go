package strategy

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

const defaultWeight uint64 = 1

// Estimation describes endpoint selection policy independent of connection state.
type Estimation struct {
	Key     endpoint.Key
	Penalty uint64
	Weight  uint64
}

// Estimator is an immutable, composable endpoint estimation policy.
// Connection ownership, health penalties, and discovery lifecycle remain outside the estimator tree.
type Estimator interface {
	Estimate(info Info, endpoints []endpoint.Endpoint) []Estimation
	String() string
}

// Info contains immutable data shared by estimators during one discovery refresh.
type Info struct {
	SelfLocation string
}

func RandomChoice() Estimator {
	return randomChoice{}
}

func SingleConn() Estimator {
	return singleConn{}
}

func Prefer(
	child Estimator,
	name string,
	match func(info Info, candidate endpoint.Info) bool,
	allowFallback bool,
) Estimator {
	return prefer{
		child:         normalize(child),
		name:          name,
		match:         match,
		allowFallback: allowFallback,
	}
}

func PreferNearestDC(
	child Estimator,
	name string,
	match func(info Info, candidate endpoint.Info) bool,
	allowFallback bool,
) Estimator {
	return nearestDC{child: Prefer(child, name, match, allowFallback)}
}

func normalize(estimator Estimator) Estimator {
	if estimator == nil {
		return RandomChoice()
	}

	return estimator
}

// UsesConfiguredEndpoint reports whether the estimator ultimately selects only the configured entrypoint.
func UsesConfiguredEndpoint(estimator Estimator) bool {
	switch current := normalize(estimator).(type) {
	case singleConn:
		return true
	case prefer:
		return UsesConfiguredEndpoint(current.child)
	case nearestDC:
		return UsesConfiguredEndpoint(current.child)
	default:
		return false
	}
}

// DetectsNearestDC reports whether the estimator needs client-side nearest DC detection.
func DetectsNearestDC(estimator Estimator) bool {
	switch current := normalize(estimator).(type) {
	case prefer:
		return DetectsNearestDC(current.child)
	case nearestDC:
		return true
	default:
		return false
	}
}

type randomChoice struct{}

func (randomChoice) Estimate(_ Info, endpoints []endpoint.Endpoint) []Estimation {
	result := make([]Estimation, len(endpoints))
	for i, candidate := range endpoints {
		result[i] = Estimation{Key: candidate.Key(), Weight: defaultWeight}
	}

	return result
}

func (randomChoice) String() string {
	return "RandomChoice"
}

type singleConn struct{}

func (singleConn) Estimate(_ Info, endpoints []endpoint.Endpoint) []Estimation {
	return randomChoice{}.Estimate(Info{}, endpoints)
}

func (singleConn) String() string {
	return "SingleConn"
}
