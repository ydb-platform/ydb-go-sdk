package strategy

import (
	"fmt"
	"math"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

type prefer struct {
	child         Estimator
	name          string
	match         func(info Info, candidate endpoint.Info) bool
	allowFallback bool
}

type nearestDC struct {
	child Estimator
}

func (p prefer) Estimate(info Info, endpoints []endpoint.Endpoint) []Estimation {
	preferred, fallback := partitionEndpoints(endpoints, p.match, info)
	result := p.child.Estimate(info, preferred)
	if !p.allowFallback {
		return result
	}

	fallbackEstimates := p.child.Estimate(info, fallback)
	shift := fallbackPenaltyShift(result)
	for i := range fallbackEstimates {
		fallbackEstimates[i].Penalty = addPenalty(fallbackEstimates[i].Penalty, shift)
	}

	return append(result, fallbackEstimates...)
}

func (p prefer) String() string {
	return fmt.Sprintf("Prefer{Filter=%s,AllowFallback=%t,Child=%s}",
		p.name, p.allowFallback, p.child.String(),
	)
}

func (n nearestDC) Estimate(info Info, endpoints []endpoint.Endpoint) []Estimation {
	return n.child.Estimate(info, endpoints)
}

func (n nearestDC) String() string {
	return n.child.String()
}

func partitionEndpoints(
	endpoints []endpoint.Endpoint,
	match func(info Info, candidate endpoint.Info) bool,
	info Info,
) (preferred, fallback []endpoint.Endpoint) {
	if match == nil {
		return endpoints, nil
	}

	preferred = make([]endpoint.Endpoint, 0, len(endpoints))
	fallback = make([]endpoint.Endpoint, 0, len(endpoints))
	for _, candidate := range endpoints {
		if match(info, candidate) {
			preferred = append(preferred, candidate)
		} else {
			fallback = append(fallback, candidate)
		}
	}

	return preferred, fallback
}

func fallbackPenaltyShift(preferred []Estimation) uint64 {
	if len(preferred) == 0 {
		return 0
	}

	var maximum uint64
	for _, estimation := range preferred {
		maximum = max(maximum, estimation.Penalty)
	}
	if maximum == math.MaxUint64 {
		return maximum
	}

	return maximum + 1
}

func addPenalty(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}

	return left + right
}
