package strategy

import (
	"fmt"
	"slices"

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
	estimates := p.child.Estimate(info, endpoints)
	preferred, fallback := partitionEstimates(endpoints, estimates, p.match, info)
	if !p.allowFallback {
		return preferred
	}

	return shiftFallbackPenalties(preferred, fallback)
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

func partitionEstimates(
	endpoints []endpoint.Endpoint,
	estimates []Estimation,
	match func(info Info, candidate endpoint.Info) bool,
	info Info,
) (preferred, fallback []Estimation) {
	if match == nil {
		return estimates, nil
	}

	preferredKeys := make(map[endpoint.Key]struct{}, len(endpoints))
	for _, candidate := range endpoints {
		if match(info, candidate) {
			preferredKeys[candidate.Key()] = struct{}{}
		}
	}

	preferred = make([]Estimation, 0, len(estimates))
	fallback = make([]Estimation, 0, len(estimates))
	for _, estimation := range estimates {
		if _, ok := preferredKeys[estimation.Key]; ok {
			preferred = append(preferred, estimation)
		} else {
			fallback = append(fallback, estimation)
		}
	}

	return preferred, fallback
}

func shiftFallbackPenalties(preferred, fallback []Estimation) []Estimation {
	result := make([]Estimation, 0, len(preferred)+len(fallback))
	result = append(result, preferred...)
	result = append(result, fallback...)
	if len(preferred) == 0 || len(fallback) == 0 {
		return result
	}

	penalties := make([]uint64, 0, len(preferred)+len(fallback))
	for _, estimations := range [][]Estimation{preferred, fallback} {
		for _, estimation := range estimations {
			penalties = append(penalties, estimation.Penalty)
		}
	}
	slices.Sort(penalties)
	penalties = compactPenalties(penalties)
	ranks := make(map[uint64]uint64, len(penalties))
	for rank, penalty := range penalties {
		ranks[penalty] = uint64(rank)
	}

	var maximumPreferred uint64
	for i := range preferred {
		result[i].Penalty = ranks[result[i].Penalty]
		maximumPreferred = max(maximumPreferred, result[i].Penalty)
	}
	shift := maximumPreferred + 1
	for i := len(preferred); i < len(result); i++ {
		result[i].Penalty = ranks[result[i].Penalty] + shift
	}

	return result
}

func compactPenalties(penalties []uint64) []uint64 {
	if len(penalties) == 0 {
		return nil
	}

	result := penalties[:1]
	for _, penalty := range penalties[1:] {
		if penalty != result[len(result)-1] {
			result = append(result, penalty)
		}
	}

	return result
}
