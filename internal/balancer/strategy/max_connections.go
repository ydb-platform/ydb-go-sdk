package strategy

import (
	"fmt"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

type maxConnections struct {
	child Estimator
	limit int
}

// WithMaxConnections decorates an estimator with an active-connection soft limit.
// It is internal until the user-facing option and its serialization are designed.
func WithMaxConnections(child Estimator, limit int) Estimator {
	return maxConnections{
		child: normalize(child),
		limit: max(0, limit),
	}
}

func (m maxConnections) Estimate(info Info, endpoints []endpoint.Endpoint) []Estimation {
	return m.child.Estimate(info, endpoints)
}

func (m maxConnections) String() string {
	return fmt.Sprintf("MaxConnections{Limit=%d,Child=%s}", m.limit, m.child.String())
}

func (m maxConnections) compile() Plan {
	plan := compile(m.child)
	plan.estimator = m
	if plan.maxConnections == 0 || (m.limit > 0 && m.limit < plan.maxConnections) {
		plan.maxConnections = m.limit
	}

	return plan
}

func selectActiveEstimates(info Info, estimates []Estimation, limit int) []Estimation {
	if limit <= 0 || len(estimates) <= limit {
		return estimates
	}

	eligibleCount := 0
	for _, estimation := range estimates {
		if estimation.Weight > 0 {
			eligibleCount++
		}
	}

	banned := make(map[endpoint.Key]struct{}, len(info.PreviousActive))
	previousOrder := make(map[endpoint.Key]int, len(info.PreviousActive))
	for index, previous := range info.PreviousActive {
		previousOrder[previous.Key] = index
		if previous.Banned {
			banned[previous.Key] = struct{}{}
		}
	}

	nonBanned := make([]Estimation, 0, eligibleCount)
	bannedEstimates := make([]Estimation, 0, len(banned))
	for _, estimation := range estimates {
		if estimation.Weight == 0 {
			continue
		}
		if _, ok := banned[estimation.Key]; ok {
			bannedEstimates = append(bannedEstimates, estimation)
		} else {
			nonBanned = append(nonBanned, estimation)
		}
	}

	sortByPenaltyAndPreviousOrder(nonBanned, previousOrder)
	sortByPenaltyAndPreviousOrder(bannedEstimates, previousOrder)
	shuffleEqualPenaltyRuns(info, nonBanned, previousOrder)
	shuffleEqualPenaltyRuns(info, bannedEstimates, previousOrder)
	result := append(make([]Estimation, 0, limit), nonBanned[:min(limit, len(nonBanned))]...)
	if len(result) < limit {
		result = append(result, bannedEstimates[:min(limit-len(result), len(bannedEstimates))]...)
	}

	return result
}

func sortByPenaltyAndPreviousOrder(estimates []Estimation, previousOrder map[endpoint.Key]int) {
	sort.SliceStable(estimates, func(i, j int) bool {
		if estimates[i].Penalty != estimates[j].Penalty {
			return estimates[i].Penalty < estimates[j].Penalty
		}
		left, leftOK := previousOrder[estimates[i].Key]
		right, rightOK := previousOrder[estimates[j].Key]
		if leftOK != rightOK {
			return leftOK
		}
		if leftOK {
			return left < right
		}

		return false
	})
}

func shuffleEqualPenaltyRuns(info Info, estimates []Estimation, previousOrder map[endpoint.Key]int) {
	if info.Rand == nil {
		return
	}

	for start := 0; start < len(estimates); {
		end := start + 1
		for end < len(estimates) && estimates[end].Penalty == estimates[start].Penalty {
			end++
		}
		firstNew := start
		for firstNew < end {
			if _, sticky := previousOrder[estimates[firstNew].Key]; !sticky {
				break
			}
			firstNew++
		}
		info.Rand.Shuffle(end-firstNew, func(i, j int) {
			estimates[firstNew+i], estimates[firstNew+j] = estimates[firstNew+j], estimates[firstNew+i]
		})
		start = end
	}
}
