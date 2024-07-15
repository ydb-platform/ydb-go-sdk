package endpoint

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xmath"
)

func Diff(previous, newest []Endpoint) (steady, added, dropped []Endpoint) {
	steady = make([]Endpoint, 0, xmath.Min(len(newest), len(previous)))
	added = make([]Endpoint, 0, len(newest))
	dropped = make([]Endpoint, 0, len(previous))

	var (
		newestMap   = make(map[string]struct{}, len(newest))
		previousMap = make(map[string]struct{}, len(previous))
	)

	sort.Slice(newest, func(i, j int) bool {
		return newest[i].Address() < newest[j].Address()
	})
	sort.Slice(previous, func(i, j int) bool {
		return previous[i].Address() < previous[j].Address()
	})

	for _, e := range previous {
		previousMap[e.Address()] = struct{}{}
	}

	for _, e := range newest {
		newestMap[e.Address()] = struct{}{}
		if _, has := previousMap[e.Address()]; !has {
			added = append(added, e.Copy())
		}
	}

	for _, e := range previous {
		if _, has := newestMap[e.Address()]; !has {
			dropped = append(dropped, e.Copy())
		} else {
			steady = append(steady, e.Copy())
		}
	}

	return steady, added, dropped
}
