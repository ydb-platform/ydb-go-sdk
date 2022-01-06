package trace

import (
	"sort"
	"strings"
)

type Details uint64

const (
	DriverSystemEvents      Details = 1 << iota // 1
	DriverClusterEvents                         // 2
	DriverNetEvents                             // 4
	DriverCoreEvents                            // 8
	DriverCredentialsEvents                     // 16
	DriverDiscoveryEvents                       // 32

	TableSessionLifeCycleEvents     // 64
	TableSessionQueryInvokeEvents   // 128
	TableSessionQueryStreamEvents   // 256
	TableSessionTransactionEvents   // 512
	TablePoolLifeCycleEvents        // 1024
	TablePoolRetryEvents            // 2048
	TablePoolSessionLifeCycleEvents // 4096
	TablePoolAPIEvents              // 8192

	DriverConnEvents = DriverNetEvents |
		DriverCoreEvents // 12
	TableSessionQueryEvents = TableSessionQueryInvokeEvents |
		TableSessionQueryStreamEvents // 384
	TableSessionEvents = TableSessionLifeCycleEvents |
		TableSessionQueryEvents |
		TableSessionTransactionEvents // 960
	TablePoolEvents = TablePoolLifeCycleEvents |
		TablePoolRetryEvents |
		TablePoolSessionLifeCycleEvents |
		TablePoolAPIEvents // 15360
	DetailsAll = ^Details(0) // 18446744073709551615
)

var (
	detailsToString = map[Details]string{
		DriverSystemEvents:              "DriverSystemEvents",
		DriverClusterEvents:             "DriverClusterEvents",
		DriverNetEvents:                 "DriverNetEvents",
		DriverCoreEvents:                "DriverCoreEvents",
		DriverCredentialsEvents:         "DriverCredentialsEvents",
		DriverDiscoveryEvents:           "DriverDiscoveryEvents",
		TableSessionLifeCycleEvents:     "TableSessionLifeCycleEvents",
		TableSessionQueryInvokeEvents:   "TableSessionQueryInvokeEvents",
		TableSessionQueryStreamEvents:   "TableSessionQueryStreamEvents",
		TableSessionTransactionEvents:   "TableSessionTransactionEvents",
		TablePoolLifeCycleEvents:        "TablePoolLifeCycleEvents",
		TablePoolRetryEvents:            "TablePoolRetryEvents",
		TablePoolSessionLifeCycleEvents: "TablePoolSessionLifeCycleEvents",
		TablePoolAPIEvents:              "TablePoolAPIEvents",
	}
	stringToDetails = map[string]Details{
		"DriverSystemEvents":              DriverSystemEvents,
		"DriverClusterEvents":             DriverClusterEvents,
		"DriverNetEvents":                 DriverNetEvents,
		"DriverCoreEvents":                DriverCoreEvents,
		"DriverCredentialsEvents":         DriverCredentialsEvents,
		"DriverDiscoveryEvents":           DriverDiscoveryEvents,
		"TableSessionLifeCycleEvents":     TableSessionLifeCycleEvents,
		"TableSessionQueryInvokeEvents":   TableSessionQueryInvokeEvents,
		"TableSessionQueryStreamEvents":   TableSessionQueryStreamEvents,
		"TableSessionTransactionEvents":   TableSessionTransactionEvents,
		"TablePoolLifeCycleEvents":        TablePoolLifeCycleEvents,
		"TablePoolRetryEvents":            TablePoolRetryEvents,
		"TablePoolSessionLifeCycleEvents": TablePoolSessionLifeCycleEvents,
		"TablePoolAPIEvents":              TablePoolAPIEvents,
	}
	maskDetails = DriverSystemEvents |
		DriverClusterEvents |
		DriverNetEvents |
		DriverCoreEvents |
		DriverCredentialsEvents |
		DriverDiscoveryEvents |
		TableSessionLifeCycleEvents |
		TableSessionQueryInvokeEvents |
		TableSessionQueryStreamEvents |
		TableSessionTransactionEvents |
		TablePoolLifeCycleEvents |
		TablePoolRetryEvents |
		TablePoolSessionLifeCycleEvents |
		TablePoolAPIEvents
)

func DetailsFromString(s string) (d Details) {
	if len(s) == 0 {
		return 0
	}
	if dd, ok := stringToDetails[s]; ok {
		return dd
	}
	s = strings.Trim(s, "[]")
	ss := strings.Split(s, ",")
	for _, sss := range ss {
		if v, ok := stringToDetails[sss]; ok {
			d |= v
		}
	}
	return d
}

func (d Details) String() string {
	if s, ok := detailsToString[d]; ok {
		return s
	}
	var ss []string
	for k, v := range detailsToString {
		if d&k != 0 {
			ss = append(ss, v)
		}
	}
	sort.Strings(ss)
	return "[" + strings.Join(ss, ",") + "]"
}
