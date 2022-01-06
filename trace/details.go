package trace

import (
	"bytes"
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

func (d Details) knownDetailsString(collectedDetails bool) string {
	switch d {
	case DriverSystemEvents:
		return "DriverSystemEvents"
	case DriverClusterEvents:
		return "DriverClusterEvents"
	case DriverNetEvents:
		return "DriverNetEvents"
	case DriverCoreEvents:
		return "DriverCoreEvents"
	case DriverCredentialsEvents:
		return "DriverCredentialsEvents"
	case DriverDiscoveryEvents:
		return "DriverDiscoveryEvents"
	case TableSessionLifeCycleEvents:
		return "TableSessionLifeCycleEvents"
	case TableSessionQueryInvokeEvents:
		return "TableSessionQueryInvokeEvents"
	case TableSessionQueryStreamEvents:
		return "TableSessionQueryStreamEvents"
	case TableSessionTransactionEvents:
		return "TableSessionTransactionEvents"
	case TablePoolLifeCycleEvents:
		return "TablePoolLifeCycleEvents"
	case TablePoolRetryEvents:
		return "TablePoolRetryEvents"
	case TablePoolSessionLifeCycleEvents:
		return "TablePoolSessionLifeCycleEvents"
	case TablePoolAPIEvents:
		return "TablePoolAPIEvents"
	case DriverConnEvents:
		if collectedDetails {
			return "DriverConnEvents"
		}
	case TableSessionQueryEvents:
		if collectedDetails {
			return "TableSessionQueryEvents"
		}
	case TableSessionEvents:
		if collectedDetails {
			return "TableSessionEvents"
		}
	case TablePoolEvents:
		if collectedDetails {
			return "TablePoolEvents"
		}
	case DetailsAll:
		if collectedDetails {
			return "DetailsAll"
		}
	}
	return ""
}

func (d Details) String() (s string) {
	if s = d.knownDetailsString(true); s != "" {
		return s
	}
	var buf bytes.Buffer
	for _, v := range []Details{
		DriverSystemEvents,
		DriverClusterEvents,
		DriverNetEvents,
		DriverCoreEvents,
		DriverCredentialsEvents,
		DriverDiscoveryEvents,
		TableSessionLifeCycleEvents,
		TableSessionQueryInvokeEvents,
		TableSessionQueryStreamEvents,
		TableSessionTransactionEvents,
		TablePoolLifeCycleEvents,
		TablePoolRetryEvents,
		TablePoolSessionLifeCycleEvents,
		TablePoolAPIEvents,
	} {
		if d&v != 0 {
			if buf.Len() != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(v.knownDetailsString(false))
		}
	}
	return "[" + buf.String() + "]"
}
