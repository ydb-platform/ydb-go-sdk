package trace

import (
	"testing"
)

func TestDetailsString(t *testing.T) {
	for _, test := range []struct {
		details Details
		exp     string
	}{
		{
			details: DriverSystemEvents,
			exp:     "DriverSystemEvents",
		},
		{
			details: DriverClusterEvents,
			exp:     "DriverClusterEvents",
		},
		{
			details: DriverNetEvents,
			exp:     "DriverNetEvents",
		},
		{
			details: DriverCoreEvents,
			exp:     "DriverCoreEvents",
		},
		{
			details: DriverCredentialsEvents,
			exp:     "DriverCredentialsEvents",
		},
		{
			details: DriverDiscoveryEvents,
			exp:     "DriverDiscoveryEvents",
		},
		{
			details: TableSessionLifeCycleEvents,
			exp:     "TableSessionLifeCycleEvents",
		},
		{
			details: TableSessionQueryInvokeEvents,
			exp:     "TableSessionQueryInvokeEvents",
		},
		{
			details: TableSessionQueryStreamEvents,
			exp:     "TableSessionQueryStreamEvents",
		},
		{
			details: TableSessionTransactionEvents,
			exp:     "TableSessionTransactionEvents",
		},
		{
			details: TablePoolLifeCycleEvents,
			exp:     "TablePoolLifeCycleEvents",
		},
		{
			details: TablePoolRetryEvents,
			exp:     "TablePoolRetryEvents",
		},
		{
			details: TablePoolSessionLifeCycleEvents,
			exp:     "TablePoolSessionLifeCycleEvents",
		},
		{
			details: TablePoolAPIEvents,
			exp:     "TablePoolAPIEvents",
		},
		{
			details: DriverSystemEvents | DriverCoreEvents,
			exp:     "DriverCoreEvents|DriverSystemEvents",
		},
		{
			details: DriverClusterEvents | TablePoolAPIEvents,
			exp:     "DriverClusterEvents|TablePoolAPIEvents",
		},
		{
			details: DriverSystemEvents | DriverClusterEvents | DriverCoreEvents | TablePoolEvents,
			exp: "DriverClusterEvents|DriverCoreEvents|DriverSystemEvents|TablePoolAPIEvents|" +
				"TablePoolLifeCycleEvents|TablePoolRetryEvents|TablePoolSessionLifeCycleEvents",
		},
	} {
		t.Run(test.exp, func(t *testing.T) {
			if test.details.String() != test.exp {
				t.Fatalf("unexpected %d serialize to string, act %s, exp %s", test.details, test.details.String(), test.exp)
			}
		})
	}
}

func TestDetailsFromString(t *testing.T) {
	for _, test := range []struct {
		exp Details
		s   string
	}{
		{
			s:   "DriverSystemEvents",
			exp: DriverSystemEvents,
		},
		{
			s:   "DriverClusterEvents",
			exp: DriverClusterEvents,
		},
		{
			s:   "DriverNetEvents",
			exp: DriverNetEvents,
		},
		{
			s:   "DriverCoreEvents",
			exp: DriverCoreEvents,
		},
		{
			s:   "DriverCredentialsEvents",
			exp: DriverCredentialsEvents,
		},
		{
			s:   "DriverDiscoveryEvents",
			exp: DriverDiscoveryEvents,
		},
		{
			s:   "TableSessionLifeCycleEvents",
			exp: TableSessionLifeCycleEvents,
		},
		{
			s:   "TableSessionQueryInvokeEvents",
			exp: TableSessionQueryInvokeEvents,
		},
		{
			s:   "TableSessionQueryStreamEvents",
			exp: TableSessionQueryStreamEvents,
		},
		{
			s:   "TableSessionTransactionEvents",
			exp: TableSessionTransactionEvents,
		},
		{
			s:   "TablePoolLifeCycleEvents",
			exp: TablePoolLifeCycleEvents,
		},
		{
			s:   "TablePoolRetryEvents",
			exp: TablePoolRetryEvents,
		},
		{
			s:   "TablePoolSessionLifeCycleEvents",
			exp: TablePoolSessionLifeCycleEvents,
		},
		{
			s:   "TablePoolAPIEvents",
			exp: TablePoolAPIEvents,
		},
		{
			s:   "DriverCoreEvents|DriverSystemEvents",
			exp: DriverSystemEvents | DriverCoreEvents,
		},
		{
			s:   "DriverClusterEvents|TablePoolAPIEvents",
			exp: DriverClusterEvents | TablePoolAPIEvents,
		},
		{
			s: "DriverClusterEvents|DriverCoreEvents|DriverSystemEvents|TablePoolAPIEvents|" +
				"TablePoolLifeCycleEvents|TablePoolRetryEvents|TablePoolSessionLifeCycleEvents",
			exp: DriverSystemEvents | DriverClusterEvents | DriverCoreEvents | TablePoolEvents,
		},
	} {
		t.Run(test.s, func(t *testing.T) {
			if DetailsFromString(test.s) != test.exp {
				t.Fatalf("unexpected %s deserialize to Details, act %d, exp %d", test.s, DetailsFromString(test.s), test.exp)
			}
		})
	}
}

func TestDetailsToStringToDetails(t *testing.T) {
	for i := 0; i < int(maskDetails); i++ {
		d := Details(i)
		s := d.String()
		t.Run(s, func(t *testing.T) {
			dd := DetailsFromString(s)
			if dd != d {
				t.Fatalf("unexpected serialize-deserialize, act %d, exp %d, intermediate string %s", dd, d, s)
			}
		})
	}
}
