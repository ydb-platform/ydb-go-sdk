package trace

import "testing"

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
			exp:     "[DriverSystemEvents,DriverCoreEvents]",
		},
		{
			details: DriverClusterEvents | TablePoolAPIEvents,
			exp:     "[DriverClusterEvents,TablePoolAPIEvents]",
		},
		{
			details: DriverSystemEvents | DriverClusterEvents | DriverCoreEvents | TablePoolEvents,
			exp: "[DriverSystemEvents,DriverClusterEvents,DriverCoreEvents,TablePoolLifeCycleEvents," +
				"TablePoolRetryEvents,TablePoolSessionLifeCycleEvents,TablePoolAPIEvents]",
		},
	} {
		if test.details.String() != test.exp {
			t.Fatalf("unexpected %d serialize to string: %s, exp %s", test.details, test.details.String(), test.exp)
		}
	}
}
