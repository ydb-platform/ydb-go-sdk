package trace

type Details int

const (
	DriverClusterEvents = Details(1 << iota)
	DriverNetEvents
	DriverCoreEvents
	DriverCredentialsEvents
	DriverDiscoveryEvents

	TableSessionLifeCycleEvents
	TableSessionQueryInvokeEvents
	TableSessionQueryStreamEvents
	TableSessionTransactionEvents
	TablePoolLifeCycleEvents
	TablePoolRetryEvents
	TablePoolSessionLifeCycleEvents
	TablePoolAPIEvents

	TableSessionQueryEvents = TableSessionQueryInvokeEvents | TableSessionQueryStreamEvents
	TableSessionEvents      = TableSessionLifeCycleEvents | TableSessionQueryEvents | TableSessionTransactionEvents
	TablePoolEvents         = TablePoolLifeCycleEvents | TablePoolRetryEvents | TablePoolSessionLifeCycleEvents | TablePoolAPIEvents
	DetailsAll              = ^Details(0)
)
