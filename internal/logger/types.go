package logger

type Details int

const (
	DriverClusterEvents = Details(1 << iota)
	driverNetEvents
	DriverCoreEvents
	DriverCredentialsEvents
	DriverDiscoveryEvents

	tableSessionEvents
	tableSessionQueryInvokeEvents
	tableSessionQueryStreamEvents
	tableSessionTransactionEvents
	tablePoolLifeCycleEvents
	tablePoolRetryEvents
	tablePoolSessionLifeCycleEvents
	tablePoolAPIEvents

	DriverConnEvents        = driverNetEvents | DriverCoreEvents
	tableSessionQueryEvents = tableSessionQueryInvokeEvents | tableSessionQueryStreamEvents
	TableSessionEvents      = tableSessionEvents | tableSessionQueryEvents | tableSessionTransactionEvents
	TablePoolEvents         = tablePoolLifeCycleEvents | tablePoolRetryEvents | tablePoolSessionLifeCycleEvents | tablePoolAPIEvents
	DetailsAll              = ^Details(0)
)
