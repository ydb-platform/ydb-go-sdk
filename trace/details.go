package trace

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
