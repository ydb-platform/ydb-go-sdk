package trace

import (
	"regexp"
)

type Details uint64

const (
	DriverNetEvents         Details = 1 << iota // 1
	DriverResolverEvents                        // 2
	DriverClusterEvents                         // 4
	DriverCoreEvents                            // 8
	DriverCredentialsEvents                     // 16

	DiscoveryEvents // 32

	TableSessionLifeCycleEvents     // 64
	TableSessionQueryInvokeEvents   // 128
	TableSessionQueryStreamEvents   // 256
	TableSessionTransactionEvents   // 512
	TablePoolLifeCycleEvents        // 1024
	TablePoolRetryEvents            // 2048
	TablePoolSessionLifeCycleEvents // 4096
	TablePoolAPIEvents              // 8192

	RetryEvents // 16384

	SchemeEvents // 32768

	ScriptingEvents // 65536

	RatelimiterEvents // 131072

	CoordinationEvents // 262144

	DriverEvents = DriverClusterEvents |
		DriverNetEvents |
		DriverResolverEvents |
		DriverCoreEvents |
		DriverCredentialsEvents // 63

	TableEvents = TableSessionLifeCycleEvents |
		TableSessionQueryInvokeEvents |
		TableSessionQueryStreamEvents |
		TableSessionTransactionEvents |
		TablePoolLifeCycleEvents |
		TablePoolRetryEvents |
		TablePoolSessionLifeCycleEvents |
		TablePoolAPIEvents // 16320

	TablePoolEvents = TablePoolLifeCycleEvents |
		TablePoolRetryEvents |
		TablePoolSessionLifeCycleEvents |
		TablePoolAPIEvents // 16320

	DriverConnEvents = DriverNetEvents |
		DriverResolverEvents |
		DriverCoreEvents // 28

	TableSessionQueryEvents = TableSessionQueryInvokeEvents |
		TableSessionQueryStreamEvents // 384
	TableSessionEvents = TableSessionLifeCycleEvents |
		TableSessionQueryEvents |
		TableSessionTransactionEvents // 960

	DetailsAll = ^Details(0) // 18446744073709551615
)

var (
	details = map[Details]string{
		DriverEvents:            "ydb.driver",
		DriverClusterEvents:     "ydb.driver.cluster",
		DriverNetEvents:         "ydb.driver.net",
		DriverResolverEvents:    "ydb.driver.resolver",
		DriverCoreEvents:        "ydb.driver.core",
		DriverCredentialsEvents: "ydb.driver.credentials",

		DiscoveryEvents: "ydb.discovery",

		RetryEvents: "ydb.retry",

		SchemeEvents: "ydb.scheme",

		ScriptingEvents: "ydb.scripting",

		CoordinationEvents: "ydb.coordination",

		RatelimiterEvents: "ydb.ratelimiter",

		TableEvents:                     "ydb.table",
		TableSessionLifeCycleEvents:     "ydb.table.session",
		TableSessionQueryInvokeEvents:   "ydb.table.session.query.invoke",
		TableSessionQueryStreamEvents:   "ydb.table.session.query.stream",
		TableSessionTransactionEvents:   "ydb.table.session.tx",
		TablePoolLifeCycleEvents:        "ydb.table.pool",
		TablePoolRetryEvents:            "ydb.table.pool.retry",
		TablePoolSessionLifeCycleEvents: "ydb.table.pool.session",
		TablePoolAPIEvents:              "ydb.table.pool.api",
	}
	defaultDetails = DetailsAll
)

type matchDetailsOptionsHolder struct {
	defaultDetails Details
	posixMatch     bool
}

type matchDetailsOption func(h *matchDetailsOptionsHolder)

func WithDefaultDetails(defaultDetails Details) matchDetailsOption {
	return func(h *matchDetailsOptionsHolder) {
		h.defaultDetails = defaultDetails
	}
}

func WithPOSIXMatch() matchDetailsOption {
	return func(h *matchDetailsOptionsHolder) {
		h.posixMatch = true
	}
}

func MatchDetails(pattern string, opts ...matchDetailsOption) (d Details) {
	var (
		h = &matchDetailsOptionsHolder{
			defaultDetails: defaultDetails,
		}
		re  *regexp.Regexp
		err error
	)

	for _, o := range opts {
		o(h)
	}
	if h.posixMatch {
		re, err = regexp.CompilePOSIX(pattern)
	} else {
		re, err = regexp.Compile(pattern)
	}
	if err != nil {
		return h.defaultDetails
	}
	for k, v := range details {
		if re.MatchString(v) {
			d |= k
		}
	}
	if d == 0 {
		return h.defaultDetails
	}
	return d
}
