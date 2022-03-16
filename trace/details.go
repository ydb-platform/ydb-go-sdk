package trace

import (
	"regexp"
)

type Details uint64

const (
	DriverNetEvents         Details = 1 << iota // 1
	DriverConnEvents                            // 2
	DriverClusterEvents                         // 4
	DriverResolverEvents                        // 8
	DriverRepeaterEvents                        // 16
	DriverCredentialsEvents                     // 32

	TableSessionLifeCycleEvents     // 64
	TableSessionQueryInvokeEvents   // 128
	TableSessionQueryStreamEvents   // 256
	TableSessionTransactionEvents   // 512
	TablePoolLifeCycleEvents        // 1024
	TablePoolSessionLifeCycleEvents // 2048
	TablePoolAPIEvents              // 4096

	RetryEvents // 8192

	DiscoveryEvents // 16384

	SchemeEvents // 32768

	ScriptingEvents // 65536

	RatelimiterEvents // 131072

	CoordinationEvents // 262144

	DriverEvents = DriverNetEvents |
		DriverConnEvents |
		DriverClusterEvents |
		DriverResolverEvents |
		DriverRepeaterEvents |
		DriverCredentialsEvents // 63

	TableEvents = TableSessionLifeCycleEvents |
		TableSessionQueryInvokeEvents |
		TableSessionQueryStreamEvents |
		TableSessionTransactionEvents |
		TablePoolLifeCycleEvents |
		TablePoolSessionLifeCycleEvents |
		TablePoolAPIEvents // 16320

	TablePoolEvents = TablePoolLifeCycleEvents |
		TablePoolSessionLifeCycleEvents |
		TablePoolAPIEvents // 15360

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
		DriverRepeaterEvents:    "ydb.driver.repeater",
		DriverConnEvents:        "ydb.driver.conn",
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
