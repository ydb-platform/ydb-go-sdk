package trace

import (
	"regexp"
)

type Details uint64

const (
	DriverNetEvents Details = 1 << iota // for bitmask: 1, 2, 4, 8, 16, 32, ...
	DriverConnEvents
	DriverBalancerEvents
	DriverResolverEvents
	DriverRepeaterEvents
	DriverCredentialsEvents

	TableSessionLifeCycleEvents
	TableSessionQueryInvokeEvents
	TableSessionQueryStreamEvents
	TableSessionTransactionEvents
	TablePoolLifeCycleEvents
	TablePoolSessionLifeCycleEvents
	TablePoolAPIEvents

	TopicControlPlaneEvents

	TopicReaderStreamLifeCycleEvents
	TopicReaderStreamEvents
	TopicReaderMessageEvents
	TopicReaderPartitionEvents

	DatabaseSQLConnectorEvents
	DatabaseSQLConnEvents
	DatabaseSQLTxEvents
	DatabaseSQLStmtEvents

	RetryEvents

	DiscoveryEvents

	SchemeEvents

	ScriptingEvents

	RatelimiterEvents

	CoordinationEvents

	// Deprecated: has no effect now
	DriverClusterEvents

	DriverEvents = DriverNetEvents |
		DriverConnEvents |
		DriverBalancerEvents |
		DriverResolverEvents |
		DriverRepeaterEvents |
		DriverCredentialsEvents

	TableEvents = TableSessionLifeCycleEvents |
		TableSessionQueryInvokeEvents |
		TableSessionQueryStreamEvents |
		TableSessionTransactionEvents |
		TablePoolLifeCycleEvents |
		TablePoolSessionLifeCycleEvents |
		TablePoolAPIEvents

	TablePoolEvents = TablePoolLifeCycleEvents |
		TablePoolSessionLifeCycleEvents |
		TablePoolAPIEvents

	TableSessionQueryEvents = TableSessionQueryInvokeEvents |
		TableSessionQueryStreamEvents
	TableSessionEvents = TableSessionLifeCycleEvents |
		TableSessionQueryEvents |
		TableSessionTransactionEvents

	TopicReaderEvents = TopicReaderStreamEvents | TopicReaderMessageEvents |
		TopicReaderPartitionEvents |
		TopicReaderStreamLifeCycleEvents

	TopicEvents = TopicControlPlaneEvents | TopicReaderEvents

	DatabaseSQLEvents = DatabaseSQLConnectorEvents |
		DatabaseSQLConnEvents |
		DatabaseSQLTxEvents |
		DatabaseSQLStmtEvents

	DetailsAll = ^Details(0) // All bits enabled
)

var (
	details = map[Details]string{
		DriverEvents:            "ydb.driver",
		DriverBalancerEvents:    "ydb.driver.balancer",
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

		DatabaseSQLEvents:          "ydb.database.sql",
		DatabaseSQLConnectorEvents: "ydb.database.sql.connector",
		DatabaseSQLConnEvents:      "ydb.database.sql.conn",
		DatabaseSQLTxEvents:        "ydb.database.sql.tx",
		DatabaseSQLStmtEvents:      "ydb.database.sql.stmt",

		TopicEvents:                      "ydb.topic",
		TopicControlPlaneEvents:          "ydb.topic.controlplane",
		TopicReaderEvents:                "ydb.topic.reader",
		TopicReaderStreamEvents:          "ydb.topic.reader.stream",
		TopicReaderMessageEvents:         "ydb.topic.reader.message",
		TopicReaderPartitionEvents:       "ydb.topic.reader.partition",
		TopicReaderStreamLifeCycleEvents: "ydb.topic.reader.lifecycle",
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
