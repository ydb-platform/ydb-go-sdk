package observability

// Chain versions are coordinated across SDKs.
// The version sequence is global for ydb-sdk-* adoption markers.
const (
	TracingChainName    = "ydb-sdk-tracing"
	MetricsChainName    = "ydb-sdk-metrics"
	TracingChainVersion = "0.1.0"
	MetricsChainVersion = "0.1.0"
)
