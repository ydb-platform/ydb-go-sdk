package meta

const (
	// outgoing headers
	HeaderDatabase           = "x-ydb-database"
	HeaderTicket             = "x-ydb-auth-ticket"
	HeaderVersion            = "x-ydb-sdk-build-info"
	HeaderRequestType        = "x-ydb-request-type"
	HeaderTraceID            = "x-ydb-trace-id"
	HeaderUserAgent          = "x-ydb-user-agent"
	HeaderClientCapabilities = "x-ydb-client-capabilities"

	// outgoing hints
	HintSessionBalancer = "session-balancer"

	// incomming headers
	HeaderServerHints   = "x-ydb-server-hints"
	HeaderConsumedUnits = "x-ydb-consumed-units"

	// incoming hints
	HintSessionClose = "session-close"
)
