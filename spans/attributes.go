package spans

// OTel-compliant span names emitted by the spans package.
//
// Adapter implementations are expected to map these names to the appropriate
// OpenTelemetry SpanKind:
//   - SpanNameCreateSession, SpanNameExecuteQuery, SpanNameCommit, SpanNameRollback
//     are emitted as CLIENT spans (gRPC calls to YDB QueryService).
//   - SpanNameRunWithRetry, SpanNameTry are emitted as INTERNAL spans (retry loop).
const (
	// SpanNameCreateSession is the span name for QueryService session creation
	// (CreateSession + first AttachStream message).
	SpanNameCreateSession = "ydb.CreateSession"

	// SpanNameExecuteQuery is the span name for QueryService ExecuteQuery RPC
	// (covers reading the response stream from start to end).
	SpanNameExecuteQuery = "ydb.ExecuteQuery"

	// SpanNameCommit is the span name for QueryService CommitTransaction RPC.
	SpanNameCommit = "ydb.Commit"

	// SpanNameRollback is the span name for QueryService RollbackTransaction RPC.
	SpanNameRollback = "ydb.Rollback"

	// SpanNameRunWithRetry is the span name for the outer retry loop wrapper.
	SpanNameRunWithRetry = "ydb.RunWithRetry"

	// SpanNameTry is the span name for one retry attempt.
	SpanNameTry = "ydb.Try"
)

// OTel and YDB attribute keys.
//
// The `db.*`, `server.*`, `network.peer.*` and `error.*` keys follow OpenTelemetry
// semantic conventions for database client spans:
// https://opentelemetry.io/docs/specs/semconv/database/database-spans/
//
// The `ydb.*` keys are YDB-specific extensions.
const (
	// AttrDBSystemName identifies the database system. Always "ydb" for spans
	// emitted by this package.
	AttrDBSystemName = "db.system.name"

	// AttrDBNamespace contains the database path (driver Database setting),
	// e.g. "/local".
	AttrDBNamespace = "db.namespace"

	// AttrServerAddress is the host portion of the connection endpoint as
	// configured in the driver.
	AttrServerAddress = "server.address"

	// AttrServerPort is the port portion of the connection endpoint as
	// configured in the driver.
	AttrServerPort = "server.port"

	// AttrNetworkPeerAddress is the actual node address an RPC was routed to.
	AttrNetworkPeerAddress = "network.peer.address"

	// AttrNetworkPeerPort is the actual node port an RPC was routed to.
	AttrNetworkPeerPort = "network.peer.port"

	// AttrErrorType is the type of an error reported by SetException
	// equivalents. For YDB transport errors the value is "transport_error", for
	// any other ydb.Error it is "ydb_error", otherwise it is the Go error's
	// dynamic type name.
	AttrErrorType = "error.type"

	// AttrDBResponseStatusCode is the YDB status code returned by the server
	// (only set for errors that carry a YDB status code).
	AttrDBResponseStatusCode = "db.response.status_code"

	// AttrYDBNodeID is the numeric YDB node id of the actual node that handled
	// the request.
	AttrYDBNodeID = "ydb.node.id"

	// AttrYDBNodeDC is the location DC reported by the node.
	AttrYDBNodeDC = "ydb.node.dc"

	// AttrYDBRetryBackoffMs is the backoff duration (in milliseconds) waited
	// before the current ydb.Try attempt was started. Only set on retry
	// attempts; the very first attempt has no preceding sleep and therefore
	// does not carry this attribute.
	AttrYDBRetryBackoffMs = "ydb.retry.backoff_ms"
)

// Standard error.type values for AttrErrorType.
const (
	ErrorTypeTransport = "transport_error"
	ErrorTypeYDB       = "ydb_error"
)
