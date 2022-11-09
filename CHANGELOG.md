* Added `options.WithAddAttribute` and `options.WithDropAttribute` options for `session.AlterTable` request

## v3.39.0
* Removed message level partitioning from experimental topic API. It is unavailable on server side yet.
* Supported `NullValue` type as received type from `YDB`
* Supported `types.SetValue` type
* Added `types.CastTo(types.Value, destination)` public method for cast `types.Value` to golang native type value destination
* Added `types.TupleItem(types.Value)`, `types.StructFields(types.Value)` and `types.DictValues(types.Value)` funcs (extractors of internal fields of tuple, struct and dict values)
* Added `types.Value.Yql()` func for getting values string representation as `YQL` literal
* Added `types.Type.Yql()` func for getting `YQL` representation of type
* Marked `table/types.WriteTypeStringTo` as deprecated 
* Added `table/options.WithDataColumns` for supporting covering indexes
* Supported `balancer` query string parameter in `DSN`
* Fixed bug with scanning `YSON` value from result set
* Added certificate caching in `WithCertificatesFromFile` and `WithCertificatesFromPem`

## v3.38.5
* Fixed bug from scan unexpected column name

## v3.38.4
* Changed type of `table/options.{Create,Alter,Drop}TableOption` from func to interface
* Added implementations of `table/options.{Create,Alter,Drop}Option`
* Changed type of `topic/topicoptions.{Create,Alter,Drop}Option` from func to interface
* Added implementations of `topic/topicoptions.{Create,Alter}Option`
* Fix internal race-condition bugs in internal background worker

## v3.38.3
* Added retries to initial discovering

## v3.38.2
* Added missing `RetentionPeriod` parameter for topic description
* Fixed reconnect problem for topic client
* Added queue limit for sent messages and split large grpc messages while send to topic service
* Improved control plane for topic services: allow list topic in schema, read cdc feeds in table, retry on contol plane operations in topic client, full info in topic describe result
* Allowed writing zero messages to topic writer

## v3.38.1
* Fixed deadlock with implicit usage of `internal.table.Client.internalPoolAsyncCloseSession` 

## v3.38.0
* Fixed commit errors for experimental topic reader
* Updated `ydb-go-genproto` dependency
* Added `table.WithSnapshotReadOnly()` `TxOption` for supporting `SnapshotReadOnly` transaction control
* Fixed bug in `db.Scripting()` queries (not checked operation results)
* Added `sugar.ToYdbParam(sql.NamedArg)` helper for converting `sql.NamedArg` to `table.ParameterOption`
* Changed type `table.ParameterOption` for getting name and value from `table.ParameterOption` instance
* Added topic writer experimental api with internal logger

## v3.37.8
* Refactored the internal closing behaviour of table client
* Implemented the `sql.driver.Validator` interface
* Fixed update token for topic reader
* Marked sessions which creates from `database/sql` driver as supported server-side session balancing

## v3.37.7
* Changed type of truncated result error from `StreamExecuteScanQuery` to retryable error
* Added closing sessions if node removed from discovery results
* Moved session status type from `table/options` package to `table`
* Changed session status source type from `uint32` to `string` alias 

## v3.37.6
* Added to balancer notifying mechanism for listening in table client event about removing some nodes and closing sessions on them 
* Removed from public client interfaces `closer.Closer` (for exclude undefined behaviour on client-side)

## v3.37.5
* Refactoring of `xsql` errors checking

## v3.37.4
* Revert the marking of context errors as required to delete session

## v3.37.3
* Fixed alter topic request - stop send empty setSupportedCodecs if customer not set them 
* Marked the context errors as required to delete session
* Added log topic api reader for internal logger

## v3.37.2
* Fixed nil pointer exception in topic reader if reconnect failed

## v3.37.1
* Refactored the `xsql.badconn.Error`

## v3.37.0
* Supported read-only `sql.LevelSnapshot` isolation with fake transaction and `OnlineReadOnly` transaction control (transient, while YDB clusters are not updated with true snapshot isolation mode)
* Supported the `*sql.Conn` as input type `ydb.Unwrap` helper for go's 1.18

## v3.36.2
* Changed output of `sugar.GenerateDeclareSection` (added error as second result)
* Specified `sugar.GenerateDeclareSection` for `go1.18` (supports input types `*table.QueryParameters` `[]table.ParameterOption` or `[]sql.NamedArg`)
* Supports different go's primitive value types as arg of `sql.Named("name", value)`
* Added `database/sql` example and docs

## v3.36.1
* Fixed `xsql.Rows` error checking

## v3.36.0
* Changed behavior on `result.Err()` on truncated result (returns non-retryable error now, exclude `StreamExecuteScanQuery`)
* Added `ydb.WithIgnoreTruncated` option for disabling errors on truncated flag
* Added simple transaction control constructors `table.OnlineReadOnlyTxControl()` and `table.StaleReadOnlyTxControl()`
* Added transaction control specifier with context `ydb.WithTxControl`
* Added value constructors `types.BytesValue`, `types.BytesValueFromString`, `types.TextValue`
* Removed auto-prepending declare section on `xsql` queries
* Supports `time.Time` as type destination in `xsql` queries
* Defined default dial timeout (5 seconds)

## v3.35.1
* Removed the deprecation warning for `ydb.WithSessionPoolIdleThreshold` option

## v3.35.0
* Replaced internal table client background worker to plain wait group for control spawned goroutines
* Replaced internal table client background session keeper to internal background session garbage collector for idle sessions
* Extended the `DescribeTopicResult` struct

## v3.34.2
* Added some description to error message from table pool get
* Moved implementation `sugar.GenerateDeclareSection` to `internal/table`
* Added transaction trace callbacks and internal logging with them
* Stored context from `BeginTx` to `internal/xsql` transaction
* Added automatically generated declare section to query text in `database/sql` usage 
* Removed supports `sql.LevelSerializable`
* Added `retry.Do` helper for retry custom lambda with `database/sql` without transactions
* Removed `retry.WithTxOptions` option (only default isolation supports)

## v3.34.1
* Changed `database/sql` driver `prepare` behaviour to `nop` with proxing call to conn exec/query with keep-in-cache flag
* Added metadata to `trace.Driver.OnInvoke` and `trace.Driver.OnNewStream` done events

## v3.34.0
* Improved the `xsql` errors mapping to `driver.ErrBadConn` 
* Extended `retry.DoTx` test for to achieve equivalence with `retry.Retry` behaviour
* Added `database/sql` events for tracing `database/sql` driver events
* Added internal logging for `database/sql` events
* Supports `YDB_LOG_DETAILS` environment variable for specify scope of log messages
* Removed support of `YDB_LOG_NO_COLOR` environment variable
* Changed default behaviour of internal logger to without coloring
* Fixed coloring (to true) with environment variable `YDB_LOG_SEVERITY_LEVEL`
* Added `ydb.WithStaticCredentials(user, password)` option for make static credentials 
* Supports static credentials as part of connection string (dsn - data source name)
* Changed minimal supported version of go from 1.14 to 1.16 (required for jwt library)


## v3.33.0
* Added `retry.DoTx` helper for retrying `database/sql` transactions 
* Implemented `database/sql` driver over `ydb-go-sdk`
* Marked as deprecated `trace.Table.OnPoolSessionNew` and `trace.Table.OnPoolSessionClose` events
* Added `trace.Table.OnPoolSessionAdd` and `trace.Table.OnPoolSessionRemove` events
* Refactored session lifecycle in session pool for fix flaked `TestTable`
* Fixed deadlock in topicreader batcher, while add and read raw server messages
* Fixed bug in `db.Topic()` with send response to stop partition message

## v3.32.1
* Fixed flaky TestTable
* Renamed topic events in `trace.Details` enum

## v3.32.0
* Refactored `trace.Topic` (experimental) handlers
* Fixed signature and names of helpers in `topic/topicsugar` package 
* Allowed parallel reading and committing topic messages

## v3.31.0
* Extended the `ydb.Connection` interface with experimental `db.Topic()` client (control plane and reader API)
* Removed `ydb.RegisterParser()` function (was needed for `database/sql` driver outside `ydb-go-sdk` repository, necessity of `ydb.RegisterParser()` disappeared with implementation `database/sql` driver in same repository)
* Refactored `db.Table().CreateSession(ctx)` (maked retryable with internal create session timeout)
* Refactored `internal/table/client.createSession(ctx)` (got rid of unnecessary goroutine)
* Supported many user-agent records

## v3.30.0
* Added `ydb.RegisterParser(name string, parser func(value string) []ydb.Option)` function for register parser of specified param name (supporting additional params in connection string)
* Fixed writing `KeepInCacheFlag` in table traces

## v3.29.5
* Fixed regression of `table/types.WriteTypeStringTo`

## v3.29.4
* Added touching of last updated timestamp in existing conns on stage of applying new endpoint list

## v3.29.3
* Reverted `xerrors.IsTransportError(err)` behaviour for raw grpc errors to false 

## v3.29.2
* Enabled server-side session balancing for sessions created from internal session pool 
* Removed unused public `meta.Meta` methods
* Renamed `meta.Meta.Meta(ctx)` public method to `meta.Meta.Context(ctx)`
* Reverted default balancer to `balancers.RandomChoice()`

## v3.29.1
* Changed default balancer to `balancers.PreferLocalDC(balancers.RandomChoice())`

## v3.29.0
* Refactored `internal/value` package for decrease CPU and memory workload with GC
* Added `table/types.Equal(lhs, rhs)` helper for check equal for two types

## v3.28.3
* Fixed false-positive node pessimization on receiving from stream io.EOF

## v3.28.2
* Upgraded dependencies (grpc, protobuf, testify)

## v3.28.1
* Marked dial errors as retryable
* Supported node pessimization on dialing errors  
* Marked error from `Invoke` and `NewStream` as retryable if request not sended to server

## v3.28.0
* Added `sugar.GenerateDeclareSection()` helper for make declare section in `YQL`
* Added check when parameter name not started from `$` and automatically prepends it to name 
* Refactored connection closing

## v3.27.0
* Added internal experimental packages `internal/value/exp` and `internal/value/exp/allocator` with alternative value implementations with zero-allocation model
* Supported parsing of database name from connection string URI path
* Added `options.WithExecuteScanQueryStats` option
* Added to query stats plan and AST
* Changed behaviour of `result.Stats()` (if query result have no stats - returns `nil`)
* Added context cancel with specific error
* Added mutex wrapper for mutex, rwmutex for guarantee unlock and better show critical section

## v3.26.10
* Fixed syntax mistake in `trace.TablePooStateChangeInfo` to `trace.TablePoolStateChangeInfo`

## v3.26.9
* Fixed bug with convert ydb value to `time.Duration` in `result.Scan[WithDefaults,Named]()`
* Fixed bug with make ydb value from `time.Duration` in `types.IntervalValueFromDuration(d)`
* Marked `table/types.{IntervalValue,NullableIntervalValue}` as deprecated

## v3.26.8
* Removed the processing of trailer metadata on stream calls

## v3.26.7
* Updated the `ydb-go-genproto` dependency

## v3.26.6
* Defined the `SerializableReadWrite` isolation level by default in `db.Table.DoTx(ctx, func(ctx, tx))`
* Updated the `ydb-go-genproto` dependency

## v3.26.5
* Disabled the `KeepInCache` policy for queries without params

## v3.26.4
* Updated the indirect dependency to `gopkg.in/yaml.v3`

## v3.26.3
* Removed `Deprecated` mark from `table/session.Prepare` method
* Added comments for `table/session.Execute` method

## v3.26.2
* Refactored of making permissions from scheme entry

## v3.26.1
* Removed deprecated traces

## v3.26.0
* Fixed data race on session stream queries
* Renamed `internal/router` package to `internal/balancer` for unambiguous understanding of package mission
* Implemented detection of local data-center with measuring tcp dial RTT
* Added `trace.Driver.OnBalancer{Init,Close,ChooseEndpoint,Update}` events
* Marked the driver cluster events as deprecated
* Simplified the balancing logic

## v3.25.3
* Changed primary license to `Apache2.0` for auto-detect license
* Refactored `types.Struct` value creation

## v3.25.2
* Fixed repeater initial force timeout from 500 to 0.5 second

## v3.25.1
* Fixed bug with unexpected failing of call `Invoke` and `NewStream` on closed cluster
* Fixed bug with releasing `internal/conn/conn.Pool` in cluster
* Replaced interface `internal/conn/conn.Pool` to struct `internal/conn/conn.Pool`

## v3.25.0
* Added `ydb.GRPCConn(ydb.Connection)` helper for connect to driver-unsupported YDB services
* Marked as deprecated `session.Prepare` callback
* Marked as deprecated `options.WithQueryCachePolicyKeepInCache` and `options.WithQueryCachePolicy` options
* Added `options.WithKeepInCache` option
* Enabled by default keep-in-cache policy for data queries
* Removed from `ydb.Connection` embedding of `grpc.ClientConnInterface`
* Fixed stopping of repeater
* Added log backoff between force repeater wake up's (from 500ms to 32s)
* Renamed `trace.DriverRepeaterTick{Start,Done}Info` to `trace.DriverRepeaterWakeUp{Start,Done}Info`
* Fixed unexpected `NullFlag` while parse nil `JSONDocument` value
* Removed `internal/conn/conn.streamUsages` and `internal/conn/conn.usages` (`internal/conn.conn` always touching last usage timestamp on API calls)
* Removed auto-reconnecting for broken conns
* Renamed `internal/database` package to `internal/router` for unambiguous understanding of package mission
* Refactored applying actual endpoints list after re-discovery (replaced diff-merge logic to swap cluster struct, cluster and balancers are immutable now)
* Added `trace.Driver.OnUnpessimizeNode` trace event

## v3.24.2
* Changed default balancer to `RandomChoice()` because `PreferLocalDC()` balancer works incorrectly with DNS-balanced call `Discovery/ListEndpoints`

## v3.24.1
* Refactored initialization of coordination, ratelimiter, scheme, scripting and table clients from `internal/lazy` package to each client initialization with `sync.Once`
* Removed `internal/lazy` package
* Added retry option `retry.WithStackTrace` for wrapping errors with stacktrace

## v3.24.0
* Fixed re-opening case after close lazy-initialized clients
* Removed dependency of call context for initializing lazy table client
* Added `config.AutoRetry()` flag with `true` value by default. `config.AutoRetry()` affects how to errors handle in sub-clients calls.
* Added `config.WithNoAutoRetry` for disabling auto-retry on errors in sub-clients calls
* Refactored `internal/lazy` package (supported check `config.AutoRetry()`, removed all error wrappings with stacktrace)

## v3.23.0
* Added `WithTLSConfig` option for redefine TLS config
* Added `sugar.LoadCertificatesFromFile` and `sugar.LoadCertificatesFromPem` helpers

## v3.22.0
* Supported `json.Unmarshaler` type for scanning row to values
* Reimplemented `sugar.DSN` with `net/url`

## v3.21.0
* Fixed gtrace tool generation code style bug with leading spaces
* Removed accounting load factor (unused field) in balancers
* Enabled by default anonymous credentials
* Enabled by default internal dns resolver
* Removed from defaults `grpc.WithBlock()` option
* Added `ydb.Open` method with required param connection string
* Marked `ydb.New` method as deprecated
* Removed package `dsn`
* Added `sugar.DSN` helper for make dsn (connection string)
* Refactored package `retry` (moved `retryBackoff` and `retryMode` implementations to `internal`)
* Refactored `config.Config` (remove interface `Config`, renamed private struct `config` to `Config`)
* Moved `discovery/config` to `internal/discovery/config`
* Moved `coordination/config` to `internal/coordination/config`
* Moved `scheme/config` to `internal/scheme/config`
* Moved `scripting/config` to `internal/scripting/config`
* Moved `table/config` to `internal/table/config`
* Moved `ratelimiter/config` to `internal/ratelimiter/config`

## v3.20.2
* Fixed race condition on lazy clients first call

## v3.20.1
* Fixed gofumpt linter issue on `credentials/credentials.go`

## v3.20.0
* Added `table.DefaultTxControl()` transaction control creator with serializable read-write isolation mode and auto-commit
* Fixed passing nil query parameters
* Fixed locking of cluster during call `cluster.Get`

## v3.19.1
* Simplified README.md for godoc documentation in pkg.go.dev

## v3.19.0
* Added public package `dsn` for making piped data source name (connection string)
* Marked `ydb.WithEndpoint`, `ydb.WithDatabase`, `ydb.WithSecure`, `ydb.WithInsecure` options as deprecated
* Moved `ydb.RegisterParser` to package `dsn`
* Added version into all error and warn log messages

## v3.18.5
* Fixed duplicating `WithPanicCallback` proxying to table config options
* Fixed comments for `xerrros.Is` and `xerrros.As`

## v3.18.4
* Renamed internal packages `errors`, `net` and `resolver` to `xerrors`, `xnet` and `xresolver` for excluding ambiguous interpretation
* Renamed internal error wrapper `xerrors.New` to `xerrors.Wrap`

## v3.18.3
* Added `WithPanicCallback` option to all service configs (discovery, coordination, ratelimiter, scheme, scripting, table) and auto-applying from `ydb.WithPanicCallback`
* Added panic recovering (if defined `ydb.WithPanicCallback` option) which thrown from retry operation

## v3.18.2
* Refactored balancers (makes concurrent-safe)
* Excluded separate balancers lock from cluster
* Refactored `cluster.Cluster` interface (`Insert` and `Remove` returning nothing now)
* Replaced unsafe `cluster.close` boolean flag to `cluster.done` chan for listening close event
* Added internal checker `cluster.isClosed()` for check cluster state
* Extracted getting available conn from balancer to internal helper `cluster.get` (called inside `cluster.Get` as last effort)
* Added checking `conn.Conn` availability with `conn.Ping()` in prefer nodeID case

## v3.18.1
* Added `conn.Ping(ctx)` method for check availability of `conn.Conn`
* Refactored `cluster.Cluster.Get(ctx)` to return only available connection (instead of returning any connection from balancer)
* Added address to error description thrown from `conn.take()`
* Renamed package `internal/db` to `internal/database` to exclude collisions with variable name `db`

## v3.18.0
* Added `go1.18` to test matrix
* Added `ydb.WithOperationTimeout` and `ydb.WithOperationCancelAfter` context modifiers

## v3.17.0
* Removed redundant `trace.With{Table,Driver,Retry}` and `trace.Context{Table,Driver,Retry}` funcs
* Moved `gtrace` tool from `./cmd/gtrace` to `./internal/cmd/gtrace`
* Refactored `gtrace` tool for generate `Compose` options
* Added panic recover on trace calls in `Compose` call step
* Added `trace.With{Discovery,Driver,Coordination,Ratelimiter,Table,Scheme,Scripting}PanicCallback` options
* Added `ydb.WithPanicCallback` option

## v3.16.12
* Fixed bug with check acquire error over `ydb.IsRatelimiterAcquireError`
* Added full changelog link to github release description

## v3.16.11
* Added stacktrace to errors with issues

## v3.16.10
* Refactored `cluster.Cluster` and `balancer.Balancer` interfaces (removed `Update` method)
* Replaced `cluster.Update` with `cluster.Remove` and `cluster.Insert` calls
* Removed `trace.Driver.OnClusterUpdate` event
* Fixed bug with unexpected changing of local datacenter flag in endpoint
* Refactored errors wrapping (stackedError are not ydb error now, checking `errors.IsYdb(err)` with `errors.As` now)
* Wrapped retry operation errors with `errors.WithStackTrace(err)`
* Changed `trace.RetryLoopStartInfo.Context` type from `context.Context` to `*context.Context`

## v3.16.9
* Refactored internal operation and transport errors

## v3.16.8
* Added `config.ExcludeGRPCCodesForPessimization()` opttion for exclude some grpc codes from pessimization rules
* Refactored pessimization node conditions
* Added closing of ticker in `conn.Conn.connParker`
* Removed `config.WithSharedPool` and usages it
* Removed `conn.Creator` interface and usage it
* Removed unnecessary options append in `ydb.With`

## v3.16.7
* Added closing `conn.Conn` if discovery client build failure
* Added wrapping errors with stacktrace
* Added discharging banned state of `conn.Conn` on `cluster.Update` step

## v3.16.6
* Rollback moving `meta.Meta` call to conn exclusively from `internal/db` and `internal/discovery`
* Added `WithMeta()` discovery config option

## v3.16.5
* Added `config.SharedPool()` setting and `config.WithSharedPool()` option
* Added management of shared pool flag on change dial timeout and credentials
* Removed explicit checks of conditions for use (or not) shared pool in `ydb.With()`
* Renamed `internal/db` interfaces
* Changed signature of `conn.Conn.Release` (added error as result)

## v3.16.4
* Removed `WithMeta()` discovery config option
* Moved `meta.Meta` call to conn exclusively

## v3.16.3
* Replaced panic on cluster close to error issues

## v3.16.2
* Fixed bug in `types.Nullable()`
* Refactored package `meta`
* Removed explicit call meta in `db.New()`

## v3.16.1
* Added `WithMeta()` discovery config option
* Fixed bug with credentials on discovery

## v3.16.0
* Refactored internal dns-resolver
* Added option `config.WithInternalDNSResolver` for use internal dns-resolver and use resolved IP-address for dialing instead FQDN-address

## v3.15.1
* Removed all conditions for trace retry errors
* Fixed background color of warn messages
* Added to log messages additional information about error, such as retryable (or not), delete session (or not), etc.

## v3.15.0
* Added github action for publish release tags
* Refactored version constant (split to major, minor and patch constants)
* Added `table.types.Nullable{*}Value` helpers and `table.types.Nullable()` common helper
* Fixed race on check trailer on closing table grpc-stream
* Refactored traces (start and done struct names have prefix about trace)
* Replaced `errors.Error`, `errors.Errorf` and `errors.ErrorfSkip` to single `errors.WithStackTrace`
* Refactored table client options
* Declared and implemented interface `errors.isYdbError` for checking ybd/non-ydb errors
* Fixed double tracing table do events
* Added `retry.WithFastBackoff` and `retry.WithFastBackoff` options
* Refactored `table.CreateSession` as retry operation with options
* Moved log level from root of repository to package `log`
* Added details and address to transport error
* Fixed `recursive` param in `ratelimiter.ListResource`
* Added counting stream usages for exclude park connection if it in use
* Added `trace.Driver` events about change stream usage and `conn.Release()` call

## 3.14.4
* Implemented auto-removing `conn.Conn` from `conn.Pool` with counting usages of `conn.Conn`
* Refactored naming of source files which declares service client interfaces

## 3.14.3
* Fixed bug with update balancer element with nil handle

## 3.14.2
* Refactored internal error wrapping (with file and line identification) - replaced `fmt.Printf("%w", err)` error wrapping to internal `stackError`

## 3.14.1
* Added `balacers.CreateFromConfig` balancer creator
* Added `Create` method to interface `balancer.Balancer`

## 3.14.0
* Added `balacers.FromConfig` balancer creator

## 3.13.3
* Fixed linter issues

## 3.13.2
* Fixed race with read/write pool conns on closing conn

## 3.13.1
* Improved error messages
* Defended `cluster.balancer` with `sync.RWMutex` on `cluster.Insert`, `cluster.Update`, `cluster.Remove` and `cluster.Get`
* Excluded `Close` and `Park` methods from `conn.Conn` interface
* Fixed bug with `Multi` balancer `Create()`
* Improved `errors.IsTransportError` (check a few transport error codes instead check single transport error code)
* Improved `errors.Is` (check a few errors instead check single error)
* Refactored YDB errors checking API on client-side
* Implemented of scripting traces

## 3.13.0
* Refactored `Connection` interface
* Removed `CustomOption` and taking client with custom options
* Removed `proxy` package
* Improved `db.With()` helper for child connections creation
* Set shared `conn.Pool` for all children `ydb.Connection`
* Fixed bug with `RoundRobin` and `RandomChoice` balancers `Create()`

## 3.12.1
* Added `trace.Driver.OnConnPark` event
* Added `trace.Driver.OnConnClose` event
* Fixed bug with closing nil session in table retryer
* Restored repeater `Force` call on pessimize event
* Changed mutex type in `conn.Conn` from `sync.Mutex` to `sync.RWMutex` for exclude deadlocks
* Reverted applying empty `discovery` results to `cluster`

## 3.12.0
* Added `balancers.Prefer` and `balancers.PreferWithFallback` constructors

## 3.11.13
* Added `trace.Driver.OnRepeaterWakeUp` event
* Refactored package `repeater`

## 3.11.12
* Added `trace.ClusterInsertDoneInfo.Inserted` boolean flag for notify about success of insert endpoint into balancer
* Added `trace.ClusterRemoveDoneInfo.Removed` boolean flag for notify about success of remove endpoint from balancer

## 3.11.11
* Reverted usage of `math/rand` (instead `crypto/rand`)

## 3.11.10
* Imported tool gtrace to `./cmd/gtrace`
* Changed minimal version of go from 1.13 to 1.14

## 3.11.9
* Fixed composing of service traces
* Fixed end-call of `trace.Driver.OnConnStateChange`

## 3.11.8
* Added `trace.EndpointInfo.LastUpdated()` timestamp
* Refactored `endpoint.Endpoint` (split to struct `endopint` and interface `Endpoint`)
* Returned safe-thread copy of `endpoint.Endpoint` to trace callbacks
* Added `endpoint.Endpoint.Touch()` func for refresh endpoint info
* Added `conn.conn.onClose` slice for call optional funcs on close step
* Added removing `conn.Conn` from `conn.Pool` on `conn.Conn.Close()` call
* Checked cluster close/empty on keeper goroutine
* Fixed `internal.errors.New` wrapping depth
* Added context flag for no wrapping operation results as error
* Refactored `trace.Driver` conn events

## 3.11.7
* Removed internal alias-type `errors.IssuesIterator`

## 3.11.6
* Changed `trace.GetCredentialsDoneInfo` token representation from bool to string
* Added `log.Secret` helper for mask token

## 3.11.5
* Replaced meta in `proxyConnection.Invoke` and `proxyConnection.NewStream`

## 3.11.4
* Refactored `internal/cluster.Cluster` (add option for notify about external lock, lock cluster for update cluster endpoints)
* Reverted `grpc.ClientConnInterface` API to `ydb.Connection`

## 3.11.3
* Replaced in `table/types/compare_test.go` checking error by error message to checking with `errors.Is()`

## 3.11.2
* Wrapped internal errors in retry operations

## 3.11.1
* Excluded error wrapping from retry operations

## 3.11.0
* Added `ydb.WithTLSSInsecureSkipVerify()` option
* Added `trace.Table.OnPoolStateChange` event
* Wrapped internal errors with print <func, file, line>
* Removed `trace.Table.OnPoolTake` event (unused)
* Refactored `trace.Details` matching by string pattern
* Added resolver trace callback
* Refactored initialization step of grpc dial options
* Added internal package `net` with `net.Conn` proxy object
* Fixed closing proxy clients
* Added `ydb.Connection.With(opts ...ydb.CustomOption)` for taking proxy `ydb.Connection` with some redefined options
* Added `ydb.MetaRequestType` and `ydb.MetaTraceID` aliases to internal `meta` package constants
* Added `ydb.WithCustomCredentials()` option
* Refactored `ydb.Ratelimiter().AcquireResource()` method (added options for defining type of acquire request)
* Removed single point to define operation mode params (each grpc-call with `OperationParams` must explicit define `OperationParams`)
* Removed defining operation params over context
* Removed `config.RequestTimeout` and `config.StreamTimeout` (each grpc-call must manage context instead define `config.RequestTimeout` or `config.StreamTimeout`)
* Added internal `OperationTimeout` and `OperationCancelAfter` to each client (ratelimiter, coordination, table, scheme, scripting, discovery) config. `OperationTimeout` and `OperationCancelAfter` config params defined from root config

## 3.10.0
* Extended `trace.Details` constants for support per-service events
* Added `trace.Discovery` struct for traces discovery events
* Added `trace.Ratelimiter`, `trace.Coordination`, `trace.Scripting`, `trace.Scheme` stubs (will be implements in the future)
* Added `ratelimiter/config`, `coordination/config`, `scripting/config`, `scheme/config`, `discovery/config` packages for specify per-service configs
* Removed `trace.Driver.OnDiscovery` callback (moved to `trace.Discovery`)
* Refactored initialization step (firstly makes discovery client)
* Removed `internal/lazy.Discovery` (discovery client always initialized)
* Fixed `trace.Table` event structs
* Refactored grpc options for define dns-balancing configuration
* Refactored `retry.Retry` signature (added `retry.WithID`, `retry.WithTrace` and `retry.WithIdempotent` opt-in args, required param `isIdempotentOperation` removed)
* Refactored package `internal/repeater`

## 3.9.4
* Fixed data race on closing session pool

## 3.9.3
* Fixed busy loop on call internal logger with external logger implementation of `log.Logger`

## 3.9.2
* Fixed `WithDiscoveryInterval()` option with negative argument (must use `SingleConn` balancer)

## 3.9.1
* Added `WithMinTLSVersion` option

## 3.9.0
* Removed `ydb.EndpointDatabase`, `ydb.ConnectionString` and `ydb.MustConnectionString` helpers
* Removed `ydb.ConnectParams` struct and `ydb.WithConnectParams` option creator
* Added internal package `dsn` for register external parsers and parse connection string
* Added `ydb.RegisterParser` method for registering external parser of connection string

## 3.8.12
* Unwrap sub-tests called as `t.Run(...)` in integration tests
* Updated `grpc` dependency (from `v1.38.0` to `v1.43.0`)
* Updated `protobuf` dependency (from `v1.26.0` to `v1.27.1`)
* Added internal retryers into `lazy.Ratelimiter`
* Added internal retryers into `lazy.Coordination`
* Added internal retryers into `lazy.Discovery`
* Added internal retryers into `lazy.Scheme`
* Added internal retryers into `lazy.Scripting`
* Added internal retryer into `lazy.Table.CreateSession`

## 3.8.11
* Fixed version

## 3.8.10
* Fixed misspell linter issue

## 3.8.9
* Removed debug print to log

## 3.8.8
* Refactored session shutdown test

## 3.8.7
* Ignored session shutdown test if no defined `YDB_SHUTDOWN_URLS` environment variable

## 3.8.6
* Added `ydb.WithInsecure()` option

## 3.8.5
* Fixed version

## 3.8.4
* Fixed syntax error in `CHANGELOG.md`

## 3.8.3
* Fixed `CHANGELOG.md`

## 3.8.2
* Updated `github.com/ydb-platform/ydb-go-genproto`

## 3.8.1
* Fixed `trace.Table.OnPoolDoTx` - added `Idempotent` flag to `trace.PoolDoTxStartInfo`

## 3.8.0
* Added `table.result.Result.ScanNamed()` scan function
* Changed connection secure to `true` by default
* Renamed public package `balancer` to `balancers` (this package contains only constructors of balancers)
* Moved interfaces from package `internal/balancer/ibalancer` to `internal/balancer`
* Added `NextResultSetErr()` func for select next result set and return error
* Added package `table/result/indexed` with interfaces `indexed.Required`, `indexed.Optional`, `indexed.RequiredOrOptional`
* Replaced abstract `interface{}` in `Scan` to `indexed.RequiredOrOptional`
* Replaced abstract `interface{}` in `ScanWithDefaults` to `indexed.Required`
* Replaced `trace.Table.OnPoolRetry` callback to `trace.Table.OnPoolDo` and `trace.Table.OnPoolDoTx` callbacks
* Supports server hint `session-close` for gracefully shutdown session

## 3.7.2
* Retry remove directory in `sugar.RemoveRecursive()` for retryable error

## 3.7.1
* Fixed panic on `result.Reset(nil)`

## 3.7.0
* Replaced `Option` to `CustomOption` on `Connection` interface methods
* Implements `WithCustom[Token,Database]` options for redefine database and token
* Removed experimental `balancer.PreferEndpoints[WithFallback][RegEx]` balancers
* Supported connections `TTL` with `Option` `WithConnectionTTL`
* Remove unnecessary `WithFastDial` option (lazy connections are always fast inserts into cluster)
* Added `Scripting` service client with API methods `Execute()`, `StreamExecute()` and `Explain()`
* Added `String()` method to `table.types.Type` interface
* Added `With[Custom]UserAgent()` `Option` and `CustomOption` constructors
* Refactored `log.Logger` interface and internal implementation
* Added `retry.RetryableError()` for returns user-defined error which must be retryed
* Renamed internal type `internal.errors.OperationCompleted` to `internal.errors.OperationStatus`
* Added `String()` method to `table.KeyRange` and `table.Value` types
* Replaced creation of goroutine on each stream call to explicit call stream.Recv() on NextResultSet()

## 3.6.2
* Refactored table retry helpers
* Added new `PreferLocations[WithFallback][RegEx]` balancers
* Added `trace.Details.String()` and `trace.Details.Strings()` helpers
* Added `trace.DetailsFromString(s)` and `trace.DetailsFromStrings(s)` helper

## 3.6.1
* Switched closing cluster after closing all sub-services
* Added windows and macOS runtimes to unit and integration tests

## 3.6.0
* Added `config/balancer` package with popular balancers
* Added new `PreferEndpoints[WithFallback][RegEx]` balancers
* Removed `config.BalancerConfig` struct
* Refactored internal packages (tree to flat, split balancers to different packages)
* Moved a taking conn to start of `conn.Invoke` /` conn.NewStream` for applying timeouts to alive conn instead lazy conn (previous logic applied timeouts to all request including dialing on lazy conn)

## 3.5.4
* Added auto-close stream result on end of stream

## 3.5.3
* Changed `Logger` interface for support custom loggers
* Added public type `LoggerOption` for proxies to internal `logger.Option`
* Fixed deadlock on table stream requests

## 3.5.2
* Fixed data race on closing table result
* Added custom dns-resolver to grpc options for use dns-balancing with round_robin balancing policy
* Wrapped with `recover()` system panic on getting system certificates pool
* Added linters and fixed issues from them
* Changed API of `sugar` package

## 3.5.1
* Added system certificates for `darwin` system
* Fixed `table.StreamResult` finishing
* Fixes `sugar.MakePath()`
* Added helper `ydb.MergeOptions()` for merge several `ydb.Option` to single `ydb.Option`

## 3.5.0
* Added `ClosabelSession` interface which extends `Session` interface and provide `Close` method
* Added `CreateSession` method into `table.Client` interface
* Added `Context` field into `trace.Driver.Net{Dial,Read,Write,Close}StartInfo` structs
* Added `Address` field into `trace.Driver.DiscoveryStartInfo` struct
* Improved logger options (provide err and out writers, provide external logger)
* Renamed package `table.resultset` to `table.result`
* Added `trace.Driver.{OnInit,OnClose}` events
* Changed unit/integration tests running
* Fixed/added YDB error checkers
* Dropped `ydb.WithDriverConfigOptions` (duplicate of `ydb.With`)
* Fixed freeze on closing driver
* Fixed `CGO` race on `Darwin` system when driver tried to expand tilde on certificates path
* Removed `EnsurePathExists` and `CleanupDatabase` from API of `scheme.Client`
* Added helpers `MakePath` and `CleanPath` to root of package `ydb-go-sdk`
* Removed call `types.Scanner.UnmarshalYDB()` inside `scanner.setDefaults()`
* Added `DoTx()` API method into `table.Client`
* Added `String()` method into `ConnectParams` for serialize params to connection string
* Added early exit from Rollback for committed transaction
* Moved `HasNextResultSet()` method from `Result` interface to common `result` interface. It provides access to `HasNextResultSet()` on both result interfaces (unary and stream results)
* Added public credentials constructors `credentials.NewAnonymousCredentials()` and `credentials.NewAccessTokenCredentials(token)`

## 3.4.4
* Prefer `ydb.table.types.Scanner` scanner implementation over `sql.Scanner`, when both available.

## 3.4.3
* Forced `round_robin` grpc load balancing instead default `pick_first`
* Added checker `IsTransportErrorCancelled`

## 3.4.2
* Simplified `Is{Transport,Operation}Error`
* Added `IsYdbError` helper

## 3.4.1
* Fixed retry reaction on operation error NotFound (non-retryable now)

## 3.4.0
* Fixed logic bug in `trace.Table.ExecuteDataQuery{Start,Done}Info`

## 3.3.3
* Cleared repeater context for discovery goroutine
* Fixed type of `trace.Details`

## 3.3.2
* Added `table.options.WithPartitioningSettings`

## 3.3.1
* Added `trace.DriverConnEvents` constant

## 3.3.0
* Stored node ID into `endpoint.Endpoint` struct
* Simplified <Host,Port> in `endpoint.Endpoint` to single fqdn Address
* On table session requests now preferred the endpoint by `ID` extracted from session `ID`. If
  endpoint by `ID` not found - using the endpoint from balancer
* Upgraded internal logger for print colored messages

## 3.2.7
* Fixed compare endpoints func

## 3.2.6
* Reverted `NodeID` as key for link between session and endpoint because yandex-cloud YDB
  installation not supported `Endpoint.ID` entity

## 3.2.5
* Dropped endpoint.Addr entity as unused. After change link type between session and endpoint
  to NodeID endpoint.Addr became unnecessary for internal logic of driver
* Enabled integration test table pool health
* Fixed race on session stream requests

## 3.2.4
* Returned context error when context is done on `session.StreamExecuteScanQuery`
  and `session.StreamReadTable`

## 3.2.3
* Fixed bug of interpret tilda in path of certificates file
* Added chapter to `README.md` about ecosystem of debug tools over `ydb-go-sdk`

## 3.2.2
* Fixed result type of `RawValue.String` (ydb string compatible)
* Fixed scans ydb types into string and slice byte receivers

## 3.2.1
* Upgraded dependencies
* Added `WithEndpoint` and `WithDatabase` Option constructors

## 3.2.0
* added package `log` with interface `log.Logger`
* implements `trace.Driver` and `trace.Table` with `log.Logger`
* added internal leveled logger which implement interface `log.Logger`
* supported environment variable `YDB_LOG_SEVERITY_LEVEL`
* changed name of the field `RetryAttempts` to` Attempts` in the structure `trace.PoolGetDoneInfo`.
  This change reduces back compatibility, but there are no external uses of v3 sdk, so this change is
  fine. We are sorry if this change broke your code

## 3.1.0
* published scheme Client interface

## 3.0.1
* refactored integration tests
* fixed table retry trace calls

## 3.0.0
* Refactored sources for splitting public interfaces and internal
  implementation for core changes in the future without change major version
* Refactored of transport level of driver - now we use grpc code generation by stock `protoc-gen-go` instead internal protoc codegen. New API provide operate from codegen grpc-clients with driver as a single grpc client connection. But driver hide inside self a pool of grpc connections to different cluster endpoints YDB. All communications with YDB (base services includes to driver: table, discovery, coordiantion and ratelimiter) provides stock codegen grpc-clients now.
* Much changed API of driver for easy usage.
* Dropped package `ydbsql` (moved to external project)
* Extracted yandex-cloud authentication to external project
* Extracted examples to external project
* Changed of traces API for next usage in jaeger и prometheus
* Dropped old APIs marked as `deprecated`
* Added integration tests with docker ydb container
* Changed table session and endpoint link type from string address to integer NodeID

## 2.11.0
* Added possibility to override `x-ydb-database` metadata value

## 2.10.9
* Fixed context cancellation inside repeater loop

## 2.10.8
* Fixed data race on cluster get/pessimize

## 2.10.7
* Dropped internal cluster connections tracker
* Switched initial connect to all endpoints after discovery to lazy connect
* Added reconnect for broken conns

## 2.10.6
* Thrown context without deadline into discovery goroutine
* Added `Address` param to `DiscoveryStartInfo` struct
* Forced `round_bobin` grpc load balancing config instead default `pick_first`
* Fixed applying driver trace from context in `connect.New`
* Excluded using session pool usage for create/take sessions in `database/sql`
  driver implementation. Package `ydbsql` with `database/sql` driver implementation
  used direct `CreateSession` table client call in the best effort loop

## 2.10.5
* Fixed panic when ready conns is zero

## 2.10.4
* Initialized repeater permanently regardless of the value `DriverConfig.DiscoveryInterval`
  This change allow forcing re-discovery depends on cluster state

## 2.10.3
* Returned context error when context is done on `StreamExecuteScanQuery`

## 2.10.2
* Fixed `mapBadSessionError()` in `ydbsql` package

## 2.10.1
* Fixed race on `ydbsql` concurrent connect. This hotfix only for v2 version

## 2.10.0
* Added `GlobalAsyncIndex` implementation of index interface

## 2.9.6
* Replaced `<session, endpoint>` link type from raw conn to plain endpoint address
* Moved checking linked endpoint from `driver.{Call,StreamRead}` to `cluster.Get`
* Added pessimization endpoint code for `driver.StreamRead` if transport error received
* Setted transport error `Cancelled` as needs to remove session from pool
* Deprecated connection use policy (used auto policy)
* Fixed goroutines leak on StreamRead call
* Fixed force re-discover on receive error after 1 second
* Added timeout to context in `cluster.Get` if context deadline not defined

## 2.9.5
* Renamed context idempotent operation flag

## 2.9.4
* Forced cancelled transport error as retriable (only idempotent operations)
* Renamed some internal retry mode types

## 2.9.3
* Forced grpc keep-alive PermitWithoutStream parameter to true

## 2.9.2
* Added errors without panic

## 2.9.1
* Added check nil grpc.ClientConn connection
* Processed nil connection error in keeper loop

## 2.9.0
* Added RawValue and supported ydb.Scanner in Scan

## 2.8.0
* Added NextResultSet for both streaming and non-streaming operations

## 2.7.0
* Dropped busy checker logic
* Refactoring of `RetryMode`, `RetryChecker` and `Retryer`
* Added fast/slow retry logic
* Supported context param for retry operation with no idempotent errors
* Added secondary indexes info to table describing method

## 2.6.1
* fix panic on lazy put to full pool

## 2.6.0
* Exported `SessionProvider.CloseSession` func
* Implements by default async closing session and putting busy
  session into pool
* Added some session pool trace funcs for execution control of
  goroutines in tests
* Switched internal session pool boolean field closed from atomic
  usage to mutex-locked usage

## 2.5.7
* Added panic on double scan per row

## 2.5.6
* Supported nil and time conventions for scanner

## 2.5.5
* Reverted adds async sessionGet and opDo into `table.Retry`.
* Added `sessionClose()` func into `SessionProvider` interface.

## 2.5.4
* Remove ready queue from session pool

## 2.5.3
* Fix put session into pool

## 2.5.2
* Fix panic on operate with result scanner

## 2.5.1
* Fix lock on write to chan in case when context is done

## 2.5.0
* Added `ScanRaw` for scan results as struct, list, tuple, map
* Created `RawScanner` interface in order to generate method With

## 2.4.1
* Fixed deadlock in the session pool

## 2.4.0
* Added new scanner API.
* Fixed dualism of interpret data (default values were deprecated for optional values)

## 2.3.3
* Fixed `internal/stats/series.go` (index out of range)
* Optimized rotate buckets in the `Series`

## 2.3.2
* Moved `api/wrap.go` to root for next replacement api package to external genproto

## 2.3.1
* Correct session pool tests
* Fixed conditions with KeepAliveMinSize and `IdleKeepAliveThreshold`

## 2.3.0
* Added credentials connect options:
  - `connect.WithAccessTokenCredentials(accessToken)`
  - `connect.WithAnonymousCredentials()`
  - `connect.WithMetadataCredentials(ctx)`
  - `connect.WithServiceAccountKeyFileCredentiials(serviceAccountKeyFile)`
* Added auth examples:
  - `example/auth/environ`
  - `example/auth/access_token_credentials`
  - `example/auth/anonymous_credentials`
  - `example/auth/metadata_credentials`
  - `example/auth/service_account_credentials`

## 2.2.1
* Fixed returning error from `table.StreamExecuteScanQuery`

## 2.2.0
* Supported loading certs from file using `YDB_SSL_ROOT_CERTIFICATES_FILE` environment variable

## 2.1.0
* Fixed erasing session from pool if session keep-alive count great then `IdleKeepAliveThreshold`
* Add major session pool config params as `connect.WithSessionPool*()` options

## 2.0.3
* Added panic for wrong `NextSet`/`NextStreamSet` call

## 2.0.2
* Fixed infinite keep alive session on transport errors `Cancelled` and `DeadlineExceeded`

## 2.0.1
* Fixed parser of connection string
* Fixed `EnsurePathExists` and `CleanupDatabase` methods
* Fixed `basic_example_v1`
* Renamed example cli flag `-link=connectionString` to `-ydb=connectionString` for connection string to YDB
* Added `-connect-timeout` flag to example cli
* Fixed some linter issues

## 2.0.0
* Renamed package ydbx to connect. New usage semantic: `connect.New()` instead `ydbx.Connect()`
* Added `healthcheck` example
* Fixed all examples with usage connect package
* Dropped `example/internal/ydbutil` package
* Simplified API of Traces - replace all pairs start/done to single handler with closure.

## 1.5.2
* Fixed `WithYdbCA` at nil certPool case

## 1.5.1
* Fixed package name of `ydbx`

## 1.5.0
* Added `ydbx` package

## 1.4.1
* Fixed `fmt.Errorf` error wrapping and some linter issues

## 1.4.0
* Added helper for create credentials from environ
* Added anonymous credentials
* Move YDB Certificate Authority from auth/iam package to root  package. YDB CA need to dial with
  dedicated YDB and not need to dial with IAM. YDB CA automatically added to all grpc calling

## 1.3.0
* Added `Compose` method to traces

## 1.2.0
* Load YDB certificates by default with TLS connection

## 1.1.0
* Support scan-query method in `ydbsql` (database/sql API)

## 1.0.7
* Use `github.com/golang-jwt/jwt` instead of `github.com/dgrijalva/jwt-go`

## 1.0.6
* Append (if not exits) SYNC Operation mode on table calls: *Session, *DataQuery, *Transaction, KeepAlive

## 1.0.5
* Remove unused ContextDeadlineMapping driver config (always used default value)
* Simplify operation params logic
* Append (if not exits) SYNC Operation mode on ExecuteDataQuery call

## 1.0.4
* Fixed timeout and cancellation setting for YDB operations
* Introduced possibility to use `ContextDeadlineNoMapping` once again

## 1.0.3
* Negative `table.Client.MaxQueryCacheSize` will disable a client query cache now
* Refactoring of `meta.go` for simple adding in the future new headers to requests
* Added support `x-ydb-trace-id` as standard SDK header

## 1.0.2
* Implements smart lazy createSession for best control of create/delete session balance. This feature fix leakage of forgotten sessions on server-side
* Some imporvements of session pool stats

## 1.0.1
* Fix closing sessions on PutBusy()
* Force setting operation timeout from client context timeout (if this timeout less then default operation timeout)
* Added helper `ydb.ContextWithoutDeadline` for clearing existing context from any deadlines

## 1.0.0
* SDK versioning switched to `Semantic Versioning 2.0.0`

## 2021.04.1
* Added `table.TimeToLiveSettings` struct and corresponding
  `table.WithTimeToLiveSettings`, `table.WithSetTimeToLive`
  and `table.WithDropTimeToLive` options.
* Deprecated `table.TTLSettings` struct alongside with
  `table.WithTTL`, `table.WithSetTTL` and `table.WithDropTTL` functions.

## 2021.03.2
* Add Truncated flag support.

## 2021.03.1
* Fixed a race between `SessionPool.Put` and `SessionPool.Get`, where the latter
  would end up waiting forever for a session that is already in the pool.

## 2021.02.1
* Changed semantics of `table.Result.O...` methods (e.g., `OUTF8`):
  it will not fail if current item is non-optional primitive.

## 2020.12.1
* added CommitTx method, which returns QueryStats

## 2020.11.4
* re-implementation of ydb.Value comparison
* fix basic examples

## 2020.11.3
* increase default and minimum `Dialer.KeepAlive` setting

## 2020.11.2
* added `ydbsql/connector` options to configure default list of `ExecDataQueryOption`

## 2020.11.1
* tune `grpc.Conn` behaviour

## 2020.10.4
* function to compare two ydb.Value

## 2020.10.3
* support scan query execution

## 2020.10.2
* add table Ttl options

## 2020.10.1
* added `KeyBloomFilter` support for `CreateTable`, `AlterTable` and `DescribeTalbe`
* added `PartitioningSettings` support for `CreateTable`, `AlterTable` and `DescribeTalbe`. Move to `PartitioningSettings` object

## 2020.09.3
* add `FastDial` option to `DriverConfig`.
  This will allow `Dialer` to return `Driver` as soon as the 1st connection is ready.

## 2020.09.2
* parallelize endpoint operations

## 2020.09.1
* added `ProcessCPUTime` method to `QueryStats`
* added `ReadReplicasSettings` support for `CreateTable`, `AlterTable` and `DescribeTalbe`
* added `StorageSettings` support for `CreateTable`, `AlterTable` and `DescribeTalbe`

## 2020.08.2
* added `PartitioningSettings` support for `CreateTable` and `AlterTable`

## 2020.08.1
* added `CPUTime` and `AffectedShards` fields to `QueryPhase` struct
* added `CompilationStats` statistics

## 2020.07.7
* support manage table attributes

## 2020.07.6
* support Column Families

## 2020.07.5
* support new types: DyNumber, JsonDocument

## 2020.07.4
* added coordination service
* added rate_limiter service

## 2020.07.3
* made `api` wrapper for `internal` api subset

## 2020.07.2
* return TableStats and PartitionStats on DescribeTable request with options
* added `ydbsql/connector` option to configure `DefaultTxControl`

## 2020.07.1
* support go modules tooling for ydbgen

## 2020.06.2
* refactored `InstanceServiceAccount`: refresh token in background.
  Also, will never produce error on creation
* added getting `ydb.Credentials` examples

## 2020.06.1

* exported internal `api.Wrap`/`api.Unwrap` methods and linked structures

## 2020.04.5

* return on discovery only endpoints that match SSL status of driver

## 2020.04.4

* added GCP metadata auth style with `InstanceServiceAccount` in `auth.iam`

## 2020.04.3

* fix race in `auth.metadata`
* fix races in test hooks

## 2020.04.2

* set limits to grpc `MaxCallRecvMsgSize` and `MaxCallSendMsgSize` to 64MB
* remove deprecated IAM (jwt) `Client` structure
* fix panic on nil dereference while accessing optional fields of `IssueMessage` message

## 2020.04.1

* added options to `DescribeTable` request
* added `ydbsql/connector` options to configure `pool`s  `KeepAliveBatchSize`, `KeepAliveTimeout`, `CreateSessionTimeout`, `DeleteTimeout`

## 2020.03.2

* set session keepAlive period to 5 min - same as in other SDKs
* fix panic on access to index after pool close

## 2020.03.1

* added session pre-creation limit check in pool
* added discovery trigger on more then half unhealthy transport connects
* await transport connect only if no healthy connections left

## 2020.02

* support cloud IAM (jwt) authorization from service account file
* minimum version of Go become 1.13. Started support of new `errors` features
