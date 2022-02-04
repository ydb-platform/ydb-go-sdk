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
