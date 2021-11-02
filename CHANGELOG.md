## 3.2.8
* Stored node ID into `endpoint.Endpoint` struct. 
* On table session requests now preferred the endpoint by `ID` extracted from session `ID`. If 
  endpoint by `ID` not found - using the endpoint from balancer 

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
