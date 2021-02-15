##2021.02.1
* Changed semantics of `table.Result.O...` methods (e.g., `OUTF8`):
  it will not fail it current item is non-optional primitive.

##2020.12.1
* added CommitTx method, which returns QueryStats

##2020.11.4
* re-implementation of ydb.Value comparison
* fix basic examples

##2020.11.3
* increase default and minimum `Dialer.KeepAlive` setting

##2020.11.2
* added `ydbsql/connector` options to configure default list of `ExecDataQueryOption`

##2020.11.1
* tune `grpc.Conn` behaviour

##2020.10.4
* function to compare two ydb.Value

##2020.10.3
* support scan query execution

##2020.10.2
* add table Ttl options

##2020.10.1
* added `KeyBloomFilter` support for `CreateTable`, `AlterTable` and `DescribeTalbe`
* added `PartitioningSettings` support for `CreateTable`, `AlterTable` and `DescribeTalbe`. Move to `PartitioningSettings` object

##2020.09.3
* add `FastDial` option to `DriverConfig`.
  This will allow `Dialer` to return `Driver` as soon as the 1st connection is ready.

##2020.09.2
* parallelize endpoint operations

##2020.09.1
* added `ProcessCPUTime` method to `QueryStats`
* added `ReadReplicasSettings` support for `CreateTable`, `AlterTable` and `DescribeTalbe`
* added `StorageSettings` support for `CreateTable`, `AlterTable` and `DescribeTalbe`

##2020.08.2
* added `PartitioningSettings` support for `CreateTable` and `AlterTable`

##2020.08.1
* added `CPUTime` and `AffectedShards` fields to `QueryPhase` struct
* added `CompilationStats` statistics

##2020.07.7
* support manage table attributes

##2020.07.6
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
