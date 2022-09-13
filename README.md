# `ydb-go-sdk` - pure Go native and `database/sql` driver for [YDB](https://github.com/ydb-platform/ydb)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![Release](https://img.shields.io/github/v/release/ydb-platform/ydb-go-sdk.svg?style=flat-square)](https://github.com/ydb-platform/ydb-go-sdk/releases)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/ydb-platform/ydb-go-sdk/v3)](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3)
![tests](https://github.com/ydb-platform/ydb-go-sdk/workflows/tests/badge.svg?branch=master)
![lint](https://github.com/ydb-platform/ydb-go-sdk/workflows/lint/badge.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ydb-platform/ydb-go-sdk/v3)](https://goreportcard.com/report/github.com/ydb-platform/ydb-go-sdk/v3)
[![codecov](https://codecov.io/gh/ydb-platform/ydb-go-sdk/branch/master/graph/badge.svg?precision=2)](https://app.codecov.io/gh/ydb-platform/ydb-go-sdk)
![Code lines](https://sloc.xyz/github/ydb-platform/ydb-go-sdk/?category=code)
[![View examples](https://img.shields.io/badge/learn-examples-brightgreen.svg)](https://github.com/ydb-platform/ydb-go-examples)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/YDBPlatform)
[![WebSite](https://img.shields.io/badge/website-ydb.tech-blue.svg)](https://ydb.tech)

Supports `table`, `discovery`, `coordination`, `ratelimiter`, `scheme`, `scripting` and `topic` clients for [YDB](https://ydb.tech).
`YDB` is an open-source Distributed SQL Database that combines high availability and scalability with strict consistency and [ACID](https://en.wikipedia.org/wiki/ACID) transactions.
`YDB` was created primarily for [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) workloads and supports some [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) scenarious.

## Installation

```sh
go get -u github.com/ydb-platform/ydb-go-sdk/v3
```

## Example Usage <a name="example"></a>

* connect to YDB
```golang
db, err := ydb.Open(ctx, "grpcs://localhost:2135/local")
if err != nil {
    log.Fatal(err)
}
```
* execute `SELECT` query
 ```golang
const query = `SELECT 42 as id, "myStr" as myStr;`

// Do retry operation on errors with best effort
queryErr := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
    _, res, err := s.Execute(ctx, table.DefaultTxControl(), query, nil)
    if err != nil {
        return err
    }
    defer res.Close()
    if err = res.NextResultSetErr(ctx); err != nil {
        return err
    }
    for res.NextRow() {
        var id    int32
        var myStr string
        err = res.ScanNamed(named.Required("id", &id),named.OptionalWithDefault("myStr", &myStr))
        if err != nil {
            log.Fatal(err)
        }
        log.Printf("id=%v, myStr='%s'\n", id, myStr)
    }
    return res.Err() // for driver retry if not nil
})
if queryErr != nil {
    log.Fatal(queryErr)
}
```
* usage with `database/sql` (see additional docs in [SQL.md](SQL.md) )
```golang
import (
    "context"
    "database/sql"
    "log"

    _ "github.com/ydb-platform/ydb-go-sdk/v3"
)

...

db, err := sql.Open("ydb", "grpcs://localhost:2135/local")
if err != nil {
    log.Fatal(err)
}
defer db.Close() // cleanup resources
var (
    id    int32
    myStr string
)
row := db.QueryRowContext(context.TODO(), `SELECT 42 as id, "my string" as myStr`)
if err = row.Scan(&id, &myStr); err != nil {
    log.Printf("select failed: %v", err)
    return
}
log.Printf("id = %d, myStr = \"%s\"", id, myStr)
```


More examples of usage placed in [examples](https://github.com/ydb-platform/ydb-go-examples) repository.

## Credentials <a name="credentials"></a>

Driver implements several ways for making credentials for `YDB`:
- `ydb.WithAnonymousCredentials()` (enabled by default unless otherwise specified)
- `ydb.WithAccessTokenCredentials("token")`
- `ydb.WithStaticCredentials("user", "password")`, 
- as part of connection string, like as `grpcs://user:password@endpoint/database`

Another variants of `credentials.Credentials` object provides with external packages:

| Package                                                                            | Type        | Description                                    | Link of example usage                                                                                                                                                                                                                                                                                                                                              |
|------------------------------------------------------------------------------------|-------------|------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc)                             | credentials | credentials provider for Yandex.Cloud          | [yc.WithServiceAccountKeyFileCredentials](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L22) [yc.WithInternalCA](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L22) [yc.WithMetadataCredentials](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L24) |
| [ydb-go-yc-metadata](https://github.com/ydb-platform/ydb-go-yc-metadata)           | credentials | metadata credentials provider for Yandex.Cloud | [yc.WithInternalCA](https://github.com/ydb-platform/ydb-go-yc-metadata/blob/master/options.go#L23) [yc.WithCredentials](https://github.com/ydb-platform/ydb-go-yc-metadata/blob/master/options.go#L17)                                                                                                                                                             |
| [ydb-go-sdk-auth-environ](https://github.com/ydb-platform/ydb-go-sdk-auth-environ) | credentials | create credentials from environ                | [ydbEnviron.WithEnvironCredentials](https://github.com/ydb-platform/ydb-go-sdk-auth-environ/blob/master/env.go#L11)                                                                                                                                                                                                                                                |

## Ecosystem of debug tools over `ydb-go-sdk` <a name="debug"></a>

Package `ydb-go-sdk` provide debugging over trace events in package `trace`.
Next packages provide debug tooling:

| Package                                                                          | Type    | Description                                                                                                               | Link of example usage                                                                                                          |
|----------------------------------------------------------------------------------|---------|---------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| [ydb-go-sdk-zap](https://github.com/ydb-platform/ydb-go-sdk-zap)                 | logging | logging ydb-go-sdk events with zap package                                                                                | [ydbZap.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-zap/blob/master/internal/cmd/bench/main.go#L64)                 |
| [ydb-go-sdk-zerolog](https://github.com/ydb-platform/ydb-go-sdk-zap)             | logging | logging ydb-go-sdk events with zerolog package                                                                            | [ydbZerolog.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-zerolog/blob/master/internal/cmd/bench/main.go#L47)         |
| [ydb-go-sdk-metrics](https://github.com/ydb-platform/ydb-go-sdk-metrics)         | metrics | common metrics of ydb-go-sdk. Package declare interfaces such as `Registry`, `GaugeVec` and `Gauge` and use it for traces |                                                                                                                                |
| [ydb-go-sdk-prometheus](https://github.com/ydb-platform/ydb-go-sdk-prometheus)   | metrics | prometheus wrapper over [ydb-go-sdk-metrics](https://github.com/ydb-platform/ydb-go-sdk-metrics)                          | [ydbPrometheus.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-prometheus/blob/master/internal/cmd/bench/main.go#L56)   |
| [ydb-go-sdk-opentracing](https://github.com/ydb-platform/ydb-go-sdk-opentracing) | tracing | opentracing plugin for trace internal ydb-go-sdk calls                                                                    | [ydbOpentracing.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-opentracing/blob/master/internal/cmd/bench/main.go#L86) |

## Environment variables <a name="environ"></a>

`ydb-go-sdk` supports next environment variables  which redefines default behavior of driver

| Name                             | Type      | Default | Description                                                                                                              |
|----------------------------------|-----------|---------|--------------------------------------------------------------------------------------------------------------------------|
| `YDB_SSL_ROOT_CERTIFICATES_FILE` | `string`  |         | path to certificates file                                                                                                |
| `YDB_LOG_SEVERITY_LEVEL`         | `string`  | `quiet` | severity logging level of internal driver logger. Supported: `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet` |
| `YDB_LOG_DETAILS`                | `string`  | `.*`    | regexp for lookup internal logger logs                                                                                   |
| `GRPC_GO_LOG_VERBOSITY_LEVEL`    | `integer` |         | set to `99` to see grpc logs                                                                                             |
| `GRPC_GO_LOG_SEVERITY_LEVEL`     | `string`  |         | set to `info` to see grpc logs                                                                                           |
