# ydb-go-sdk

[![PkgGoDev](https://pkg.go.dev/badge/github.com/ydb-platform/ydb-go-sdk/v3)](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3)
[![GoDoc](https://godoc.org/github.com/ydb-platform/ydb-go-sdk/v3?status.svg)](https://godoc.org/github.com/ydb-platform/ydb-go-sdk/v3)
![tests](https://github.com/ydb-platform/ydb-go-sdk/workflows/tests/badge.svg?branch=master)
![lint](https://github.com/ydb-platform/ydb-go-sdk/workflows/lint/badge.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ydb-platform/ydb-go-sdk/v3)](https://goreportcard.com/report/github.com/ydb-platform/ydb-go-sdk/v3)
[![codecov](https://codecov.io/gh/ydb-platform/ydb-go-sdk/branch/master/graph/badge.svg?precision=2)](https://app.codecov.io/gh/ydb-platform/ydb-go-sdk)

[YDB](https://github.com/ydb-platform/ydb) native Go's driver.

Supports `table`, `discovery`, `coordination`, `ratelimiter`, `scheme` and `scripting` clients for `YDB`.

```go
import (
  "github.com/ydb-platform/ydb-go-sdk/v3"
  "github.com/ydb-platform/ydb-go-sdk/v3/table"
  "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
  "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

db, err := ydb.New(ctx,
  ydb.WithConnectionString("grpcs://localhost:2135/?database=/local"),
  ydb.WithAnonymousCredentials(),
)
if err != nil {
  log.Fatal(err)
}
defer func() { _ = db.Close(ctx) }()
err = db.Table().Do(
  ctx,
  func(ctx context.Context, s table.Session) (err error) {
    _, res, err := s.Execute(ctx,
      table.TxControl(
        table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx(),
      ),
      "DECLARE $myStr AS Utf8; SELECT 42 as id, $myStr as myStr",
      table.NewQueryParameters(
        table.ValueParam("$myStr", types.UTF8Value("test")),
      ),
    )
    if err != nil {
      return err // for driver retry
    }
    defer func() { _ = res.Close() }()
    var (
      id    int32
      myStr *string //optional value
    )
    for res.NextResultSet(ctx) {
      for res.NextRow() {
        err := res.ScanNamed(
          named.Required("id", &id),
          named.Optional("myStr", &myStr),
        )
        if err != nil {
          return err
        }
        fmt.Printf("got id %v, got mystr: %v\n", id, *myStr)
      }
    }
    return res.Err()
  },
)
if err != nil {
  log.Fatal(err)
}
```
More examples are listed in [examples](https://github.com/ydb-platform/ydb-go-examples) repository.

## Credentials <a name="Credentials"></a>

Driver contains two options for making simple `credentials.Credentials`:
- `ydb.WithAnonymousCredentials()`
- `ydb.WithAccessTokenCredentials("token")`

Another variants to get `credentials.Credentials` object provides with external packages:

Package | Type | Description                                                                                                                                                                                 | Link of example usage
--- | --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ---
[ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc) | credentials | credentials provider for Yandex.Cloud | [yc.WithServiceAccountKeyFileCredentials](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L22) [yc.WithInternalCA](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L22) [yc.WithMetadataCredentials](https://github.com/ydb-platform/ydb-go-yc/blob/master/internal/cmd/connect/main.go#L24)
[ydb-go-yc-metadata](https://github.com/ydb-platform/ydb-go-yc-metadata) | credentials | metadata credentials provider for Yandex.Cloud | [yc.WithInternalCA](https://github.com/ydb-platform/ydb-go-yc-metadata/blob/master/options.go#L23) [yc.WithCredentials](https://github.com/ydb-platform/ydb-go-yc-metadata/blob/master/options.go#L17)
[ydb-go-sdk-auth-environ](https://github.com/ydb-platform/ydb-go-sdk-auth-environ) | credentials | create credentials from environ | [ydbEnviron. WithEnvironCredentials](https://github.com/ydb-platform/ydb-go-sdk-auth-environ/blob/master/env.go#L11)

Usage examples can be found [here](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth).

## Environment variables <a name="Environ"></a>

Name | Type | Default | Description
--- | --- | --- | ---
`YDB_SSL_ROOT_CERTIFICATES_FILE` | `string` | | path to certificates file
`YDB_LOG_SEVERITY_LEVEL` | `string` | `quiet` | severity logging level. Supported: `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `quiet`
`YDB_LOG_NO_COLOR` | `bool` | `false` | set any non empty value to disable colouring logs
`GRPC_GO_LOG_VERBOSITY_LEVEL` | `integer` | | set to `99` to see grpc logs
`GRPC_GO_LOG_SEVERITY_LEVEL` | `string` | | set to `info` to see grpc logs

## Ecosystem of debug tools over `ydb-go-sdk` <a name="Debug"></a>

Package `ydb-go-sdk` provide debugging over trace events in package `trace`.
Now supports driver events in `trace.Driver` struct and table-service events in `trace.Table` struct.
Next packages provide debug tooling:

Package | Type | Description                                                                                                                                                                                 | Link of example usage
--- | --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ---
[ydb-go-sdk-zap](https://github.com/ydb-platform/ydb-go-sdk-zap) | logging | logging ydb-go-sdk events with zap package                                                                                                                                                  | [ydbZap.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-zap/blob/master/internal/cmd/bench/main.go#L64)
[ydb-go-sdk-zerolog](https://github.com/ydb-platform/ydb-go-sdk-zap) | logging | logging ydb-go-sdk events with zerolog package                                                                                                                                              | [ydbZerolog.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-zerolog/blob/master/internal/cmd/bench/main.go#L47)
[ydb-go-sdk-metrics](https://github.com/ydb-platform/ydb-go-sdk-metrics) | metrics | common metrics of ydb-go-sdk. Package declare interfaces such as `Registry`, `GaugeVec` and `Gauge` and use it for create `trace.Driver` and `trace.Table` traces                           |
[ydb-go-sdk-prometheus](https://github.com/ydb-platform/ydb-go-sdk-prometheus) | metrics | prometheus wrapper over [ydb-go-sdk-metrics](https://github.com/ydb-platform/ydb-go-sdk-metrics) | [ydbPrometheus.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-prometheus/blob/master/internal/cmd/bench/main.go#L56)
[ydb-go-sdk-opentracing](https://github.com/ydb-platform/ydb-go-sdk-opentracing) | tracing | opentracing plugin for trace internal ydb-go-sdk calls | [ydbOpentracing.WithTraces](https://github.com/ydb-platform/ydb-go-sdk-opentracing/blob/master/internal/cmd/bench/main.go#L86)
