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
More examples of usage placed in [examples](https://github.com/ydb-platform/ydb-go-examples) repository.

See also [CREDENTIALS.md](CREDENTIALS.md) about supported YDB credentials.

See [DEBUG.md](DEBUG.md) about supported debug tooling over `ydb-go-sdk`.

See [ENVIRON.md](ENVIRON.md) about supported environment variables of `ydb-go-sdk`.
