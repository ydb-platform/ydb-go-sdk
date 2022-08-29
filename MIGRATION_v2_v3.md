# Migration from `ydb-go-sdk/v2` to `ydb-go-sdk/v3`

> Article contains some cases for migrate from `github.com/yandex-cloud/ydb-go-sdk/v2` to `github.com/ydb-platform/ydb-go-sdk/v3`

## Table of contents
1. [Imports](#imports)
2. [Connect to `YDB` by `endpoint` and `database`](#connect)
3. [Connect to `YDB` using connection string](#connect-dsn)
4. [Make table client and session pool](#table-client)
5. [Execute query with table client and session pool](#execute-queries)
6. [Scans query result into local variables](#scan-result)
7. [Logging SDK's events](#logs)
8. [Add metrics obout SDK's events](#metrics)
9. [Add `Jaeger` traces about SDK's events](#jaeger)

## Imports <a name="imports"></a>
- in `v2`: 
  ```
  "github.com/yandex-cloud/ydb-go-sdk/v2"
  "github.com/yandex-cloud/ydb-go-sdk/v2/table"
  ```
- in `v3`: 
  ```
  "github.com/ydb-platform/ydb-go-sdk/v3"
  "github.com/ydb-platform/ydb-go-sdk/v3/table"
  ```  

## Connect to `YDB` by `endpoint` and `database` <a name="connect"></a>
- in `v2`: 
  ```go
  config := &ydb.DriverConfig{
    Database: cfg.Database,
  }
  driver, err := (&ydb.Dialer{
    DriverConfig: config,
  }).Dial(ctx, cfg.Addr)
  if err != nil {
    // error fallback
  }
  defer func() {
    _ = driver.Close()
  }()
  ```
- in `v3`: 
  ```go
  import (
    "github.com/ydb-platform/ydb-go-sdk/v3/sugar"
  )
  ...
  db, err := ydb.Open(ctx,
    sugar.DSN(cfg.Endpoint, cfg.Database, cfg.Secure)
  )
  if err != nil {
    // error fallback
  }
  defer func() {
    _ = db.Close(ctx)
  }()
  ```  

## Connect to `YDB` using connection string <a name="connect-dsn"></a>
- in `v2`: 
  ```go
  import (
    "github.com/yandex-cloud/ydb-go-sdk/v2/connect"
  )
  ...
  params, err := connect.ConnectionString("grpc://ydb-ru.yandex.net:2135/?database=/ru/home/my/db")
  if err != nil {
    // error fallback
  }
  ...
  config.Database = params.Database()
  ...
  driver, err := (&ydb.Dialer{
    DriverConfig: config,
  }).Dial(ctx, params.Endpoint())
  ```
- in `v3`: 
  ```go
  db, err := ydb.Open(ctx,
    "grpc://ydb-ru.yandex.net:2135/ru/home/my/db",
  )
  if err != nil {
    // error fallback
  }
  defer func() {
    _ = db.Close(ctx)
  }()
  ```

## Make table client and session pool <a name="table-client"></a>
- in `v2`: 
  ```go
  import (
    "github.com/yandex-cloud/ydb-go-sdk/v2/table"
  )
  ...
  tableClient := &table.Client{
    Driver: driver,
  }
  sp := &table.SessionPool{
    Builder: tableClient,
  }
  defer func() {
    _ = sp.Close(ctx)
  }()
  ```
- in `v3`: nothing to do, table client with internal session pool always available with `db.Table()`

## Execute query with table client and session pool <a name="execute-queries"></a>
- in `v2`: 
  ```go
  var res *table.Result
  err := table.Retry(ctx, sp,
    table.OperationFunc(
        func(ctx context.Context, s *table.Session) (err error) {
            _, res, err = s.Execute(ctx, readTx, "SELECT 1+1")
            return err
        },
    )
  )
  if err != nil {
    // error fallback
  }
  ```
- in `v3`: 
  ```go
  import (
    "github.com/ydb-platform/ydb-go-sdk/v3/table/result"  
  )
  ...
  var res result.Result
  err := db.Table().Do(ctx,
    func(ctx context.Context, s table.Session) (err error) {
        _, res, err = s.Execute(ctx, readTx, "SELECT 1+1")
        return err
    },
    table.WithIdempotent(), // only idempotent queries
  )
  if err != nil {
    // error fallback
  }
  ```  

## Scans query result into local variables <a name="scan-result"></a>
- in `v2`: 
  ```go
  var (
    id    uint64
    title string
    date  uint64
    description string
    duration uint64
  )
  for res.NextStreamSet(ctx) {
    for res.NextRow() {
        res.SeekItem("series_id")
        id = res.OUint64()
  
        res.SeekItem("title")
        title = res.OUTF8()
  
        res.SeekItem("release_date")
        date = res.OUint64()
  
        res.SeekItem("description")
        description = res.OUTF8()
  
        res.SeekItem("duration")
        duration = res.OUint64()
  
        log.Printf("#  %d %s %s %s %v",
            id, title, time.UnixMilli(date).Format("02/01/2006, 15:04:05"),
            description, time.Duration(duration) * time.Millisecond,
        )
    }
  }
  if err := res.Err(); err != nil {
    // error fallback
  }
  ```
- in `v3`: 
  ```go
  import (
    "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"  
  )
  ...
  var (
    id    uint64
    title *string
    date  *time.Time
    description *string
    duration time.Duration
  )
  for res.NextResultSet(ctx) {
    for res.NextRow() {
        err := res.ScanNamed(
            named.Required("series_id", &id),
            named.Optional("title", &title),
            named.Optional("release_date", &date),
            named.Optional("description", &description),
            named.OptionalWithDefault("duration", &duration),
        )
        if err != nil {
            // error fallback
        }
        log.Printf("#  %d %s %s %s %v",
            id, title, date.Format("02/01/2006, 15:04:05"),
            description, duration,
        )
    }
  }
  if err := res.Err(); err != nil {
    // error fallback
  }
  ```  

## Logging SDK's events <a name="logs"></a>
- in `v2`: 
  ```go
  config.Trace = ydb.DriverTrace{
    OnDial: func(info ydb.DialStartInfo) func(info ydb.DialDoneInfo) {
        address := info.Address
        fmt.Printf(`dial start {address:"%s"}`, address)
        start := time.Now()
        return func(info ydb.DialDoneInfo) {
            if info.Error == nil {
                fmt.Printf(`dial done {latency:"%s",address:"%s"}`, time.Since(start), address)
            } else {
                fmt.Printf(`dial failed {latency:"%s",address:"%s",error:"%s"}`, time.Since(start), address, info.Error)
            }
        }
    },
    // ... and more callbacks of ydb.DriverTrace need to define  
  }
  sp.Trace = table.Trace{
    // must callbacks of table.Trace  
  }
  ```
- in `v3`:
  * `ydb-go-sdk/v3` contains internal logger, which may to enable with env `YDB_LOG_SEVERITY_LEVEL=info`
  * external `zap` logger:
    ```go
    import ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
    ...
    db, err := ydb.Open(ctx, connectionString, 
        ...
        ydbZap.WithTraces(log, trace.DetailsAll),
    )
    ```
  * external `zerolog` logger:
    ```go
    import ydbZerolog "github.com/ydb-platform/ydb-go-sdk-zerolog"
    ...
    db, err := ydb.Open(ctx, connectionString, 
       ...
       ydbZerolog.WithTraces(log, trace.DetailsAll),
    )
    ```

## Add metrics obout SDK's events <a name="metrics"></a>
- in `v2`: 
  ```go
  config.Trace = ydb.DriverTrace{
    // must define callbacks of ydb.DriverTrace  
  }
  sp.Trace = table.Trace{
    // must define callbacks of table.Trace  
  }
  ```
- in `v3`: 
  * metrics into `Prometheus` system
    ```go
    import (
       ydbMetrics "github.com/ydb-platform/ydb-go-sdk-prometheus"
    )
    ...
    db, err := ydb.Open(ctx, connectionString,
      ...
      ydbMetrics.WithTraces(log, trace.DriverConnEvents | trace.DriverClusterEvents),
    )
    ```
  * metrics to other monitoring systems may be add with common package `"github.com/ydb-platform/ydb-go-sdk-metrics"`

## Add `Jaeger` traces about SDK's events <a name="jaeger"></a>
- in `v2`: 
  ```go
  config.Trace = ydb.DriverTrace{
    // must define callbacks of ydb.DriverTrace  
  }
  sp.Trace = table.Trace{
    // must define callbacks of table.Trace  
  }
  ```
- in `v3`: 
  ```go
  import (
    ydbTracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
  )
  ...
  db, err := ydb.Open(ctx, connectionString,
    ...
    ydbTracing.WithTraces(log, trace.DriverConnEvents | trace.DriverClusterEvents | trace.DriverRepeaterEvents | trace.DiscoveryEvents),
  )
  ```  

See additional docs in [code recipes](https://ydb.tech/docs/reference/ydb-sdk/recipes/).