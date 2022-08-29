# `database/sql` driver for `YDB`

Package `ydb-go-sdk` provides usage `database/sql` API also. 
`database/sql` driver implementation use `ydb-go-sdk` native driver API's.

## Table of contents
1. [Initialization of `database/sql` driver](#init)
   * [Configure driver with `ydb.Connector` (recommended way)](#init-connector)
   * [Configure driver only with data source name (connection string)](#init-dsn)
2. [About session pool](#session-pool)
3. [Execute queries](#queries)
   * [On database object](#queries-db)
   * [With transaction](#queries-tx)
4. [Query modes (DDL, DML, DQL, etc.)](#query-modes)
5. [Retry helpers for `YDB` `database/sql` driver](#retry)
   * [Over `sql.Conn` object](#retry-conn)
   * [Over `sql.Tx`](#retry-tx)
6. [Query args types](#arg-types)
7. [Get native driver from `*sql.DB`](#unwrap)
8. [Logging SDK's events](#logs)
9. [Add metrics about SDK's events](#metrics)
10. [Add `Jaeger` traces about driver events](#jaeger)

## Initialization of `database/sql` driver <a name="init"></a>

### Configure driver with `ydb.Connector` (recommended way) <a name="init-connector"></a>
```go
import (
	"database/sql"
	
    "github.com/ydb-platform/ydb-go-sdk/v3"
)

func main() {
    // init native ydb-go-sdk driver
    nativeDriver, err := ydb.Open(context.TODO(), "grpcs://localhost:2135/local",
        // See many ydb.Option's for configure driver https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3#Option
    )
    if err != nil {
        // fallback on error
    }
    connector, err := ydb.Connector(nativeDriver,
        // See ydb.ConnectorOption's for configure connector https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3#ConnectorOption
    )
    if err != nil {
        // fallback on error
    }
    db := sql.OpenDB(connector)
}
```
### Configure driver only with data source name (connection string) <a name="init-dsn"></a>
```go
import (
	"database/sql"
	
    _ "github.com/ydb-platform/ydb-go-sdk/v3"
)

func main() {
    db, err := sql.Open("ydb", "grpcs://localhost:2135/local")
}
```
Data source name parameters:
* `token` – access token to be used during requests (required)
* static credentials with authority part of URI, like `grpcs://root:password@localhost:2135/local`
* `query_mode=scripting` - you can redefine default [DML](https://en.wikipedia.org/wiki/Data_manipulation_language) query mode

## About session pool <a name="session-pool"></a>

Native driver `ydb-go-sdk/v3` implements internal session pool, which uses with `db.Table().Do()` or `db.Table().DoTx()` methods.
Internal session pool configures with options like `ydb.WithSessionPoolSizeLimit()` and other.
Unlike session pool in native driver, `database/sql` contains another session pool, which configures with `*sql.DB.SetMaxOpenConns` and `*sql.DB.SetMaxIdleConns`.
Lifetime of `YDB` session depends on driver configuration and predefined errors, such as `sql.driver.ErrBadConn`.
`YDB` driver for `database/sql` transform internal `YDB` errors into `sql.driver.ErrBadConn` depending on result of internal error check (delete session on error or not and other).
In most cases this behaviour of `database/sql` driver implementation for specific RDBMS completes queries without result errors.
But some errors on unfortunate cases may be get on client-side.
That's why querying must be wrapped with retry helpers, such as `retry.Do` or `retry.DoTx` (see more about retry helpers in [retry section](#retry)).

## Execute queries <a name="queries"></a>

### On database object <a name="queries-db"></a>
```go
rows, err := db.QueryContext(ctx,
    "SELECT series_id, title, release_date FROM /full/path/of/table/series;"
)
if err != nil {
    log.Fatal(err)
}
defer rows.Close() // always close rows 
var (
    id          *string
    title       *string
    releaseDate *time.Time
)
for rows.Next() { // iterate over rows
    // apply database values to go's type destinations
    if err = rows.Scan(&id, &title, &releaseDate); err != nil {
        log.Fatal(err)
    }
    log.Printf("> [%s] %s (%s)", *id, *title, releaseDate.Format("2006-01-02"))
}
if err = rows.Err(); err != nil { // always check final rows err
    log.Fatal(err)
}
```

### With transaction <a name="queries-tx"></a>
Supports only `default` transaction options which mapped to `YDB` `SerializableReadWrite` transaction settings.

`YDB`'s `OnlineReadOnly` and `StaleReadOnly` transaction settings are not compatible with interactive transactions such as `database/sql`'s `*sql.Tx`.
That's why `ydb-go-sdk` implements read-only `sql.LevelSnapshot` with fake transaction (temporary, while `YDB` main clusters are supports true snapshot isolation mode)
```go
tx, err := db.BeginTx(ctx, sql.TxOptions{})
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()
rows, err := tx.QueryContext(ctx,
    "SELECT series_id, title, release_date FROM /full/path/of/table/series;"
)
if err != nil {
    log.Fatal(err)
}
defer rows.Close() // always close rows 
var (
    id          *string
    title       *string
    releaseDate *time.Time
)
for rows.Next() { // iterate over rows
    // apply database values to go's type destinations
    if err = rows.Scan(&id, &title, &releaseDate); err != nil {
        log.Fatal(err)
    }
    log.Printf("> [%s] %s (%s)", *id, *title, releaseDate.Format("2006-01-02"))
}
if err = rows.Err(); err != nil { // always check final rows err
    log.Fatal(err)
}
if err = tx.Commit(); err != nil {
    log.Fatal(err)
}
```
## Query modes (DDL, DML, DQL, etc.) <a name="query-modes"></a>
The `YDB` server API is currently requires to select a specific method by specific request type. For example, [DDL](https://en.wikipedia.org/wiki/Data_definition_language) must be called with `table.session.ExecuteSchemeQuery`, [DML](https://en.wikipedia.org/wiki/Data_manipulation_language) must be called with `table.session.Execute`, [DQL](https://en.wikipedia.org/wiki/Data_query_language) may be called with `table.session.Execute` or `table.session.StreamExecuteScanQuery` etc. `YDB` have a `scripting` service also, which provides different query types with single method, but not supports transactions.

That's why needs to select query mode on client side currently.

`YDB` team have a roadmap goal to implements a single method for executing different query types.

`database/sql` driver implementation for `YDB` supports next query modes:
* `ydb.DataQueryMode` - default query mode, for lookup [DQL](https://en.wikipedia.org/wiki/Data_query_language) queries and [DML](https://en.wikipedia.org/wiki/Data_manipulation_language) queries.
* `ydb.ExplainQueryMode` - for gettting plan and [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree) of some query
* `ydb.ScanQueryMode` - for strong [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) scenarious, [DQL-only](https://en.wikipedia.org/wiki/Data_query_language) queries. Read more about scan queries in [ydb.tech](https://ydb.tech/en/docs/concepts/scan_query)
* `ydb.SchemeQueryMode` - for [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries
* `ydb.ScriptingQueryMode` - for [DDL](https://en.wikipedia.org/wiki/Data_definition_language), [DML](https://en.wikipedia.org/wiki/Data_manipulation_language), [DQL](https://en.wikipedia.org/wiki/Data_query_language) queries (not a [TCL](https://en.wikipedia.org/wiki/SQL#Transaction_controls)). Be careful: queries executes longer than with other query modes and consumes bigger server-side resources

Example for changing default query mode:
```go
res, err = db.ExecContext(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
   "DROP TABLE `/full/path/to/table/series`",
)
```

## Specify `YDB` transaction control <a name="tx-control"></a>

Default `YDB`'s transaction control is a `SerializableReadWrite`. 
Default transaction control outside interactive transactions may be changed with context:
```
ctx := ydb.WithTxControl(ctx, table.OnlineReadOnlyTxControl())
```

## Retry helpers for `YDB` `database/sql` driver <a name="retry"></a>

`YDB` is a distributed `RDBMS` with non-stop 24/7 releases flow.
It means some nodes may be not available for queries.
Also network errors may be occurred.
That's why some queries may be complete with errors.
Most of those errors are transient.
`ydb-go-sdk`'s "knows" what to do on specific error: retry or not, with or without backoff, with or whithout deleting session, etc.
`ydb-go-sdk` provides retry helpers which can work either with plain database connection object, or with interactive transaction object.

### Over `sql.Conn` object <a name="retry-conn"></a>

`retry.Do` helper allow custom lambda, which must return error for processing or nil if retry operation is ok.
```
import (
    "github.com/ydb-platform/ydb-go-sdk/v3/retry"
)
...
err := retry.Do(context.TODO(), db, func(ctx context.Context, cc *sql.Conn) error {
   // work with cc
   rows, err := cc.QueryContext(ctx, "SELECT 1;")
   if err != nil {
       return err // return err to retryer
   }
   ...
   return nil // good final of retry operation
}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))

```

### Over `sql.Tx` <a name="retry-tx"></a>

`retry.DoTx` helper allow custom lambda, which must return error for processing or nil if retry operation is ok.

`tx` object is a prepared transaction object which not requires commit or rollback at the end - `retry.DoTx` do it automatically.    
```
import (
    "github.com/ydb-platform/ydb-go-sdk/v3/retry"
)
...
err := retry.DoTx(context.TODO(), db, func(ctx context.Context, tx *sql.Tx) error {
    // work with tx
    rows, err := tx.QueryContext(ctx, "SELECT 1;")
    if err != nil {
        return err // return err to retryer
    }
    ...
    return nil // good final of retry tx operation
}, retry.WithDoTxRetryOptions(
    retry.WithIdempotent(true),
), retry.WithTxOptions(&sql.TxOptions{
    Isolation: sql.LevelSnapshot,
    ReadOnly:  true,
}))
```

## Query args types <a name="arg-types"></a>

`database/sql` driver for `YDB` supports next types of query args:
* multiple `sql.NamedArg` (uniform `database/sql` arg)
   ```
   rows, err := cc.QueryContext(ctx, `
          DECLARE $seasonTitle AS Utf8;
          DECLARE $views AS Uint64;
          SELECT season_id FROM seasons WHERE title LIKE $seasonTitle AND vews > $views;
      `,
      sql.Named("seasonTitle", "%Season 1%"),
      sql.Named("views", uint64(1000)),
   )
   ```
* multiple native for `ydb-go-sdk` `table.ParameterOption` which constructs from `table.ValueParam("name", value)`
   ```
   rows, err := cc.QueryContext(ctx, `
          DECLARE $seasonTitle AS Utf8;
          DECLARE $views AS Uint64;
          SELECT season_id FROM seasons WHERE title LIKE $seasonTitle AND vews > $views;
      `,
      table.ValueParam("seasonTitle", types.TextValue("%Season 1%")),
      table.ValueParam("views", types.Uint64Value((1000)),
   )
   ```
* single native for `ydb-go-sdk` `*table.QueryParameters` which constructs from `table.NewQueryParameters(parameterOptions...)`
   ```
   rows, err := cc.QueryContext(ctx, `
          DECLARE $seasonTitle AS Utf8;
          DECLARE $views AS Uint64;
          SELECT season_id FROM seasons WHERE title LIKE $seasonTitle AND vews > $views;
      `,
      table.NewQueryParameters(
          table.ValueParam("seasonTitle", types.TextValue("%Season 1%")),
          table.ValueParam("views", types.Uint64Value((1000)),
      ),
   )
   ```
## Get native driver from `*sql.DB` <a name="unwrap"></a>

```go
db, err := sql.Open("ydb", "grpcs://localhost:2135/local")
if err != nil {
    t.Fatal(err)
}
nativeDriver, err = ydb.Unwrap(db)
if err != nil {
    t.Fatal(err)
}
nativeDriver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
    // doing with native YDB session
    return nil
})
```

## Logging driver events <a name="logging"></a>

Adding a logging driver events allowed only if connection to `YDB` opens over [connector](##init-connector).
Adding of logging provides with [debug adapters](README.md#debug) and wrotes in [migration notes](MIGRATION_v2_v3.md#logs)
Example of adding `zap` logging:
```go
import (
    "github.com/ydb-platform/ydb-go-sdk/v3/trace"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
)
...
nativeDriver, err := ydb.Open(ctx, connectionString,
    ...
    ydbZap.WithTraces(log, trace.DetailsAll),
)
if err != nil {
    // fallback on error
}
connector, err := ydb.Connector(nativeDriver)
if err != nil {
    // fallback on error
}
db := sql.OpenDB(connector)
```

## Add metrics about SDK's events <a name="metrics"></a>

Adding of driver events monitoring allowed only if connection to `YDB` opens over [connector](##init-connector).
Monitoring of driver events provides with [debug adapters](README.md#debug) and wrotes in [migration notes](MIGRATION_v2_v3.md#metrics)
Example of adding `Prometheus` monitoring:
```go
import (
    "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    ydbMetrics "github.com/ydb-platform/ydb-go-sdk-prometheus"
)
...
nativeDriver, err := ydb.Open(ctx, connectionString,
    ...
    ydbMetrics.WithTraces(registry, ydbMetrics.WithDetails(trace.DetailsAll)),
)
if err != nil {
    // fallback on error
}
connector, err := ydb.Connector(nativeDriver)
if err != nil {
    // fallback on error
}
db := sql.OpenDB(connector)
```

## Add `Jaeger` traces about driver events <a name="jaeger"></a>

Adding of `Jaeger` traces about driver events allowed only if connection to `YDB` opens over [connector](##init-connector).
`Jaeger` tracing provides with [debug adapters](README.md#debug) and wrotes in [migration notes](MIGRATION_v2_v3.md#jaeger)
Example of adding `Jaeger` tracing:
```go
import (
    "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    ydbOpentracing "github.com/ydb-platform/ydb-go-sdk-opentracing"
)
...
nativeDriver, err := ydb.Open(ctx, connectionString,
    ...
    ydbOpentracing.WithTraces(trace.DriverConnEvents | trace.DriverClusterEvents | trace.DriverRepeaterEvents | trace.DiscoveryEvents),
)
if err != nil {
    // fallback on error
}
connector, err := ydb.Connector(nativeDriver)
if err != nil {
    // fallback on error
}
db := sql.OpenDB(connector)
```
