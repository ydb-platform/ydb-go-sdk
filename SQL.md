# `database/sql` driver for `YDB`

In addition to native YDB Go driver APIs, package `ydb-go-sdk` provides standard APIs for `database/sql`.
It allows to use "regular" Go facilities to access YDB databases.
Behind the scene, `database/sql` APIs are implemented using the native interfaces.


## Table of contents
1. [Initialization of `database/sql` driver](#init)
   * [Configure driver with `ydb.Connector` (recommended way)](#init-connector)
   * [Configure driver with data source name or connection string](#init-dsn)
2. [Session pooling](#session-pool)
3. [Query execution](#queries)
   * [Queries on database object](#queries-db)
   * [Queries on transaction object](#queries-tx)
4. [Query modes (DDL, DML, DQL, etc.)](#query-modes)
5. [Changing the transaction control mode](#tx-control)
6. [Retry helpers for `YDB` `database/sql` driver](#retry)
   * [Retries over `sql.Conn` object](#retry-conn)
   * [Retries over `sql.Tx`](#retry-tx)
7. [Specifying query parameters](#arg-types)
8. [Accessing the native driver from `*sql.DB`](#unwrap)

## Initialization of `database/sql` driver <a name="init"></a>

### Configure driver with `ydb.Connector` (recommended way) <a name="init-connector"></a>
```go
import (
    "github.com/ydb-platform/ydb-go-sdk/v3"
)

func main() {
    // init native ydb-go-sdk driver
    nativeDriver, err := ydb.Open(context.TODO(), "grpcs://localhost:2135/local",
        // See many ydb.Option's for configure driver
    )
    if err != nil {
        // fallback on error
    }
    connector, err := ydb.Connector(nativeDriver,
        // See ydb.ConnectorOption's for configure connector
    )
    if err != nil {
        // fallback on error
    }
    db := sql.OpenDB(connector)
}
```

### Configure driver with data source name or connection string <a name="init-dsn"></a>
```go
import (
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

## Session pooling <a name="session-pool"></a>

Native driver `ydb-go-sdk/v3` implements the internal session pool, which uses with `db.Table().Do()` or `db.Table().DoTx()` methods.
Internal session pool is configured with options like `ydb.WithSessionPoolSizeLimit()` and other.
Unlike the session pool in the native driver, `database/sql` contains a different implementation of the session pool, which is configured with `*sql.DB.SetMaxOpenConns` and `*sql.DB.SetMaxIdleConns`.
Lifetime of a `YDB` session depends on driver configuration and error occurance, such as `sql.driver.ErrBadConn`.
`YDB` driver for `database/sql` includes the logic to transform the internal `YDB` error codes into `sql.driver.ErrBadConn` and other retryable and non-retryable error types.

In most cases the implementation of `database/sql` driver for YDB allows to complete queries without user-visible errors.
But some errors need to be handled on the client side, by re-running not a single operation, but a complete transaction.
Therefore the transaction logic needs to be wrapped with retry helpers, such as `retry.Do` or `retry.DoTx` (see more about retry helpers in the [retry section](#retry)).

## Query execution <a name="queries"></a>

### Queries on database object <a name="queries-db"></a>
```go
rows, err := db.QueryContext(ctx,
    "SELECT series_id, title, release_date FROM `/full/path/of/table/series`;"
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

### Queries on transaction object <a name="queries-tx"></a>
Query execution on transaction object supports only `default` transaction options
which are mapped to `YDB` `SerializableReadWrite` transaction settings.

`YDB`'s `OnlineReadOnly` and `StaleReadOnly` transaction settings are not compatible
with interactive transactions such as `database/sql`'s `*sql.Tx`.
That's why `ydb-go-sdk` implements read-only `sql.LevelSnapshot` with fake transaction
(temporary, until `YDB` starts to support true snapshot isolation mode).

```go
tx, err := db.BeginTx(ctx, sql.TxOptions{})
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()
rows, err := tx.QueryContext(ctx,
    "SELECT series_id, title, release_date FROM `/full/path/of/table/series`;"
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
Currently the `YDB` server APIs require the use of a proper GRPC service method depending on the specific request type.
In particular, [DDL](https://en.wikipedia.org/wiki/Data_definition_language) must be called through `table.session.ExecuteSchemeQuery`,
[DML](https://en.wikipedia.org/wiki/Data_manipulation_language) needs `table.session.Execute`, and
[DQL](https://en.wikipedia.org/wiki/Data_query_language) should be passed via `table.session.Execute` or `table.session.StreamExecuteScanQuery`.
`YDB` also has a so-called "scripting" service, which supports different query types within a single method,
but without support for transactions.

Unfortunately, this leads to the need to choose the proper query mode on the application side.

`YDB` team has a roadmap goal to implement a single universal service method for executing
different query types and without the limitations of the "scripting" service method.

`database/sql` driver implementation for `YDB` supports the following query modes:
* `ydb.DataQueryMode` - default query mode, for lookup [DQL](https://en.wikipedia.org/wiki/Data_query_language) queries and [DML](https://en.wikipedia.org/wiki/Data_manipulation_language) queries.
* `ydb.ExplainQueryMode` - for gettting plan and [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree) of the query
* `ydb.ScanQueryMode` - for heavy [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) style scenarious, with [DQL-only](https://en.wikipedia.org/wiki/Data_query_language) queries. Read more about scan queries in [ydb.tech](https://ydb.tech/en/docs/concepts/scan_query)
* `ydb.SchemeQueryMode` - for [DDL](https://en.wikipedia.org/wiki/Data_definition_language) queries
* `ydb.ScriptingQueryMode` - for [DDL](https://en.wikipedia.org/wiki/Data_definition_language), [DML](https://en.wikipedia.org/wiki/Data_manipulation_language), [DQL](https://en.wikipedia.org/wiki/Data_query_language) queries (not a [TCL](https://en.wikipedia.org/wiki/SQL#Transaction_controls)). Be careful: queries execute longer than with other query modes, and consume more server-side resources

Example for changing the default query mode:
```go
res, err = db.ExecContext(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
   "DROP TABLE `/full/path/to/table/series`",
)
```

## Changing the transaction control mode <a name="tx-control"></a>

Default `YDB`'s transaction control mode is a `SerializableReadWrite`. 
Default transaction control mode can be changed outside of interactive transactions by updating the context object:
```
ctx := ydb.WithTxControl(ctx, table.OnlineReadOnlyTxControl())
```

## Retry helpers for `YDB` `database/sql` driver <a name="retry"></a>

`YDB` is a distributed `RDBMS` with non-stop 24/7 releases flow.
It means some nodes may be unavailable for queries at some point in time.
Network errors may also occur.
That's why some queries may complete with errors.
Most of those errors are transient.
`ydb-go-sdk`'s "knows" what to do on specific error: retry or not, with or without backoff, with or without the need to re-establish the session, etc.
`ydb-go-sdk` provides retry helpers which can work either with the database connection object, or with the transaction object.

### Retries over `sql.Conn` object <a name="retry-conn"></a>

`retry.Do` helper accepts custom lambda, which must return error if it happens during the processing,
or nil if the operation succeeds.
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

### Retries over `sql.Tx` <a name="retry-tx"></a>

`retry.DoTx` helper accepts custom lambda, which must return error if it happens during processing,
or nil if the operation succeeds.

`tx` object is a prepared transaction object.

The logic within the custom lambda does not need the explicit commit or rollback at the end - `retry.DoTx` does it automatically.

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

## Specifying query parameters <a name="arg-types"></a>

`database/sql` driver for `YDB` supports the following types of query parameters:
* multiple `sql.NamedArg` arguments (standard `database/sql` query parameters)
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
* multiple native `ydb-go-sdk` `table.ParameterOption` arguments which are constructed with `table.ValueParam("name", value)`
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
* single native `ydb-go-sdk` `*table.QueryParameters` argument which are constructed with `table.NewQueryParameters(parameterOptions...)`
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

## Accessing the native driver from `*sql.DB` <a name="unwrap"></a>

```
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
