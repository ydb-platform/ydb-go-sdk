# `database/sql` driver for `YDB`

Package `ydb-go-sdk` provides usage `database/sql` API also. 
`database/sql` driver implementation use `ydb-go-sdk` native driver API's.

## Table of contents
1. [Initialization of `database/sql` driver](#init)
   * [Configure driver with `ydb.Connector` (recommended way)](#init-connector)
   * [Configure driver only with data source name (connection string)](#init-dsn)
2. [Execute queries](#queries)
   * [On database object](#queries-db)
   * [With transaction](#queries-tx)
3. [Query modes (DDL, DML, DQL, etc.)](#query-modes)
4. [Retryers for `YDB` `database/sql` driver](#retry)
   * [Over `sql.Conn` object](#retry-conn)
   * [Over `sql.Tx`](#retry-tx)
5. [Query args types](#arg-types)
6. [Get native driver from `*sql.DB`](#unwrap)

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
### Configure driver only with data source name (connection string) <a name="init-dsn"></a>
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
* `query_mode=scripting` - you can redefine default `data` query mode

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
That's why `ydb-go-sdk` implements read-only `sql.LevelSnapshot` with fake transaction (transient, while YDB clusters are not updated with true snapshot isolation mode)
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
The `YDB` server API is currently requires to select a specific method by specific request type. For example, `DDL` must be called with `table.session.ExecuteSchemeQuery`, `DML` must be called with `table.session.Execute`, `DQL` may be called with `table.session.Execute` or `table.session.StreamExecuteScanQuery` etc. `YDB` have a `scripting` service also, which provides different query types with single method, but not supports transactions.

That's why needs to select query mode on client side currently.

`YDB` team have a roadmap goal to implements a single method for executing different query types.

`database/sql` driver implementation for `YDB` supports next query modes:
* `ydb.DataQueryMode` - default query mode, for lookup `DQL` queries and `DML` queries.
* `ydb.ExplainQueryMode` - for gettting plan and `AST` of some query
* `ydb.ScanQueryMode` - for fullscan `DQL` queries
* `ydb.SchemeQueryMode` - for `DDL` queries
* `ydb.ScriptingQueryMode` - for `DDL`, `DML`, `DQL` queries (not a `TCL`). Be careful: queries executes longer than with other query modes and consumes bigger server-side resources

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

## Retryers for `YDB` `database/sql` driver <a name="retry"></a>

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
