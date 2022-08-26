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
Supports only `default` transaction options which mapped to `YDB`'s `SerializableReadWrite` transaction settings.
`YDB`'s `OnlineReadOnly` and `StaleReadOnly` transaction settings are not compatible with interactive transactions such as `database/sql`'s `*sql.Tx`.
`YDB`'s `OnlineReadOnly` and `StaleReadOnly` transaction settings can be explicitly applied to each query outside interactive transaction (see more in [Isolation levels support](#tx-control))
```
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

Thats why needs to select query mode on client side currently.

`YDB` team have a roadmap goal to implements a single method for executing different query types.

`database/sql` driver implementation for `YDB` supports next query modes:
* `ydb.DataQueryMode` - default query mode, for lookup `DQL` queries and `DML` queries.
* `ydb.ExplainQueryMode` - for gettting plan and `AST` of some query
* `ydb.ScanQueryMode` - for fullscan `DQL` queries
* `ydb.SchemeQueryMode` - for `DDL` queries
* `ydb.ScriptingQueryMode` - for `DDL`, `DML`, `DQL` queries (not a `TCL`). Be careful: queries executes longer than with other query modes and consumes bigger server-side resources

Example for changing default query mode:
```
res, err = db.ExecContext(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
   "DROP TABLE `/full/path/to/table/series`",
)
```

## Specify `YDB` transaction control <a name="tx-control"></a>

## Retryer's for `YDB` `database/sql` driver

### Over `sql.Conn` object

### Over `sql.Tx`
