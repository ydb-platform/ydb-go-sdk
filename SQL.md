# `database/sql` driver for `YDB`

## Initialization of `database/sql` driver

### Configure driver with `ydb.Connector` (recommended way)
    ```
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
### Configure driver only with data source name (connection string)
    ```
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

## Execute queries

### On database object

### With transaction

## Query modes

## Specify `YDB` transaction control 

## Retryer's for `YDB` `database/sql` driver

### Over `sql.Conn` object

### Over `sql.Tx`
