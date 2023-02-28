# Basic example via database/sql driver

Basic example demonstrates the possibilities of `YDB` with `database/sql` driver:
 - create/drop tables with `scheme` query mode
 - upsert data with `data` query mode
 - select with `data` query mode (unary request `ExecuteDataQuery` in native `ydb-go-sdk`)
 - select with `scan` query mode (streaming request `StreamExecuteScanQuery` in native `ydb-go-sdk`)
 - `explain` query mode for getting AST and Plan of query processing
 - modify transaction control
 - different types of query args:
   - multiple `sql.NamedArg` (uniform `database/sql` arg)
   - multiple native (for `ydb-go-sdk`) `table.ParameterOption` which constructs from `table.ValueParam("name", value)`
   - single native (for `ydb-go-sdk`) `*table.QueryParameters` which constructs from `table.NewQueryParameters(parameterOptions...)`
 