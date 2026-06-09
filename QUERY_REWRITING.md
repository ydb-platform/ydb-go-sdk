# Query Rewriting for YDB Optimization

## Overview

The `WithRewriteQueryArgs()` connector option enables automatic query rewriting to optimize SQL queries for YDB. This feature transforms standard SQL patterns into YDB-optimized equivalents.

## Transformations

### 1. IN Clause Optimization

Transforms multiple parameters in IN clauses into a single list parameter.

**Before:**
```go
rows, err := db.QueryContext(ctx, 
    "SELECT * FROM t WHERE id IN ($p1, $p2, $p3, $p4)",
    sql.Named("p1", 1),
    sql.Named("p2", 2),
    sql.Named("p3", 3),
    sql.Named("p4", 4),
)
```

**After (automatically transformed):**
```yql
SELECT * FROM t WHERE id IN $argsList0
-- $argsList0 = List[Uint64(1), Uint64(2), Uint64(3), Uint64(4)]
```

### 2. INSERT VALUES Optimization

Transforms INSERT/UPSERT/REPLACE statements with multiple value tuples into SELECT FROM AS_TABLE.

**Before:**
```go
_, err := db.ExecContext(ctx, 
    "INSERT INTO t (id, value) VALUES ($p1, $p2), ($p3, $p4), ($p5, $p6)",
    sql.Named("p1", 1),
    sql.Named("p2", "one"),
    sql.Named("p3", 2),
    sql.Named("p4", "two"),
    sql.Named("p5", 3),
    sql.Named("p6", "three"),
)
```

**After (automatically transformed):**
```yql
INSERT INTO t SELECT id, value FROM AS_TABLE($valuesList0)
-- $valuesList0 = List[
--   Struct{id: Uint64(1), value: Text("one")},
--   Struct{id: Uint64(2), value: Text("two")},
--   Struct{id: Uint64(3), value: Text("three")},
-- ]
```

## Usage

Enable query rewriting by adding the `WithRewriteQueryArgs()` option when creating a connector:

```go
import (
    "database/sql"
    "github.com/ydb-platform/ydb-go-sdk/v3"
)

func main() {
    // Create YDB driver
    nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
    if err != nil {
        panic(err)
    }
    defer nativeDriver.Close(ctx)

    // Create connector with query rewriting enabled
    connector, err := ydb.Connector(nativeDriver,
        ydb.WithRewriteQueryArgs(),  // Enable query rewriting
        ydb.WithNumericArgs(),        // Optional: also use numeric args
        ydb.WithAutoDeclare(),        // Optional: also use auto-declare
    )
    if err != nil {
        panic(err)
    }
    defer connector.Close()

    // Use standard database/sql
    db := sql.OpenDB(connector)
    defer db.Close()

    // Your queries will be automatically optimized
    rows, err := db.QueryContext(ctx, 
        "SELECT * FROM users WHERE id IN ($1, $2, $3)",
        100, 200, 300,
    )
    // ... use rows ...
}
```

## Behavior

- **Single parameter IN clauses** are not transformed (e.g., `IN ($p1)` remains unchanged)
- **Single tuple INSERT statements** are not transformed (e.g., `VALUES ($p1, $p2)` remains unchanged)
- **Multiple IN clauses** in the same query are each transformed with separate list parameters
- **Works with UPSERT and REPLACE** in addition to INSERT

## Known Limitations

1. **Nested Parentheses**: The regex-based parser does not fully support nested parentheses in IN clauses. For example:
   ```sql
   -- This may not work correctly:
   SELECT * FROM t WHERE id IN (FUNC($p1), $p2)
   ```

2. **Qualified Table Names**: Table names with schemas (e.g., `schema.table`) may not be parsed correctly in INSERT transformations.

3. **Quoted Strings**: SQL strings containing patterns like `'IN ('` are not explicitly excluded. However, in practice, these won't be transformed because they won't match the parameter pattern.

4. **Comments**: SQL comments containing IN or INSERT patterns are not excluded from transformation.

These limitations are acceptable for the common use cases in YDB SQL queries. If you encounter issues with any of these edge cases, you can disable query rewriting by removing the `WithRewriteQueryArgs()` option.

## Performance Benefits

Query rewriting provides several benefits for YDB:

1. **Reduced Parameter Count**: Fewer parameters mean less overhead in query processing
2. **Better Query Plans**: YDB can optimize list parameters more efficiently
3. **Improved Caching**: Fewer parameter variations lead to better query plan caching
4. **Bulk Operations**: AS_TABLE enables efficient bulk inserts/upserts

## Compatibility

This feature is compatible with other YDB connector options:

- `WithNumericArgs()`: Transforms `$1, $2, ...` to named parameters
- `WithPositionalArgs()`: Transforms `?` to named parameters
- `WithAutoDeclare()`: Adds DECLARE statements for parameters
- `WithTablePathPrefix()`: Adds PRAGMA TablePathPrefix

The query rewriting happens at the `blockYQL` stage, which is after argument transformation but before final execution.

## Testing

The implementation includes comprehensive tests:

- **Unit Tests**: `internal/bind/rewrite_query_args_test.go` covers all transformation cases
- **Integration Tests**: `tests/integration/rewrite_query_args_test.go` validates end-to-end functionality

Run tests with:
```bash
go test ./internal/bind/... -run TestRewriteQueryArgs
```
