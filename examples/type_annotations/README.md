# Type Annotations Example

This example demonstrates how to use YDB type annotations in struct field tags to validate column types and improve code documentation.

## Features Demonstrated

1. **Basic Type Annotations**: Annotate struct fields with YDB types like `Uint64`, `Text`, `Double`
2. **Complex Type Annotations**: Use `List<T>`, `Optional<T>`, and `Dict<K,V>` annotations
3. **Type Validation**: Automatic validation that database column types match your struct annotations
4. **Error Detection**: Clear error messages when types don't match

## Struct Tag Syntax

The type annotation is added to the struct tag using the `type:` prefix:

```go
type Product struct {
    // Column name: product_id, YDB type: Uint64
    ProductID uint64 `sql:"product_id,type:Uint64"`
    
    // Column name: tags, YDB type: List<Text>
    Tags []string `sql:"tags,type:List<Text>"`
    
    // Column name: rating, YDB type: Optional<Double>
    Rating *float64 `sql:"rating,type:Optional<Double>"`
}
```

## Supported YDB Types

### Primitive Types
- `Bool`, `Int8`, `Int16`, `Int32`, `Int64`
- `Uint8`, `Uint16`, `Uint32`, `Uint64`
- `Float`, `Double`
- `Date`, `Datetime`, `Timestamp`
- `Text` (UTF-8), `Bytes` (binary)
- `JSON`, `YSON`, `UUID`

### Complex Types
- `List<T>` - List of items of type T
- `Optional<T>` - Optional (nullable) value of type T
- `Dict<K,V>` - Dictionary with key type K and value type V

### Nested Types
You can nest complex types:
- `List<Optional<Text>>` - List of optional text values
- `Optional<List<Uint64>>` - Optional list of unsigned integers
- `Dict<Text,List<Uint64>>` - Dictionary mapping text to lists of integers

## Benefits

1. **Documentation**: Type annotations serve as inline documentation of expected database schema
2. **Validation**: Runtime validation ensures your Go types match the database schema
3. **Error Prevention**: Catch type mismatches early before they cause runtime errors
4. **Code Clarity**: Makes it explicit what YDB types you expect from the database

## Running the Example

```bash
# Set your YDB connection string
export YDB_CONNECTION_STRING="grpc://localhost:2136/local"

# Run the example
go run main.go
```

## When to Use Type Annotations

Type annotations are **optional** but recommended when:

- Working with complex types (Lists, Dicts, Optional values)
- Building a strict API where type safety is critical
- Documenting expected database schema in code
- Working in a team where schema changes need to be validated

## Backward Compatibility

Type annotations are completely optional. Existing code without type annotations continues to work exactly as before. You can add annotations gradually to your codebase.
