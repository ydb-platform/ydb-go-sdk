# Custom Converter Example

This example demonstrates how to use custom converters with the ydb-go-sdk database/sql driver to handle specialized type conversions.

## Overview

Custom converters allow you to extend the parameter conversion system to handle specific types that require special conversion logic beyond the standard type conversions. This is particularly useful when:

- You have custom domain types that need specific YDB representation
- You need to handle parameter names in special ways
- You want to override default conversion behavior for certain types
- You need to integrate with external data formats

## Key Features

- **Type-specific converters**: Handle conversion of custom Go types to YDB values
- **Named value converters**: Access both parameter name and value for sophisticated conversion logic
- **Registry system**: Register multiple converters that are tried in order
- **Integration with database/sql**: Seamless integration with Go's standard database/sql interface

## Example Types

This example demonstrates conversion of:

1. **CustomTime**: A wrapper around `time.Time` that formats dates as "2006-01-02 15:04:05"
2. **CustomID**: A custom identifier type that adds "ID_" prefix
3. **Special parameters**: Named parameters with special handling based on parameter names

## Running the Example

```bash
cd examples/custom_converter
go run main.go
```

## Expected Output

```
Retrieved record:
  ID: ID_12345
  Name: Example Record
  Created At: 2023-12-07 14:30:45
  Updated At: 1701943845
  Version: v1.0.0

Custom converter example completed successfully!
```

## Code Structure

### Custom Types

```go
type CustomTime struct {
    time.Time
}

type CustomID struct {
    ID string
}
```

### Converter Implementations

```go
type CustomTimeConverter struct{}

func (c *CustomTimeConverter) Convert(v any) (value.Value, bool) {
    if ct, ok := v.(CustomTime); ok {
        return value.TextValue(ct.Format("2006-01-02 15:04:05")), true
    }
    return nil, false
}

type SpecialParameterConverter struct{}

func (c *SpecialParameterConverter) ConvertNamedValue(nv sql.NamedValue) (value.Value, bool) {
    switch nv.Name {
    case "timestamp":
        if t, ok := nv.Value.(time.Time); ok {
            return value.Int64Value(t.Unix()), true
        }
    case "version":
        if s, ok := nv.Value.(string); ok {
            return value.TextValue("v" + s), true
        }
    }
    return nil, false
}
```

### Usage

```go
// Create connector with custom converters
connector, err := ydb.Connector(
    db.Driver().(*ydb.Driver),
    ydb.WithCustomConverter(&CustomTimeConverter{}),
    ydb.WithCustomConverter(&CustomIDConverter{}),
    ydb.WithCustomNamedValueConverter(&SpecialParameterConverter{}),
)

// Use with database/sql
db := sql.OpenDB(connector)

// Use custom types in queries
_, err = db.ExecContext(ctx, `
    INSERT INTO custom_data (id, name, created_at, updated_at, version) 
    VALUES ($id, $name, $created_at, $timestamp, $version)
`,
    sql.Named("id", CustomID{ID: "12345"}),
    sql.Named("created_at", CustomTime{Time: time.Now()}),
    sql.Named("timestamp", time.Now()),
    sql.Named("version", "1.0.0"),
)
```

## Best Practices

1. **Type safety**: Ensure your type check functions are specific to avoid false positives
2. **Error handling**: Return errors from conversion functions when conversion fails
3. **Performance**: Keep conversion logic simple and fast as it's called for every parameter
4. **Naming**: Use descriptive names for your converters to make the code maintainable
5. **Testing**: Test both successful conversions and edge cases

## Advanced Usage

### Generic Custom Type Converter

For simple cases, you can use the generic converter:

```go
converter := bind.NewCustomTypeConverter(
    func(v any) bool { _, ok := v.(MyType); return ok },
    func(v any) (value.Value, error) { 
        return value.TextValue(v.(MyType).String()), nil 
    },
)
bind.RegisterConverter(converter)
```

### Multiple Converters

Register multiple converters for different types:

```go
bind.RegisterConverter(&TimeConverter{})
bind.RegisterConverter(&IDConverter{})
bind.RegisterConverter(&JSONConverter{})
```

Converters are tried in registration order, so register more specific converters first.

### Built-in Converters

The SDK includes built-in converters for common types:

- **JSONConverter**: Handles types implementing `json.Marshaler`
- **UUIDConverter**: Handles `google/uuid.UUID` types

These are automatically registered when you import the bind package.
