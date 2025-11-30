# feat: custom converter interface for database/sql query parameters

## Summary

Implements a comprehensive custom converter system for the ydb-go-sdk database/sql driver, allowing users to extend parameter conversion logic for custom types and special use cases.

## Fixes #433

This PR addresses the feature request for adding a custom converter interface to handle specialized type conversions beyond the standard database/sql parameter types.

## 🚀 Features

### Core Converter Interfaces
- **`Converter`**: Basic interface for converting Go values to YDB values
- **`NamedValueConverter`**: Extended interface with access to parameter names
- **Registry System**: Global and isolated converter registries

### Built-in Converters
- **`JSONConverter`**: Handles types implementing `json.Marshaler`
- **`UUIDConverter`**: Handles `google/uuid.UUID` and pointer types
- **`CustomTypeConverter`**: Generic converter with configurable functions

### Database/SQL Integration
- **`WithCustomConverter()`**: Register custom converters with connectors
- **`WithCustomNamedValueConverter()`**: Register named value converters
- Seamless integration with existing `database/sql` interface


### Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   database/sql  │───▶│   ydb.Connector  │───▶│  Converter      │
│   Parameters    │    │   with Options   │    │  Registry       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
                       ┌──────────────────────────────┼──────────────────────────────┐
                       │                              │                              │
              ┌────────▼────────┐            ┌─────────▼─────────┐          ┌────────▼────────┐
              │ CustomConverter │            │ JSONConverter     │          │ UUIDConverter   │
              │                 │            │                   │          │                 │
              │ - User types    │            │ - json.Marshaler  │          │ - uuid.UUID     │
              │ - Domain logic  │            │ - Maps/Slices     │          │ - Pointers      │
              └─────────────────┘            └───────────────────┘          └─────────────────┘
```

## 🧪 Testing

### Unit Tests
```bash
# Test core converter functionality
go test ./internal/bind/... -v

# Test public API
go test ./bind/... -v
```

### Integration Tests
```bash
# Test database/sql integration
go test ./tests/integration/... -v -run TestDatabaseSQL_CustomConverter
```

### Example Application
```bash
# Run the complete example
cd examples/custom_converter
go run main.go
```

## 📖 Usage Examples

### Basic Custom Converter
```go
type CustomTime struct {
    time.Time
}

type CustomTimeConverter struct{}

func (c *CustomTimeConverter) Convert(v any) (value.Value, bool) {
    if ct, ok := v.(CustomTime); ok {
        return value.TextValue(ct.Format("2006-01-02 15:04:05")), true
    }
    return nil, false
}

// Usage
connector, err := ydb.Connector(
    driver,
    ydb.WithCustomConverter(&CustomTimeConverter{}),
)
```

### Named Value Converter
```go
type SpecialParameterConverter struct{}

func (c *SpecialParameterConverter) ConvertNamedValue(nv driver.NamedValue) (value.Value, bool) {
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

### Generic Converter
```go
converter := bind.NewCustomTypeConverter(
    func(v any) bool { _, ok := v.(MyType); return ok },
    func(v any) (value.Value, error) { 
        return value.TextValue(v.(MyType).String()), nil 
    },
)
bind.RegisterConverter(converter)
```

## 🔧 Configuration Options

### Connector Options
- `ydb.WithCustomConverter(converter)` - Register a custom converter
- `ydb.WithCustomNamedValueConverter(converter)` - Register a named value converter

### Registry Functions
- `bind.RegisterConverter(converter)` - Register with global registry
- `bind.RegisterNamedValueConverter(converter)` - Register named converter
- `bind.NewConverterRegistry()` - Create isolated registry
- `bind.NewCustomTypeConverter(typeCheck, convertFunc)` - Generic converter

## 📊 Performance Considerations

1. **Converter Order**: Converters are tried in registration order
2. **Type Checking**: Fast type checks should be performed first
3. **Error Handling**: Return `(nil, false)` for unsupported types
4. **Built-in Optimization**: JSON and UUID converters are optimized

## 🔄 Migration Guide

### From Manual Conversion
```go
// Before
db.Exec("INSERT INTO table (json_data) VALUES ($1)", 
    string(jsonBytes))

// After
db.Exec("INSERT INTO table (json_data) VALUES ($1)", 
    customJSONObject) // Automatically converted by JSONConverter
```

### From Custom Types
```go
// Before
db.Exec("INSERT INTO table (id) VALUES ($1)", 
    "ID_"+customID.String())

// After with CustomIDConverter
db.Exec("INSERT INTO table (id) VALUES ($1)", 
    customID) // Automatically converted
```

## 🧩 Breaking Changes

None. This is a purely additive feature that maintains full backward compatibility.

## 📝 Documentation

- [Example README](examples/custom_converter/README.md) - Comprehensive usage guide
- [GoDoc Comments](bind/converter.go) - Inline documentation
- [Integration Tests](tests/integration/database_sql_custom_converter_test.go) - Working examples

## ✅ Verification Checklist

- [x] Unit tests pass for all converter components
- [x] Integration tests demonstrate end-to-end functionality
- [x] Example application runs successfully
- [x] Documentation is comprehensive and accurate
- [x] Backward compatibility is maintained
- [x] Performance impact is minimal
- [x] Error handling is robust
- [x] Thread safety is maintained

## 🔍 Code Review Points

1. **Interface Design**: Clean separation between Converter and NamedValueConverter
2. **Registry Pattern**: Global and isolated registry options
3. **Integration**: Seamless integration with existing binding system
4. **Testing**: Comprehensive test coverage including edge cases
5. **Documentation**: Clear examples and usage patterns

## 🚦 Deployment

This feature is ready for production use:
- Extensive test coverage
- Backward compatible
- Performance optimized
- Well documented
- Example applications provided

## 📈 Future Enhancements

Potential future improvements:
- Converter priority system
- Conditional converters based on query context
- More built-in converters for common types
- Converter composition utilities
