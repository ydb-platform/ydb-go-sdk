package bind

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

// Converter defines the interface for custom conversion of database/sql query parameters
// to YDB values. Implementations can handle specific types that require special conversion
// logic beyond the standard type conversions.
//
// Example:
//
//	type MyCustomType struct {
//		Field string
//	}
//
//	func (c *MyCustomConverter) Convert(v any) (value.Value, bool) {
//		if custom, ok := v.(MyCustomType); ok {
//			return value.TextValue(custom.Field), true
//		}
//		return nil, false
//	}
type Converter = bind.Converter

// NamedValueConverter extends Converter to handle driver.NamedValue types
//
// This is useful when you need access to both the name and value of a parameter
// for conversion logic.
//
// Example:
//
//	func (c *MyNamedConverter) ConvertNamedValue(nv driver.NamedValue) (value.Value, bool) {
//		if nv.Name == "special_param" {
//			// Custom handling for named parameter
//			return value.TextValue(fmt.Sprintf("special_%v", nv.Value)), true
//		}
//		return c.Convert(nv.Value)
//	}
type NamedValueConverter = bind.NamedValueConverter

// RegisterConverter registers a custom converter with the default registry
//
// Custom converters are tried before the standard conversion logic, allowing
// you to override or extend the default behavior for specific types.
//
// Example:
//
//	bind.RegisterConverter(&MyCustomConverter{})
func RegisterConverter(converter Converter) {
	bind.RegisterConverter(converter)
}

// RegisterNamedValueConverter registers a named value converter with the default registry
//
// Named value converters are tried before standard converters when handling
// driver.NamedValue instances.
//
// Example:
//
//	bind.RegisterNamedValueConverter(&MyNamedConverter{})
func RegisterNamedValueConverter(converter NamedValueConverter) {
	bind.RegisterNamedValueConverter(converter)
}

// CustomTypeConverter is a generic converter that can be configured with custom conversion functions
//
// This provides a convenient way to create converters without defining a new type.
//
// Example:
//
//	converter := bind.NewCustomTypeConverter(
//		func(v any) bool { _, ok := v.(MyType); return ok },
//		func(v any) (value.Value, error) { return value.TextValue(v.(MyType).String()), nil },
//	)
//	bind.RegisterConverter(converter)
type CustomTypeConverter = bind.CustomTypeConverter

// NewCustomTypeConverter creates a new custom type converter
//
// typeCheck: function that returns true if the converter can handle the given value
// convertFunc: function that converts the value to a YDB value
func NewCustomTypeConverter(
	typeCheck func(any) bool,
	convertFunc func(any) (value.Value, error),
) *CustomTypeConverter {
	return bind.NewCustomTypeConverter(typeCheck, convertFunc)
}

// JSONConverter handles conversion of JSON documents to YDB JSON values
//
// This converter automatically handles any type that implements json.Marshaler
type JSONConverter = bind.JSONConverter

// UUIDConverter handles conversion of UUID types to YDB UUID values
//
// This converter handles google/uuid.UUID and pointer types
type UUIDConverter = bind.UUIDConverter

// ConverterRegistry manages a collection of custom converters
//
// You can create your own registry if you want to use a separate set of converters
// from the global default registry.
type ConverterRegistry = bind.ConverterRegistry

// NewConverterRegistry creates a new converter registry
func NewConverterRegistry() *ConverterRegistry {
	return bind.NewConverterRegistry()
}

// NamedValueConverterRegistry manages a collection of named value converters
type NamedValueConverterRegistry = bind.NamedValueConverterRegistry

// NewNamedValueConverterRegistry creates a new named value converter registry
func NewNamedValueConverterRegistry() *NamedValueConverterRegistry {
	return bind.NewNamedValueConverterRegistry()
}
