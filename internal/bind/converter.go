package bind

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"time"

	"github.com/google/uuid"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

// Converter defines the interface for custom conversion of database/sql query parameters
// to YDB values. Implementations can handle specific types that require special conversion
// logic beyond the standard type conversions.
	// Convert converts a value to a YDB Value.
	// Convert converts a value to a YDB Value.
	// Returns the converted value and true if the converter can handle the type,
	// or nil/zero value and false if the converter cannot handle the type.
	Convert(v any) (value.Value, bool)
}

// ConverterRegistry manages a collection of custom converters
type ConverterRegistry struct {
	converters []Converter
}

// NewConverterRegistry creates a new converter registry
func NewConverterRegistry() *ConverterRegistry {
	return &ConverterRegistry{
		converters: make([]Converter, 0),
	}
}

// Register adds a converter to the registry
func (r *ConverterRegistry) Register(converter Converter) {
	r.converters = append(r.converters, converter)
}

// Convert attempts to convert a value using registered converters
// Returns the converted value and true if successful, or nil/zero value and false
// if no converter could handle the value.
func (r *ConverterRegistry) Convert(v any) (value.Value, bool) {
	for _, converter := range r.converters {
		if result, ok := converter.Convert(v); ok {
			return result, true
		}
	}

	return nil, false
}

// DefaultConverterRegistry is the global registry used by the binding system
var DefaultConverterRegistry = NewConverterRegistry()

// RegisterConverter registers a converter with the default registry
func RegisterConverter(converter Converter) {
	DefaultConverterRegistry.Register(converter)
}

// convertWithCustomConverters attempts to convert a value using custom converters
// before falling back to the standard conversion logic
func convertWithCustomConverters(v any) (value.Value, bool) {
	return DefaultConverterRegistry.Convert(v)
}

// NamedValueConverter extends Converter to handle driver.NamedValue types
type NamedValueConverter interface {
	Converter
	// ConvertNamedValue converts a driver.NamedValue to a YDB Value.
	// Returns the converted value and true if the converter can handle the type,
	// or nil/zero value and false if the converter cannot handle the type.
	ConvertNamedValue(nv driver.NamedValue) (value.Value, bool)
}

// NamedValueConverterRegistry manages a collection of named value converters
type NamedValueConverterRegistry struct {
	converters []NamedValueConverter
}

// NewNamedValueConverterRegistry creates a new named value converter registry
func NewNamedValueConverterRegistry() *NamedValueConverterRegistry {
	return &NamedValueConverterRegistry{
		converters: make([]NamedValueConverter, 0),
	}
}

// Register adds a named value converter to the registry
func (r *NamedValueConverterRegistry) Register(converter NamedValueConverter) {
	r.converters = append(r.converters, converter)
}

// Convert attempts to convert a named value using registered converters
// Returns the converted value and true if successful, or nil/zero value and false
// if no converter could handle the value.
func (r *NamedValueConverterRegistry) Convert(nv driver.NamedValue) (value.Value, bool) {
	for _, converter := range r.converters {
		if result, ok := converter.ConvertNamedValue(nv); ok {
			return result, true
		}
	}

	return nil, false
}

// DefaultNamedValueConverterRegistry is the global registry used for named value conversion
var DefaultNamedValueConverterRegistry = NewNamedValueConverterRegistry()

// RegisterNamedValueConverter registers a named value converter with the default registry
func RegisterNamedValueConverter(converter NamedValueConverter) {
	DefaultNamedValueConverterRegistry.Register(converter)
}

// convertNamedValueWithCustomConverters attempts to convert a named value using custom converters
func convertNamedValueWithCustomConverters(nv driver.NamedValue) (value.Value, bool) {
	return DefaultNamedValueConverterRegistry.Convert(nv)
}

// Example converters for common use cases

// JSONConverter handles conversion of JSON documents to YDB JSON values
type JSONConverter struct{}

func (c *JSONConverter) Convert(v any) (value.Value, bool) {
	if v == nil {
		return nil, false
	}
	// Don't handle time.Time at all - let it fall through to standard conversion
	if _, ok := v.(time.Time); ok {
		return nil, false
	}
	// Don't handle *time.Time at all - let it fall through to standard conversion
	if _, ok := v.(*time.Time); ok {
		return nil, false
	}
	// Check if the value implements json.Marshaler
	if marshaler, ok := v.(interface{ MarshalJSON() ([]byte, error) }); ok {
		bytes, err := marshaler.MarshalJSON()
		if err != nil {
			return nil, false
		}

		return value.JSONValue(string(bytes)), true
	}
	// Only handle specific types that should be JSON
	switch v.(type) {
	case map[string]any, []any:
		// For these types, use json.Marshal
		bytes, err := json.Marshal(v)
		if err != nil {
			return nil, false
		}

		return value.JSONValue(string(bytes)), true
	default:
		// Don't handle other types - let them fall through to standard conversion
		return nil, false
	}
}

func (c *JSONConverter) ConvertNamedValue(nv driver.NamedValue) (value.Value, bool) {
	return c.Convert(nv.Value)
}

// UUIDConverter handles conversion of UUID types to YDB UUID values
type UUIDConverter struct{}

func (c *UUIDConverter) Convert(v any) (value.Value, bool) {
	uuidType := reflect.TypeOf(uuid.UUID{})
	uuidPtrType := reflect.TypeOf((*uuid.UUID)(nil))

	switch reflect.TypeOf(v) {
	case uuidType:
		vv, ok := v.(uuid.UUID)
		if !ok {
			return nil, false
		}

		return value.Uuid(vv), true
	case uuidPtrType:
		vv, ok := v.(*uuid.UUID)
		if !ok {
			return nil, false
		}

		if vv == nil {
			return value.NullValue(types.UUID), true
		}

		return value.OptionalValue(value.Uuid(*(vv))), true
	}

	return nil, false
}

func (c *UUIDConverter) ConvertNamedValue(nv driver.NamedValue) (value.Value, bool) {
	return c.Convert(nv.Value)
}

// CustomTypeConverter is a generic converter that can be configured with custom conversion functions
type CustomTypeConverter struct {
	typeCheck   func(any) bool
	convertFunc func(any) (value.Value, error)
}

// NewCustomTypeConverter creates a new custom type converter
func NewCustomTypeConverter(typeCheck func(any) bool, convertFunc func(any) (value.Value, error)) *CustomTypeConverter {
	return &CustomTypeConverter{
		typeCheck:   typeCheck,
		convertFunc: convertFunc,
	}
}

func (c *CustomTypeConverter) Convert(v any) (value.Value, bool) {
	if !c.typeCheck(v) {
		return nil, false
	}
	result, err := c.convertFunc(v)
	if err != nil {
		return nil, false
	}

	return result, true
}

func (c *CustomTypeConverter) ConvertNamedValue(nv driver.NamedValue) (value.Value, bool) {
	return c.Convert(nv.Value)
}

// RegisterDefaultConverters registers the built-in converters with the default registries
func RegisterDefaultConverters() {
	RegisterConverter(&JSONConverter{})
	RegisterConverter(&UUIDConverter{})
	RegisterNamedValueConverter(&JSONConverter{})
	RegisterNamedValueConverter(&UUIDConverter{})
}

func init() {
	RegisterDefaultConverters()
}
