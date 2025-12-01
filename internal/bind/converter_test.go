package bind

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestConverterRegistry(t *testing.T) {
	t.Run("register and convert", func(t *testing.T) {
		registry := NewConverterRegistry()
		// Register a simple converter
		converter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)

				return ok
			},
			func(v any) (value.Value, error) { return value.TextValue("converted_" + v.(string)), nil },
		)
		registry.Register(converter)

		// Test conversion
		result, ok := registry.Convert("test")
		require.True(t, ok)
		require.Equal(t, "\"converted_test\"u", result.Yql())

		// Test non-matching type
		_, ok = registry.Convert(123)
		require.False(t, ok)
	})

	t.Run("multiple converters", func(t *testing.T) {
		registry := NewConverterRegistry()

		// Register string converter
		stringConverter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)

				return ok
			},
			func(v any) (value.Value, error) { return value.TextValue("string_" + v.(string)), nil },
		)
		registry.Register(stringConverter)

		// Register int converter
		intConverter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(int)

				return ok
			},
			func(v any) (value.Value, error) { return value.Int32Value(int32(v.(int))), nil },
		)
		registry.Register(intConverter)

		// Test string conversion
		result, ok := registry.Convert("test")
		require.True(t, ok)
		require.Equal(t, "\"string_test\"u", result.Yql())

		// Test int conversion
		result, ok = registry.Convert(42)
		require.True(t, ok)
		var intVal int32
		err := value.CastTo(result, &intVal)
		require.NoError(t, err)
		require.Equal(t, int32(42), intVal)

		// Test non-matching type
		_, ok = registry.Convert(3.14)
		require.False(t, ok)
	})
}

func TestNamedValueConverterRegistry(t *testing.T) {
	t.Run("convert named value", func(t *testing.T) {
		registry := NewNamedValueConverterRegistry()

		// Register a named value converter
		converter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)

				return ok
			},
			func(v any) (value.Value, error) { return value.TextValue("named_" + v.(string)), nil },
		)
		registry.Register(converter)

		// Test conversion
		nv := driver.NamedValue{Name: "test_param", Value: "test_value"}
		result, ok := registry.Convert(nv)
		require.True(t, ok)
		require.Equal(t, "\"named_test_value\"u", result.Yql())

		// Test non-matching type
		nv = driver.NamedValue{Name: "test_param", Value: 123}
		_, ok = registry.Convert(nv)
		require.False(t, ok)
	})
}

func TestJSONConverter(t *testing.T) {
	converter := &JSONConverter{}

	t.Run("marshaler type", func(t *testing.T) {
		data := map[string]any{
			"name":  "test",
			"value": 42,
		}

		result, ok := converter.Convert(data)
		require.True(t, ok)

		expectedJSON, _ := json.Marshal(data)
		require.Equal(t, "Json(@@"+string(expectedJSON)+"@@)", result.Yql())
	})

	t.Run("non-marshaler type", func(t *testing.T) {
		_, ok := converter.Convert("not a marshaler")
		require.False(t, ok) // Strings should not be handled by JSON converter
	})
}

func TestUUIDConverter(t *testing.T) {
	converter := &UUIDConverter{}

	t.Run("uuid type", func(t *testing.T) {
		id := uuid.New()
		result, ok := converter.Convert(id)
		require.True(t, ok)
		var uuidVal uuid.UUID
		err := value.CastTo(result, &uuidVal)
		require.NoError(t, err)
		require.Equal(t, id, uuidVal)
	})

	t.Run("uuid pointer type", func(t *testing.T) {
		id := uuid.New()
		result, ok := converter.Convert(&id)
		require.True(t, ok)
		var uuidVal uuid.UUID
		err := value.CastTo(result, &uuidVal)
		require.NoError(t, err)
		require.Equal(t, id, uuidVal)
	})

	t.Run("nil uuid pointer", func(t *testing.T) {
		var id *uuid.UUID
		result, ok := converter.Convert(id)
		require.True(t, ok)
		require.True(t, value.IsNull(result))
	})

	t.Run("non-uuid type", func(t *testing.T) {
		_, ok := converter.Convert("not a uuid")
		require.False(t, ok)
	})
}

func TestCustomTypeConverter(t *testing.T) {
	t.Run("successful conversion", func(t *testing.T) {
		converter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(time.Time)

				return ok
			},
			func(v any) (value.Value, error) {
				t := v.(time.Time)

				return value.TextValue(t.Format(time.RFC3339)), nil
			},
		)

		now := time.Now()
		result, ok := converter.Convert(now)
		require.True(t, ok)
		require.Equal(t, "\""+now.Format(time.RFC3339)+"\"u", result.Yql())
	})

	t.Run("type check fails", func(t *testing.T) {
		converter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(time.Time)

				return ok
			},
			func(v any) (value.Value, error) {
				t := v.(time.Time)

				return value.TextValue(t.Format(time.RFC3339)), nil
			},
		)

		_, ok := converter.Convert("not a time")
		require.False(t, ok)
	})

	t.Run("conversion error", func(t *testing.T) {
		converter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)

				return ok
			},
			func(v any) (value.Value, error) {
				return nil, &testError{}
			},
		)

		_, ok := converter.Convert("test")
		require.False(t, ok)
	})
}

func TestDefaultConverterRegistry(t *testing.T) {
	// Save original registry
	originalConverters := make([]Converter, len(DefaultConverterRegistry.converters))
	copy(originalConverters, DefaultConverterRegistry.converters)

	defer func() {
		// Restore original registry
		DefaultConverterRegistry.converters = originalConverters
	}()

	// Clear registry for test
	DefaultConverterRegistry.converters = make([]Converter, 0)

	t.Run("empty registry", func(t *testing.T) {
		_, ok := DefaultConverterRegistry.Convert("test")
		require.False(t, ok)
	})

	t.Run("register and convert", func(t *testing.T) {
		converter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)

				return ok
			},
			func(v any) (value.Value, error) { return value.TextValue("default_" + v.(string)), nil },
		)
		RegisterConverter(converter)

		result, ok := DefaultConverterRegistry.Convert("test")
		require.True(t, ok)
		require.Equal(t, "\"default_test\"u", result.Yql())
	})
}

func TestDefaultNamedValueConverterRegistry(t *testing.T) {
	// Save original registry
	originalConverters := make([]NamedValueConverter, len(DefaultNamedValueConverterRegistry.converters))
	copy(originalConverters, DefaultNamedValueConverterRegistry.converters)

	defer func() {
		// Restore original registry
		DefaultNamedValueConverterRegistry.converters = originalConverters
	}()

	// Clear registry for test
	DefaultNamedValueConverterRegistry.converters = make([]NamedValueConverter, 0)

	t.Run("empty registry", func(t *testing.T) {
		nv := driver.NamedValue{Name: "test", Value: "test"}
		_, ok := DefaultNamedValueConverterRegistry.Convert(nv)
		require.False(t, ok)
	})

	t.Run("register and convert", func(t *testing.T) {
		converter := NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)

				return ok
			},
			func(v any) (value.Value, error) { return value.TextValue("named_default_" + v.(string)), nil },
		)
		RegisterNamedValueConverter(converter)

		nv := driver.NamedValue{Name: "test", Value: "test"}
		result, ok := DefaultNamedValueConverterRegistry.Convert(nv)
		require.True(t, ok)
		require.Equal(t, "\"named_default_test\"u", result.Yql())
	})
}

func TestRegisterDefaultConverters(t *testing.T) {
	// Save original registries
	originalConverters := make([]Converter, len(DefaultConverterRegistry.converters))
	copy(originalConverters, DefaultConverterRegistry.converters)

	originalNamedConverters := make([]NamedValueConverter, len(DefaultNamedValueConverterRegistry.converters))
	copy(originalNamedConverters, DefaultNamedValueConverterRegistry.converters)

	defer func() {
		// Restore original registries
		DefaultConverterRegistry.converters = originalConverters
		DefaultNamedValueConverterRegistry.converters = originalNamedConverters
	}()

	// Clear registries for test
	DefaultConverterRegistry.converters = make([]Converter, 0)
	DefaultNamedValueConverterRegistry.converters = make([]NamedValueConverter, 0)

	// Register default converters
	RegisterDefaultConverters()

	// Test that JSON converter is registered
	data := map[string]any{"Field": "test"}
	result, ok := DefaultConverterRegistry.Convert(data)
	require.True(t, ok)

	expectedJSON, err := json.Marshal(data)
	require.NoError(t, err)
	require.Equal(t, "Json(@@"+string(expectedJSON)+"@@)", result.Yql())

	// Test that UUID converter is registered
	id := uuid.New()
	result, ok = DefaultConverterRegistry.Convert(id)
	require.True(t, ok)
	require.Contains(t, result.Yql(), id.String())
}

// Test helper types
type testError struct{}

func (e *testError) Error() string {
	return "test error"
}
