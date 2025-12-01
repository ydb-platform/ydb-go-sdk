package bind

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestConverterRegistry(t *testing.T) {
	t.Run("register and convert", func(t *testing.T) {
		registry := bind.NewConverterRegistry()

		// Register a simple converter
		converter := bind.NewCustomTypeConverter(
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
		registry := bind.NewConverterRegistry()

		// Register string converter
		stringConverter := bind.NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)
				return ok
			},
			func(v any) (value.Value, error) { return value.TextValue("string_" + v.(string)), nil },
		)
		registry.Register(stringConverter)

		// Register int converter
		intConverter := bind.NewCustomTypeConverter(
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
		registry := bind.NewNamedValueConverterRegistry()

		// Register a named value converter
		converter := bind.NewCustomTypeConverter(
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
	converter := &bind.JSONConverter{}

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
		require.False(t, ok)
	})
}

func TestUUIDConverter(t *testing.T) {
	converter := &bind.UUIDConverter{}

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
		converter := bind.NewCustomTypeConverter(
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
		converter := bind.NewCustomTypeConverter(
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
		converter := bind.NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(time.Time)
				return ok
			},
			func(v any) (value.Value, error) {
				return nil, &testError{}
			},
		)

		_, ok := converter.Convert(time.Now())
		require.False(t, ok)
	})
}

func TestDefaultConverterRegistry(t *testing.T) {
	// Save original registry
	originalRegistry := bind.DefaultConverterRegistry

	defer func() {
		// Restore original registry
		bind.DefaultConverterRegistry = originalRegistry
	}()

	// Clear registry for testing
	bind.DefaultConverterRegistry = bind.NewConverterRegistry()

	t.Run("register and convert", func(t *testing.T) {
		converter := bind.NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)
				return ok
			},
			func(v any) (value.Value, error) { return value.TextValue("default_" + v.(string)), nil },
		)
		bind.RegisterConverter(converter)

		result, ok := bind.DefaultConverterRegistry.Convert("test")
		require.True(t, ok)
		require.Equal(t, "\"default_test\"u", result.Yql())
	})
}

func TestDefaultNamedValueConverterRegistry(t *testing.T) {
	// Save original registry
	originalRegistry := bind.DefaultNamedValueConverterRegistry

	defer func() {
		// Restore original registry
		bind.DefaultNamedValueConverterRegistry = originalRegistry
	}()

	// Clear registry for testing
	bind.DefaultNamedValueConverterRegistry = bind.NewNamedValueConverterRegistry()

	t.Run("register and convert", func(t *testing.T) {
		converter := bind.NewCustomTypeConverter(
			func(v any) bool {
				_, ok := v.(string)
				return ok
			},
			func(v any) (value.Value, error) { return value.TextValue("named_default_" + v.(string)), nil },
		)
		bind.RegisterNamedValueConverter(converter)

		nv := driver.NamedValue{Name: "test", Value: "test"}
		result, ok := bind.DefaultNamedValueConverterRegistry.Convert(nv)
		require.True(t, ok)
		require.Equal(t, "\"named_default_test\"u", result.Yql())
	})
}

func TestRegisterDefaultConverters(t *testing.T) {
	// Save original registries
	originalConverterRegistry := bind.DefaultConverterRegistry
	originalNamedConverterRegistry := bind.DefaultNamedValueConverterRegistry

	defer func() {
		// Restore original registries
		bind.DefaultConverterRegistry = originalConverterRegistry
		bind.DefaultNamedValueConverterRegistry = originalNamedConverterRegistry
	}()

	// Clear registries
	bind.DefaultConverterRegistry = bind.NewConverterRegistry()
	bind.DefaultNamedValueConverterRegistry = bind.NewNamedValueConverterRegistry()

	// Register default converters
	bind.RegisterDefaultConverters()

	// Test that JSON converter is registered
	data := map[string]any{"Field": "test"}
	result, ok := bind.DefaultConverterRegistry.Convert(data)
	require.True(t, ok)

	expectedJSON, err := json.Marshal(data)
	require.NoError(t, err)
	require.Equal(t, "Json(@@"+string(expectedJSON)+"@@)", result.Yql())

	// Test that UUID converter is registered
	id := uuid.New()
	result, ok = bind.DefaultConverterRegistry.Convert(id)
	require.True(t, ok)
	var uuidVal uuid.UUID
	err = value.CastTo(result, &uuidVal)
	require.NoError(t, err)
	require.Equal(t, id, uuidVal)
}

// Test helper types
type testError struct{}

func (e *testError) Error() string {
	return "test error"
}
