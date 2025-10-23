package value

import (
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestIsNull(t *testing.T) {
	for _, tt := range []struct {
		v      Value
		isNull bool
	}{
		{
			v:      nil,
			isNull: true,
		},
		{
			v:      NullValue(types.Date),
			isNull: true,
		},
		{
			v:      NullValue(types.NewOptional(types.Date)),
			isNull: true,
		},
		{
			v:      OptionalValue(Uint32Value(1)),
			isNull: false,
		},
		{
			v:      OptionalValue(OptionalValue(Uint32Value(1))),
			isNull: false,
		},
		{
			v:      Uint32Value(1),
			isNull: false,
		},
	} {
		t.Run(func() string {
			if tt.v == nil {
				return "nil"
			}

			return tt.v.Yql()
		}(), func(t *testing.T) {
			require.Equal(t, tt.isNull, IsNull(tt.v))
		})
	}
}

func TestUnwrap(t *testing.T) {
	for _, tt := range []struct {
		name     string
		v        Value
		expected Value
	}{
		{
			name:     "nil value",
			v:        nil,
			expected: nil,
		},
		{
			name:     "optional with value",
			v:        OptionalValue(Uint32Value(42)),
			expected: Uint32Value(42),
		},
		{
			name:     "optional with null",
			v:        NullValue(types.Date),
			expected: nil,
		},
		{
			name:     "non-optional value",
			v:        Int64Value(123),
			expected: Int64Value(123),
		},
		{
			name:     "nested optional",
			v:        OptionalValue(OptionalValue(TextValue("test"))),
			expected: OptionalValue(TextValue("test")),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := Unwrap(tt.v)
			if tt.expected == nil {
				require.Nil(t, result)
			} else {
				require.Equal(t, tt.expected.Yql(), result.Yql())
			}
		})
	}
}

func TestNullableDecimalValue(t *testing.T) {
	t.Run("nil value", func(t *testing.T) {
		v := NullableDecimalValue(nil, 22, 9)
		require.True(t, IsNull(v))
		// Check that the inner type is decimal
		optType, ok := v.Type().(types.Optional)
		require.True(t, ok)
		decType, ok := optType.InnerType().(*types.Decimal)
		require.True(t, ok)
		require.Equal(t, uint32(22), decType.Precision())
		require.Equal(t, uint32(9), decType.Scale())
	})

	t.Run("non-nil value", func(t *testing.T) {
		val := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		v := NullableDecimalValue(&val, 22, 9)
		require.False(t, IsNull(v))
	})
}

func TestNullableDecimalValueFromBigInt(t *testing.T) {
	t.Run("nil value", func(t *testing.T) {
		v := NullableDecimalValueFromBigInt(nil, 22, 9)
		require.True(t, IsNull(v))
		// Check that the inner type is decimal
		optType, ok := v.Type().(types.Optional)
		require.True(t, ok)
		decType, ok := optType.InnerType().(*types.Decimal)
		require.True(t, ok)
		require.Equal(t, uint32(22), decType.Precision())
		require.Equal(t, uint32(9), decType.Scale())
	})

	t.Run("non-nil value", func(t *testing.T) {
		bigInt := big.NewInt(12345)
		v := NullableDecimalValueFromBigInt(bigInt, 22, 9)
		require.False(t, IsNull(v))
	})
}

func TestNullableUUIDValueWithIssue1501(t *testing.T) {
	t.Run("nil value", func(t *testing.T) {
		v := NullableUUIDValueWithIssue1501(nil)
		require.True(t, IsNull(v))
		// Check that the inner type is UUID
		optType, ok := v.Type().(types.Optional)
		require.True(t, ok)
		require.Equal(t, types.UUID, optType.InnerType())
	})

	t.Run("non-nil value", func(t *testing.T) {
		uuidBytes := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		v := NullableUUIDValueWithIssue1501(&uuidBytes)
		require.False(t, IsNull(v))
	})
}

func TestNullableUuidValue(t *testing.T) {
	t.Run("nil value", func(t *testing.T) {
		v := NullableUuidValue(nil)
		require.True(t, IsNull(v))
		// Check that the inner type is UUID
		optType, ok := v.Type().(types.Optional)
		require.True(t, ok)
		require.Equal(t, types.UUID, optType.InnerType())
	})

	t.Run("non-nil value", func(t *testing.T) {
		u := uuid.New()
		v := NullableUuidValue(&u)
		require.False(t, IsNull(v))
	})
}

func TestNullableFromTimeValues(t *testing.T) {
	now := time.Now()

	t.Run("NullableDateValueFromTime_nil", func(t *testing.T) {
		v := NullableDateValueFromTime(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableDateValueFromTime_nonNil", func(t *testing.T) {
		v := NullableDateValueFromTime(&now)
		require.False(t, IsNull(v))
	})

	t.Run("NullableDatetimeValueFromTime_nil", func(t *testing.T) {
		v := NullableDatetimeValueFromTime(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableDatetimeValueFromTime_nonNil", func(t *testing.T) {
		v := NullableDatetimeValueFromTime(&now)
		require.False(t, IsNull(v))
	})

	t.Run("NullableTzDateValueFromTime_nil", func(t *testing.T) {
		v := NullableTzDateValueFromTime(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableTzDateValueFromTime_nonNil", func(t *testing.T) {
		v := NullableTzDateValueFromTime(&now)
		require.False(t, IsNull(v))
	})

	t.Run("NullableTzDatetimeValueFromTime_nil", func(t *testing.T) {
		v := NullableTzDatetimeValueFromTime(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableTzDatetimeValueFromTime_nonNil", func(t *testing.T) {
		v := NullableTzDatetimeValueFromTime(&now)
		require.False(t, IsNull(v))
	})

	t.Run("NullableTimestampValueFromTime_nil", func(t *testing.T) {
		v := NullableTimestampValueFromTime(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableTimestampValueFromTime_nonNil", func(t *testing.T) {
		v := NullableTimestampValueFromTime(&now)
		require.False(t, IsNull(v))
	})

	t.Run("NullableTzTimestampValueFromTime_nil", func(t *testing.T) {
		v := NullableTzTimestampValueFromTime(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableTzTimestampValueFromTime_nonNil", func(t *testing.T) {
		v := NullableTzTimestampValueFromTime(&now)
		require.False(t, IsNull(v))
	})
}

func TestNullableFromDurationValues(t *testing.T) {
	duration := time.Hour

	t.Run("NullableIntervalValueFromDuration_nil", func(t *testing.T) {
		v := NullableIntervalValueFromDuration(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableIntervalValueFromDuration_nonNil", func(t *testing.T) {
		v := NullableIntervalValueFromDuration(&duration)
		require.False(t, IsNull(v))
	})
}

func TestNullableBytesAndStringValues(t *testing.T) {
	t.Run("NullableBytesValue_nil", func(t *testing.T) {
		v := NullableBytesValue(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableBytesValue_nonNil", func(t *testing.T) {
		bytes := []byte("test")
		v := NullableBytesValue(&bytes)
		require.False(t, IsNull(v))
	})

	t.Run("NullableYSONValueFromBytes_nil", func(t *testing.T) {
		v := NullableYSONValueFromBytes(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableYSONValueFromBytes_nonNil", func(t *testing.T) {
		bytes := []byte(`{"key": "value"}`)
		v := NullableYSONValueFromBytes(&bytes)
		require.False(t, IsNull(v))
	})

	t.Run("NullableJSONValueFromBytes_nil", func(t *testing.T) {
		v := NullableJSONValueFromBytes(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableJSONValueFromBytes_nonNil", func(t *testing.T) {
		bytes := []byte(`{"key": "value"}`)
		v := NullableJSONValueFromBytes(&bytes)
		require.False(t, IsNull(v))
	})

	t.Run("NullableUUIDValue_nil", func(t *testing.T) {
		v := NullableUUIDValue(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableUUIDValue_nonNil", func(t *testing.T) {
		uuidBytes := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		v := NullableUUIDValue(&uuidBytes)
		require.False(t, IsNull(v))
	})

	t.Run("NullableJSONDocumentValueFromBytes_nil", func(t *testing.T) {
		v := NullableJSONDocumentValueFromBytes(nil)
		require.True(t, IsNull(v))
	})

	t.Run("NullableJSONDocumentValueFromBytes_nonNil", func(t *testing.T) {
		bytes := []byte(`{"key": "value"}`)
		v := NullableJSONDocumentValueFromBytes(&bytes)
		require.False(t, IsNull(v))
	})
}

func TestNullableWithVariousTypes(t *testing.T) {
	t.Run("Float", func(t *testing.T) {
		val := float32(1.23)
		v := Nullable(types.Float, &val)
		require.False(t, IsNull(v))
	})

	t.Run("FloatNil", func(t *testing.T) {
		v := Nullable(types.Float, (*float32)(nil))
		require.True(t, IsNull(v))
	})

	t.Run("Double", func(t *testing.T) {
		val := float64(1.23)
		v := Nullable(types.Double, &val)
		require.False(t, IsNull(v))
	})

	t.Run("DoubleNil", func(t *testing.T) {
		v := Nullable(types.Double, (*float64)(nil))
		require.True(t, IsNull(v))
	})

	t.Run("Date", func(t *testing.T) {
		val := uint32(18000)
		v := Nullable(types.Date, &val)
		require.False(t, IsNull(v))
	})

	t.Run("Datetime", func(t *testing.T) {
		val := uint32(1234567890)
		v := Nullable(types.Datetime, &val)
		require.False(t, IsNull(v))
	})

	t.Run("Timestamp", func(t *testing.T) {
		val := uint64(1234567890)
		v := Nullable(types.Timestamp, &val)
		require.False(t, IsNull(v))
	})

	t.Run("Interval", func(t *testing.T) {
		val := int64(1000000)
		v := Nullable(types.Interval, &val)
		require.False(t, IsNull(v))
	})

	t.Run("TzDate", func(t *testing.T) {
		val := "2020-01-01,UTC"
		v := Nullable(types.TzDate, &val)
		require.False(t, IsNull(v))
	})

	t.Run("TzDatetime", func(t *testing.T) {
		val := "2020-01-01T12:00:00,UTC"
		v := Nullable(types.TzDatetime, &val)
		require.False(t, IsNull(v))
	})

	t.Run("TzTimestamp", func(t *testing.T) {
		val := "2020-01-01T12:00:00.123456,UTC"
		v := Nullable(types.TzTimestamp, &val)
		require.False(t, IsNull(v))
	})

	t.Run("Text", func(t *testing.T) {
		val := "test"
		v := Nullable(types.Text, &val)
		require.False(t, IsNull(v))
	})

	t.Run("Bytes", func(t *testing.T) {
		val := []byte("test")
		v := Nullable(types.Bytes, &val)
		require.False(t, IsNull(v))
	})

	t.Run("YSON", func(t *testing.T) {
		val := "{}"
		v := Nullable(types.YSON, &val)
		require.False(t, IsNull(v))
	})

	t.Run("JSON", func(t *testing.T) {
		val := "{}"
		v := Nullable(types.JSON, &val)
		require.False(t, IsNull(v))
	})

	t.Run("UUID", func(t *testing.T) {
		val := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		v := Nullable(types.UUID, &val)
		require.False(t, IsNull(v))
	})

	t.Run("JSONDocument", func(t *testing.T) {
		val := "{}"
		v := Nullable(types.JSONDocument, &val)
		require.False(t, IsNull(v))
	})

	t.Run("DyNumber", func(t *testing.T) {
		val := "123.456"
		v := Nullable(types.DyNumber, &val)
		require.False(t, IsNull(v))
	})
}
