package value

import (
	"math/big"
	"testing"

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
