package value

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func ptr[T any]() interface{} {
	var zeroValue T

	return &zeroValue
}

func value2ptr[T any](v T) *T {
	return &v
}

func unwrapPtr(v interface{}) interface{} {
	return reflect.ValueOf(v).Elem().Interface()
}

func TestCastTo(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value Value
		dst   interface{}
		exp   interface{}
		err   error
	}{
		{
			name:  xtest.CurrentFileLine(),
			value: TextValue("test"),
			dst:   (interface{})(nil),
			err:   errNilDestination,
		},

		{
			name:  xtest.CurrentFileLine(),
			value: TextValue("test"),
			dst:   ptr[Value](),
			exp:   TextValue("test"),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: OptionalValue(TextValue("test")),
			dst:   ptr[Value](),
			exp:   OptionalValue(TextValue("test")),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: TextValue("test"),
			dst:   ptr[string](),
			exp:   "test",
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: OptionalValue(TextValue("test")),
			dst:   ptr[*string](),
			exp:   value2ptr("test"),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: TextValue("test"),
			dst:   ptr[[]byte](),
			exp:   []byte("test"),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: OptionalValue(TextValue("test")),
			dst:   ptr[*[]byte](),
			exp:   value2ptr([]byte("test")),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: TextValue("test"),
			dst:   ptr[[]byte](),
			exp:   []byte("test"),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: TextValue("test"),
			dst:   ptr[int](),
			err:   ErrCannotCast,
		},

		{
			name:  xtest.CurrentFileLine(),
			value: BytesValue([]byte("test")),
			dst:   ptr[Value](),
			exp:   BytesValue([]byte("test")),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: BytesValue([]byte("test")),
			dst:   ptr[string](),
			exp:   "test",
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: BytesValue([]byte("test")),
			dst:   ptr[[]byte](),
			exp:   []byte("test"),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: BytesValue([]byte("test")),
			dst:   ptr[[]byte](),
			exp:   []byte("test"),
			err:   nil,
		},
		{
			name:  xtest.CurrentFileLine(),
			value: BytesValue([]byte("test")),
			dst:   ptr[int](),
			err:   ErrCannotCast,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				require.NoError(t, CastTo(tt.value, tt.dst))
				require.Equal(t, tt.exp, unwrapPtr(tt.dst))
			} else {
				require.ErrorIs(t, CastTo(tt.value, tt.dst), tt.err)
			}
		})
	}
}
