package value

import (
	"testing"

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
