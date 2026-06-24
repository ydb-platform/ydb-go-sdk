package value_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestBuilder(t *testing.T) {
	t.Run("List", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			act  value.Value
			exp  value.Value
		}{
			{
				name: "Uint8",
				act:  value.Builder{}.List().Uint8().Add(1).Add(2).Add(3).Build(),
				exp: value.ListValue(
					value.Uint8Value(1),
					value.Uint8Value(2),
					value.Uint8Value(3),
				),
			},
			{
				name: "Uint16",
				act:  value.Builder{}.List().Uint16().Add(1).Add(2).Add(3).Build(),
				exp: value.ListValue(
					value.Uint16Value(1),
					value.Uint16Value(2),
					value.Uint16Value(3),
				),
			},
			{
				name: "Uint32",
				act:  value.Builder{}.List().Uint32().Add(1).Add(2).Add(3).Build(),
				exp: value.ListValue(
					value.Uint32Value(1),
					value.Uint32Value(2),
					value.Uint32Value(3),
				),
			},
			{
				name: "Uint64",
				act:  value.Builder{}.List().Uint64().Add(1).Add(2).Add(3).Build(),
				exp: value.ListValue(
					value.Uint64Value(1),
					value.Uint64Value(2),
					value.Uint64Value(3),
				),
			},
			{
				name: "Int8",
				act:  value.Builder{}.List().Int8().Add(1).Add(2).Add(3).Build(),
				exp: value.ListValue(
					value.Int8Value(1),
					value.Int8Value(2),
					value.Int8Value(3),
				),
			},
			{
				name: "Int16",
				act:  value.Builder{}.List().Int16().Add(1).Add(2).Add(3).Build(),
				exp: value.ListValue(
					value.Int16Value(1),
					value.Int16Value(2),
					value.Int16Value(3),
				),
			},
			{
				name: "Int32",
				act:  value.Builder{}.List().Int32().Add(1).Add(2).Add(3).Build(),
				exp: value.ListValue(
					value.Int32Value(1),
					value.Int32Value(2),
					value.Int32Value(3),
				),
			},
			{
				name: "Int64",
				act:  value.Builder{}.List().Int64().Add(1).Add(2).Add(3).Build(),
				exp: value.ListValue(
					value.Int64Value(1),
					value.Int64Value(2),
					value.Int64Value(3),
				),
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				require.Equal(t, tt.exp, tt.act)
			})
		}
	})
}
