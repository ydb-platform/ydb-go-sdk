package topicoptions

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptionsCompare(t *testing.T) {
	for _, tt := range []struct {
		lhs   []AlterOption
		rhs   []AlterOption
		equal bool
	}{
		{
			[]AlterOption{
				AlterConsumerWithImportant("name", true),
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
			},
			[]AlterOption{
				AlterConsumerWithImportant("name", true),
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
			},
			true,
		},
		{
			[]AlterOption{
				AlterConsumerWithImportant("name", true),
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
			},
			[]AlterOption{
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
				AlterConsumerWithImportant("name", true),
			},
			false,
		},
		{
			[]AlterOption{
				AlterConsumerWithImportant("name", true),
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
				AlterWithDropConsumers("a", "b"),
			},
			[]AlterOption{
				AlterConsumerWithImportant("name", true),
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
				AlterWithDropConsumers("a", "b"),
			},
			true,
		},
		{
			[]AlterOption{
				AlterConsumerWithImportant("name", true),
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
				AlterWithDropConsumers("b", "a"),
			},
			[]AlterOption{
				AlterConsumerWithImportant("name", true),
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
				AlterWithDropConsumers("a", "b"),
			},
			true,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.equal, reflect.DeepEqual(tt.lhs, tt.rhs))
		})
	}
}
