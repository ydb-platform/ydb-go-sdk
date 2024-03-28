package topicoptions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEqualAlterOptions(t *testing.T) {
	for _, tt := range []struct {
		lhs []AlterOption
		rhs []AlterOption
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
		},
		{
			[]AlterOption{
				AlterConsumerWithImportant("name", true),
				AlterWithMinActivePartitions(15),
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
			},
			[]AlterOption{
				AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
				AlterConsumerWithImportant("name", true),
				AlterWithMinActivePartitions(15),
			},
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
		},
	} {
		t.Run("", func(t *testing.T) {
			assert.ElementsMatch(t, tt.lhs, tt.rhs) // compare slices with ignore ordering
		})
	}
}

func TestEqualCreateOptions(t *testing.T) {
	for _, tt := range []struct {
		lhs []CreateOption
		rhs []CreateOption
	}{
		{
			[]CreateOption{
				CreateWithMeteringMode(1),
				CreateWithPartitionCountLimit(100),
				CreateWithRetentionPeriod(5),
			},
			[]CreateOption{
				CreateWithRetentionPeriod(5),
				CreateWithMeteringMode(1),
				CreateWithPartitionCountLimit(100),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			assert.ElementsMatch(t, tt.lhs, tt.rhs) // compare slices with ignore ordering
		})
	}
}
