package topicoptions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
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

func TestCreateWithMetricsLevel(t *testing.T) {
	req := &rawtopic.CreateTopicRequest{}
	CreateWithMetricsLevel(3).ApplyCreateOption(req)

	require.True(t, req.MetricsLevel.HasValue)
	require.Equal(t, uint32(3), req.MetricsLevel.Value)

	proto := req.ToProto()
	require.Equal(t, uint32(3), proto.GetMetricsLevel())
}

func TestAlterWithSetMetricsLevel(t *testing.T) {
	req := &rawtopic.AlterTopicRequest{}
	AlterWithSetMetricsLevel(2).ApplyAlterOption(req)

	require.True(t, req.SetMetricsLevel.HasValue)
	require.Equal(t, uint32(2), req.SetMetricsLevel.Value)
	require.False(t, req.ResetMetricsLevel)

	proto := req.ToProto()
	require.Equal(t, uint32(2), proto.GetSetMetricsLevel())
}

func TestAlterWithResetMetricsLevel(t *testing.T) {
	req := &rawtopic.AlterTopicRequest{}
	AlterWithSetMetricsLevel(5).ApplyAlterOption(req)
	AlterWithResetMetricsLevel().ApplyAlterOption(req)

	require.False(t, req.SetMetricsLevel.HasValue)
	require.True(t, req.ResetMetricsLevel)

	proto := req.ToProto()
	require.NotNil(t, proto.GetResetMetricsLevel())
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
