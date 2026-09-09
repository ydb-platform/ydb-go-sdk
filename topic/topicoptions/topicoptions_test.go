package topicoptions

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topiclistenerinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
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

func TestWithListenerBufferSizeBytes(t *testing.T) {
	cfg := topiclistenerinternal.NewStreamListenerConfig()

	WithListenerBufferSizeBytes(42)(&cfg)

	require.Equal(t, 42, cfg.BufferSize)
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

func TestAlterConsumerOptionsDoNotResetSupportedCodecs(t *testing.T) {
	// Altering an unrelated consumer property must not send set_supported_codecs,
	// otherwise the server wipes the consumer's existing codec restriction.
	for _, opt := range []AlterOption{
		AlterConsumerWithImportant("name", true),
		AlterConsumerWithReadFrom("name", time.Unix(0, 0)),
		AlterConsumerWithAttributes("name", map[string]string{"k": "v"}),
		AlterConsumerWithAvailabilityPeriod("name", time.Hour),
	} {
		req := &rawtopic.AlterTopicRequest{}
		opt.ApplyAlterOption(req)

		proto := req.ToProto()
		require.Len(t, proto.GetAlterConsumers(), 1)
		require.Nil(t, proto.GetAlterConsumers()[0].GetSetSupportedCodecs())
	}
}

func TestAlterConsumerWithSupportedCodecsSetsCodecs(t *testing.T) {
	req := &rawtopic.AlterTopicRequest{}
	AlterConsumerWithSupportedCodecs("name", []topictypes.Codec{topictypes.CodecGzip}).ApplyAlterOption(req)

	proto := req.ToProto()
	require.Len(t, proto.GetAlterConsumers(), 1)
	require.NotNil(t, proto.GetAlterConsumers()[0].GetSetSupportedCodecs())
	require.Equal(t,
		[]int32{int32(topictypes.CodecGzip)},
		proto.GetAlterConsumers()[0].GetSetSupportedCodecs().GetCodecs(),
	)
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
