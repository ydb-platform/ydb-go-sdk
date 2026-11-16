package topictypes

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
)

func TestPartitionSettingsFromRawClearsLegacyLimit(t *testing.T) {
	settings := PartitionSettings{PartitionCountLimit: 100}
	settings.FromRaw(&rawtopic.PartitioningSettings{MinActivePartitions: 4, MaxActivePartitions: 6})

	require.Zero(t, settings.PartitionCountLimit)
	require.Equal(t, int64(4), settings.MinActivePartitions)
	require.Equal(t, int64(6), settings.MaxActivePartitions)
}

func TestPartitionSettingsToRawIgnoresLegacyLimit(t *testing.T) {
	settings := PartitionSettings{MinActivePartitions: 4, MaxActivePartitions: 6, PartitionCountLimit: 100}
	var raw rawtopic.PartitioningSettings
	settings.ToRaw(&raw)

	proto := raw.ToProto()
	require.Equal(t, int64(4), proto.GetMinActivePartitions())
	require.Equal(t, int64(6), proto.GetMaxActivePartitions())
	message := proto.ProtoReflect()
	require.False(t, message.Has(message.Descriptor().Fields().ByNumber(2)))
}
