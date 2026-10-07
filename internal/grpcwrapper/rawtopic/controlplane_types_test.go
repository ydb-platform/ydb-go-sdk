package rawtopic

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestPartitioningSettingsFromProtoIgnoresLegacyLimit(t *testing.T) {
	proto := &Ydb_Topic.PartitioningSettings{
		MinActivePartitions:      4,
		MaxActivePartitions:      6,
		AutoPartitioningSettings: &Ydb_Topic.AutoPartitioningSettings{},
	}
	message := proto.ProtoReflect()
	message.Set(message.Descriptor().Fields().ByNumber(2), protoreflect.ValueOfInt64(100))

	var settings PartitioningSettings
	require.NoError(t, settings.FromProto(proto))
	require.Equal(t, int64(4), settings.MinActivePartitions)
	require.Equal(t, int64(6), settings.MaxActivePartitions)

	roundTrip := settings.ToProto().ProtoReflect()
	require.False(t, roundTrip.Has(roundTrip.Descriptor().Fields().ByNumber(2)))
}
