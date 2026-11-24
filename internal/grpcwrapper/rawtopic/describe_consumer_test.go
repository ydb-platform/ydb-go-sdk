package rawtopic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
)

func TestPartitionStatsFromProto(t *testing.T) {
	t.Run("ReadsMaxWriteTimeLag", func(t *testing.T) {
		var stats PartitionStats
		require.NoError(t, stats.FromProto(&Ydb_Topic.PartitionStats{
			PartitionOffsets: &Ydb_Topic.OffsetsRange{},
			StoreSizeBytes:   10,
			MaxWriteTimeLag:  durationpb.New(time.Hour),
		}))
		require.Equal(t, int64(10), stats.StoreSizeBytes)
		require.Equal(t, rawoptional.Duration{Value: time.Hour, HasValue: true}, stats.MaxWriteTimeLag)
	})

	t.Run("KeepsMaxWriteTimeLagEmptyWhenNotSet", func(t *testing.T) {
		var stats PartitionStats
		require.NoError(t, stats.FromProto(&Ydb_Topic.PartitionStats{
			PartitionOffsets: &Ydb_Topic.OffsetsRange{},
		}))
		require.False(t, stats.MaxWriteTimeLag.HasValue)
	})
}
