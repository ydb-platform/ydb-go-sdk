package partitionchooser

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestHashPartitionChooser(t *testing.T) {
	t.Parallel()

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()

		chooser := NewHashPartitionChooser()

		partitionsToAdd := make([]topictypes.PartitionInfo, 0, 4)
		for i := range 4 {
			partitionsToAdd = append(
				partitionsToAdd,
				topictypes.PartitionInfo{PartitionID: int64(i), FromBound: []byte{}, ToBound: []byte{}},
			)
		}

		require.NoError(t, chooser.AddNewPartitions(partitionsToAdd...))

		msg := topicwriterinternal.PublicMessage{Key: "key1"}

		partitionID, err := chooser.ChoosePartition(msg)
		require.NoError(t, err)
		require.True(t, partitionID >= 0 && partitionID < 4)

		// Same key should return same partition.
		partitionID2, err := chooser.ChoosePartition(msg)
		require.NoError(t, err)
		require.Equal(t, partitionID, partitionID2)
	})

	t.Run("AddRemovePartition", func(t *testing.T) {
		t.Parallel()

		chooser := NewHashPartitionChooser()
		partitionsToAdd := make([]topictypes.PartitionInfo, 0, 3)
		partitionsToAdd = append(
			partitionsToAdd,
			topictypes.PartitionInfo{PartitionID: 0, FromBound: []byte{}, ToBound: []byte{}},
		)
		partitionsToAdd = append(
			partitionsToAdd,
			topictypes.PartitionInfo{PartitionID: 1, FromBound: []byte{}, ToBound: []byte{}},
		)
		partitionsToAdd = append(
			partitionsToAdd,
			topictypes.PartitionInfo{PartitionID: 3, FromBound: []byte{}, ToBound: []byte{}},
		)

		require.NoError(t, chooser.AddNewPartitions(partitionsToAdd...))

		msg := topicwriterinternal.PublicMessage{Key: "key"}

		partitionID, err := chooser.ChoosePartition(msg)
		require.NoError(t, err)
		require.Contains(t, []int64{0, 1, 3}, partitionID)

		chooser.RemovePartition(3)
		partitionID, err = chooser.ChoosePartition(msg)
		require.NoError(t, err)
		require.Contains(t, []int64{0, 1}, partitionID)
	})
}
