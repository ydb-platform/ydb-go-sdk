package partitionchooser

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicpartitions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func messageWithKey(key string) topicpartitions.Message {
	return topicwriterinternal.PublicMessage{Key: key}
}

func TestBoundPartitionChooser(t *testing.T) {
	t.Parallel()

	t.Run("SinglePartition", func(t *testing.T) {
		t.Parallel()

		chooser := NewBoundPartitionChooser()
		partitions := []topictypes.PartitionInfo{
			{PartitionID: 1, FromBound: []byte{}, ToBound: []byte("z")},
		}

		require.NoError(t, chooser.AddNewPartitions(partitions...))

		partitionID, err := chooser.ChoosePartition(messageWithKey("key-a"))
		require.NoError(t, err)
		require.Equal(t, int64(1), partitionID)
	})

	t.Run("MultiplePartitions", func(t *testing.T) {
		t.Parallel()

		chooser := NewBoundPartitionChooser(
			WithKeyHasher(func(k string) string { return k }),
		)
		partitions := []topictypes.PartitionInfo{
			{PartitionID: 1, FromBound: []byte{}, ToBound: []byte("m")},
			{PartitionID: 2, FromBound: []byte("m"), ToBound: []byte("z")},
		}

		require.NoError(t, chooser.AddNewPartitions(partitions...))

		partitionID, err := chooser.ChoosePartition(messageWithKey("a"))
		require.NoError(t, err)
		require.Equal(t, int64(1), partitionID)

		partitionID, err = chooser.ChoosePartition(messageWithKey("n"))
		require.NoError(t, err)
		require.Equal(t, int64(2), partitionID)
	})

	t.Run("WithKeyHasher", func(t *testing.T) {
		t.Parallel()

		chooser := NewBoundPartitionChooser(
			WithKeyHasher(func(key string) string {
				return "hashed-" + key
			}),
		)

		partitions := []topictypes.PartitionInfo{
			{PartitionID: 1, Active: true, FromBound: []byte{}, ToBound: []byte("zzzz")},
		}

		require.NoError(t, chooser.AddNewPartitions(partitions...))

		partitionID, err := chooser.ChoosePartition(messageWithKey("key"))
		require.NoError(t, err)
		require.Equal(t, int64(1), partitionID)
	})

	t.Run("AddNewPartition", func(t *testing.T) {
		t.Parallel()

		chooser := NewBoundPartitionChooser(
			WithKeyHasher(func(k string) string { return k }),
		)
		partitions := []topictypes.PartitionInfo{
			{PartitionID: 1, Active: true, FromBound: []byte{}, ToBound: []byte("m")},
		}

		require.NoError(t, chooser.AddNewPartitions(partitions...))
		require.NoError(
			t,
			chooser.AddNewPartitions(
				topictypes.PartitionInfo{
					PartitionID: 2,
					Active:      true,
					FromBound:   []byte("m"),
					ToBound:     []byte("z"),
				},
			),
		)

		partitionID, err := chooser.ChoosePartition(messageWithKey("n"))
		require.NoError(t, err)
		require.Equal(t, int64(2), partitionID)
	})

	t.Run("RemovePartition", func(t *testing.T) {
		t.Parallel()

		chooser := NewBoundPartitionChooser()
		partitions := []topictypes.PartitionInfo{
			{PartitionID: 1, FromBound: []byte{}, ToBound: []byte("z")},
		}

		require.NoError(t, chooser.AddNewPartitions(partitions...))

		chooser.RemovePartition(1)
		_, err := chooser.ChoosePartition(messageWithKey("key"))
		require.Error(t, err)
	})
}
