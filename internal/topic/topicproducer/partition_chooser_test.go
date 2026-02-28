package topicproducer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionChooser_Bound(t *testing.T) {
	t.Run("SinglePartition", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{}
		partitions := map[int64]*PartitionInfo{
			1: {ID: 1, FromBound: []byte{}, ToBound: []byte("z")},
		}
		chooser, err := newBoundPartitionChooser(cfg, partitions)
		require.NoError(t, err)

		partitionID, err := chooser.ChoosePartition("key-a")
		require.NoError(t, err)
		require.Equal(t, int64(1), partitionID)
	})

	t.Run("MultiplePartitions", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{}
		partitions := map[int64]*PartitionInfo{
			1: {ID: 1, FromBound: []byte{}, ToBound: []byte("m")},
			2: {ID: 2, FromBound: []byte("m"), ToBound: []byte("z")},
		}
		chooser, err := newBoundPartitionChooser(cfg, partitions)
		require.NoError(t, err)

		partitionID, err := chooser.ChoosePartition("a")
		require.NoError(t, err)
		require.Equal(t, int64(1), partitionID)

		partitionID, err = chooser.ChoosePartition("n")
		require.NoError(t, err)
		require.Equal(t, int64(2), partitionID)
	})

	t.Run("WithPartitioningKeyHasher", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{
			PartitioningKeyHasher: func(key string) string {
				return "hashed-" + key
			},
		}
		partitions := map[int64]*PartitionInfo{
			1: {ID: 1, FromBound: []byte{}, ToBound: []byte("zzzz")},
		}
		chooser, err := newBoundPartitionChooser(cfg, partitions)
		require.NoError(t, err)

		partitionID, err := chooser.ChoosePartition("key")
		require.NoError(t, err)
		require.Equal(t, int64(1), partitionID)
	})

	t.Run("NoPartitions", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{}
		partitions := map[int64]*PartitionInfo{}
		chooser, err := newBoundPartitionChooser(cfg, partitions)
		require.NoError(t, err)

		_, err = chooser.ChoosePartition("key")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no partitions configured")
	})

	t.Run("PartitionWithoutBounds", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{}
		partitions := map[int64]*PartitionInfo{
			1: {ID: 1, FromBound: []byte{}, ToBound: []byte("z")},
			2: {ID: 2, FromBound: []byte{}, ToBound: []byte{}}, // no bounds
		}
		_, err := newBoundPartitionChooser(cfg, partitions)
		require.Error(t, err)
		require.Contains(t, err.Error(), "has no bounds")
	})

	t.Run("AddNewPartition", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{}
		partitions := map[int64]*PartitionInfo{
			1: {ID: 1, FromBound: []byte{}, ToBound: []byte("m")},
		}
		chooser, err := newBoundPartitionChooser(cfg, partitions)
		require.NoError(t, err)

		chooser.AddNewPartition(2, []byte("m"), []byte("z"))

		partitionID, err := chooser.ChoosePartition("n")
		require.NoError(t, err)
		require.Equal(t, int64(2), partitionID)
	})

	t.Run("RemovePartition", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{}
		partitions := map[int64]*PartitionInfo{
			1: {ID: 1, FromBound: []byte{}, ToBound: []byte("z")},
		}
		chooser, err := newBoundPartitionChooser(cfg, partitions)
		require.NoError(t, err)

		chooser.RemovePartition(1)
		_, err = chooser.ChoosePartition("key")
		require.Error(t, err)
	})
}

func TestPartitionChooser_Hash(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{}
		chooser := newHashPartitionChooser(cfg, 4)

		partitionID, err := chooser.ChoosePartition("key1")
		require.NoError(t, err)
		require.True(t, partitionID >= 0 && partitionID < 4)

		// Same key should return same partition
		partitionID2, err := chooser.ChoosePartition("key1")
		require.NoError(t, err)
		require.Equal(t, partitionID, partitionID2)
	})

	t.Run("WithPartitioningKeyHasher", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{
			PartitioningKeyHasher: func(key string) string {
				return "hashed-" + key
			},
		}
		chooser := newHashPartitionChooser(cfg, 2)

		partitionID, err := chooser.ChoosePartition("key")
		require.NoError(t, err)
		require.True(t, partitionID >= 0 && partitionID < 2)
	})

	t.Run("AddRemovePartition", func(t *testing.T) {
		t.Parallel()

		cfg := &ProducerConfig{}
		chooser := newHashPartitionChooser(cfg, 2)

		chooser.AddNewPartition(3, nil, nil)
		partitionID, err := chooser.ChoosePartition("key")
		require.NoError(t, err)
		require.True(t, partitionID >= 0 && partitionID < 3)

		chooser.RemovePartition(3)
		partitionID, err = chooser.ChoosePartition("key")
		require.NoError(t, err)
		require.True(t, partitionID >= 0 && partitionID < 2)
	})
}
