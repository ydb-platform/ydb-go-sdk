package topicproducer

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spaolacci/murmur3"
)

var (
	_ PartitionChooser = (*boundPartitionChooser)(nil)
	_ PartitionChooser = (*hashPartitionChooser)(nil)
)

type PartitionChooser interface {
	ChoosePartition(key string) (int64, error)
}

type boundPartitionChooser struct {
	cfg        *ProducerConfig
	partitions []partitionShortInfo
}

func newBoundPartitionChooser(cfg *ProducerConfig, partitions map[int64]*PartitionInfo) (*boundPartitionChooser, error) {
	partitionShortInfos := make([]partitionShortInfo, 0, len(partitions))
	for _, partition := range partitions {
		if len(partitions) > 1 && len(partition.FromBound) == 0 && len(partition.ToBound) == 0 {
			return nil, fmt.Errorf("partition %d has no bounds", partition.ID)
		}

		partitionShortInfos = append(partitionShortInfos, partitionShortInfo{
			ID:        partition.ID,
			FromBound: string(partition.FromBound),
			ToBound:   string(partition.ToBound),
		})
	}

	sort.Slice(partitionShortInfos, func(i, j int) bool {
		return strings.Compare(partitionShortInfos[i].FromBound, partitionShortInfos[j].FromBound) < 0
	})

	return &boundPartitionChooser{
		cfg:        cfg,
		partitions: partitionShortInfos,
	}, nil
}

func (c *boundPartitionChooser) ChoosePartition(key string) (int64, error) {
	if len(c.partitions) == 0 {
		return 0, fmt.Errorf("no partitions configured")
	}

	hashedKey := c.cfg.PartitioningKeyHasher(key)

	// Find first partition whose lower bound is strictly greater than hashedKey.
	// Then take the previous one as the partition for this key.
	idx := sort.Search(len(c.partitions), func(i int) bool {
		return strings.Compare(c.partitions[i].FromBound, hashedKey) > 0
	})

	// If idx == 0, all FromBound > key. This should be impossible in normal server behavior,
	// because the first partition is expected to have an empty FromBound.
	// Panic here to highlight a protocol/metadata inconsistency.
	if idx == 0 {
		panic("ydb: unexpected partition bounds state: lower bound search returned index 0")
	}

	// If idx == len, key is >= all FromBound, take the last partition.
	// Otherwise take idx-1.
	switch {
	case idx >= len(c.partitions):
		return c.partitions[len(c.partitions)-1].ID, nil
	default:
		return c.partitions[idx-1].ID, nil
	}
}

type hashPartitionChooser struct {
	cfg        *ProducerConfig
	partitions uint64
}

func newHashPartitionChooser(cfg *ProducerConfig, partitionsCount uint64) *hashPartitionChooser {
	return &hashPartitionChooser{
		cfg:        cfg,
		partitions: partitionsCount,
	}
}

func (c *hashPartitionChooser) ChoosePartition(key string) (int64, error) {
	hasher := murmur3.New64()
	hasher.Write([]byte(c.cfg.PartitioningKeyHasher(key)))
	low := hasher.Sum64()

	return int64(low % c.partitions), nil
}
