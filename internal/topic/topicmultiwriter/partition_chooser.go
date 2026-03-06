package topicmultiwriter

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xhash"
)

var (
	_ PartitionChooser = (*boundPartitionChooser)(nil)
	_ PartitionChooser = (*hashPartitionChooser)(nil)
)

type PartitionChooser interface {
	ChoosePartition(msg message) (int64, error)
	AddNewPartition(partitionID int64, fromBound, toBound []byte)
	RemovePartition(partitionID int64)
}

type boundPartitionChooser struct {
	cfg        *MultiWriterConfig
	partitions []partitionShortInfo
}

func newBoundPartitionChooser(
	cfg *MultiWriterConfig,
	partitions map[int64]*PartitionInfo,
) (*boundPartitionChooser, error) {
	partitionShortInfos := make([]partitionShortInfo, 0, len(partitions))
	for _, partition := range partitions {
		if len(partitions) > 1 && len(partition.FromBound) == 0 && len(partition.ToBound) == 0 {
			return nil, fmt.Errorf("%w: partition %d has no bounds", ErrNoBounds, partition.ID)
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

func (c *boundPartitionChooser) ChoosePartition(msg message) (int64, error) {
	if len(c.partitions) == 0 {
		return 0, fmt.Errorf("no partitions configured")
	}

	hashedKey := msg.Key
	if c.cfg.PartitioningKeyHasher != nil {
		hashedKey = c.cfg.PartitioningKeyHasher(msg.Key)
	}

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

func (c *boundPartitionChooser) AddNewPartition(partitionID int64, fromBound, toBound []byte) {
	c.partitions = append(c.partitions, partitionShortInfo{
		ID:        partitionID,
		FromBound: string(fromBound),
		ToBound:   string(toBound),
	})

	sort.Slice(c.partitions, func(i, j int) bool {
		return strings.Compare(c.partitions[i].FromBound, c.partitions[j].FromBound) < 0
	})
}

func (c *boundPartitionChooser) RemovePartition(partitionID int64) {
	c.partitions = slices.DeleteFunc(c.partitions, func(partition partitionShortInfo) bool {
		return partition.ID == partitionID
	})
}

type hashPartitionChooser struct {
	cfg        *MultiWriterConfig
	partitions []int64
}

func newHashPartitionChooser(cfg *MultiWriterConfig, partitions []int64) *hashPartitionChooser {
	return &hashPartitionChooser{
		cfg:        cfg,
		partitions: partitions,
	}
}

func (c *hashPartitionChooser) ChoosePartition(msg message) (int64, error) {
	if len(c.partitions) == 0 {
		return 0, fmt.Errorf("no partitions configured")
	}

	// Same as Kafka Partitioner
	// See: https://github.com/apache/kafka/blob/4.2/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L330 //nolint:lll
	hash := xhash.Murmur2Hash32([]byte(msg.Key), 0)

	return c.partitions[hash%uint32(len(c.partitions))], nil
}

func (c *hashPartitionChooser) AddNewPartition(partitionID int64, _, _ []byte) {
	c.partitions = append(c.partitions, partitionID)
}

func (c *hashPartitionChooser) RemovePartition(partitionID int64) {
	c.partitions = slices.DeleteFunc(c.partitions, func(partition int64) bool {
		return partition == partitionID
	})
}
