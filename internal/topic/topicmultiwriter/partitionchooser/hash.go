package partitionchooser

import (
	"fmt"
	"slices"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xhash"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type HashPartitionChooser struct {
	partitions []int64
}

func NewHashPartitionChooser() *HashPartitionChooser {
	return &HashPartitionChooser{}
}

func (c *HashPartitionChooser) ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error) {
	if len(c.partitions) == 0 {
		return 0, fmt.Errorf("no partitions configured")
	}

	// Same as Kafka Partitioner
	//nolint:lll
	// See: https://github.com/apache/kafka/blob/4.2/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L330
	hash := xhash.Murmur2Hash32([]byte(msg.Key), 0)

	return c.partitions[hash%uint32(len(c.partitions))], nil
}

func (c *HashPartitionChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	for _, partition := range partitions {
		if len(partition.FromBound) > 0 || len(partition.ToBound) > 0 {
			return fmt.Errorf("%w: boundaries are not supported for hash partition chooser", ErrUnsupported)
		}

		c.partitions = append(c.partitions, partition.PartitionID)
	}

	slices.Sort(c.partitions)

	return nil
}

func (c *HashPartitionChooser) RemovePartition(partitionID int64) {
	c.partitions = slices.DeleteFunc(c.partitions, func(partition int64) bool {
		return partition == partitionID
	})
}
