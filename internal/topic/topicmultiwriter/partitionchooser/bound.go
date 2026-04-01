package partitionchooser

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xhash"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type KeyHasher func(key string) string

type BoundPartitionChooserOption func(cfg *BoundPartitionChooser)

func WithKeyHasher(keyHasher KeyHasher) BoundPartitionChooserOption {
	return func(cfg *BoundPartitionChooser) {
		cfg.keyHasher = keyHasher
	}
}

func defaultKeyHasher(key string) string {
	// Same as C++ TProducerSettings::DefaultPartitioningKeyHasher:
	// MurmurHash64 with seed 0, result as 8 bytes in big-endian (network byte order)
	lo := xhash.Murmur2Hash64A([]byte(key), 0)
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, lo)

	return string(out)
}

type partitionShortInfo struct {
	ID        int64
	FromBound string
	ToBound   string
}

type BoundPartitionChooser struct {
	keyHasher  KeyHasher
	partitions []partitionShortInfo
}

func NewBoundPartitionChooser(options ...BoundPartitionChooserOption) *BoundPartitionChooser {
	chooser := &BoundPartitionChooser{}
	for _, option := range options {
		option(chooser)
	}

	if chooser.keyHasher == nil {
		chooser.keyHasher = KeyHasher(defaultKeyHasher)
	}

	return chooser
}

func (c *BoundPartitionChooser) ChoosePartition(msg topicwriterinternal.PublicMessage) (int64, error) {
	if len(c.partitions) == 0 {
		return 0, fmt.Errorf("no partitions configured")
	}

	hashedKey := msg.Key
	if c.keyHasher != nil {
		hashedKey = c.keyHasher(msg.Key)
	}

	if msg.Metadata != nil {
		msg.Metadata[ChoosePartitionKeyMetadataKey] = []byte(hashedKey)
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

func (c *BoundPartitionChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	for i, partition := range partitions {
		if i > 0 && len(partition.FromBound) == 0 && len(partition.ToBound) == 0 {
			return ErrNoBounds
		}

		c.partitions = append(c.partitions, partitionShortInfo{
			ID:        partition.PartitionID,
			FromBound: string(partition.FromBound),
			ToBound:   string(partition.ToBound),
		})
	}

	sort.Slice(c.partitions, func(i, j int) bool {
		return strings.Compare(c.partitions[i].FromBound, c.partitions[j].FromBound) < 0
	})

	return nil
}

func (c *BoundPartitionChooser) RemovePartition(partitionID int64) {
	c.partitions = slices.DeleteFunc(c.partitions, func(partition partitionShortInfo) bool {
		return partition.ID == partitionID
	})
}
