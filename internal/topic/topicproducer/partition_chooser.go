package topicproducer

import "github.com/spaolacci/murmur3"

var (
	_ PartitionChooser = (*boundPartitionChooser)(nil)
	_ PartitionChooser = (*hashPartitionChooser)(nil)
)

type PartitionChooser interface {
	ChoosePartition(key string) (int64, error)
}

type boundPartitionChooser struct {
	cfg *ProducerConfig
}

func (c *boundPartitionChooser) ChoosePartition(key string) (int64, error) {
	return 0, nil
}

type hashPartitionChooser struct {
	cfg        *ProducerConfig
	hasher     murmur3.Hash128
	partitions uint64
}

func (c *hashPartitionChooser) ChoosePartition(key string) (int64, error) {
	const seed = 0x9E3779B97F4A7C15

	hasher := murmur3.New128()
	hasher.Write([]byte(c.cfg.PartitioningKeyHasher(key)))
	low, high := c.hasher.Sum128()

	return int64((low + high*seed) % c.partitions), nil
}
