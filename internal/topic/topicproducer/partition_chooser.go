package topicproducer

import "github.com/spaolacci/murmur3"

type PartitionChooser interface {
	ChoosePartition(key string) int64
}

type boundPartitionChooser struct {
	cfg *ProducerConfig
}

func (c *boundPartitionChooser) ChoosePartition(key string) int64 {
	return 0
}

type hashPartitionChooser struct {
	cfg        *ProducerConfig
	hasher     murmur3.Hash128
	partitions int64
}

func (c *hashPartitionChooser) ChoosePartition(key string) int64 {
	low, high := c.hasher.Sum128([]byte(key))
	return int64((low + high) % c.partitions)
}
