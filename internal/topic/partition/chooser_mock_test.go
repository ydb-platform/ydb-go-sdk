package partition

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// recordingChooser keeps the partitions currently visible to a chooser.
// It is safe to inspect while Source updates it from another goroutine.
type recordingChooser struct {
	mu         sync.Mutex
	partitions map[int64]topictypes.PartitionInfo
}

func (c *recordingChooser) ChoosePartition(topicwriterinternal.PublicMessage) (int64, error) {
	return 0, nil
}

func (c *recordingChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitions == nil {
		c.partitions = make(map[int64]topictypes.PartitionInfo)
	}
	for _, partition := range partitions {
		c.partitions[partition.PartitionID] = partition
	}

	return nil
}

func (c *recordingChooser) RemovePartition(partitionID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.partitions, partitionID)
}

func (c *recordingChooser) PartitionIDs() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]int64, 0, len(c.partitions))
	for partitionID := range c.partitions {
		ids = append(ids, partitionID)
	}
	slices.Sort(ids)

	return ids
}

func (c *recordingChooser) Partition(partitionID int64) topictypes.PartitionInfo {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.partitions[partitionID]
}

// blockingChooser pauses the first topology initialization until the test releases it.
type blockingChooser struct {
	recordingChooser

	firstAdd sync.Once
	started  chan struct{}
	release  chan struct{}
}

// secondAddErrorChooser blocks initialization and fails the subsequent topology update.
type secondAddErrorChooser struct {
	recordingChooser

	addCalls atomic.Int64
	started  chan struct{}
	release  chan struct{}
	err      error
}

// updateErrorChooser successfully chooses partition 2, accepts initialization,
// and fails subsequent topology updates.
type updateErrorChooser struct {
	addCalls atomic.Int64
	err      error
}

// blockingUpdateChooser pauses the second topology update, leaving initialization unblocked.
type blockingUpdateChooser struct {
	recordingChooser

	addCalls atomic.Int64
	started  chan struct{}
	release  chan struct{}
}

func (c *blockingUpdateChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	if c.addCalls.Add(1) == 2 {
		close(c.started)
		<-c.release
	}

	return c.recordingChooser.AddNewPartitions(partitions...)
}

func (c *blockingChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	c.firstAdd.Do(func() {
		close(c.started)
		<-c.release
	})

	return c.recordingChooser.AddNewPartitions(partitions...)
}

func (c *secondAddErrorChooser) AddNewPartitions(partitions ...topictypes.PartitionInfo) error {
	if c.addCalls.Add(1) == 1 {
		close(c.started)
		<-c.release

		return c.recordingChooser.AddNewPartitions(partitions...)
	}

	return c.err
}

func (c *updateErrorChooser) AddNewPartitions(...topictypes.PartitionInfo) error {
	if c.addCalls.Add(1) > 1 {
		return c.err
	}

	return nil
}

func (c *updateErrorChooser) ChoosePartition(topicwriterinternal.PublicMessage) (int64, error) {
	return 2, nil
}

func (c *updateErrorChooser) RemovePartition(int64) {
}

// fixedChooser always selects the configured partition and records topology updates.
type fixedChooser struct {
	recordingChooser

	partitionID int64
}

func (c *fixedChooser) ChoosePartition(topicwriterinternal.PublicMessage) (int64, error) {
	return c.partitionID, nil
}

// errorChooser returns configured errors from topology initialization or partition selection.
type errorChooser struct {
	recordingChooser

	addErr    error
	chooseErr error
}

func (c *errorChooser) AddNewPartitions(...topictypes.PartitionInfo) error {
	return c.addErr
}

func (c *errorChooser) ChoosePartition(topicwriterinternal.PublicMessage) (int64, error) {
	return 0, c.chooseErr
}
