package topicreaderinternal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

type BatchTxStorageTestSuite struct {
	suite.Suite

	storage *batchTxStorage
}

func (s *BatchTxStorageTestSuite) SetupTest() {
	s.storage = newBatchTxStorage("test-consumer")
}

func (s *BatchTxStorageTestSuite) materializeTx(tx *mockTransaction) {
	_ = tx.UnLazy(context.Background())
}

func TestBatchTxStorage(t *testing.T) {
	suite.Run(t, new(BatchTxStorageTestSuite))
}

func (s *BatchTxStorageTestSuite) TestAdd_NewTransaction() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch := s.createTestBatch("topic-1", 1, 10, 20, 1)

	exists := s.storage.Add(tx, batch)

	s.False(exists)
}

func (s *BatchTxStorageTestSuite) TestAdd_ExistingTransaction() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-1", 1, 20, 30, 1)

	_ = s.storage.Add(tx, batch1)
	exists := s.storage.Add(tx, batch2)

	s.True(exists)
}

func (s *BatchTxStorageTestSuite) TestGetBatches_Empty() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)

	batches := s.storage.GetBatches(tx)

	s.Empty(batches)
}

func (s *BatchTxStorageTestSuite) TestGetBatches_WithBatches() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-1", 1, 20, 30, 1)

	_ = s.storage.Add(tx, batch1)
	_ = s.storage.Add(tx, batch2)

	batches := s.storage.GetBatches(tx)

	s.Len(batches, 2)
	s.Equal(batch1, batches[0])
	s.Equal(batch2, batches[1])
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_Empty() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)

	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Nil(req)
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_SingleBatch() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch := s.createTestBatch("topic-1", 1, 10, 20, 1)

	_ = s.storage.Add(tx, batch)
	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Require().NotNil(req)
	s.Equal("test-consumer", req.Consumer)
	s.Equal("tx-1", req.Tx.ID)
	s.Equal("session-1", req.Tx.Session)
	s.Len(req.Topics, 1)
	s.Equal("topic-1", req.Topics[0].Path)
	s.Len(req.Topics[0].Partitions, 1)
	s.Equal(int64(1), req.Topics[0].Partitions[0].PartitionID)
	s.Len(req.Topics[0].Partitions[0].PartitionOffsets, 1)
	s.Equal(rawtopiccommon.Offset(10), req.Topics[0].Partitions[0].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(20), req.Topics[0].Partitions[0].PartitionOffsets[0].End)
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_MultipleBatches() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-1", 1, 20, 30, 1)

	_ = s.storage.Add(tx, batch1)
	_ = s.storage.Add(tx, batch2)
	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Require().NotNil(req)
	s.Len(req.Topics, 1)
	s.Len(req.Topics[0].Partitions, 1)
	s.Len(req.Topics[0].Partitions[0].PartitionOffsets, 1)
	// Should be optimized to single range
	s.Equal(rawtopiccommon.Offset(10), req.Topics[0].Partitions[0].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(30), req.Topics[0].Partitions[0].PartitionOffsets[0].End)
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_MultipleTopics() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-2", 2, 30, 40, 2)

	_ = s.storage.Add(tx, batch1)
	_ = s.storage.Add(tx, batch2)
	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Require().NotNil(req)
	s.Len(req.Topics, 2)
	// Topics should be grouped by path
	topicMap := make(map[string]bool)
	for _, topic := range req.Topics {
		topicMap[topic.Path] = true
	}
	s.True(topicMap["topic-1"])
	s.True(topicMap["topic-2"])
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_MultiplePartitionsSameTopic() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-1", 2, 30, 40, 2)

	_ = s.storage.Add(tx, batch1)
	_ = s.storage.Add(tx, batch2)
	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Require().NotNil(req)
	s.Len(req.Topics, 1)
	s.Equal("topic-1", req.Topics[0].Path)
	s.Len(req.Topics[0].Partitions, 2)

	// Find partitions
	partitionMap := make(map[int64]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets)
	for _, partition := range req.Topics[0].Partitions {
		partitionMap[partition.PartitionID] = partition
	}

	s.Contains(partitionMap, int64(1))
	s.Contains(partitionMap, int64(2))
	s.Len(partitionMap[1].PartitionOffsets, 1)
	s.Len(partitionMap[2].PartitionOffsets, 1)
	s.Equal(rawtopiccommon.Offset(10), partitionMap[1].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(20), partitionMap[1].PartitionOffsets[0].End)
	s.Equal(rawtopiccommon.Offset(30), partitionMap[2].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(40), partitionMap[2].PartitionOffsets[0].End)
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_NonAdjacentBatches() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-1", 1, 30, 40, 1) // Gap between 20 and 30

	_ = s.storage.Add(tx, batch1)
	_ = s.storage.Add(tx, batch2)
	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Require().NotNil(req)
	s.Len(req.Topics, 1)
	s.Len(req.Topics[0].Partitions, 1)
	s.Len(req.Topics[0].Partitions[0].PartitionOffsets, 2) // Should not be merged
	s.Equal(rawtopiccommon.Offset(10), req.Topics[0].Partitions[0].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(20), req.Topics[0].Partitions[0].PartitionOffsets[0].End)
	s.Equal(rawtopiccommon.Offset(30), req.Topics[0].Partitions[0].PartitionOffsets[1].Start)
	s.Equal(rawtopiccommon.Offset(40), req.Topics[0].Partitions[0].PartitionOffsets[1].End)
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_MultiplePartitionsMultipleTopics() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-1", 2, 30, 40, 2)
	batch3 := s.createTestBatch("topic-2", 1, 50, 60, 3)
	batch4 := s.createTestBatch("topic-2", 2, 70, 80, 4)

	_ = s.storage.Add(tx, batch1)
	_ = s.storage.Add(tx, batch2)
	_ = s.storage.Add(tx, batch3)
	_ = s.storage.Add(tx, batch4)
	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Require().NotNil(req)
	s.Len(req.Topics, 2)

	// Find topics
	topicMap := make(map[string]rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets)
	for _, topic := range req.Topics {
		topicMap[topic.Path] = topic
	}

	s.Contains(topicMap, "topic-1")
	s.Contains(topicMap, "topic-2")
	s.Len(topicMap["topic-1"].Partitions, 2)
	s.Len(topicMap["topic-2"].Partitions, 2)

	// Check topic-1 partitions
	partition1Map := make(map[int64]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets)
	for _, partition := range topicMap["topic-1"].Partitions {
		partition1Map[partition.PartitionID] = partition
	}
	s.Contains(partition1Map, int64(1))
	s.Contains(partition1Map, int64(2))

	// Check topic-2 partitions
	partition2Map := make(map[int64]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets)
	for _, partition := range topicMap["topic-2"].Partitions {
		partition2Map[partition.PartitionID] = partition
	}
	s.Contains(partition2Map, int64(1))
	s.Contains(partition2Map, int64(2))
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_ComplexOptimization() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	// Adjacent batches that should be merged
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-1", 1, 20, 30, 1)
	batch3 := s.createTestBatch("topic-1", 1, 30, 40, 1)
	// Non-adjacent batch (should not merge)
	batch4 := s.createTestBatch("topic-1", 1, 50, 60, 1)

	_ = s.storage.Add(tx, batch1)
	_ = s.storage.Add(tx, batch2)
	_ = s.storage.Add(tx, batch3)
	_ = s.storage.Add(tx, batch4)
	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Require().NotNil(req)
	s.Len(req.Topics, 1)
	s.Len(req.Topics[0].Partitions, 1)
	// First three should be merged into one range, fourth is separate
	s.Len(req.Topics[0].Partitions[0].PartitionOffsets, 2)
	s.Equal(rawtopiccommon.Offset(10), req.Topics[0].Partitions[0].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(40), req.Topics[0].Partitions[0].PartitionOffsets[0].End)
	s.Equal(rawtopiccommon.Offset(50), req.Topics[0].Partitions[0].PartitionOffsets[1].Start)
	s.Equal(rawtopiccommon.Offset(60), req.Topics[0].Partitions[0].PartitionOffsets[1].End)
}

func (s *BatchTxStorageTestSuite) TestGetUpdateOffsetsInTransactionRequest_MixedPartitionsAndTopics() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	// Topic 1, partition 1 - adjacent batches (should merge)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-1", 1, 20, 30, 1)
	// Topic 1, partition 2 - non-adjacent batches (should not merge)
	batch3 := s.createTestBatch("topic-1", 2, 40, 50, 2)
	batch4 := s.createTestBatch("topic-1", 2, 60, 70, 2)
	// Topic 2, partition 1 - single batch
	batch5 := s.createTestBatch("topic-2", 1, 80, 90, 3)

	_ = s.storage.Add(tx, batch1)
	_ = s.storage.Add(tx, batch2)
	_ = s.storage.Add(tx, batch3)
	_ = s.storage.Add(tx, batch4)
	_ = s.storage.Add(tx, batch5)
	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)

	s.Require().NotNil(req)
	s.Len(req.Topics, 2)

	// Find topics
	topicMap := make(map[string]rawtopic.UpdateOffsetsInTransactionRequest_TopicOffsets)
	for _, topic := range req.Topics {
		topicMap[topic.Path] = topic
	}

	// Check topic-1: should have 2 partitions
	s.Contains(topicMap, "topic-1")
	s.Len(topicMap["topic-1"].Partitions, 2)

	// Check topic-2: should have 1 partition
	s.Contains(topicMap, "topic-2")
	s.Len(topicMap["topic-2"].Partitions, 1)

	// Verify topic-1 partition 1 (merged)
	partition1Map := make(map[int64]rawtopic.UpdateOffsetsInTransactionRequest_PartitionOffsets)
	for _, partition := range topicMap["topic-1"].Partitions {
		partition1Map[partition.PartitionID] = partition
	}
	s.Len(partition1Map[1].PartitionOffsets, 1) // Merged
	s.Equal(rawtopiccommon.Offset(10), partition1Map[1].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(30), partition1Map[1].PartitionOffsets[0].End)

	// Verify topic-1 partition 2 (not merged)
	s.Len(partition1Map[2].PartitionOffsets, 2) // Not merged
	s.Equal(rawtopiccommon.Offset(40), partition1Map[2].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(50), partition1Map[2].PartitionOffsets[0].End)
	s.Equal(rawtopiccommon.Offset(60), partition1Map[2].PartitionOffsets[1].Start)
	s.Equal(rawtopiccommon.Offset(70), partition1Map[2].PartitionOffsets[1].End)

	// Verify topic-2 partition 1
	s.Len(topicMap["topic-2"].Partitions[0].PartitionOffsets, 1)
	s.Equal(rawtopiccommon.Offset(80), topicMap["topic-2"].Partitions[0].PartitionOffsets[0].Start)
	s.Equal(rawtopiccommon.Offset(90), topicMap["topic-2"].Partitions[0].PartitionOffsets[0].End)
}

func (s *BatchTxStorageTestSuite) TestClear() {
	tx := newMockTransactionWrapper("session-1", "tx-1")
	s.materializeTx(tx)
	batch := s.createTestBatch("topic-1", 1, 10, 20, 1)

	_ = s.storage.Add(tx, batch)
	s.storage.Clear(tx)

	batches := s.storage.GetBatches(tx)
	s.Empty(batches)

	req := s.storage.GetUpdateOffsetsInTransactionRequest(tx)
	s.Nil(req)
}

func (s *BatchTxStorageTestSuite) TestMultipleTransactions() {
	tx1 := newMockTransactionWrapper("session-1", "tx-1")
	tx2 := newMockTransactionWrapper("session-2", "tx-2")
	s.materializeTx(tx1)
	s.materializeTx(tx2)
	batch1 := s.createTestBatch("topic-1", 1, 10, 20, 1)
	batch2 := s.createTestBatch("topic-2", 2, 30, 40, 2)

	_ = s.storage.Add(tx1, batch1)
	_ = s.storage.Add(tx2, batch2)

	batches1 := s.storage.GetBatches(tx1)
	s.Len(batches1, 1)
	s.Equal(batch1, batches1[0])

	batches2 := s.storage.GetBatches(tx2)
	s.Len(batches2, 1)
	s.Equal(batch2, batches2[0])

	s.storage.Clear(tx1)

	batches1 = s.storage.GetBatches(tx1)
	s.Empty(batches1)

	batches2 = s.storage.GetBatches(tx2)
	s.Len(batches2, 1)
	s.Equal(batch2, batches2[0])
}

// Helper methods

func (s *BatchTxStorageTestSuite) createTestBatch(topic string, partitionID int64, startOffset, endOffset int64, sessionID int) *topicreadercommon.PublicBatch {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := topicreadercommon.NewPartitionSession(
		ctx,
		topic,
		partitionID,
		1,
		"connection-1",
		rawtopicreader.PartitionSessionID(sessionID),
		int64(sessionID),
		rawtopiccommon.Offset(0),
	)

	batch, err := topicreadercommon.NewBatch(session, nil)
	s.Require().NoError(err)

	commitRange := topicreadercommon.CommitRange{
		CommitOffsetStart: rawtopiccommon.Offset(startOffset),
		CommitOffsetEnd:   rawtopiccommon.Offset(endOffset),
		PartitionSession:  session,
	}

	return topicreadercommon.BatchSetCommitRangeForTest(batch, commitRange)
}
